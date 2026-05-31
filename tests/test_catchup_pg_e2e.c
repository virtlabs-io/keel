/**
 * @file test_catchup_pg_e2e.c
 * @brief Live-PostgreSQL end-to-end test of the catch-up probe state
 *        machine (Phase 2b-test, part 4).
 *
 * Exercises the full reactor-async probe path against a real backend:
 *
 *   1. Build a `backend_pool_t` for a replica using
 *      `backend_pool_create()` (does DNS resolution).
 *   2. Wire it into a minimal `keel_worker_t` shim alongside a real
 *      `keel_reactor_t`.
 *   3. Create a `keel_catchup_manager_t`.
 *   4. Read the primary's current LSN via the test oracle
 *      (`integ_pg_*`).
 *   5. Enqueue a waiter against `server_index = 0` carrying that LSN.
 *   6. Drive the manager → `keel_catchup_pg_drive()`.
 *   7. The PG probe SM runs the COMPLETE
 *      `backend_async_start` (TCP + StartupMessage + SCRAM-SHA-256 /
 *      AuthOK + optional TLS) followed by the catch-up
 *      `SELECT (pg_last_wal_replay_lsn() IS NULL …)` round-trip.
 *   8. Tick the reactor until the resume callback fires.
 *   9. Assert outcome == `KEEL_CATCHUP_REACHED`.
 *
 * The test skips gracefully when no PostgreSQL cluster is reachable.
 * In CI, point KEEL_TEST_PG_HOST1 / KEEL_TEST_PG_PORT1 (primary) and
 * KEEL_TEST_PG_HOST2 / KEEL_TEST_PG_PORT2 (replica) at a streaming
 * pair, or start `docker/compose/pg-streaming.yml` (defaults
 * 127.0.0.1:15432 / :15433).
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"

#include "../src/worker/worker_catchup_internal.h"

#include "keel/engine/backend_pool.h"
#include "keel/engine/catchup.h"
#include "keel/engine/worker.h"
#include "keel/mem/mem.h"
#include "keel/plugin/plugin_types.h"
#include "keel/reactor/reactor.h"
#include "keel/util/util.h"

#include <libpq-fe.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int g_tests_run = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;
int test_summary(void) { return g_tests_failed ? 1 : 0; }

static bool g_skip = false;

/* Endpoint discovery: env-driven, with sensible local-docker defaults.
 * The catch-up probe runs against the *replica*; we also need libpq
 * access to sample its current replay LSN as the test target. */
static const char* replica_host(void) {
    const char* v = getenv("KEEL_TEST_PG_HOST2");
    return (v && *v) ? v : "127.0.0.1";
}
static int replica_port(void) {
    const char* v = getenv("KEEL_TEST_PG_PORT2");
    return (v && *v) ? atoi(v) : 5433;
}
static const char* pg_user(void) {
    const char* v = getenv("KEEL_TEST_PG_USER");
    return (v && *v) ? v : "postgres";
}
static const char* pg_password(void) {
    const char* v = getenv("KEEL_TEST_PG_PASSWORD");
    return (v && *v) ? v : "postgres";
}
static const char* pg_database(void) {
    const char* v = getenv("KEEL_TEST_PG_DATABASE");
    return (v && *v) ? v : "postgres";
}

static PGconn* pg_open(const char* host, int port) {
    char conninfo[512];
    snprintf(conninfo, sizeof conninfo,
        "host=%s port=%d user=%s password=%s dbname=%s connect_timeout=5",
        host, port, pg_user(), pg_password(), pg_database());
    PGconn* c = PQconnectdb(conninfo);
    if (PQstatus(c) != CONNECTION_OK) {
        fprintf(stderr, "  libpq connect to %s:%d failed: %s",
                host, port, PQerrorMessage(c));
        PQfinish(c);
        return NULL;
    }
    return c;
}

/* Sentinel — the manager never dereferences the session pointer. */
static int dummy_session_sentinel;

typedef struct {
    keel_catchup_outcome_t outcome;
    int                    call_count;
} resume_capture_t;

static void capture_cb(struct keel_session* s,
                       keel_catchup_outcome_t outcome,
                       void* userdata)
{
    (void)s;
    resume_capture_t* cap = (resume_capture_t*)userdata;
    cap->outcome = outcome;
    cap->call_count++;
}

/** Tick the reactor once with a small timeout. */
static int reactor_tick(keel_reactor_t* r, int timeout_ms)
{
    keel_reactor_submit(r);
    int n = keel_reactor_wait(r, timeout_ms);
    if (n <= 0) return n;
    return keel_reactor_process(r);
}

/** Read the replica's current replay LSN as a "HHHH/LLLLLLLL" string.
 *
 *  We aim the catch-up probe at the replica's *own* current position
 *  (or anything ≤ it), which guarantees the probe returns REACHED on
 *  the very first round regardless of replication lag in the test
 *  fixture. The point of this e2e is to validate the catch-up plumbing
 *  end-to-end (TCP + SCRAM + Query + parse + waiter release), not to
 *  measure replication lag.
 */
static bool read_replica_replay_lsn(char* out, size_t cap)
{
    PGconn* c = pg_open(replica_host(), replica_port());
    if (!c) return false;

    PGresult* r = PQexec(c, "SELECT pg_last_wal_replay_lsn()::text");
    bool ok = false;
    if (PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) == 1) {
        const char* v = PQgetvalue(r, 0, 0);
        if (v && *v) {
            strncpy(out, v, cap - 1);
            out[cap - 1] = '\0';
            ok = true;
        }
    }
    PQclear(r);
    PQfinish(c);
    return ok;
}

/* ==========================================================================
 * Test: full handshake + probe + REACHED against a live replica
 * ==========================================================================*/
static void test_e2e_probe_reaches_lsn(void)
{
    TEST_BEGIN("pg catchup e2e: full backend_async_start + probe + REACHED");

    if (g_skip) { printf("  SKIP (no cluster)\n"); TEST_END(); return; }

    /* --- Discover replica endpoint. --- */
    const char* rhost = replica_host();
    int         rport = replica_port();

    /* Sanity: replica must answer libpq before we bother spinning up the
     * reactor-async stack. */
    {
        PGconn* probe = pg_open(rhost, rport);
        if (!probe) {
            printf("  SKIP (replica %s:%d unreachable)\n", rhost, rport);
            TEST_END();
            return;
        }
        PQfinish(probe);
    }

    /* --- Build the backend pool for the replica. --- */
    backend_pool_config_t cfg = {
        .host            = rhost,
        .port            = (uint16_t)rport,
        .user            = pg_user(),
        .password        = pg_password(),
        .database        = pg_database(),
        .protocol        = "postgres",
        .min_connections = 0,
        .max_connections = 4,
        .max_waiting     = 16,
        .wait_timeout_ms = 5000,
    };
    backend_pool_t* pool = backend_pool_create(&cfg);
    TEST_ASSERT_NOT_NULL(pool);

    /* --- Reactor + minimal worker shim. --- */
    keel_reactor_t* r = keel_reactor_create(NULL);
    TEST_ASSERT_NOT_NULL(r);

    backend_pool_t* pools[1] = { pool };
    keel_worker_t w = {0};
    w.reactor           = r;
    w.server_pools      = pools;
    w.server_pool_count = 1;

    keel_catchup_manager_t* m = keel_catchup_manager_create(&w, NULL);
    TEST_ASSERT_NOT_NULL(m);

    /* --- Read the replica's current replay LSN. --- */
    char lsn[64] = {0};
    bool got_lsn = read_replica_replay_lsn(lsn, sizeof lsn);
    TEST_ASSERT(got_lsn);
    printf("  replica replay LSN target: %s\n", lsn);

    /* Park one waiter at that LSN. */
    keel_consistency_token_t tok = {0};
    strncpy(tok.value, lsn, sizeof tok.value - 1);
    tok.timeline_id = 1;

    resume_capture_t cap = {0};
    keel_catchup_waiter_t* w_handle = keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, /*server*/ 0, &tok,
        /*max_wait_ms*/ 30000, capture_cb, &cap);
    TEST_ASSERT_NOT_NULL(w_handle);

    /* --- Kick the SM and tick the reactor until resume. --- *
     *
     *  Budget generously: the path includes a fresh TCP connect, the
     *  PostgreSQL startup handshake (StartupMessage → optional
     *  AuthenticationSASL → SCRAM-SHA-256 exchange → AuthenticationOk
     *  → ParameterStatus/BackendKeyData/ReadyForQuery), then the
     *  catch-up Query round-trip, and finally the replica catching
     *  up to the primary's just-switched WAL position. 10 s is a
     *  comfortable upper bound for a local docker-compose cluster.
     */
    keel_catchup_pg_drive(m, /*server*/ 0, (uint64_t)keel_time_now());

    const int  budget_ticks = 1000;       /* 1000 × 10 ms = 10 s */
    int        ticks        = 0;
    while (cap.call_count == 0 && ticks < budget_ticks) {
        reactor_tick(r, 10);
        /* Re-kick periodically — the SM is edge-triggered: after the
         * initial CONNECTING completion, subsequent state transitions
         * are driven by I/O completions, but if the wait list grows
         * mid-probe we want pg_drive to be invoked again. */
        if ((ticks % 25) == 0) {
            keel_catchup_pg_drive(m, 0, (uint64_t)keel_time_now());
        }
        ticks++;
    }

    if (cap.call_count == 0) {
        keel_catchup_stats_snapshot_t dbg;
        keel_catchup_manager_snapshot(m, &dbg);
        fprintf(stderr,
            "  diagnostic: probes_issued=%lu succeeded=%lu failed=%lu "
            "cache_hits=%lu waiters_active=%zu after %d ticks\n",
            (unsigned long)dbg.probes_issued_total,
            (unsigned long)dbg.probes_succeeded_total,
            (unsigned long)dbg.probes_failed_total,
            (unsigned long)dbg.cache_hits_total,
            dbg.waiters_active, ticks);
    }    TEST_ASSERT_EQ(cap.call_count, 1);
    TEST_ASSERT_EQ(cap.outcome, KEEL_CATCHUP_REACHED);

    keel_catchup_stats_snapshot_t snap;
    keel_catchup_manager_snapshot(m, &snap);
    TEST_ASSERT(snap.probes_succeeded_total >= 1);
    TEST_ASSERT_EQ(snap.waiters_active, (size_t)0);

    /* --- Teardown. --- */
    keel_catchup_manager_destroy(m);
    keel_reactor_destroy(r);
    backend_pool_destroy(pool);

    TEST_END();
}

int main(void)
{
    keel_mem_init(NULL);

    /* Probe the replica via libpq — supports SCRAM-SHA-256 / MD5 / trust.
     * We need libpq to talk to the replica too (to sample its current
     * replay LSN as the catch-up target). */
    PGconn* sanity = pg_open(replica_host(), replica_port());
    if (!sanity) {
        printf("SKIP: replica unreachable at %s:%d — set "
               "KEEL_TEST_PG_HOST2/KEEL_TEST_PG_PORT2 (the catch-up target), "
               "or start docker/compose/pg-streaming.yml\n",
               replica_host(), replica_port());
        g_skip = true;
    } else {
        PQfinish(sanity);
    }

    test_e2e_probe_reaches_lsn();

    printf("\n%d tests run — %d passed, %d failed\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return test_summary();
}
