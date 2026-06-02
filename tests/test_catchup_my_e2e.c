/**
 * @file test_catchup_my_e2e.c
 * @brief Live-MySQL end-to-end test of the catch-up probe state
 *        machine (Phase 2c).
 *
 * Mirrors test_catchup_pg_e2e.c for MySQL. Exercises the full
 * reactor-async probe path against a real backend:
 *
 *   1. Build a `backend_pool_t` for a replica.
 *   2. Wire it into a minimal `keel_worker_t` shim + `keel_reactor_t`.
 *   3. Create a `keel_catchup_manager_t`.
 *   4. Read the replica's current `@@global.gtid_executed` via the
 *      `mysql` CLI as oracle.
 *   5. Park a waiter against `server_index = 0` carrying that GTID set.
 *   6. Drive `keel_catchup_my_drive()`.
 *   7. The MySQL probe SM runs the COMPLETE `backend_async_start`
 *      (TCP + handshake + caching_sha2_password / mysql_native_password
 *      + optional TLS) followed by the catch-up
 *      `SELECT WAIT_FOR_EXECUTED_GTID_SET('<gtid>', 0)` round-trip.
 *   8. Tick the reactor until the resume callback fires.
 *   9. Assert outcome == `KEEL_CATCHUP_REACHED`.
 *
 * The test SKIPS gracefully when no MySQL replica is reachable or
 * when the `mysql` CLI is not on PATH. In CI, point
 * `KEEL_TEST_MY_HOST2` / `KEEL_TEST_MY_PORT2` (replica) at a
 * GTID-enabled MySQL, or start `docker/compose/mysql-replication.yml`
 * (defaults 127.0.0.1:3307).
 *
 * We use the `mysql` shell CLI as oracle (rather than linking
 * libmysqlclient into the test binary) so this test stays independent
 * of the libmysql build dependency. The probe SM itself still goes
 * through KEEL's own backend_async_start, which is the actual subject
 * under test.
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int g_tests_run = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;
int test_summary(void) { return g_tests_failed ? 1 : 0; }

static bool g_skip = false;

/* Endpoint discovery: env-driven, with docker-compose defaults. */
static const char* replica_host(void) {
    const char* v = getenv("KEEL_TEST_MY_HOST2");
    return (v && *v) ? v : "127.0.0.1";
}
static int replica_port(void) {
    const char* v = getenv("KEEL_TEST_MY_PORT2");
    return (v && *v) ? atoi(v) : 3307;
}
static const char* my_user(void) {
    const char* v = getenv("KEEL_TEST_MY_USER");
    return (v && *v) ? v : "root";
}
static const char* my_password(void) {
    const char* v = getenv("KEEL_TEST_MY_PASSWORD");
    return (v && *v) ? v : "root";
}
static const char* my_database(void) {
    const char* v = getenv("KEEL_TEST_MY_DATABASE");
    return (v && *v) ? v : "mysql";
}

/** Run `mysql --batch --skip-column-names -e <sql>` against the replica
 *  and capture the first line of output into @p out. Returns false on
 *  failure (mysql not on PATH, connect failure, empty result). */
static bool mysql_cli_query(const char* sql, char* out, size_t cap)
{
    char cmd[2048];
    snprintf(cmd, sizeof cmd,
        "mysql --protocol=TCP -h '%s' -P %d -u '%s' -p'%s' "
        "--batch --skip-column-names --connect-timeout=5 "
        "-e \"%s\" '%s' 2>/dev/null",
        replica_host(), replica_port(), my_user(), my_password(),
        sql, my_database());

    FILE* f = popen(cmd, "r");
    if (!f) return false;

    bool ok = false;
    out[0] = '\0';
    if (fgets(out, (int)cap, f)) {
        /* Strip trailing newline. */
        size_t n = strlen(out);
        while (n > 0 && (out[n-1] == '\n' || out[n-1] == '\r')) {
            out[--n] = '\0';
        }
        ok = (out[0] != '\0');
    }
    int rc = pclose(f);
    if (rc != 0) ok = false;
    return ok;
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

static int reactor_tick(keel_reactor_t* r, int timeout_ms)
{
    keel_reactor_submit(r);
    int n = keel_reactor_wait(r, timeout_ms);
    if (n <= 0) return n;
    return keel_reactor_process(r);
}

/* ==========================================================================
 * Test: full handshake + probe + REACHED against a live MySQL replica
 * ==========================================================================*/
static void test_e2e_probe_reaches_gtid(void)
{
    TEST_BEGIN("my catchup e2e: full backend_async_start + probe + REACHED");

    if (g_skip) { printf("  SKIP (no MySQL replica or no mysql CLI)\n");
                  TEST_END(); return; }

    /* --- Read the replica's current GTID set as the catch-up target. */
    char gtid[KEEL_CONSISTENCY_TOKEN_MAX] = {0};
    if (!mysql_cli_query("SELECT @@global.gtid_executed",
                         gtid, sizeof gtid))
    {
        printf("  SKIP (could not read @@global.gtid_executed)\n");
        TEST_END();
        return;
    }
    if (gtid[0] == '\0' || strcmp(gtid, "NULL") == 0) {
        printf("  SKIP (GTID mode disabled on replica)\n");
        TEST_END();
        return;
    }
    printf("  replica GTID target: %s\n", gtid);

    /* --- Build the backend pool for the replica. --- */
    backend_pool_config_t cfg = {
        .host            = replica_host(),
        .port            = (uint16_t)replica_port(),
        .user            = my_user(),
        .password        = my_password(),
        .database        = my_database(),
        .protocol        = "mysql",
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

    /* Park one waiter at the replica's own GTID set — guaranteed REACHED. */
    keel_consistency_token_t tok = {0};
    strncpy(tok.value, gtid, sizeof tok.value - 1);
    tok.timeline_id = 1;

    resume_capture_t cap = {0};
    keel_catchup_waiter_t* w_handle = keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, /*server*/ 0, &tok,
        /*max_wait_ms*/ 30000, capture_cb, &cap);
    TEST_ASSERT_NOT_NULL(w_handle);

    /* --- Kick the SM and tick the reactor until resume. --- */
    keel_catchup_my_drive(m, /*server*/ 0, (uint64_t)keel_time_now());

    const int  budget_ticks = 1000;       /* 1000 × 10 ms = 10 s */
    int        ticks        = 0;
    while (cap.call_count == 0 && ticks < budget_ticks) {
        reactor_tick(r, 10);
        if ((ticks % 25) == 0) {
            keel_catchup_my_drive(m, 0, (uint64_t)keel_time_now());
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
    }
    TEST_ASSERT_EQ(cap.call_count, 1);
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

    /* Sanity probe via the mysql CLI — handles both auth + reachability. */
    char ping[64];
    if (!mysql_cli_query("SELECT 1", ping, sizeof ping)) {
        printf("SKIP: MySQL replica unreachable at %s:%d (or mysql CLI not "
               "installed) — set KEEL_TEST_MY_HOST2/KEEL_TEST_MY_PORT2 "
               "(the catch-up target), or start "
               "docker/compose/mysql-replication.yml\n",
               replica_host(), replica_port());
        g_skip = true;
    }

    test_e2e_probe_reaches_gtid();

    printf("\n%d tests run — %d passed, %d failed\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return test_summary();
}
