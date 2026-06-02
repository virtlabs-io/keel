/**
 * @file test_engine_catchup_consult_pg_e2e.c
 * @brief Live-PostgreSQL e2e for the engine wait-catchup consult helper
 *        (Patch 2d-5).
 *
 * Validates the engine integration from Patch 2d-4 against a real
 * PostgreSQL primary + replica pair. The helper
 * `keel_engine_should_degrade_to_primary_on_wait` (the predicate
 * `engine_flow.c` consults before READ-pool selection) is exercised
 * with:
 *
 *   - a `keel_router_t` configured for
 *       consistency_mode  = read_your_writes
 *       stale_read_policy = wait
 *     plus a real primary endpoint and a real replica endpoint
 *     (DNS-resolved through libpq);
 *   - a `keel_consistency_token_t` whose `value` is a WAL LSN actually
 *     captured from the live primary via libpq
 *     (`SELECT pg_current_wal_lsn()::text`);
 *   - a query tree parsed by `keel_sql_analyze_full` from a real
 *     replica-safe SELECT.
 *
 * Asserts the helper returns `true` (engine must degrade to primary)
 * and that the returned `keel_route_decision_t` carries
 * `KEEL_ROUTE_REASON_WAIT_CATCHUP`, a non-zero `wait_max_ms`, and the
 * exact LSN we fed in.
 *
 * Skips gracefully when no PostgreSQL primary is reachable.  In CI
 * point KEEL_TEST_PG_HOST1/PORT1 at the primary, or just start
 * docker/compose/pg-streaming.yml — defaults 127.0.0.1:5432 match the
 * developer-laptop docker compose used by the rest of the suite.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/engine/catchup_consult.h"
#include "keel/core/router.h"
#include "keel/sql/sql.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <libpq-fe.h>

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST_BEGIN(name) do { printf("  %-66s ", name); fflush(stdout); } while (0)
#define TEST_PASS()      do { printf("[PASS]\n"); tests_passed++; } while (0)
#define TEST_SKIP(why)   do { printf("[SKIP] %s\n", (why)); } while (0)
#define ASSERT(cond) do { \
    if (!(cond)) { \
        printf("[FAIL]\n    Assertion failed: %s\n    at %s:%d\n", \
               #cond, __FILE__, __LINE__); \
        tests_failed++; return; \
    } \
} while (0)
#define ASSERT_EQ(a, b)  ASSERT((a) == (b))
#define ASSERT_NE(a, b)  ASSERT((a) != (b))
#define ASSERT_TRUE(x)   ASSERT(x)

static const char* primary_host(void) {
    const char* v = getenv("KEEL_TEST_PG_HOST1");
    return (v && *v) ? v : "127.0.0.1";
}
static int primary_port(void) {
    const char* v = getenv("KEEL_TEST_PG_PORT1");
    return (v && *v) ? atoi(v) : 5432;
}
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

/** Capture the primary's current WAL LSN as a "HHHH/LLLLLLLL" string.
 *  Returns false on any error; the test then skips. */
static bool capture_primary_lsn(char* out, size_t cap)
{
    PGconn* c = pg_open(primary_host(), primary_port());
    if (!c) return false;

    PGresult* r = PQexec(c, "SELECT pg_current_wal_lsn()::text");
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

static keel_router_t* build_ryw_wait_router_for_live(void)
{
    keel_router_config_t cfg   = keel_router_config_default();
    cfg.consistency_mode       = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy      = KEEL_STALE_READ_WAIT;
    cfg.max_replica_catchup_ms = 250;
    keel_router_t* r = keel_router_create(&cfg);
    if (!r) return NULL;

    keel_route_server_t p;
    memset(&p, 0, sizeof(p));
    p.name        = "primary";
    p.host        = primary_host();
    p.port        = (uint16_t)primary_port();
    p.role        = KEEL_SERVER_PRIMARY;
    p.timeline_id = 1;
    p.weight      = 100;
    p.health      = KEEL_HEALTH_UP;

    keel_route_server_t rep;
    memset(&rep, 0, sizeof(rep));
    rep.name        = "replica1";
    rep.host        = replica_host();
    rep.port        = (uint16_t)replica_port();
    rep.role        = KEEL_SERVER_REPLICA;
    rep.timeline_id = 1;
    rep.weight      = 100;
    rep.health      = KEEL_HEALTH_UP;

    if (keel_router_add_server(r, &p)   != KEEL_OK) { keel_router_destroy(r); return NULL; }
    if (keel_router_add_server(r, &rep) != KEEL_OK) { keel_router_destroy(r); return NULL; }
    return r;
}

/* ==========================================================================
 * Test: real LSN captured from live primary triggers WAIT verdict and
 *       the helper instructs the engine to degrade.
 * ==========================================================================*/
static void test_live_primary_lsn_triggers_wait_degrade(void)
{
    TEST_BEGIN("pg engine-consult e2e: live LSN -> WAIT_CATCHUP -> degrade");

    char lsn[64] = { 0 };
    if (!capture_primary_lsn(lsn, sizeof lsn)) {
        TEST_SKIP("primary unreachable (set KEEL_TEST_PG_HOST1/PORT1 or start docker)");
        return;
    }
    /* PG LSN is non-empty hex/slash; first char must be a hex digit. */
    ASSERT_TRUE(lsn[0] != '\0');
    ASSERT_TRUE(strchr(lsn, '/') != NULL);

    keel_router_t* r = build_ryw_wait_router_for_live();
    ASSERT_NE(r, NULL);

    keel_arena_t* arena = keel_arena_create(65536);
    ASSERT_NE(arena, NULL);
    const char* sql = "SELECT id, email FROM users WHERE id = 42";
    keel_str_t s = { .data = sql, .len = strlen(sql) };
    const keel_qt_query_t* qt = keel_sql_analyze_full(s, arena);
    ASSERT_NE(qt, NULL);

    keel_consistency_token_t t;
    memset(&t, 0, sizeof(t));
    snprintf(t.value, sizeof(t.value), "%s", lsn);
    t.timeline_id = 1;

    keel_route_decision_t rd;
    memset(&rd, 0, sizeof(rd));
    bool degrade = keel_engine_should_degrade_to_primary_on_wait(
        r, qt, &t, /*in_transaction*/ false, &rd);

    ASSERT_TRUE(degrade);
    ASSERT_EQ(rd.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);
    ASSERT_TRUE((rd.decision_factors & KEEL_DF_WAIT_CATCHUP) != 0);
    ASSERT_TRUE(rd.wait_max_ms > 0);
    /* The decision must echo back the exact LSN we asked the router to
     * wait on — no truncation, no mutation. */
    ASSERT_EQ(strcmp(rd.wait_token.value, lsn), 0);
    ASSERT_EQ(rd.wait_token.timeline_id, 1u);

    keel_arena_destroy(arena);
    keel_router_destroy(r);

    printf("(primary LSN=%s, wait_max_ms=%u)  ", lsn, rd.wait_max_ms);
    TEST_PASS();
}

/* ==========================================================================
 * Test: same live LSN under in_transaction=true MUST NOT degrade —
 *       even with WAIT policy, the router skips WAIT inside transactions.
 * ==========================================================================*/
static void test_live_primary_lsn_in_txn_does_not_degrade(void)
{
    TEST_BEGIN("pg engine-consult e2e: live LSN + in_txn -> no degrade");

    char lsn[64] = { 0 };
    if (!capture_primary_lsn(lsn, sizeof lsn)) {
        TEST_SKIP("primary unreachable");
        return;
    }

    keel_router_t* r = build_ryw_wait_router_for_live();
    ASSERT_NE(r, NULL);

    keel_arena_t* arena = keel_arena_create(65536);
    ASSERT_NE(arena, NULL);
    const char* sql = "SELECT 1";
    keel_str_t s = { .data = sql, .len = strlen(sql) };
    const keel_qt_query_t* qt = keel_sql_analyze_full(s, arena);
    ASSERT_NE(qt, NULL);

    keel_consistency_token_t t;
    memset(&t, 0, sizeof(t));
    snprintf(t.value, sizeof(t.value), "%s", lsn);
    t.timeline_id = 1;

    bool degrade = keel_engine_should_degrade_to_primary_on_wait(
        r, qt, &t, /*in_transaction*/ true, NULL);
    ASSERT_TRUE(!degrade);

    keel_arena_destroy(arena);
    keel_router_destroy(r);
    TEST_PASS();
}

int main(void)
{
    printf("\n=== test_engine_catchup_consult_pg_e2e (Patch 2d-5) ===\n");

    test_live_primary_lsn_triggers_wait_degrade();
    test_live_primary_lsn_in_txn_does_not_degrade();

    printf("\n%d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
