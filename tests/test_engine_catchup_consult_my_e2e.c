/**
 * @file test_engine_catchup_consult_my_e2e.c
 * @brief Live-MySQL e2e for the engine wait-catchup consult helper
 *        (Patch 2d-5, MySQL parity to test_engine_catchup_consult_pg_e2e.c).
 *
 * Validates the engine integration from Patch 2d-4 against a real
 * GTID-enabled MySQL primary. The helper
 * `keel_engine_should_degrade_to_primary_on_wait` (the predicate
 * `engine_flow.c` consults before READ-pool selection) is exercised
 * with:
 *
 *   - a `keel_router_t` configured for
 *       consistency_mode  = read_your_writes
 *       stale_read_policy = wait
 *     plus a primary endpoint and a replica endpoint (both addresses
 *     are merely registered with the router — the helper never opens
 *     a connection through them);
 *   - a `keel_consistency_token_t` whose `value` is a GTID set actually
 *     captured from the live MySQL primary via
 *     `SELECT @@global.gtid_executed`;
 *   - a query tree parsed by `keel_sql_analyze_full` from a real
 *     replica-safe SELECT.
 *
 * Asserts the helper returns `true` (engine must degrade to primary)
 * and that the returned `keel_route_decision_t` carries
 * `KEEL_ROUTE_REASON_WAIT_CATCHUP`, a non-zero `wait_max_ms`, and the
 * exact GTID we fed in.
 *
 * Skips gracefully when no MySQL primary is reachable or `mysql` CLI
 * is missing.  In CI point KEEL_TEST_MY_HOST1/PORT1 at the primary,
 * or start docker/compose/mysql-replication.yml — defaults
 * 127.0.0.1:33060 match the developer-laptop docker compose used by
 * the rest of the suite.
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
    const char* v = getenv("KEEL_TEST_MY_HOST1");
    return (v && *v) ? v : "127.0.0.1";
}
static int primary_port(void) {
    const char* v = getenv("KEEL_TEST_MY_PORT1");
    return (v && *v) ? atoi(v) : 33060;
}
static const char* replica_host(void) {
    const char* v = getenv("KEEL_TEST_MY_HOST2");
    return (v && *v) ? v : "127.0.0.1";
}
static int replica_port(void) {
    /* When no live MySQL replica is available, the docker compose
     * developer setup runs only the primary on 33060; re-using the
     * primary endpoint here is safe because the helper never opens
     * a connection through the registered router entries. */
    const char* v = getenv("KEEL_TEST_MY_PORT2");
    return (v && *v) ? atoi(v) : 33060;
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

/** Run `mysql --batch --skip-column-names -e <sql>` against the primary
 *  and capture the first line of output into @p out. Returns false on
 *  failure (mysql not on PATH, connect failure, empty result). */
static bool mysql_cli_query(const char* sql, char* out, size_t cap)
{
    char cmd[2048];
    snprintf(cmd, sizeof cmd,
        "mysql --protocol=TCP -h '%s' -P %d -u '%s' -p'%s' "
        "--batch --skip-column-names --connect-timeout=5 "
        "-e \"%s\" '%s' 2>/dev/null",
        primary_host(), primary_port(), my_user(), my_password(),
        sql, my_database());

    FILE* f = popen(cmd, "r");
    if (!f) return false;

    bool ok = false;
    out[0] = '\0';
    if (fgets(out, (int)cap, f)) {
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
 * Test: real GTID captured from live primary triggers WAIT verdict and
 *       the helper instructs the engine to degrade.
 * ==========================================================================*/
static void test_live_primary_gtid_triggers_wait_degrade(void)
{
    TEST_BEGIN("my engine-consult e2e: live GTID -> WAIT_CATCHUP -> degrade");

    char gtid[KEEL_CONSISTENCY_TOKEN_MAX] = { 0 };
    if (!mysql_cli_query("SELECT @@global.gtid_executed",
                         gtid, sizeof gtid)) {
        TEST_SKIP("primary unreachable / mysql CLI missing "
                  "(set KEEL_TEST_MY_HOST1/PORT1 or start docker)");
        return;
    }
    if (gtid[0] == '\0' || strcmp(gtid, "NULL") == 0) {
        TEST_SKIP("GTID mode disabled on primary");
        return;
    }

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
    snprintf(t.value, sizeof(t.value), "%s", gtid);
    t.timeline_id = 1;

    keel_route_decision_t rd;
    memset(&rd, 0, sizeof(rd));
    bool degrade = keel_engine_should_degrade_to_primary_on_wait(
        r, qt, &t, /*in_transaction*/ false, &rd);

    ASSERT_TRUE(degrade);
    ASSERT_EQ(rd.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);
    ASSERT_TRUE((rd.decision_factors & KEEL_DF_WAIT_CATCHUP) != 0);
    ASSERT_TRUE(rd.wait_max_ms > 0);
    /* The decision must echo back the exact GTID we asked the router to
     * wait on — no truncation, no mutation. */
    ASSERT_EQ(strcmp(rd.wait_token.value, gtid), 0);
    ASSERT_EQ(rd.wait_token.timeline_id, 1u);

    keel_arena_destroy(arena);
    keel_router_destroy(r);

    printf("(primary GTID=%s, wait_max_ms=%u)  ", gtid, rd.wait_max_ms);
    TEST_PASS();
}

/* ==========================================================================
 * Test: same live GTID under in_transaction=true MUST NOT degrade —
 *       even with WAIT policy, the router skips WAIT inside transactions.
 * ==========================================================================*/
static void test_live_primary_gtid_in_txn_does_not_degrade(void)
{
    TEST_BEGIN("my engine-consult e2e: live GTID + in_txn -> no degrade");

    char gtid[KEEL_CONSISTENCY_TOKEN_MAX] = { 0 };
    if (!mysql_cli_query("SELECT @@global.gtid_executed",
                         gtid, sizeof gtid)) {
        TEST_SKIP("primary unreachable / mysql CLI missing");
        return;
    }
    if (gtid[0] == '\0' || strcmp(gtid, "NULL") == 0) {
        TEST_SKIP("GTID mode disabled on primary");
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
    snprintf(t.value, sizeof(t.value), "%s", gtid);
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
    printf("\n=== test_engine_catchup_consult_my_e2e (Patch 2d-5, MySQL parity) ===\n");

    test_live_primary_gtid_triggers_wait_degrade();
    test_live_primary_gtid_in_txn_does_not_degrade();

    printf("\n%d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
