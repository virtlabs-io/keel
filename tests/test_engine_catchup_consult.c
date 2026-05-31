/**
 * @file test_engine_catchup_consult.c
 * @brief Unit tests for the engine-side wait-catchup consult helper (Patch 2d-4).
 *
 * Covers `keel_engine_should_degrade_to_primary_on_wait` — the boolean
 * predicate the engine hot path calls to decide whether a token-bearing
 * replica-eligible read must be degraded to the primary because the
 * deployment configured `stale_read_policy=wait` but v0.5-alpha cannot
 * yet park + re-dispatch.
 *
 * NULL/empty input must be safe (short-circuit false). WAIT verdict must
 * be reported as true and forward the wait_* fields. Non-WAIT verdicts
 * (route_primary, no replica, in-transaction) must be reported as false.
 */

#include "keel/engine/catchup_consult.h"
#include "keel/core/router.h"
#include "keel/sql/sql.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <stdio.h>
#include <string.h>

static keel_arena_t* g_qt_arena = NULL;

static const keel_qt_query_t* parse_read_qt(const char* sql)
{
    if (!g_qt_arena) g_qt_arena = keel_arena_create(65536);
    keel_str_t s = { .data = sql, .len = strlen(sql) };
    return keel_sql_analyze_full(s, g_qt_arena);
}

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) static void test_##name(void)
#define RUN_TEST(name) do { \
    printf("  %-60s ", #name); fflush(stdout); \
    test_##name(); \
    printf("[PASS]\n"); tests_passed++; \
} while(0)
#define ASSERT(cond) do { \
    if (!(cond)) { \
        printf("[FAIL]\n    Assertion failed: %s\n    at %s:%d\n", \
               #cond, __FILE__, __LINE__); \
        tests_failed++; return; \
    } \
} while(0)
#define ASSERT_EQ(a, b)     ASSERT((a) == (b))
#define ASSERT_NE(a, b)     ASSERT((a) != (b))
#define ASSERT_TRUE(x)      ASSERT(x)
#define ASSERT_FALSE(x)     ASSERT(!(x))

static keel_router_t* create_ryw_wait_router(void)
{
    keel_router_config_t cfg   = keel_router_config_default();
    cfg.consistency_mode       = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy      = KEEL_STALE_READ_WAIT;
    cfg.max_replica_catchup_ms = 100;
    keel_router_t* r = keel_router_create(&cfg);
    if (!r) return NULL;

    keel_route_server_t p = {
        .name = "primary", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 5,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t rep = {
        .name = "replica1", .host = "127.0.0.1", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 5,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    if (keel_router_add_server(r, &p)   != KEEL_OK) { keel_router_destroy(r); return NULL; }
    if (keel_router_add_server(r, &rep) != KEEL_OK) { keel_router_destroy(r); return NULL; }
    return r;
}

static keel_router_t* create_ryw_route_primary_router(void)
{
    keel_router_config_t cfg   = keel_router_config_default();
    cfg.consistency_mode       = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy      = KEEL_STALE_READ_ROUTE_PRIMARY;
    keel_router_t* r = keel_router_create(&cfg);
    if (!r) return NULL;
    keel_route_server_t p = {
        .name = "primary", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 5,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t rep = {
        .name = "replica1", .host = "127.0.0.1", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 5,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    (void)keel_router_add_server(r, &p);
    (void)keel_router_add_server(r, &rep);
    return r;
}

static keel_consistency_token_t make_token(const char* lsn, uint32_t tl)
{
    keel_consistency_token_t t;
    memset(&t, 0, sizeof(t));
    snprintf(t.value, sizeof(t.value), "%s", lsn);
    t.timeline_id = tl;
    return t;
}

/* §1: NULL router → false, no crash. */
TEST(null_router_returns_false) {
    keel_consistency_token_t t = make_token("0/100", 5);
    const keel_qt_query_t* qt = parse_read_qt("SELECT 1");
    keel_route_decision_t rd;
    memset(&rd, 0, sizeof(rd));
    ASSERT_FALSE(keel_engine_should_degrade_to_primary_on_wait(NULL, qt, &t, false, &rd));
}

/* §2: NULL token → false. */
TEST(null_token_returns_false) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    const keel_qt_query_t* qt = parse_read_qt("SELECT 1");
    ASSERT_FALSE(keel_engine_should_degrade_to_primary_on_wait(r, qt, NULL, false, NULL));
    keel_router_destroy(r);
}

/* §3: Empty token (value[0]=='\0') → false. */
TEST(empty_token_returns_false) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    const keel_qt_query_t* qt = parse_read_qt("SELECT 1");
    keel_consistency_token_t t;
    memset(&t, 0, sizeof(t));   /* value[0]==0 */
    ASSERT_FALSE(keel_engine_should_degrade_to_primary_on_wait(r, qt, &t, false, NULL));
    keel_router_destroy(r);
}

/* §3b: NULL qt → false (router cannot classify without a parsed query). */
TEST(null_qt_returns_false) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    keel_consistency_token_t t = make_token("0/100", 5);
    ASSERT_FALSE(keel_engine_should_degrade_to_primary_on_wait(r, NULL, &t, false, NULL));
    keel_router_destroy(r);
}

/* §4: WAIT verdict — populated token + replica + RYW+WAIT policy → true,
 *     out_decision carries the wait_* fields. */
TEST(wait_verdict_returns_true_and_fills_decision) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    keel_consistency_token_t t = make_token("0/ABCDEF12", 5);
    const keel_qt_query_t* qt = parse_read_qt("SELECT id, name FROM users WHERE id = 7");
    ASSERT_NE(qt, NULL);

    keel_route_decision_t rd;
    memset(&rd, 0, sizeof(rd));
    bool degrade = keel_engine_should_degrade_to_primary_on_wait(r, qt, &t, false, &rd);
    ASSERT_TRUE(degrade);
    ASSERT_EQ(rd.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);
    ASSERT_TRUE((rd.decision_factors & KEEL_DF_WAIT_CATCHUP) != 0);
    ASSERT_TRUE(rd.wait_max_ms > 0);
    ASSERT_EQ(strcmp(rd.wait_token.value, t.value), 0);
    ASSERT_EQ(rd.wait_token.timeline_id, t.timeline_id);
    keel_router_destroy(r);
}

/* §5: out_decision==NULL is allowed; helper still returns the right bool. */
TEST(null_out_decision_is_allowed) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    keel_consistency_token_t t = make_token("0/100", 5);
    const keel_qt_query_t* qt = parse_read_qt("SELECT id FROM users");
    ASSERT_TRUE(keel_engine_should_degrade_to_primary_on_wait(r, qt, &t, false, NULL));
    keel_router_destroy(r);
}

/* §6: stale_read_policy = route_primary → false (no WAIT verdict). */
TEST(route_primary_policy_returns_false) {
    keel_router_t* r = create_ryw_route_primary_router();
    ASSERT_NE(r, NULL);
    keel_consistency_token_t t = make_token("0/100", 5);
    const keel_qt_query_t* qt = parse_read_qt("SELECT 1");
    keel_route_decision_t rd;
    memset(&rd, 0, sizeof(rd));
    ASSERT_FALSE(keel_engine_should_degrade_to_primary_on_wait(r, qt, &t, false, &rd));
    ASSERT_NE(rd.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);
    keel_router_destroy(r);
}

/* §7: in_transaction=true skips WAIT (router invariant from 2d-2). */
TEST(in_transaction_returns_false) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    keel_consistency_token_t t = make_token("0/100", 5);
    const keel_qt_query_t* qt = parse_read_qt("SELECT 1");
    ASSERT_FALSE(keel_engine_should_degrade_to_primary_on_wait(r, qt, &t, true, NULL));
    keel_router_destroy(r);
}

int main(void)
{
    printf("\n=== test_engine_catchup_consult (Patch 2d-4) ===\n");

    RUN_TEST(null_router_returns_false);
    RUN_TEST(null_token_returns_false);
    RUN_TEST(empty_token_returns_false);
    RUN_TEST(null_qt_returns_false);
    RUN_TEST(wait_verdict_returns_true_and_fills_decision);
    RUN_TEST(null_out_decision_is_allowed);
    RUN_TEST(route_primary_policy_returns_false);
    RUN_TEST(in_transaction_returns_false);

    if (g_qt_arena) keel_arena_destroy(g_qt_arena);

    printf("\n%d passed, %d failed\n", tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
