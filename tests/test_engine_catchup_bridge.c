/**
 * @file test_engine_catchup_bridge.c
 * @brief Unit tests for the engine ↔ catch-up manager bridge (Patch 2d-3).
 *
 * Validates `keel_engine_consult_catchup()`: given a router that emits
 * `KEEL_ROUTE_REASON_WAIT_CATCHUP` (Patch 2d-2) and a real per-worker
 * catch-up manager, the bridge MUST enqueue a waiter and return
 * `KEEL_FLOW_WAIT_CATCHUP`. All non-WAIT verdicts MUST collapse to
 * `KEEL_FLOW_OK`. NULL inputs MUST be no-ops.
 *
 * These tests do not drive the probe state machine — the bridge's contract
 * stops at the `keel_catchup_enqueue()` call boundary. Probe-SM behaviour
 * (REACHED / TIMEOUT / PROBE_FAILED) is covered by test_catchup_pg_mock.c
 * and test_catchup_my_mock.c.
 */

#include "keel/engine/catchup.h"
#include "keel/engine/catchup_bridge.h"
#include "keel/core/router.h"
#include "keel/sql/sql.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <stdio.h>
#include <string.h>

/* Shared arena for parsed query trees used in WAIT tests. Reset per test. */
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

/* ============================================================================
 * Fixture: router with primary + 2 replicas under RYW + WAIT, plus a
 * standalone catch-up manager (worker pointer is NULL — supported per
 * keel_catchup_manager_create() doc).
 * ============================================================================ */

typedef struct fixture {
    keel_router_t*          router;
    keel_catchup_manager_t* catchup;
} fixture_t;

static fixture_t make_fixture_ryw_wait(void)
{
    fixture_t fx = {0};

    keel_router_config_t cfg   = keel_router_config_default();
    cfg.consistency_mode       = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy      = KEEL_STALE_READ_WAIT;
    cfg.max_replica_catchup_ms = 75;
    fx.router = keel_router_create(&cfg);
    if (!fx.router) return fx;

    keel_route_server_t p = {
        .name = "primary", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 7,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t r1 = {
        .name = "replica1", .host = "127.0.0.1", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 7,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t r2 = {
        .name = "replica2", .host = "127.0.0.1", .port = 5434,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 7,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_router_add_server(fx.router, &p);
    keel_router_add_server(fx.router, &r1);
    keel_router_add_server(fx.router, &r2);

    keel_catchup_config_t ccfg = (keel_catchup_config_t)KEEL_CATCHUP_CONFIG_DEFAULT;
    fx.catchup = keel_catchup_manager_create(NULL, &ccfg);
    return fx;
}

static void destroy_fixture(fixture_t* fx)
{
    if (fx->catchup) keel_catchup_manager_destroy(fx->catchup);
    if (fx->router)  keel_router_destroy(fx->router);
    memset(fx, 0, sizeof(*fx));
}

static keel_route_session_t make_token_session(const char* lsn, uint32_t tl)
{
    keel_route_session_t s;
    memset(&s, 0, sizeof(s));
    s.requires_consistent_read = true;
    snprintf(s.required_consistency_token.value,
             sizeof(s.required_consistency_token.value), "%s", lsn);
    s.required_consistency_token.timeline_id = tl;
    return s;
}

/* Resume callback captures outcome for inspection. The pointer value of
 * `session` is sentinel-only: the bridge never dereferences it and neither
 * do these tests. */
typedef struct resume_capture {
    int                    call_count;
    keel_catchup_outcome_t outcome;
} resume_capture_t;

static void capture_cb(struct keel_session* s,
                       keel_catchup_outcome_t outcome,
                       void* userdata)
{
    (void)s;
    resume_capture_t* cap = (resume_capture_t*)userdata;
    cap->call_count++;
    cap->outcome = outcome;
}

static struct keel_session* dummy_session_sentinel(void)
{
    static char sentinel;
    return (struct keel_session*)&sentinel;
}

/* ============================================================================
 * Tests
 * ============================================================================ */

/* §1: Token-bearing read under RYW+WAIT parks the session and returns
 *     WAIT_CATCHUP. Manager's waiters_active increments. */
TEST(consult_parks_session_on_wait_catchup) {
    fixture_t fx = make_fixture_ryw_wait();
    ASSERT_NE(fx.router, NULL);
    ASSERT_NE(fx.catchup, NULL);

    keel_catchup_stats_snapshot_t s0;
    keel_catchup_manager_snapshot(fx.catchup, &s0);
    ASSERT_EQ(s0.waiters_active, (size_t)0);
    ASSERT_EQ(s0.waiters_enqueued_total, (uint64_t)0);

    keel_route_session_t rs = make_token_session("0/16B3740", 7);
    resume_capture_t cap = {0};
    keel_route_decision_t decision;
    const keel_qt_query_t* qt = parse_read_qt("SELECT * FROM users WHERE id = 1");
    ASSERT_NE(qt, NULL);

    keel_flow_result_t fr = keel_engine_consult_catchup(
        fx.router, fx.catchup, dummy_session_sentinel(),
        &rs, qt, capture_cb, &cap, &decision);

    ASSERT_EQ(fr, KEEL_FLOW_WAIT_CATCHUP);
    ASSERT_EQ(decision.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);
    ASSERT_EQ(decision.wait_max_ms, (uint32_t)75);
    ASSERT_EQ(decision.wait_token.timeline_id, (uint32_t)7);
    ASSERT(strcmp(decision.wait_token.value, "0/16B3740") == 0);

    keel_catchup_stats_snapshot_t s1;
    keel_catchup_manager_snapshot(fx.catchup, &s1);
    ASSERT_EQ(s1.waiters_active, (size_t)1);
    ASSERT_EQ(s1.waiters_enqueued_total, (uint64_t)1);

    /* Resume cb must not have fired yet — waiter is parked. */
    ASSERT_EQ(cap.call_count, 0);

    destroy_fixture(&fx);
}

/* §2: Tokenless read does not park — the router never asks for WAIT.
 *     Bridge returns OK, no waiter enqueued. */
TEST(consult_no_token_returns_ok) {
    fixture_t fx = make_fixture_ryw_wait();
    ASSERT_NE(fx.router, NULL);

    keel_route_session_t rs;
    memset(&rs, 0, sizeof(rs));
    /* No token, no requires_consistent_read */

    resume_capture_t cap = {0};
    keel_flow_result_t fr = keel_engine_consult_catchup(
        fx.router, fx.catchup, dummy_session_sentinel(),
        &rs, NULL, capture_cb, &cap, NULL);

    ASSERT_EQ(fr, KEEL_FLOW_OK);

    keel_catchup_stats_snapshot_t s;
    keel_catchup_manager_snapshot(fx.catchup, &s);
    ASSERT_EQ(s.waiters_active, (size_t)0);

    destroy_fixture(&fx);
}

/* §3: NULL router → bridge is a no-op, returns OK. */
TEST(consult_null_router_is_noop) {
    fixture_t fx = make_fixture_ryw_wait();
    keel_route_session_t rs = make_token_session("0/16B3740", 7);
    resume_capture_t cap = {0};

    keel_flow_result_t fr = keel_engine_consult_catchup(
        /*router*/ NULL, fx.catchup, dummy_session_sentinel(),
        &rs, NULL, capture_cb, &cap, NULL);

    ASSERT_EQ(fr, KEEL_FLOW_OK);

    keel_catchup_stats_snapshot_t s;
    keel_catchup_manager_snapshot(fx.catchup, &s);
    ASSERT_EQ(s.waiters_active, (size_t)0);

    destroy_fixture(&fx);
}

/* §4: NULL catchup manager → bridge is a no-op even with a WAIT router. */
TEST(consult_null_catchup_is_noop) {
    fixture_t fx = make_fixture_ryw_wait();
    keel_route_session_t rs = make_token_session("0/16B3740", 7);
    resume_capture_t cap = {0};

    keel_flow_result_t fr = keel_engine_consult_catchup(
        fx.router, /*catchup*/ NULL, dummy_session_sentinel(),
        &rs, NULL, capture_cb, &cap, NULL);

    ASSERT_EQ(fr, KEEL_FLOW_OK);

    destroy_fixture(&fx);
}

/* §5: ROUTE_PRIMARY policy never emits WAIT_CATCHUP — bridge returns OK
 *     and out_decision reports CONSISTENCY_PRIMARY. */
TEST(consult_route_primary_policy_returns_ok) {
    keel_router_config_t cfg   = keel_router_config_default();
    cfg.consistency_mode       = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy      = KEEL_STALE_READ_ROUTE_PRIMARY;
    keel_router_t* r = keel_router_create(&cfg);
    ASSERT_NE(r, NULL);

    keel_route_server_t p = {
        .name = "primary", .host = "127.0.0.1", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 7,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t r1 = {
        .name = "replica1", .host = "127.0.0.1", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 7,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_router_add_server(r, &p);
    keel_router_add_server(r, &r1);

    keel_catchup_config_t ccfg = (keel_catchup_config_t)KEEL_CATCHUP_CONFIG_DEFAULT;
    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, &ccfg);

    keel_route_session_t rs = make_token_session("0/16B3740", 7);
    resume_capture_t cap = {0};
    keel_route_decision_t decision;

    keel_flow_result_t fr = keel_engine_consult_catchup(
        r, m, dummy_session_sentinel(), &rs, NULL,
        capture_cb, &cap, &decision);

    ASSERT_EQ(fr, KEEL_FLOW_OK);
    ASSERT_NE(decision.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);

    keel_catchup_stats_snapshot_t s;
    keel_catchup_manager_snapshot(m, &s);
    ASSERT_EQ(s.waiters_active, (size_t)0);

    keel_catchup_manager_destroy(m);
    keel_router_destroy(r);
}

/* §6: Two consecutive WAIT_CATCHUP consultations park two distinct
 *     waiters and the resume callbacks remain pending. */
TEST(consult_two_sessions_park_two_waiters) {
    fixture_t fx = make_fixture_ryw_wait();

    keel_route_session_t rs1 = make_token_session("0/16B3740", 7);
    keel_route_session_t rs2 = make_token_session("0/16B3800", 7);
    resume_capture_t cap1 = {0}, cap2 = {0};
    const keel_qt_query_t* qt = parse_read_qt("SELECT id FROM orders WHERE id = 42");
    ASSERT_NE(qt, NULL);

    keel_flow_result_t fr1 = keel_engine_consult_catchup(
        fx.router, fx.catchup, dummy_session_sentinel(),
        &rs1, qt, capture_cb, &cap1, NULL);
    keel_flow_result_t fr2 = keel_engine_consult_catchup(
        fx.router, fx.catchup, dummy_session_sentinel(),
        &rs2, qt, capture_cb, &cap2, NULL);

    ASSERT_EQ(fr1, KEEL_FLOW_WAIT_CATCHUP);
    ASSERT_EQ(fr2, KEEL_FLOW_WAIT_CATCHUP);

    keel_catchup_stats_snapshot_t s;
    keel_catchup_manager_snapshot(fx.catchup, &s);
    ASSERT_EQ(s.waiters_active, (size_t)2);
    ASSERT_EQ(s.waiters_enqueued_total, (uint64_t)2);

    ASSERT_EQ(cap1.call_count, 0);
    ASSERT_EQ(cap2.call_count, 0);

    destroy_fixture(&fx);
}

/* §7: Destroying the manager with parked waiters fires every resume_cb
 *     with KEEL_CATCHUP_CANCELLED. This is the documented behaviour and
 *     guarantees the bridge does not leak parked sessions on shutdown. */
TEST(destroy_cancels_parked_waiters) {
    fixture_t fx = make_fixture_ryw_wait();

    keel_route_session_t rs = make_token_session("0/16B3740", 7);
    resume_capture_t cap = {0};
    const keel_qt_query_t* qt = parse_read_qt("SELECT 1");
    ASSERT_NE(qt, NULL);

    keel_flow_result_t fr = keel_engine_consult_catchup(
        fx.router, fx.catchup, dummy_session_sentinel(),
        &rs, qt, capture_cb, &cap, NULL);
    ASSERT_EQ(fr, KEEL_FLOW_WAIT_CATCHUP);
    ASSERT_EQ(cap.call_count, 0);

    /* Tear down catchup manager — must fire resume_cb with CANCELLED. */
    keel_catchup_manager_destroy(fx.catchup);
    fx.catchup = NULL;

    ASSERT_EQ(cap.call_count, 1);
    ASSERT_EQ(cap.outcome, KEEL_CATCHUP_CANCELLED);

    destroy_fixture(&fx);
}

/* §8: NULL route_session is treated as a no-op (defensive — engine should
 *     never call us this way, but we don't want to crash on it). */
TEST(consult_null_route_session_is_noop) {
    fixture_t fx = make_fixture_ryw_wait();
    resume_capture_t cap = {0};

    keel_flow_result_t fr = keel_engine_consult_catchup(
        fx.router, fx.catchup, dummy_session_sentinel(),
        /*route_session*/ NULL, NULL, capture_cb, &cap, NULL);

    ASSERT_EQ(fr, KEEL_FLOW_OK);
    destroy_fixture(&fx);
}

int main(void)
{
    printf("Running engine catchup-bridge tests...\n");

    RUN_TEST(consult_parks_session_on_wait_catchup);
    RUN_TEST(consult_no_token_returns_ok);
    RUN_TEST(consult_null_router_is_noop);
    RUN_TEST(consult_null_catchup_is_noop);
    RUN_TEST(consult_route_primary_policy_returns_ok);
    RUN_TEST(consult_two_sessions_park_two_waiters);
    RUN_TEST(destroy_cancels_parked_waiters);
    RUN_TEST(consult_null_route_session_is_noop);

    if (g_qt_arena) keel_arena_destroy(g_qt_arena);

    printf("\n=== Results ===\n");
    printf("Passed: %d\n", tests_passed);
    printf("Failed: %d\n", tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
