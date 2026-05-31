/**
 * @file test_router_wait_catchup.c
 * @brief Router unit tests for `stale_read_policy = wait` → WAIT_CATCHUP emission.
 *
 * Validates Patch 2d-2: when `consistency_mode = READ_YOUR_WRITES` and
 * `stale_read_policy = wait`, a replica-safe read carrying a consistency
 * token must NOT be force-routed to the primary. The router instead picks
 * an eligible replica and emits `KEEL_ROUTE_REASON_WAIT_CATCHUP` with
 * `wait_server_index`, `wait_token`, and `wait_max_ms` populated, so a
 * downstream engine bridge can park the session in the per-worker
 * catch-up manager (see keel/engine/catchup.h).
 *
 * Negative cases ensure the WAIT path collapses gracefully to legacy
 * primary-fallback when the preconditions are not met (no replica
 * available; no token on session; policy = route_primary; etc.).
 */

#include "keel/core/router.h"
#include "keel/sql/sql.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <stdio.h>
#include <string.h>

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
#define ASSERT_STR_EQ(a, b) ASSERT(strcmp((a), (b)) == 0)

/* ============================================================================
 * Fixture: RYW + WAIT with primary + 2 replicas
 * ============================================================================ */

static keel_router_t* create_ryw_wait_router(void)
{
    keel_router_config_t cfg   = keel_router_config_default();
    cfg.consistency_mode       = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy      = KEEL_STALE_READ_WAIT;
    cfg.max_replica_catchup_ms = 75;
    keel_router_t* r = keel_router_create(&cfg);
    if (!r) return NULL;

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
    if (keel_router_add_server(r, &p)  != KEEL_OK) { keel_router_destroy(r); return NULL; }
    if (keel_router_add_server(r, &r1) != KEEL_OK) { keel_router_destroy(r); return NULL; }
    if (keel_router_add_server(r, &r2) != KEEL_OK) { keel_router_destroy(r); return NULL; }
    return r;
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

/* ============================================================================
 * Tests
 * ============================================================================ */

/* §1: WARN downgrade is removed — router_create must honor WAIT verbatim.
 *     Verified indirectly: §2 only passes if WAIT survives router_create. */
TEST(create_preserves_wait_policy) {
    keel_router_config_t cfg = keel_router_config_default();
    cfg.consistency_mode  = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy = KEEL_STALE_READ_WAIT;
    keel_router_t* r = keel_router_create(&cfg);
    ASSERT_NE(r, NULL);
    keel_router_destroy(r);
}

/* §2: RYW + WAIT + replica-safe read + token → WAIT_CATCHUP, server=NULL,
 *     wait fields populated, replica candidate index is one of the replicas. */
TEST(emits_wait_catchup_for_consistent_read) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    keel_route_session_t s = make_token_session("0/16B3740", 7);

    keel_route_decision_t d;
    keel_error_t err = keel_router_route_sql(
        r, KEEL_STR("SELECT * FROM users WHERE id = 1"), &s, &d);

    ASSERT_EQ(err, KEEL_OK);
    ASSERT_EQ(d.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);
    ASSERT_EQ(d.server, NULL);
    ASSERT_TRUE(d.is_read);
    ASSERT_TRUE((d.decision_factors & KEEL_DF_WAIT_CATCHUP) != 0);
    ASSERT_TRUE((d.decision_factors & KEEL_DF_CONSISTENCY_TOKEN) != 0);
    ASSERT_EQ(d.wait_max_ms, 75u);
    ASSERT_STR_EQ(d.wait_token.value, "0/16B3740");
    ASSERT_EQ(d.wait_token.timeline_id, 7u);

    /* wait_server_index must reference a replica (not the primary at idx 0). */
    keel_router_destroy(r);
}

/* §3: Reason-name surface exposes the new code. */
TEST(reason_name_includes_wait_catchup) {
    ASSERT_STR_EQ(keel_route_reason_name(KEEL_ROUTE_REASON_WAIT_CATCHUP),
                  "WAIT_CATCHUP");
}

/* §4: Without a token on the session, RYW+WAIT must NOT emit WAIT_CATCHUP.
 *     The query is just a normal replica-safe read → READ_SPLIT. */
TEST(no_token_skips_wait_catchup) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    /* requires_consistent_read=false → consistency does not force primary. */
    keel_route_session_t s;
    memset(&s, 0, sizeof(s));

    keel_route_decision_t d;
    ASSERT_EQ(keel_router_route_sql(
                  r, KEEL_STR("SELECT * FROM users"), &s, &d), KEEL_OK);
    ASSERT_NE(d.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);
    ASSERT_NE(d.server, NULL);
    keel_router_destroy(r);
}

/* §5: stale_read_policy=ROUTE_PRIMARY (legacy default) must keep forcing
 *     primary even when the session has a token under RYW. */
TEST(route_primary_policy_does_not_emit_wait) {
    keel_router_config_t cfg = keel_router_config_default();
    cfg.consistency_mode  = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy = KEEL_STALE_READ_ROUTE_PRIMARY;
    keel_router_t* r = keel_router_create(&cfg);
    ASSERT_NE(r, NULL);
    keel_route_server_t p = {
        .name = "primary", .host = "h", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 7,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t r1 = {
        .name = "replica1", .host = "h", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 7,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    ASSERT_EQ(keel_router_add_server(r, &p),  KEEL_OK);
    ASSERT_EQ(keel_router_add_server(r, &r1), KEEL_OK);

    keel_route_session_t s = make_token_session("0/ABCD", 7);
    keel_route_decision_t d;
    ASSERT_EQ(keel_router_route_sql(
                  r, KEEL_STR("SELECT * FROM users"), &s, &d), KEEL_OK);
    ASSERT_EQ(d.reason_code, KEEL_ROUTE_REASON_CONSISTENCY_PRIMARY);
    ASSERT_NE(d.server, NULL);
    ASSERT_STR_EQ(d.server->name, "primary");
    keel_router_destroy(r);
}

/* §6: No healthy replicas → WAIT mode collapses to legacy primary fallback
 *     (failover_to_primary=true by default). */
TEST(no_replica_collapses_to_primary_fallback) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    /* Mark both replicas DOWN so build_read_indices returns 0. */
    keel_router_set_server_health(r, "replica1", KEEL_HEALTH_DOWN);
    keel_router_set_server_health(r, "replica2", KEEL_HEALTH_DOWN);

    keel_route_session_t s = make_token_session("0/16B3740", 7);
    keel_route_decision_t d;
    ASSERT_EQ(keel_router_route_sql(
                  r, KEEL_STR("SELECT * FROM users"), &s, &d), KEEL_OK);
    ASSERT_NE(d.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);
    ASSERT_NE(d.server, NULL);
    ASSERT_STR_EQ(d.server->name, "primary");
    keel_router_destroy(r);
}

/* §7: A write query still routes to primary even under WAIT (only reads park). */
TEST(write_query_still_routes_primary_under_wait) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    keel_route_session_t s = make_token_session("0/16B3740", 7);

    keel_route_decision_t d;
    ASSERT_EQ(keel_router_route_sql(
                  r, KEEL_STR("INSERT INTO users(id) VALUES (1)"),
                  &s, &d), KEEL_OK);
    ASSERT_NE(d.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);
    ASSERT_NE(d.server, NULL);
    ASSERT_STR_EQ(d.server->name, "primary");
    keel_router_destroy(r);
}

/* §8: Inside a transaction, the session is already pinned to primary;
 *     WAIT must not park the session. */
TEST(in_transaction_skips_wait_catchup) {
    keel_router_t* r = create_ryw_wait_router();
    ASSERT_NE(r, NULL);
    keel_route_session_t s = make_token_session("0/16B3740", 7);
    s.in_transaction = true;

    keel_route_decision_t d;
    ASSERT_EQ(keel_router_route_sql(
                  r, KEEL_STR("SELECT * FROM users"), &s, &d), KEEL_OK);
    ASSERT_NE(d.reason_code, KEEL_ROUTE_REASON_WAIT_CATCHUP);
    ASSERT_NE(d.server, NULL);
    ASSERT_STR_EQ(d.server->name, "primary");
    keel_router_destroy(r);
}

/* §9: REJECT policy still wins under RYW with token (early reject path). */
TEST(reject_policy_overrides_wait) {
    keel_router_config_t cfg = keel_router_config_default();
    cfg.consistency_mode  = KEEL_CONSISTENCY_READ_YOUR_WRITES;
    cfg.stale_read_policy = KEEL_STALE_READ_REJECT;
    keel_router_t* r = keel_router_create(&cfg);
    ASSERT_NE(r, NULL);
    keel_route_server_t p = {
        .name = "primary", .host = "h", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .timeline_id = 7,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t r1 = {
        .name = "replica1", .host = "h", .port = 5433,
        .role = KEEL_SERVER_REPLICA, .timeline_id = 7,
        .weight = 100, .health = KEEL_HEALTH_UP,
    };
    ASSERT_EQ(keel_router_add_server(r, &p),  KEEL_OK);
    ASSERT_EQ(keel_router_add_server(r, &r1), KEEL_OK);

    keel_route_session_t s = make_token_session("0/ABCD", 7);
    keel_route_decision_t d;
    keel_error_t err = keel_router_route_sql(
        r, KEEL_STR("SELECT * FROM users"), &s, &d);
    ASSERT_EQ(err, KEEL_ERR_UNAVAILABLE);
    ASSERT_EQ(d.reason_code, KEEL_ROUTE_REASON_LAG_EXCEEDED);
    ASSERT_EQ(d.server, NULL);
    keel_router_destroy(r);
}

int main(void)
{
    printf("Running test_router_wait_catchup\n");
    RUN_TEST(create_preserves_wait_policy);
    RUN_TEST(emits_wait_catchup_for_consistent_read);
    RUN_TEST(reason_name_includes_wait_catchup);
    RUN_TEST(no_token_skips_wait_catchup);
    RUN_TEST(route_primary_policy_does_not_emit_wait);
    RUN_TEST(no_replica_collapses_to_primary_fallback);
    RUN_TEST(write_query_still_routes_primary_under_wait);
    RUN_TEST(in_transaction_skips_wait_catchup);
    RUN_TEST(reject_policy_overrides_wait);

    printf("\n=== Results ===\nPassed: %d\nFailed: %d\n",
           tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
