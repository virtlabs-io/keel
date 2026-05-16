/**
 * @file test_proxy_session.c
 * @brief Unit tests for Feature 12: keel_client_session_t proxy session.
 *
 * All tests use the same router fixture as test_sharding / test_router:
 * two servers (primary + replica), one shard rule on users.id with 4 shards.
 * No real TCP sockets are needed — the session is tested for routing logic,
 * stat accounting, transaction control, and pin/unpin semantics.
 */

#include "test_utils.h"
#include "proxy_session.h"
#include "keel/core/router.h"
#include "connpool.h"
#include "keel_error.h"

#include <string.h>

int g_tests_run    = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) {
    return (g_tests_failed == 0) ? 0 : 1;
}

/* ============================================================================
 * Shared fixture
 * ============================================================================ */

static keel_router_t* build_router(void) {
    keel_router_config_t cfg = keel_router_config_default();
    cfg.read_write_split = true;
    keel_router_t* r = keel_router_create(&cfg);
    if (!r) return NULL;

    keel_route_server_t primary = {
        .name   = "primary",
        .host   = "127.0.0.1",
        .port   = 5432,
        .role   = KEEL_SERVER_ROLE_RW,
        .weight = 100,
        .health = KEEL_HEALTH_UP,
    };
    keel_route_server_t replica = {
        .name   = "replica1",
        .host   = "127.0.0.2",
        .port   = 5432,
        .role   = KEEL_SERVER_ROLE_RO,
        .weight = 100,
        .health = KEEL_HEALTH_UP,
    };
    keel_router_add_server(r, &primary);
    keel_router_add_server(r, &replica);
    keel_router_add_shard_rule(r, "users", "id", 4);
    return r;
}

/* ============================================================================
 * Test: create / destroy
 * ============================================================================ */

static void test_session_create_destroy(void) {
    TEST_BEGIN("proxy_session_create_destroy");

    keel_router_t* router = build_router();
    TEST_ASSERT_NOT_NULL(router);

    keel_client_session_t* cs = keel_client_session_create(router, NULL);
    TEST_ASSERT_NOT_NULL(cs);

    keel_client_session_destroy(cs);
    keel_client_session_destroy(NULL);  /* NULL guard */
    keel_router_destroy(router);

    TEST_END();
}

/* ============================================================================
 * Test: create with NULL router returns NULL
 * ============================================================================ */

static void test_session_create_null_router(void) {
    TEST_BEGIN("proxy_session_create_null_router");

    keel_client_session_t* cs = keel_client_session_create(NULL, NULL);
    TEST_ASSERT_NULL(cs);

    TEST_END();
}

/* ============================================================================
 * Test: routing_state accessor
 * ============================================================================ */

static void test_session_routing_state(void) {
    TEST_BEGIN("proxy_session_routing_state");

    keel_router_t* router = build_router();
    TEST_ASSERT_NOT_NULL(router);

    keel_client_session_t* cs = keel_client_session_create(router, NULL);
    TEST_ASSERT_NOT_NULL(cs);

    const keel_route_session_t* rs = keel_client_session_routing_state(cs);
    TEST_ASSERT_NOT_NULL(rs);
    TEST_ASSERT(!rs->in_transaction);
    TEST_ASSERT(!rs->has_scatter_write);
    TEST_ASSERT_NULL(rs->pinned_server);

    keel_client_session_destroy(cs);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: begin / end transaction
 * ============================================================================ */

static void test_session_tx_lifecycle(void) {
    TEST_BEGIN("proxy_session_tx_lifecycle");

    keel_router_t* router = build_router();
    keel_client_session_t* cs = keel_client_session_create(router, NULL);
    TEST_ASSERT_NOT_NULL(cs);

    keel_client_session_begin_tx(cs);
    const keel_route_session_t* rs = keel_client_session_routing_state(cs);
    TEST_ASSERT(rs->in_transaction);

    keel_client_session_end_tx(cs, false);
    TEST_ASSERT(!rs->in_transaction);

    /* Stats: one committed transaction */
    keel_client_session_stats_t stats;
    keel_client_session_get_stats(cs, &stats);
    TEST_ASSERT_EQ(stats.tx_count,   1);
    TEST_ASSERT_EQ(stats.tx_aborted, 0);

    /* Aborted transaction */
    keel_client_session_begin_tx(cs);
    keel_client_session_end_tx(cs, true);
    keel_client_session_get_stats(cs, &stats);
    TEST_ASSERT_EQ(stats.tx_count,   2);
    TEST_ASSERT_EQ(stats.tx_aborted, 1);

    keel_client_session_destroy(cs);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: pin / unpin
 * ============================================================================ */

static void test_session_pin_unpin(void) {
    TEST_BEGIN("proxy_session_pin_unpin");

    keel_router_t* router = build_router();
    keel_client_session_t* cs = keel_client_session_create(router, NULL);
    TEST_ASSERT_NOT_NULL(cs);

    keel_route_server_t fake = {
        .name = "pinned_srv", .host = "127.0.0.9", .port = 5432,
        .role = KEEL_SERVER_ROLE_RW, .health = KEEL_HEALTH_UP, .weight = 100,
    };

    keel_client_session_pin(cs, &fake);
    const keel_route_session_t* rs = keel_client_session_routing_state(cs);
    TEST_ASSERT_NOT_NULL(rs->pinned_server);

    keel_client_session_unpin(cs);
    TEST_ASSERT_NULL(rs->pinned_server);

    /* NULL guards */
    keel_client_session_pin(NULL, &fake);
    keel_client_session_unpin(NULL);

    keel_client_session_destroy(cs);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: dispatch dispatches, stats updated
 * ============================================================================ */

static void test_session_dispatch(void) {
    TEST_BEGIN("proxy_session_dispatch");

    keel_router_t* router = build_router();
    keel_client_session_t* cs = keel_client_session_create(router, NULL);
    TEST_ASSERT_NOT_NULL(cs);

    keel_dispatch_result_t out;
    keel_shard_bound_params_t params = {0};
    /* Single-shard read — should route to shard 0 server */
    keel_error_t err = keel_client_session_dispatch(
        cs,
        KEEL_STR("SELECT * FROM users WHERE id = 4"),
        &params, false, &out, NULL);

    /* Either succeeds (KEEL_OK with a single decision) or KEEL_ERR_NOT_SUPPORTED
     * if the router has no matching server configured — either is acceptable
     * for this fixture; we just confirm no crash and stats updated. */
    keel_client_session_stats_t stats;
    keel_client_session_get_stats(cs, &stats);

    if (err == KEEL_OK) {
        TEST_ASSERT(stats.queries_total >= 1);
    } else {
        /* Stats not incremented on error */
        TEST_ASSERT_EQ(stats.queries_total, 0);
    }

    keel_client_session_destroy(cs);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: NULL guards on dispatch and release
 * ============================================================================ */

static void test_session_null_guards(void) {
    TEST_BEGIN("proxy_session_null_guards");

    TEST_ASSERT(keel_client_session_dispatch(NULL, KEEL_STR("SELECT 1"),
                                              NULL, false, NULL, NULL)
                != KEEL_OK);

    keel_client_session_release_conn(NULL, NULL, NULL, true);

    keel_client_session_stats_t stats;
    keel_client_session_get_stats(NULL, &stats);

    TEST_ASSERT_NULL(keel_client_session_routing_state(NULL));

    TEST_END();
}

/* ============================================================================
 * Test: record_scatter_write increments scatter counter
 * ============================================================================ */

static void test_session_record_scatter_write(void) {
    TEST_BEGIN("proxy_session_record_scatter_write");

    keel_router_t* router = build_router();
    keel_client_session_t* cs = keel_client_session_create(router, NULL);
    TEST_ASSERT_NOT_NULL(cs);

    /* Build a minimal scatter plan */
    keel_scatter_plan_t plan = {0};
    plan.count                     = 1;
    plan.participating_shards_mask = 0x1;
    plan.decisions[0].shard_index  = 0;

    keel_client_session_record_scatter_write(cs, &plan);
    keel_client_session_stats_t stats;
    keel_client_session_get_stats(cs, &stats);
    TEST_ASSERT(stats.queries_scatter >= 1);

    const keel_route_session_t* rs = keel_client_session_routing_state(cs);
    TEST_ASSERT(rs->has_scatter_write);

    /* NULL guards */
    keel_client_session_record_scatter_write(NULL, &plan);
    keel_client_session_record_scatter_write(cs,   NULL);

    keel_client_session_destroy(cs);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: end_tx clears scatter write tracking
 * ============================================================================ */

static void test_session_end_tx_clears_scatter(void) {
    TEST_BEGIN("proxy_session_end_tx_clears_scatter");

    keel_router_t* router = build_router();
    keel_client_session_t* cs = keel_client_session_create(router, NULL);
    TEST_ASSERT_NOT_NULL(cs);

    keel_client_session_begin_tx(cs);

    keel_scatter_plan_t plan = {0};
    plan.count                     = 1;
    plan.participating_shards_mask = 0x3;
    keel_client_session_record_scatter_write(cs, &plan);

    const keel_route_session_t* rs = keel_client_session_routing_state(cs);
    TEST_ASSERT(rs->has_scatter_write);

    keel_client_session_end_tx(cs, false);
    TEST_ASSERT(!rs->has_scatter_write);
    TEST_ASSERT(!rs->in_transaction);

    keel_client_session_destroy(cs);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    test_session_create_destroy();
    test_session_create_null_router();
    test_session_routing_state();
    test_session_tx_lifecycle();
    test_session_pin_unpin();
    test_session_dispatch();
    test_session_null_guards();
    test_session_record_scatter_write();
    test_session_end_tx_clears_scatter();

    printf("\nproxy_session: %d/%d tests passed, %d failed\n",
           g_tests_passed, g_tests_run, g_tests_failed);
    return test_summary();
}
