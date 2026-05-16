/**
 * @file test_router.c
 * @brief Tests for weighted read/write query routing
 *
 * Validates the core router that maps SQL intent (read vs write)
 * to a backend server (primary vs replica).  The router is the
 * critical decision point for read/write split: any bug here sends
 * writes to a read-only replica or starves replicas of read traffic.
 *
 * Test families:
 *   §1  — Default config: strategy, split flag, weight bounds.
 *   §2  — Server management: add primary + replicas, duplicate
 *          rejection, get-by-name, healthy-count queries.
 *   §3  — Write routing: all writes go to primary (SELECT … FOR
 *          UPDATE included), even when replicas are healthy.
 *   §4  — Read routing: reads spread across replicas; fall back
 *          to primary when replicas are down.
 *   §5  — Transaction routing: once inside BEGIN, all statements
 *          route to the same backend (primary).
 *   §6  — Weight distribution: after many iterations the observed
 *          distribution converges on the configured weights.
 *   §7  — Failover: when all replicas are marked DOWN, reads
 *          degrade to primary; when a single replica is down
 *          its share is redistributed.
 *   §8  — Session pinning: after explicit pin the router always
 *          returns the pinned server.
 *   §9  — Direct SQL routing via keel_router_route_sql().
 *   §10 — Statistics: query/byte counters increment correctly.
 *   §11 — Diagnostics: keel_router_dump produces output.
 *   §12 — Discovery: config default, create/destroy, on_failover
 *          callback, NULL safety, stop-when-not-running.
 *
 * Uses its own ASSERT/RUN_TEST macros rather than the shared
 * test_utils.h harness, because this file predates the unified
 * framework and the local macros include a pass/fail summary.
 */


#include "keel/core/router.h"
#include "keel/core/router_discovery.h"
#include "keel/sql/query_tree.h"
#include "keel/sql/sql.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <stdio.h>
#include <string.h>
#include <math.h>

/* ============================================================================
 * Test Helpers
 * ============================================================================ */

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) static void test_##name(void)
#define RUN_TEST(name) do { \
    printf("  %-50s ", #name); \
    fflush(stdout); \
    test_##name(); \
    printf("[PASS]\n"); \
    tests_passed++; \
} while(0)

#define ASSERT(cond) do { \
    if (!(cond)) { \
        printf("[FAIL]\n    Assertion failed: %s\n    at %s:%d\n", \
               #cond, __FILE__, __LINE__); \
        tests_failed++; \
        return; \
    } \
} while(0)

#define ASSERT_EQ(a, b) ASSERT((a) == (b))
#define ASSERT_NE(a, b) ASSERT((a) != (b))
#define ASSERT_TRUE(x) ASSERT(x)
#define ASSERT_FALSE(x) ASSERT(!(x))
#define ASSERT_STR_EQ(a, b) ASSERT(strcmp((a), (b)) == 0)

/* ============================================================================
 * Router Creation Tests
 * ============================================================================ */

TEST(router_create_default) {
    keel_router_config_t config = keel_router_config_default();
    
    ASSERT_EQ(config.strategy, KEEL_ROUTE_STRATEGY_WEIGHTED_ROUND_ROBIN);
    ASSERT_TRUE(config.read_write_split);
    ASSERT_TRUE(config.primary_read_weight > 0.0);
    ASSERT_TRUE(config.primary_read_weight <= 1.0);
    
    keel_router_t* router = keel_router_create(&config);
    ASSERT_NE(router, NULL);
    
    keel_router_destroy(router);
}

TEST(router_add_servers) {
    keel_router_t* router = keel_router_create(NULL);
    ASSERT_NE(router, NULL);
    
    /* Add primary */
    keel_route_server_t primary = {
        .name = "primary",
        .host = "db-primary.internal",
        .port = 5432,
        .role = KEEL_SERVER_PRIMARY,
        .weight = 100
    };
    ASSERT_EQ(keel_router_add_server(router, &primary), KEEL_OK);
    
    /* Add replicas */
    keel_route_server_t replica1 = {
        .name = "replica1",
        .host = "db-replica1.internal",
        .port = 5432,
        .role = KEEL_SERVER_REPLICA,
        .weight = 100
    };
    ASSERT_EQ(keel_router_add_server(router, &replica1), KEEL_OK);
    
    keel_route_server_t replica2 = {
        .name = "replica2",
        .host = "db-replica2.internal",
        .port = 5432,
        .role = KEEL_SERVER_REPLICA,
        .weight = 100
    };
    ASSERT_EQ(keel_router_add_server(router, &replica2), KEEL_OK);
    
    /* Verify servers are accessible */
    keel_route_server_t* srv = keel_router_get_server(router, "primary");
    ASSERT_NE(srv, NULL);
    ASSERT_STR_EQ(srv->name, "primary");
    ASSERT_EQ(srv->role, KEEL_SERVER_PRIMARY);
    
    srv = keel_router_get_server(router, "replica1");
    ASSERT_NE(srv, NULL);
    ASSERT_EQ(srv->role, KEEL_SERVER_REPLICA);
    
    /* Check counts */
    ASSERT_EQ(keel_router_count_healthy(router, KEEL_SERVER_PRIMARY), 1);
    ASSERT_EQ(keel_router_count_healthy(router, KEEL_SERVER_REPLICA), 2);
    
    keel_router_destroy(router);
}

TEST(router_duplicate_server_rejected) {
    keel_router_t* router = keel_router_create(NULL);
    ASSERT_NE(router, NULL);
    
    keel_route_server_t srv = {
        .name = "myserver",
        .host = "localhost",
        .port = 5432,
        .role = KEEL_SERVER_PRIMARY,
        .weight = 100
    };
    
    ASSERT_EQ(keel_router_add_server(router, &srv), KEEL_OK);
    ASSERT_EQ(keel_router_add_server(router, &srv), KEEL_ERR_ALREADY_EXISTS);
    
    keel_router_destroy(router);
}

/* ============================================================================
 * Routing Decision Tests
 * ============================================================================ */

/**
 * Helper to create a router with 1 primary + 2 replicas
 */
static keel_router_t* create_test_router(void) {
    keel_router_config_t config = keel_router_config_default();
    config.primary_read_weight = 0.5;
    
    keel_router_t* router = keel_router_create(&config);
    if (!router) return NULL;
    
    keel_route_server_t primary = {
        .name = "primary",
        .host = "db-primary",
        .port = 5432,
        .role = KEEL_SERVER_PRIMARY,
        .weight = 100,
        .health = KEEL_HEALTH_UP
    };
    keel_router_add_server(router, &primary);
    keel_router_set_server_health(router, "primary", KEEL_HEALTH_UP);
    
    keel_route_server_t replica1 = {
        .name = "replica1",
        .host = "db-replica1",
        .port = 5432,
        .role = KEEL_SERVER_REPLICA,
        .weight = 100,
        .health = KEEL_HEALTH_UP
    };
    keel_router_add_server(router, &replica1);
    keel_router_set_server_health(router, "replica1", KEEL_HEALTH_UP);
    
    keel_route_server_t replica2 = {
        .name = "replica2",
        .host = "db-replica2",
        .port = 5432,
        .role = KEEL_SERVER_REPLICA,
        .weight = 100,
        .health = KEEL_HEALTH_UP
    };
    keel_router_add_server(router, &replica2);
    keel_router_set_server_health(router, "replica2", KEEL_HEALTH_UP);
    
    return router;
}

TEST(route_write_to_primary) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* INSERT should go to primary */
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("INSERT INTO users (name) VALUES ('alice')"), arena);
    ASSERT_NE(qt, NULL);
    
    keel_route_decision_t decision;
    ASSERT_EQ(keel_router_route(router, qt, NULL, &decision), KEEL_OK);
    ASSERT_NE(decision.server, NULL);
    ASSERT_STR_EQ(decision.server->name, "primary");
    ASSERT_FALSE(decision.is_read);
    
    /* UPDATE should go to primary */
    qt = keel_sql_analyze_full(
        KEEL_STR("UPDATE users SET name = 'bob' WHERE id = 1"), arena);
    ASSERT_EQ(keel_router_route(router, qt, NULL, &decision), KEEL_OK);
    ASSERT_STR_EQ(decision.server->name, "primary");
    
    /* DELETE should go to primary */
    qt = keel_sql_analyze_full(
        KEEL_STR("DELETE FROM users WHERE id = 1"), arena);
    ASSERT_EQ(keel_router_route(router, qt, NULL, &decision), KEEL_OK);
    ASSERT_STR_EQ(decision.server->name, "primary");
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

TEST(route_read_to_replicas_or_primary) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* SELECT should be routed to replicas OR primary */
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users WHERE id = 1"), arena);
    ASSERT_NE(qt, NULL);
    ASSERT_TRUE(keel_qt_can_use_replica(qt));
    
    keel_route_decision_t decision;
    ASSERT_EQ(keel_router_route(router, qt, NULL, &decision), KEEL_OK);
    ASSERT_NE(decision.server, NULL);
    ASSERT_TRUE(decision.is_read);
    
    /* Server should be primary, replica1, or replica2 */
    bool valid = strcmp(decision.server->name, "primary") == 0 ||
                 strcmp(decision.server->name, "replica1") == 0 ||
                 strcmp(decision.server->name, "replica2") == 0;
    ASSERT_TRUE(valid);
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

TEST(route_select_for_update_to_primary) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* SELECT FOR UPDATE must go to primary */
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users WHERE id = 1 FOR UPDATE"), arena);
    ASSERT_NE(qt, NULL);
    ASSERT_FALSE(keel_qt_can_use_replica(qt));
    
    keel_route_decision_t decision;
    ASSERT_EQ(keel_router_route(router, qt, NULL, &decision), KEEL_OK);
    ASSERT_STR_EQ(decision.server->name, "primary");
    ASSERT_FALSE(decision.is_read);
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

TEST(route_transaction_to_primary) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* BEGIN should go to primary */
    keel_qt_query_t* qt = keel_sql_analyze_full(KEEL_STR("BEGIN"), arena);
    ASSERT_NE(qt, NULL);
    
    keel_route_decision_t decision;
    ASSERT_EQ(keel_router_route(router, qt, NULL, &decision), KEEL_OK);
    ASSERT_STR_EQ(decision.server->name, "primary");
    
    /* Queries inside transaction go to primary even if they're reads */
    keel_route_session_t session = { .in_transaction = true };
    
    qt = keel_sql_analyze_full(KEEL_STR("SELECT * FROM users"), arena);
    ASSERT_EQ(keel_router_route(router, qt, &session, &decision), KEEL_OK);
    ASSERT_STR_EQ(decision.server->name, "primary");
    ASSERT_FALSE(decision.is_read);  /* Not treated as read when in txn */
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

/* ============================================================================
 * Weight Distribution Tests
 * ============================================================================ */

TEST(weight_distribution) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* Parse a simple SELECT */
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users"), arena);
    ASSERT_NE(qt, NULL);
    
    /* Route many queries and count distribution */
    int primary_count = 0;
    int replica1_count = 0;
    int replica2_count = 0;
    const int total_routes = 1000;
    
    for (int i = 0; i < total_routes; i++) {
        keel_route_decision_t decision;
        ASSERT_EQ(keel_router_route(router, qt, NULL, &decision), KEEL_OK);
        
        if (strcmp(decision.server->name, "primary") == 0) {
            primary_count++;
        } else if (strcmp(decision.server->name, "replica1") == 0) {
            replica1_count++;
        } else if (strcmp(decision.server->name, "replica2") == 0) {
            replica2_count++;
        }
    }
    
    /* With primary_read_weight = 0.5:
     * - Primary effective weight: 100 * 0.5 = 50
     * - Replica1 effective weight: 100
     * - Replica2 effective weight: 100
     * - Total: 250
     * 
     * Expected distribution:
     * - Primary: 50/250 = 20% = 200 routes
     * - Replica1: 100/250 = 40% = 400 routes
     * - Replica2: 100/250 = 40% = 400 routes
     */
    
    printf("\n    Distribution over %d routes:\n", total_routes);
    printf("      Primary:  %d (%.1f%%, expected ~20%%)\n", 
           primary_count, 100.0 * primary_count / total_routes);
    printf("      Replica1: %d (%.1f%%, expected ~40%%)\n",
           replica1_count, 100.0 * replica1_count / total_routes);
    printf("      Replica2: %d (%.1f%%, expected ~40%%)\n",
           replica2_count, 100.0 * replica2_count / total_routes);
    
    /* Allow 5% tolerance */
    double primary_pct = 100.0 * primary_count / total_routes;
    double replica1_pct = 100.0 * replica1_count / total_routes;
    double replica2_pct = 100.0 * replica2_count / total_routes;
    
    ASSERT_TRUE(fabs(primary_pct - 20.0) < 5.0);
    ASSERT_TRUE(fabs(replica1_pct - 40.0) < 5.0);
    ASSERT_TRUE(fabs(replica2_pct - 40.0) < 5.0);
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

TEST(write_distribution_all_primary) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* Route many write queries */
    const int total_routes = 100;
    int primary_count = 0;
    
    for (int i = 0; i < total_routes; i++) {
        keel_qt_query_t* qt = keel_sql_analyze_full(
            KEEL_STR("INSERT INTO logs (msg) VALUES ('test')"), arena);
        
        keel_route_decision_t decision;
        ASSERT_EQ(keel_router_route(router, qt, NULL, &decision), KEEL_OK);
        
        if (strcmp(decision.server->name, "primary") == 0) {
            primary_count++;
        }
        
        /* Reset arena for next parse */
        keel_arena_reset(arena);
    }
    
    /* All writes should go to primary */
    ASSERT_EQ(primary_count, total_routes);
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

/* ============================================================================
 * Failover Tests
 * ============================================================================ */

TEST(failover_to_primary_when_replicas_down) {
    /* Create router with primary NOT in read pool (weight=0) to test failover */
    keel_router_config_t config = keel_router_config_default();
    config.primary_read_weight = 0.0;  /* Primary not used for reads normally */
    config.failover_to_primary = true;
    
    keel_router_t* router = keel_router_create(&config);
    ASSERT_NE(router, NULL);
    
    keel_route_server_t primary = {
        .name = "primary",
        .host = "db-primary",
        .port = 5432,
        .role = KEEL_SERVER_PRIMARY,
        .weight = 100,
        .health = KEEL_HEALTH_UP
    };
    keel_router_add_server(router, &primary);
    keel_router_set_server_health(router, "primary", KEEL_HEALTH_UP);
    
    keel_route_server_t replica1 = {
        .name = "replica1",
        .host = "db-replica1",
        .port = 5432,
        .role = KEEL_SERVER_REPLICA,
        .weight = 100,
        .health = KEEL_HEALTH_UP
    };
    keel_router_add_server(router, &replica1);
    keel_router_set_server_health(router, "replica1", KEEL_HEALTH_UP);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* Mark replica as down */
    keel_router_set_server_health(router, "replica1", KEEL_HEALTH_DOWN);
    
    /* Read queries should failover to primary */
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users"), arena);
    
    keel_route_decision_t decision;
    ASSERT_EQ(keel_router_route(router, qt, NULL, &decision), KEEL_OK);
    ASSERT_STR_EQ(decision.server->name, "primary");
    
    keel_router_stats_t stats;
    keel_router_get_stats(router, &stats);
    ASSERT_TRUE(stats.failover_routes > 0);
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

TEST(skip_down_replica) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* Mark replica1 as down */
    keel_router_set_server_health(router, "replica1", KEEL_HEALTH_DOWN);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users"), arena);
    
    /* Route many queries - none should go to replica1 */
    for (int i = 0; i < 100; i++) {
        keel_route_decision_t decision;
        ASSERT_EQ(keel_router_route(router, qt, NULL, &decision), KEEL_OK);
        ASSERT_NE(decision.server, NULL);
        ASSERT_TRUE(strcmp(decision.server->name, "replica1") != 0);
    }
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

/* ============================================================================
 * Session Pinning Tests
 * ============================================================================ */

TEST(session_pinning) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* Get a reference to replica1 */
    keel_route_server_t* replica1 = keel_router_get_server(router, "replica1");
    ASSERT_NE(replica1, NULL);
    
    /* Pin session to replica1 */
    keel_route_session_t session = {
        .pinned_server = replica1
    };
    
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users"), arena);
    
    /* All queries should go to pinned server */
    for (int i = 0; i < 10; i++) {
        keel_route_decision_t decision;
        ASSERT_EQ(keel_router_route(router, qt, &session, &decision), KEEL_OK);
        ASSERT_STR_EQ(decision.server->name, "replica1");
        ASSERT_TRUE(decision.was_pinned);
    }
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

/* ============================================================================
 * SQL-based Routing Tests
 * ============================================================================ */

TEST(route_sql_directly) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    /* Route using raw SQL */
    keel_route_decision_t decision;
    
    /* Read query */
    ASSERT_EQ(keel_router_route_sql(router, 
                                    KEEL_STR("SELECT * FROM users"),
                                    NULL, &decision), KEEL_OK);
    ASSERT_TRUE(decision.is_read);
    
    /* Write query */
    ASSERT_EQ(keel_router_route_sql(router,
                                    KEEL_STR("UPDATE users SET name = 'x'"),
                                    NULL, &decision), KEEL_OK);
    ASSERT_FALSE(decision.is_read);
    ASSERT_STR_EQ(decision.server->name, "primary");
    
    keel_router_destroy(router);
}

/* ============================================================================
 * Statistics Tests
 * ============================================================================ */

TEST(router_statistics) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* Route some reads and writes */
    keel_route_decision_t decision;
    
    for (int i = 0; i < 10; i++) {
        keel_qt_query_t* qt = keel_sql_analyze_full(
            KEEL_STR("SELECT * FROM users"), arena);
        keel_router_route(router, qt, NULL, &decision);
        keel_arena_reset(arena);
    }
    
    for (int i = 0; i < 5; i++) {
        keel_qt_query_t* qt = keel_sql_analyze_full(
            KEEL_STR("INSERT INTO users (name) VALUES ('x')"), arena);
        keel_router_route(router, qt, NULL, &decision);
        keel_arena_reset(arena);
    }
    
    /* Check stats */
    keel_router_stats_t stats;
    keel_router_get_stats(router, &stats);
    
    ASSERT_EQ(stats.total_routes, 15);
    ASSERT_EQ(stats.read_routes, 10);
    ASSERT_EQ(stats.write_routes, 5);
    
    /* Reset and verify */
    keel_router_reset_stats(router);
    keel_router_get_stats(router, &stats);
    ASSERT_EQ(stats.total_routes, 0);
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

TEST(router_can_use_replica) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* Read query - can use replica */
    keel_qt_query_t* qt_read = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users"), arena);
    ASSERT_TRUE(keel_router_can_use_replica(router, qt_read, NULL));
    
    /* Write query - cannot use replica */
    keel_qt_query_t* qt_write = keel_sql_analyze_full(
        KEEL_STR("INSERT INTO users VALUES (1)"), arena);
    ASSERT_FALSE(keel_router_can_use_replica(router, qt_write, NULL));
    
    /* SELECT FOR UPDATE - cannot use replica */
    keel_qt_query_t* qt_lock = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users FOR UPDATE"), arena);
    ASSERT_FALSE(keel_router_can_use_replica(router, qt_lock, NULL));
    
    /* NULL router */
    ASSERT_FALSE(keel_router_can_use_replica(NULL, qt_read, NULL));
    
    keel_arena_destroy(arena);
    keel_router_destroy(router);
}

TEST(router_dump) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    /* Dump to /dev/null - just ensure it doesn't crash */
    FILE* devnull = fopen("/dev/null", "w");
    if (devnull) {
        keel_router_dump(router, devnull);
        fclose(devnull);
    }
    
    /* NULL inputs should not crash */
    keel_router_dump(NULL, stdout);
    keel_router_dump(router, NULL);
    
    keel_router_destroy(router);
}

/* ============================================================================
 * Discovery Tests
 * ============================================================================ */

TEST(discovery_config_default) {
    keel_discovery_config_t config = keel_discovery_config_default();
    
    /* Verify defaults are sensible */
    ASSERT_EQ(config.method, KEEL_DISCOVER_SQL);
    ASSERT_TRUE(config.probe_timeout > 0);
    ASSERT_TRUE(config.probe_retries > 0);
    ASSERT_TRUE(config.probe_interval > 0);
    ASSERT_TRUE(config.max_lag_bytes > 0);
    ASSERT_TRUE(config.max_lag_seconds > 0);
}

TEST(discovery_create_destroy) {
    keel_discovery_config_t config = keel_discovery_config_default();
    
    /* Create with default config */
    keel_discovery_t* disc = keel_discovery_create(&config);
    ASSERT_NE(disc, NULL);
    
    /* Verify not running initially */
    ASSERT_FALSE(keel_discovery_is_running(disc));
    
    /* Destroy */
    keel_discovery_destroy(disc);
    
    /* NULL destroy should not crash */
    keel_discovery_destroy(NULL);
}

TEST(discovery_create_null_config) {
    /* Create with NULL config should still work using defaults */
    keel_discovery_t* disc = keel_discovery_create(NULL);
    ASSERT_NE(disc, NULL);
    
    ASSERT_FALSE(keel_discovery_is_running(disc));
    
    keel_discovery_destroy(disc);
}

TEST(discovery_on_failover) {
    keel_discovery_config_t config = keel_discovery_config_default();
    keel_discovery_t* disc = keel_discovery_create(&config);
    ASSERT_NE(disc, NULL);
    
    /* Set failover callback (doesn't actually trigger, just tests the API) */
    int user_data = 42;
    keel_discovery_on_failover(disc, NULL, &user_data);
    
    /* NULL discovery should not crash */
    keel_discovery_on_failover(NULL, NULL, NULL);
    
    keel_discovery_destroy(disc);
}

TEST(discovery_stop_when_not_running) {
    keel_discovery_config_t config = keel_discovery_config_default();
    keel_discovery_t* disc = keel_discovery_create(&config);
    ASSERT_NE(disc, NULL);
    
    /* Stop when not running should not crash */
    keel_discovery_stop(disc);
    ASSERT_FALSE(keel_discovery_is_running(disc));
    
    /* NULL stop should not crash */
    keel_discovery_stop(NULL);
    
    keel_discovery_destroy(disc);
}

TEST(discovery_is_running_null) {
    /* NULL discovery should return false */
    ASSERT_FALSE(keel_discovery_is_running(NULL));
}

TEST(topology_free_null) {
    /* Free NULL topology should not crash */
    keel_topology_free(NULL);
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;
    
    printf("\n=== Router Tests ===\n\n");
    
    printf("Creation tests:\n");
    RUN_TEST(router_create_default);
    RUN_TEST(router_add_servers);
    RUN_TEST(router_duplicate_server_rejected);
    
    printf("\nRouting decision tests:\n");
    RUN_TEST(route_write_to_primary);
    RUN_TEST(route_read_to_replicas_or_primary);
    RUN_TEST(route_select_for_update_to_primary);
    RUN_TEST(route_transaction_to_primary);
    
    printf("\nWeight distribution tests:\n");
    RUN_TEST(weight_distribution);
    RUN_TEST(write_distribution_all_primary);
    
    printf("\nFailover tests:\n");
    RUN_TEST(failover_to_primary_when_replicas_down);
    RUN_TEST(skip_down_replica);
    
    printf("\nSession tests:\n");
    RUN_TEST(session_pinning);
    
    printf("\nSQL routing tests:\n");
    RUN_TEST(route_sql_directly);
    
    printf("\nStatistics tests:\n");
    RUN_TEST(router_statistics);
    
    printf("\nAPI tests:\n");
    RUN_TEST(router_can_use_replica);
    RUN_TEST(router_dump);
    
    printf("\nDiscovery tests:\n");
    RUN_TEST(discovery_config_default);
    RUN_TEST(discovery_create_destroy);
    RUN_TEST(discovery_create_null_config);
    RUN_TEST(discovery_on_failover);
    RUN_TEST(discovery_stop_when_not_running);
    RUN_TEST(discovery_is_running_null);
    RUN_TEST(topology_free_null);
    
    printf("\n=== Results ===\n");
    printf("Passed: %d\n", tests_passed);
    printf("Failed: %d\n", tests_failed);
    
    return tests_failed > 0 ? 1 : 0;
}
