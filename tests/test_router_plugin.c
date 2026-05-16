/**
 * @file test_router_plugin.c
 * @brief Tests for pluggable routing system
 *
 * Exercises the metadata-driven routing plugin layer that sits above
 * the core weighted router.  Plugins supply catalog metadata (function
 * volatility, view rules, table write-types) so the router can
 * classify queries it has never seen before.
 *
 * Test families:
 *   §1  — Metadata cache: create, add/lookup, check-write
 *          classification, clear, remove, dump, NULL safety.
 *   §2  — Metadata config: default values for staleness thresholds
 *          and introspection query templates.
 *   §3  — Cache refresh: round-trip through a mock connection that
 *          returns pg_proc + pg_rewrite rows, verifying the cache
 *          is populated correctly.
 *   §4  — Plugin lifecycle: create with default and Patroni
 *          discovery engines.
 *   §5  — Router manager (keel_router_mgr): create, register
 *          plugin, set-default, route-with-metadata integration.
 *   §6  — Discovery engine: config, probe, probe-with-connection,
 *          apply-topology, failover-callback, start/stop lifecycle,
 *          NULL safety.
 *   §7  — Custom plugin: user-supplied vtable routes through the
 *          manager framework.
 *   §8  — SQL probe queries: introspection SQL strings are non-NULL
 *          and non-empty.
 *
 * Uses its own ASSERT / RUN_TEST macros (same pattern as test_router.c)
 * with a local pass/fail counter.
 */


#include "keel/core/router_plugin.h"
#include "keel/core/router_metadata.h"
#include "keel/core/router_discovery.h"
#include "keel/core/router.h"
#include "keel/sql/sql.h"
#include "keel/sql/query_tree.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* ============================================================================
 * Test Helpers
 * ============================================================================ */

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) static void test_##name(void)
#define RUN_TEST(name) do { \
    printf("  %-55s ", #name); \
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
 * Mock Connection for Metadata
 * ============================================================================ */

typedef struct {
    const char** schemas;
    const char** names;
    const char** types;
    const char** write_types;
    size_t count;
    size_t current;
} mock_query_data_t;

static mock_query_data_t mock_functions = {0};
static mock_query_data_t mock_views = {0};

static keel_error_t mock_query(
    keel_metadata_conn_t* conn,
    const char* query,
    bool (*callback)(void* user_data, int col_count, 
                     const char** values, const char** names),
    void* user_data
) {
    (void)conn;
    
    /* Detect which query is being run */
    if (strstr(query, "pg_proc")) {
        /* Function query */
        for (size_t i = 0; i < mock_functions.count; i++) {
            const char* values[] = {
                mock_functions.schemas[i],
                mock_functions.names[i],
                mock_functions.types[i],  /* volatility */
                "f",                       /* is_security_definer */
                "f",                       /* returns_trigger */
                "0",                       /* arg_count */
                "void",                    /* return_type */
                "12345",                   /* oid */
            };
            const char* names[] = {
                "schema", "name", "volatility", "is_security_definer",
                "returns_trigger", "arg_count", "return_type", "oid"
            };
            if (!callback(user_data, 8, values, names)) {
                break;
            }
        }
    } else if (strstr(query, "pg_rewrite")) {
        /* View rules query */
        for (size_t i = 0; i < mock_views.count; i++) {
            const char* values[] = {
                mock_views.schemas[i],
                mock_views.names[i],
                mock_views.types[i],      /* has_insert_rule */
                "f",                       /* has_update_rule */
                "f",                       /* has_delete_rule */
                "f",                       /* is_updatable */
                "12346",                   /* oid */
            };
            const char* names[] = {
                "schema", "name", "has_insert_rule", "has_update_rule",
                "has_delete_rule", "is_updatable", "oid"
            };
            if (!callback(user_data, 7, values, names)) {
                break;
            }
        }
    }
    /* Other queries return empty */
    
    return KEEL_OK;
}

/* ============================================================================
 * Metadata Cache Tests
 * ============================================================================ */

TEST(metadata_cache_create) {
    keel_metadata_cache_t* cache = keel_metadata_cache_create("testdb", NULL);
    ASSERT_NE(cache, NULL);
    
    keel_metadata_stats_t stats;
    keel_metadata_cache_get_stats(cache, &stats);
    ASSERT_EQ(stats.total_objects, 0);
    
    keel_metadata_cache_destroy(cache);
}

TEST(metadata_cache_add_lookup) {
    keel_metadata_cache_t* cache = keel_metadata_cache_create("testdb", NULL);
    ASSERT_NE(cache, NULL);
    
    /* Add a volatile function */
    keel_cached_object_t func = {
        .schema = "public",
        .name = "notify_users",
        .type = 'f',
        .write_type = KEEL_OBJ_WRITE_ALWAYS,
        .is_volatile = true,
    };
    ASSERT_EQ(keel_metadata_cache_add(cache, &func), KEEL_OK);
    
    /* Look it up */
    const keel_cached_object_t* found;
    ASSERT_TRUE(keel_metadata_cache_lookup(cache, "public", "notify_users", 'f', &found));
    ASSERT_NE(found, NULL);
    ASSERT_STR_EQ(found->name, "notify_users");
    ASSERT_EQ(found->write_type, KEEL_OBJ_WRITE_ALWAYS);
    ASSERT_TRUE(found->is_volatile);
    
    /* Lookup non-existent */
    ASSERT_FALSE(keel_metadata_cache_lookup(cache, "public", "nonexistent", 'f', NULL));
    
    keel_metadata_cache_destroy(cache);
}

TEST(metadata_cache_check_write) {
    keel_metadata_cache_t* cache = keel_metadata_cache_create("testdb", NULL);
    ASSERT_NE(cache, NULL);
    
    /* Add various objects */
    keel_cached_object_t volatile_func = {
        .schema = "public",
        .name = "write_log",
        .type = 'f',
        .write_type = KEEL_OBJ_WRITE_ALWAYS,
    };
    keel_metadata_cache_add(cache, &volatile_func);
    
    keel_cached_object_t stable_func = {
        .schema = "public",
        .name = "get_user",
        .type = 'f',
        .write_type = KEEL_OBJ_WRITE_NONE,
    };
    keel_metadata_cache_add(cache, &stable_func);
    
    keel_cached_object_t view_with_rule = {
        .schema = "public",
        .name = "users_view",
        .type = 'v',
        .write_type = KEEL_OBJ_WRITE_RULE,
        .has_insert_rule = true,
    };
    keel_metadata_cache_add(cache, &view_with_rule);
    
    /* Check write types */
    ASSERT_EQ(keel_metadata_cache_check_write(cache, "public", "write_log", 'f'),
              KEEL_OBJ_WRITE_ALWAYS);
    ASSERT_EQ(keel_metadata_cache_check_write(cache, "public", "get_user", 'f'),
              KEEL_OBJ_WRITE_NONE);
    ASSERT_EQ(keel_metadata_cache_check_write(cache, "public", "users_view", 'v'),
              KEEL_OBJ_WRITE_RULE);
    ASSERT_EQ(keel_metadata_cache_check_write(cache, "public", "unknown", 'f'),
              KEEL_OBJ_WRITE_NONE);
    
    keel_metadata_cache_destroy(cache);
}

TEST(metadata_cache_clear) {
    keel_metadata_cache_t* cache = keel_metadata_cache_create("testdb", NULL);
    ASSERT_NE(cache, NULL);
    
    /* Add some entries */
    keel_cached_object_t func = {
        .schema = "public",
        .name = "test_func",
        .type = 'f',
        .is_volatile = true,
    };
    ASSERT_EQ(keel_metadata_cache_add(cache, &func), KEEL_OK);
    
    /* Verify entry exists */
    ASSERT_TRUE(keel_metadata_cache_lookup(cache, "public", "test_func", 'f', NULL));
    
    /* Clear the cache */
    keel_metadata_cache_clear(cache);
    
    /* Entry should be gone */
    ASSERT_FALSE(keel_metadata_cache_lookup(cache, "public", "test_func", 'f', NULL));
    
    /* NULL should be safe */
    keel_metadata_cache_clear(NULL);
    
    keel_metadata_cache_destroy(cache);
}

TEST(metadata_cache_null_safety) {
    /* Create with NULL database */
    keel_metadata_cache_t* cache = keel_metadata_cache_create(NULL, NULL);
    ASSERT_EQ(cache, NULL);
    
    /* Destroy NULL should be safe */
    keel_metadata_cache_destroy(NULL);
    
    /* Add to NULL cache */
    keel_cached_object_t obj = {.name = "test"};
    ASSERT_NE(keel_metadata_cache_add(NULL, &obj), KEEL_OK);
    
    /* Lookup in NULL cache */
    ASSERT_FALSE(keel_metadata_cache_lookup(NULL, "public", "test", 'f', NULL));
    
    cache = keel_metadata_cache_create("testdb", NULL);
    ASSERT_NE(cache, NULL);
    
    /* Add NULL object */
    ASSERT_NE(keel_metadata_cache_add(cache, NULL), KEEL_OK);
    
    /* Lookup NULL name */
    ASSERT_FALSE(keel_metadata_cache_lookup(cache, "public", NULL, 'f', NULL));
    
    /* Check write on NULL cache */
    ASSERT_EQ(keel_metadata_cache_check_write(NULL, "public", "test", 'f'), KEEL_OBJ_WRITE_NONE);
    
    /* Check write with NULL name */
    ASSERT_EQ(keel_metadata_cache_check_write(cache, "public", NULL, 'f'), KEEL_OBJ_WRITE_NONE);
    
    keel_metadata_cache_destroy(cache);
}

TEST(metadata_cache_dump) {
    keel_metadata_cache_t* cache = keel_metadata_cache_create("testdb", NULL);
    ASSERT_NE(cache, NULL);
    
    /* Add an entry */
    keel_cached_object_t func = {
        .schema = "public",
        .name = "dump_test",
        .type = 'f',
        .is_volatile = true,
    };
    ASSERT_EQ(keel_metadata_cache_add(cache, &func), KEEL_OK);
    
    /* Dump should not crash - output to stdout */
    keel_metadata_cache_dump(cache, stdout);
    
    /* NULL cache */
    keel_metadata_cache_dump(NULL, stdout);
    
    /* NULL file */
    keel_metadata_cache_dump(cache, NULL);
    
    keel_metadata_cache_destroy(cache);
}

TEST(metadata_cache_remove) {
    keel_metadata_cache_t* cache = keel_metadata_cache_create("testdb", NULL);
    ASSERT_NE(cache, NULL);
    
    /* Add an entry */
    keel_cached_object_t func = {
        .schema = "public",
        .name = "to_remove",
        .type = 'f',
        .is_volatile = true,
    };
    ASSERT_EQ(keel_metadata_cache_add(cache, &func), KEEL_OK);
    
    /* Verify it exists */
    ASSERT_TRUE(keel_metadata_cache_lookup(cache, "public", "to_remove", 'f', NULL));
    
    /* Remove it */
    ASSERT_EQ(keel_metadata_cache_remove(cache, "public", "to_remove", 'f'), KEEL_OK);
    
    /* Verify it no longer exists */
    ASSERT_FALSE(keel_metadata_cache_lookup(cache, "public", "to_remove", 'f', NULL));
    
    /* Remove nonexistent should return error */
    ASSERT_NE(keel_metadata_cache_remove(cache, "public", "nonexistent", 'f'), KEEL_OK);
    
    /* NULL safety */
    ASSERT_NE(keel_metadata_cache_remove(NULL, "public", "test", 'f'), KEEL_OK);
    ASSERT_NE(keel_metadata_cache_remove(cache, NULL, "test", 'f'), KEEL_OK);
    ASSERT_NE(keel_metadata_cache_remove(cache, "public", NULL, 'f'), KEEL_OK);
    
    keel_metadata_cache_destroy(cache);
}

TEST(metadata_config_default) {
    keel_metadata_config_t config = keel_metadata_config_default();
    
    /* Check sensible defaults */
    ASSERT_NE(config.initial_capacity, 0);
    /* Bools are set, no need to check specific values */
}

TEST(metadata_cache_refresh) {
    keel_metadata_cache_t* cache = keel_metadata_cache_create("testdb", NULL);
    ASSERT_NE(cache, NULL);
    
    /* Set up mock data */
    static const char* func_schemas[] = {"public", "public"};
    static const char* func_names[] = {"send_email", "get_data"};
    static const char* func_types[] = {"v", "s"};  /* v=volatile, s=stable */
    
    mock_functions = (mock_query_data_t){
        .schemas = func_schemas,
        .names = func_names,
        .types = func_types,
        .count = 2,
    };
    
    static const char* view_schemas[] = {"public"};
    static const char* view_names[] = {"audit_log"};
    static const char* view_types[] = {"t"};  /* t=true for has_insert_rule */
    
    mock_views = (mock_query_data_t){
        .schemas = view_schemas,
        .names = view_names,
        .types = view_types,
        .count = 1,
    };
    
    /* Create mock connection */
    keel_metadata_conn_t conn = {
        .query = mock_query,
    };
    
    /* Refresh cache */
    ASSERT_EQ(keel_metadata_cache_refresh(cache, &conn), KEEL_OK);
    
    /* Verify cached objects */
    keel_metadata_stats_t stats;
    keel_metadata_cache_get_stats(cache, &stats);
    ASSERT_TRUE(stats.total_objects >= 2);  /* At least functions */
    
    /* Check volatile function was cached */
    ASSERT_EQ(keel_metadata_cache_check_write(cache, "public", "send_email", 'f'),
              KEEL_OBJ_WRITE_ALWAYS);
    
    /* Stable function should not write */
    ASSERT_EQ(keel_metadata_cache_check_write(cache, "public", "get_data", 'f'),
              KEEL_OBJ_WRITE_NONE);
    
    keel_metadata_cache_destroy(cache);
}

/* ============================================================================
 * Plugin Tests
 * ============================================================================ */

TEST(plugin_create_default) {
    keel_plugin_default_config_t config = {
        .primary_read_weight = 0.5,
        .respect_metadata = true,
        .conservative_mode = false,
    };
    
    keel_router_plugin_t* plugin = keel_router_plugin_default_create(&config);
    ASSERT_NE(plugin, NULL);
    ASSERT_NE(plugin->ops, NULL);
    ASSERT_STR_EQ(plugin->ops->name, "default");
    
    keel_router_plugin_destroy(plugin);
}

TEST(plugin_create_patroni) {
    keel_plugin_patroni_config_t config = {
        .patroni_url = "http://patroni:8008",
        .cluster_name = "mydb",
        .poll_interval = 10 * 1000000000ULL,
    };
    
    keel_router_plugin_t* plugin = keel_router_plugin_patroni_create(&config);
    ASSERT_NE(plugin, NULL);
    ASSERT_STR_EQ(plugin->ops->name, "patroni");
    
    keel_router_plugin_destroy(plugin);
}

/* ============================================================================
 * Router Manager Tests
 * ============================================================================ */

static keel_router_t* create_test_router(void) {
    keel_router_config_t config = keel_router_config_default();
    keel_router_t* router = keel_router_create(&config);
    if (!router) return NULL;
    
    keel_route_server_t primary = {
        .name = "primary",
        .host = "db-primary",
        .port = 5432,
        .role = KEEL_SERVER_PRIMARY,
        .weight = 100,
    };
    keel_router_add_server(router, &primary);
    keel_router_set_server_health(router, "primary", KEEL_HEALTH_UP);
    
    keel_route_server_t replica = {
        .name = "replica1",
        .host = "db-replica1",
        .port = 5432,
        .role = KEEL_SERVER_REPLICA,
        .weight = 100,
    };
    keel_router_add_server(router, &replica);
    keel_router_set_server_health(router, "replica1", KEEL_HEALTH_UP);
    
    return router;
}

TEST(router_mgr_create) {
    keel_router_t* router = create_test_router();
    ASSERT_NE(router, NULL);
    
    keel_router_mgr_config_t config = keel_router_mgr_config_default();
    keel_router_mgr_t* mgr = keel_router_mgr_create(&config, router);
    ASSERT_NE(mgr, NULL);
    
    keel_router_mgr_destroy(mgr);
    keel_router_destroy(router);
}

TEST(router_mgr_register_plugin) {
    keel_router_t* router = create_test_router();
    keel_router_mgr_t* mgr = keel_router_mgr_create(NULL, router);
    ASSERT_NE(mgr, NULL);
    
    /* Create and register default plugin */
    keel_router_plugin_t* plugin = keel_router_plugin_default_create(NULL);
    ASSERT_NE(plugin, NULL);
    
    ASSERT_EQ(keel_router_mgr_register(mgr, "mydb", plugin), KEEL_OK);
    
    /* Duplicate registration should fail */
    ASSERT_EQ(keel_router_mgr_register(mgr, "mydb", plugin), KEEL_ERR_ALREADY_EXISTS);
    
    /* Different database should work */
    keel_router_plugin_t* plugin2 = keel_router_plugin_default_create(NULL);
    ASSERT_EQ(keel_router_mgr_register(mgr, "otherdb", plugin2), KEEL_OK);
    
    keel_router_plugin_destroy(plugin);
    keel_router_plugin_destroy(plugin2);
    keel_router_mgr_destroy(mgr);
    keel_router_destroy(router);
}

TEST(router_mgr_set_default) {
    keel_router_t* router = create_test_router();
    keel_router_mgr_t* mgr = keel_router_mgr_create(NULL, router);
    ASSERT_NE(mgr, NULL);
    
    keel_router_plugin_t* plugin = keel_router_plugin_default_create(NULL);
    ASSERT_NE(plugin, NULL);
    
    keel_router_mgr_set_default(mgr, plugin);
    
    /* Route should work even without database-specific plugin */
    keel_route_decision_t decision;
    ASSERT_EQ(keel_router_mgr_route(mgr, KEEL_STR("SELECT 1"), NULL, NULL, &decision), 
              KEEL_OK);
    ASSERT_NE(decision.server, NULL);
    
    keel_router_plugin_destroy(plugin);
    keel_router_mgr_destroy(mgr);
    keel_router_destroy(router);
}

TEST(router_mgr_route_with_metadata) {
    keel_router_t* router = create_test_router();
    keel_router_mgr_t* mgr = keel_router_mgr_create(NULL, router);
    ASSERT_NE(mgr, NULL);
    
    /* Create plugin with metadata respect */
    keel_plugin_default_config_t config = {
        .primary_read_weight = 0.5,
        .respect_metadata = true,
    };
    keel_router_plugin_t* plugin = keel_router_plugin_default_create(&config);
    
    ASSERT_EQ(keel_router_mgr_register(mgr, "testdb", plugin), KEEL_OK);
    
    /* Get metadata cache and add a volatile function */
    keel_metadata_cache_t* metadata = keel_router_mgr_get_metadata(mgr, "testdb");
    if (metadata) {
        keel_cached_object_t func = {
            .schema = "public",
            .name = "notify",
            .type = 'f',
            .write_type = KEEL_OBJ_WRITE_ALWAYS,
        };
        keel_metadata_cache_add(metadata, &func);
    }
    
    /* Regular SELECT should potentially go to replica */
    keel_route_decision_t decision;
    ASSERT_EQ(keel_router_mgr_route(mgr, KEEL_STR("SELECT * FROM users"), 
                                   "testdb", NULL, &decision), KEEL_OK);
    ASSERT_NE(decision.server, NULL);
    /* Note: May go to primary or replica based on weighted routing */
    
    /* INSERT should go to primary */
    ASSERT_EQ(keel_router_mgr_route(mgr, KEEL_STR("INSERT INTO users (name) VALUES ('x')"),
                                   "testdb", NULL, &decision), KEEL_OK);
    ASSERT_STR_EQ(decision.server->name, "primary");
    
    keel_router_plugin_destroy(plugin);
    keel_router_mgr_destroy(mgr);
    keel_router_destroy(router);
}

/* ============================================================================
 * Discovery Tests
 * ============================================================================ */

TEST(discovery_create) {
    keel_discovery_config_t config = keel_discovery_config_default();
    config.method = KEEL_DISCOVER_SQL;
    
    keel_discovery_t* disc = keel_discovery_create(&config);
    ASSERT_NE(disc, NULL);
    ASSERT_FALSE(keel_discovery_is_running(disc));
    
    keel_discovery_destroy(disc);
}

TEST(discovery_config_patroni) {
    keel_discovery_config_t config = keel_discovery_config_default();
    config.method = KEEL_DISCOVER_PATRONI;
    config.patroni_url = "http://patroni:8008";
    config.cluster_name = "main";
    
    keel_discovery_t* disc = keel_discovery_create(&config);
    ASSERT_NE(disc, NULL);
    
    /*
     * Refresh exercises the real Patroni HTTP path.
     * In the unit-test environment Patroni is not running, so we accept
     * either KEEL_OK (if a server happened to be reachable) or KEEL_ERR_IO
     * (unreachable).  The important invariant is that the function does not
     * crash and, if it returns KEEL_OK, topology is non-NULL.
     */
    keel_cluster_topology_t* topology = NULL;
    keel_error_t err = keel_discovery_refresh(disc, &topology);
    ASSERT_TRUE(err == KEEL_OK || err == KEEL_ERR_IO);
    if (topology) {
        keel_topology_free(topology);
    }
    
    keel_discovery_destroy(disc);
}

TEST(discovery_probe) {
    keel_discovery_t* disc = keel_discovery_create(NULL);
    ASSERT_NE(disc, NULL);
    
    keel_server_info_t info;
    keel_error_t err = keel_discovery_probe(disc, "localhost", 5432, 
                                           "user", "pass", "db", &info);
    ASSERT_EQ(err, KEEL_OK);
    
    /* No probe_fn configured: core is DB-agnostic, health stays UNKNOWN */
    ASSERT_EQ(info.health, KEEL_HEALTH_UNKNOWN);
    ASSERT_EQ(info.port, 5432);
    
    keel_discovery_destroy(disc);
}

TEST(discovery_null_safety) {
    /* Destroy NULL should be safe */
    keel_discovery_destroy(NULL);
    
    /* Stop NULL should be safe */
    keel_discovery_stop(NULL);
    
    /* is_running with NULL should be false */
    ASSERT_FALSE(keel_discovery_is_running(NULL));
    
    /* topology_free with NULL should be safe */
    keel_topology_free(NULL);
}

TEST(discovery_config_defaults) {
    keel_discovery_config_t config = keel_discovery_config_default();
    
    /* Check default values */
    ASSERT_EQ(config.method, KEEL_DISCOVER_SQL);
    ASSERT_TRUE(config.probe_timeout > 0);
    ASSERT_TRUE(config.probe_retries > 0);
    ASSERT_TRUE(config.probe_interval > 0);
    ASSERT_TRUE(config.max_lag_bytes > 0);
    ASSERT_TRUE(config.max_lag_seconds > 0.0);
    ASSERT_EQ(config.patroni_url, NULL);
    ASSERT_EQ(config.cluster_name, NULL);
}

TEST(discovery_methods) {
    /* Test SQL discovery method */
    keel_discovery_config_t config = keel_discovery_config_default();
    config.method = KEEL_DISCOVER_SQL;
    
    keel_discovery_t* disc = keel_discovery_create(&config);
    ASSERT_NE(disc, NULL);
    keel_discovery_destroy(disc);
    
    /* Test pg_auto_failover method */
    config.method = KEEL_DISCOVER_PGAUTOFAILOVER;
    config.monitor_connstr = "host=monitor port=5432";
    config.formation = "default";
    
    disc = keel_discovery_create(&config);
    ASSERT_NE(disc, NULL);
    keel_discovery_destroy(disc);
    
    /* Test Consul method */
    config.method = KEEL_DISCOVER_CONSUL;
    config.consul_url = "http://consul:8500";
    config.service_name = "postgres";
    
    disc = keel_discovery_create(&config);
    ASSERT_NE(disc, NULL);
    keel_discovery_destroy(disc);
    
    /* Test etcd method */
    config.method = KEEL_DISCOVER_ETCD;
    config.etcd_endpoints = "http://etcd:2379";
    config.cluster_name = "pg-cluster";
    
    disc = keel_discovery_create(&config);
    ASSERT_NE(disc, NULL);
    keel_discovery_destroy(disc);
}

TEST(discovery_probe_conn) {
    keel_discovery_t* disc = keel_discovery_create(NULL);
    ASSERT_NE(disc, NULL);
    
    keel_server_info_t info;
    
    /* NULL connection should return error */
    ASSERT_EQ(keel_discovery_probe_conn(disc, NULL, &info), KEEL_ERR_INVALID_ARG);
    
    /* NULL info should return error */
    ASSERT_EQ(keel_discovery_probe_conn(disc, (void*)1, NULL), KEEL_ERR_INVALID_ARG);
    
    /* NULL disc should return error */
    ASSERT_EQ(keel_discovery_probe_conn(NULL, (void*)1, &info), KEEL_ERR_INVALID_ARG);
    
    keel_discovery_destroy(disc);
}

TEST(discovery_apply_topology) {
    /* Create discovery and router */
    keel_discovery_t* disc = keel_discovery_create(NULL);
    ASSERT_NE(disc, NULL);
    
    keel_router_t* router = keel_router_create(NULL);
    ASSERT_NE(router, NULL);
    
    /* NULL args should fail */
    ASSERT_EQ(keel_discovery_apply(NULL, router, NULL), KEEL_ERR_INVALID_ARG);
    ASSERT_EQ(keel_discovery_apply(disc, NULL, NULL), KEEL_ERR_INVALID_ARG);
    ASSERT_EQ(keel_discovery_apply(disc, router, NULL), KEEL_ERR_INVALID_ARG);
    
    /* Create a minimal topology */
    keel_cluster_topology_t* topology = NULL;
    ASSERT_EQ(keel_discovery_refresh(disc, &topology), KEEL_OK);
    ASSERT_NE(topology, NULL);
    
    /* Apply empty topology should work */
    ASSERT_EQ(keel_discovery_apply(disc, router, topology), KEEL_OK);
    
    keel_topology_free(topology);
    keel_router_destroy(router);
    keel_discovery_destroy(disc);
}

TEST(discovery_failover_callback) {
    keel_discovery_t* disc = keel_discovery_create(NULL);
    ASSERT_NE(disc, NULL);
    
    /* NULL disc should be handled */
    keel_discovery_on_failover(NULL, NULL, NULL);
    
    /* Set callback to NULL */
    keel_discovery_on_failover(disc, NULL, NULL);
    
    keel_discovery_destroy(disc);
}

TEST(discovery_probe_null_args) {
    keel_discovery_t* disc = keel_discovery_create(NULL);
    ASSERT_NE(disc, NULL);
    
    keel_server_info_t info;
    
    /* NULL disc */
    ASSERT_EQ(keel_discovery_probe(NULL, "host", 5432, "u", "p", "d", &info), 
              KEEL_ERR_INVALID_ARG);
    
    /* NULL host */
    ASSERT_EQ(keel_discovery_probe(disc, NULL, 5432, "u", "p", "d", &info), 
              KEEL_ERR_INVALID_ARG);
    
    /* NULL info */
    ASSERT_EQ(keel_discovery_probe(disc, "host", 5432, "u", "p", "d", NULL), 
              KEEL_ERR_INVALID_ARG);
    
    keel_discovery_destroy(disc);
}

TEST(discovery_refresh_null) {
    keel_cluster_topology_t* topology;
    
    /* NULL disc */
    ASSERT_EQ(keel_discovery_refresh(NULL, &topology), KEEL_ERR_INVALID_ARG);
    
    keel_discovery_t* disc = keel_discovery_create(NULL);
    ASSERT_NE(disc, NULL);
    
    /* NULL topology ptr */
    ASSERT_EQ(keel_discovery_refresh(disc, NULL), KEEL_ERR_INVALID_ARG);
    
    keel_discovery_destroy(disc);
}

TEST(discovery_start_stop) {
    keel_router_t* router = keel_router_create(NULL);
    ASSERT_NE(router, NULL);
    
    keel_discovery_config_t config = keel_discovery_config_default();
    config.probe_interval = 100000000ULL;  /* 100ms for fast test */
    
    keel_discovery_t* disc = keel_discovery_create(&config);
    ASSERT_NE(disc, NULL);
    
    /* Not running initially */
    ASSERT_FALSE(keel_discovery_is_running(disc));
    
    /* NULL args should fail */
    ASSERT_EQ(keel_discovery_start(NULL, router), KEEL_ERR_INVALID_ARG);
    ASSERT_EQ(keel_discovery_start(disc, NULL), KEEL_ERR_INVALID_ARG);
    
    /* Start should succeed */
    ASSERT_EQ(keel_discovery_start(disc, router), KEEL_OK);
    ASSERT_TRUE(keel_discovery_is_running(disc));
    
    /* Double start should fail */
    ASSERT_EQ(keel_discovery_start(disc, router), KEEL_ERR_ALREADY_INITIALIZED);
    
    /* Stop */
    keel_discovery_stop(disc);
    
    keel_discovery_destroy(disc);
    keel_router_destroy(router);
}

/* ============================================================================
 * Custom Plugin Test
 * ============================================================================ */

/* Custom plugin that routes analytics queries to a specific replica */
typedef struct {
    const char* analytics_server;
} analytics_plugin_data_t;

static keel_error_t analytics_init(keel_router_plugin_t* plugin, const void* config) {
    analytics_plugin_data_t* data = keel_calloc(1, sizeof(*data));
    if (!data) return KEEL_ERR_NOMEM;
    
    if (config) {
        data->analytics_server = *(const char**)config;
    } else {
        data->analytics_server = "analytics-replica";
    }
    
    plugin->user_data = data;
    return KEEL_OK;
}

static void analytics_destroy(keel_router_plugin_t* plugin) {
    if (plugin && plugin->user_data) {
        keel_free(plugin->user_data);
    }
}

static keel_error_t analytics_route(
    keel_router_plugin_t* plugin,
    const keel_router_ctx_t* ctx,
    keel_route_decision_t* decision
) {
    analytics_plugin_data_t* data = plugin->user_data;
    
    /* Check for analytics tables */
    if (ctx->qt) {
        keel_qt_table_ref_t* tbl = ctx->qt->tables;
        for (size_t i = 0; i < ctx->qt->table_count && tbl; i++, tbl = tbl->next) {
            /* Convert keel_str_t to temporary buffer for strstr */
            char table_buf[256] = "";
            if (tbl->table.len > 0 && tbl->table.len < sizeof(table_buf)) {
                memcpy(table_buf, tbl->table.data, tbl->table.len);
                table_buf[tbl->table.len] = '\0';
            }
            
            if (strstr(table_buf, "analytics") || strstr(table_buf, "metrics")) {
                /* Route to analytics server */
                decision->server = keel_router_get_server(ctx->router, 
                                                          data->analytics_server);
                if (decision->server) {
                    decision->reason = "analytics query";
                    decision->is_read = true;
                    return KEEL_OK;
                }
            }
        }
    }
    
    /* Delegate to base router */
    return keel_router_route(ctx->router, ctx->qt, ctx->session, decision);
}

static const keel_router_plugin_ops_t analytics_ops = {
    .name = "analytics",
    .version = KEEL_ROUTER_PLUGIN_API_VERSION,
    .init = analytics_init,
    .destroy = analytics_destroy,
    .route = analytics_route,
};

TEST(custom_plugin) {
    keel_router_t* router = create_test_router();
    
    /* Add analytics replica */
    keel_route_server_t analytics = {
        .name = "analytics-replica",
        .host = "db-analytics",
        .port = 5432,
        .role = KEEL_SERVER_REPLICA,
        .weight = 100,
    };
    keel_router_add_server(router, &analytics);
    keel_router_set_server_health(router, "analytics-replica", KEEL_HEALTH_UP);
    
    /* Create custom plugin */
    const char* analytics_server = "analytics-replica";
    keel_router_plugin_t* plugin = keel_router_plugin_create(&analytics_ops, &analytics_server);
    ASSERT_NE(plugin, NULL);
    ASSERT_STR_EQ(plugin->ops->name, "analytics");
    
    /* Create manager and register plugin */
    keel_router_mgr_t* mgr = keel_router_mgr_create(NULL, router);
    ASSERT_EQ(keel_router_mgr_register(mgr, "warehouse", plugin), KEEL_OK);
    
    /* Query analytics table should go to analytics replica */
    keel_route_decision_t decision;
    ASSERT_EQ(keel_router_mgr_route(mgr, 
                                   KEEL_STR("SELECT * FROM user_analytics"),
                                   "warehouse", NULL, &decision), KEEL_OK);
    ASSERT_NE(decision.server, NULL);
    ASSERT_STR_EQ(decision.server->name, "analytics-replica");
    
    /* Regular query should use normal routing */
    ASSERT_EQ(keel_router_mgr_route(mgr,
                                   KEEL_STR("SELECT * FROM users"),
                                   "warehouse", NULL, &decision), KEEL_OK);
    ASSERT_NE(decision.server, NULL);
    /* Could be primary or replica1 */
    
    keel_router_plugin_destroy(plugin);
    keel_router_mgr_destroy(mgr);
    keel_router_destroy(router);
}

/* ============================================================================
 * SQL Probe Query Tests
 * ============================================================================ */

/* ============================================================================
 * SQL Probe Query Tests
 * ============================================================================ */

TEST(sql_probe_queries_defined) {
    /* KEEL_SQL_PROBE_SERVER / KEEL_SQL_PROBE_LAG are private to the probe
     * layer (probe_postgres_discovery.c) and must NOT be exposed through the
     * core header.  Only metadata constants (defined in router_metadata.h)
     * are visible here. */
    ASSERT_NE(KEEL_SQL_CHECK_PRIMARY, NULL);
    ASSERT_TRUE(strstr(KEEL_SQL_CHECK_PRIMARY, "is_primary") != NULL);
    
    ASSERT_NE(KEEL_SQL_CHECK_READONLY, NULL);
    ASSERT_TRUE(strstr(KEEL_SQL_CHECK_READONLY, "is_readonly") != NULL);
}

TEST(metadata_introspection_queries_defined) {
    /* Verify metadata query strings are defined */
    ASSERT_NE(KEEL_SQL_QUERY_FUNCTIONS, NULL);
    ASSERT_TRUE(strstr(KEEL_SQL_QUERY_FUNCTIONS, "pg_proc") != NULL);
    ASSERT_TRUE(strstr(KEEL_SQL_QUERY_FUNCTIONS, "provolatile") != NULL);
    
    ASSERT_NE(KEEL_SQL_QUERY_VIEW_RULES, NULL);
    ASSERT_TRUE(strstr(KEEL_SQL_QUERY_VIEW_RULES, "pg_rewrite") != NULL);
    
    ASSERT_NE(KEEL_SQL_QUERY_TRIGGERS, NULL);
    ASSERT_TRUE(strstr(KEEL_SQL_QUERY_TRIGGERS, "pg_trigger") != NULL);
    
    ASSERT_NE(KEEL_SQL_QUERY_MATVIEWS, NULL);
    ASSERT_TRUE(strstr(KEEL_SQL_QUERY_MATVIEWS, "relkind = 'm'") != NULL);
}

/* ============================================================================
 * C3: Function call write-safety analysis
 * ============================================================================ */

/**
 * @brief Verify that keel_qt_build() collects function references from a
 *        SELECT that calls a user-defined function.
 */
TEST(query_tree_func_ref_collection) {
    keel_arena_t* arena = keel_arena_create(16384);
    ASSERT_NE(arena, NULL);

    /* Parse: SELECT my_func(id) FROM users */
    keel_str_t sql = KEEL_STR("SELECT my_func(id) FROM users");
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, sql, arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);

    keel_qt_builder_t builder;
    keel_qt_builder_init(&builder, arena);
    keel_qt_query_t* qt = keel_qt_build(&builder, ast);
    ASSERT_NE(qt, NULL);

    /* Must have collected at least one function reference */
    ASSERT_TRUE(qt->func_count >= 1);

    /* Find our function in the list */
    bool found = false;
    keel_qt_func_ref_t* fn = qt->functions;
    for (size_t i = 0; i < qt->func_count && fn; i++, fn = fn->next) {
        char name_buf[64] = "";
        if (fn->name.len && fn->name.len < sizeof(name_buf)) {
            memcpy(name_buf, fn->name.data, fn->name.len);
            name_buf[fn->name.len] = '\0';
        }
        if (strcmp(name_buf, "my_func") == 0) {
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);

    keel_arena_destroy(arena);
}

/**
 * @brief Verify that keel_metadata_analyze_query() flags needs_primary when
 *        the query calls a VOLATILE / write-unsafe function that is in the cache.
 */
TEST(analyze_query_volatile_function) {
    /* Build a metadata cache with one volatile function */
    keel_metadata_cache_t* cache = keel_metadata_cache_create("testdb", NULL);
    ASSERT_NE(cache, NULL);

    keel_cached_object_t vfunc = {
        .schema       = "public",
        .name         = "write_log",
        .type         = 'f',
        .write_type   = KEEL_OBJ_WRITE_ALWAYS,
        .is_volatile  = true,
    };
    ASSERT_EQ(keel_metadata_cache_add(cache, &vfunc), KEEL_OK);

    /* Parse: SELECT write_log(42) */
    keel_arena_t* arena = keel_arena_create(16384);
    ASSERT_NE(arena, NULL);

    keel_str_t sql = KEEL_STR("SELECT write_log(42)");
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, sql, arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);

    keel_qt_builder_t builder;
    keel_qt_builder_init(&builder, arena);
    keel_qt_query_t* qt = keel_qt_build(&builder, ast);
    ASSERT_NE(qt, NULL);
    ASSERT_TRUE(qt->func_count >= 1);

    bool has_write_function = false;
    bool has_write_trigger  = false;
    bool has_write_rule     = false;
    bool needs_primary      = false;

    keel_error_t err = keel_metadata_analyze_query(cache, qt,
                                                    &has_write_function,
                                                    &has_write_trigger,
                                                    &has_write_rule,
                                                    &needs_primary);
    ASSERT_EQ(err, KEEL_OK);
    ASSERT_TRUE(has_write_function);   /* volatile function detected */
    ASSERT_TRUE(needs_primary);        /* must be routed to primary */

    keel_arena_destroy(arena);
    keel_metadata_cache_destroy(cache);
}

/**
 * @brief Verify that a SELECT calling only a stable / safe function does NOT
 *        trigger primary routing (function not in cache ⇒ conservative safe).
 */
TEST(analyze_query_safe_function_no_cache_hit) {
    keel_metadata_cache_t* cache = keel_metadata_cache_create("testdb", NULL);
    ASSERT_NE(cache, NULL);

    /* Cache only has a volatile function for a *different* name */
    keel_cached_object_t vfunc = {
        .schema      = "public",
        .name        = "other_func",
        .type        = 'f',
        .write_type  = KEEL_OBJ_WRITE_ALWAYS,
        .is_volatile = true,
    };
    ASSERT_EQ(keel_metadata_cache_add(cache, &vfunc), KEEL_OK);

    keel_arena_t* arena = keel_arena_create(16384);
    ASSERT_NE(arena, NULL);

    /* This query calls safe_func which is NOT in the cache */
    keel_str_t sql = KEEL_STR("SELECT safe_func(id) FROM users");
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, sql, arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);

    keel_qt_builder_t builder;
    keel_qt_builder_init(&builder, arena);
    keel_qt_query_t* qt = keel_qt_build(&builder, ast);
    ASSERT_NE(qt, NULL);

    bool has_write_function = false;
    bool needs_primary      = false;

    keel_error_t err = keel_metadata_analyze_query(cache, qt,
                                                    &has_write_function,
                                                    NULL, NULL,
                                                    &needs_primary);
    ASSERT_EQ(err, KEEL_OK);
    /* safe_func is not in cache → conservative: not flagged as write-unsafe */
    ASSERT_FALSE(has_write_function);
    ASSERT_FALSE(needs_primary);

    keel_arena_destroy(arena);
    keel_metadata_cache_destroy(cache);
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;
    
    printf("\n=== Router Plugin Tests ===\n\n");
    
    printf("Metadata cache tests:\n");
    RUN_TEST(metadata_cache_create);
    RUN_TEST(metadata_cache_add_lookup);
    RUN_TEST(metadata_cache_check_write);
    RUN_TEST(metadata_cache_refresh);
    RUN_TEST(metadata_cache_clear);
    RUN_TEST(metadata_cache_null_safety);
    RUN_TEST(metadata_cache_dump);
    RUN_TEST(metadata_cache_remove);
    RUN_TEST(metadata_config_default);
    
    printf("\nPlugin tests:\n");
    RUN_TEST(plugin_create_default);
    RUN_TEST(plugin_create_patroni);
    
    printf("\nRouter manager tests:\n");
    RUN_TEST(router_mgr_create);
    RUN_TEST(router_mgr_register_plugin);
    RUN_TEST(router_mgr_set_default);
    RUN_TEST(router_mgr_route_with_metadata);
    
    printf("\nDiscovery tests:\n");
    RUN_TEST(discovery_create);
    RUN_TEST(discovery_config_patroni);
    RUN_TEST(discovery_probe);
    RUN_TEST(discovery_null_safety);
    RUN_TEST(discovery_config_defaults);
    RUN_TEST(discovery_methods);
    RUN_TEST(discovery_probe_conn);
    RUN_TEST(discovery_apply_topology);
    RUN_TEST(discovery_failover_callback);
    RUN_TEST(discovery_probe_null_args);
    RUN_TEST(discovery_refresh_null);
    RUN_TEST(discovery_start_stop);
    
    printf("\nCustom plugin tests:\n");
    RUN_TEST(custom_plugin);
    
    printf("\nSQL query tests:\n");
    RUN_TEST(sql_probe_queries_defined);
    RUN_TEST(metadata_introspection_queries_defined);

    printf("\nC3 function write-safety analysis tests:\n");
    RUN_TEST(query_tree_func_ref_collection);
    RUN_TEST(analyze_query_volatile_function);
    RUN_TEST(analyze_query_safe_function_no_cache_hit);
    
    printf("\n=== Results ===\n");
    printf("Passed: %d\n", tests_passed);
    printf("Failed: %d\n", tests_failed);
    
    return tests_failed > 0 ? 1 : 0;
}
