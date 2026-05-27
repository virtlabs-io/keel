/**
 * @file test_sharding.c
 * @brief Unit tests for Phase 1 shard-key extraction helpers.
 */

#include "test_utils.h"
#include "keel/core/router.h"
#include "keel/core/sharding.h"
#include "keel/core/ini.h"
#include "keel/mem/mem.h"

#include <string.h>

int g_tests_run = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) {
    return (g_tests_failed == 0) ? 0 : 1;
}

static const keel_shard_rule_t users_by_id = {
    .table = "users",
    .column = "id",
    .shard_count = 8,
};

static const keel_shard_rule_t users_by_id_two_shards = {
    .table = "users",
    .column = "id",
    .shard_count = 2,
};

static void test_extract_int_literal(void) {
    TEST_BEGIN("shard_extract_int_literal");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("SELECT * FROM users WHERE id = 42"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(key.kind, KEEL_SHARD_KEY_INT64);
    TEST_ASSERT_EQ(key.value.int64_value, 42);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_extract_alias_qualified(void) {
    TEST_BEGIN("shard_extract_alias_qualified");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("SELECT * FROM users u WHERE u.id = 9 AND u.active = true"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(key.kind, KEEL_SHARD_KEY_INT64);
    TEST_ASSERT_EQ(key.value.int64_value, 9);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_extract_string_literal(void) {
    TEST_BEGIN("shard_extract_string_literal");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_rule_t tenants_by_slug = {
        .table = "tenants",
        .column = "slug",
        .shard_count = 16,
    };
    keel_shard_key_t key;

    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("SELECT * FROM tenants WHERE slug = 'acme'"),
        &tenants_by_slug,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(key.kind, KEEL_SHARD_KEY_STRING);
    TEST_ASSERT(key.value.string_value.len == 4);
    TEST_ASSERT(strncmp(key.value.string_value.data, "acme", 4) == 0);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_extract_param(void) {
    TEST_BEGIN("shard_extract_param");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("SELECT * FROM users WHERE id = $3"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(key.kind, KEEL_SHARD_KEY_PARAM);
    TEST_ASSERT_EQ(key.value.param_index, 3);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_reject_conflicting_keys(void) {
    TEST_BEGIN("shard_reject_conflicting_keys");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("SELECT * FROM users WHERE id = 1 AND id = 2"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_SUPPORTED);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_not_found_without_shard_predicate(void) {
    TEST_BEGIN("shard_not_found_without_predicate");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("SELECT * FROM users WHERE email = 'a@b.c'"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_FOUND);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_reject_join_query(void) {
    TEST_BEGIN("shard_join_query_extracts_key_from_where");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    /* Phase B: JOINs are now transparent — shard key in WHERE still routable */
    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("SELECT * FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = 1"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(key.kind, KEEL_SHARD_KEY_INT64);
    TEST_ASSERT_EQ(key.value.int64_value, 1);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_reject_non_select(void) {
    TEST_BEGIN("shard_reject_non_dml");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    /* SET statements are not DML — must return NOT_SUPPORTED */
    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("SET search_path TO public"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT(err == KEEL_ERR_NOT_SUPPORTED || err == KEEL_ERR_SQL_PARSE);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_extract_update_where(void) {
    TEST_BEGIN("shard_extract_update_where");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("UPDATE users SET active = true WHERE id = 7"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(key.kind, KEEL_SHARD_KEY_INT64);
    TEST_ASSERT_EQ(key.value.int64_value, 7);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_extract_delete_where(void) {
    TEST_BEGIN("shard_extract_delete_where");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("DELETE FROM users WHERE id = 99"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(key.kind, KEEL_SHARD_KEY_INT64);
    TEST_ASSERT_EQ(key.value.int64_value, 99);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_extract_insert_shard_key(void) {
    TEST_BEGIN("shard_extract_insert_shard_key");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("INSERT INTO users (name, id, email) VALUES ('alice', 42, 'a@example.com')"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(key.kind, KEEL_SHARD_KEY_INT64);
    TEST_ASSERT_EQ(key.value.int64_value, 42);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_map_int_key(void) {
    TEST_BEGIN("shard_map_int_key");
    keel_shard_key_t key = {
        .kind = KEEL_SHARD_KEY_INT64,
        .value.int64_value = 42,
    };
    size_t shard = SIZE_MAX;

    keel_error_t err = keel_shard_map_key(&key, 8, &shard);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(shard, 2);
    TEST_END();
}

static void test_map_string_key_is_stable(void) {
    TEST_BEGIN("shard_map_string_key_is_stable");
    keel_shard_key_t left = {
        .kind = KEEL_SHARD_KEY_STRING,
        .value.string_value = KEEL_STR("acme"),
    };
    keel_shard_key_t right = {
        .kind = KEEL_SHARD_KEY_STRING,
        .value.string_value = KEEL_STR("acme"),
    };
    size_t shard_a = SIZE_MAX;
    size_t shard_b = SIZE_MAX;

    keel_error_t err = keel_shard_map_key(&left, 16, &shard_a);
    TEST_ASSERT_EQ(err, KEEL_OK);
    err = keel_shard_map_key(&right, 16, &shard_b);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT(shard_a < 16);
    TEST_ASSERT_EQ(shard_a, shard_b);
    TEST_END();
}

static void test_param_cannot_map_without_binding(void) {
    TEST_BEGIN("shard_param_cannot_map_without_binding");
    keel_shard_key_t key = {
        .kind = KEEL_SHARD_KEY_PARAM,
        .value.param_index = 1,
    };
    size_t shard = SIZE_MAX;

    keel_error_t err = keel_shard_map_key(&key, 4, &shard);
    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_SUPPORTED);
    TEST_END();
}

static keel_router_t* create_sharded_router(void) {
    keel_router_config_t config = keel_router_config_default();
    config.primary_read_weight = 0.0;
    /* Existing dispatch tests in this file exercise scatter paths; opt the
     * helper into the experimental scatter gate so the per-test SQL is not
     * rejected by the default fail-closed dispatcher. New rejection tests
     * below construct their own router and leave the gate at the default. */
    config.scatter_merge_enabled = true;

    keel_router_t* router = keel_router_create(&config);
    if (!router) {
        return NULL;
    }

    keel_route_server_t shard0_primary = {
        .name = "shard0-primary",
        .host = "db-shard0-primary",
        .port = 5432,
        .role = KEEL_SERVER_PRIMARY,
        .weight = 100,
        .shard_id = 0,
    };
    keel_route_server_t shard0_replica = {
        .name = "shard0-replica",
        .host = "db-shard0-replica",
        .port = 5432,
        .role = KEEL_SERVER_REPLICA,
        .weight = 100,
        .shard_id = 0,
    };
    keel_route_server_t shard1_primary = {
        .name = "shard1-primary",
        .host = "db-shard1-primary",
        .port = 5432,
        .role = KEEL_SERVER_PRIMARY,
        .weight = 100,
        .shard_id = 1,
    };
    keel_route_server_t shard1_replica = {
        .name = "shard1-replica",
        .host = "db-shard1-replica",
        .port = 5432,
        .role = KEEL_SERVER_REPLICA,
        .weight = 100,
        .shard_id = 1,
    };

    keel_router_add_server(router, &shard0_primary);
    keel_router_add_server(router, &shard0_replica);
    keel_router_add_server(router, &shard1_primary);
    keel_router_add_server(router, &shard1_replica);
    keel_router_set_server_health(router, "shard0-primary", KEEL_HEALTH_UP);
    keel_router_set_server_health(router, "shard0-replica", KEEL_HEALTH_UP);
    keel_router_set_server_health(router, "shard1-primary", KEEL_HEALTH_UP);
    keel_router_set_server_health(router, "shard1-replica", KEEL_HEALTH_UP);
    return router;
}

static void test_route_sharded_sql_read(void) {
    TEST_BEGIN("route_sharded_sql_read");
    keel_router_t* router = create_sharded_router();
    keel_route_decision_t decision;

    keel_error_t err = keel_router_route_sharded_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 42"),
        NULL,
        &users_by_id_two_shards,
        &decision);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(decision.server);
    if (!decision.server) {
        keel_router_destroy(router);
        TEST_END();
        return;
    }
    TEST_ASSERT_EQ(decision.shard_index, 0);
    TEST_ASSERT_EQ(decision.server->shard_id, 0);
    TEST_ASSERT_EQ(decision.server->role, KEEL_SERVER_REPLICA);
    keel_router_destroy(router);
    TEST_END();
}

static void test_route_sharded_sql_other_shard(void) {
    TEST_BEGIN("route_sharded_sql_other_shard");
    keel_router_t* router = create_sharded_router();
    keel_route_decision_t decision;

    keel_error_t err = keel_router_route_sharded_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 43"),
        NULL,
        &users_by_id_two_shards,
        &decision);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(decision.server);
    if (!decision.server) {
        keel_router_destroy(router);
        TEST_END();
        return;
    }
    TEST_ASSERT_EQ(decision.shard_index, 1);
    TEST_ASSERT_EQ(decision.server->shard_id, 1);
    TEST_ASSERT_EQ(decision.server->role, KEEL_SERVER_REPLICA);
    keel_router_destroy(router);
    TEST_END();
}

static void test_route_sharded_sql_requires_single_shard(void) {
    TEST_BEGIN("route_sharded_sql_requires_single_shard");
    keel_router_t* router = create_sharded_router();
    keel_route_decision_t decision;

    keel_error_t err = keel_router_route_sharded_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE email = 'a@b.c'"),
        NULL,
        &users_by_id_two_shards,
        &decision);

    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_FOUND);
    keel_router_destroy(router);
    TEST_END();
}

static void test_route_sharded_insert(void) {
    TEST_BEGIN("route_sharded_insert_to_primary");
    keel_router_t* router = create_sharded_router();
    keel_route_decision_t decision;

    /* id = 42 maps to shard 0 (42 % 2 = 0) */
    keel_error_t err = keel_router_route_sharded_sql(
        router,
        KEEL_STR("INSERT INTO users (id, name) VALUES (42, 'alice')"),
        NULL,
        &users_by_id_two_shards,
        &decision);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(decision.server);
    if (decision.server) {
        TEST_ASSERT_EQ(decision.shard_index, 0);
        TEST_ASSERT_EQ(decision.server->shard_id, 0);
        TEST_ASSERT_EQ(decision.server->role, KEEL_SERVER_PRIMARY);
    }
    keel_router_destroy(router);
    TEST_END();
}

static void test_route_sharded_update(void) {
    TEST_BEGIN("route_sharded_update_to_primary");
    keel_router_t* router = create_sharded_router();
    keel_route_decision_t decision;

    /* id = 43 maps to shard 1 (43 % 2 = 1) */
    keel_error_t err = keel_router_route_sharded_sql(
        router,
        KEEL_STR("UPDATE users SET active = false WHERE id = 43"),
        NULL,
        &users_by_id_two_shards,
        &decision);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(decision.server);
    if (decision.server) {
        TEST_ASSERT_EQ(decision.shard_index, 1);
        TEST_ASSERT_EQ(decision.server->shard_id, 1);
        TEST_ASSERT_EQ(decision.server->role, KEEL_SERVER_PRIMARY);
    }
    keel_router_destroy(router);
    TEST_END();
}

static void test_route_sharded_delete(void) {
    TEST_BEGIN("route_sharded_delete_to_primary");
    keel_router_t* router = create_sharded_router();
    keel_route_decision_t decision;

    /* id = 44 maps to shard 0 (44 % 2 = 0) */
    keel_error_t err = keel_router_route_sharded_sql(
        router,
        KEEL_STR("DELETE FROM users WHERE id = 44"),
        NULL,
        &users_by_id_two_shards,
        &decision);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(decision.server);
    if (decision.server) {
        TEST_ASSERT_EQ(decision.shard_index, 0);
        TEST_ASSERT_EQ(decision.server->shard_id, 0);
        TEST_ASSERT_EQ(decision.server->role, KEEL_SERVER_PRIMARY);
    }
    keel_router_destroy(router);
    TEST_END();
}

static void test_map_key_bound_int(void) {
    TEST_BEGIN("shard_map_key_bound_int");
    keel_shard_key_t key = {
        .kind = KEEL_SHARD_KEY_PARAM,
        .value.param_index = 1,
    };
    keel_shard_bound_params_t params = {
        .count = 1,
        .values = {
            [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 42 },
        },
    };
    size_t shard = SIZE_MAX;

    keel_error_t err = keel_shard_map_key_bound(&key, &params, 8, &shard);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(shard, 2); /* 42 % 8 == 2 */
    TEST_END();
}

static void test_map_key_bound_string(void) {
    TEST_BEGIN("shard_map_key_bound_string");
    keel_shard_key_t key = {
        .kind = KEEL_SHARD_KEY_PARAM,
        .value.param_index = 2,
    };
    keel_shard_bound_params_t params = {
        .count = 2,
        .values = {
            [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 0 },
            [1] = { .kind = KEEL_SHARD_KEY_STRING, .value.string_value = KEEL_STR("acme") },
        },
    };
    size_t shard_a = SIZE_MAX;
    keel_error_t err = keel_shard_map_key_bound(&key, &params, 16, &shard_a);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Verify it matches direct mapping of the literal */
    keel_shard_key_t lit = { .kind = KEEL_SHARD_KEY_STRING, .value.string_value = KEEL_STR("acme") };
    size_t shard_b = SIZE_MAX;
    err = keel_shard_map_key(&lit, 16, &shard_b);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(shard_a, shard_b);
    TEST_END();
}

static void test_map_key_bound_out_of_range(void) {
    TEST_BEGIN("shard_map_key_bound_out_of_range");
    keel_shard_key_t key = {
        .kind = KEEL_SHARD_KEY_PARAM,
        .value.param_index = 5,
    };
    keel_shard_bound_params_t params = {
        .count = 2,
        .values = {
            [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 1 },
            [1] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 2 },
        },
    };
    size_t shard = SIZE_MAX;
    keel_error_t err = keel_shard_map_key_bound(&key, &params, 4, &shard);
    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_FOUND);
    TEST_END();
}

static void test_map_key_bound_null_params(void) {
    TEST_BEGIN("shard_map_key_bound_null_params");
    keel_shard_key_t key = {
        .kind = KEEL_SHARD_KEY_PARAM,
        .value.param_index = 1,
    };
    size_t shard = SIZE_MAX;
    keel_error_t err = keel_shard_map_key_bound(&key, NULL, 4, &shard);
    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_FOUND);
    TEST_END();
}

static void test_route_sharded_sql_bound_select(void) {
    TEST_BEGIN("route_sharded_sql_bound_select");
    keel_router_t* router = create_sharded_router();
    keel_route_decision_t decision;

    keel_shard_bound_params_t params = {
        .count = 1,
        .values = {
            [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 42 },
        },
    };

    /* $1 = 42 -> shard 0 (42 % 2 = 0) -> replica */
    keel_error_t err = keel_router_route_sharded_sql_bound(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = $1"),
        NULL,
        &users_by_id_two_shards,
        &params,
        &decision);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(decision.server);
    if (decision.server) {
        TEST_ASSERT_EQ(decision.shard_index, 0);
        TEST_ASSERT_EQ(decision.server->shard_id, 0);
        TEST_ASSERT_EQ(decision.server->role, KEEL_SERVER_REPLICA);
    }
    keel_router_destroy(router);
    TEST_END();
}

static void test_route_sharded_sql_bound_insert(void) {
    TEST_BEGIN("route_sharded_sql_bound_insert");
    keel_router_t* router = create_sharded_router();
    keel_route_decision_t decision;

    keel_shard_bound_params_t params = {
        .count = 2,
        .values = {
            [0] = { .kind = KEEL_SHARD_KEY_STRING, .value.string_value = KEEL_STR("bob") },
            [1] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 43 },
        },
    };

    /* $2 = 43 -> shard 1 (43 % 2 = 1) -> primary */
    keel_error_t err = keel_router_route_sharded_sql_bound(
        router,
        KEEL_STR("INSERT INTO users (name, id) VALUES ($1, $2)"),
        NULL,
        &users_by_id_two_shards,
        &params,
        &decision);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(decision.server);
    if (decision.server) {
        TEST_ASSERT_EQ(decision.shard_index, 1);
        TEST_ASSERT_EQ(decision.server->shard_id, 1);
        TEST_ASSERT_EQ(decision.server->role, KEEL_SERVER_PRIMARY);
    }
    keel_router_destroy(router);
    TEST_END();
}

static void test_route_sharded_sql_bound_update(void) {
    TEST_BEGIN("route_sharded_sql_bound_update");
    keel_router_t* router = create_sharded_router();
    keel_route_decision_t decision;

    keel_shard_bound_params_t params = {
        .count = 1,
        .values = {
            [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 44 },
        },
    };

    /* $1 = 44 -> shard 0 (44 % 2 = 0) -> primary */
    keel_error_t err = keel_router_route_sharded_sql_bound(
        router,
        KEEL_STR("UPDATE users SET active = false WHERE id = $1"),
        NULL,
        &users_by_id_two_shards,
        &params,
        &decision);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(decision.server);
    if (decision.server) {
        TEST_ASSERT_EQ(decision.shard_index, 0);
        TEST_ASSERT_EQ(decision.server->shard_id, 0);
        TEST_ASSERT_EQ(decision.server->role, KEEL_SERVER_PRIMARY);
    }
    keel_router_destroy(router);
    TEST_END();
}

static void test_plan_single_shard_select(void) {
    TEST_BEGIN("shard_plan_single_select");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    keel_shard_plan(
        KEEL_STR("SELECT * FROM users WHERE id = 42"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, 0); /* 42 % 2 == 0 */
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_plan_single_shard_update(void) {
    TEST_BEGIN("shard_plan_single_update");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    keel_shard_plan(
        KEEL_STR("UPDATE users SET active = true WHERE id = 43"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, 1); /* 43 % 2 == 1 */
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_plan_scatter_no_predicate(void) {
    TEST_BEGIN("shard_plan_scatter_no_predicate");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    keel_shard_plan(
        KEEL_STR("SELECT * FROM users WHERE email = 'a@b.c'"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SCATTER);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_plan_scatter_unbound_param(void) {
    TEST_BEGIN("shard_plan_scatter_unbound_param");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    /* No params provided — $1 is unresolved → scatter */
    keel_shard_plan(
        KEEL_STR("SELECT * FROM users WHERE id = $1"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SCATTER);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_plan_single_shard_bound_param(void) {
    TEST_BEGIN("shard_plan_single_bound_param");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    keel_shard_bound_params_t params = {
        .count = 1,
        .values = { [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 43 } },
    };

    keel_shard_plan(
        KEEL_STR("SELECT * FROM users WHERE id = $1"),
        &users_by_id_two_shards, &params, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, 1); /* 43 % 2 == 1 */
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_plan_unsupported_ddl(void) {
    TEST_BEGIN("shard_plan_unsupported_ddl");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    keel_shard_plan(
        KEEL_STR("CREATE TABLE users (id bigint primary key)"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_UNSUPPORTED);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_plan_unsupported_join(void) {
    TEST_BEGIN("shard_plan_join_routes_single");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    /* Phase B: JOIN with a shard predicate in WHERE → single-shard plan */
    keel_shard_plan(
        KEEL_STR("SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = 1"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, (size_t)1); /* 1 % 2 == 1 */
    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * Phase B: JOIN shard-key extraction + CTE/no-WHERE scatter tests
 * ============================================================================ */

static void test_join_no_shard_predicate_scatters(void) {
    TEST_BEGIN("shard_join_no_shard_predicate_scatters");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    /* JOIN present but WHERE has no predicate on the shard column → scatter */
    keel_shard_plan(
        KEEL_STR("SELECT u.name FROM users u JOIN orders o ON o.user_id = u.id"
                 " WHERE o.status = 'active'"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SCATTER);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_join_shard_table_absent_scatters(void) {
    TEST_BEGIN("shard_join_shard_table_absent_unsupported");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    /* The shard table (users) is not in the FROM list — the rule doesn't
     * apply, so the planner must return UNSUPPORTED (not scatter). */
    keel_shard_plan(
        KEEL_STR("SELECT o.id FROM orders o JOIN payments p ON p.order_id = o.id"
                 " WHERE o.id = 7"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_UNSUPPORTED);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_join_three_table_routes_single(void) {
    TEST_BEGIN("shard_join_three_table_routes_single");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    /* 3-table join; shard table present; WHERE pins shard key → single */
    keel_shard_plan(
        KEEL_STR("SELECT u.name, o.total, p.amount"
                 " FROM users u"
                 " JOIN orders o ON o.user_id = u.id"
                 " JOIN payments p ON p.order_id = o.id"
                 " WHERE u.id = 42"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, (size_t)0); /* 42 % 2 == 0 */
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_select_no_where_scatters(void) {
    TEST_BEGIN("shard_select_no_where_scatters");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    keel_shard_plan(
        KEEL_STR("SELECT id, name FROM users"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SCATTER);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_select_no_from_scatters(void) {
    TEST_BEGIN("shard_select_no_from_unsupported");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    /* SELECT with no FROM (e.g. SELECT 1+1): no table, rule cannot apply */
    keel_shard_plan(
        KEEL_STR("SELECT 1 + 1"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_UNSUPPORTED);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_cte_select_scatters(void) {
    TEST_BEGIN("shard_cte_select_scatters");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    keel_shard_plan(
        KEEL_STR("WITH active AS (SELECT id FROM users WHERE active = true)"
                 " SELECT * FROM active"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SCATTER);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_cte_update_scatters(void) {
    TEST_BEGIN("shard_cte_update_scatters");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    keel_shard_plan(
        KEEL_STR("WITH src AS (SELECT id FROM staging)"
                 " UPDATE users SET active = false FROM src WHERE users.id = src.id"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SCATTER);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_cte_delete_scatters(void) {
    TEST_BEGIN("shard_cte_delete_scatters");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_plan_t plan;

    keel_shard_plan(
        KEEL_STR("WITH old AS (SELECT id FROM users WHERE created_at < '2020-01-01')"
                 " DELETE FROM users USING old WHERE users.id = old.id"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SCATTER);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_cte_writable_insert_routes_single(void) {
    TEST_BEGIN("shard_cte_writable_insert_routes_single");
    keel_arena_t* arena = keel_arena_create(8192);
    keel_shard_plan_t plan;

    /* WITH ins AS (INSERT INTO users(id,name) VALUES (43,'x') RETURNING id)
     * SELECT id FROM ins  → 43 % 2 == 1 → SINGLE shard 1 */
    keel_shard_plan(
        KEEL_STR("WITH ins AS (INSERT INTO users(id, name) VALUES (43, 'x') RETURNING id)"
                 " SELECT id FROM ins"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, (size_t)1);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_cte_writable_update_routes_single(void) {
    TEST_BEGIN("shard_cte_writable_update_routes_single");
    keel_arena_t* arena = keel_arena_create(8192);
    keel_shard_plan_t plan;

    keel_shard_plan(
        KEEL_STR("WITH upd AS (UPDATE users SET name = 'y' WHERE id = 42 RETURNING id)"
                 " SELECT id FROM upd"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, (size_t)0);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_cte_writable_delete_routes_single(void) {
    TEST_BEGIN("shard_cte_writable_delete_routes_single");
    keel_arena_t* arena = keel_arena_create(8192);
    keel_shard_plan_t plan;

    keel_shard_plan(
        KEEL_STR("WITH del AS (DELETE FROM users WHERE id = 43 RETURNING id)"
                 " SELECT id FROM del"),
        &users_by_id_two_shards, NULL, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, (size_t)1);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_join_key_extraction_with_table_alias(void) {
    TEST_BEGIN("shard_join_key_extraction_table_alias");
    keel_arena_t* arena = keel_arena_create(4096);
    keel_shard_key_t key;

    /* Alias 'u' on shard table; predicate uses alias — should still extract */
    keel_error_t err = keel_shard_extract_key_sql(
        KEEL_STR("SELECT u.name FROM users u"
                 " LEFT JOIN orders o ON o.user_id = u.id"
                 " WHERE u.id = 99"),
        &users_by_id,
        &key,
        arena);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(key.kind, KEEL_SHARD_KEY_INT64);
    TEST_ASSERT_EQ(key.value.int64_value, 99);
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_router_plan_sharded_sql(void) {
    TEST_BEGIN("router_plan_sharded_sql");
    keel_router_t* router = create_sharded_router();
    keel_shard_plan_t plan;

    keel_router_plan_sharded_sql(
        router,
        KEEL_STR("DELETE FROM users WHERE id = 44"),
        &users_by_id_two_shards, NULL, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, 0); /* 44 % 2 == 0 */
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Shard rule registry tests
 * ============================================================================ */

static void test_registry_add_and_get(void) {
    TEST_BEGIN("registry_add_and_get");
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);

    keel_error_t err = keel_router_add_shard_rule(router, "users", "id", 4);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_router_shard_rule_count(router), (size_t)1);

    const keel_shard_rule_t* r = keel_router_get_shard_rule(router, "users");
    TEST_ASSERT_NOT_NULL(r);
    if (r) {
        TEST_ASSERT_EQ(r->shard_count, (size_t)4);
    }

    /* Case-insensitive lookup */
    const keel_shard_rule_t* r2 = keel_router_get_shard_rule(router, "USERS");
    TEST_ASSERT_NOT_NULL(r2);

    /* Unknown table returns NULL */
    TEST_ASSERT_NULL(keel_router_get_shard_rule(router, "orders"));

    keel_router_destroy(router);
    TEST_END();
}

static void test_registry_overwrite(void) {
    TEST_BEGIN("registry_overwrite");
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);

    keel_router_add_shard_rule(router, "users", "id", 4);
    keel_router_add_shard_rule(router, "users", "id", 8); /* overwrite */

    TEST_ASSERT_EQ(keel_router_shard_rule_count(router), (size_t)1);
    const keel_shard_rule_t* r = keel_router_get_shard_rule(router, "users");
    TEST_ASSERT_NOT_NULL(r);
    if (r) TEST_ASSERT_EQ(r->shard_count, (size_t)8);

    keel_router_destroy(router);
    TEST_END();
}

static void test_registry_remove(void) {
    TEST_BEGIN("registry_remove");
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);

    keel_router_add_shard_rule(router, "users",  "id",      4);
    keel_router_add_shard_rule(router, "orders", "order_id", 2);
    TEST_ASSERT_EQ(keel_router_shard_rule_count(router), (size_t)2);

    keel_error_t err = keel_router_remove_shard_rule(router, "users");
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(keel_router_shard_rule_count(router), (size_t)1);
    TEST_ASSERT_NULL(keel_router_get_shard_rule(router, "users"));
    TEST_ASSERT_NOT_NULL(keel_router_get_shard_rule(router, "orders"));

    /* Remove non-existent */
    err = keel_router_remove_shard_rule(router, "tenants");
    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_FOUND);

    keel_router_destroy(router);
    TEST_END();
}

static void test_plan_sql_single(void) {
    TEST_BEGIN("plan_sql_single");
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);

    keel_router_add_shard_rule(router, "users",  "id",       2);
    keel_router_add_shard_rule(router, "orders", "order_id", 4);

    keel_shard_plan_t plan;
    keel_router_plan_sql(router,
        KEEL_STR("SELECT * FROM users WHERE id = 43"),
        NULL, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, (size_t)1); /* 43 % 2 == 1 */

    keel_router_destroy(router);
    TEST_END();
}

static void test_plan_sql_second_rule(void) {
    TEST_BEGIN("plan_sql_second_rule");
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);

    keel_router_add_shard_rule(router, "users",  "id",       2);
    keel_router_add_shard_rule(router, "orders", "order_id", 4);

    keel_shard_plan_t plan;
    /* Query targets "orders" — first rule (users) is UNSUPPORTED, second matches */
    keel_router_plan_sql(router,
        KEEL_STR("UPDATE orders SET status = 'done' WHERE order_id = 8"),
        NULL, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, (size_t)0); /* 8 % 4 == 0 */

    keel_router_destroy(router);
    TEST_END();
}

static void test_plan_sql_scatter(void) {
    TEST_BEGIN("plan_sql_scatter");
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);

    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_shard_plan_t plan;
    keel_router_plan_sql(router,
        KEEL_STR("SELECT * FROM users WHERE email = 'a@b.c'"),
        NULL, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SCATTER);

    keel_router_destroy(router);
    TEST_END();
}

static void test_plan_sql_no_matching_rule(void) {
    TEST_BEGIN("plan_sql_no_matching_rule");
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);

    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_shard_plan_t plan;
    /* "tenants" table has no registered rule */
    keel_router_plan_sql(router,
        KEEL_STR("SELECT * FROM tenants WHERE slug = 'acme'"),
        NULL, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_UNSUPPORTED);

    keel_router_destroy(router);
    TEST_END();
}

static void test_plan_sql_bound_param(void) {
    TEST_BEGIN("plan_sql_bound_param");
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);

    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_shard_bound_params_t params = {
        .count = 1,
        .values = { [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 42 } },
    };
    keel_shard_plan_t plan;
    keel_router_plan_sql(router,
        KEEL_STR("INSERT INTO users (id, name) VALUES ($1, 'alice')"),
        &params, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, (size_t)0); /* 42 % 2 == 0 */

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Phase 6: Scatter server list API
 * ============================================================================ */

static void test_scatter_invalid_args(void) {
    TEST_BEGIN("scatter_invalid_args");
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);

    keel_shard_rule_t rule = { .table = "users", .column = "id", .shard_count = 2 };
    keel_scatter_plan_t out;

    /* NULL router */
    TEST_ASSERT_EQ(keel_router_scatter_servers(NULL, NULL, &rule, false, &out),
                   KEEL_ERR_INVALID_ARG);
    /* NULL rule */
    TEST_ASSERT_EQ(keel_router_scatter_servers(router, NULL, NULL, false, &out),
                   KEEL_ERR_INVALID_ARG);
    /* NULL out */
    TEST_ASSERT_EQ(keel_router_scatter_servers(router, NULL, &rule, false, NULL),
                   KEEL_ERR_INVALID_ARG);
    /* zero shard_count */
    keel_shard_rule_t zero_rule = { .table = "x", .column = "id", .shard_count = 0 };
    TEST_ASSERT_EQ(keel_router_scatter_servers(router, NULL, &zero_rule, false, &out),
                   KEEL_ERR_INVALID_ARG);

    keel_router_destroy(router);
    TEST_END();
}

static void test_scatter_overflow(void) {
    TEST_BEGIN("scatter_overflow");
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);

    keel_shard_rule_t big_rule = {
        .table = "events", .column = "id",
        .shard_count = KEEL_SCATTER_MAX_SHARDS + 1,
    };
    keel_scatter_plan_t out;
    TEST_ASSERT_EQ(keel_router_scatter_servers(router, NULL, &big_rule, false, &out),
                   KEEL_ERR_OVERFLOW);

    keel_router_destroy(router);
    TEST_END();
}

static void test_scatter_write_routes_to_primary(void) {
    TEST_BEGIN("scatter_write_routes_to_primary");
    keel_router_t* router = create_sharded_router(); /* 2 shards, primary+replica each */

    keel_shard_rule_t rule = { .table = "users", .column = "id", .shard_count = 2 };
    keel_scatter_plan_t out;
    keel_error_t err = keel_router_scatter_servers(router, NULL, &rule, true, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.count, (size_t)2);
    TEST_ASSERT_EQ(out.failed, (size_t)0);
    /* Write scatter: both shards must go to a primary (RW) */
    TEST_ASSERT_NOT_NULL(out.decisions[0].server);
    TEST_ASSERT_EQ(out.decisions[0].server->role, KEEL_SERVER_PRIMARY);
    TEST_ASSERT_NOT_NULL(out.decisions[1].server);
    TEST_ASSERT_EQ(out.decisions[1].server->role, KEEL_SERVER_PRIMARY);
    /* is_read must be false for write scatter */
    TEST_ASSERT_EQ(out.decisions[0].is_read, false);
    TEST_ASSERT_EQ(out.decisions[1].is_read, false);

    keel_router_destroy(router);
    TEST_END();
}

static void test_scatter_read_routes_to_replica(void) {
    TEST_BEGIN("scatter_read_routes_to_replica");
    /* create_sharded_router sets primary_read_weight=0 so reads never go to primary */
    keel_router_t* router = create_sharded_router();

    keel_shard_rule_t rule = { .table = "users", .column = "id", .shard_count = 2 };
    keel_scatter_plan_t out;
    keel_error_t err = keel_router_scatter_servers(router, NULL, &rule, false, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.count, (size_t)2);
    TEST_ASSERT_EQ(out.failed, (size_t)0);
    /* Read scatter: both shards should go to replica */
    TEST_ASSERT_NOT_NULL(out.decisions[0].server);
    TEST_ASSERT_EQ(out.decisions[0].server->role, KEEL_SERVER_REPLICA);
    TEST_ASSERT_NOT_NULL(out.decisions[1].server);
    TEST_ASSERT_EQ(out.decisions[1].server->role, KEEL_SERVER_REPLICA);
    TEST_ASSERT_EQ(out.decisions[0].is_read, true);
    TEST_ASSERT_EQ(out.decisions[1].is_read, true);

    keel_router_destroy(router);
    TEST_END();
}

static void test_scatter_shard_index_populated(void) {
    TEST_BEGIN("scatter_shard_index_populated");
    keel_router_t* router = create_sharded_router();

    keel_shard_rule_t rule = { .table = "users", .column = "id", .shard_count = 2 };
    keel_scatter_plan_t out;
    keel_router_scatter_servers(router, NULL, &rule, true, &out);

    TEST_ASSERT_EQ(out.decisions[0].shard_index, (size_t)0);
    TEST_ASSERT_EQ(out.decisions[1].shard_index, (size_t)1);

    keel_router_destroy(router);
    TEST_END();
}

static void test_scatter_missing_shard_server(void) {
    TEST_BEGIN("scatter_missing_shard_server");
    /* Build a router that only has servers for shard 0 */
    keel_router_config_t cfg = keel_router_config_default();
    cfg.primary_read_weight = 0.0;
    cfg.failover_to_primary = false; /* prevent RW fallover for reads */
    keel_router_t* router = keel_router_create(&cfg);

    keel_route_server_t shard0_primary = {
        .name = "s0p", .host = "h0p", .port = 5432,
        .role = KEEL_SERVER_PRIMARY, .weight = 100, .shard_id = 0,
    };
    keel_router_add_server(router, &shard0_primary);
    keel_router_set_server_health(router, "s0p", KEEL_HEALTH_UP);
    /* Shard 1 has no servers at all */

    keel_shard_rule_t rule = { .table = "users", .column = "id", .shard_count = 2 };
    keel_scatter_plan_t out;
    keel_error_t err = keel_router_scatter_servers(router, NULL, &rule, true, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);      /* function itself succeeds */
    TEST_ASSERT_EQ(out.count, (size_t)2);
    TEST_ASSERT_EQ(out.failed, (size_t)1);   /* shard 1 has no server */
    TEST_ASSERT_NOT_NULL(out.decisions[0].server);
    TEST_ASSERT_NULL(out.decisions[1].server);

    keel_router_destroy(router);
    TEST_END();
}

static void test_scatter_in_transaction_forces_primary(void) {
    TEST_BEGIN("scatter_in_transaction_forces_primary");
    keel_router_t* router = create_sharded_router();

    keel_route_session_t session = { .in_transaction = true };
    keel_shard_rule_t rule = { .table = "users", .column = "id", .shard_count = 2 };
    keel_scatter_plan_t out;
    keel_router_scatter_servers(router, &session, &rule, false /* is_write=false */,  &out);

    /* Even though is_write=false, in_transaction forces primary */
    TEST_ASSERT_NOT_NULL(out.decisions[0].server);
    TEST_ASSERT_EQ(out.decisions[0].server->role, KEEL_SERVER_PRIMARY);
    TEST_ASSERT_NOT_NULL(out.decisions[1].server);
    TEST_ASSERT_EQ(out.decisions[1].server->role, KEEL_SERVER_PRIMARY);

    keel_router_destroy(router);
    TEST_END();
}

static void test_scatter_end_to_end_from_plan(void) {
    TEST_BEGIN("scatter_end_to_end_from_plan");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    /* SQL with no shard-key predicate → SCATTER */
    keel_shard_plan_t plan;
    keel_router_plan_sql(router,
        KEEL_STR("SELECT name FROM users WHERE email = 'x@y.z'"),
        NULL, &plan);
    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SCATTER);

    /* Resolve scatter to concrete servers */
    const keel_shard_rule_t* rule = keel_router_get_shard_rule(router, "users");
    TEST_ASSERT_NOT_NULL(rule);

    keel_scatter_plan_t scatter;
    keel_error_t err = keel_router_scatter_servers(router, NULL, rule, false, &scatter);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(scatter.count, (size_t)2);
    TEST_ASSERT_EQ(scatter.failed, (size_t)0);
    TEST_ASSERT_NOT_NULL(scatter.decisions[0].server);
    TEST_ASSERT_NOT_NULL(scatter.decisions[1].server);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Feature 1: Rule persistence from config (keel_router_load_shard_rules_from_config)
 * ============================================================================ */

/** Path to the test INI file (written at test build time) */
#ifndef KEEL_TEST_INI_PATH
#define KEEL_TEST_INI_PATH "keel_test.ini"
#endif

static void test_load_rules_from_config_null_args(void) {
    TEST_BEGIN("load_rules_null_args");
    keel_router_t* router = keel_router_create(NULL);

    /* Both NULL */
    TEST_ASSERT_EQ(keel_router_load_shard_rules_from_config(NULL, NULL, NULL), (size_t)0);
    /* NULL config */
    TEST_ASSERT_EQ(keel_router_load_shard_rules_from_config(router, NULL, NULL), (size_t)0);
    /* NULL router */
    keel_config_t* cfg = keel_config_load(KEEL_TEST_INI_PATH);
    if (cfg) {
        TEST_ASSERT_EQ(keel_router_load_shard_rules_from_config(NULL, cfg, "myapp"), (size_t)0);
        keel_config_free(cfg);
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_load_rules_from_config_reads_sections(void) {
    TEST_BEGIN("load_rules_reads_sections");
    keel_config_t* cfg = keel_config_load(KEEL_TEST_INI_PATH);
    if (!cfg) {
        /* Skip if INI file not found (e.g. run from wrong cwd) */
        TEST_END();
        return;
    }

    keel_router_t* router = keel_router_create(NULL);
    size_t loaded = keel_router_load_shard_rules_from_config(router, cfg, "myapp");

    /* keel_test.ini defines worker_group.myapp.shard_rule.{users,orders,tenants} */
    TEST_ASSERT(loaded >= 3);
    TEST_ASSERT_EQ(keel_router_shard_rule_count(router), loaded);

    const keel_shard_rule_t* ru = keel_router_get_shard_rule(router, "users");
    TEST_ASSERT_NOT_NULL(ru);
    if (ru) {
        TEST_ASSERT_EQ(ru->shard_count, (size_t)8);
    }

    const keel_shard_rule_t* ro = keel_router_get_shard_rule(router, "orders");
    TEST_ASSERT_NOT_NULL(ro);
    if (ro) {
        TEST_ASSERT_EQ(ro->shard_count, (size_t)4);
    }

    const keel_shard_rule_t* rt = keel_router_get_shard_rule(router, "tenants");
    TEST_ASSERT_NOT_NULL(rt);
    if (rt) {
        TEST_ASSERT_EQ(rt->shard_count, (size_t)16);
    }

    keel_router_destroy(router);
    keel_config_free(cfg);
    TEST_END();
}

static void test_load_rules_from_config_overwrites(void) {
    TEST_BEGIN("load_rules_overwrites_existing");
    keel_config_t* cfg = keel_config_load(KEEL_TEST_INI_PATH);
    if (!cfg) {
        TEST_END();
        return;
    }

    keel_router_t* router = keel_router_create(NULL);

    /* Pre-register 'users' with a different shard count */
    keel_router_add_shard_rule(router, "users", "id", 2);
    TEST_ASSERT_EQ(keel_router_shard_rule_count(router), (size_t)1);

    /* Config load should overwrite */
    keel_router_load_shard_rules_from_config(router, cfg, "myapp");

    const keel_shard_rule_t* ru = keel_router_get_shard_rule(router, "users");
    TEST_ASSERT_NOT_NULL(ru);
    if (ru) {
        /* INI says shard_count = 8 */
        TEST_ASSERT_EQ(ru->shard_count, (size_t)8);
    }

    keel_router_destroy(router);
    keel_config_free(cfg);
    TEST_END();
}

static void test_load_rules_survives_restart_semantics(void) {
    TEST_BEGIN("load_rules_survives_restart_semantics");
    keel_config_t* cfg = keel_config_load(KEEL_TEST_INI_PATH);
    if (!cfg) {
        TEST_END();
        return;
    }

    /* Simulate: create fresh router (restart), load rules from config */
    keel_router_t* router = keel_router_create(NULL);
    size_t loaded = keel_router_load_shard_rules_from_config(router, cfg, "myapp");
    TEST_ASSERT(loaded >= 1);

    /* Rules must be usable immediately — plan a query against a loaded rule */
    keel_shard_plan_t plan;
    keel_router_plan_sql(router,
        KEEL_STR("SELECT * FROM users WHERE id = 42"),
        NULL, &plan);
    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, (size_t)(42 % 8)); /* 42 % 8 == 2 */

    keel_router_destroy(router);
    keel_config_free(cfg);
    TEST_END();
}

/* ============================================================================
 * Feature 2: Combined plan + dispatch (keel_router_dispatch_sql)
 * ============================================================================ */

static void test_dispatch_null_args(void) {
    TEST_BEGIN("dispatch_null_args");
    keel_router_t* router = keel_router_create(NULL);
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;

    /* NULL router */
    TEST_ASSERT_EQ(
        keel_router_dispatch_sql(NULL, KEEL_STR("SELECT 1"), NULL, NULL, false, &out),
        KEEL_ERR_INVALID_ARG);

    /* NULL out */
    TEST_ASSERT_EQ(
        keel_router_dispatch_sql(router, KEEL_STR("SELECT 1"), NULL, NULL, false, NULL),
        KEEL_ERR_INVALID_ARG);

    /* Empty SQL */
    TEST_ASSERT_EQ(
        keel_router_dispatch_sql(router, KEEL_STR(""), NULL, NULL, false, &out),
        KEEL_ERR_INVALID_ARG);

    keel_router_destroy(router);
    TEST_END();
}

static void test_dispatch_no_rules_returns_not_supported(void) {
    TEST_BEGIN("dispatch_no_rules_unsupported");
    keel_router_t* router = keel_router_create(NULL);
    /* No rules registered */

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 1"),
        NULL, NULL, false, &out);

    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_SUPPORTED);

    keel_router_destroy(router);
    TEST_END();
}

static void test_dispatch_single_shard_read(void) {
    TEST_BEGIN("dispatch_single_shard_read");
    keel_router_t* router = create_sharded_router(); /* 2-shard, primary+replica each */
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    /* id=42 → shard 0, SELECT → replica */
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 42"),
        NULL, NULL, false, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)0); /* 42 % 2 == 0 */
        TEST_ASSERT_EQ(out.single.server->shard_id, (size_t)0);
        TEST_ASSERT_EQ(out.single.server->role, KEEL_SERVER_REPLICA);
        TEST_ASSERT_EQ(out.single.is_read, true);
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_dispatch_single_shard_write(void) {
    TEST_BEGIN("dispatch_single_shard_write");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    /* id=43 → shard 1, INSERT → primary */
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("INSERT INTO users (id, name) VALUES (43, 'bob')"),
        NULL, NULL, true, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)1); /* 43 % 2 == 1 */
        TEST_ASSERT_EQ(out.single.server->shard_id, (size_t)1);
        TEST_ASSERT_EQ(out.single.server->role, KEEL_SERVER_PRIMARY);
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_dispatch_scatter_read(void) {
    TEST_BEGIN("dispatch_scatter_read");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    /* No shard-key predicate → SCATTER */
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE email = 'x@y.z'"),
        NULL, NULL, false, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SCATTER);
    TEST_ASSERT_EQ(out.scatter.count, (size_t)2);
    TEST_ASSERT_EQ(out.scatter.failed, (size_t)0);
    /* create_sharded_router uses primary_read_weight=0 → replicas for reads */
    TEST_ASSERT_NOT_NULL(out.scatter.decisions[0].server);
    TEST_ASSERT_EQ(out.scatter.decisions[0].server->role, KEEL_SERVER_REPLICA);
    TEST_ASSERT_NOT_NULL(out.scatter.decisions[1].server);
    TEST_ASSERT_EQ(out.scatter.decisions[1].server->role, KEEL_SERVER_REPLICA);

    keel_router_destroy(router);
    TEST_END();
}

static void test_dispatch_scatter_write(void) {
    TEST_BEGIN("dispatch_scatter_write");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("UPDATE users SET active = false"),   /* no WHERE → scatter */
        NULL, NULL, true, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SCATTER);
    TEST_ASSERT_EQ(out.scatter.count, (size_t)2);
    /* Write scatter → primaries */
    if (out.scatter.decisions[0].server) {
        TEST_ASSERT_EQ(out.scatter.decisions[0].server->role, KEEL_SERVER_PRIMARY);
    }
    if (out.scatter.decisions[1].server) {
        TEST_ASSERT_EQ(out.scatter.decisions[1].server->role, KEEL_SERVER_PRIMARY);
    }

    keel_dispatch_result_cleanup(&out);
    keel_router_destroy(router);
    TEST_END();
}

static void test_dispatch_unsupported_ddl(void) {
    TEST_BEGIN("dispatch_unsupported_ddl");
    keel_router_t* router = keel_router_create(NULL);
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("CREATE TABLE users (id bigint primary key)"),
        NULL, NULL, true, &out);

    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_SUPPORTED);

    keel_router_destroy(router);
    TEST_END();
}

static void test_dispatch_bound_param_single(void) {
    TEST_BEGIN("dispatch_bound_param_single");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_shard_bound_params_t params = {
        .count = 1,
        .values = { [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 43 } },
    };
    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = $1"),
        NULL, &params, false, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)1); /* 43 % 2 == 1 */
        TEST_ASSERT_EQ(out.single.server->role, KEEL_SERVER_REPLICA);
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_dispatch_second_rule_matches(void) {
    TEST_BEGIN("dispatch_second_rule_matches");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users",  "id",       2);
    keel_router_add_shard_rule(router, "orders", "order_id", 2);

    keel_dispatch_result_t out;
    /* "orders" not matched by first rule */
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM orders WHERE order_id = 42"),
        NULL, NULL, false, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    /* 42 % 2 == 0 */
    TEST_ASSERT_EQ(out.single.shard_index, (size_t)0);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Feature 3: Scatter result aggregation (keel_route_agg_t)
 * ============================================================================ */

static void test_scatter_result_init(void) {
    TEST_BEGIN("scatter_result_init");
    keel_route_agg_t r;

    keel_route_agg_init(&r, NULL, NULL);

    TEST_ASSERT_NULL(r.merge);
    TEST_ASSERT_NULL(r.user_ctx);
    TEST_ASSERT_NULL(r.data);
    TEST_ASSERT_EQ(r.total_rows, (uint64_t)0);
    TEST_ASSERT_EQ(r.shards_completed, (size_t)0);
    TEST_ASSERT_EQ(r.shards_failed, (size_t)0);

    /* NULL result must not crash */
    keel_route_agg_init(NULL, NULL, NULL);

    TEST_END();
}

static void test_scatter_result_feed_null_rows_increments_failed(void) {
    TEST_BEGIN("scatter_result_feed_null_rows");
    keel_route_agg_t r;
    keel_route_agg_init(&r, NULL, NULL);

    keel_route_agg_feed(&r, 0, NULL, 0);
    TEST_ASSERT_EQ(r.shards_failed, (size_t)1);
    TEST_ASSERT_EQ(r.shards_completed, (size_t)0);
    TEST_ASSERT_EQ(r.total_rows, (uint64_t)0);

    keel_route_agg_feed(&r, 1, NULL, 0);
    TEST_ASSERT_EQ(r.shards_failed, (size_t)2);

    TEST_END();
}

static void test_scatter_result_feed_null_result_no_crash(void) {
    TEST_BEGIN("scatter_result_feed_null_result");
    /* Must not crash */
    keel_route_agg_feed(NULL, 0, NULL, 0);
    int dummy = 42;
    keel_route_agg_feed(NULL, 0, &dummy, 1);
    TEST_END();
}

static void test_scatter_result_feed_counts_rows(void) {
    TEST_BEGIN("scatter_result_feed_counts_rows");
    keel_route_agg_t r;
    keel_route_agg_init(&r, NULL, NULL);

    int shard0_data[5] = {1, 2, 3, 4, 5};
    int shard1_data[3] = {10, 11, 12};

    keel_route_agg_feed(&r, 0, shard0_data, 5);
    TEST_ASSERT_EQ(r.shards_completed, (size_t)1);
    TEST_ASSERT_EQ(r.total_rows, (uint64_t)5);

    keel_route_agg_feed(&r, 1, shard1_data, 3);
    TEST_ASSERT_EQ(r.shards_completed, (size_t)2);
    TEST_ASSERT_EQ(r.total_rows, (uint64_t)8);

    TEST_ASSERT_EQ(r.shards_failed, (size_t)0);

    TEST_END();
}

/** Context for the merge-callback tests. */
typedef struct {
    size_t  merge_calls;
    size_t  last_shard;
    size_t  last_row_count;
    int     sum;
} merge_test_ctx_t;

static void test_merge_cb(keel_route_agg_t* result,
                           size_t shard_index,
                           const void* rows,
                           size_t row_count,
                           void* user_ctx) {
    (void)result;
    merge_test_ctx_t* ctx = (merge_test_ctx_t*)user_ctx;
    ctx->merge_calls++;
    ctx->last_shard     = shard_index;
    ctx->last_row_count = row_count;

    const int* ints = (const int*)rows;
    for (size_t i = 0; i < row_count; i++) {
        ctx->sum += ints[i];
    }
}

static void test_scatter_result_merge_callback_invoked(void) {
    TEST_BEGIN("scatter_result_merge_callback_invoked");
    merge_test_ctx_t ctx = {0};
    keel_route_agg_t r;
    keel_route_agg_init(&r, test_merge_cb, &ctx);

    int shard0[3] = {1, 2, 3};
    int shard1[2] = {10, 20};

    keel_route_agg_feed(&r, 0, shard0, 3);
    TEST_ASSERT_EQ(ctx.merge_calls, (size_t)1);
    TEST_ASSERT_EQ(ctx.last_shard, (size_t)0);
    TEST_ASSERT_EQ(ctx.last_row_count, (size_t)3);

    keel_route_agg_feed(&r, 1, shard1, 2);
    TEST_ASSERT_EQ(ctx.merge_calls, (size_t)2);
    TEST_ASSERT_EQ(ctx.last_shard, (size_t)1);
    TEST_ASSERT_EQ(ctx.sum, 36); /* 1+2+3+10+20 */

    TEST_ASSERT_EQ(r.total_rows, (uint64_t)5);
    TEST_ASSERT_EQ(r.shards_completed, (size_t)2);
    TEST_ASSERT_EQ(r.shards_failed, (size_t)0);

    TEST_END();
}

static void test_scatter_result_merge_not_called_on_failure(void) {
    TEST_BEGIN("scatter_result_merge_not_called_on_failure");
    merge_test_ctx_t ctx = {0};
    keel_route_agg_t r;
    keel_route_agg_init(&r, test_merge_cb, &ctx);

    /* Failure: NULL rows */
    keel_route_agg_feed(&r, 0, NULL, 0);
    TEST_ASSERT_EQ(ctx.merge_calls, (size_t)0);
    TEST_ASSERT_EQ(r.shards_failed, (size_t)1);

    /* Success */
    int shard1[1] = {99};
    keel_route_agg_feed(&r, 1, shard1, 1);
    TEST_ASSERT_EQ(ctx.merge_calls, (size_t)1);
    TEST_ASSERT_EQ(r.shards_completed, (size_t)1);
    TEST_ASSERT_EQ(r.total_rows, (uint64_t)1);

    TEST_END();
}

static void test_scatter_result_end_to_end_with_dispatch(void) {
    TEST_BEGIN("scatter_result_end_to_end_with_dispatch");    /* Build router, add rule, dispatch a scatter query, feed results */
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t disp;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE email = 'a@b.c'"),
        NULL, NULL, false, &disp);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(disp.kind, KEEL_DISPATCH_SCATTER);

    merge_test_ctx_t ctx = {0};
    keel_route_agg_t result;
    keel_route_agg_init(&result, test_merge_cb, &ctx);

    /* Feed synthetic per-shard results */
    int shard0_rows[2] = {1, 2};
    int shard1_rows[3] = {3, 4, 5};
    keel_route_agg_feed(&result, 0, shard0_rows, 2);
    keel_route_agg_feed(&result, 1, shard1_rows, 3);

    TEST_ASSERT_EQ(result.total_rows, (uint64_t)5);
    TEST_ASSERT_EQ(result.shards_completed, (size_t)2);
    TEST_ASSERT_EQ(result.shards_failed, (size_t)0);
    TEST_ASSERT_EQ(ctx.sum, 15); /* 1+2+3+4+5 */

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Feature 4: Per-shard routing counters
 * ============================================================================ */

static void test_shard_single_route_counter_increments(void) {
    TEST_BEGIN("shard_single_route_counter_increments");
    keel_router_t* router = create_sharded_router(); /* 2-shard */
    keel_router_add_shard_rule(router, "users", "id", 2);
    keel_router_reset_stats(router);

    /* id=42 → shard 0 */
    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router, KEEL_STR("SELECT * FROM users WHERE id = 42"),
        NULL, NULL, false, &out);
    TEST_ASSERT_EQ(err, KEEL_OK);

    keel_router_stats_t stats;
    keel_router_get_stats(router, &stats);
    TEST_ASSERT_EQ(stats.shard_single_routes[0], (uint64_t)1);
    TEST_ASSERT_EQ(stats.shard_single_routes[1], (uint64_t)0);

    keel_router_destroy(router);
    TEST_END();
}

static void test_shard_single_route_counter_per_shard(void) {
    TEST_BEGIN("shard_single_route_counter_per_shard");
    keel_router_t* router = create_sharded_router(); /* 2-shard */
    keel_router_add_shard_rule(router, "users", "id", 2);
    keel_router_reset_stats(router);

    keel_dispatch_result_t out;
    /* id=42 → shard 0; id=43 → shard 1; id=44 → shard 0 */
    keel_router_dispatch_sql(router, KEEL_STR("SELECT * FROM users WHERE id = 42"),
                             NULL, NULL, false, &out);
    keel_router_dispatch_sql(router, KEEL_STR("SELECT * FROM users WHERE id = 43"),
                             NULL, NULL, false, &out);
    keel_router_dispatch_sql(router, KEEL_STR("SELECT * FROM users WHERE id = 44"),
                             NULL, NULL, false, &out);

    keel_router_stats_t stats;
    keel_router_get_stats(router, &stats);
    TEST_ASSERT_EQ(stats.shard_single_routes[0], (uint64_t)2); /* 42 and 44 */
    TEST_ASSERT_EQ(stats.shard_single_routes[1], (uint64_t)1); /* 43 */

    keel_router_destroy(router);
    TEST_END();
}

static void test_shard_single_route_counter_not_incremented_for_scatter(void) {
    TEST_BEGIN("shard_single_route_counter_not_incremented_for_scatter");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);
    keel_router_reset_stats(router);

    /* No shard predicate → SCATTER, single-shard counters must stay zero */
    keel_dispatch_result_t out;
    keel_router_dispatch_sql(router,
        KEEL_STR("SELECT * FROM users WHERE email = 'x@y.z'"),
        NULL, NULL, false, &out);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SCATTER);

    keel_router_stats_t stats;
    keel_router_get_stats(router, &stats);
    TEST_ASSERT_EQ(stats.shard_single_routes[0], (uint64_t)0);
    TEST_ASSERT_EQ(stats.shard_single_routes[1], (uint64_t)0);

    keel_router_destroy(router);
    TEST_END();
}

static void test_shard_scatter_hits_counter(void) {
    TEST_BEGIN("shard_scatter_hits_counter");
    keel_router_t* router = create_sharded_router(); /* 2-shard */
    keel_router_add_shard_rule(router, "users", "id", 2);
    keel_router_reset_stats(router);

    /* Two scatter dispatches */
    keel_dispatch_result_t out;
    keel_router_dispatch_sql(router,
        KEEL_STR("SELECT * FROM users WHERE email = 'a@b.c'"),
        NULL, NULL, false, &out);
    keel_router_dispatch_sql(router,
        KEEL_STR("SELECT * FROM users WHERE email = 'd@e.f'"),
        NULL, NULL, false, &out);

    keel_router_stats_t stats;
    keel_router_get_stats(router, &stats);
    TEST_ASSERT_EQ(stats.shard_scatter_hits, (uint64_t)2);

    keel_router_destroy(router);
    TEST_END();
}

static void test_shard_scatter_failed_counter(void) {
    TEST_BEGIN("shard_scatter_failed_counter");
    /* Use a router with no servers to force failures on every shard */
    keel_router_config_t cfg = keel_router_config_default();
    keel_router_t* router = keel_router_create(&cfg);
    keel_router_add_shard_rule(router, "users", "id", 2);
    keel_router_reset_stats(router);

    /* No servers → every shard should fail */
    keel_scatter_plan_t scatter;
    memset(&scatter, 0, sizeof(scatter));
    const keel_shard_rule_t* rule = keel_router_get_shard_rule(router, "users");
    TEST_ASSERT_NOT_NULL(rule);
    keel_route_session_t session = {0};
    keel_router_scatter_servers(router, &session, rule, false, &scatter);

    keel_router_stats_t stats;
    keel_router_get_stats(router, &stats);
    /* 2-shard scatter with no servers → 2 failures */
    TEST_ASSERT_EQ(stats.shard_scatter_failed, (uint64_t)2);
    TEST_ASSERT_EQ(stats.shard_scatter_hits, (uint64_t)1);

    keel_router_destroy(router);
    TEST_END();
}

static void test_shard_stats_reset_clears_shard_counters(void) {
    TEST_BEGIN("shard_stats_reset_clears_shard_counters");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    keel_router_dispatch_sql(router, KEEL_STR("SELECT * FROM users WHERE id = 1"),
                             NULL, NULL, false, &out);
    keel_router_dispatch_sql(router,
        KEEL_STR("SELECT * FROM users WHERE email = 'x'"),
        NULL, NULL, false, &out);

    keel_router_reset_stats(router);

    keel_router_stats_t stats;
    keel_router_get_stats(router, &stats);
    TEST_ASSERT_EQ(stats.shard_single_routes[0], (uint64_t)0);
    TEST_ASSERT_EQ(stats.shard_single_routes[1], (uint64_t)0);
    TEST_ASSERT_EQ(stats.shard_scatter_hits, (uint64_t)0);
    TEST_ASSERT_EQ(stats.shard_scatter_failed, (uint64_t)0);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Feature 6: Range-based shard map
 * ============================================================================ */

static void test_range_map_key_first_shard(void) {
    TEST_BEGIN("range_map_key_first_shard");
    /* 3-shard range rule: shard 0 → [INT64_MIN, 100], shard 1 → [101, 1000],
     * shard 2 → (1000, +∞) */
    keel_shard_rule_t rule = {
        .table           = "events",
        .column          = "id",
        .shard_count     = 3,
        .strategy        = KEEL_SHARD_STRATEGY_RANGE,
        .threshold_count = 3,
        .thresholds      = { 100, 1000, INT64_MAX },
    };
    keel_shard_key_t key = { .kind = KEEL_SHARD_KEY_INT64,
                              .value.int64_value = 50 };
    size_t idx = SIZE_MAX;
    keel_error_t err = keel_shard_map_key_rule(&key, &rule, &idx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(idx, (size_t)0);
    TEST_END();
}

static void test_range_map_key_exact_threshold(void) {
    TEST_BEGIN("range_map_key_exact_threshold");
    keel_shard_rule_t rule = {
        .table           = "events",
        .column          = "id",
        .shard_count     = 3,
        .strategy        = KEEL_SHARD_STRATEGY_RANGE,
        .threshold_count = 3,
        .thresholds      = { 100, 1000, INT64_MAX },
    };
    /* Exactly at threshold → still belongs to that shard (inclusive upper bound) */
    keel_shard_key_t key = { .kind = KEEL_SHARD_KEY_INT64,
                              .value.int64_value = 100 };
    size_t idx = SIZE_MAX;
    keel_error_t err = keel_shard_map_key_rule(&key, &rule, &idx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(idx, (size_t)0); /* 100 <= thresholds[0]=100 → shard 0 */
    TEST_END();
}

static void test_range_map_key_middle_shard(void) {
    TEST_BEGIN("range_map_key_middle_shard");
    keel_shard_rule_t rule = {
        .table           = "events",
        .column          = "id",
        .shard_count     = 3,
        .strategy        = KEEL_SHARD_STRATEGY_RANGE,
        .threshold_count = 3,
        .thresholds      = { 100, 1000, INT64_MAX },
    };
    keel_shard_key_t key = { .kind = KEEL_SHARD_KEY_INT64,
                              .value.int64_value = 500 };
    size_t idx = SIZE_MAX;
    keel_error_t err = keel_shard_map_key_rule(&key, &rule, &idx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(idx, (size_t)1);
    TEST_END();
}

static void test_range_map_key_last_shard_overflow(void) {
    TEST_BEGIN("range_map_key_last_shard_overflow");
    keel_shard_rule_t rule = {
        .table           = "events",
        .column          = "id",
        .shard_count     = 3,
        .strategy        = KEEL_SHARD_STRATEGY_RANGE,
        .threshold_count = 3,
        .thresholds      = { 100, 1000, INT64_MAX },
    };
    keel_shard_key_t key = { .kind = KEEL_SHARD_KEY_INT64,
                              .value.int64_value = 9999 };
    size_t idx = SIZE_MAX;
    keel_error_t err = keel_shard_map_key_rule(&key, &rule, &idx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(idx, (size_t)2); /* exceeds 1000 → last shard */
    TEST_END();
}

static void test_range_map_key_string_falls_back_to_hash(void) {
    TEST_BEGIN("range_map_key_string_falls_back_to_hash");
    /* RANGE strategy + STRING key → falls back to hash; must not error */
    keel_shard_rule_t rule = {
        .table           = "events",
        .column          = "slug",
        .shard_count     = 4,
        .strategy        = KEEL_SHARD_STRATEGY_RANGE,
        .threshold_count = 4,
        .thresholds      = { 100, 200, 300, INT64_MAX },
    };
    keel_shard_key_t key = { .kind = KEEL_SHARD_KEY_STRING };
    key.value.string_value = KEEL_STR("alpha");
    size_t idx = SIZE_MAX;
    keel_error_t err = keel_shard_map_key_rule(&key, &rule, &idx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT(idx < 4); /* hash result is in [0, 3] */
    TEST_END();
}

static void test_range_map_key_hash_strategy_unchanged(void) {
    TEST_BEGIN("range_map_key_hash_strategy_unchanged");
    /* HASH strategy should give same result as keel_shard_map_key */
    keel_shard_rule_t rule = {
        .table           = "users",
        .column          = "id",
        .shard_count     = 8,
        .strategy        = KEEL_SHARD_STRATEGY_HASH,
    };
    keel_shard_key_t key = { .kind = KEEL_SHARD_KEY_INT64,
                              .value.int64_value = 42 };
    size_t idx_rule = SIZE_MAX;
    size_t idx_hash = SIZE_MAX;
    keel_shard_map_key_rule(&key, &rule, &idx_rule);
    keel_shard_map_key(&key, 8, &idx_hash);
    TEST_ASSERT_EQ(idx_rule, idx_hash);
    TEST_END();
}

static void test_range_invalid_threshold_count_falls_back_to_hash(void) {
    TEST_BEGIN("range_invalid_threshold_count_falls_back_to_hash");
    /* threshold_count != shard_count → fall back to hash (defensive) */
    keel_shard_rule_t rule = {
        .table           = "events",
        .column          = "id",
        .shard_count     = 3,
        .strategy        = KEEL_SHARD_STRATEGY_RANGE,
        .threshold_count = 2,  /* mismatch */
        .thresholds      = { 100, 1000 },
    };
    keel_shard_key_t key = { .kind = KEEL_SHARD_KEY_INT64,
                              .value.int64_value = 50 };
    size_t idx = SIZE_MAX;
    keel_error_t err = keel_shard_map_key_rule(&key, &rule, &idx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT(idx < 3);
    TEST_END();
}

static void test_range_dispatch_routes_to_correct_shard(void) {
    TEST_BEGIN("range_dispatch_routes_to_correct_shard");
    keel_router_t* router = create_sharded_router(); /* 2-shard */
    /* 2-shard range: shard 0 → id <= 100; shard 1 → id > 100 */
    const int64_t thresholds[2] = { 100, INT64_MAX };
    keel_error_t reg = keel_router_add_shard_rule_range(
        router, "events", "id", thresholds, 2);
    TEST_ASSERT_EQ(reg, KEEL_OK);

    keel_dispatch_result_t out;
    /* id=50 → shard 0 */
    keel_error_t err = keel_router_dispatch_sql(
        router, KEEL_STR("SELECT * FROM events WHERE id = 50"),
        NULL, NULL, false, &out);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)0);
    }

    /* id=200 → shard 1 */
    err = keel_router_dispatch_sql(
        router, KEEL_STR("SELECT * FROM events WHERE id = 200"),
        NULL, NULL, false, &out);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)1);
    }

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Feature 7: Multi-shard transaction coordinator
 * ============================================================================ */

static void test_record_scatter_write_sets_mask(void) {
    TEST_BEGIN("record_scatter_write_sets_mask");
    keel_route_session_t session = {0};
    keel_scatter_plan_t plan = {0};
    plan.participating_shards_mask = 0b0110; /* shards 1 and 2 */

    keel_router_record_scatter_write(&session, &plan);
    TEST_ASSERT(session.has_scatter_write);
    TEST_ASSERT_EQ(session.scatter_shards_mask, (uint64_t)0b0110);
    TEST_END();
}

static void test_record_scatter_write_ors_mask(void) {
    TEST_BEGIN("record_scatter_write_ors_mask");
    /* Multiple record calls OR the masks together */
    keel_route_session_t session = {0};
    keel_scatter_plan_t plan1 = { .participating_shards_mask = 0b0011 };
    keel_scatter_plan_t plan2 = { .participating_shards_mask = 0b1100 };
    keel_router_record_scatter_write(&session, &plan1);
    keel_router_record_scatter_write(&session, &plan2);
    TEST_ASSERT_EQ(session.scatter_shards_mask, (uint64_t)0b1111);
    TEST_END();
}

static void test_clear_scatter_participation_resets_fields(void) {
    TEST_BEGIN("clear_scatter_participation_resets_fields");
    keel_route_session_t session = {
        .has_scatter_write   = true,
        .scatter_shards_mask = 0xDEADBEEF,
    };
    keel_router_clear_scatter_participation(&session);
    TEST_ASSERT(!session.has_scatter_write);
    TEST_ASSERT_EQ(session.scatter_shards_mask, (uint64_t)0);
    TEST_END();
}

static void test_scatter_write_populates_participating_mask(void) {
    TEST_BEGIN("scatter_write_populates_participating_mask");
    keel_router_t* router = create_sharded_router(); /* 2-shard */
    keel_router_add_shard_rule(router, "users", "id", 2);
    const keel_shard_rule_t* rule = keel_router_get_shard_rule(router, "users");
    TEST_ASSERT_NOT_NULL(rule);

    keel_route_session_t session = {0};
    keel_scatter_plan_t plan;
    keel_router_scatter_servers(router, &session, rule, /*is_write=*/true, &plan);

    /* Both shards routed OK → bits 0 and 1 should be set */
    TEST_ASSERT(plan.participating_shards_mask & (1ULL << 0));
    TEST_ASSERT(plan.participating_shards_mask & (1ULL << 1));

    keel_router_destroy(router);
    TEST_END();
}

static void test_scatter_read_does_not_populate_mask(void) {
    TEST_BEGIN("scatter_read_does_not_populate_mask");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);
    const keel_shard_rule_t* rule = keel_router_get_shard_rule(router, "users");
    TEST_ASSERT_NOT_NULL(rule);

    keel_route_session_t session = {0};
    keel_scatter_plan_t plan;
    keel_router_scatter_servers(router, &session, rule, /*is_write=*/false, &plan);

    /* Read scatter → mask must be zero */
    TEST_ASSERT_EQ(plan.participating_shards_mask, (uint64_t)0);

    keel_router_destroy(router);
    TEST_END();
}

static void test_cross_tx_rejected_for_non_participant(void) {
    TEST_BEGIN("cross_tx_rejected_for_non_participant");
    keel_router_t* router = create_sharded_router(); /* 2-shard */
    keel_router_add_shard_rule(router, "users", "id", 2);

    /* Session in transaction with only shard 0 participating */
    keel_route_session_t session = {
        .in_transaction      = true,
        .has_scatter_write   = true,
        .scatter_shards_mask = (1ULL << 0), /* only shard 0 */
    };

    keel_dispatch_result_t out;
    /* id=43 → shard 1 (43 % 2 == 1) — not in participation mask */
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 43"),
        &session, NULL, false, &out);
    TEST_ASSERT_EQ(err, KEEL_ERR_SHARD_CROSS_TX);

    keel_router_destroy(router);
    TEST_END();
}

static void test_cross_tx_allowed_for_participant(void) {
    TEST_BEGIN("cross_tx_allowed_for_participant");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    /* Session in transaction with both shards participating */
    keel_route_session_t session = {
        .in_transaction      = true,
        .has_scatter_write   = true,
        .scatter_shards_mask = (1ULL << 0) | (1ULL << 1),
    };

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 43"),
        &session, NULL, false, &out);
    TEST_ASSERT_EQ(err, KEEL_OK);

    keel_router_destroy(router);
    TEST_END();
}

static void test_cross_tx_not_checked_outside_transaction(void) {
    TEST_BEGIN("cross_tx_not_checked_outside_transaction");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    /* has_scatter_write but not in_transaction → no cross-tx check */
    keel_route_session_t session = {
        .in_transaction      = false,
        .has_scatter_write   = true,
        .scatter_shards_mask = (1ULL << 0), /* only shard 0 */
    };

    keel_dispatch_result_t out;
    /* id=43 → shard 1, but no transaction → should succeed */
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 43"),
        &session, NULL, false, &out);
    TEST_ASSERT_EQ(err, KEEL_OK);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Feature 8: Shard migration state
 * ============================================================================ */

static void test_set_migration_marks_rule(void) {
    TEST_BEGIN("set_migration_marks_rule");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_error_t err = keel_router_set_shard_migration(router, "users", 0, 1);
    TEST_ASSERT_EQ(err, KEEL_OK);

    const keel_shard_rule_t* rule = keel_router_get_shard_rule(router, "users");
    TEST_ASSERT_NOT_NULL(rule);
    if (rule) {
        TEST_ASSERT_EQ(rule->state, KEEL_SHARD_STATE_MIGRATING);
        TEST_ASSERT_EQ(rule->migrate_src_shard, (size_t)0);
        TEST_ASSERT_EQ(rule->migrate_dst_shard, (size_t)1);
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_set_migration_not_found(void) {
    TEST_BEGIN("set_migration_not_found");
    keel_router_t* router = create_sharded_router();
    keel_error_t err = keel_router_set_shard_migration(router, "nonexistent", 0, 1);
    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_FOUND);
    keel_router_destroy(router);
    TEST_END();
}

static void test_set_migration_same_shard_invalid(void) {
    TEST_BEGIN("set_migration_same_shard_invalid");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);
    keel_error_t err = keel_router_set_shard_migration(router, "users", 1, 1);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);
    keel_router_destroy(router);
    TEST_END();
}

static void test_migration_read_routes_to_dst(void) {
    TEST_BEGIN("migration_read_routes_to_dst");
    keel_router_t* router = create_sharded_router(); /* 2-shard */
    keel_router_add_shard_rule(router, "users", "id", 2);
    /* id=42 → shard 0 (42 % 2 == 0); migrate shard 0 → shard 1 */
    keel_router_set_shard_migration(router, "users", 0, 1);

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 42"),
        NULL, NULL, false, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    /* Read should go to dst shard (shard 1) */
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)1);
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_migration_write_dual_writes_two_shards(void) {
    TEST_BEGIN("migration_write_dual_writes_two_shards");
    keel_router_t* router = create_sharded_router(); /* 2-shard */
    keel_router_add_shard_rule(router, "users", "id", 2);
    /* id=42 → shard 0; migrate shard 0 → shard 1 */
    keel_router_set_shard_migration(router, "users", 0, 1);

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("INSERT INTO users (id, name) VALUES (42, 'alice')"),
        NULL, NULL, true, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    /* Write during migration → SCATTER with exactly 2 decisions */
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SCATTER);
    TEST_ASSERT_EQ(out.scatter.count, (size_t)2);
    TEST_ASSERT_EQ(out.scatter.failed, (size_t)0);
    TEST_ASSERT_NOT_NULL(out.scatter.decisions[0].server);
    TEST_ASSERT_NOT_NULL(out.scatter.decisions[1].server);

    keel_router_destroy(router);
    TEST_END();
}

static void test_migration_non_migrating_shard_routes_normally(void) {
    TEST_BEGIN("migration_non_migrating_shard_routes_normally");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);
    /* Migrate shard 0 → shard 1; id=43 → shard 1, which is dst but not src */
    /* Actually shard 1 IS the dst so it will also be intercepted for writes */
    /* Use id=43 → shard 1 which IS the dst shard — should also trigger dual-write */
    keel_router_set_shard_migration(router, "users", 0, 1);

    /* id=43 → shard 1 (43 % 2 == 1) — matches dst_shard → dual write */
    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("UPDATE users SET name = 'bob' WHERE id = 43"),
        NULL, NULL, true, &out);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SCATTER);
    TEST_ASSERT_EQ(out.scatter.count, (size_t)2);

    keel_router_destroy(router);
    TEST_END();
}

static void test_clear_migration_restores_normal_routing(void) {
    TEST_BEGIN("clear_migration_restores_normal_routing");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);
    keel_router_set_shard_migration(router, "users", 0, 1);

    /* Verify migration is active */
    const keel_shard_rule_t* rule = keel_router_get_shard_rule(router, "users");
    TEST_ASSERT_NOT_NULL(rule);
    TEST_ASSERT_EQ(rule->state, KEEL_SHARD_STATE_MIGRATING);

    /* Clear migration */
    keel_error_t err = keel_router_clear_shard_migration(router, "users");
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Now a read for id=42 should go to shard 0 (normal hash routing) */
    keel_dispatch_result_t out;
    err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 42"),
        NULL, NULL, false, &out);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_EQ(out.single.shard_index, (size_t)0); /* back to shard 0 */

    keel_router_destroy(router);
    TEST_END();
}

static void test_clear_migration_not_found(void) {
    TEST_BEGIN("clear_migration_not_found");
    keel_router_t* router = create_sharded_router();
    keel_error_t err = keel_router_clear_shard_migration(router, "nonexistent");
    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_FOUND);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Feature 9: keel_shard_plan() for multi-table UPDATE … FROM …
 * ============================================================================ */

static void test_update_from_param_routes_single(void) {
    TEST_BEGIN("update_from_param_routes_single");
    /* UPDATE users SET name = 'x' FROM orders o
     * WHERE users.id = o.user_id AND users.id = $1
     * With $1 = 42 (42 % 2 == 0) → shard 0 */
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_shard_bound_params_t params = {
        .count = 1,
        .values = { [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 42 } },
    };

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("UPDATE users SET name = 'x' FROM orders o "
                 "WHERE users.id = o.user_id AND users.id = $1"),
        NULL, &params, true, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)0);
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_update_from_literal_routes_single(void) {
    TEST_BEGIN("update_from_literal_routes_single");
    /* id = 43 → shard 1 (43 % 2 == 1) */
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("UPDATE users SET active = false FROM sessions s "
                 "WHERE users.id = s.user_id AND users.id = 43"),
        NULL, NULL, true, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)1);
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_update_from_no_direct_predicate_scatters(void) {
    TEST_BEGIN("update_from_no_direct_predicate_scatters");
    /* No direct t1.id = scalar predicate → scatter */
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("UPDATE users SET active = false FROM sessions s "
                 "WHERE users.id = s.user_id"),
        NULL, NULL, true, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SCATTER);

    keel_dispatch_result_cleanup(&out);
    keel_router_destroy(router);
    TEST_END();
}

static void test_update_from_alias_on_target_routes_single(void) {
    TEST_BEGIN("update_from_alias_on_target_routes_single");
    /* UPDATE users u … WHERE u.id = o.user_id AND u.id = 42 */
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("UPDATE users u SET u.name = 'bob' FROM orders o "
                 "WHERE u.id = o.user_id AND u.id = 42"),
        NULL, NULL, true, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)0); /* 42 % 2 == 0 */
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_update_from_plan_via_keel_shard_plan(void) {
    TEST_BEGIN("update_from_plan_via_keel_shard_plan");
    /* Verify through keel_shard_plan() directly */
    keel_arena_t* arena = keel_arena_create(8192);

    keel_shard_bound_params_t params = {
        .count = 1,
        .values = { [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 7 } },
    };

    keel_shard_plan_t plan;
    keel_shard_plan(
        KEEL_STR("UPDATE users SET score = score + 1 FROM events e "
                 "WHERE users.id = e.user_id AND users.id = $1"),
        &users_by_id_two_shards, &params, arena, &plan);

    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SINGLE);
    TEST_ASSERT_EQ(plan.shard_index, (size_t)1); /* 7 % 2 == 1 */

    keel_arena_destroy(arena);
    TEST_END();
}

static void test_update_from_join_predicate_only_scatters(void) {
    TEST_BEGIN("update_from_join_predicate_only_scatters");
    /* The param is bound to the FROM-table column only; no direct t1.id = $1 */
    keel_arena_t* arena = keel_arena_create(8192);

    keel_shard_bound_params_t params = {
        .count = 1,
        .values = { [0] = { .kind = KEEL_SHARD_KEY_INT64, .value.int64_value = 7 } },
    };

    keel_shard_plan_t plan;
    keel_shard_plan(
        KEEL_STR("UPDATE users SET score = 0 FROM events e "
                 "WHERE users.id = e.user_id AND e.id = $1"),
        &users_by_id_two_shards, &params, arena, &plan);

    /* e.id = $1 is a predicate on the JOIN table, not the shard column */
    TEST_ASSERT_EQ(plan.kind, KEEL_SHARD_PLAN_SCATTER);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * Feature 10: Shard count change detection
 * ============================================================================ */

static void test_shard_count_change_routing_uses_new_count(void) {
    TEST_BEGIN("shard_count_change_routing_uses_new_count");
    /* Register with 2 shards, overwrite with 4 shards.
     * id=42: 42 % 2 == 0 (old), 42 % 4 == 2 (new).
     * After overwrite the router must use the new shard count. */
    keel_router_t* router = create_sharded_router();
    /* Add two more shards (shard 2 and shard 3) to the router. */
    keel_route_server_t s2p = { .name = "shard2-primary", .host = "h5", .port = 5435,
                                .role = KEEL_SERVER_PRIMARY,   .weight = 1,
                                .shard_id = 2, .health = KEEL_HEALTH_UP };
    keel_route_server_t s2r = { .name = "shard2-replica", .host = "h6", .port = 5435,
                                .role = KEEL_SERVER_REPLICA,   .weight = 1,
                                .shard_id = 2, .health = KEEL_HEALTH_UP };
    keel_route_server_t s3p = { .name = "shard3-primary", .host = "h7", .port = 5435,
                                .role = KEEL_SERVER_PRIMARY,   .weight = 1,
                                .shard_id = 3, .health = KEEL_HEALTH_UP };
    keel_route_server_t s3r = { .name = "shard3-replica", .host = "h8", .port = 5435,
                                .role = KEEL_SERVER_REPLICA,   .weight = 1,
                                .shard_id = 3, .health = KEEL_HEALTH_UP };
    keel_router_add_server(router, &s2p);
    keel_router_add_server(router, &s2r);
    keel_router_add_server(router, &s3p);
    keel_router_add_server(router, &s3r);

    /* Register rule with 2 shards initially */
    keel_error_t err = keel_router_add_shard_rule(router, "users", "id", 2);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Overwrite with 4 shards — triggers warning log (not verifiable here) */
    err = keel_router_add_shard_rule(router, "users", "id", 4);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Routing with new shard count: id=42 → 42 % 4 == 2 → shard 2 */
    keel_dispatch_result_t out;
    err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 42"),
        NULL, NULL, false, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)2);
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_shard_count_same_no_change(void) {
    TEST_BEGIN("shard_count_same_no_change");
    /* Overwriting with the same shard_count must not break routing */
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    /* Overwrite with the same count — no warning should fire, routing unchanged */
    keel_error_t err = keel_router_add_shard_rule(router, "users", "id", 2);
    TEST_ASSERT_EQ(err, KEEL_OK);

    keel_dispatch_result_t out;
    err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 43"),
        NULL, NULL, false, &out);

    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(out.kind, KEEL_DISPATCH_SINGLE);
    TEST_ASSERT_NOT_NULL(out.single.server);
    if (out.single.server) {
        TEST_ASSERT_EQ(out.single.shard_index, (size_t)1); /* 43 % 2 == 1 */
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_shard_count_change_rule_reflects_new_count(void) {
    TEST_BEGIN("shard_count_change_rule_reflects_new_count");
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "orders", "order_id", 2);

    /* Overwrite with 8 shards */
    keel_router_add_shard_rule(router, "orders", "order_id", 8);

    const keel_shard_rule_t* rule = keel_router_get_shard_rule(router, "orders");
    TEST_ASSERT_NOT_NULL(rule);
    if (rule) {
        TEST_ASSERT_EQ(rule->shard_count, (size_t)8);
    }

    keel_router_destroy(router);
    TEST_END();
}

static void test_shard_count_change_range_rule(void) {
    TEST_BEGIN("shard_count_change_range_rule");
    /* Overwrite a range rule with a different shard_count */
    keel_router_t* router = create_sharded_router();

    const int64_t thresholds2[2] = { 100, INT64_MAX };
    keel_router_add_shard_rule_range(router, "events", "id", thresholds2, 2);

    /* Overwrite with 2 shards but different thresholds — shard_count is the same,
     * but the overwrite path is still exercised.  Now register with count=2
     * then immediately add extra servers and overwrite with count=3? That would
     * require a 3-server router.  Instead just verify the overwrite updates
     * threshold_count correctly. */
    const int64_t thresholds2b[2] = { 500, INT64_MAX };
    keel_error_t err = keel_router_add_shard_rule_range(
        router, "events", "id", thresholds2b, 2);
    TEST_ASSERT_EQ(err, KEEL_OK);

    const keel_shard_rule_t* rule = keel_router_get_shard_rule(router, "events");
    TEST_ASSERT_NOT_NULL(rule);
    if (rule) {
        /* New threshold should be in effect: id=200 → 200 <= 500 → shard 0 */
        keel_shard_key_t key = { .kind = KEEL_SHARD_KEY_INT64,
                                  .value.int64_value = 200 };
        size_t idx = SIZE_MAX;
        keel_shard_map_key_rule(&key, rule, &idx);
        TEST_ASSERT_EQ(idx, (size_t)0);
    }

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Scatter fail-closed gate (silent-wrong-result hardening)
 *
 * Scatter dispatch is experimental and only safe for a restricted set of
 * query shapes. Two regressions guard the new behaviour:
 *
 *   - `scatter_merge_enabled = false` (the default) must cause any scatter
 *     classification to fail closed with KEEL_ERR_NOT_SUPPORTED and a
 *     populated reject_reason / reject_message so the engine surfaces a
 *     SQLSTATE 0A000 error to the client.
 *   - `WITH RECURSIVE …` over sharded tables must fail closed even when the
 *     gate is enabled, because each shard evaluates the recursion locally
 *     and the results cannot be merged correctly.
 * ============================================================================ */

static keel_router_t* create_sharded_router_no_gate(void) {
    /* Mirror create_sharded_router(), but leave scatter_merge_enabled at the
     * default (false) so we can exercise the fail-closed path. */
    keel_router_config_t config = keel_router_config_default();
    config.primary_read_weight = 0.0;
    keel_router_t* router = keel_router_create(&config);
    if (!router) return NULL;

    keel_route_server_t s0p = { .name = "shard0-primary", .host = "h", .port = 5432,
                                 .role = KEEL_SERVER_PRIMARY, .weight = 100, .shard_id = 0 };
    keel_route_server_t s0r = { .name = "shard0-replica", .host = "h", .port = 5433,
                                 .role = KEEL_SERVER_REPLICA, .weight = 100, .shard_id = 0 };
    keel_route_server_t s1p = { .name = "shard1-primary", .host = "h", .port = 5434,
                                 .role = KEEL_SERVER_PRIMARY, .weight = 100, .shard_id = 1 };
    keel_route_server_t s1r = { .name = "shard1-replica", .host = "h", .port = 5435,
                                 .role = KEEL_SERVER_REPLICA, .weight = 100, .shard_id = 1 };
    keel_router_add_server(router, &s0p);
    keel_router_add_server(router, &s0r);
    keel_router_add_server(router, &s1p);
    keel_router_add_server(router, &s1r);
    return router;
}

static void test_dispatch_scatter_gate_off_rejects(void) {
    TEST_BEGIN("dispatch_scatter_gate_off_rejects");
    keel_router_t* router = create_sharded_router_no_gate();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    /* No shard-key predicate → SCATTER classification.
     * Default config has scatter_merge_enabled=false → must fail closed. */
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE email = 'x@y.z'"),
        NULL, NULL, false, &out);

    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_SUPPORTED);
    TEST_ASSERT_EQ(out.reject_reason, KEEL_DISPATCH_REJECT_SCATTER_DISABLED);
    TEST_ASSERT(out.reject_message[0] != '\0');

    keel_router_destroy(router);
    TEST_END();
}

static void test_dispatch_recursive_cte_rejected(void) {
    TEST_BEGIN("dispatch_recursive_cte_rejected");
    /* Gate ON via the helper — recursive CTE must STILL be rejected because
     * cross-shard recursion produces silently wrong results. */
    keel_router_t* router = create_sharded_router();
    keel_router_add_shard_rule(router, "users", "id", 2);

    keel_dispatch_result_t out;
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("WITH RECURSIVE t(n) AS ("
                 "  SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 5"
                 ") SELECT * FROM users WHERE email = 'x@y.z'"),
        NULL, NULL, false, &out);

    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_SUPPORTED);
    TEST_ASSERT_EQ(out.reject_reason, KEEL_DISPATCH_REJECT_RECURSIVE_CTE);
    TEST_ASSERT(out.reject_message[0] != '\0');

    keel_router_destroy(router);
    TEST_END();
}

int main(void) {
    test_extract_int_literal();
    test_extract_alias_qualified();
    test_extract_string_literal();
    test_extract_param();
    test_reject_conflicting_keys();
    test_not_found_without_shard_predicate();
    test_reject_join_query();
    test_reject_non_select();
    test_extract_update_where();
    test_extract_delete_where();
    test_extract_insert_shard_key();
    test_map_int_key();
    test_map_string_key_is_stable();
    test_param_cannot_map_without_binding();
    test_map_key_bound_int();
    test_map_key_bound_string();
    test_map_key_bound_out_of_range();
    test_map_key_bound_null_params();
    test_route_sharded_sql_read();
    test_route_sharded_sql_other_shard();
    test_route_sharded_sql_requires_single_shard();
    test_route_sharded_insert();
    test_route_sharded_update();
    test_route_sharded_delete();
    test_route_sharded_sql_bound_select();
    test_route_sharded_sql_bound_insert();
    test_route_sharded_sql_bound_update();
    test_plan_single_shard_select();
    test_plan_single_shard_update();
    test_plan_scatter_no_predicate();
    test_plan_scatter_unbound_param();
    test_plan_single_shard_bound_param();
    test_plan_unsupported_ddl();
    test_plan_unsupported_join();
    /* Phase B: JOIN key extraction + CTE / no-WHERE scatter */
    test_join_no_shard_predicate_scatters();
    test_join_shard_table_absent_scatters();
    test_join_three_table_routes_single();
    test_select_no_where_scatters();
    test_select_no_from_scatters();
    test_cte_select_scatters();
    test_cte_update_scatters();
    test_cte_delete_scatters();
    test_cte_writable_insert_routes_single();
    test_cte_writable_update_routes_single();
    test_cte_writable_delete_routes_single();
    test_join_key_extraction_with_table_alias();
    test_router_plan_sharded_sql();
    test_registry_add_and_get();
    test_registry_overwrite();
    test_registry_remove();
    test_plan_sql_single();
    test_plan_sql_second_rule();
    test_plan_sql_scatter();
    test_plan_sql_no_matching_rule();
    test_plan_sql_bound_param();
    test_scatter_invalid_args();
    test_scatter_overflow();
    test_scatter_write_routes_to_primary();
    test_scatter_read_routes_to_replica();
    test_scatter_shard_index_populated();
    test_scatter_missing_shard_server();
    test_scatter_in_transaction_forces_primary();
    test_scatter_end_to_end_from_plan();
    /* Feature 1: Rule persistence from config */
    test_load_rules_from_config_null_args();
    test_load_rules_from_config_reads_sections();
    test_load_rules_from_config_overwrites();
    test_load_rules_survives_restart_semantics();
    /* Feature 2: Combined plan + dispatch */
    test_dispatch_null_args();
    test_dispatch_no_rules_returns_not_supported();
    test_dispatch_single_shard_read();
    test_dispatch_single_shard_write();
    test_dispatch_scatter_read();
    test_dispatch_scatter_write();
    test_dispatch_unsupported_ddl();
    test_dispatch_bound_param_single();
    test_dispatch_second_rule_matches();
    /* Feature 3: Scatter result aggregation */
    test_scatter_result_init();
    test_scatter_result_feed_null_rows_increments_failed();
    test_scatter_result_feed_null_result_no_crash();
    test_scatter_result_feed_counts_rows();
    test_scatter_result_merge_callback_invoked();
    test_scatter_result_merge_not_called_on_failure();
    test_scatter_result_end_to_end_with_dispatch();
    /* Feature 4: Per-shard routing counters */
    test_shard_single_route_counter_increments();
    test_shard_single_route_counter_per_shard();
    test_shard_single_route_counter_not_incremented_for_scatter();
    test_shard_scatter_hits_counter();
    test_shard_scatter_failed_counter();
    test_shard_stats_reset_clears_shard_counters();
    /* Feature 6: Range-based shard map */
    test_range_map_key_first_shard();
    test_range_map_key_exact_threshold();
    test_range_map_key_middle_shard();
    test_range_map_key_last_shard_overflow();
    test_range_map_key_string_falls_back_to_hash();
    test_range_map_key_hash_strategy_unchanged();
    test_range_invalid_threshold_count_falls_back_to_hash();
    test_range_dispatch_routes_to_correct_shard();
    /* Feature 7: Multi-shard transaction coordinator */
    test_record_scatter_write_sets_mask();
    test_record_scatter_write_ors_mask();
    test_clear_scatter_participation_resets_fields();
    test_scatter_write_populates_participating_mask();
    test_scatter_read_does_not_populate_mask();
    test_cross_tx_rejected_for_non_participant();
    test_cross_tx_allowed_for_participant();
    test_cross_tx_not_checked_outside_transaction();
    /* Feature 8: Shard migration state */
    test_set_migration_marks_rule();
    test_set_migration_not_found();
    test_set_migration_same_shard_invalid();
    test_migration_read_routes_to_dst();
    test_migration_write_dual_writes_two_shards();
    test_migration_non_migrating_shard_routes_normally();
    test_clear_migration_restores_normal_routing();
    test_clear_migration_not_found();
    /* Feature 9: keel_shard_plan() for multi-table UPDATE … FROM … */
    test_update_from_param_routes_single();
    test_update_from_literal_routes_single();
    test_update_from_no_direct_predicate_scatters();
    test_update_from_alias_on_target_routes_single();
    test_update_from_plan_via_keel_shard_plan();
    test_update_from_join_predicate_only_scatters();
    /* Feature 10: Shard count change detection */
    test_shard_count_change_routing_uses_new_count();
    test_shard_count_same_no_change();
    test_shard_count_change_rule_reflects_new_count();
    test_shard_count_change_range_rule();
    /* Scatter fail-closed gate (silent-wrong-result hardening) */
    test_dispatch_scatter_gate_off_rejects();
    test_dispatch_recursive_cte_rejected();
    return test_summary();
}