/**
 * @file test_shard_hot_reload.c
 * @brief Unit tests for Feature 13: keel_config_reload_shard_rules().
 *
 * Exercises the hot-reload path by:
 *   1. Loading an initial set of shard rules directly with keel_router_add_shard_rule().
 *   2. Building a synthetic keel_config_t that matches or changes those rules.
 *   3. Calling keel_config_reload_shard_rules() and verifying the result counters
 *      and the router's runtime rule state.
 */

#include "test_utils.h"
#include "keel/core/router.h"
#include "keel/core/config_reload.h"
#include "keel/core/ini.h"
#include "keel_error.h"

#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>

int g_tests_run    = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) {
    return (g_tests_failed == 0) ? 0 : 1;
}

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * Parse an INI-style config string and return a keel_config_t*.
 * The caller must call keel_config_free() when done.
 */
static keel_config_t* parse_ini(const char* text) {
    char path[] = "/tmp/keel_test_XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) return NULL;

    size_t len = strlen(text);
    ssize_t written = write(fd, text, len);
    close(fd);
    if (written != (ssize_t)len) {
        unlink(path);
        return NULL;
    }

    keel_config_t* cfg = keel_config_load(path);
    unlink(path);
    return cfg;
}

static keel_router_t* build_empty_router(void) {
    keel_router_config_t cfg = keel_router_config_default();
    return keel_router_create(&cfg);
}

/* ============================================================================
 * Test: reload adds a new rule
 * ============================================================================ */

static void test_reload_adds_rule(void) {
    TEST_BEGIN("shard_hot_reload_adds_rule");

    keel_router_t* router = build_empty_router();
    TEST_ASSERT_NOT_NULL(router);

    const char* ini =
        "[worker_group.test.shard_rule.orders]\n"
        "column = user_id\n"
        "shard_count = 8\n"
        "strategy = hash\n";

    keel_config_t* cfg = parse_ini(ini);
    TEST_ASSERT_NOT_NULL(cfg);

    keel_reload_result_t result = {0};
    keel_config_reload_shard_rules(cfg, "test", router, &result);

    TEST_ASSERT(result.applied >= 1);
    TEST_ASSERT_EQ(result.errors, 0);

    /* Verify the rule is accessible via dispatch */
    keel_dispatch_result_t out;
    keel_shard_bound_params_t params = {0};
    /* Without servers the dispatch will fail, but the rule must have been loaded */
    keel_error_t err = keel_router_dispatch_sql(
        router,
        KEEL_STR("INSERT INTO orders (user_id) VALUES (1)"),
        NULL, &params, true, &out);
    /* KEEL_ERR_NO_SERVERS or similar is acceptable — rule was loaded */
    TEST_ASSERT(err != KEEL_ERR_NOT_SUPPORTED);   /* rule found */

    keel_config_free(cfg);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: reload with changed shard_count triggers overwrite (Feature 10 warning)
 * ============================================================================ */

static void test_reload_changes_shard_count(void) {
    TEST_BEGIN("shard_hot_reload_changes_shard_count");

    keel_router_t* router = build_empty_router();
    TEST_ASSERT_NOT_NULL(router);

    /* Initial rule: 4 shards */
    keel_router_add_shard_rule(router, "orders", "user_id", 4);

    /* Config with 8 shards for the same table */
    const char* ini =
        "[worker_group.test.shard_rule.orders]\n"
        "column = user_id\n"
        "shard_count = 8\n";

    keel_config_t* cfg = parse_ini(ini);
    TEST_ASSERT_NOT_NULL(cfg);

    keel_reload_result_t result = {0};
    keel_config_reload_shard_rules(cfg, "test", router, &result);

    /* The rule must have been overwritten (applied), not skipped or errored */
    TEST_ASSERT(result.applied >= 1);
    TEST_ASSERT_EQ(result.errors, 0);

    keel_config_free(cfg);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: reload skips sections with missing column/shard_count
 * ============================================================================ */

static void test_reload_skips_invalid_sections(void) {
    TEST_BEGIN("shard_hot_reload_skips_invalid");

    keel_router_t* router = build_empty_router();
    TEST_ASSERT_NOT_NULL(router);

    /* Missing column */
    const char* ini_no_column =
        "[worker_group.test.shard_rule.users]\n"
        "shard_count = 4\n";

    keel_config_t* cfg = parse_ini(ini_no_column);
    TEST_ASSERT_NOT_NULL(cfg);

    keel_reload_result_t result = {0};
    keel_config_reload_shard_rules(cfg, "test", router, &result);

    TEST_ASSERT_EQ(result.applied, 0);
    TEST_ASSERT(result.skipped >= 1);

    keel_config_free(cfg);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: reload with range strategy
 * ============================================================================ */

static void test_reload_range_strategy(void) {
    TEST_BEGIN("shard_hot_reload_range_strategy");

    keel_router_t* router = build_empty_router();
    TEST_ASSERT_NOT_NULL(router);

    const char* ini =
        "[worker_group.test.shard_rule.accounts]\n"
        "column = account_id\n"
        "shard_count = 4\n"
        "strategy = range\n";

    keel_config_t* cfg = parse_ini(ini);
    TEST_ASSERT_NOT_NULL(cfg);

    keel_reload_result_t result = {0};
    keel_config_reload_shard_rules(cfg, "test", router, &result);

    TEST_ASSERT(result.applied >= 1);
    TEST_ASSERT_EQ(result.errors, 0);

    keel_config_free(cfg);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: reload with empty config leaves router unchanged
 * ============================================================================ */

static void test_reload_empty_config(void) {
    TEST_BEGIN("shard_hot_reload_empty_config");

    keel_router_t* router = build_empty_router();
    TEST_ASSERT_NOT_NULL(router);

    keel_router_add_shard_rule(router, "users", "id", 4);

    const char* ini = "[global]\nfoo = bar\n";
    keel_config_t* cfg = parse_ini(ini);
    TEST_ASSERT_NOT_NULL(cfg);

    keel_reload_result_t result = {0};
    keel_config_reload_shard_rules(cfg, "test", router, &result);

    /* No shard_rule.* sections → nothing applied */
    TEST_ASSERT_EQ(result.applied, 0);
    TEST_ASSERT_EQ(result.errors,  0);

    keel_config_free(cfg);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: NULL guards
 * ============================================================================ */

static void test_reload_null_guards(void) {
    TEST_BEGIN("shard_hot_reload_null_guards");

    keel_reload_result_t result = {0};
    /* Must not crash */
    keel_config_reload_shard_rules(NULL, NULL, NULL, NULL);
    keel_config_reload_shard_rules(NULL, NULL, NULL, &result);

    TEST_ASSERT_EQ(result.applied, 0);
    TEST_ASSERT_EQ(result.errors,  0);

    TEST_END();
}

/* ============================================================================
 * Test: reload multiple rules in one config
 * ============================================================================ */

static void test_reload_multiple_rules(void) {
    TEST_BEGIN("shard_hot_reload_multiple_rules");

    keel_router_t* router = build_empty_router();
    TEST_ASSERT_NOT_NULL(router);

    const char* ini =
        "[worker_group.test.shard_rule.users]\n"
        "column = id\n"
        "shard_count = 4\n"
        "\n"
        "[worker_group.test.shard_rule.orders]\n"
        "column = user_id\n"
        "shard_count = 8\n"
        "\n"
        "[worker_group.test.shard_rule.products]\n"
        "column = sku_id\n"
        "shard_count = 2\n";

    keel_config_t* cfg = parse_ini(ini);
    TEST_ASSERT_NOT_NULL(cfg);

    keel_reload_result_t result = {0};
    keel_config_reload_shard_rules(cfg, "test", router, &result);
    TEST_ASSERT_EQ(result.errors, 0);

    keel_config_free(cfg);
    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    test_reload_adds_rule();
    test_reload_changes_shard_count();
    test_reload_skips_invalid_sections();
    test_reload_range_strategy();
    test_reload_empty_config();
    test_reload_null_guards();
    test_reload_multiple_rules();

    printf("\nshard_hot_reload: %d/%d tests passed, %d failed\n",
           g_tests_passed, g_tests_run, g_tests_failed);
    return test_summary();
}
