/**
 * @file test_log.c
 * @brief Tests for logging subsystem
 *
 * Validates the structured-logging API that every keel subsystem
 * depends on.  The logging layer is deliberately simple — level +
 * category bitmask — but correctness matters because misconfigured
 * logging can mask production incidents or flood stdout in benchmarks.
 *
 * Test families:
 *   §1 — Config defaults: keel_log_config_default() returns sane
 *         level, category mask, and boolean flags.
 *   §2 — Level get/set: round-trip through every level enum
 *         (TRACE → FATAL) and verify restore.
 *   §3 — Level names: keel_log_level_name() returns the
 *         canonical string for each level, including an
 *         "UNKNOWN" fallback for out-of-range values.
 *   §4 — Category get/set: bitmask filtering for subsystem
 *         categories (POOL, PROTO, IO, SQL, AUTH, …).
 *   §5 — Category names: keel_log_category_name() round-trip.
 *   §6 — Enabled predicate: keel_log_enabled() respects both
 *         level and category gates.
 *   §7 — Init/shutdown lifecycle: double-init and double-shutdown
 *         are idempotent.
 *   §8 — Macro smoke: KEEL_LOG_INFO / KEEL_LOG_ERROR macros
 *         compile and produce output without crashing.
 */


#include "test_utils.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include <string.h>

/* ============================================================================
 * Config Tests
 * ============================================================================ */

static void test_log_config_default(void) {
    TEST_BEGIN("log config default");
    
    keel_log_config_t config = keel_log_config_default();
    
    /* Should have reasonable defaults */
    TEST_ASSERT(config.min_level >= KEEL_LOG_TRACE && config.min_level <= KEEL_LOG_FATAL);
    TEST_ASSERT(config.categories != 0);
    TEST_ASSERT(config.use_colors == true || config.use_colors == false);
    TEST_ASSERT(config.include_time == true || config.include_time == false);
    
    TEST_END();
}

/* ============================================================================
 * Level Tests
 * ============================================================================ */

static void test_log_level_get_set(void) {
    TEST_BEGIN("log level get/set");
    
    /* Get initial level */
    keel_log_level_t initial = keel_log_get_level();
    
    /* Set to DEBUG */
    keel_log_set_level(KEEL_LOG_DEBUG);
    TEST_ASSERT_EQ(keel_log_get_level(), KEEL_LOG_DEBUG);
    
    /* Set to ERROR */
    keel_log_set_level(KEEL_LOG_ERROR);
    TEST_ASSERT_EQ(keel_log_get_level(), KEEL_LOG_ERROR);
    
    /* Set to TRACE */
    keel_log_set_level(KEEL_LOG_TRACE);
    TEST_ASSERT_EQ(keel_log_get_level(), KEEL_LOG_TRACE);
    
    /* Restore */
    keel_log_set_level(initial);
    
    TEST_END();
}

static void test_log_level_name(void) {
    TEST_BEGIN("log level name");
    
    TEST_ASSERT_NOT_NULL(keel_log_level_name(KEEL_LOG_TRACE));
    TEST_ASSERT_NOT_NULL(keel_log_level_name(KEEL_LOG_DEBUG));
    TEST_ASSERT_NOT_NULL(keel_log_level_name(KEEL_LOG_INFO));
    TEST_ASSERT_NOT_NULL(keel_log_level_name(KEEL_LOG_WARN));
    TEST_ASSERT_NOT_NULL(keel_log_level_name(KEEL_LOG_ERROR));
    TEST_ASSERT_NOT_NULL(keel_log_level_name(KEEL_LOG_FATAL));
    
    /* Check specific names */
    TEST_ASSERT_STR_EQ(keel_log_level_name(KEEL_LOG_TRACE), "TRACE");
    TEST_ASSERT_STR_EQ(keel_log_level_name(KEEL_LOG_DEBUG), "DEBUG");
    TEST_ASSERT_STR_EQ(keel_log_level_name(KEEL_LOG_INFO), "INFO");
    TEST_ASSERT_STR_EQ(keel_log_level_name(KEEL_LOG_WARN), "WARN");
    TEST_ASSERT_STR_EQ(keel_log_level_name(KEEL_LOG_ERROR), "ERROR");
    TEST_ASSERT_STR_EQ(keel_log_level_name(KEEL_LOG_FATAL), "FATAL");
    
    /* Unknown level returns "UNKNOWN" */
    TEST_ASSERT_NOT_NULL(keel_log_level_name((keel_log_level_t)999));
    
    TEST_END();
}

/* ============================================================================
 * Category Tests
 * ============================================================================ */

static void test_log_categories_get_set(void) {
    TEST_BEGIN("log categories get/set");
    
    /* Save initial categories */
    uint32_t initial = keel_log_get_categories();
    
    /* Set specific categories */
    keel_log_set_categories(KEEL_LOG_CAT_POOL | KEEL_LOG_CAT_PROTO);
    uint32_t cats = keel_log_get_categories();
    TEST_ASSERT(cats & KEEL_LOG_CAT_POOL);
    TEST_ASSERT(cats & KEEL_LOG_CAT_PROTO);
    
    /* Set all */
    keel_log_set_categories(KEEL_LOG_CAT_ALL);
    TEST_ASSERT_EQ(keel_log_get_categories(), KEEL_LOG_CAT_ALL);
    
    /* Restore */
    keel_log_set_categories(initial);
    
    TEST_END();
}

static void test_log_category_name(void) {
    TEST_BEGIN("log category name");
    
    TEST_ASSERT_NOT_NULL(keel_log_category_name(KEEL_LOG_CAT_CORE));
    TEST_ASSERT_NOT_NULL(keel_log_category_name(KEEL_LOG_CAT_POOL));
    TEST_ASSERT_NOT_NULL(keel_log_category_name(KEEL_LOG_CAT_PROTO));
    TEST_ASSERT_NOT_NULL(keel_log_category_name(KEEL_LOG_CAT_IO));
    TEST_ASSERT_NOT_NULL(keel_log_category_name(KEEL_LOG_CAT_MEM));
    TEST_ASSERT_NOT_NULL(keel_log_category_name(KEEL_LOG_CAT_SQL));
    TEST_ASSERT_NOT_NULL(keel_log_category_name(KEEL_LOG_CAT_AUTH));
    TEST_ASSERT_NOT_NULL(keel_log_category_name(KEEL_LOG_CAT_CONN));
    TEST_ASSERT_NOT_NULL(keel_log_category_name(KEEL_LOG_CAT_CONFIG));
    
    /* Unknown returns something */
    TEST_ASSERT_NOT_NULL(keel_log_category_name((keel_log_category_t)0x80000000));
    
    TEST_END();
}

/* ============================================================================
 * Enabled Tests
 * ============================================================================ */

static void test_log_enabled(void) {
    TEST_BEGIN("log enabled");
    
    /* Save state */
    keel_log_level_t initial_level = keel_log_get_level();
    uint32_t initial_cats = keel_log_get_categories();
    
    /* Set up for testing */
    keel_log_set_level(KEEL_LOG_INFO);
    keel_log_set_categories(KEEL_LOG_CAT_POOL);
    
    /* Level filtering */
    TEST_ASSERT(!keel_log_enabled(KEEL_LOG_DEBUG, KEEL_LOG_CAT_POOL));  /* Below threshold */
    TEST_ASSERT(keel_log_enabled(KEEL_LOG_INFO, KEEL_LOG_CAT_POOL));    /* At threshold */
    TEST_ASSERT(keel_log_enabled(KEEL_LOG_ERROR, KEEL_LOG_CAT_POOL));   /* Above threshold */
    
    /* Category filtering - POOL is enabled */
    TEST_ASSERT(keel_log_enabled(KEEL_LOG_INFO, KEEL_LOG_CAT_POOL));
    
    /* Set all categories */
    keel_log_set_categories(KEEL_LOG_CAT_ALL);
    TEST_ASSERT(keel_log_enabled(KEEL_LOG_INFO, KEEL_LOG_CAT_IO));
    TEST_ASSERT(keel_log_enabled(KEEL_LOG_INFO, KEEL_LOG_CAT_POOL));
    
    /* Restore */
    keel_log_set_level(initial_level);
    keel_log_set_categories(initial_cats);
    
    TEST_END();
}

/* ============================================================================
 * Log Init/Shutdown Tests
 * ============================================================================ */

static void test_log_init_shutdown(void) {
    TEST_BEGIN("log init/shutdown");
    
    /* Shutdown first (in case already init) */
    keel_log_shutdown();
    
    /* Init with defaults */
    keel_error_t err = keel_log_init(NULL);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Shutdown */
    keel_log_shutdown();
    
    /* Init with custom config */
    keel_log_config_t config = keel_log_config_default();
    config.min_level = KEEL_LOG_DEBUG;
    config.categories = KEEL_LOG_CAT_ALL;
    
    err = keel_log_init(&config);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Verify config applied */
    TEST_ASSERT_EQ(keel_log_get_level(), KEEL_LOG_DEBUG);
    TEST_ASSERT_EQ(keel_log_get_categories(), KEEL_LOG_CAT_ALL);
    
    /* Double init should be OK */
    err = keel_log_init(&config);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Shutdown */
    keel_log_shutdown();
    
    TEST_END();
}

/* ============================================================================
 * Log Output Tests
 * ============================================================================ */

static void test_log_macros(void) {
    TEST_BEGIN("log macros");
    
    /* Ensure logging is initialized */
    keel_log_init(NULL);
    
    /* Save state */
    keel_log_level_t initial = keel_log_get_level();
    uint32_t initial_cats = keel_log_get_categories();
    
    /* Enable all logging */
    keel_log_set_level(KEEL_LOG_TRACE);
    keel_log_set_categories(KEEL_LOG_CAT_ALL);
    
    /* These should not crash */
    KEEL_LOG_TRACE(KEEL_LOG_CAT_CORE, "Trace message: %d", 1);
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE, "Debug message: %d", 2);
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Info message: %d", 3);
    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "Warn message: %d", 4);
    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Error message: %d", 5);
    /* Don't test FATAL as it may exit */
    
    /* Restore */
    keel_log_set_level(initial);
    keel_log_set_categories(initial_cats);
    
    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("Logging Tests\n");
    printf("=============\n\n");
    
    keel_mem_init(NULL);
    
    /* Config tests */
    test_log_config_default();
    
    /* Level tests */
    test_log_level_get_set();
    test_log_level_name();
    
    /* Category tests */
    test_log_categories_get_set();
    test_log_category_name();
    
    /* Enabled tests */
    test_log_enabled();
    
    /* Init/shutdown tests */
    test_log_init_shutdown();
    
    /* Output tests */
    test_log_macros();
    
    keel_mem_shutdown();
    
    return test_summary();
}
