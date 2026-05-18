/**
 * @file test_result_cache_gate.c
 * @brief Issue 9 — Result Cache Must Remain Disabled: config-layer unit tests.
 *
 * Verifies that:
 *   §1  keel_engine_config_t zero-value has result_cache = false
 *   §2  KEEL_ENGINE_CONFIG_DEFAULT macro produces result_cache = false
 *   §3  INI parser returns false for result_cache when the key is absent
 *   §4  INI parser returns true  when result_cache = on  is present
 *   §5  INI parser returns false when result_cache = off is present
 *   §6  INI parser returns false when result_cache = false is present
 *   §7  Default argument to keel_config_get_bool is respected for absent key
 *
 * The config-rejection path (result_cache=on without experimental_features)
 * is covered at the binary level by the check_result_cache_gate CTest
 * (scripts/check_result_cache_gate.sh) which exercises the full startup
 * validation path via --check-config.
 */

#include "test_utils.h"
#include "keel/core/ini.h"
#include "keel/engine/engine.h"
#include "keel/mem/mem.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static char g_ini_path[256];

/* ============================================================================
 * Helpers
 * ============================================================================ */

static bool write_ini(const char* content)
{
    snprintf(g_ini_path, sizeof(g_ini_path),
             "/tmp/keel_test_result_cache_gate_%d.ini", getpid());
    FILE* f = fopen(g_ini_path, "w");
    if (!f) return false;
    fputs(content, f);
    fclose(f);
    return true;
}

static void cleanup_ini(void) { unlink(g_ini_path); }

/* ============================================================================
 * §1 — keel_engine_config_t zero-init has result_cache = false
 * ============================================================================ */

static void test_engine_config_zero_result_cache_false(void)
{
    TEST_BEGIN("engine config zero-init: result_cache = false");

    keel_engine_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    /* Zero-initialised struct must have result_cache == false (boolean false == 0) */
    TEST_ASSERT(!cfg.result_cache);

    TEST_END();
}

/* ============================================================================
 * §2 — KEEL_ENGINE_CONFIG_DEFAULT macro has .result_cache = false
 * ============================================================================ */

static void test_engine_config_default_result_cache_false(void)
{
    TEST_BEGIN("KEEL_ENGINE_CONFIG_DEFAULT: .result_cache = false");

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    TEST_ASSERT(!cfg.result_cache);

    TEST_END();
}

/* ============================================================================
 * §3 — INI parser returns false when result_cache key is absent
 * ============================================================================ */

static void test_ini_result_cache_absent(void)
{
    TEST_BEGIN("INI: result_cache absent → default false");

    TEST_ASSERT(write_ini(
        "[keel]\n"
        "listen_address = 127.0.0.1\n"
        "\n"
        "[worker_group.default]\n"
        "host = 127.0.0.1\n"
        "port = 5432\n"
    ));

    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    bool rc = keel_config_get_bool(cfg, "worker_group.default", "result_cache", false);
    TEST_ASSERT(!rc);

    keel_config_free(cfg);
    cleanup_ini();

    TEST_END();
}

/* ============================================================================
 * §4 — INI parser returns true when result_cache = on is present
 * ============================================================================ */

static void test_ini_result_cache_on(void)
{
    TEST_BEGIN("INI: result_cache = on → true");

    TEST_ASSERT(write_ini(
        "[keel]\n"
        "listen_address = 127.0.0.1\n"
        "\n"
        "[worker_group.default]\n"
        "host = 127.0.0.1\n"
        "port = 5432\n"
        "result_cache = on\n"
    ));

    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    bool rc = keel_config_get_bool(cfg, "worker_group.default", "result_cache", false);
    TEST_ASSERT(rc);

    keel_config_free(cfg);
    cleanup_ini();

    TEST_END();
}

/* ============================================================================
 * §5 — INI parser returns false when result_cache = off is present
 * ============================================================================ */

static void test_ini_result_cache_off(void)
{
    TEST_BEGIN("INI: result_cache = off → false");

    TEST_ASSERT(write_ini(
        "[keel]\n"
        "listen_address = 127.0.0.1\n"
        "\n"
        "[worker_group.default]\n"
        "host = 127.0.0.1\n"
        "port = 5432\n"
        "result_cache = off\n"
    ));

    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    bool rc = keel_config_get_bool(cfg, "worker_group.default", "result_cache", false);
    TEST_ASSERT(!rc);

    keel_config_free(cfg);
    cleanup_ini();

    TEST_END();
}

/* ============================================================================
 * §6 — INI parser returns false when result_cache = false is present
 * ============================================================================ */

static void test_ini_result_cache_false_explicit(void)
{
    TEST_BEGIN("INI: result_cache = false → false");

    TEST_ASSERT(write_ini(
        "[keel]\n"
        "listen_address = 127.0.0.1\n"
        "\n"
        "[worker_group.default]\n"
        "host = 127.0.0.1\n"
        "port = 5432\n"
        "result_cache = false\n"
    ));

    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    bool rc = keel_config_get_bool(cfg, "worker_group.default", "result_cache", false);
    TEST_ASSERT(!rc);

    keel_config_free(cfg);
    cleanup_ini();

    TEST_END();
}

/* ============================================================================
 * §7 — Default argument to keel_config_get_bool is respected for absent key
 * ============================================================================ */

static void test_ini_result_cache_default_argument(void)
{
    TEST_BEGIN("INI: absent key respects default argument");

    TEST_ASSERT(write_ini(
        "[worker_group.default]\n"
        "host = 127.0.0.1\n"
    ));

    keel_config_t* cfg = keel_config_load(g_ini_path);
    TEST_ASSERT_NOT_NULL(cfg);

    /* Absent key + default=false → false (safe production default) */
    bool rc_false = keel_config_get_bool(cfg, "worker_group.default", "result_cache", false);
    TEST_ASSERT(!rc_false);

    /* Absent key + default=true → true (explicit opt-in path is testable) */
    bool rc_true = keel_config_get_bool(cfg, "worker_group.default", "result_cache", true);
    TEST_ASSERT(rc_true);

    keel_config_free(cfg);
    cleanup_ini();

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void)
{
    printf("test_result_cache_gate: Result Cache Experimental Feature Gate\n");
    printf("===============================================================\n\n");

    keel_mem_init(NULL);

    /* Engine config defaults */
    test_engine_config_zero_result_cache_false();
    test_engine_config_default_result_cache_false();

    /* INI parsing */
    test_ini_result_cache_absent();
    test_ini_result_cache_on();
    test_ini_result_cache_off();
    test_ini_result_cache_false_explicit();
    test_ini_result_cache_default_argument();

    keel_mem_shutdown();

    return test_summary();
}
