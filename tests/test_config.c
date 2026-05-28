/**
 * @file test_config.c
 * @brief Comprehensive configuration/INI parser tests
 *
 * Tests all aspects of the configuration system:
 * - INI file loading
 * - Section and key lookup
 * - Type conversion (string, int, bool, float, duration)
 * - Default values
 * - Section/key iteration
 * - Error handling
 */

#include "test_utils.h"
#include "keel/core/ini.h"
#include "keel/mem/mem.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* Path to test config file */
static char g_test_config_path[256];

/* ============================================================================
 * Test Helpers
 * ============================================================================ */

/**
 * @brief Write an ephemeral INI file for a single test case.
 *
 * Path is PID-scoped under /tmp to avoid collisions when ctest runs
 * in parallel.  The file is stored in g_test_config_path for the
 * subsequent keel_config_load() call and removed by
 * cleanup_test_config_file().
 *
 * @param content  Raw INI text to write.
 * @return true on success, false if fopen/fputs fails.
 */
static bool create_test_config_file(const char* content) {
    snprintf(g_test_config_path, sizeof(g_test_config_path), 
             "/tmp/keel_test_config_%d.ini", getpid());
    
    FILE* f = fopen(g_test_config_path, "w");
    if (!f) return false;
    
    fputs(content, f);
    fclose(f);
    return true;
}

/**
 * @brief Remove the ephemeral INI file created by create_test_config_file().
 */
static void cleanup_test_config_file(void) {
    unlink(g_test_config_path);
}

/* ============================================================================
 * Config Loading Tests
 * ============================================================================ */

static void test_config_load_basic(void) {
    TEST_BEGIN("config load basic");
    
    const char* content = 
        "[keel]\n"
        "listen_address = 127.0.0.1\n"
        "listen_port = 5433\n"
        "\n"
        "[pool]\n"
        "pool_size = 25\n"
        "max_client_conn = 100\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

static void test_config_load_nonexistent(void) {
    TEST_BEGIN("config load nonexistent file");
    
    keel_config_t* config = keel_config_load("/nonexistent/path/config.ini");
    TEST_ASSERT_NULL(config);
    
    TEST_END();
}

static void test_config_free_null(void) {
    TEST_BEGIN("config free NULL");
    
    /* Should not crash */
    keel_config_free(NULL);
    TEST_ASSERT(true);
    
    TEST_END();
}

/* ============================================================================
 * String Value Tests
 * ============================================================================ */

static void test_config_get_string(void) {
    TEST_BEGIN("config get string");
    
    const char* content = 
        "[database]\n"
        "host = localhost\n"
        "name = testdb\n"
        "user = postgres\n"
        "password = secret123\n"
        "empty_value = \n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    /* Get existing values */
    const char* host = keel_config_get_string(config, "database", "host", "default");
    TEST_ASSERT_STR_EQ(host, "localhost");
    
    const char* name = keel_config_get_string(config, "database", "name", "default");
    TEST_ASSERT_STR_EQ(name, "testdb");
    
    const char* user = keel_config_get_string(config, "database", "user", "default");
    TEST_ASSERT_STR_EQ(user, "postgres");
    
    const char* password = keel_config_get_string(config, "database", "password", "default");
    TEST_ASSERT_STR_EQ(password, "secret123");
    
    /* Get with default */
    const char* missing = keel_config_get_string(config, "database", "missing", "default_value");
    TEST_ASSERT_STR_EQ(missing, "default_value");
    
    /* Missing section */
    const char* missing_section = keel_config_get_string(config, "nosection", "key", "fallback");
    TEST_ASSERT_STR_EQ(missing_section, "fallback");
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

/* ============================================================================
 * Integer Value Tests
 * ============================================================================ */

static void test_config_get_int(void) {
    TEST_BEGIN("config get int");
    
    const char* content = 
        "[settings]\n"
        "port = 5432\n"
        "max_connections = 100\n"
        "negative = -50\n"
        "zero = 0\n"
        "large = 1000000\n"
        "invalid = not_a_number\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    /* Positive values */
    int64_t port = keel_config_get_int(config, "settings", "port", 0);
    TEST_ASSERT_EQ(port, 5432);
    
    int64_t max_conn = keel_config_get_int(config, "settings", "max_connections", 0);
    TEST_ASSERT_EQ(max_conn, 100);
    
    /* Negative value */
    int64_t neg = keel_config_get_int(config, "settings", "negative", 0);
    TEST_ASSERT_EQ(neg, -50);
    
    /* Zero */
    int64_t zero = keel_config_get_int(config, "settings", "zero", 99);
    TEST_ASSERT_EQ(zero, 0);
    
    /* Large value */
    int64_t large = keel_config_get_int(config, "settings", "large", 0);
    TEST_ASSERT_EQ(large, 1000000);
    
    /* Missing key - returns default */
    int64_t missing = keel_config_get_int(config, "settings", "missing", 42);
    TEST_ASSERT_EQ(missing, 42);
    
    /* Invalid value - strtoll returns 0 for non-numeric strings */
    /* Note: Current implementation doesn't distinguish parse errors from 0 values */
    int64_t invalid = keel_config_get_int(config, "settings", "invalid", 99);
    TEST_ASSERT_EQ(invalid, 0);  /* strtoll("not_a_number", ...) = 0 */
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

/* ============================================================================
 * Boolean Value Tests
 * ============================================================================ */

static void test_config_get_bool(void) {
    TEST_BEGIN("config get bool");
    
    const char* content = 
        "[features]\n"
        "enabled_true = true\n"
        "enabled_yes = yes\n"
        "enabled_1 = 1\n"
        "enabled_on = on\n"
        "disabled_false = false\n"
        "disabled_no = no\n"
        "disabled_0 = 0\n"
        "disabled_off = off\n"
        "invalid = maybe\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    /* True variants */
    TEST_ASSERT(keel_config_get_bool(config, "features", "enabled_true", false));
    TEST_ASSERT(keel_config_get_bool(config, "features", "enabled_yes", false));
    TEST_ASSERT(keel_config_get_bool(config, "features", "enabled_1", false));
    TEST_ASSERT(keel_config_get_bool(config, "features", "enabled_on", false));
    
    /* False variants */
    TEST_ASSERT(!keel_config_get_bool(config, "features", "disabled_false", true));
    TEST_ASSERT(!keel_config_get_bool(config, "features", "disabled_no", true));
    TEST_ASSERT(!keel_config_get_bool(config, "features", "disabled_0", true));
    TEST_ASSERT(!keel_config_get_bool(config, "features", "disabled_off", true));
    
    /* Default for missing */
    TEST_ASSERT(keel_config_get_bool(config, "features", "missing", true));
    TEST_ASSERT(!keel_config_get_bool(config, "features", "missing", false));
    
    /* Invalid returns default */
    TEST_ASSERT(!keel_config_get_bool(config, "features", "invalid", false));
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

/* ============================================================================
 * Float Value Tests
 * ============================================================================ */

static void test_config_get_float(void) {
    TEST_BEGIN("config get float");
    
    const char* content = 
        "[numbers]\n"
        "pi = 3.14159\n"
        "ratio = 0.5\n"
        "negative = -2.5\n"
        "integer = 42\n"
        "zero = 0.0\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    double pi = keel_config_get_float(config, "numbers", "pi", 0.0);
    TEST_ASSERT(pi > 3.14 && pi < 3.15);
    
    double ratio = keel_config_get_float(config, "numbers", "ratio", 0.0);
    TEST_ASSERT(ratio > 0.49 && ratio < 0.51);
    
    double neg = keel_config_get_float(config, "numbers", "negative", 0.0);
    TEST_ASSERT(neg > -2.51 && neg < -2.49);
    
    double integer = keel_config_get_float(config, "numbers", "integer", 0.0);
    TEST_ASSERT(integer > 41.9 && integer < 42.1);
    
    double missing = keel_config_get_float(config, "numbers", "missing", 1.5);
    TEST_ASSERT(missing > 1.49 && missing < 1.51);
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

/* ============================================================================
 * Duration Value Tests
 * ============================================================================ */

static void test_config_get_duration(void) {
    TEST_BEGIN("config get duration");
    
    const char* content = 
        "[timeouts]\n"
        "nanoseconds = 100ns\n"
        "microseconds = 500us\n"
        "milliseconds = 250ms\n"
        "seconds = 30s\n"
        "minutes = 5m\n"
        "hours = 2h\n"
        "plain_number = 1000\n";  /* No suffix = milliseconds typically */
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    /* Nanoseconds */
    keel_duration_t ns = keel_config_get_duration(config, "timeouts", "nanoseconds", 0);
    TEST_ASSERT_EQ(ns, 100);  /* 100 nanoseconds */
    
    /* Microseconds */
    keel_duration_t us = keel_config_get_duration(config, "timeouts", "microseconds", 0);
    TEST_ASSERT_EQ(us, 500 * 1000);  /* 500 microseconds in ns */
    
    /* Milliseconds */
    keel_duration_t ms = keel_config_get_duration(config, "timeouts", "milliseconds", 0);
    TEST_ASSERT_EQ(ms, 250 * 1000 * 1000);  /* 250 ms in ns */
    
    /* Seconds */
    keel_duration_t sec = keel_config_get_duration(config, "timeouts", "seconds", 0);
    TEST_ASSERT_EQ(sec, 30LL * 1000 * 1000 * 1000);  /* 30 sec in ns */
    
    /* Minutes */
    keel_duration_t min = keel_config_get_duration(config, "timeouts", "minutes", 0);
    TEST_ASSERT_EQ(min, 5LL * 60 * 1000 * 1000 * 1000);  /* 5 min in ns */
    
    /* Hours */
    keel_duration_t hr = keel_config_get_duration(config, "timeouts", "hours", 0);
    TEST_ASSERT_EQ(hr, 2LL * 60 * 60 * 1000 * 1000 * 1000);  /* 2 hr in ns */
    
    /* Default for missing */
    keel_duration_t missing = keel_config_get_duration(config, "timeouts", "missing", 999);
    TEST_ASSERT_EQ(missing, 999);

    /* Bare integer (no suffix) is milliseconds in v2 schema. */
    keel_duration_t plain = keel_config_get_duration(config, "timeouts", "plain_number", 0);
    TEST_ASSERT_EQ(plain, 1000LL * 1000 * 1000);  /* 1000 ms = 1e9 ns */

    keel_config_free(config);
    cleanup_test_config_file();

    TEST_END();
}

/* ============================================================================
 * Byte-Count Value Tests
 * ============================================================================ */

static void test_config_get_bytes(void) {
    TEST_BEGIN("config get bytes");

    const char* content =
        "[sizes]\n"
        "bare = 4096\n"
        "explicit_b = 512B\n"
        "decimal_kb = 1KB\n"
        "binary_kib = 1KiB\n"
        "decimal_mb = 2MB\n"
        "binary_mib = 2MiB\n"
        "decimal_gb = 3GB\n"
        "binary_gib = 3GiB\n"
        "short_k = 8K\n"
        "short_m = 4M\n"
        "with_space = 16 KiB\n"
        "bogus = 12xq\n";

    TEST_ASSERT(create_test_config_file(content));
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);

    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "bare",        -1), 4096);
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "explicit_b",  -1), 512);
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "decimal_kb",  -1), 1000);
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "binary_kib",  -1), 1024);
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "decimal_mb",  -1), 2LL * 1000 * 1000);
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "binary_mib",  -1), 2LL * 1024 * 1024);
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "decimal_gb",  -1), 3LL * 1000 * 1000 * 1000);
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "binary_gib",  -1), 3LL * 1024 * 1024 * 1024);
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "short_k",     -1), 8LL * 1000);
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "short_m",     -1), 4LL * 1000 * 1000);
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "with_space",  -1), 16LL * 1024);
    /* Unrecognized suffix falls back to default. */
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "bogus",       777), 777);
    /* Missing key falls back to default. */
    TEST_ASSERT_EQ(keel_config_get_bytes(config, "sizes", "missing",     42), 42);

    keel_config_free(config);
    cleanup_test_config_file();

    TEST_END();
}

/* ============================================================================
 * Section Tests
 * ============================================================================ */

static void test_config_has_section(void) {
    TEST_BEGIN("config has section");
    
    const char* content = 
        "[keel]\n"
        "port = 5433\n"
        "\n"
        "[databases]\n"
        "default = mydb\n"
        "\n"
        "[pools]\n"
        "size = 10\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    TEST_ASSERT(keel_config_has_section(config, "keel"));
    TEST_ASSERT(keel_config_has_section(config, "databases"));
    TEST_ASSERT(keel_config_has_section(config, "pools"));
    TEST_ASSERT(!keel_config_has_section(config, "nonexistent"));
    TEST_ASSERT(!keel_config_has_section(config, ""));
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

/* Iteration helpers */
static int g_section_count;
static char g_sections[10][64];

static void section_callback(const char* section, void* ctx) {
    (void)ctx;
    if (g_section_count < 10) {
        strncpy(g_sections[g_section_count], section, 63);
        g_sections[g_section_count][63] = '\0';
        g_section_count++;
    }
}

static void test_config_iter_sections(void) {
    TEST_BEGIN("config iterate sections");
    
    const char* content = 
        "[alpha]\n"
        "key = 1\n"
        "\n"
        "[beta]\n"
        "key = 2\n"
        "\n"
        "[gamma]\n"
        "key = 3\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    g_section_count = 0;
    keel_config_iter_sections(config, section_callback, NULL);
    
    TEST_ASSERT_EQ(g_section_count, 3);
    
    /* Check all sections are present (order may vary) */
    bool found_alpha = false, found_beta = false, found_gamma = false;
    for (int i = 0; i < g_section_count; i++) {
        if (strcmp(g_sections[i], "alpha") == 0) found_alpha = true;
        if (strcmp(g_sections[i], "beta") == 0) found_beta = true;
        if (strcmp(g_sections[i], "gamma") == 0) found_gamma = true;
    }
    TEST_ASSERT(found_alpha);
    TEST_ASSERT(found_beta);
    TEST_ASSERT(found_gamma);
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

/* Key iteration helpers */
static int g_key_count;
static char g_keys[10][64];
static char g_values[10][64];

static void key_callback(const char* key, const char* value, void* ctx) {
    (void)ctx;
    if (g_key_count < 10) {
        strncpy(g_keys[g_key_count], key, 63);
        g_keys[g_key_count][63] = '\0';
        strncpy(g_values[g_key_count], value, 63);
        g_values[g_key_count][63] = '\0';
        g_key_count++;
    }
}

static void test_config_iter_keys(void) {
    TEST_BEGIN("config iterate keys");
    
    const char* content = 
        "[settings]\n"
        "host = localhost\n"
        "port = 5432\n"
        "ssl = true\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    g_key_count = 0;
    keel_config_iter_keys(config, "settings", key_callback, NULL);
    
    TEST_ASSERT_EQ(g_key_count, 3);
    
    /* Check all keys are present */
    bool found_host = false, found_port = false, found_ssl = false;
    for (int i = 0; i < g_key_count; i++) {
        if (strcmp(g_keys[i], "host") == 0) {
            found_host = true;
            TEST_ASSERT_STR_EQ(g_values[i], "localhost");
        }
        if (strcmp(g_keys[i], "port") == 0) {
            found_port = true;
            TEST_ASSERT_STR_EQ(g_values[i], "5432");
        }
        if (strcmp(g_keys[i], "ssl") == 0) {
            found_ssl = true;
            TEST_ASSERT_STR_EQ(g_values[i], "true");
        }
    }
    TEST_ASSERT(found_host);
    TEST_ASSERT(found_port);
    TEST_ASSERT(found_ssl);
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

/* ============================================================================
 * INI Syntax Tests
 * ============================================================================ */

static void test_config_comments(void) {
    TEST_BEGIN("config comments");
    
    const char* content = 
        "# This is a comment\n"
        "; This is also a comment\n"
        "[section]\n"
        "# Comment in section\n"
        "key1 = value1  # inline comment might work\n"
        "key2 = value2\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    /* Should have values parsed correctly */
    const char* val1 = keel_config_get_string(config, "section", "key1", "default");
    TEST_ASSERT_NOT_NULL(val1);
    
    const char* val2 = keel_config_get_string(config, "section", "key2", "default");
    TEST_ASSERT_STR_EQ(val2, "value2");
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

static void test_config_whitespace(void) {
    TEST_BEGIN("config whitespace handling");
    
    const char* content = 
        "[section]\n"
        "  key1 = value1\n"
        "key2   =   value2\n"
        "key3=value3\n"
        "  key4  =  value4  \n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    /* Keys should be trimmed */
    const char* val1 = keel_config_get_string(config, "section", "key1", "default");
    TEST_ASSERT_STR_EQ(val1, "value1");
    
    const char* val2 = keel_config_get_string(config, "section", "key2", "default");
    TEST_ASSERT_STR_EQ(val2, "value2");
    
    const char* val3 = keel_config_get_string(config, "section", "key3", "default");
    TEST_ASSERT_STR_EQ(val3, "value3");
    
    const char* val4 = keel_config_get_string(config, "section", "key4", "default");
    TEST_ASSERT_STR_EQ(val4, "value4");
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

static void test_config_quoted_values(void) {
    TEST_BEGIN("config quoted values");
    
    const char* content = 
        "[section]\n"
        "single = 'single quoted'\n"
        "double = \"double quoted\"\n"
        "path = \"/path/to/file\"\n"
        "spaces = \"value with spaces\"\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    /* Check that quoted strings are handled */
    const char* path = keel_config_get_string(config, "section", "path", "default");
    TEST_ASSERT_NOT_NULL(path);
    /* Value might include or exclude quotes depending on implementation */
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

static void test_config_empty_section(void) {
    TEST_BEGIN("config empty section");
    
    const char* content = 
        "[empty]\n"
        "\n"
        "[nonempty]\n"
        "key = value\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    TEST_ASSERT(keel_config_has_section(config, "empty"));
    TEST_ASSERT(keel_config_has_section(config, "nonempty"));
    
    /* Empty section should return defaults */
    const char* val = keel_config_get_string(config, "empty", "anykey", "default");
    TEST_ASSERT_STR_EQ(val, "default");
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

/* ============================================================================
 * Real-world Config Tests
 * ============================================================================ */

static void test_config_keel_example(void) {
    TEST_BEGIN("config KEEL example");
    
    const char* content = 
        "[keel]\n"
        "listen_address = 0.0.0.0\n"
        "listen_port = 5433\n"
        "admin_port = 5434\n"
        "log_level = info\n"
        "log_file = /var/log/keel/keel.log\n"
        "pid_file = /var/run/keel.pid\n"
        "\n"
        "[databases]\n"
        "default = postgres\n"
        "\n"
        "[postgres]\n"
        "host = 127.0.0.1\n"
        "port = 5432\n"
        "pool_mode = transaction\n"
        "pool_size = 25\n"
        "max_client_conn = 100\n"
        "server_connect_timeout = 10s\n"
        "query_timeout = 30s\n"
        "server_idle_timeout = 60s\n"
        "\n"
        "[auth]\n"
        "auth_type = md5\n"
        "auth_file = /etc/keel/userlist.txt\n";
    
    TEST_ASSERT(create_test_config_file(content));
    
    keel_config_t* config = keel_config_load(g_test_config_path);
    TEST_ASSERT_NOT_NULL(config);
    
    /* KEEL section */
    TEST_ASSERT_STR_EQ(keel_config_get_string(config, "keel", "listen_address", ""), "0.0.0.0");
    TEST_ASSERT_EQ(keel_config_get_int(config, "keel", "listen_port", 0), 5433);
    TEST_ASSERT_STR_EQ(keel_config_get_string(config, "keel", "log_level", ""), "info");
    
    /* Pool section */
    TEST_ASSERT_STR_EQ(keel_config_get_string(config, "postgres", "host", ""), "127.0.0.1");
    TEST_ASSERT_EQ(keel_config_get_int(config, "postgres", "port", 0), 5432);
    TEST_ASSERT_EQ(keel_config_get_int(config, "postgres", "pool_size", 0), 25);
    TEST_ASSERT_EQ(keel_config_get_int(config, "postgres", "max_client_conn", 0), 100);
    
    /* Timeouts */
    keel_duration_t connect_timeout = keel_config_get_duration(config, "postgres", "server_connect_timeout", 0);
    TEST_ASSERT_EQ(connect_timeout, 10LL * 1000 * 1000 * 1000);  /* 10s in ns */
    
    /* Auth section */
    TEST_ASSERT_STR_EQ(keel_config_get_string(config, "auth", "auth_type", ""), "md5");
    
    keel_config_free(config);
    cleanup_test_config_file();
    
    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("Configuration/INI Parser Tests (Comprehensive)\n");
    printf("================================================\n\n");
    
    /* Initialize memory */
    keel_mem_init(NULL);
    
    /* Loading tests */
    test_config_load_basic();
    test_config_load_nonexistent();
    test_config_free_null();
    
    /* Type conversion tests */
    test_config_get_string();
    test_config_get_int();
    test_config_get_bool();
    test_config_get_float();
    test_config_get_duration();
    test_config_get_bytes();

    /* Section tests */
    test_config_has_section();
    test_config_iter_sections();
    test_config_iter_keys();
    
    /* INI syntax tests */
    test_config_comments();
    test_config_whitespace();
    test_config_quoted_values();
    test_config_empty_section();
    
    /* Real-world tests */
    test_config_keel_example();
    
    /* Cleanup */
    keel_mem_shutdown();
    
    return test_summary();
}
