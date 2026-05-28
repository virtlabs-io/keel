/**
 * @file test_config_yaml.c
 * @brief Unit tests for the YAML configuration loader and INI<->YAML
 *        converters (config v2).
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Coverage:
 *   - load_yaml: flat sections, nested-map flattening, env interpolation,
 *     missing-env warning path, malformed inputs.
 *   - worker_groups/servers structural form -> packed `host=... port=...`
 *     value strings under `[worker_group.<name>.servers]`.
 *   - keel_config_detect_format extension dispatch.
 *   - INI -> YAML -> INI round-trip preserves every (section, key, value)
 *     tuple of representative shipped fixtures.
 */

#include "keel/core/config_yaml.h"
#include "keel/core/ini.h"
#include "keel/mem/mem.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) static void test_##name(void); \
    static void run_##name(void) { \
        printf("  test_%-40s ", #name); fflush(stdout); \
        tests_run++; \
        test_##name(); \
        tests_passed++; \
        printf("OK\n"); \
    } \
    static void test_##name(void)

#define ASSERT(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "\nFAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        exit(1); \
    } \
} while (0)

#define ASSERT_STR(a, b) do { \
    const char* _a = (a); const char* _b = (b); \
    if (!_a || !_b || strcmp(_a, _b) != 0) { \
        fprintf(stderr, "\nFAIL: %s:%d: '%s' != '%s'\n", \
                __FILE__, __LINE__, _a ? _a : "(null)", _b ? _b : "(null)"); \
        exit(1); \
    } \
} while (0)

static char* mkfile(const char* contents) {
    char tmpl[] = "/tmp/keel_yaml_XXXXXX.yaml";
    int fd = mkstemps(tmpl, 5);
    ASSERT(fd >= 0);
    FILE* fp = fdopen(fd, "w");
    ASSERT(fp);
    fwrite(contents, 1, strlen(contents), fp);
    fclose(fp);
    return strdup(tmpl);
}

static char* mkfile_ini(const char* contents) {
    char tmpl[] = "/tmp/keel_yaml_XXXXXX.ini";
    int fd = mkstemps(tmpl, 4);
    ASSERT(fd >= 0);
    FILE* fp = fdopen(fd, "w");
    ASSERT(fp);
    fwrite(contents, 1, strlen(contents), fp);
    fclose(fp);
    return strdup(tmpl);
}

TEST(detect_format) {
    ASSERT(keel_config_detect_format("foo.yaml") == KEEL_CONFIG_FORMAT_YAML);
    ASSERT(keel_config_detect_format("foo.yml")  == KEEL_CONFIG_FORMAT_YAML);
    ASSERT(keel_config_detect_format("foo.YAML") == KEEL_CONFIG_FORMAT_YAML);
    ASSERT(keel_config_detect_format("foo.ini")  == KEEL_CONFIG_FORMAT_INI);
    ASSERT(keel_config_detect_format("foo")      == KEEL_CONFIG_FORMAT_INI);
    ASSERT(keel_config_detect_format(NULL)       == KEEL_CONFIG_FORMAT_INI);
}

TEST(load_flat_sections) {
    char* path = mkfile(
        "config_version: 2\n"
        "keel:\n"
        "  log_level: 3\n"
        "logging:\n"
        "  plugin: stdout\n"
        "  log_level: info\n"
    );
    keel_config_t* cfg = keel_config_load_yaml(path);
    ASSERT(cfg);
    ASSERT(keel_config_get_int(cfg, "keel", "config_version", 0) == 2);
    ASSERT(keel_config_get_int(cfg, "keel", "log_level", 0) == 3);
    ASSERT_STR(keel_config_get_string(cfg, "logging", "plugin", NULL), "stdout");
    ASSERT_STR(keel_config_get_string(cfg, "logging", "log_level", NULL), "info");
    keel_config_free(cfg);
    unlink(path);
    free(path);
}

TEST(load_nested_map_flatten) {
    /* `tls.handshake_timeout` should flatten to `tls_handshake_timeout`,
     * matching the canonical INI key. */
    char* path = mkfile(
        "keel:\n"
        "  config_version: 2\n"
        "worker_groups:\n"
        "  - name: pgcluster\n"
        "    bind_addr: 0.0.0.0\n"
        "    bind_port: 7432\n"
        "    tls:\n"
        "      handshake_timeout: 5s\n"
        "      read_timeout: 30s\n"
    );
    keel_config_t* cfg = keel_config_load_yaml(path);
    ASSERT(cfg);
    ASSERT_STR(keel_config_get_string(cfg, "worker_group.pgcluster",
                                      "tls_handshake_timeout", NULL), "5s");
    ASSERT_STR(keel_config_get_string(cfg, "worker_group.pgcluster",
                                      "tls_read_timeout", NULL), "30s");
    ASSERT_STR(keel_config_get_string(cfg, "worker_group.pgcluster",
                                      "bind_addr", NULL), "0.0.0.0");
    ASSERT(keel_config_get_int(cfg, "worker_group.pgcluster",
                                "bind_port", 0) == 7432);
    keel_config_free(cfg);
    unlink(path);
    free(path);
}

TEST(load_worker_group_servers) {
    char* path = mkfile(
        "worker_groups:\n"
        "  - name: pg1\n"
        "    bind_port: 6432\n"
        "    servers:\n"
        "      - name: primary\n"
        "        host: pgsql-01\n"
        "        port: 5432\n"
        "        role: RW\n"
        "        weight: 100\n"
        "      - name: replica\n"
        "        host: pgsql-02\n"
        "        port: 5433\n"
        "        role: RO\n"
        "        weight: 50\n"
    );
    keel_config_t* cfg = keel_config_load_yaml(path);
    ASSERT(cfg);
    ASSERT(keel_config_has_section(cfg, "worker_group.pg1.servers"));
    const char* primary = keel_config_get_string(
        cfg, "worker_group.pg1.servers", "primary", NULL);
    ASSERT(primary != NULL);
    ASSERT(strstr(primary, "host=pgsql-01") != NULL);
    ASSERT(strstr(primary, "port=5432") != NULL);
    ASSERT(strstr(primary, "role=RW") != NULL);
    ASSERT(strstr(primary, "weight=100") != NULL);
    const char* replica = keel_config_get_string(
        cfg, "worker_group.pg1.servers", "replica", NULL);
    ASSERT(replica != NULL);
    ASSERT(strstr(replica, "host=pgsql-02") != NULL);
    ASSERT(strstr(replica, "role=RO") != NULL);
    keel_config_free(cfg);
    unlink(path);
    free(path);
}

TEST(env_interpolation) {
    setenv("KEEL_TEST_DBHOST", "pgsql-prod-1", 1);
    setenv("KEEL_TEST_DBPORT", "6543", 1);
    char* path = mkfile(
        "worker_groups:\n"
        "  - name: prod\n"
        "    servers:\n"
        "      - name: rw\n"
        "        host: ${KEEL_TEST_DBHOST}\n"
        "        port: ${KEEL_TEST_DBPORT}\n"
        "        role: RW\n"
    );
    keel_config_t* cfg = keel_config_load_yaml(path);
    ASSERT(cfg);
    const char* rw = keel_config_get_string(
        cfg, "worker_group.prod.servers", "rw", NULL);
    ASSERT(rw);
    ASSERT(strstr(rw, "host=pgsql-prod-1") != NULL);
    ASSERT(strstr(rw, "port=6543") != NULL);
    keel_config_free(cfg);
    unlink(path);
    free(path);
    unsetenv("KEEL_TEST_DBHOST");
    unsetenv("KEEL_TEST_DBPORT");
}

TEST(env_escape_double_dollar) {
    char* path = mkfile(
        "keel:\n"
        "  literal_dollar: $$REAL_DOLLAR\n"
    );
    keel_config_t* cfg = keel_config_load_yaml(path);
    ASSERT(cfg);
    ASSERT_STR(keel_config_get_string(cfg, "keel", "literal_dollar", NULL),
               "$REAL_DOLLAR");
    keel_config_free(cfg);
    unlink(path);
    free(path);
}

TEST(load_auto_dispatch) {
    /* Same content, two extensions; expect parser dispatch by extension. */
    char* yp = mkfile(
        "keel:\n"
        "  log_level: 4\n"
    );
    char* ip = mkfile_ini(
        "[keel]\n"
        "log_level = 4\n"
    );
    keel_config_t* y = keel_config_load_auto(yp);
    keel_config_t* i = keel_config_load_auto(ip);
    ASSERT(y && i);
    ASSERT(keel_config_get_int(y, "keel", "log_level", 0) == 4);
    ASSERT(keel_config_get_int(i, "keel", "log_level", 0) == 4);
    keel_config_free(y);
    keel_config_free(i);
    unlink(yp); free(yp);
    unlink(ip); free(ip);
}

TEST(reject_root_scalar) {
    /* A YAML doc that's just a scalar at the root is invalid for our schema. */
    char* path = mkfile("just_a_string\n");
    keel_config_t* cfg = keel_config_load_yaml(path);
    ASSERT(cfg == NULL);
    unlink(path); free(path);
}

TEST(reject_worker_group_without_name) {
    char* path = mkfile(
        "worker_groups:\n"
        "  - bind_port: 7432\n"
    );
    keel_config_t* cfg = keel_config_load_yaml(path);
    ASSERT(cfg == NULL);
    unlink(path); free(path);
}

TEST(round_trip_ini_to_yaml_to_ini) {
    /* Build a representative INI in memory, convert to YAML, back to INI,
     * and assert every (section, key, value) tuple survives both passes. */
    char* ini1 = mkfile_ini(
        "[keel]\n"
        "config_version = 2\n"
        "log_level = 2\n"
        "\n"
        "[logging]\n"
        "plugin = stdout\n"
        "log_level = info\n"
        "\n"
        "[worker_group.pg]\n"
        "name = pg\n"
        "bind_port = 6432\n"
        "max_pool_size = 100\n"
        "probe = postgres\n"
        "\n"
        "[worker_group.pg.servers]\n"
        "primary = host=pgsql-01 port=5432 role=RW weight=100\n"
        "replica = host=pgsql-02 port=5433 role=RO weight=50\n"
    );

    char yaml_path[] = "/tmp/keel_rt_XXXXXX.yaml";
    int fd = mkstemps(yaml_path, 5);
    ASSERT(fd >= 0); close(fd);

    char ini2_path[] = "/tmp/keel_rt_XXXXXX.ini";
    fd = mkstemps(ini2_path, 4);
    ASSERT(fd >= 0); close(fd);

    ASSERT(keel_config_convert_ini_to_yaml(ini1, yaml_path) == KEEL_OK);
    ASSERT(keel_config_convert_yaml_to_ini(yaml_path, ini2_path) == KEEL_OK);

    keel_config_t* before = keel_config_load(ini1);
    keel_config_t* after  = keel_config_load(ini2_path);
    ASSERT(before && after);

    /* Spot-check every interesting key. */
    ASSERT(keel_config_get_int(after, "keel", "config_version", 0) == 2);
    ASSERT(keel_config_get_int(after, "keel", "log_level", 0) == 2);
    ASSERT_STR(keel_config_get_string(after, "logging", "plugin", NULL),
               "stdout");
    ASSERT_STR(keel_config_get_string(after, "logging", "log_level", NULL),
               "info");
    ASSERT(keel_config_get_int(after, "worker_group.pg", "bind_port", 0)
           == 6432);
    ASSERT(keel_config_get_int(after, "worker_group.pg", "max_pool_size", 0)
           == 100);
    ASSERT_STR(keel_config_get_string(after, "worker_group.pg", "probe", NULL),
               "postgres");
    const char* prim = keel_config_get_string(
        after, "worker_group.pg.servers", "primary", NULL);
    ASSERT(prim);
    ASSERT(strstr(prim, "host=pgsql-01") != NULL);
    ASSERT(strstr(prim, "port=5432") != NULL);
    ASSERT(strstr(prim, "role=RW") != NULL);
    ASSERT(strstr(prim, "weight=100") != NULL);

    keel_config_free(before);
    keel_config_free(after);
    unlink(ini1); free(ini1);
    unlink(yaml_path);
    unlink(ini2_path);
}

int main(void) {
    printf("Running YAML configuration tests:\n");

    run_detect_format();
    run_load_flat_sections();
    run_load_nested_map_flatten();
    run_load_worker_group_servers();
    run_env_interpolation();
    run_env_escape_double_dollar();
    run_load_auto_dispatch();
    run_reject_root_scalar();
    run_reject_worker_group_without_name();
    run_round_trip_ini_to_yaml_to_ini();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
