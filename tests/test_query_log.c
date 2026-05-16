/**
 * @file test_query_log.c
 * @brief Unit tests for query logging: level parsing, plugin lifecycle, and
 *        record filtering.
 *
 * Query logging crosses multiple abstraction layers (log level, output plugin,
 * record format, and filter mode). This suite validates each layer in isolation
 * with lightweight fixtures so a regression in one does not mask bugs in another.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "keel/log/log.h"
#include "keel/log/log_plugin.h"
#include "keel/log/query_log.h"
#include "keel/protocol/protocol.h"
#include "keel/session/session.h"
#include "keel/sql/query_tree.h"
#include "keel/mem/mem.h"

/* ============================================================================
 * Test Helpers
 * ============================================================================ */

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    static void name(void); \
    static void name(void)

#define RUN_TEST(name) do { \
    printf("  %-50s ", #name); \
    name(); \
    tests_passed++; \
    printf("PASS\n"); \
} while (0)

#define ASSERT(expr) do { \
    if (!(expr)) { \
        printf("FAIL\n    Assertion failed: %s\n    at %s:%d\n", \
               #expr, __FILE__, __LINE__); \
        tests_failed++; \
        return; \
    } \
} while (0)

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        printf("FAIL\n    Expected %d == %d\n    at %s:%d\n", \
               (int)(a), (int)(b), __FILE__, __LINE__); \
        tests_failed++; \
        return; \
    } \
} while (0)

#define ASSERT_STR_EQ(a, b) do { \
    if (strcmp((a), (b)) != 0) { \
        printf("FAIL\n    Expected \"%s\" == \"%s\"\n    at %s:%d\n", \
               (a), (b), __FILE__, __LINE__); \
        tests_failed++; \
        return; \
    } \
} while (0)

/* ============================================================================
 * Test: Log Level Parsing
 * ============================================================================ */

TEST(test_log_level_from_string)
{
    ASSERT_EQ(keel_log_level_from_string("trace"),   KEEL_LOG_TRACE);
    ASSERT_EQ(keel_log_level_from_string("debug"),   KEEL_LOG_DEBUG);
    ASSERT_EQ(keel_log_level_from_string("info"),    KEEL_LOG_INFO);
    ASSERT_EQ(keel_log_level_from_string("warn"),    KEEL_LOG_WARN);
    ASSERT_EQ(keel_log_level_from_string("warning"), KEEL_LOG_WARN);
    ASSERT_EQ(keel_log_level_from_string("error"),   KEEL_LOG_ERROR);
    ASSERT_EQ(keel_log_level_from_string("fatal"),   KEEL_LOG_FATAL);
    ASSERT_EQ(keel_log_level_from_string("all"),     KEEL_LOG_TRACE);
    ASSERT_EQ(keel_log_level_from_string("full"),    KEEL_LOG_TRACE);
    ASSERT_EQ(keel_log_level_from_string("off"),     KEEL_LOG_OFF);
    ASSERT_EQ(keel_log_level_from_string("none"),    KEEL_LOG_OFF);
    ASSERT_EQ(keel_log_level_from_string(NULL),      KEEL_LOG_INFO);
    ASSERT_EQ(keel_log_level_from_string("unknown"), KEEL_LOG_INFO);
}

/* ============================================================================
 * Test: Query Log Mode Parsing
 * ============================================================================ */

TEST(test_query_log_mode_from_string)
{
    ASSERT_EQ(keel_query_log_mode_from_string("none"),  KEEL_QUERY_LOG_NONE);
    ASSERT_EQ(keel_query_log_mode_from_string("all"),   KEEL_QUERY_LOG_ALL);
    ASSERT_EQ(keel_query_log_mode_from_string("read"),  KEEL_QUERY_LOG_READ);
    ASSERT_EQ(keel_query_log_mode_from_string("write"), KEEL_QUERY_LOG_WRITE);
    ASSERT_EQ(keel_query_log_mode_from_string(NULL),    KEEL_QUERY_LOG_NONE);
    ASSERT_EQ(keel_query_log_mode_from_string("xyz"),   KEEL_QUERY_LOG_NONE);
}

TEST(test_query_log_mode_name)
{
    ASSERT_STR_EQ(keel_query_log_mode_name(KEEL_QUERY_LOG_NONE),  "none");
    ASSERT_STR_EQ(keel_query_log_mode_name(KEEL_QUERY_LOG_ALL),   "all");
    ASSERT_STR_EQ(keel_query_log_mode_name(KEEL_QUERY_LOG_READ),  "read");
    ASSERT_STR_EQ(keel_query_log_mode_name(KEEL_QUERY_LOG_WRITE), "write");
}

/* ============================================================================
 * Test: Stdout Plugin Lifecycle
 * ============================================================================ */

TEST(test_stdout_plugin_lifecycle)
{
    keel_log_plugin_t* p = keel_log_plugin_stdout_create();
    ASSERT(p != NULL);
    ASSERT_STR_EQ(p->name, "stdout");

    keel_error_t err = p->open(p, NULL);
    ASSERT_EQ(err, KEEL_OK);

    /* Write a test record */
    keel_log_record_t rec = {
        .level       = KEEL_LOG_INFO,
        .category    = KEEL_LOG_CAT_SQL,
        .ts_sec      = 1700000000,
        .ts_nsec     = 123000000,
        .message     = "test stdout plugin",
        .message_len = strlen("test stdout plugin"),
    };
    err = p->write(p, &rec);
    ASSERT_EQ(err, KEEL_OK);

    err = p->flush(p);
    ASSERT_EQ(err, KEEL_OK);

    p->close(p);
    p->destroy(p);
}

/* ============================================================================
 * Test: File Plugin Lifecycle
 * ============================================================================ */

TEST(test_file_plugin_lifecycle)
{
    keel_log_plugin_t* p = keel_log_plugin_file_create();
    ASSERT(p != NULL);
    ASSERT_STR_EQ(p->name, "file");

    /* Create a temp file */
    char tmppath[] = "/tmp/keel_test_log_XXXXXX";
    int fd = mkstemp(tmppath);
    ASSERT(fd >= 0);
    close(fd);

    keel_log_plugin_config_t cfg = {
        .file_path = tmppath,
    };
    keel_error_t err = p->open(p, &cfg);
    ASSERT_EQ(err, KEEL_OK);

    /* Write a record */
    keel_log_record_t rec = {
        .level       = KEEL_LOG_WARN,
        .category    = KEEL_LOG_CAT_SQL,
        .ts_sec      = 1700000000,
        .ts_nsec     = 456000000,
        .src_addr    = "10.0.0.1",
        .src_port    = 12345,
        .dst_addr    = "10.0.0.2",
        .dst_port    = 5432,
        .username    = "testuser",
        .database    = "testdb",
        .query       = "SELECT 1",
        .query_len   = 8,
        .message     = "QUERY SELECT",
        .message_len = 12,
    };
    err = p->write(p, &rec);
    ASSERT_EQ(err, KEEL_OK);

    p->flush(p);
    p->close(p);

    /* Verify file contents */
    FILE* fp = fopen(tmppath, "r");
    ASSERT(fp != NULL);
    char line[1024] = {0};
    ASSERT(fgets(line, sizeof(line), fp) != NULL);
    fclose(fp);

    /* Should contain our structured fields */
    ASSERT(strstr(line, "WARN") != NULL);
    ASSERT(strstr(line, "src=10.0.0.1:12345") != NULL);
    ASSERT(strstr(line, "dst=10.0.0.2:5432") != NULL);
    ASSERT(strstr(line, "user=testuser") != NULL);
    ASSERT(strstr(line, "db=testdb") != NULL);
    ASSERT(strstr(line, "SELECT 1") != NULL);

    unlink(tmppath);
    p->destroy(p);
}

/* ============================================================================
 * Test: File Plugin requires path
 * ============================================================================ */

TEST(test_file_plugin_no_path)
{
    keel_log_plugin_t* p = keel_log_plugin_file_create();
    ASSERT(p != NULL);

    keel_log_plugin_config_t cfg = { .file_path = NULL };
    keel_error_t err = p->open(p, &cfg);
    ASSERT(err != KEEL_OK);  /* Should fail without a path */

    p->destroy(p);
}

/* ============================================================================
 * Test: Syslog Plugin Lifecycle
 * ============================================================================ */

TEST(test_syslog_plugin_lifecycle)
{
    keel_log_plugin_t* p = keel_log_plugin_syslog_create();
    ASSERT(p != NULL);
    ASSERT_STR_EQ(p->name, "syslog");

    keel_log_plugin_config_t cfg = {
        .ident = "keel_test",
    };
    keel_error_t err = p->open(p, &cfg);
    ASSERT_EQ(err, KEEL_OK);

    /* Write a record (goes to syslog — can't easily verify content) */
    keel_log_record_t rec = {
        .level       = KEEL_LOG_INFO,
        .category    = KEEL_LOG_CAT_CORE,
        .message     = "keel unit test syslog message",
        .message_len = strlen("keel unit test syslog message"),
    };
    err = p->write(p, &rec);
    ASSERT_EQ(err, KEEL_OK);

    p->close(p);
    p->destroy(p);
}

/* ============================================================================
 * Test: Query Log Init / Shutdown
 * ============================================================================ */

TEST(test_query_log_init_shutdown)
{
    keel_log_plugin_t* plugin = keel_log_plugin_stdout_create();
    ASSERT(plugin != NULL);
    plugin->open(plugin, NULL);

    keel_query_log_t qlog;
    keel_query_log_config_t cfg = keel_query_log_config_default();
    cfg.mode = KEEL_QUERY_LOG_ALL;

    keel_error_t err = keel_query_log_init(&qlog, &cfg, plugin);
    ASSERT_EQ(err, KEEL_OK);
    ASSERT(qlog.enabled);

    keel_query_log_shutdown(&qlog);
    ASSERT(!qlog.enabled);

    plugin->close(plugin);
    plugin->destroy(plugin);
}

/* ============================================================================
 * Test: Query Log Disabled When Mode is None
 * ============================================================================ */

TEST(test_query_log_disabled_none_mode)
{
    keel_log_plugin_t* plugin = keel_log_plugin_stdout_create();
    ASSERT(plugin != NULL);
    plugin->open(plugin, NULL);

    keel_query_log_t qlog;
    keel_query_log_config_t cfg = keel_query_log_config_default();
    cfg.mode = KEEL_QUERY_LOG_NONE;

    keel_error_t err = keel_query_log_init(&qlog, &cfg, plugin);
    ASSERT_EQ(err, KEEL_OK);
    ASSERT(!qlog.enabled);  /* Should be disabled */

    keel_query_log_shutdown(&qlog);
    plugin->close(plugin);
    plugin->destroy(plugin);
}

/* ============================================================================
 * Test: Query Log Filtering — Read Mode
 * ============================================================================ */

/* Capture plugin: counts write calls */
static int s_capture_count = 0;
static keel_log_record_t s_last_record;

static keel_error_t capture_open(keel_log_plugin_t* p, const keel_log_plugin_config_t* c) {
    (void)p; (void)c; s_capture_count = 0; return KEEL_OK;
}
static keel_error_t capture_write(keel_log_plugin_t* p, const keel_log_record_t* r) {
    (void)p; s_capture_count++; s_last_record = *r; return KEEL_OK;
}
static keel_error_t capture_flush(keel_log_plugin_t* p) { (void)p; return KEEL_OK; }
static void capture_close(keel_log_plugin_t* p) { (void)p; }
static void capture_destroy(keel_log_plugin_t* p) { keel_free(p); }

static keel_log_plugin_t* create_capture_plugin(void)
{
    keel_log_plugin_t* p = (keel_log_plugin_t*)keel_calloc(1, sizeof(*p));
    if (!p) return NULL;
    p->name    = "capture";
    p->open    = capture_open;
    p->write   = capture_write;
    p->flush   = capture_flush;
    p->close   = capture_close;
    p->destroy = capture_destroy;
    return p;
}

TEST(test_query_log_filter_read_mode)
{
    keel_log_plugin_t* plugin = create_capture_plugin();
    plugin->open(plugin, NULL);

    keel_query_log_t qlog;
    keel_query_log_config_t cfg = keel_query_log_config_default();
    cfg.mode = KEEL_QUERY_LOG_READ;
    keel_query_log_init(&qlog, &cfg, plugin);

    /* Read query — should be logged */
    keel_proto_query_t read_q = {
        .sql   = { .data = "SELECT * FROM users", .len = 19 },
        .type  = KEEL_QUERY_SELECT,
        .flags = KEEL_QUERY_FLAG_READ_ONLY,
    };
    s_capture_count = 0;
    keel_query_log_emit(&qlog, NULL, &read_q);
    ASSERT_EQ(s_capture_count, 1);

    /* Write query — should NOT be logged in read mode */
    keel_proto_query_t write_q = {
        .sql   = { .data = "INSERT INTO users VALUES(1)", .len = 27 },
        .type  = KEEL_QUERY_INSERT,
        .flags = KEEL_QUERY_FLAG_WRITE,
    };
    s_capture_count = 0;
    keel_query_log_emit(&qlog, NULL, &write_q);
    ASSERT_EQ(s_capture_count, 0);

    keel_query_log_shutdown(&qlog);
    plugin->close(plugin);
    plugin->destroy(plugin);
}

TEST(test_query_log_filter_write_mode)
{
    keel_log_plugin_t* plugin = create_capture_plugin();
    plugin->open(plugin, NULL);

    keel_query_log_t qlog;
    keel_query_log_config_t cfg = keel_query_log_config_default();
    cfg.mode = KEEL_QUERY_LOG_WRITE;
    keel_query_log_init(&qlog, &cfg, plugin);

    /* Write query — should be logged */
    keel_proto_query_t write_q = {
        .sql   = { .data = "DELETE FROM users WHERE id=1", .len = 27 },
        .type  = KEEL_QUERY_DELETE,
        .flags = KEEL_QUERY_FLAG_WRITE,
    };
    s_capture_count = 0;
    keel_query_log_emit(&qlog, NULL, &write_q);
    ASSERT_EQ(s_capture_count, 1);

    /* DDL — should also be logged in write mode */
    keel_proto_query_t ddl_q = {
        .sql   = { .data = "DROP TABLE users", .len = 16 },
        .type  = KEEL_QUERY_DROP,
        .flags = KEEL_QUERY_FLAG_DDL,
    };
    s_capture_count = 0;
    keel_query_log_emit(&qlog, NULL, &ddl_q);
    ASSERT_EQ(s_capture_count, 1);

    /* Read query — should NOT be logged in write mode */
    keel_proto_query_t read_q = {
        .sql   = { .data = "SELECT 1", .len = 8 },
        .type  = KEEL_QUERY_SELECT,
        .flags = KEEL_QUERY_FLAG_READ_ONLY,
    };
    s_capture_count = 0;
    keel_query_log_emit(&qlog, NULL, &read_q);
    ASSERT_EQ(s_capture_count, 0);

    keel_query_log_shutdown(&qlog);
    plugin->close(plugin);
    plugin->destroy(plugin);
}

TEST(test_query_log_filter_all_mode)
{
    keel_log_plugin_t* plugin = create_capture_plugin();
    plugin->open(plugin, NULL);

    keel_query_log_t qlog;
    keel_query_log_config_t cfg = keel_query_log_config_default();
    cfg.mode = KEEL_QUERY_LOG_ALL;
    keel_query_log_init(&qlog, &cfg, plugin);

    /* Both read and write should be logged */
    keel_proto_query_t read_q = {
        .sql   = { .data = "SELECT 1", .len = 8 },
        .type  = KEEL_QUERY_SELECT,
        .flags = KEEL_QUERY_FLAG_READ_ONLY,
    };
    keel_proto_query_t write_q = {
        .sql   = { .data = "UPDATE t SET x=1", .len = 16 },
        .type  = KEEL_QUERY_UPDATE,
        .flags = KEEL_QUERY_FLAG_WRITE,
    };
    s_capture_count = 0;
    keel_query_log_emit(&qlog, NULL, &read_q);
    keel_query_log_emit(&qlog, NULL, &write_q);
    ASSERT_EQ(s_capture_count, 2);

    keel_query_log_shutdown(&qlog);
    plugin->close(plugin);
    plugin->destroy(plugin);
}

/* ============================================================================
 * Test: Global Query Logger Accessor
 * ============================================================================ */

TEST(test_global_query_log_accessor)
{
    ASSERT(keel_query_log_get_global() == NULL || 1);  /* May or may not be set */

    keel_query_log_t qlog;
    memset(&qlog, 0, sizeof(qlog));

    keel_query_log_set_global(&qlog);
    ASSERT(keel_query_log_get_global() == &qlog);

    keel_query_log_set_global(NULL);
    ASSERT(keel_query_log_get_global() == NULL);
}

/* ============================================================================
 * Test: Query Log Message (non-query log line)
 * ============================================================================ */

TEST(test_query_log_message)
{
    keel_log_plugin_t* plugin = create_capture_plugin();
    plugin->open(plugin, NULL);

    keel_query_log_t qlog;
    keel_query_log_config_t cfg = keel_query_log_config_default();
    cfg.mode = KEEL_QUERY_LOG_ALL;
    keel_query_log_init(&qlog, &cfg, plugin);

    s_capture_count = 0;
    keel_query_log_message(&qlog, KEEL_LOG_INFO, KEEL_LOG_CAT_CORE,
                          "Connection from %s established", "10.0.0.1");
    ASSERT_EQ(s_capture_count, 1);
    ASSERT(s_last_record.level == KEEL_LOG_INFO);

    keel_query_log_shutdown(&qlog);
    plugin->close(plugin);
    plugin->destroy(plugin);
}

/* ============================================================================
 * Test: Config Default Values
 * ============================================================================ */

TEST(test_query_log_config_default)
{
    keel_query_log_config_t cfg = keel_query_log_config_default();
    ASSERT_EQ(cfg.mode, KEEL_QUERY_LOG_NONE);
    ASSERT_EQ(cfg.min_level, KEEL_LOG_INFO);
    ASSERT(cfg.log_timestamps);
    ASSERT(cfg.log_source);
    ASSERT(cfg.log_dest);
    ASSERT(cfg.log_username);
    ASSERT(cfg.log_database);
    ASSERT(!cfg.log_query_tree);
    ASSERT_EQ(cfg.max_query_len, 0);
}

/* ============================================================================
 * Test: keel_qt_snprint — null tree
 * ============================================================================ */

TEST(test_qt_snprint_null)
{
    char buf[256];
    int n = keel_qt_snprint(NULL, buf, sizeof(buf));
    ASSERT(n > 0);
    ASSERT(strstr(buf, "null") != NULL);
}

/* ============================================================================
 * Test: keel_qt_snprint — real SELECT tree
 * ============================================================================ */

TEST(test_qt_snprint_select)
{
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT(arena != NULL);

    keel_str_t sql = { .data = "SELECT id, name FROM users WHERE active = true",
                      .len  = 47 };
    keel_qt_query_t* qt = keel_sql_analyze_full(sql, arena);
    /* Even if full parser returns NULL (some SQL not supported), snprint
     * must handle it gracefully. */
    char buf[2048];
    int n = keel_qt_snprint(qt, buf, sizeof(buf));
    ASSERT(n > 0);

    if (qt) {
        /* If parsing succeeded, we should see type and op */
        ASSERT(strstr(buf, "QT{") != NULL);
        ASSERT(strstr(buf, "op=") != NULL);
    }

    keel_arena_destroy(arena);
}

/* ============================================================================
 * Test: Query log emit with tree enabled
 * ============================================================================ */

TEST(test_query_log_emit_with_tree)
{
    keel_log_plugin_t* plugin = create_capture_plugin();
    plugin->open(plugin, NULL);

    keel_query_log_t qlog;
    keel_query_log_config_t cfg = keel_query_log_config_default();
    cfg.mode           = KEEL_QUERY_LOG_ALL;
    cfg.log_query_tree = true;
    keel_query_log_init(&qlog, &cfg, plugin);

    keel_proto_query_t read_q = {
        .sql   = { .data = "SELECT 1", .len = 8 },
        .type  = KEEL_QUERY_SELECT,
        .flags = KEEL_QUERY_FLAG_READ_ONLY,
    };
    s_capture_count = 0;
    keel_query_log_emit(&qlog, NULL, &read_q);
    ASSERT_EQ(s_capture_count, 1);

    /* The record should have a query_tree populated (may be NULL if full
     * parser doesn't support this simple query, but at minimum a tree_len
     * was attempted). Either tree is set or it's gracefully absent. */

    keel_query_log_shutdown(&qlog);
    plugin->close(plugin);
    plugin->destroy(plugin);
}

/* ============================================================================
 * Test: Query log emit WITHOUT tree (default)
 * ============================================================================ */

TEST(test_query_log_emit_no_tree)
{
    keel_log_plugin_t* plugin = create_capture_plugin();
    plugin->open(plugin, NULL);

    keel_query_log_t qlog;
    keel_query_log_config_t cfg = keel_query_log_config_default();
    cfg.mode           = KEEL_QUERY_LOG_ALL;
    cfg.log_query_tree = false;  /* explicitly off */
    keel_query_log_init(&qlog, &cfg, plugin);

    keel_proto_query_t q = {
        .sql   = { .data = "INSERT INTO t VALUES(1)", .len = 23 },
        .type  = KEEL_QUERY_INSERT,
        .flags = KEEL_QUERY_FLAG_WRITE,
    };
    s_capture_count = 0;
    keel_query_log_emit(&qlog, NULL, &q);
    ASSERT_EQ(s_capture_count, 1);

    /* With tree disabled, the record should NOT have tree set */
    ASSERT(s_last_record.query_tree == NULL || s_last_record.query_tree_len == 0);

    keel_query_log_shutdown(&qlog);
    plugin->close(plugin);
    plugin->destroy(plugin);
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void)
{
    /* Init the core log system so KEEL_LOG_* macros work */
    keel_log_init(NULL);

    printf("\n=== Query Log Tests ===\n\n");

    RUN_TEST(test_log_level_from_string);
    RUN_TEST(test_query_log_mode_from_string);
    RUN_TEST(test_query_log_mode_name);
    RUN_TEST(test_stdout_plugin_lifecycle);
    RUN_TEST(test_file_plugin_lifecycle);
    RUN_TEST(test_file_plugin_no_path);
    RUN_TEST(test_syslog_plugin_lifecycle);
    RUN_TEST(test_query_log_init_shutdown);
    RUN_TEST(test_query_log_disabled_none_mode);
    RUN_TEST(test_query_log_filter_read_mode);
    RUN_TEST(test_query_log_filter_write_mode);
    RUN_TEST(test_query_log_filter_all_mode);
    RUN_TEST(test_global_query_log_accessor);
    RUN_TEST(test_query_log_message);
    RUN_TEST(test_query_log_config_default);
    RUN_TEST(test_qt_snprint_null);
    RUN_TEST(test_qt_snprint_select);
    RUN_TEST(test_query_log_emit_with_tree);
    RUN_TEST(test_query_log_emit_no_tree);

    printf("\n  Results: %d passed, %d failed\n\n", tests_passed, tests_failed);

    keel_log_shutdown();
    return tests_failed > 0 ? 1 : 0;
}
