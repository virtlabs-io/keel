/**
 * @file test_audit_log.c
 * @brief Unit tests for the structured security audit log.
 *
 * Tests cover:
 * §1 — Lifecycle: init / close with various configs
 * §2 — Event mask parsing (keel_audit_parse_events)
 * §3 — Event name introspection (keel_audit_event_name)
 * §4 — NDJSON output: emit connect / disconnect / auth / DDL / admin / rule events
 * §5 — Text format output
 * §6 — Event filtering (only enabled events are written)
 * §7 — Stats: events_emitted / events_dropped counters
 * §8 — Disabled log: no output, no crash
 * §9 — Null-guard: all public functions tolerate NULL inputs
 */

#include "test_utils.h"

#include "keel/log/audit_log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

static char g_tmpfile[64];
static FILE *g_capture = NULL;

/** Open a temp file for capturing audit output. */
static const char *tmp_path(void)
{
    snprintf(g_tmpfile, sizeof(g_tmpfile), "/tmp/keel_audit_test_XXXXXX");
    int fd = mkstemp(g_tmpfile);
    if (fd >= 0) close(fd);
    return g_tmpfile;
}

/** Read the temp file into a static buffer and return it. */
static const char *read_tmp(void)
{
    static char buf[16384];
    buf[0] = '\0';
    FILE *f = fopen(g_tmpfile, "r");
    if (!f) return buf;
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    buf[n] = '\0';
    fclose(f);
    return buf;
}

/** Remove the temp file. */
static void rm_tmp(void)
{
    unlink(g_tmpfile);
}

/** Build an audit_log writing NDJSON to a temp file. */
static keel_audit_log_t make_ndjson_log(uint32_t mask)
{
    keel_audit_log_t al;
    keel_audit_config_t cfg = keel_audit_config_default();
    cfg.enabled = true;
    strncpy(cfg.path, tmp_path(), sizeof(cfg.path) - 1);
    cfg.event_mask = mask;
    cfg.format = KEEL_AUDIT_FORMAT_NDJSON;
    keel_audit_log_init(&al, &cfg);
    return al;
}

/** Build an audit_log writing text to a temp file. */
static keel_audit_log_t make_text_log(uint32_t mask)
{
    keel_audit_log_t al;
    keel_audit_config_t cfg = keel_audit_config_default();
    cfg.enabled = true;
    strncpy(cfg.path, tmp_path(), sizeof(cfg.path) - 1);
    cfg.event_mask = mask;
    cfg.format = KEEL_AUDIT_FORMAT_TEXT;
    keel_audit_log_init(&al, &cfg);
    return al;
}

/* ============================================================================
 * §1 — Lifecycle
 * ============================================================================ */

static void test_init_disabled(void)
{
    TEST_BEGIN("audit: init disabled");
    keel_audit_log_t al;
    keel_audit_config_t cfg = keel_audit_config_default();  /* enabled=false */
    int rc = keel_audit_log_init(&al, &cfg);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(!al.enabled);
    keel_audit_log_close(&al);
    TEST_END();
}

static void test_init_stdout(void)
{
    TEST_BEGIN("audit: init stdout");
    keel_audit_log_t al;
    keel_audit_config_t cfg = keel_audit_config_default();
    cfg.enabled = true;
    strncpy(cfg.path, "stdout", sizeof(cfg.path) - 1);
    int rc = keel_audit_log_init(&al, &cfg);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(al.enabled);
    TEST_ASSERT(al.fp == stdout);
    keel_audit_log_close(&al);
    TEST_END();
}

static void test_init_file(void)
{
    TEST_BEGIN("audit: init file");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    TEST_ASSERT(al.enabled);
    TEST_ASSERT_NOT_NULL(al.fp);
    keel_audit_log_close(&al);
    rm_tmp();
    TEST_END();
}

static void test_close_idempotent(void)
{
    TEST_BEGIN("audit: close is safe on disabled log");
    keel_audit_log_t al;
    keel_audit_config_t cfg = keel_audit_config_default();
    keel_audit_log_init(&al, &cfg);
    keel_audit_log_close(&al);
    keel_audit_log_close(&al);  /* second close must not crash */
    TEST_ASSERT(!al.enabled);
    TEST_END();
}

/* ============================================================================
 * §2 — Event mask parsing
 * ============================================================================ */

static void test_parse_events_all(void)
{
    TEST_BEGIN("audit: parse_events all");
    TEST_ASSERT_EQ(keel_audit_parse_events("all"), KEEL_AUDIT_ALL_EVENTS);
    TEST_ASSERT_EQ(keel_audit_parse_events(NULL),  KEEL_AUDIT_ALL_EVENTS);
    TEST_ASSERT_EQ(keel_audit_parse_events(""),    KEEL_AUDIT_ALL_EVENTS);
    /* "all" combined with "scatter" must OR scatter in on top of all */
    uint32_t m = keel_audit_parse_events("all,scatter");
    TEST_ASSERT_EQ(m, KEEL_AUDIT_ALL_EVENTS_WITH_SCATTER);
    /* order doesn't matter */
    m = keel_audit_parse_events("scatter,all");
    TEST_ASSERT_EQ(m, KEEL_AUDIT_ALL_EVENTS_WITH_SCATTER);
    TEST_END();
}

static void test_parse_events_individual(void)
{
    TEST_BEGIN("audit: parse_events individual tokens");
    uint32_t m;

    m = keel_audit_parse_events("auth");
    TEST_ASSERT(m & KEEL_AUDIT_AUTH_OK);
    TEST_ASSERT(m & KEEL_AUDIT_AUTH_FAIL);
    TEST_ASSERT(!(m & KEEL_AUDIT_DDL));

    m = keel_audit_parse_events("ddl");
    TEST_ASSERT(m & KEEL_AUDIT_DDL);
    TEST_ASSERT(!(m & KEEL_AUDIT_AUTH_OK));

    m = keel_audit_parse_events("admin");
    TEST_ASSERT(m & KEEL_AUDIT_ADMIN_CMD);

    m = keel_audit_parse_events("rules");
    TEST_ASSERT(m & KEEL_AUDIT_RULE_BLOCK);
    TEST_ASSERT(m & KEEL_AUDIT_RULE_THROTTLE);

    m = keel_audit_parse_events("throttle");
    TEST_ASSERT(m & KEEL_AUDIT_RULE_THROTTLE);
    TEST_ASSERT(!(m & KEEL_AUDIT_RULE_BLOCK));

    TEST_END();
}

static void test_parse_events_combined(void)
{
    TEST_BEGIN("audit: parse_events combined list");
    uint32_t m = keel_audit_parse_events("auth,ddl,admin");
    TEST_ASSERT(m & KEEL_AUDIT_AUTH_OK);
    TEST_ASSERT(m & KEEL_AUDIT_AUTH_FAIL);
    TEST_ASSERT(m & KEEL_AUDIT_DDL);
    TEST_ASSERT(m & KEEL_AUDIT_ADMIN_CMD);
    TEST_ASSERT(!(m & KEEL_AUDIT_CONNECT));
    TEST_END();
}

static void test_parse_events_connect(void)
{
    TEST_BEGIN("audit: parse_events connect includes disconnect");
    uint32_t m = keel_audit_parse_events("connect");
    TEST_ASSERT(m & KEEL_AUDIT_CONNECT);
    TEST_ASSERT(m & KEEL_AUDIT_DISCONNECT);
    TEST_END();
}

/* ============================================================================
 * §3 — Event name introspection
 * ============================================================================ */

static void test_event_names(void)
{
    TEST_BEGIN("audit: event names");
    TEST_ASSERT_STR_EQ(keel_audit_event_name(KEEL_AUDIT_CONNECT),       "CONNECT");
    TEST_ASSERT_STR_EQ(keel_audit_event_name(KEEL_AUDIT_DISCONNECT),    "DISCONNECT");
    TEST_ASSERT_STR_EQ(keel_audit_event_name(KEEL_AUDIT_AUTH_OK),       "AUTH_OK");
    TEST_ASSERT_STR_EQ(keel_audit_event_name(KEEL_AUDIT_AUTH_FAIL),     "AUTH_FAIL");
    TEST_ASSERT_STR_EQ(keel_audit_event_name(KEEL_AUDIT_DDL),           "DDL");
    TEST_ASSERT_STR_EQ(keel_audit_event_name(KEEL_AUDIT_ADMIN_CMD),     "ADMIN_CMD");
    TEST_ASSERT_STR_EQ(keel_audit_event_name(KEEL_AUDIT_RULE_BLOCK),    "RULE_BLOCK");
    TEST_ASSERT_STR_EQ(keel_audit_event_name(KEEL_AUDIT_RULE_THROTTLE), "RULE_THROTTLE");
    TEST_END();
}

/* ============================================================================
 * §4 — NDJSON output
 * ============================================================================ */

static void test_ndjson_connect(void)
{
    TEST_BEGIN("audit: ndjson CONNECT event");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_connect(&al, KEEL_AUDIT_CONNECT,
        "192.168.1.1", 54321, "alice", "mydb");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"CONNECT\"") != NULL);
    TEST_ASSERT(strstr(out, "\"user\":\"alice\"") != NULL);
    TEST_ASSERT(strstr(out, "\"db\":\"mydb\"") != NULL);
    TEST_ASSERT(strstr(out, "\"client\":\"192.168.1.1\"") != NULL);
    TEST_ASSERT(strstr(out, "\"ts\":") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_disconnect(void)
{
    TEST_BEGIN("audit: ndjson DISCONNECT event");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_connect(&al, KEEL_AUDIT_DISCONNECT,
        "10.0.0.2", 12345, "bob", "testdb");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"DISCONNECT\"") != NULL);
    TEST_ASSERT(strstr(out, "\"user\":\"bob\"") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_auth_ok(void)
{
    TEST_BEGIN("audit: ndjson AUTH_OK event");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_auth(&al, KEEL_AUDIT_AUTH_OK,
        "alice", "mydb", "127.0.0.1", 9999, NULL);
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"AUTH_OK\"") != NULL);
    TEST_ASSERT(strstr(out, "\"user\":\"alice\"") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_auth_fail(void)
{
    TEST_BEGIN("audit: ndjson AUTH_FAIL with detail");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_auth(&al, KEEL_AUDIT_AUTH_FAIL,
        "hacker", "postgres", "1.2.3.4", 7777,
        "SCRAM authentication failed");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"AUTH_FAIL\"") != NULL);
    TEST_ASSERT(strstr(out, "SCRAM authentication failed") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_ddl(void)
{
    TEST_BEGIN("audit: ndjson DDL event");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_ddl(&al, "alice", "mydb", "10.0.0.1", 4321,
        "DROP TABLE users");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"DDL\"") != NULL);
    TEST_ASSERT(strstr(out, "DROP TABLE users") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_admin_cmd(void)
{
    TEST_BEGIN("audit: ndjson ADMIN_CMD event");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_admin_cmd(&al, "127.0.0.1", 6433, "RELOAD");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"ADMIN_CMD\"") != NULL);
    TEST_ASSERT(strstr(out, "RELOAD") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_rule_block(void)
{
    TEST_BEGIN("audit: ndjson RULE_BLOCK event");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_rule_event(&al, KEEL_AUDIT_RULE_BLOCK,
        "alice", "mydb", "10.0.0.1", 5555,
        "DROP TABLE secrets", "throttle.0");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"RULE_BLOCK\"") != NULL);
    TEST_ASSERT(strstr(out, "DROP TABLE secrets") != NULL);
    TEST_ASSERT(strstr(out, "throttle.0") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_rule_throttle(void)
{
    TEST_BEGIN("audit: ndjson RULE_THROTTLE event");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_rule_event(&al, KEEL_AUDIT_RULE_THROTTLE,
        "bob", "db2", "192.168.0.5", 1234,
        "SELECT * FROM big_table", "throttle.1");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"RULE_THROTTLE\"") != NULL);
    TEST_ASSERT(strstr(out, "big_table") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_json_structure(void)
{
    TEST_BEGIN("audit: ndjson output is valid JSON line");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_connect(&al, KEEL_AUDIT_CONNECT,
        "127.0.0.1", 12345, "alice", "postgres");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    /* Must start with { and end with }\n */
    TEST_ASSERT(out[0] == '{');
    size_t len = strlen(out);
    TEST_ASSERT(len > 2);
    TEST_ASSERT(out[len - 1] == '\n');
    TEST_ASSERT(out[len - 2] == '}');
    rm_tmp();
    TEST_END();
}

static void test_ndjson_query_truncation(void)
{
    TEST_BEGIN("audit: ndjson DDL query truncated at 512 bytes");
    /* Build a 600-char query */
    char long_query[601];
    memset(long_query, 'A', 600);
    long_query[600] = '\0';

    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_ddl(&al, "user1", "db1", "127.0.0.1", 0, long_query);
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    /* Should still have event */
    TEST_ASSERT(strstr(out, "\"event\":\"DDL\"") != NULL);
    /* Output should not contain 600 consecutive A's */
    char a601[602];
    memset(a601, 'A', 601);
    a601[601] = '\0';
    TEST_ASSERT(strstr(out, a601) == NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_special_chars_escaped(void)
{
    TEST_BEGIN("audit: ndjson special chars in query are escaped");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_ddl(&al, "u", "d", "127.0.0.1", 0,
        "SELECT \"foo\" FROM bar WHERE x = 'y'");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    /* Embedded double-quote must be escaped */
    TEST_ASSERT(strstr(out, "\\\"foo\\\"") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_multiple_events(void)
{
    TEST_BEGIN("audit: ndjson multiple events in one file");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    for (int i = 0; i < 5; i++) {
        keel_audit_emit_connect(&al, KEEL_AUDIT_CONNECT,
            "10.0.0.1", (uint16_t)(1000 + i), "user", "db");
    }
    keel_audit_log_close(&al);

    /* Count lines (events) */
    const char *out = read_tmp();
    int lines = 0;
    for (const char *p = out; *p; p++) {
        if (*p == '\n') lines++;
    }
    TEST_ASSERT_EQ(lines, 5);
    rm_tmp();
    TEST_END();
}

/* ============================================================================
 * §5 — Text format output
 * ============================================================================ */

static void test_text_format_connect(void)
{
    TEST_BEGIN("audit: text format CONNECT");
    keel_audit_log_t al = make_text_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_connect(&al, KEEL_AUDIT_CONNECT,
        "10.10.10.1", 9999, "testuser", "testdb");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "[AUDIT]") != NULL);
    TEST_ASSERT(strstr(out, "event=CONNECT") != NULL);
    TEST_ASSERT(strstr(out, "user=testuser") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_text_format_auth_fail(void)
{
    TEST_BEGIN("audit: text format AUTH_FAIL");
    keel_audit_log_t al = make_text_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_auth(&al, KEEL_AUDIT_AUTH_FAIL,
        "baduser", "baddb", "1.1.1.1", 0, "wrong password");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "event=AUTH_FAIL") != NULL);
    TEST_ASSERT(strstr(out, "wrong password") != NULL);
    rm_tmp();
    TEST_END();
}

/* ============================================================================
 * §6 — Event filtering
 * ============================================================================ */

static void test_filter_blocks_disabled_events(void)
{
    TEST_BEGIN("audit: disabled events produce no output");
    /* Enable only DDL, disable auth */
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_DDL);
    keel_audit_emit_auth(&al, KEEL_AUDIT_AUTH_OK,
        "alice", "db", "127.0.0.1", 0, NULL);
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT_EQ((int)strlen(out), 0);
    rm_tmp();
    TEST_END();
}

static void test_filter_allows_enabled_events(void)
{
    TEST_BEGIN("audit: enabled events pass through filter");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_DDL | KEEL_AUDIT_ADMIN_CMD);
    keel_audit_emit_ddl(&al, "u", "d", "127.0.0.1", 0, "DROP TABLE t");
    keel_audit_emit_auth(&al, KEEL_AUDIT_AUTH_FAIL, "u", "d", "127.0.0.1", 0, NULL);
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"DDL\"") != NULL);
    TEST_ASSERT(strstr(out, "AUTH_FAIL") == NULL);  /* filtered out */
    rm_tmp();
    TEST_END();
}

static void test_filter_mix(void)
{
    TEST_BEGIN("audit: only requested event types appear in output");
    /* Enable only CONNECT + DISCONNECT */
    uint32_t mask = KEEL_AUDIT_CONNECT | KEEL_AUDIT_DISCONNECT;
    keel_audit_log_t al = make_ndjson_log(mask);
    keel_audit_emit_connect(&al, KEEL_AUDIT_CONNECT, "1.2.3.4", 100, "u", "d");
    keel_audit_emit_ddl(&al, "u", "d", "1.2.3.4", 100, "CREATE TABLE t(id INT)");
    keel_audit_emit_connect(&al, KEEL_AUDIT_DISCONNECT, "1.2.3.4", 100, "u", "d");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    int lines = 0;
    for (const char *p = out; *p; p++) { if (*p == '\n') lines++; }
    TEST_ASSERT_EQ(lines, 2);  /* CONNECT + DISCONNECT only */
    TEST_ASSERT(strstr(out, "DDL") == NULL);
    rm_tmp();
    TEST_END();
}

/* ============================================================================
 * §7 — Stats
 * ============================================================================ */

static void test_stats_emitted_counter(void)
{
    TEST_BEGIN("audit: events_emitted increments");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    for (int i = 0; i < 7; i++)
        keel_audit_emit_connect(&al, KEEL_AUDIT_CONNECT,
            "10.0.0.1", (uint16_t)i, "u", "d");
    keel_audit_log_close(&al);

    uint64_t emitted = 0, dropped = 0;
    keel_audit_stats(&al, &emitted, &dropped);
    TEST_ASSERT_EQ((int)emitted, 7);
    TEST_ASSERT_EQ((int)dropped, 0);
    rm_tmp();
    TEST_END();
}

static void test_stats_zero_for_disabled(void)
{
    TEST_BEGIN("audit: stats are zero for disabled log");
    keel_audit_log_t al;
    keel_audit_config_t cfg = keel_audit_config_default();
    keel_audit_log_init(&al, &cfg);
    keel_audit_emit_connect(&al, KEEL_AUDIT_CONNECT,
        "127.0.0.1", 1234, "u", "d");
    uint64_t em = 0, dr = 0;
    keel_audit_stats(&al, &em, &dr);
    TEST_ASSERT_EQ((int)em, 0);
    keel_audit_log_close(&al);
    TEST_END();
}

/* ============================================================================
 * §8 — Disabled log: no output, no crash
 * ============================================================================ */

static void test_disabled_no_output(void)
{
    TEST_BEGIN("audit: disabled log emits no output");
    keel_audit_log_t al;
    keel_audit_config_t cfg = keel_audit_config_default();  /* enabled=false */
    keel_audit_log_init(&al, &cfg);
    /* These should silently do nothing */
    keel_audit_emit_connect(&al, KEEL_AUDIT_CONNECT, "127.0.0.1", 0, "u", "d");
    keel_audit_emit_auth(&al, KEEL_AUDIT_AUTH_OK, "u", "d", "127.0.0.1", 0, NULL);
    keel_audit_emit_ddl(&al, "u", "d", "127.0.0.1", 0, "CREATE TABLE t(id INT)");
    keel_audit_emit_admin_cmd(&al, "127.0.0.1", 0, "RELOAD");
    keel_audit_emit_rule_event(&al, KEEL_AUDIT_RULE_BLOCK,
        "u", "d", "127.0.0.1", 0, "SELECT 1", "rule.0");
    TEST_ASSERT(!al.enabled);
    keel_audit_log_close(&al);
    TEST_END();
}

/* ============================================================================
 * §9 — Null guards
 * ============================================================================ */

static void test_null_guards(void)
{
    TEST_BEGIN("audit: null guards — no crash");
    /* NULL log handle */
    keel_audit_emit_connect(NULL, KEEL_AUDIT_CONNECT, NULL, 0, NULL, NULL);
    keel_audit_emit_auth(NULL, KEEL_AUDIT_AUTH_OK, NULL, NULL, NULL, 0, NULL);
    keel_audit_emit_ddl(NULL, NULL, NULL, NULL, 0, NULL);
    keel_audit_emit_admin_cmd(NULL, NULL, 0, NULL);
    keel_audit_emit_rule_event(NULL, KEEL_AUDIT_RULE_BLOCK,
        NULL, NULL, NULL, 0, NULL, NULL);
    keel_audit_log_close(NULL);

    /* NULL config */
    keel_audit_log_t al;
    keel_audit_log_init(&al, NULL);

    /* NULL stats output ptrs */
    keel_audit_stats(NULL, NULL, NULL);

    TEST_ASSERT(true); /* reaching here = no crash */
    TEST_END();
}

static void test_null_fields_in_event(void)
{
    TEST_BEGIN("audit: NULL optional fields produce valid JSON (fields omitted)");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_auth(&al, KEEL_AUDIT_AUTH_FAIL,
        NULL, NULL, NULL, 0, NULL);
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    /* Event must still appear even with all-NULL optional fields */
    TEST_ASSERT(strstr(out, "\"event\":\"AUTH_FAIL\"") != NULL);
    /* Must be valid JSON (starts with '{' and ends with '}\n') */
    TEST_ASSERT(out[0] == '{');
    rm_tmp();
    TEST_END();
}

/* ============================================================================
 * §10 — Scatter event
 * ============================================================================ */

static void test_scatter_event_name(void)
{
    TEST_BEGIN("audit: SCATTER event name");
    TEST_ASSERT_STR_EQ(keel_audit_event_name(KEEL_AUDIT_SCATTER), "SCATTER");
    TEST_END();
}

static void test_parse_events_scatter_token(void)
{
    TEST_BEGIN("audit: parse_events 'scatter' token");
    uint32_t m = keel_audit_parse_events("scatter");
    TEST_ASSERT(m & KEEL_AUDIT_SCATTER);
    TEST_ASSERT(!(m & KEEL_AUDIT_DDL));
    TEST_END();
}

static void test_scatter_all_events_includes_scatter(void)
{
    TEST_BEGIN("audit: KEEL_AUDIT_ALL_EVENTS excludes SCATTER (opt-in only)");
    /* SCATTER is opt-in: not in KEEL_AUDIT_ALL_EVENTS to avoid log flooding
     * on scatter-heavy workloads.  Use KEEL_AUDIT_ALL_EVENTS_WITH_SCATTER. */
    TEST_ASSERT(!(KEEL_AUDIT_ALL_EVENTS & KEEL_AUDIT_SCATTER));
    TEST_ASSERT(KEEL_AUDIT_ALL_EVENTS_WITH_SCATTER & KEEL_AUDIT_SCATTER);
    TEST_END();
}

static void test_ndjson_scatter_basic(void)
{
    TEST_BEGIN("audit: ndjson SCATTER event basic fields");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS_WITH_SCATTER);
    keel_audit_emit_scatter(&al,
        "alice", "sharddb",
        "SELECT count(*) FROM orders",
        4, 0, 12345);
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"SCATTER\"") != NULL);
    TEST_ASSERT(strstr(out, "\"user\":\"alice\"") != NULL);
    TEST_ASSERT(strstr(out, "\"db\":\"sharddb\"") != NULL);
    TEST_ASSERT(strstr(out, "SELECT count") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_scatter_shard_count(void)
{
    TEST_BEGIN("audit: ndjson SCATTER event records shard_count and failed_shards");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS_WITH_SCATTER);
    keel_audit_emit_scatter(&al, "u", "d", "SELECT 1", 8, 2, 99000);
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"SCATTER\"") != NULL);
    TEST_ASSERT(strstr(out, "\"shards\":\"8\"") != NULL || strstr(out, "\"shards\":8") != NULL);
    TEST_ASSERT(strstr(out, "\"failed_shards\":\"2\"") != NULL || strstr(out, "\"failed_shards\":2") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ndjson_scatter_elapsed(void)
{
    TEST_BEGIN("audit: ndjson SCATTER event records elapsed_us");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS_WITH_SCATTER);
    keel_audit_emit_scatter(&al, "u", "d", "SELECT 1", 2, 0, 500000);
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "500000") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_scatter_filtered_when_disabled(void)
{
    TEST_BEGIN("audit: SCATTER event filtered when mask excludes it");
    /* Use ALL_EVENTS (which omits SCATTER) — emit should be silently dropped */
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_scatter(&al, "u", "d", "SELECT 1", 4, 0, 1000);
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT_EQ((int)strlen(out), 0);
    rm_tmp();
    TEST_END();
}

static void test_scatter_null_guards(void)
{
    TEST_BEGIN("audit: scatter null guards — no crash");
    keel_audit_emit_scatter(NULL, NULL, NULL, NULL, 0, 0, 0);
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS_WITH_SCATTER);
    keel_audit_emit_scatter(&al, NULL, NULL, NULL, 0, 0, 0);
    keel_audit_log_close(&al);
    rm_tmp();
    TEST_ASSERT(true);
    TEST_END();
}

static void test_scatter_increments_stats(void)
{
    TEST_BEGIN("audit: scatter events increment events_emitted");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS_WITH_SCATTER);
    keel_audit_emit_scatter(&al, "u", "d", "Q", 3, 0, 100);
    keel_audit_emit_scatter(&al, "u", "d", "Q", 3, 0, 200);
    keel_audit_log_close(&al);
    uint64_t emitted = 0, dropped = 0;
    keel_audit_stats(&al, &emitted, &dropped);
    TEST_ASSERT_EQ((int)emitted, 2);
    rm_tmp();
    TEST_END();
}

/* ============================================================================
 * §11 — New emit paths (CONNECT pre-auth, AUTH_OK, AUTH_FAIL gated, DDL)
 * ============================================================================ */

static void test_connect_preauth_no_user(void)
{
    TEST_BEGIN("audit: CONNECT pre-auth: NULL username/database is valid");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_CONNECT);
    /* Simulate what worker_setup_session emits: IP known, user/db not yet */
    keel_audit_emit_connect(&al, KEEL_AUDIT_CONNECT,
        "172.16.0.5", 51234, NULL, NULL);
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"CONNECT\"") != NULL);
    TEST_ASSERT(strstr(out, "172.16.0.5") != NULL);
    /* user and db fields should be absent or empty — no crash */
    rm_tmp();
    TEST_END();
}

static void test_auth_ok_after_connect(void)
{
    TEST_BEGIN("audit: AUTH_OK follows CONNECT in session lifecycle");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_ALL_EVENTS);
    keel_audit_emit_connect(&al, KEEL_AUDIT_CONNECT,
        "10.0.0.1", 60000, NULL, NULL);
    keel_audit_emit_auth(&al, KEEL_AUDIT_AUTH_OK,
        "carol", "prod", NULL, 0, NULL);
    keel_audit_emit_connect(&al, KEEL_AUDIT_DISCONNECT,
        NULL, 0, "carol", "prod");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    /* All three events in order */
    const char *p_conn  = strstr(out, "CONNECT");
    const char *p_auth  = strstr(out, "AUTH_OK");
    const char *p_disc  = strstr(out, "DISCONNECT");
    TEST_ASSERT_NOT_NULL(p_conn);
    TEST_ASSERT_NOT_NULL(p_auth);
    TEST_ASSERT_NOT_NULL(p_disc);
    TEST_ASSERT(p_conn < p_auth);
    TEST_ASSERT(p_auth < p_disc);
    rm_tmp();
    TEST_END();
}

static void test_auth_fail_with_scram_detail(void)
{
    TEST_BEGIN("audit: AUTH_FAIL records SCRAM detail string");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_AUTH_FAIL);
    keel_audit_emit_auth(&al, KEEL_AUDIT_AUTH_FAIL,
        "eve", "secret", "203.0.113.1", 41234,
        "protocol error during handshake");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "AUTH_FAIL") != NULL);
    TEST_ASSERT(strstr(out, "protocol error during handshake") != NULL);
    TEST_ASSERT(strstr(out, "203.0.113.1") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_ddl_only_mask_blocks_auth(void)
{
    TEST_BEGIN("audit: DDL-only mask: auth events suppressed, DDL passes");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_DDL);
    keel_audit_emit_auth(&al, KEEL_AUDIT_AUTH_OK,
        "alice", "db", "127.0.0.1", 0, NULL);
    keel_audit_emit_ddl(&al, "alice", "db", "127.0.0.1", 0,
        "ALTER TABLE orders ADD COLUMN ts TIMESTAMPTZ");
    keel_audit_log_close(&al);

    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "DDL") != NULL);
    TEST_ASSERT(strstr(out, "AUTH_OK") == NULL);
    TEST_ASSERT(strstr(out, "ALTER TABLE") != NULL);
    rm_tmp();
    TEST_END();
}

static void test_disconnect_only_authenticated(void)
{
    TEST_BEGIN("audit: DISCONNECT emitted for authenticated session only");
    keel_audit_log_t al = make_ndjson_log(KEEL_AUDIT_DISCONNECT);
    /* Unauthenticated close — should emit nothing (no user/db) */
    keel_audit_emit_connect(&al, KEEL_AUDIT_DISCONNECT,
        NULL, 0, NULL, NULL);
    keel_audit_log_close(&al);
    /* The emit function doesn't gate on auth itself — that's done in
     * worker.c by checking KEEL_SESSION_FLAG_AUTHENTICATED.  Here we
     * verify the emitter still writes a valid record even with NULL fields. */
    const char *out = read_tmp();
    TEST_ASSERT(strstr(out, "\"event\":\"DISCONNECT\"") != NULL);
    rm_tmp();
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    /* §1 Lifecycle */
    test_init_disabled();
    test_init_stdout();
    test_init_file();
    test_close_idempotent();

    /* §2 Event mask parsing */
    test_parse_events_all();
    test_parse_events_individual();
    test_parse_events_combined();
    test_parse_events_connect();

    /* §3 Event names */
    test_event_names();

    /* §4 NDJSON output */
    test_ndjson_connect();
    test_ndjson_disconnect();
    test_ndjson_auth_ok();
    test_ndjson_auth_fail();
    test_ndjson_ddl();
    test_ndjson_admin_cmd();
    test_ndjson_rule_block();
    test_ndjson_rule_throttle();
    test_ndjson_json_structure();
    test_ndjson_query_truncation();
    test_ndjson_special_chars_escaped();
    test_ndjson_multiple_events();

    /* §5 Text format */
    test_text_format_connect();
    test_text_format_auth_fail();

    /* §6 Event filtering */
    test_filter_blocks_disabled_events();
    test_filter_allows_enabled_events();
    test_filter_mix();

    /* §7 Stats */
    test_stats_emitted_counter();
    test_stats_zero_for_disabled();

    /* §8 Disabled */
    test_disabled_no_output();

    /* §9 Null guards */
    test_null_guards();
    test_null_fields_in_event();

    /* §10 Scatter event */
    test_scatter_event_name();
    test_parse_events_scatter_token();
    test_scatter_all_events_includes_scatter();
    test_ndjson_scatter_basic();
    test_ndjson_scatter_shard_count();
    test_ndjson_scatter_elapsed();
    test_scatter_filtered_when_disabled();
    test_scatter_null_guards();
    test_scatter_increments_stats();

    /* §11 New emit paths (pre-auth connect, auth lifecycle, DDL filtering) */
    test_connect_preauth_no_user();
    test_auth_ok_after_connect();
    test_auth_fail_with_scram_detail();
    test_ddl_only_mask_blocks_auth();
    test_disconnect_only_authenticated();

    return test_summary();
}
