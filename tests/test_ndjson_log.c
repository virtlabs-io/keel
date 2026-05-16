/**
 * @file test_ndjson_log.c
 * @brief Unit tests for NDJSON log record output.
 *
 * Validates keel_log_record_write_json() produces well-formed NDJSON lines
 * with correct field names, escaping, and optional field presence.
 */

#include "test_utils.h"
#include "keel/log/log.h"
#include "keel/log/log_plugin.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/* ============================================================================
 * Helper: write a record to a temp buffer via open_memstream
 * ============================================================================ */

static char* record_to_json(const keel_log_record_t* rec, size_t* out_len) {
    char* buf = NULL;
    size_t buf_len = 0;
    FILE* f = open_memstream(&buf, &buf_len);
    if (!f) return NULL;

    keel_log_record_write_json(f, rec);
    fclose(f);

    if (out_len) *out_len = buf_len;
    return buf;
}

/* ============================================================================
 * Basic Structure
 * ============================================================================ */

static void test_ndjson_basic_fields(void) {
    TEST_BEGIN("NDJSON basic fields present");

    keel_log_record_t rec = {
        .level    = KEEL_LOG_INFO,
        .category = KEEL_LOG_CAT_CORE,
        .ts_sec   = 1700000000,
        .ts_nsec  = 123456789,
        .file     = "/path/to/session.c",
        .line     = 42,
        .func     = "handle_query",
        .message  = "query completed",
        .message_len = strlen("query completed"),
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    /* Required fields */
    TEST_ASSERT(strstr(json, "\"ts\":") != NULL);
    TEST_ASSERT(strstr(json, "\"level\":\"INFO\"") != NULL);
    TEST_ASSERT(strstr(json, "\"cat\":\"CORE\"") != NULL);

    /* Source location (basename, not full path) */
    TEST_ASSERT(strstr(json, "\"file\":\"session.c\"") != NULL);
    TEST_ASSERT(strstr(json, "\"line\":42") != NULL);
    TEST_ASSERT(strstr(json, "\"func\":\"handle_query\"") != NULL);

    /* Message */
    TEST_ASSERT(strstr(json, "\"msg\":\"query completed\"") != NULL);

    /* Must end with newline (NDJSON) */
    size_t len = strlen(json);
    TEST_ASSERT(len > 0);
    TEST_ASSERT(json[len - 1] == '\n');

    free(json);
    TEST_END();
}

static void test_ndjson_timestamp_format(void) {
    TEST_BEGIN("NDJSON timestamp ISO 8601 format");

    keel_log_record_t rec = {
        .level    = KEEL_LOG_DEBUG,
        .category = KEEL_LOG_CAT_SQL,
        .ts_sec   = 1700000000,  /* 2023-11-14T22:13:20 UTC */
        .ts_nsec  = 500000000,   /* 500ms → 500000 μs */
        .message  = "test",
        .message_len = 4,
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    /* Should have ISO 8601 with microseconds */
    TEST_ASSERT(strstr(json, "2023-11-14T22:13:20.500000Z") != NULL);

    free(json);
    TEST_END();
}

static void test_ndjson_all_levels(void) {
    TEST_BEGIN("NDJSON all log levels");

    const char* expected[] = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"};

    for (int i = 0; i < 6; i++) {
        keel_log_record_t rec = {
            .level    = (keel_log_level_t)i,
            .category = KEEL_LOG_CAT_CORE,
            .ts_sec   = 1700000000,
            .message  = "test",
            .message_len = 4,
        };

        char* json = record_to_json(&rec, NULL);
        TEST_ASSERT_NOT_NULL(json);

        char needle[64];
        snprintf(needle, sizeof(needle), "\"level\":\"%s\"", expected[i]);
        TEST_ASSERT(strstr(json, needle) != NULL);

        free(json);
    }

    TEST_END();
}

/* ============================================================================
 * Structured Fields
 * ============================================================================ */

static void test_ndjson_network_fields(void) {
    TEST_BEGIN("NDJSON network fields");

    keel_log_record_t rec = {
        .level    = KEEL_LOG_INFO,
        .category = KEEL_LOG_CAT_CONN,
        .ts_sec   = 1700000000,
        .message  = "connected",
        .message_len = 9,
        .src_addr = "192.168.1.100",
        .src_port = 45678,
        .dst_addr = "10.0.0.1",
        .dst_port = 5432,
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    TEST_ASSERT(strstr(json, "\"src_addr\":\"192.168.1.100\"") != NULL);
    TEST_ASSERT(strstr(json, "\"src_port\":45678") != NULL);
    TEST_ASSERT(strstr(json, "\"dst_addr\":\"10.0.0.1\"") != NULL);
    TEST_ASSERT(strstr(json, "\"dst_port\":5432") != NULL);

    free(json);
    TEST_END();
}

static void test_ndjson_user_database(void) {
    TEST_BEGIN("NDJSON user and database fields");

    keel_log_record_t rec = {
        .level    = KEEL_LOG_INFO,
        .category = KEEL_LOG_CAT_SQL,
        .ts_sec   = 1700000000,
        .message  = "query",
        .message_len = 5,
        .username = "app_user",
        .database = "production_db",
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    TEST_ASSERT(strstr(json, "\"user\":\"app_user\"") != NULL);
    TEST_ASSERT(strstr(json, "\"db\":\"production_db\"") != NULL);

    free(json);
    TEST_END();
}

static void test_ndjson_query_text(void) {
    TEST_BEGIN("NDJSON query text");

    const char* sql = "SELECT * FROM users WHERE id = 1";
    keel_log_record_t rec = {
        .level    = KEEL_LOG_INFO,
        .category = KEEL_LOG_CAT_SQL,
        .ts_sec   = 1700000000,
        .message  = "execute",
        .message_len = 7,
        .query    = sql,
        .query_len = strlen(sql),
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    TEST_ASSERT(strstr(json, "\"query\":\"SELECT * FROM users WHERE id = 1\"") != NULL);

    free(json);
    TEST_END();
}

/* ============================================================================
 * JSON Escaping
 * ============================================================================ */

static void test_ndjson_escape_quotes(void) {
    TEST_BEGIN("NDJSON escapes double quotes in message");

    keel_log_record_t rec = {
        .level    = KEEL_LOG_WARN,
        .category = KEEL_LOG_CAT_SQL,
        .ts_sec   = 1700000000,
        .message  = "bad value: \"hello\"",
        .message_len = strlen("bad value: \"hello\""),
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    /* Should have escaped quotes */
    TEST_ASSERT(strstr(json, "\\\"hello\\\"") != NULL);

    free(json);
    TEST_END();
}

static void test_ndjson_escape_newlines(void) {
    TEST_BEGIN("NDJSON escapes newlines in query");

    const char* sql = "SELECT\n  *\n  FROM users";
    keel_log_record_t rec = {
        .level    = KEEL_LOG_INFO,
        .category = KEEL_LOG_CAT_SQL,
        .ts_sec   = 1700000000,
        .message  = "query",
        .message_len = 5,
        .query    = sql,
        .query_len = strlen(sql),
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    /* Newlines must be escaped, not literal */
    TEST_ASSERT(strstr(json, "\\n") != NULL);
    /* The output should be a single line (NDJSON) */
    char* first_nl = strchr(json, '\n');
    /* Should only have the trailing newline */
    TEST_ASSERT_NOT_NULL(first_nl);
    TEST_ASSERT(first_nl[1] == '\0');

    free(json);
    TEST_END();
}

static void test_ndjson_escape_backslash(void) {
    TEST_BEGIN("NDJSON escapes backslashes");

    keel_log_record_t rec = {
        .level    = KEEL_LOG_INFO,
        .category = KEEL_LOG_CAT_CORE,
        .ts_sec   = 1700000000,
        .message  = "path: C:\\data\\file",
        .message_len = strlen("path: C:\\data\\file"),
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    /* Each backslash should be doubled */
    TEST_ASSERT(strstr(json, "C:\\\\data\\\\file") != NULL);

    free(json);
    TEST_END();
}

/* ============================================================================
 * Optional Field Omission
 * ============================================================================ */

static void test_ndjson_minimal_record(void) {
    TEST_BEGIN("NDJSON minimal record (no optional fields)");

    keel_log_record_t rec = {
        .level    = KEEL_LOG_ERROR,
        .category = KEEL_LOG_CAT_POOL,
        .ts_sec   = 1700000000,
        .message  = "pool exhausted",
        .message_len = strlen("pool exhausted"),
        /* All optional fields NULL/0 */
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    /* Should NOT contain optional fields */
    TEST_ASSERT(strstr(json, "\"src_addr\"") == NULL);
    TEST_ASSERT(strstr(json, "\"dst_addr\"") == NULL);
    TEST_ASSERT(strstr(json, "\"user\"") == NULL);
    TEST_ASSERT(strstr(json, "\"db\"") == NULL);
    TEST_ASSERT(strstr(json, "\"query\"") == NULL);
    TEST_ASSERT(strstr(json, "\"query_tree\"") == NULL);

    /* Should contain required fields */
    TEST_ASSERT(strstr(json, "\"level\":\"ERROR\"") != NULL);
    TEST_ASSERT(strstr(json, "\"cat\":\"POOL\"") != NULL);
    TEST_ASSERT(strstr(json, "\"msg\":\"pool exhausted\"") != NULL);

    free(json);
    TEST_END();
}

static void test_ndjson_query_tree(void) {
    TEST_BEGIN("NDJSON query_tree field when present");

    const char* tree = "{\"type\":\"SELECT\",\"columns\":[\"*\"]}";
    keel_log_record_t rec = {
        .level    = KEEL_LOG_DEBUG,
        .category = KEEL_LOG_CAT_SQL,
        .ts_sec   = 1700000000,
        .message  = "parsed",
        .message_len = 6,
        .query_tree = tree,
        .query_tree_len = strlen(tree),
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    TEST_ASSERT(strstr(json, "\"query_tree\":") != NULL);

    free(json);
    TEST_END();
}

/* ============================================================================
 * Fallback Timestamp
 * ============================================================================ */

static void test_ndjson_auto_timestamp(void) {
    TEST_BEGIN("NDJSON auto-generates timestamp when ts_sec=0");

    keel_log_record_t rec = {
        .level    = KEEL_LOG_INFO,
        .category = KEEL_LOG_CAT_CORE,
        .ts_sec   = 0,
        .ts_nsec  = 0,
        .message  = "test",
        .message_len = 4,
    };

    char* json = record_to_json(&rec, NULL);
    TEST_ASSERT_NOT_NULL(json);

    /* Should still have a timestamp (auto-generated from clock) */
    TEST_ASSERT(strstr(json, "\"ts\":\"20") != NULL);

    free(json);
    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    /* Basic structure */
    test_ndjson_basic_fields();
    test_ndjson_timestamp_format();
    test_ndjson_all_levels();

    /* Structured fields */
    test_ndjson_network_fields();
    test_ndjson_user_database();
    test_ndjson_query_text();

    /* Escaping */
    test_ndjson_escape_quotes();
    test_ndjson_escape_newlines();
    test_ndjson_escape_backslash();

    /* Optional field omission */
    test_ndjson_minimal_record();
    test_ndjson_query_tree();

    /* Timestamp fallback */
    test_ndjson_auto_timestamp();

    return test_summary();
}
