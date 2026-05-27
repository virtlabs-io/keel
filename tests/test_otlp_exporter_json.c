/**
 * @file test_otlp_exporter_json.c
 * @brief Verify JSON serialization of exporter self-stats.
 */

#include "test_utils.h"
#include "keel_exporter_json.h"
#include "keel_exporter_stats.h"
#include "keel_otlp_http.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static void test_initial_state(void)
{
    keel_exporter_stats_t s;
    keel_exporter_stats_init(&s, 4);
    char buf[1024];
    int n = keel_exporter_stats_to_json(&s, buf, sizeof(buf));
    TEST_ASSERT(n > 0 && n < (int)sizeof(buf));

    /* All required keys present. */
    static const char* required[] = {
        "\"export_queue_depth\":0",
        "\"export_queue_capacity\":4",
        "\"export_snapshots_dropped_total\":0",
        "\"export_attempts_total\":0",
        "\"export_success_total\":0",
        "\"export_failure_total\":0",
        "\"export_timeout_total\":0",
        "\"last_export_duration_ns\":0",
        "\"last_export_status\":\"none\"",
        "\"last_export_error\":null",
        "\"last_success_timestamp_ms\":0",
        "\"last_failure_timestamp_ms\":0",
    };
    for (size_t i = 0; i < sizeof(required) / sizeof(required[0]); ++i)
        TEST_ASSERT(strstr(buf, required[i]) != NULL);
}

static void test_success_state(void)
{
    keel_exporter_stats_t s;
    keel_exporter_stats_init(&s, 8);
    atomic_store(&s.attempts,             5);
    atomic_store(&s.successes,            5);
    atomic_store(&s.queue_depth,          2);
    atomic_store(&s.last_duration_ns,     123456);
    atomic_store(&s.last_status,          (int64_t)KEEL_OTLP_HTTP_OK);
    atomic_store(&s.last_success_unix_ms, 1748169600000ULL);

    char buf[1024];
    int n = keel_exporter_stats_to_json(&s, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(strstr(buf, "\"export_attempts_total\":5") != NULL);
    TEST_ASSERT(strstr(buf, "\"export_success_total\":5") != NULL);
    TEST_ASSERT(strstr(buf, "\"export_failure_total\":0") != NULL);
    TEST_ASSERT(strstr(buf, "\"export_queue_depth\":2") != NULL);
    TEST_ASSERT(strstr(buf, "\"export_queue_capacity\":8") != NULL);
    TEST_ASSERT(strstr(buf, "\"last_export_duration_ns\":123456") != NULL);
    TEST_ASSERT(strstr(buf, "\"last_export_status\":\"ok\"") != NULL);
    TEST_ASSERT(strstr(buf, "\"last_export_error\":null") != NULL);
    TEST_ASSERT(strstr(buf, "\"last_success_timestamp_ms\":1748169600000") != NULL);
}

static void test_failure_state(void)
{
    keel_exporter_stats_t s;
    keel_exporter_stats_init(&s, 4);
    atomic_store(&s.attempts,             3);
    atomic_store(&s.successes,            1);
    atomic_store(&s.failures,             2);
    atomic_store(&s.timeouts,             1);
    atomic_store(&s.dropped,              7);
    atomic_store(&s.last_status,          (int64_t)KEEL_OTLP_HTTP_TIMEOUT);
    atomic_store(&s.last_failure_unix_ms, 1748169700000ULL);

    char buf[1024];
    int n = keel_exporter_stats_to_json(&s, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(strstr(buf, "\"export_failure_total\":2") != NULL);
    TEST_ASSERT(strstr(buf, "\"export_timeout_total\":1") != NULL);
    TEST_ASSERT(strstr(buf, "\"export_snapshots_dropped_total\":7") != NULL);
    TEST_ASSERT(strstr(buf, "\"last_export_status\":\"timeout\"") != NULL);
    TEST_ASSERT(strstr(buf, "\"last_export_error\":\"timeout\"") != NULL);
    TEST_ASSERT(strstr(buf, "\"last_failure_timestamp_ms\":1748169700000") != NULL);
}

static void test_status_str_mapping(void)
{
    TEST_ASSERT_STR_EQ(keel_otlp_http_result_str(KEEL_OTLP_HTTP_OK),             "ok");
    TEST_ASSERT_STR_EQ(keel_otlp_http_result_str(KEEL_OTLP_HTTP_CONNECT_FAILED), "connect_failed");
    TEST_ASSERT_STR_EQ(keel_otlp_http_result_str(KEEL_OTLP_HTTP_TIMEOUT),        "timeout");
    TEST_ASSERT_STR_EQ(keel_otlp_http_result_str(KEEL_OTLP_HTTP_PROTOCOL_ERROR), "protocol_error");
    TEST_ASSERT_STR_EQ(keel_otlp_http_result_str(KEEL_OTLP_HTTP_SERVER_REJECT),  "server_reject");
    TEST_ASSERT_STR_EQ(keel_otlp_http_result_str(KEEL_OTLP_HTTP_SERVER_RETRY),   "server_retry");
    TEST_ASSERT_STR_EQ(keel_otlp_http_result_str(KEEL_OTLP_HTTP_NOT_IMPLEMENTED),"not_implemented");
    TEST_ASSERT_STR_EQ(keel_otlp_http_result_str(99999),                         "unknown");
}

static void test_truncation_signal(void)
{
    keel_exporter_stats_t s;
    keel_exporter_stats_init(&s, 4);
    char tiny[16];
    int  n = keel_exporter_stats_to_json(&s, tiny, sizeof(tiny));
    TEST_ASSERT(n >= (int)sizeof(tiny));  /* would-have-written semantics */
}

static void test_invalid_args(void)
{
    keel_exporter_stats_t s;
    char buf[64];
    TEST_ASSERT_EQ(keel_exporter_stats_to_json(NULL, buf, sizeof(buf)), -1);
    TEST_ASSERT_EQ(keel_exporter_stats_to_json(&s, NULL, sizeof(buf)), -1);
    TEST_ASSERT_EQ(keel_exporter_stats_to_json(&s, buf, 0), -1);
}

int main(void) {
    test_initial_state();
    test_success_state();
    test_failure_state();
    test_status_str_mapping();
    test_truncation_signal();
    test_invalid_args();
    return test_summary();
}
