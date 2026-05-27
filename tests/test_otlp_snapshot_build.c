/**
 * @file test_otlp_snapshot_build.c
 * @brief Unit tests for keel_otlp_snapshot_from_stats().
 */

#include "test_utils.h"

#include "keel/core/stats.h"
#include "../src/observability/otlp/keel_otlp_aggregator.h"
#include "../src/observability/otlp/keel_otlp_encode.h"

#include <stdint.h>
#include <string.h>

static const keel_otlp_metric_sample_t* find_metric(
    const keel_otlp_snapshot_t* s, const char* name)
{
    for (size_t i = 0; i < s->metric_count; i++) {
        if (strcmp(s->metrics[i].name, name) == 0) return &s->metrics[i];
    }
    return NULL;
}

static void test_invalid_args(void) {
    keel_otlp_snapshot_t out;
    keel_stats_snapshot_t in = {0};
    TEST_ASSERT_EQ(-1, keel_otlp_snapshot_from_stats(NULL, 0, 0, &out));
    TEST_ASSERT_EQ(-1, keel_otlp_snapshot_from_stats(&in,  0, 0, NULL));
}

static void test_empty_snapshot_emits_zero_metrics(void) {
    keel_stats_snapshot_t in = {0};
    keel_otlp_snapshot_t  out;
    TEST_ASSERT_EQ(0, keel_otlp_snapshot_from_stats(&in, 111, 222, &out));
    TEST_ASSERT_EQ((uint64_t)111, out.start_time_unix_nano);
    TEST_ASSERT_EQ((uint64_t)222, out.time_unix_nano);
    TEST_ASSERT(out.metric_count >= 25);
    TEST_ASSERT(out.metric_count <= KEEL_OTLP_MAX_METRICS);

    const keel_otlp_metric_sample_t* m;
    m = find_metric(&out, "keel_queries_total");
    TEST_ASSERT_NOT_NULL(m); TEST_ASSERT_EQ((uint64_t)0, m->value);
    m = find_metric(&out, "keel_sessions_active");
    TEST_ASSERT_NOT_NULL(m); TEST_ASSERT_EQ((uint64_t)0, m->value);
    m = find_metric(&out, "keel_uptime_seconds");
    TEST_ASSERT_NOT_NULL(m); TEST_ASSERT_EQ((uint64_t)0, m->value);
}

static void test_populated_snapshot_round_trips_values(void) {
    keel_stats_snapshot_t in = {0};
    in.num_workers = 4;
    in.uptime_ns   = 123 * 1000000000LL;  /* 123 s */

    keel_counter_add(&in.basic.queries_total,       1000);
    keel_counter_add(&in.basic.queries_read,         700);
    keel_counter_add(&in.basic.queries_write,        250);
    keel_counter_add(&in.basic.queries_tx,            50);
    keel_counter_add(&in.basic.errors_total,           7);
    keel_counter_add(&in.basic.errors_auth,            1);
    keel_counter_add(&in.basic.errors_proto,           2);
    keel_counter_add(&in.basic.errors_backend,         3);
    keel_counter_add(&in.basic.errors_timeout,         1);
    keel_counter_add(&in.basic.bytes_recv,         8 * 1024);
    keel_counter_add(&in.basic.bytes_sent,        16 * 1024);
    keel_counter_add(&in.basic.bytes_backend_recv, 4 * 1024);
    keel_counter_add(&in.basic.bytes_backend_sent, 2 * 1024);
    keel_counter_add(&in.basic.bytes_spliced,     32 * 1024);
    keel_counter_add(&in.basic.pool_borrows,        500);
    keel_counter_add(&in.basic.pool_returns,        490);
    keel_counter_add(&in.basic.pool_creates,         20);
    keel_counter_add(&in.basic.pool_destroys,        10);
    keel_counter_add(&in.basic.pool_hits,           480);
    keel_counter_add(&in.basic.pool_misses,          20);
    keel_counter_add(&in.basic.sessions_created,    100);
    keel_counter_add(&in.basic.sessions_closed,      90);
    keel_counter_add(&in.basic.loop_iterations,   10000);
    keel_counter_add(&in.basic.ops_submitted,      9000);
    keel_counter_add(&in.basic.ops_completed,      8999);
    keel_counter_add(&in.basic.discard_all_count,    12);
    keel_counter_add(&in.basic.state_sync_count,      8);
    keel_gauge_set(&in.basic.sessions_active,        10);
    keel_gauge_set(&in.basic.sessions_pinned,         3);
    keel_gauge_set(&in.basic.backends_cleaning,       1);

    keel_otlp_snapshot_t out;
    TEST_ASSERT_EQ(0, keel_otlp_snapshot_from_stats(&in, 1000, 2000, &out));

    const struct { const char* name; uint64_t expected; } expected[] = {
        { "keel_queries_total",            1000 },
        { "keel_queries_read_total",        700 },
        { "keel_queries_write_total",       250 },
        { "keel_queries_tx_total",           50 },
        { "keel_errors_total",                7 },
        { "keel_errors_auth_total",           1 },
        { "keel_errors_proto_total",          2 },
        { "keel_errors_backend_total",        3 },
        { "keel_errors_timeout_total",        1 },
        { "keel_bytes_recv_total",         8192 },
        { "keel_bytes_sent_total",        16384 },
        { "keel_bytes_backend_recv_total", 4096 },
        { "keel_bytes_backend_sent_total", 2048 },
        { "keel_bytes_spliced_total",     32768 },
        { "keel_pool_borrows_total",        500 },
        { "keel_pool_returns_total",        490 },
        { "keel_pool_creates_total",         20 },
        { "keel_pool_destroys_total",        10 },
        { "keel_pool_hits_total",           480 },
        { "keel_pool_misses_total",          20 },
        { "keel_sessions_created_total",    100 },
        { "keel_sessions_closed_total",      90 },
        { "keel_loop_iterations_total",   10000 },
        { "keel_ops_submitted_total",      9000 },
        { "keel_ops_completed_total",      8999 },
        { "keel_discard_all_total",          12 },
        { "keel_state_sync_total",            8 },
        { "keel_sessions_active",            10 },
        { "keel_sessions_pinned",             3 },
        { "keel_backends_cleaning",           1 },
        { "keel_uptime_seconds",            123 },
        { "keel_workers",                     4 },
    };
    for (size_t i = 0; i < sizeof(expected)/sizeof(expected[0]); i++) {
        const keel_otlp_metric_sample_t* m = find_metric(&out, expected[i].name);
        if (!m) {
            fprintf(stderr, "missing metric: %s\n", expected[i].name);
        }
        TEST_ASSERT_NOT_NULL(m);
        if (m->value != expected[i].expected) {
            fprintf(stderr, "metric %s: expected %llu got %llu\n",
                    expected[i].name,
                    (unsigned long long)expected[i].expected,
                    (unsigned long long)m->value);
        }
        TEST_ASSERT_EQ(expected[i].expected, m->value);
    }
}

static void test_negative_gauge_clamped_to_zero(void) {
    keel_stats_snapshot_t in = {0};
    keel_gauge_set(&in.basic.sessions_active, -5);
    keel_otlp_snapshot_t out;
    TEST_ASSERT_EQ(0, keel_otlp_snapshot_from_stats(&in, 0, 0, &out));
    const keel_otlp_metric_sample_t* m = find_metric(&out, "keel_sessions_active");
    TEST_ASSERT_NOT_NULL(m);
    TEST_ASSERT_EQ((uint64_t)0, m->value);
}

int main(void) {
    test_invalid_args();
    test_empty_snapshot_emits_zero_metrics();
    test_populated_snapshot_round_trips_values();
    test_negative_gauge_clamped_to_zero();
    return test_summary();
}
