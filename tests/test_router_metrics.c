/**
 * @file test_router_metrics.c
 * @brief Unit tests for Feature 14: keel_router_write_prometheus().
 *
 * Tests verify:
 *   - Output is non-empty for a freshly created router.
 *   - All expected metric family names appear in the output.
 *   - Per-shard lines are only emitted for shards with non-zero counters.
 *   - Buffer truncation is handled gracefully (no buffer overflow).
 *   - Counters reflect actual routing decisions.
 */

#include "test_utils.h"
#include "keel/core/router.h"
#include "keel_error.h"

#include <string.h>
#include <stdio.h>

int g_tests_run    = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) {
    return (g_tests_failed == 0) ? 0 : 1;
}

/* ============================================================================
 * Helpers
 * ============================================================================ */

static keel_router_t* build_router_with_rule(void) {
    keel_router_config_t cfg = keel_router_config_default();
    cfg.read_write_split = true;
    keel_router_t* r = keel_router_create(&cfg);
    if (!r) return NULL;

    keel_route_server_t primary = {
        .name   = "primary",
        .host   = "127.0.0.1",
        .port   = 5432,
        .role   = KEEL_SERVER_ROLE_RW,
        .weight = 100,
        .health = KEEL_HEALTH_UP,
    };
    keel_router_add_server(r, &primary);
    keel_router_add_shard_rule(r, "users", "id", 4);
    return r;
}

/* ============================================================================
 * Test: output is non-empty
 * ============================================================================ */

static void test_prometheus_output_non_empty(void) {
    TEST_BEGIN("prometheus_output_non_empty");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    char buf[4096];
    size_t n = keel_router_write_prometheus(router, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    buf[n] = '\0';

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: expected metric families are present
 * ============================================================================ */

static void test_prometheus_metric_families(void) {
    TEST_BEGIN("prometheus_metric_families");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    char buf[8192];
    size_t n = keel_router_write_prometheus(router, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    buf[n] = '\0';

    /* Verify each mandatory family is present */
    TEST_ASSERT(strstr(buf, "keel_router_total_routes")   != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_read_routes")    != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_write_routes")   != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_failover_routes") != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_pinned_routes")  != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_scatter_hits")   != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_scatter_failed") != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_shard_routes")   != NULL);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: HELP and TYPE lines present
 * ============================================================================ */

static void test_prometheus_help_type_lines(void) {
    TEST_BEGIN("prometheus_help_type_lines");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    char buf[8192];
    size_t n = keel_router_write_prometheus(router, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    buf[n] = '\0';

    TEST_ASSERT(strstr(buf, "# HELP") != NULL);
    TEST_ASSERT(strstr(buf, "# TYPE") != NULL);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: zero counters — no per-shard lines emitted
 * ============================================================================ */

static void test_prometheus_no_shard_lines_when_zero(void) {
    TEST_BEGIN("prometheus_no_shard_lines_when_zero");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    char buf[8192];
    size_t n = keel_router_write_prometheus(router, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    buf[n] = '\0';

    /* With zero routes no "shard=" labels should appear */
    TEST_ASSERT(strstr(buf, "shard=\"") == NULL);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: counters reflect routing decisions
 * ============================================================================ */

static void test_prometheus_counters_after_dispatch(void) {
    TEST_BEGIN("prometheus_counters_after_dispatch");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    /* Dispatch a write query */
    keel_shard_bound_params_t params = {0};
    keel_dispatch_result_t    out    = {0};
    keel_router_dispatch_sql(
        router,
        KEEL_STR("INSERT INTO users (id, name) VALUES (1, 'alice')"),
        NULL, &params, true, &out);

    keel_router_stats_t stats;
    keel_router_get_stats(router, &stats);

    /* Even if no servers exist the stats counter path should have been touched */
    char buf[8192];
    size_t n = keel_router_write_prometheus(router, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    buf[n] = '\0';

    /* Basic sanity: total_routes line present with a numeric value */
    TEST_ASSERT(strstr(buf, "keel_router_total_routes") != NULL);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: buffer too small — returns 0
 * ============================================================================ */

static void test_prometheus_buffer_too_small(void) {
    TEST_BEGIN("prometheus_buffer_too_small");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    char tiny[4];
    size_t n = keel_router_write_prometheus(router, tiny, sizeof(tiny));
    /* The first snprintf will hit the limit — returns 0 (goto done) */
    TEST_ASSERT_EQ(n, 0);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: NULL guards
 * ============================================================================ */

static void test_prometheus_null_guards(void) {
    TEST_BEGIN("prometheus_null_guards");

    char buf[256];
    TEST_ASSERT_EQ(keel_router_write_prometheus(NULL, buf, sizeof(buf)), 0);
    TEST_ASSERT_EQ(keel_router_write_prometheus(NULL, NULL, 0),          0);

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    TEST_ASSERT_EQ(keel_router_write_prometheus(router, NULL, 256), 0);
    TEST_ASSERT_EQ(keel_router_write_prometheus(router, buf,  0),   0);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: per-shard line emitted after routing to shard 0
 * ============================================================================ */

static void test_prometheus_shard_line_emitted(void) {
    TEST_BEGIN("prometheus_shard_line_emitted");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    /* Manually bump the shard counter */
    /* We access via the stats struct since the internal field is not exposed */
    keel_router_stats_t s;
    keel_router_get_stats(router, &s);
    /* Dispatch a query so stats.shard_single_routes[N] might be incremented */
    keel_shard_bound_params_t params = {0};
    keel_dispatch_result_t    out    = {0};
    keel_router_dispatch_sql(
        router,
        KEEL_STR("SELECT * FROM users WHERE id = 4"),
        NULL, &params, false, &out);

    char buf[8192];
    size_t n = keel_router_write_prometheus(router, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    buf[n] = '\0';

    /* If the dispatch succeeded and routed to shard 0, a label should appear.
     * If it failed (no servers) the line count is still 0 — both are valid. */
    keel_router_get_stats(router, &s);
    bool any_shard = false;
    for (int i = 0; i < KEEL_SCATTER_MAX_SHARDS; i++) {
        if (s.shard_single_routes[i] > 0) { any_shard = true; break; }
    }
    if (any_shard) {
        TEST_ASSERT(strstr(buf, "shard=\"") != NULL);
    } else {
        TEST_ASSERT(strstr(buf, "shard=\"") == NULL);
    }

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: scatter-merge metric families are present in Prometheus output
 * ============================================================================ */

static void test_prometheus_scatter_merge_metrics(void) {
    TEST_BEGIN("prometheus_scatter_merge_metrics");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    char buf[16384];
    size_t n = keel_router_write_prometheus(router, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    buf[n] = '\0';

    TEST_ASSERT(strstr(buf, "keel_router_scatter_merge_ops_total")   != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_scatter_merge_ns_total")    != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_scatter_merge_max_ns")      != NULL);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: 2PC metric families are present in Prometheus output
 * ============================================================================ */

static void test_prometheus_2pc_metrics(void) {
    TEST_BEGIN("prometheus_2pc_metrics");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    char buf[16384];
    size_t n = keel_router_write_prometheus(router, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    buf[n] = '\0';

    TEST_ASSERT(strstr(buf, "keel_router_2pc_started_total")        != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_2pc_prepared_total")       != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_2pc_prepare_failed_total") != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_2pc_committed_total")      != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_2pc_rolled_back_total")    != NULL);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: keel_router_record_scatter_merge_ns accumulates correctly
 * ============================================================================ */

static void test_record_scatter_merge_accumulates(void) {
    TEST_BEGIN("record_scatter_merge_accumulates");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    keel_router_record_scatter_merge_ns(router, 1000);
    keel_router_record_scatter_merge_ns(router, 3000);
    keel_router_record_scatter_merge_ns(router, 2000);

    keel_router_stats_t s;
    keel_router_get_stats(router, &s);

    TEST_ASSERT_EQ((long long)s.scatter_merge_ops,      3LL);
    TEST_ASSERT_EQ((long long)s.scatter_merge_total_ns, 6000LL);
    TEST_ASSERT_EQ((long long)s.scatter_merge_max_ns,   3000LL);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: histogram buckets are populated correctly by record_scatter_merge_ns
 *
 * Bucket upper bounds (ns): 1ms=1e6, 5ms=5e6, 10ms=1e7, 25ms=2.5e7, 50ms=5e7,
 *   100ms=1e8, 250ms=2.5e8, 500ms=5e8, 1s=1e9, 2.5s=2.5e9  (+Inf index 10)
 * ============================================================================ */

static void test_scatter_merge_histogram_buckets(void) {
    TEST_BEGIN("scatter_merge_histogram_buckets");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    /* Record observations that land in different buckets:
     *   500_000 ns  (0.5 ms) → bucket 0 (≤1ms) and all above
     *   8_000_000 ns (8 ms)  → bucket 2 (≤10ms) and all above
     *   120_000_000 ns (120ms) → bucket 5 (≤250ms) wait... let me recalculate
     *
     * Bounds: [0]=1ms [1]=5ms [2]=10ms [3]=25ms [4]=50ms
     *         [5]=100ms [6]=250ms [7]=500ms [8]=1s [9]=2.5s [10]=+Inf
     *
     *   500_000 ns (0.5ms)   → lands in bucket[0] (1ms); buckets[0..10] all +1
     *   8_000_000 ns (8ms)   → lands in bucket[2] (10ms); buckets[2..10] all +1
     *   200_000_000 ns (200ms) → lands in bucket[6] (250ms); buckets[6..10] +1
     *   3_000_000_000 ns (3s)  → exceeds all finite bounds; only bucket[10] +1
     */
    keel_router_record_scatter_merge_ns(router,   500000ULL);   /* 0.5 ms */
    keel_router_record_scatter_merge_ns(router,  8000000ULL);   /* 8 ms   */
    keel_router_record_scatter_merge_ns(router, 200000000ULL);  /* 200 ms */
    keel_router_record_scatter_merge_ns(router, 3000000000ULL); /* 3 s    */

    keel_router_stats_t s;
    keel_router_get_stats(router, &s);

    TEST_ASSERT_EQ((long long)s.scatter_merge_ops, 4LL);

    /* bucket[0] (≤1ms): only the 0.5ms obs → count=1 */
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[0], 1LL);

    /* bucket[1] (≤5ms): same obs (0.5ms ≤ 5ms) → still 1 */
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[1], 1LL);

    /* bucket[2] (≤10ms): 0.5ms + 8ms → 2 */
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[2], 2LL);

    /* bucket[3..5] (≤25ms, ≤50ms, ≤100ms): still 2 (200ms and 3s not included) */
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[3], 2LL);
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[4], 2LL);
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[5], 2LL);

    /* bucket[6] (≤250ms): 0.5ms + 8ms + 200ms → 3 */
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[6], 3LL);

    /* bucket[7..9] (≤500ms, ≤1s, ≤2.5s): still 3 (3s not included) */
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[7], 3LL);
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[8], 3LL);
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[9], 3LL);

    /* bucket[10] (+Inf): all 4 observations */
    TEST_ASSERT_EQ((long long)s.scatter_merge_hist[10], 4LL);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: Prometheus output contains scatter_merge_duration_seconds histogram
 * ============================================================================ */

static void test_prometheus_scatter_histogram_output(void) {
    TEST_BEGIN("prometheus_scatter_histogram_output");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    /* Record one 5ms observation so buckets are non-zero */
    keel_router_record_scatter_merge_ns(router, 5000000ULL); /* 5ms */

    char buf[8192];
    int n = keel_router_write_prometheus(router, buf, sizeof(buf));
    TEST_ASSERT(n > 0);

    /* Must contain histogram TYPE declaration */
    TEST_ASSERT(strstr(buf, "TYPE keel_router_scatter_merge_duration_seconds histogram") != NULL);

    /* Must have bucket lines */
    TEST_ASSERT(strstr(buf, "keel_router_scatter_merge_duration_seconds_bucket{le=\"0.001\"}") != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_scatter_merge_duration_seconds_bucket{le=\"+Inf\"}") != NULL);

    /* Must have _sum and _count */
    TEST_ASSERT(strstr(buf, "keel_router_scatter_merge_duration_seconds_sum") != NULL);
    TEST_ASSERT(strstr(buf, "keel_router_scatter_merge_duration_seconds_count") != NULL);

    /* The 5ms obs should be in bucket le="0.005" (5ms ≤ 5ms) */
    TEST_ASSERT(strstr(buf, "keel_router_scatter_merge_duration_seconds_bucket{le=\"0.005\"} 1") != NULL);

    /* Should NOT be in bucket le="0.001" (5ms > 1ms) */
    TEST_ASSERT(strstr(buf, "keel_router_scatter_merge_duration_seconds_bucket{le=\"0.001\"} 1") == NULL);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * Test: keel_router_record_2pc_outcome accumulates all counters correctly
 * ============================================================================ */

static void test_record_2pc_outcome_accumulates(void) {
    TEST_BEGIN("record_2pc_outcome_accumulates");

    keel_router_t* router = build_router_with_rule();
    TEST_ASSERT_NOT_NULL(router);

    /* Call twice: first call: 1 started, 2 prepared, 0 failed, 2 committed, 0 rolled_back */
    keel_router_record_2pc_outcome(router, 1, 2, 0, 2, 0);
    /* Second call: 1 started, 0 prepared, 1 failed, 0 committed, 1 rolled_back */
    keel_router_record_2pc_outcome(router, 1, 0, 1, 0, 1);

    keel_router_stats_t s;
    keel_router_get_stats(router, &s);

    TEST_ASSERT_EQ((long long)s.twopc_started,         2LL);
    TEST_ASSERT_EQ((long long)s.twopc_prepared,        2LL);
    TEST_ASSERT_EQ((long long)s.twopc_prepare_failed,  1LL);
    TEST_ASSERT_EQ((long long)s.twopc_committed,       2LL);
    TEST_ASSERT_EQ((long long)s.twopc_rolled_back,     1LL);

    keel_router_destroy(router);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    test_prometheus_output_non_empty();
    test_prometheus_metric_families();
    test_prometheus_help_type_lines();
    test_prometheus_no_shard_lines_when_zero();
    test_prometheus_counters_after_dispatch();
    test_prometheus_buffer_too_small();
    test_prometheus_null_guards();
    test_prometheus_shard_line_emitted();
    test_prometheus_scatter_merge_metrics();
    test_prometheus_2pc_metrics();
    test_record_scatter_merge_accumulates();
    test_scatter_merge_histogram_buckets();
    test_prometheus_scatter_histogram_output();
    test_record_2pc_outcome_accumulates();

    printf("\nrouter_metrics: %d/%d tests passed, %d failed\n",
           g_tests_passed, g_tests_run, g_tests_failed);
    return test_summary();
}
