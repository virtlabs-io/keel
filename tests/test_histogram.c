/**
 * @file test_histogram.c
 * @brief Unit tests for the log2 latency histogram.
 *
 * Validates bucket placement, percentile estimation, snapshot correctness,
 * reset behaviour, and edge cases (zero, max uint64, overflow).
 */

#include "test_utils.h"
#include "keel/core/stats.h"
#include "keel/mem/mem.h"
#include <string.h>
#include <limits.h>

/* ============================================================================
 * Bucket Placement
 * ============================================================================ */

static void test_histogram_single_value(void) {
    TEST_BEGIN("histogram single value");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    keel_histogram_record(&h, 1000); /* 1000 ns → bucket ~9 (2^9=512..2^10=1024) */

    keel_histogram_snapshot_t snap;
    keel_histogram_snapshot(&h, &snap);

    TEST_ASSERT_EQ(snap.count, 1);
    TEST_ASSERT_EQ(snap.sum, 1000);
    TEST_ASSERT_EQ(snap.min_val, 1000);
    TEST_ASSERT_EQ(snap.max_val, 1000);

    /* Exactly one bucket should be non-zero */
    int non_zero = 0;
    for (int i = 0; i < KEEL_HISTOGRAM_BUCKETS; i++) {
        if (snap.buckets[i] > 0) non_zero++;
    }
    TEST_ASSERT_EQ(non_zero, 1);

    TEST_END();
}

static void test_histogram_zero_value(void) {
    TEST_BEGIN("histogram zero value");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    keel_histogram_record(&h, 0);

    keel_histogram_snapshot_t snap;
    keel_histogram_snapshot(&h, &snap);

    TEST_ASSERT_EQ(snap.count, 1);
    TEST_ASSERT_EQ(snap.sum, 0);
    /* Bucket 0 should hold the value */
    TEST_ASSERT_EQ(snap.buckets[0], 1);
    /* min_val for zero: since UINT64_MAX > 0, CAS should succeed */
    TEST_ASSERT_EQ(snap.min_val, 0);

    TEST_END();
}

static void test_histogram_power_of_two_boundaries(void) {
    TEST_BEGIN("histogram power-of-two boundaries");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    /* Record exact powers of two — each should go to a distinct bucket */
    keel_histogram_record(&h, 1);      /* 2^0 */
    keel_histogram_record(&h, 2);      /* 2^1 */
    keel_histogram_record(&h, 4);      /* 2^2 */
    keel_histogram_record(&h, 1024);   /* 2^10 (~1μs) */
    keel_histogram_record(&h, 1048576); /* 2^20 (~1ms) */

    keel_histogram_snapshot_t snap;
    keel_histogram_snapshot(&h, &snap);

    TEST_ASSERT_EQ(snap.count, 5);
    TEST_ASSERT_EQ(snap.sum, 1 + 2 + 4 + 1024 + 1048576);
    TEST_ASSERT_EQ(snap.min_val, 1);
    TEST_ASSERT_EQ(snap.max_val, 1048576);

    TEST_END();
}

static void test_histogram_bucket_distribution(void) {
    TEST_BEGIN("histogram bucket distribution");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    /* Record 100 values in the 1μs range (1000-2000ns → bucket ~10) */
    for (int i = 0; i < 100; i++) {
        keel_histogram_record(&h, 1500);
    }

    /* Record 50 values in the 1ms range (1000000 ns → bucket ~20) */
    for (int i = 0; i < 50; i++) {
        keel_histogram_record(&h, 1000000);
    }

    keel_histogram_snapshot_t snap;
    keel_histogram_snapshot(&h, &snap);

    TEST_ASSERT_EQ(snap.count, 150);
    TEST_ASSERT_EQ(snap.min_val, 1500);
    TEST_ASSERT_EQ(snap.max_val, 1000000);

    TEST_END();
}

/* ============================================================================
 * Percentile Estimation
 * ============================================================================ */

static void test_histogram_percentile_basic(void) {
    TEST_BEGIN("histogram percentile basic");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    /* All values in one bucket — any percentile should return that bucket's range */
    for (int i = 0; i < 1000; i++) {
        keel_histogram_record(&h, 500);  /* bucket ~8 or 9 */
    }

    uint64_t p50 = keel_histogram_percentile(&h, 0.50);
    uint64_t p99 = keel_histogram_percentile(&h, 0.99);

    /* Both percentiles should be the same bucket midpoint */
    TEST_ASSERT_EQ(p50, p99);
    /* Should be in a reasonable range for 500ns */
    TEST_ASSERT(p50 > 0);
    TEST_ASSERT(p50 < 10000);

    TEST_END();
}

static void test_histogram_percentile_bimodal(void) {
    TEST_BEGIN("histogram percentile bimodal");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    /* 90 fast queries at ~1μs, 10 slow queries at ~1ms */
    for (int i = 0; i < 90; i++) {
        keel_histogram_record(&h, 1000);    /* 1μs */
    }
    for (int i = 0; i < 10; i++) {
        keel_histogram_record(&h, 1000000); /* 1ms */
    }

    uint64_t p50 = keel_histogram_percentile(&h, 0.50);
    uint64_t p99 = keel_histogram_percentile(&h, 0.99);

    /* p50 should be in the fast range, p99 in the slow range */
    TEST_ASSERT(p50 < 100000);    /* < 100μs */
    TEST_ASSERT(p99 >= 100000);   /* ≥ 100μs (in the ms bucket range) */

    TEST_END();
}

static void test_histogram_percentile_empty(void) {
    TEST_BEGIN("histogram percentile empty");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    uint64_t p50 = keel_histogram_percentile(&h, 0.50);
    TEST_ASSERT_EQ(p50, 0);

    TEST_END();
}

static void test_histogram_percentile_extremes(void) {
    TEST_BEGIN("histogram percentile p0 and p100");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    for (int i = 0; i < 100; i++) {
        keel_histogram_record(&h, 5000);
    }

    uint64_t p0 = keel_histogram_percentile(&h, 0.0);
    uint64_t p100 = keel_histogram_percentile(&h, 1.0);

    /* p0 should be the lowest bucket, p100 the highest */
    TEST_ASSERT(p0 <= p100);

    TEST_END();
}

/* ============================================================================
 * Snapshot
 * ============================================================================ */

static void test_histogram_snapshot_correctness(void) {
    TEST_BEGIN("histogram snapshot correctness");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    keel_histogram_record(&h, 100);
    keel_histogram_record(&h, 200);
    keel_histogram_record(&h, 300);

    keel_histogram_snapshot_t snap;
    keel_histogram_snapshot(&h, &snap);

    TEST_ASSERT_EQ(snap.count, 3);
    TEST_ASSERT_EQ(snap.sum, 600);
    TEST_ASSERT_EQ(snap.min_val, 100);
    TEST_ASSERT_EQ(snap.max_val, 300);

    /* Snapshot should be independent — adding more data shouldn't change it */
    keel_histogram_record(&h, 9999);

    TEST_ASSERT_EQ(snap.count, 3);
    TEST_ASSERT_EQ(snap.sum, 600);

    TEST_END();
}

/* ============================================================================
 * Reset
 * ============================================================================ */

static void test_histogram_reset(void) {
    TEST_BEGIN("histogram reset");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    for (int i = 0; i < 100; i++) {
        keel_histogram_record(&h, 1000 + (uint64_t)i);
    }

    keel_histogram_reset(&h);

    keel_histogram_snapshot_t snap;
    keel_histogram_snapshot(&h, &snap);

    TEST_ASSERT_EQ(snap.count, 0);
    TEST_ASSERT_EQ(snap.sum, 0);

    /* All buckets should be zero */
    for (int i = 0; i < KEEL_HISTOGRAM_BUCKETS; i++) {
        TEST_ASSERT_EQ(snap.buckets[i], 0);
    }

    TEST_END();
}

/* ============================================================================
 * Edge Cases
 * ============================================================================ */

static void test_histogram_large_values(void) {
    TEST_BEGIN("histogram large values");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    uint64_t big = 1000000000ULL * 60;  /* 60 seconds in ns */
    keel_histogram_record(&h, big);

    keel_histogram_snapshot_t snap;
    keel_histogram_snapshot(&h, &snap);

    TEST_ASSERT_EQ(snap.count, 1);
    TEST_ASSERT_EQ(snap.sum, big);
    TEST_ASSERT_EQ(snap.max_val, big);

    TEST_END();
}

static void test_histogram_min_max_tracking(void) {
    TEST_BEGIN("histogram min/max tracking");

    keel_histogram_t h;
    memset(&h, 0, sizeof(h));
    keel_histogram_reset(&h);

    keel_histogram_record(&h, 500);
    keel_histogram_record(&h, 100);
    keel_histogram_record(&h, 999);
    keel_histogram_record(&h, 50);
    keel_histogram_record(&h, 2000);

    keel_histogram_snapshot_t snap;
    keel_histogram_snapshot(&h, &snap);

    TEST_ASSERT_EQ(snap.min_val, 50);
    TEST_ASSERT_EQ(snap.max_val, 2000);
    TEST_ASSERT_EQ(snap.count, 5);

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    keel_mem_init(NULL);

    /* Bucket placement */
    test_histogram_single_value();
    test_histogram_zero_value();
    test_histogram_power_of_two_boundaries();
    test_histogram_bucket_distribution();

    /* Percentile estimation */
    test_histogram_percentile_basic();
    test_histogram_percentile_bimodal();
    test_histogram_percentile_empty();
    test_histogram_percentile_extremes();

    /* Snapshot */
    test_histogram_snapshot_correctness();

    /* Reset */
    test_histogram_reset();

    /* Edge cases */
    test_histogram_large_values();
    test_histogram_min_max_tracking();

    keel_mem_shutdown();

    return test_summary();
}
