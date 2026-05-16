/**
 * @file test_stats_alignment.c
 * @brief Regression tests for stats context alignment and collector lifecycle.
 *
 * Prevents startup crashes on strict-alignment architectures by asserting
 * 64-byte alignment guarantees for per-worker stats contexts.
 */

#include "test_utils.h"
#include "keel/core/stats.h"

#include <stdint.h>
#include <stddef.h>

static void test_stats_collector_basic_alignment(void)
{
    printf("  test_stats_collector_basic_alignment...\n");

    const size_t workers = 8;
    keel_stats_collector_t* collector =
        keel_stats_collector_create(KEEL_STATS_BASIC, workers);

    TEST_ASSERT_NOT_NULL(collector);
    TEST_ASSERT_NOT_NULL(collector->contexts);
    TEST_ASSERT(((uintptr_t)collector->contexts % 64u) == 0u);

    for (size_t i = 0; i < workers; i++) {
        keel_stats_ctx_t* ctx = keel_stats_collector_get_ctx(collector, (uint32_t)i);
        TEST_ASSERT_NOT_NULL(ctx);
        TEST_ASSERT(((uintptr_t)ctx % 64u) == 0u);
        TEST_ASSERT_EQ(ctx->worker_id, (uint32_t)i);
    }

    keel_stats_snapshot_t snap;
    keel_stats_snapshot_take(collector, &snap);
    TEST_ASSERT_EQ(snap.num_workers, workers);
    TEST_ASSERT_EQ(snap.level, KEEL_STATS_BASIC);

    keel_stats_collector_destroy(collector);
}

static void test_stats_collector_extended_alignment(void)
{
    printf("  test_stats_collector_extended_alignment...\n");

    const size_t workers = 4;
    keel_stats_collector_t* collector =
        keel_stats_collector_create(KEEL_STATS_EXTENDED, workers);

    TEST_ASSERT_NOT_NULL(collector);
    TEST_ASSERT_NOT_NULL(collector->contexts);
    TEST_ASSERT(((uintptr_t)collector->contexts % 64u) == 0u);

    for (size_t i = 0; i < workers; i++) {
        keel_stats_ctx_t* ctx = keel_stats_collector_get_ctx(collector, (uint32_t)i);
        TEST_ASSERT_NOT_NULL(ctx);
        TEST_ASSERT(((uintptr_t)ctx % 64u) == 0u);
    }

    keel_stats_snapshot_t snap;
    keel_stats_snapshot_take(collector, &snap);
    TEST_ASSERT_EQ(snap.num_workers, workers);
    TEST_ASSERT_EQ(snap.level, KEEL_STATS_EXTENDED);

    keel_stats_collector_destroy(collector);
}

static void test_stats_collector_off_has_no_contexts(void)
{
    printf("  test_stats_collector_off_has_no_contexts...\n");

    keel_stats_collector_t* collector =
        keel_stats_collector_create(KEEL_STATS_OFF, 4);

    TEST_ASSERT_NOT_NULL(collector);
    TEST_ASSERT_NULL(collector->contexts);
    TEST_ASSERT_NULL(keel_stats_collector_get_ctx(collector, 0));

    keel_stats_snapshot_t snap;
    keel_stats_snapshot_take(collector, &snap);
    TEST_ASSERT_EQ(snap.level, KEEL_STATS_OFF);

    keel_stats_collector_destroy(collector);
}

int main(void)
{
    printf("=== Stats Alignment Regression Tests ===\n\n");

    test_stats_collector_basic_alignment();
    test_stats_collector_extended_alignment();
    test_stats_collector_off_has_no_contexts();

    printf("\n========================================\n");
    return test_summary();
}
