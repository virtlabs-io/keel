/**
 * @file test_otlp_snapshot_queue.c
 * @brief Bounded snapshot queue: drop-oldest policy + SPSC producer/consumer.
 */

#include "test_utils.h"
#include "keel_snapshot_queue.h"
#include "keel_otlp_encode.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static keel_otlp_snapshot_t make_snap(uint64_t seq)
{
    keel_otlp_snapshot_t s = {0};
    s.start_time_unix_nano = 1000;
    s.time_unix_nano       = 1000 + seq;
    s.metric_count         = 1;
    snprintf(s.metrics[0].name, sizeof(s.metrics[0].name), "seq_%llu",
             (unsigned long long)seq);
    s.metrics[0].value = seq;
    return s;
}

static void test_create_destroy(void)
{
    keel_snapshot_queue_t* q = keel_snapshot_queue_create(4);
    TEST_ASSERT_NOT_NULL(q);
    TEST_ASSERT_EQ((int)keel_snapshot_queue_capacity(q), 4);
    TEST_ASSERT_EQ((int)keel_snapshot_queue_depth(q), 0);
    TEST_ASSERT_EQ((int)keel_snapshot_queue_dropped_total(q), 0);
    keel_snapshot_queue_destroy(q);

    TEST_ASSERT_NULL(keel_snapshot_queue_create(0));
}

static void test_push_pop_fifo(void)
{
    keel_snapshot_queue_t* q = keel_snapshot_queue_create(4);
    for (uint64_t i = 0; i < 3; ++i) {
        keel_otlp_snapshot_t s = make_snap(i);
        TEST_ASSERT_EQ(keel_snapshot_queue_push(q, &s), 0);
    }
    TEST_ASSERT_EQ((int)keel_snapshot_queue_depth(q), 3);

    for (uint64_t i = 0; i < 3; ++i) {
        keel_otlp_snapshot_t out;
        TEST_ASSERT_EQ(keel_snapshot_queue_try_pop(q, &out), 1);
        TEST_ASSERT_EQ((int)out.metrics[0].value, (int)i);
    }
    keel_otlp_snapshot_t out;
    TEST_ASSERT_EQ(keel_snapshot_queue_try_pop(q, &out), 0);
    keel_snapshot_queue_destroy(q);
}

static void test_drop_oldest_when_full(void)
{
    keel_snapshot_queue_t* q = keel_snapshot_queue_create(3);
    for (uint64_t i = 0; i < 3; ++i) {
        keel_otlp_snapshot_t s = make_snap(i);
        TEST_ASSERT_EQ(keel_snapshot_queue_push(q, &s), 0);
    }
    /* Now full. Pushing seq=3 drops seq=0. */
    keel_otlp_snapshot_t s3 = make_snap(3);
    TEST_ASSERT_EQ(keel_snapshot_queue_push(q, &s3), 1);
    TEST_ASSERT_EQ((int)keel_snapshot_queue_dropped_total(q), 1);
    TEST_ASSERT_EQ((int)keel_snapshot_queue_depth(q), 3);

    /* Pushing seq=4 drops seq=1. */
    keel_otlp_snapshot_t s4 = make_snap(4);
    TEST_ASSERT_EQ(keel_snapshot_queue_push(q, &s4), 1);
    TEST_ASSERT_EQ((int)keel_snapshot_queue_dropped_total(q), 2);

    /* Remaining order must be 2,3,4. */
    keel_otlp_snapshot_t out;
    for (uint64_t expect = 2; expect <= 4; ++expect) {
        TEST_ASSERT_EQ(keel_snapshot_queue_try_pop(q, &out), 1);
        TEST_ASSERT_EQ((int)out.metrics[0].value, (int)expect);
    }
    keel_snapshot_queue_destroy(q);
}

static void test_timed_pop_empty(void)
{
    keel_snapshot_queue_t* q = keel_snapshot_queue_create(2);
    keel_otlp_snapshot_t out;
    int r = keel_snapshot_queue_pop(q, &out, 20);
    TEST_ASSERT_EQ(r, 0);
    keel_snapshot_queue_destroy(q);
}

static void test_shutdown_releases_waiter(void)
{
    keel_snapshot_queue_t* q = keel_snapshot_queue_create(2);
    keel_snapshot_queue_shutdown(q);
    keel_otlp_snapshot_t out;
    int r = keel_snapshot_queue_pop(q, &out, 1000);
    TEST_ASSERT_EQ(r, -1);
    keel_snapshot_queue_destroy(q);
}

/* ----------------------------------------------------------------------- */
/* SPSC concurrency: producer pushes N, consumer pops; sequence preserved
 * modulo dropped entries. */

typedef struct {
    keel_snapshot_queue_t* q;
    uint64_t               total_to_push;
    atomic_uint            pushed;
} producer_args_t;

static void* producer_thread(void* arg)
{
    producer_args_t* a = arg;
    for (uint64_t i = 0; i < a->total_to_push; ++i) {
        keel_otlp_snapshot_t s = make_snap(i);
        keel_snapshot_queue_push(a->q, &s);
        atomic_fetch_add(&a->pushed, 1);
    }
    return NULL;
}

static void test_spsc_concurrent(void)
{
    keel_snapshot_queue_t* q = keel_snapshot_queue_create(8);
    producer_args_t a = { .q = q, .total_to_push = 5000 };
    atomic_store(&a.pushed, 0);

    pthread_t prod;
    TEST_ASSERT_EQ(pthread_create(&prod, NULL, producer_thread, &a), 0);

    /* Consumer drains until producer is done and queue empties. */
    uint64_t popped     = 0;
    uint64_t last_value = 0;
    bool     have_last  = false;
    for (;;) {
        keel_otlp_snapshot_t out;
        int r = keel_snapshot_queue_pop(q, &out, 50);
        if (r == 1) {
            if (have_last) {
                /* Sequence may jump forward due to drops, but must be strictly increasing. */
                TEST_ASSERT(out.metrics[0].value > last_value);
            }
            last_value = out.metrics[0].value;
            have_last  = true;
            popped++;
        } else {
            if (atomic_load(&a.pushed) == a.total_to_push &&
                keel_snapshot_queue_depth(q) == 0)
                break;
        }
    }
    pthread_join(prod, NULL);

    uint64_t dropped = keel_snapshot_queue_dropped_total(q);
    TEST_ASSERT_EQ((int)(popped + dropped), (int)a.total_to_push);
    keel_snapshot_queue_destroy(q);
}

int main(void) {
    test_create_destroy();
    test_push_pop_fifo();
    test_drop_oldest_when_full();
    test_timed_pop_empty();
    test_shutdown_releases_waiter();
    test_spsc_concurrent();
    return test_summary();
}
