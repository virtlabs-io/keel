/**
 * @file test_catchup_waitlist.c
 * @brief Unit tests for reactor-owned replica catch-up wait list (Phase 2a).
 *
 * These tests exercise the manager directly without spinning up a worker or
 * reactor.  They use the public `keel_catchup_manager_tick(m, now_ns)` entry
 * point with a caller-supplied virtual clock so deadlines fire
 * deterministically.
 */

#include "test_utils.h"

#include "keel/engine/catchup.h"
#include "keel/plugin/plugin_types.h"
#include "keel/util/util.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* A dummy session pointer — the manager never dereferences it. */
static int dummy_session_sentinel;

typedef struct {
    keel_catchup_outcome_t outcome;
    int                    call_count;
    void*                  expected_userdata;
    bool                   userdata_ok;
} resume_capture_t;

static void capture_cb(struct keel_session* s,
                       keel_catchup_outcome_t outcome,
                       void* userdata)
{
    (void)s;
    resume_capture_t* cap = (resume_capture_t*)userdata;
    /* Defensive: keep the latest outcome but count every invocation so we
     * can assert exactly-once semantics. */
    cap->outcome = outcome;
    cap->call_count++;
    cap->userdata_ok = (userdata == cap->expected_userdata);
}

static keel_consistency_token_t make_token(const char* v, uint32_t tl)
{
    keel_consistency_token_t t = {0};
    strncpy(t.value, v, sizeof t.value - 1);
    t.timeline_id = tl;
    return t;
}

/* --------------------------------------------------------------------------
 * Lifecycle
 * --------------------------------------------------------------------------*/
static void test_lifecycle(void)
{
    TEST_BEGIN("catchup_manager: lifecycle");
    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, NULL);
    TEST_ASSERT_NOT_NULL(m);
    keel_catchup_manager_destroy(m);
    /* Double-destroy NULL must be safe. */
    keel_catchup_manager_destroy(NULL);
    TEST_END();
}

/* --------------------------------------------------------------------------
 * Enqueue → timeout
 * --------------------------------------------------------------------------*/
static void test_enqueue_and_timeout(void)
{
    TEST_BEGIN("catchup_manager: waiter fires KEEL_CATCHUP_TIMEOUT past deadline");
    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, NULL);
    TEST_ASSERT_NOT_NULL(m);

    resume_capture_t cap = { .expected_userdata = &cap };
    keel_consistency_token_t tok = make_token("0/16B3740", 1);

    /* The manager stamps `enqueued_ns` from the real monotonic clock at
     * enqueue time, so virtual ticks must be expressed as offsets from
     * that same base — not as absolute small numbers. */
    uint64_t base_ns = (uint64_t)keel_time_now();
    keel_catchup_waiter_t* w = keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel,
        /*server_index*/ 2, &tok, /*max_wait_ms*/ 10,
        capture_cb, &cap);
    TEST_ASSERT_NOT_NULL(w);

    /* Tick at base+5 ms — should NOT fire. */
    keel_catchup_manager_tick(m, base_ns + 5ULL * 1000000ULL);
    TEST_ASSERT_EQ(cap.call_count, 0);

    /* Tick well past the deadline (base + 50 ms). */
    keel_catchup_manager_tick(m, base_ns + 50ULL * 1000000ULL);
    TEST_ASSERT_EQ(cap.call_count, 1);
    TEST_ASSERT_EQ(cap.outcome, KEEL_CATCHUP_TIMEOUT);
    TEST_ASSERT(cap.userdata_ok);

    keel_catchup_stats_snapshot_t snap;
    keel_catchup_manager_snapshot(m, &snap);
    TEST_ASSERT_EQ(snap.waiters_active, (size_t)0);
    TEST_ASSERT_EQ(snap.waiters_timeout_total, (uint64_t)1);
    TEST_ASSERT_EQ(snap.waiters_enqueued_total, (uint64_t)1);

    keel_catchup_manager_destroy(m);
    TEST_END();
}

/* --------------------------------------------------------------------------
 * Cancel before deadline
 * --------------------------------------------------------------------------*/
static void test_cancel(void)
{
    TEST_BEGIN("catchup_manager: explicit cancel fires KEEL_CATCHUP_CANCELLED exactly once");
    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, NULL);
    resume_capture_t cap = { .expected_userdata = &cap };
    keel_consistency_token_t tok = make_token("0/A", 0);

    keel_catchup_waiter_t* w = keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel,
        0, &tok, 1000, capture_cb, &cap);
    TEST_ASSERT_NOT_NULL(w);

    keel_catchup_cancel(m, w);
    TEST_ASSERT_EQ(cap.call_count, 1);
    TEST_ASSERT_EQ(cap.outcome, KEEL_CATCHUP_CANCELLED);

    /* Tick well past the (1s) deadline: must be a no-op since the
     * waiter has already been released and freed by cancel. */
    uint64_t far_future = (uint64_t)keel_time_now() + 5ULL * 1000000000ULL;
    keel_catchup_manager_tick(m, far_future);
    TEST_ASSERT_EQ(cap.call_count, 1);

    keel_catchup_stats_snapshot_t snap;
    keel_catchup_manager_snapshot(m, &snap);
    TEST_ASSERT_EQ(snap.waiters_cancelled_total, (uint64_t)1);
    TEST_ASSERT_EQ(snap.waiters_active, (size_t)0);

    keel_catchup_manager_destroy(m);
    TEST_END();
}

/* --------------------------------------------------------------------------
 * Manager destroy cancels all parked waiters
 * --------------------------------------------------------------------------*/
static void test_destroy_releases_waiters(void)
{
    TEST_BEGIN("catchup_manager: destroy cancels every parked waiter");
    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, NULL);

    resume_capture_t caps[3] = { {0}, {0}, {0} };
    for (int i = 0; i < 3; i++) {
        caps[i].expected_userdata = &caps[i];
        keel_consistency_token_t tok = make_token("0/B", 0);
        keel_catchup_waiter_t* w = keel_catchup_enqueue(
            m, (struct keel_session*)&dummy_session_sentinel,
            (size_t)i, &tok, 60000, capture_cb, &caps[i]);
        TEST_ASSERT_NOT_NULL(w);
    }

    keel_catchup_manager_destroy(m);
    for (int i = 0; i < 3; i++) {
        TEST_ASSERT_EQ(caps[i].call_count, 1);
        TEST_ASSERT_EQ(caps[i].outcome, KEEL_CATCHUP_CANCELLED);
    }
    TEST_END();
}

/* --------------------------------------------------------------------------
 * max_waiters cap
 * --------------------------------------------------------------------------*/
static void test_max_waiters_cap(void)
{
    TEST_BEGIN("catchup_manager: max_waiters rejects further enqueues");
    keel_catchup_config_t cfg = KEEL_CATCHUP_CONFIG_DEFAULT;
    cfg.max_waiters = 2;
    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, &cfg);

    resume_capture_t cap = { .expected_userdata = &cap };
    keel_consistency_token_t tok = make_token("0/C", 0);

    keel_catchup_waiter_t* w1 = keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel,
        0, &tok, 60000, capture_cb, &cap);
    keel_catchup_waiter_t* w2 = keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel,
        1, &tok, 60000, capture_cb, &cap);
    keel_catchup_waiter_t* w3 = keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel,
        2, &tok, 60000, capture_cb, &cap);

    TEST_ASSERT_NOT_NULL(w1);
    TEST_ASSERT_NOT_NULL(w2);
    TEST_ASSERT_NULL(w3);

    keel_catchup_manager_destroy(m);
    TEST_END();
}

/* --------------------------------------------------------------------------
 * Input validation
 * --------------------------------------------------------------------------*/
static void test_enqueue_validation(void)
{
    TEST_BEGIN("catchup_manager: enqueue rejects invalid inputs");
    keel_catchup_manager_t* m = keel_catchup_manager_create(NULL, NULL);
    resume_capture_t cap = { .expected_userdata = &cap };
    keel_consistency_token_t tok = make_token("0/D", 0);

    TEST_ASSERT_NULL(keel_catchup_enqueue(
        NULL, (struct keel_session*)&dummy_session_sentinel, 0, &tok, 100, capture_cb, &cap));
    TEST_ASSERT_NULL(keel_catchup_enqueue(
        m, NULL, 0, &tok, 100, capture_cb, &cap));
    TEST_ASSERT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, NULL, 100, capture_cb, &cap));
    TEST_ASSERT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &tok, 100, NULL, &cap));
    /* server_index out of range. */
    TEST_ASSERT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel,
        (size_t)1024, &tok, 100, capture_cb, &cap));

    keel_catchup_manager_destroy(m);
    TEST_END();
}

int main(void)
{
    test_lifecycle();
    test_enqueue_and_timeout();
    test_cancel();
    test_destroy_releases_waiters();
    test_max_waiters_cap();
    test_enqueue_validation();
    return test_summary();
}
