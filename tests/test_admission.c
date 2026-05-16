/**
 * @file test_admission.c
 * @brief Unit tests for the admission control gate guarding frontend and backend
 *        slot limits.
 *
 * Admission control is one of the few subsystems that must be correct under every
 * load pattern: normal traffic, burst arrivals, and sustained saturation. This
 * suite verifies counter coherence, rejection thresholds, wait-queue bookkeeping,
 * underflow protection, and the unlimited fallback path so quota bugs do not
 * silently turn into connection storms or spurious client rejections.
 */

#include "test_utils.h"
#include "keel/session/admission.h"

#include <string.h>
#include <stdio.h>

/* ============================================================================
 * §1 — Init
 * ============================================================================ */

static void test_init(void)
{
    TEST_BEGIN("admission init — zeroed counters, limits set");
    keel_admission_t adm;
    keel_admission_init(&adm, 100, 50, 10);

    TEST_ASSERT_EQ(adm.max_frontends, 100u);
    TEST_ASSERT_EQ(adm.max_backends, 50u);
    TEST_ASSERT_EQ(adm.max_waiting, 10u);
    TEST_ASSERT_EQ(adm.cur_frontends, 0u);
    TEST_ASSERT_EQ(adm.cur_backends, 0u);
    TEST_ASSERT_EQ(adm.cur_waiting, 0u);
    TEST_ASSERT_EQ(adm.total_accepted, 0ULL);
    TEST_ASSERT_EQ(adm.total_rejected, 0ULL);
    TEST_END();
}

static void test_init_null(void)
{
    TEST_BEGIN("admission init — NULL pointer safety");
    keel_admission_init(NULL, 10, 10, 10); /* should not crash */
    TEST_END();
}

/* ============================================================================
 * §2 — Frontend Admission
 * ============================================================================ */

static void test_frontend_admit_under_limit(void)
{
    TEST_BEGIN("frontend — admit under limit");
    keel_admission_t adm;
    keel_admission_init(&adm, 3, 0, 0);

    TEST_ASSERT_EQ(keel_admission_try_frontend(&adm), KEEL_ADMIT_OK);
    TEST_ASSERT_EQ(adm.cur_frontends, 1u);
    TEST_ASSERT_EQ(adm.total_accepted, 1ULL);

    TEST_ASSERT_EQ(keel_admission_try_frontend(&adm), KEEL_ADMIT_OK);
    TEST_ASSERT_EQ(keel_admission_try_frontend(&adm), KEEL_ADMIT_OK);
    TEST_ASSERT_EQ(adm.cur_frontends, 3u);
    TEST_END();
}

static void test_frontend_reject_at_limit(void)
{
    TEST_BEGIN("frontend — reject at limit");
    keel_admission_t adm;
    keel_admission_init(&adm, 2, 0, 0);

    keel_admission_try_frontend(&adm);
    keel_admission_try_frontend(&adm);

    /* 3rd should be rejected */
    TEST_ASSERT_EQ(keel_admission_try_frontend(&adm), KEEL_ADMIT_REJECTED);
    TEST_ASSERT_EQ(adm.cur_frontends, 2u);
    TEST_ASSERT_EQ(adm.total_rejected, 1ULL);
    TEST_END();
}

static void test_frontend_release(void)
{
    TEST_BEGIN("frontend — release decrements");
    keel_admission_t adm;
    keel_admission_init(&adm, 10, 0, 0);

    keel_admission_try_frontend(&adm);
    keel_admission_try_frontend(&adm);
    TEST_ASSERT_EQ(adm.cur_frontends, 2u);

    keel_admission_release_frontend(&adm);
    TEST_ASSERT_EQ(adm.cur_frontends, 1u);

    keel_admission_release_frontend(&adm);
    TEST_ASSERT_EQ(adm.cur_frontends, 0u);

    /* Release when zero — should not underflow */
    keel_admission_release_frontend(&adm);
    TEST_ASSERT_EQ(adm.cur_frontends, 0u);
    TEST_END();
}

static void test_frontend_unlimited(void)
{
    TEST_BEGIN("frontend — unlimited (max=0)");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 0, 0);

    /* Admit many — should never reject */
    for (int i = 0; i < 10000; i++) {
        TEST_ASSERT_EQ(keel_admission_try_frontend(&adm), KEEL_ADMIT_OK);
    }
    TEST_ASSERT_EQ(adm.cur_frontends, 10000u);
    TEST_ASSERT_EQ(adm.total_rejected, 0ULL);
    TEST_END();
}

/* ============================================================================
 * §3 — Backend Admission
 * ============================================================================ */

static void test_backend_admit_under_limit(void)
{
    TEST_BEGIN("backend — admit under limit");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 5, 3);

    TEST_ASSERT_EQ(keel_admission_try_backend(&adm), KEEL_ADMIT_OK);
    TEST_ASSERT_EQ(adm.cur_backends, 1u);
    TEST_END();
}

static void test_backend_queued_at_limit(void)
{
    TEST_BEGIN("backend — QUEUED when at limit but queue has room");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 2, 5);

    keel_admission_try_backend(&adm);
    keel_admission_try_backend(&adm);
    /* Backend at limit */

    TEST_ASSERT_EQ(keel_admission_try_backend(&adm), KEEL_ADMIT_QUEUED);
    /* cur_backends unchanged — queued, not admitted */
    TEST_ASSERT_EQ(adm.cur_backends, 2u);
    TEST_END();
}

static void test_backend_rejected_queue_full(void)
{
    TEST_BEGIN("backend — REJECTED when at limit and queue full");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 1, 2);

    keel_admission_try_backend(&adm); /* admitted */
    /* At backend limit. Fill wait queue. */
    keel_admission_enqueue_waiter(&adm);
    keel_admission_enqueue_waiter(&adm);
    /* Queue full */

    TEST_ASSERT_EQ(keel_admission_try_backend(&adm), KEEL_ADMIT_REJECTED);
    TEST_ASSERT_EQ(adm.total_rejected, 1ULL);
    TEST_END();
}

static void test_backend_release(void)
{
    TEST_BEGIN("backend — release decrements");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 10, 0);

    keel_admission_try_backend(&adm);
    keel_admission_try_backend(&adm);
    keel_admission_release_backend(&adm);
    TEST_ASSERT_EQ(adm.cur_backends, 1u);

    keel_admission_release_backend(&adm);
    TEST_ASSERT_EQ(adm.cur_backends, 0u);

    /* No underflow */
    keel_admission_release_backend(&adm);
    TEST_ASSERT_EQ(adm.cur_backends, 0u);
    TEST_END();
}

static void test_backend_unlimited(void)
{
    TEST_BEGIN("backend — unlimited (max=0)");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 0, 0);

    for (int i = 0; i < 5000; i++) {
        TEST_ASSERT_EQ(keel_admission_try_backend(&adm), KEEL_ADMIT_OK);
    }
    TEST_ASSERT_EQ(adm.cur_backends, 5000u);
    TEST_END();
}

/* ============================================================================
 * §4 — Wait Queue
 * ============================================================================ */

static void test_waiter_enqueue_dequeue(void)
{
    TEST_BEGIN("waiter — enqueue and dequeue");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 0, 10);

    TEST_ASSERT(keel_admission_enqueue_waiter(&adm));
    TEST_ASSERT_EQ(adm.cur_waiting, 1u);
    TEST_ASSERT_EQ(adm.total_queued, 1ULL);

    TEST_ASSERT(keel_admission_dequeue_waiter(&adm));
    TEST_ASSERT_EQ(adm.cur_waiting, 0u);
    TEST_END();
}

static void test_waiter_dequeue_empty(void)
{
    TEST_BEGIN("waiter — dequeue from empty returns false");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 0, 10);

    TEST_ASSERT(!keel_admission_dequeue_waiter(&adm));
    TEST_END();
}

static void test_waiter_queue_full(void)
{
    TEST_BEGIN("waiter — enqueue fails when queue full");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 0, 3);

    TEST_ASSERT(keel_admission_enqueue_waiter(&adm));
    TEST_ASSERT(keel_admission_enqueue_waiter(&adm));
    TEST_ASSERT(keel_admission_enqueue_waiter(&adm));

    /* 4th should fail */
    TEST_ASSERT(!keel_admission_enqueue_waiter(&adm));
    TEST_ASSERT_EQ(adm.cur_waiting, 3u);
    TEST_END();
}

static void test_waiter_unlimited_queue(void)
{
    TEST_BEGIN("waiter — unlimited queue (max_waiting=0)");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 0, 0);

    for (int i = 0; i < 1000; i++) {
        TEST_ASSERT(keel_admission_enqueue_waiter(&adm));
    }
    TEST_ASSERT_EQ(adm.cur_waiting, 1000u);
    TEST_END();
}

static void test_waiter_timeout(void)
{
    TEST_BEGIN("waiter — timeout decrements queue and counts");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 0, 10);

    keel_admission_enqueue_waiter(&adm);
    keel_admission_enqueue_waiter(&adm);

    keel_admission_timeout_waiter(&adm);
    TEST_ASSERT_EQ(adm.cur_waiting, 1u);
    TEST_ASSERT_EQ(adm.total_queue_timeout, 1ULL);
    TEST_END();
}

static void test_waiter_timeout_on_empty(void)
{
    TEST_BEGIN("waiter — timeout on empty queue doesn't underflow");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 0, 10);

    keel_admission_timeout_waiter(&adm);
    TEST_ASSERT_EQ(adm.cur_waiting, 0u);
    TEST_ASSERT_EQ(adm.total_queue_timeout, 1ULL);
    TEST_END();
}

/* ============================================================================
 * §5 — Peak Tracking
 * ============================================================================ */

static void test_peak_tracking(void)
{
    TEST_BEGIN("admission — peak (high-water mark) tracking");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 0, 0);

    /* Admit 5, release 3, admit 2 more → peak should be 5 */
    for (int i = 0; i < 5; i++) keel_admission_try_frontend(&adm);
    TEST_ASSERT_EQ(adm.peak_frontends, 5ULL);

    for (int i = 0; i < 3; i++) keel_admission_release_frontend(&adm);
    TEST_ASSERT_EQ(adm.cur_frontends, 2u);

    for (int i = 0; i < 2; i++) keel_admission_try_frontend(&adm);
    TEST_ASSERT_EQ(adm.cur_frontends, 4u);
    TEST_ASSERT_EQ(adm.peak_frontends, 5ULL); /* unchanged */

    /* Now go above previous peak */
    for (int i = 0; i < 2; i++) keel_admission_try_frontend(&adm);
    TEST_ASSERT_EQ(adm.peak_frontends, 6ULL);

    /* Backend peak */
    for (int i = 0; i < 3; i++) keel_admission_try_backend(&adm);
    TEST_ASSERT_EQ(adm.peak_backends, 3ULL);
    TEST_END();
}

/* ============================================================================
 * §6 — Combined Pressure
 * ============================================================================ */

static void test_combined_pressure(void)
{
    TEST_BEGIN("admission — combined frontend + backend + waiter pressure");
    keel_admission_t adm;
    keel_admission_init(&adm, 100, 10, 5);

    /* Admit 50 frontends */
    for (int i = 0; i < 50; i++) {
        TEST_ASSERT_EQ(keel_admission_try_frontend(&adm), KEEL_ADMIT_OK);
    }

    /* Admit 10 backends (filling the pool) */
    for (int i = 0; i < 10; i++) {
        TEST_ASSERT_EQ(keel_admission_try_backend(&adm), KEEL_ADMIT_OK);
    }

    /* Next backend attempts should queue */
    for (int i = 0; i < 5; i++) {
        TEST_ASSERT_EQ(keel_admission_try_backend(&adm), KEEL_ADMIT_QUEUED);
        keel_admission_enqueue_waiter(&adm);
    }

    /* Queue is now full — next should be rejected */
    TEST_ASSERT_EQ(keel_admission_try_backend(&adm), KEEL_ADMIT_REJECTED);

    /* Release a backend, dequeue a waiter */
    keel_admission_release_backend(&adm);
    TEST_ASSERT(keel_admission_dequeue_waiter(&adm));
    TEST_ASSERT_EQ(adm.cur_waiting, 4u);
    TEST_ASSERT_EQ(adm.cur_backends, 9u);

    /* Backend slot available again */
    TEST_ASSERT_EQ(keel_admission_try_backend(&adm), KEEL_ADMIT_OK);
    TEST_ASSERT_EQ(adm.cur_backends, 10u);
    TEST_END();
}

/* ============================================================================
 * §7 — Rapid Admit/Release Cycles
 * ============================================================================ */

static void test_rapid_cycles(void)
{
    TEST_BEGIN("admission — rapid admit/release cycles (10K)");
    keel_admission_t adm;
    keel_admission_init(&adm, 5, 5, 5);

    for (int i = 0; i < 10000; i++) {
        TEST_ASSERT_EQ(keel_admission_try_frontend(&adm), KEEL_ADMIT_OK);
        keel_admission_release_frontend(&adm);

        TEST_ASSERT_EQ(keel_admission_try_backend(&adm), KEEL_ADMIT_OK);
        keel_admission_release_backend(&adm);
    }

    TEST_ASSERT_EQ(adm.cur_frontends, 0u);
    TEST_ASSERT_EQ(adm.cur_backends, 0u);
    TEST_ASSERT_EQ(adm.total_accepted, 10000ULL);
    TEST_ASSERT_EQ(adm.total_rejected, 0ULL);
    TEST_END();
}

/* ============================================================================
 * §8 — can_open_backend helper
 * ============================================================================ */

static void test_can_open_backend(void)
{
    TEST_BEGIN("admission — can_open_backend helper");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 3, 0);

    TEST_ASSERT(keel_admission_can_open_backend(&adm));

    keel_admission_try_backend(&adm);
    keel_admission_try_backend(&adm);
    TEST_ASSERT(keel_admission_can_open_backend(&adm));

    keel_admission_try_backend(&adm);
    TEST_ASSERT(!keel_admission_can_open_backend(&adm));

    keel_admission_release_backend(&adm);
    TEST_ASSERT(keel_admission_can_open_backend(&adm));
    TEST_END();
}

static void test_can_open_backend_unlimited(void)
{
    TEST_BEGIN("admission — can_open_backend unlimited");
    keel_admission_t adm;
    keel_admission_init(&adm, 0, 0, 0);

    /* Always true when unlimited */
    TEST_ASSERT(keel_admission_can_open_backend(&adm));
    for (int i = 0; i < 100; i++) keel_admission_try_backend(&adm);
    TEST_ASSERT(keel_admission_can_open_backend(&adm));
    TEST_END();
}

/* ============================================================================
 * §9 — Load Factor
 * ============================================================================ */

static void test_load_factor(void)
{
    TEST_BEGIN("admission — load factor");
    keel_admission_t adm;
    keel_admission_init(&adm, 10, 0, 0);

    TEST_ASSERT(keel_admission_load_factor(&adm) < 0.001);

    for (int i = 0; i < 5; i++) keel_admission_try_frontend(&adm);
    double lf = keel_admission_load_factor(&adm);
    TEST_ASSERT(lf > 0.49 && lf < 0.51);

    for (int i = 0; i < 5; i++) keel_admission_try_frontend(&adm);
    lf = keel_admission_load_factor(&adm);
    TEST_ASSERT(lf > 0.99 && lf < 1.01);
    TEST_END();
}

/* ============================================================================
 * §10 — NULL Safety
 * ============================================================================ */

static void test_null_safety(void)
{
    TEST_BEGIN("admission — NULL pointer safety");

    TEST_ASSERT_EQ(keel_admission_try_frontend(NULL), KEEL_ADMIT_REJECTED);
    TEST_ASSERT_EQ(keel_admission_try_backend(NULL), KEEL_ADMIT_REJECTED);
    keel_admission_release_frontend(NULL); /* should not crash */
    keel_admission_release_backend(NULL);  /* should not crash */
    TEST_ASSERT(!keel_admission_enqueue_waiter(NULL));
    TEST_ASSERT(!keel_admission_dequeue_waiter(NULL));
    keel_admission_timeout_waiter(NULL); /* should not crash */

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    printf("=== KEEL Admission Control Tests ===\n\n");

    test_init();
    test_init_null();
    test_frontend_admit_under_limit();
    test_frontend_reject_at_limit();
    test_frontend_release();
    test_frontend_unlimited();
    test_backend_admit_under_limit();
    test_backend_queued_at_limit();
    test_backend_rejected_queue_full();
    test_backend_release();
    test_backend_unlimited();
    test_waiter_enqueue_dequeue();
    test_waiter_dequeue_empty();
    test_waiter_queue_full();
    test_waiter_unlimited_queue();
    test_waiter_timeout();
    test_waiter_timeout_on_empty();
    test_peak_tracking();
    test_combined_pressure();
    test_rapid_cycles();
    test_can_open_backend();
    test_can_open_backend_unlimited();
    test_load_factor();
    test_null_safety();

    return test_summary();
}
