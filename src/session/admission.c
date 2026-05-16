/**
 * @file admission.c
 * @brief Worker-local admission counters for overload and queue pressure control.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Admission decisions must be cheap because they run on the connection and
 * backend-borrow hot paths. This module therefore implements only local counter
 * bookkeeping and limit checks; scheduling policy, wake-up order, and queue data
 * structures are left to higher layers.
 */

#include "keel/session/admission.h"

#include <string.h>
#include <stdio.h>

/* ============================================================================
 * Internal: Update high-water marks
 * ============================================================================ */

/**
 * @brief Maintain a monotonic high-water mark for one counter.
 *
 * @param peak [in,out] Peak value to update.
 * @param current Current live count.
 * @return
 */
static inline void update_peak(uint64_t* peak, uint32_t current)
{
    if ((uint64_t)current > *peak) {
        *peak = (uint64_t)current;
    }
}

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Initialize local admission limits and zero all statistics.
 */
void keel_admission_init(keel_admission_t* adm,
                         uint32_t max_frontends,
                         uint32_t max_backends,
                         uint32_t max_waiting)
{
    if (!adm) return;

    memset(adm, 0, sizeof(*adm));
    adm->max_frontends = max_frontends;
    adm->max_backends  = max_backends;
    adm->max_waiting   = max_waiting;
}

/**
 * @brief Attempt to admit one new frontend connection.
 */
keel_admit_result_t keel_admission_try_frontend(keel_admission_t* adm)
{
    if (!adm) return KEEL_ADMIT_REJECTED;

    /* Check frontend limit (0 = unlimited) */
    if (adm->max_frontends > 0 && adm->cur_frontends >= adm->max_frontends) {
        adm->total_rejected++;
        return KEEL_ADMIT_REJECTED;
    }

    adm->cur_frontends++;
    adm->total_accepted++;
    update_peak(&adm->peak_frontends, adm->cur_frontends);

    return KEEL_ADMIT_OK;
}

/**
 * @brief Release one live frontend slot.
 */
void keel_admission_release_frontend(keel_admission_t* adm)
{
    if (!adm || adm->cur_frontends == 0) return;
    adm->cur_frontends--;
}

/**
 * @brief Decide whether backend demand may proceed immediately, queue, or fail.
 */
keel_admit_result_t keel_admission_try_backend(keel_admission_t* adm)
{
    if (!adm) return KEEL_ADMIT_REJECTED;

    /* Check backend limit */
    if (adm->max_backends > 0 && adm->cur_backends >= adm->max_backends) {
        /* Backend at limit — check if we can queue */
        if (adm->max_waiting == 0 || adm->cur_waiting < adm->max_waiting) {
            return KEEL_ADMIT_QUEUED;
        }
        /* Both at limit */
        adm->total_rejected++;
        return KEEL_ADMIT_REJECTED;
    }

    adm->cur_backends++;
    update_peak(&adm->peak_backends, adm->cur_backends);

    return KEEL_ADMIT_OK;
}

/**
 * @brief Release one backend-capacity slot.
 */
void keel_admission_release_backend(keel_admission_t* adm)
{
    if (!adm || adm->cur_backends == 0) return;
    adm->cur_backends--;
}

/**
 * @brief Increment the waiter count when a session enters the backend queue.
 */
bool keel_admission_enqueue_waiter(keel_admission_t* adm)
{
    if (!adm) return false;

    if (adm->max_waiting > 0 && adm->cur_waiting >= adm->max_waiting) {
        return false;  /* Queue full */
    }

    adm->cur_waiting++;
    adm->total_queued++;
    update_peak(&adm->peak_waiting, adm->cur_waiting);

    return true;
}

/**
 * @brief Decrement the waiter count when a queued session resumes or leaves.
 */
bool keel_admission_dequeue_waiter(keel_admission_t* adm)
{
    if (!adm || adm->cur_waiting == 0) return false;
    adm->cur_waiting--;
    return true;
}

/**
 * @brief Record that one queued session timed out before service.
 */
void keel_admission_timeout_waiter(keel_admission_t* adm)
{
    if (!adm) return;
    if (adm->cur_waiting > 0) adm->cur_waiting--;
    adm->total_queue_timeout++;
}

/**
 * @brief Print a human-readable snapshot of admission counters and peaks.
 */
void keel_admission_dump(const keel_admission_t* adm, const char* label)
{
    if (!adm) {
        fprintf(stderr, "[admission] %s: (null)\n", label ? label : "?");
        return;
    }

    fprintf(stderr,
            "[admission] %s: "
            "fe=%u/%u be=%u/%u wait=%u/%u | "
            "accepted=%lu rejected=%lu queued=%lu timeout=%lu | "
            "peak(fe=%lu be=%lu wait=%lu)\n",
            label ? label : "?",
            adm->cur_frontends, adm->max_frontends,
            adm->cur_backends, adm->max_backends,
            adm->cur_waiting, adm->max_waiting,
            (unsigned long)adm->total_accepted,
            (unsigned long)adm->total_rejected,
            (unsigned long)adm->total_queued,
            (unsigned long)adm->total_queue_timeout,
            (unsigned long)adm->peak_frontends,
            (unsigned long)adm->peak_backends,
            (unsigned long)adm->peak_waiting);
}
