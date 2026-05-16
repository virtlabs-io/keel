/**
 * @file admission.h
 * @brief Worker-local admission limits for frontends, backends, and waiters.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Admission control is KEEL's first line of overload management. Instead of
 * letting every accepted frontend immediately contend for scarce backend slots,
 * each worker keeps cheap local counters that answer three questions:
 *
 * - can this frontend be admitted at all?
 * - can it borrow or open a backend immediately?
 * - if not, is there still bounded room to wait?
 *
 * The design intentionally avoids global synchronization. Each worker enforces
 * its own configured share of capacity, which keeps the decision path O(1) and
 * lock-free. The tradeoff is that limits are approximate at process scope, but
 * that is acceptable because the goal is rapid local backpressure rather than
 * perfectly centralized quota accounting.
 */

#ifndef KEEL_ADMISSION_H
#define KEEL_ADMISSION_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Admission Result
 * ============================================================================ */

typedef enum keel_admit_result {
    KEEL_ADMIT_OK       = 0,    /**< Admitted — proceed */
    KEEL_ADMIT_QUEUED   = 1,    /**< Admitted to wait queue — will be woken */
    KEEL_ADMIT_REJECTED = 2,    /**< Rejected — over limit, send error */
} keel_admit_result_t;

/* ============================================================================
 * Admission Controller
 * ============================================================================ */

typedef struct keel_admission {
    /* Limits (set once at init from config) */
    uint32_t    max_frontends;      /**< Max frontend connections */
    uint32_t    max_backends;       /**< Max backend connections */
    uint32_t    max_waiting;        /**< Max waiting queue depth */

    /* Current counts */
    uint32_t    cur_frontends;      /**< Active frontend connections */
    uint32_t    cur_backends;       /**< Active backend connections */
    uint32_t    cur_waiting;        /**< Sessions in wait queue */

    /* Statistics */
    uint64_t    total_accepted;     /**< Lifetime accepted connections */
    uint64_t    total_rejected;     /**< Lifetime rejected connections */
    uint64_t    total_queued;       /**< Lifetime queued requests */
    uint64_t    total_queue_timeout;/**< Lifetime queue timeouts */
    uint64_t    peak_frontends;     /**< High-water mark */
    uint64_t    peak_backends;      /**< High-water mark */
    uint64_t    peak_waiting;       /**< High-water mark */
} keel_admission_t;

/* ============================================================================
 * API
 * ============================================================================ */

/**
 * @brief Initialize per-worker admission counters and configured limits.
 *
 * A limit of zero means "unbounded" for that dimension. Counters and high-water
 * marks start at zero.
 *
 * @param adm Admission controller to initialize.
 * @param max_frontends Maximum concurrent frontend sessions, or `0` for unlimited.
 * @param max_backends Maximum concurrent backend leases, or `0` for unlimited.
 * @param max_waiting Maximum queued waiters, or `0` for unlimited.
 * @return
 */
void keel_admission_init(keel_admission_t* adm,
                         uint32_t max_frontends,
                         uint32_t max_backends,
                         uint32_t max_waiting);

/**
 * @brief Attempt to consume one frontend-admission slot.
 *
 * This is called immediately after `accept()` succeeds. Rejecting here is
 * intentionally cheap and prevents the worker from performing additional setup
 * for a connection that would exceed its local capacity budget.
 *
 * @param adm Admission controller tracking worker-local frontend load.
 * @return `KEEL_ADMIT_OK` if a slot was reserved, otherwise `KEEL_ADMIT_REJECTED`.
 */
keel_admit_result_t keel_admission_try_frontend(keel_admission_t* adm);

/**
 * @brief Release one frontend-admission slot.
 *
 * @param adm Admission controller to update.
 * @return
 */
void keel_admission_release_frontend(keel_admission_t* adm);

/**
 * @brief Decide whether a session may proceed to backend acquisition.
 *
 * This is the key overload gate for pool pressure. A worker that has spare
 * backend capacity admits immediately. If backend capacity is exhausted but the
 * waiting budget still allows queueing, the caller can park the session. Only
 * when both limits are saturated does the request fail outright.
 *
 * @param adm Admission controller to query and update.
 * @return Admission result describing immediate admit, queue, or rejection.
 */
keel_admit_result_t keel_admission_try_backend(keel_admission_t* adm);

/**
 * @brief Release one backend-capacity slot.
 *
 * @param adm Admission controller to update.
 * @return
 */
void keel_admission_release_backend(keel_admission_t* adm);

/**
 * @brief Record that a session has entered the backend wait queue.
 *
 * This function only updates counters; queue ordering and wake-up mechanics live
 * elsewhere. The separation keeps the admission primitive simple and reusable.
 *
 * @param adm Admission controller to update.
 * @return `true` if the waiter count was incremented, otherwise `false` if full.
 */
bool keel_admission_enqueue_waiter(keel_admission_t* adm);

/**
 * @brief Record that one queued waiter has left the queue.
 *
 * @param adm Admission controller to update.
 * @return `true` if a waiter was removed, otherwise `false` if none were queued.
 */
bool keel_admission_dequeue_waiter(keel_admission_t* adm);

/**
 * @brief Record that a queued session timed out before receiving a backend.
 *
 * @param adm Admission controller to update.
 * @return
 */
void keel_admission_timeout_waiter(keel_admission_t* adm);

/**
 * @brief Check if a new backend connection can be opened
 */
static inline bool keel_admission_can_open_backend(const keel_admission_t* adm)
{
    if (adm->max_backends == 0) return true;  /* unlimited */
    return adm->cur_backends < adm->max_backends;
}

/**
 * @brief Get current load factor (frontends / max_frontends)
 */
static inline double keel_admission_load_factor(const keel_admission_t* adm)
{
    if (adm->max_frontends == 0) return 0.0;
    return (double)adm->cur_frontends / (double)adm->max_frontends;
}

/**
 * @brief Dump current counters and lifetime statistics to `stderr`.
 *
 * @param adm Admission controller to print.
 * @param label Optional label included in the dump output.
 * @return
 */
void keel_admission_dump(const keel_admission_t* adm, const char* label);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ADMISSION_H */
