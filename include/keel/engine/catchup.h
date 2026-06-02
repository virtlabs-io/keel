/**
 * @file catchup.h
 * @brief Reactor-owned replica catch-up wait list (Phase 2 of consistent reads).
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * # Purpose
 *
 * When a session requires read-after-write consistency and the candidate
 * replica is still behind the required LSN (PostgreSQL) or GTID set (MySQL),
 * the router can either:
 *   1. fall back to primary (default `stale_read_policy = route_primary`), or
 *   2. **park the session here** until the replica catches up, then resume it
 *      (`stale_read_policy = wait`), or
 *   3. reject the read (`stale_read_policy = reject`).
 *
 * This module owns option (2). It runs entirely inside one worker thread:
 *   - a per-worker **wait list** of `(session, server, token, deadline)`
 *     entries,
 *   - a per-worker **probe-socket cache** keyed by server index (one
 *     persistent socket per replica server, opened lazily, reused across
 *     probes, reopened with exponential backoff on failure),
 *   - a per-worker **probe-result cache** keyed by `(server, token)` with a
 *     short TTL so K parallel waiters for the same token cost one probe,
 *   - a per-worker **timer tick** (default 5 ms) that drives the probe
 *     state machines, expires waiters past their deadline, and applies
 *     backoff to failing replicas.
 *
 * Shared-nothing: the manager is owned by exactly one worker thread and
 * touched only from that worker's reactor; no locking is needed in the hot
 * path.  Cross-worker integration is out of scope — sessions are already
 * affined to one worker by `accept()`.
 *
 * # Phase 2a status
 *
 * This header defines the public API and types; the implementation in
 * `src/worker/worker_catchup.c` ships in two stages:
 *   - **2a** (this commit): manager lifecycle, wait-list data structure,
 *     probe-result cache, timer wiring. Probes are NOT issued yet — waiters
 *     are released only via deadline expiry. This is invisible to operators
 *     because `KEEL_STALE_READ_WAIT` is still downgraded to `ROUTE_PRIMARY`
 *     by `keel_router_create()`.
 *   - **2b/2c**: PostgreSQL + MySQL reactor-async probe state machines wired
 *     into the tick callback.
 *   - **2d**: router emits a `WAIT` decision, engine returns
 *     `KEEL_FLOW_WAIT_CATCHUP`, WARN downgrade is removed.
 */

#ifndef KEEL_ENGINE_CATCHUP_H
#define KEEL_ENGINE_CATCHUP_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "keel/plugin/plugin_types.h"   /* keel_consistency_token_t */

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
struct keel_worker;
struct keel_session;
struct keel_reactor;
struct keel_catchup_manager;
struct keel_catchup_waiter;

typedef struct keel_catchup_manager keel_catchup_manager_t;
typedef struct keel_catchup_waiter  keel_catchup_waiter_t;

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @brief Tunables for the catch-up manager.
 *
 * Field names use `_ms` suffixes internally to match the rest of KEEL's C
 * runtime conventions; the *INI keys* that map into these fields are
 * unit-bearing (e.g. `tick_interval = 5ms`, `probe_backoff_max = 30s`).
 */
typedef struct keel_catchup_config {
    uint32_t tick_interval_ms;         /**< Timer tick (default 5).             INI: `catchup_tick_interval = 5ms` */
    uint32_t cache_ttl_ms;             /**< Probe-result cache TTL (default 100).  INI: `catchup_cache_ttl = 100ms` */
    uint32_t probe_backoff_initial_ms; /**< Initial reopen backoff (default 50).   INI: `catchup_probe_backoff_initial = 50ms` */
    uint32_t probe_backoff_max_ms;     /**< Backoff cap (default 30000).            INI: `catchup_probe_backoff_max = 30s` */
    uint32_t probe_idle_close_ms;      /**< Auto-close probe socket after this much idle time (0 = never). INI: `catchup_probe_idle_close = 0s` */
    uint32_t probe_timeout_ms;         /**< Per-probe send/recv deadline (default 1000). INI: `catchup_probe_timeout = 1s` */
    uint32_t max_waiters;              /**< Hard cap on simultaneously parked sessions (default 4096; 0 = unlimited). */
} keel_catchup_config_t;

#define KEEL_CATCHUP_CONFIG_DEFAULT { \
    .tick_interval_ms          = 5,     \
    .cache_ttl_ms              = 100,   \
    .probe_backoff_initial_ms  = 50,    \
    .probe_backoff_max_ms      = 30000, \
    .probe_idle_close_ms       = 0,     \
    .probe_timeout_ms          = 1000,  \
    .max_waiters               = 4096,  \
}

/* ============================================================================
 * Resume contract
 * ============================================================================ */

/**
 * @brief Outcome reported back to the engine when a waiter is released.
 */
typedef enum keel_catchup_outcome {
    KEEL_CATCHUP_REACHED = 0,  /**< Replica reached the token — re-dispatch on the originally selected replica */
    KEEL_CATCHUP_TIMEOUT,      /**< `max_replica_catchup_ms` elapsed before any probe succeeded — fall back per `stale_read_policy` */
    KEEL_CATCHUP_PROBE_FAILED, /**< All probe attempts errored (e.g., replica unreachable) — treat as timeout per policy */
    KEEL_CATCHUP_CANCELLED,    /**< Session closed or the waiter was explicitly cancelled */
} keel_catchup_outcome_t;

/**
 * @brief Callback invoked from the worker thread when a waiter is released.
 *
 * Called from the worker's reactor context (timer tick or probe completion).
 * The implementation MUST be quick and non-blocking; long work belongs in
 * the engine flow path the callback hands control back to.
 *
 * After the callback returns, the `waiter` pointer is invalid — the manager
 * has already removed it from the wait list and freed it.
 *
 * @param session  Original session pointer captured at enqueue time.
 * @param outcome  Why the waiter is being released.
 * @param userdata Opaque pointer passed to `keel_catchup_enqueue()`.
 */
typedef void (*keel_catchup_resume_cb)(struct keel_session* session,
                                       keel_catchup_outcome_t outcome,
                                       void* userdata);

/* ============================================================================
 * Manager lifecycle
 * ============================================================================
 * The manager is normally created and destroyed by the owning worker; tests
 * may create one standalone (worker pointer may be NULL for unit tests).
 */

/**
 * @brief Allocate a catch-up manager.
 *
 * @param worker  Owning worker (may be NULL in unit tests).
 * @param config  Configuration (NULL = defaults).
 * @return        New manager handle, or NULL on allocation failure.
 */
keel_catchup_manager_t* keel_catchup_manager_create(
    struct keel_worker* worker,
    const keel_catchup_config_t* config);

/**
 * @brief Destroy a catch-up manager.
 *
 * Cancels every waiter (their resume callbacks fire with
 * KEEL_CATCHUP_CANCELLED), closes every probe socket, then frees the
 * manager.  Safe to call with NULL.
 */
void keel_catchup_manager_destroy(keel_catchup_manager_t* m);

/* ============================================================================
 * Wait-list operations
 * ============================================================================ */

/**
 * @brief Park a session until @p server reaches @p token (or deadline).
 *
 * @param m              Manager.
 * @param session        Session being parked. Stored verbatim; not dereferenced
 *                       by the manager except to pass back to @p resume.
 * @param server_index   Index into `keel_server_pool_t::servers[]`.
 *                       The probe-socket cache is keyed on this.
 * @param token          Required consistency token (copied by value).
 * @param max_wait_ms    Absolute upper bound — typically
 *                       `router_config.max_replica_catchup_ms`.
 * @param resume         Callback invoked when the waiter is released.
 * @param userdata       Opaque pointer forwarded to @p resume.
 * @return               Opaque waiter handle on success (owned by the
 *                       manager; valid only until the resume callback
 *                       fires), or NULL if the wait list is full or the
 *                       inputs were rejected.
 */
keel_catchup_waiter_t* keel_catchup_enqueue(
    keel_catchup_manager_t* m,
    struct keel_session* session,
    size_t server_index,
    const keel_consistency_token_t* token,
    uint32_t max_wait_ms,
    keel_catchup_resume_cb resume,
    void* userdata);

/**
 * @brief Cancel a parked waiter.
 *
 * Triggers the resume callback with KEEL_CATCHUP_CANCELLED unless the
 * waiter has already fired, in which case this is a no-op. Safe to call
 * from the resume callback (returns immediately without re-firing).
 */
void keel_catchup_cancel(keel_catchup_manager_t* m,
                         keel_catchup_waiter_t* w);

/* ============================================================================
 * Reactor integration
 * ============================================================================ */

/**
 * @brief Periodic tick driving the wait list and probe state machines.
 *
 * Normally registered with the worker's timer wheel and called every
 * `config.tick_interval_ms`. Exposed for tests that drive time manually.
 *
 * @param m       Manager.
 * @param now_ns  Current monotonic time in nanoseconds.
 */
void keel_catchup_manager_tick(keel_catchup_manager_t* m, uint64_t now_ns);

/* ============================================================================
 * Introspection (test + admin)
 * ============================================================================ */

/**
 * @brief Snapshot of manager runtime state.
 */
typedef struct keel_catchup_stats_snapshot {
    size_t   waiters_active;
    size_t   waiters_high_water;
    size_t   probe_sockets_open;
    uint64_t waiters_enqueued_total;
    uint64_t waiters_fulfilled_total;
    uint64_t waiters_timeout_total;
    uint64_t waiters_cancelled_total;
    uint64_t probes_issued_total;
    uint64_t probes_succeeded_total;
    uint64_t probes_failed_total;
    uint64_t cache_hits_total;
} keel_catchup_stats_snapshot_t;

/**
 * @brief Read current manager stats. Safe to call from the owning worker.
 */
void keel_catchup_manager_snapshot(const keel_catchup_manager_t* m,
                                   keel_catchup_stats_snapshot_t* out);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ENGINE_CATCHUP_H */
