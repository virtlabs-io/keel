/**
 * @file failover_manager.h
 * @brief Active failover-manager detector loop.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The failover manager runs its own detector thread that polls per-server
 * probe state at a configurable cadence (failover_detection_interval),
 * applies a failure_threshold against consecutive role-change observations,
 * enforces a promotion_grace cooldown between consecutive flips, mutates
 * the authoritative server-pool topology, drains stale idle worker
 * connections on the affected pools, and finally bumps the router's
 * cluster epoch via keel_router_observe_primary() so dependent paths
 * (session timeline checks, degraded-mode exit, node fencing) converge
 * on the new topology.
 *
 * Architectural relationship to the probe manager:
 *   - probe_manager owns ONE concern: periodically interrogate each
 *     backend and update its per-server atomic state (health, detected
 *     role, latency, error). It must NOT mutate pool->servers[].role.
 *   - failover_manager owns ALL topology mutation: AUTO→role first
 *     resolution, RW↔RO flips, index rebuilds, idle-pool drains, and
 *     router epoch updates.
 *
 * Multi-primary mode (all configured servers are RW): the detector
 * loop runs but skips flip logic — each server is an independent
 * shard primary, not a replica.
 */

#ifndef KEEL_FAILOVER_MANAGER_H
#define KEEL_FAILOVER_MANAGER_H

#include "keel_types.h"
#include "keel_error.h"

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
typedef struct keel_engine          keel_engine_t;
typedef struct keel_server_pool     keel_server_pool_t;
typedef struct keel_router          keel_router_t;
typedef struct keel_probe_manager   keel_probe_manager_t;
typedef struct keel_failover_manager keel_failover_manager_t;

/* ============================================================================
 * Configuration
 * ============================================================================ */

typedef struct keel_failover_manager_config {
    uint32_t detection_interval_ms;   /**< Detector tick cadence (default 500) */
    uint32_t failure_threshold;       /**< Consecutive same-role observations before flip (default 3) */
    uint32_t promotion_grace_ms;      /**< Cooldown between consecutive flips (default 3000) */
    bool     old_primary_fencing;     /**< Mark demoted RW as DEMOTED (router-side) instead of DRAINING (default true) */
} keel_failover_manager_config_t;

#define KEEL_FAILOVER_MANAGER_CONFIG_DEFAULT { \
    .detection_interval_ms = 500,              \
    .failure_threshold     = 3,                \
    .promotion_grace_ms    = 3000,             \
    .old_primary_fencing   = true,             \
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Create a failover manager bound to one server pool.
 *
 * @param cfg       Detector cadence and policy. NULL → defaults.
 * @param probe_mgr Probe manager whose per-server states are sampled.
 *                  May be NULL when the caller drives flips manually
 *                  (testing); in that case the detector loop is a no-op
 *                  and only the public flip helper is useful.
 * @param pool      Authoritative server pool (roles mutated here).
 * @param engine    Engine handle for draining worker-owned idle pools.
 * @param router    Optional router; when non-NULL, flips invoke
 *                  keel_router_observe_primary() to bump the cluster
 *                  epoch. Pass NULL when no router is wired (rare).
 * @return New manager, or NULL on allocation / argument failure.
 */
keel_failover_manager_t* keel_failover_manager_create(
    const keel_failover_manager_config_t* cfg,
    keel_probe_manager_t*                  probe_mgr,
    keel_server_pool_t*                    pool,
    keel_engine_t*                         engine,
    keel_router_t*                         router);

/**
 * @brief Start the detector thread.
 *
 * Safe to call once. Subsequent calls on a running manager return
 * KEEL_OK without spawning a second thread.
 */
keel_error_t keel_failover_manager_start(keel_failover_manager_t* mgr);

/**
 * @brief Stop the detector thread and join.
 *
 * NULL-safe; idempotent.
 */
void keel_failover_manager_stop(keel_failover_manager_t* mgr);

/**
 * @brief Release all resources. Calls stop() first.
 */
void keel_failover_manager_destroy(keel_failover_manager_t* mgr);

/* ============================================================================
 * Inspection (for tests, admin console, metrics)
 * ============================================================================ */

/**
 * @brief Total number of flips this manager has performed since start.
 *
 * AUTO-first-resolution is NOT counted as a flip; only RW↔RO transitions
 * that involved displacing an existing RW server.
 */
uint64_t keel_failover_manager_flip_count(const keel_failover_manager_t* mgr);

/**
 * @brief Current logical timeline generation observed by this manager.
 *
 * Starts at 0 and increments by 1 on each performed flip. Used as the
 * @c timeline_id argument to keel_router_observe_primary().
 */
uint32_t keel_failover_manager_current_timeline(const keel_failover_manager_t* mgr);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_FAILOVER_MANAGER_H */
