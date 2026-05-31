/**
 * @file probe_manager.c
 * @brief Manager thread that executes probes and converts observations into topology updates.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The manager embodies KEEL's failover control loop. It periodically samples all
 * configured backends, tracks transient versus sustained failure, and updates the
 * authoritative server-pool metadata that routing decisions consult. A key design
 * choice is that the manager does not rewrite worker routing code directly; it
 * mutates shared pool metadata and drains stale idle connections so the existing
 * routing and pooling machinery naturally converges on the new topology.
 */

#include "keel/probe/probe.h"
#include "keel/engine/engine.h"
#include "keel/engine/worker.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel_hook.h"

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Forward declaration to access backend_pool */
struct backend_pool;

/* Pool update/drain functions from backend_pool.h */
void backend_pool_update_target(struct backend_pool* pool, const char* host, uint16_t port);
size_t backend_pool_drain_idle(struct backend_pool* pool);

/* ============================================================================
 * Probe Manager Structure
 * ============================================================================ */

/**
 * @brief Probe manager internal state.
 *
 * Owns: probe contexts array, health state array, and the probe thread.
 * The thread iterates all servers in round-robin, calling ops->check()
 * for each.  Health transitions and role changes trigger logging and
 * potentially a failover (pool pointer swap across all workers).
 */
struct keel_probe_manager {
    /* Configuration */
    keel_probe_config_t  config;

    /* The plugin ops resolved from config.probe_type */
    const keel_probe_ops_t* ops;
    
    /* Per-server probe contexts (from ops->create) */
    void**              probe_ctxs;     /* [server_pool->count] */
    
    /* Per-server health state */
    keel_server_state_t* states;         /* [server_pool->count] */
    
    /* Server pool being monitored */
    keel_server_pool_t*  pool;
    
    /* Engine handle (for accessing workers) */
    keel_engine_t*       engine;

    /* Multi-primary mode: all servers were configured with role=RW.
     * In this mode the probe skips role-based failover — each server
     * is an independent shard primary, not a replica. */
    bool                 multi_primary_mode;
    
    /* Thread */
    pthread_t           thread;
    volatile bool       running;
    volatile bool       should_stop;
};

/* ============================================================================
 * Time Helper
 * ============================================================================ */

/**
 * @brief Read a monotonic nanosecond timestamp for probe scheduling and accounting.
 *
 * @return Monotonic time in nanoseconds.
 */
static uint64_t clock_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ============================================================================
 * Failover: Role Update + Index Rebuild
 * ============================================================================
 *
 * When we detect that server[changed_idx] has a new role:
 *
 *   1. Update the server's role in the pool metadata
 *   2. If the new role is RW and another server was RW, demote it to RO
 *   3. Rebuild server_pool index arrays (rw_indices, ro_indices, wo_indices)
 *   4. Drain idle connections in affected server_pools[] to force reconnects
 *
 * This is safe because:
 *   - Workers use index arrays for routing; pointer swap is not needed
 *     since each server already has its own pool (server_pools[i])
 *   - Index array updates (count + indices) use atomic-width writes
 *   - Existing connections drain naturally when returned to pools
 */

/**
 * @brief Apply a primary-role transition to the live server-pool topology.
 *
 * The algorithm is deliberately conservative: it updates the role metadata first,
 * rebuilds the role-specific index arrays second, and only then drains idle worker
 * connections associated with the affected backends. This sequencing ensures that
 * new borrows see the new role mapping before stale idle connections are recycled.
 *
 * @param mgr Probe manager owning the monitored pool.
 * @param old_rw_idx Index of the previously writable backend.
 * @param new_rw_idx Index of the backend now considered writable.
 * @return
 */
static void perform_failover(keel_probe_manager_t* mgr,
                             size_t old_rw_idx,
                             size_t new_rw_idx)
{
    keel_server_pool_t* pool = mgr->pool;
    uint32_t nworkers = keel_engine_get_num_workers(mgr->engine);

    KEEL_LOG_WARN(KEEL_LOG_CAT_PROBE,
            "FAILOVER: server[%zu] (%s:%u) → RW, "
            "server[%zu] (%s:%u) → RO",
            new_rw_idx,
            pool->servers[new_rw_idx].host,
            pool->servers[new_rw_idx].port,
            old_rw_idx,
            pool->servers[old_rw_idx].host,
            pool->servers[old_rw_idx].port);

    /* Update server roles */
    pool->servers[old_rw_idx].role = KEEL_SERVER_ROLE_RO;
    pool->servers[new_rw_idx].role = KEEL_SERVER_ROLE_RW;

    /* Rebuild the index arrays so routing picks up the change */
    keel_server_pool_rebuild_indices(pool);

    /* Drain idle connections in the demoted and promoted server pools
     * across all workers.  This forces refill to reconnect with proper
     * session state for the new role. */
    for (uint32_t w = 0; w < nworkers; w++) {
        keel_worker_t* worker = keel_engine_get_worker_mut(mgr->engine, w);
        if (!worker) continue;

        if (old_rw_idx < worker->server_pool_count && worker->server_pools[old_rw_idx]) {
            backend_pool_drain_idle(worker->server_pools[old_rw_idx]);
        }
        if (new_rw_idx < worker->server_pool_count && worker->server_pools[new_rw_idx]) {
            backend_pool_drain_idle(worker->server_pools[new_rw_idx]);
        }
    }

    KEEL_LOG_WARN(KEEL_LOG_CAT_PROBE,
            "FAILOVER: complete — indices rebuilt, idle connections drained in %u workers",
            nworkers);
}

/* ============================================================================
 * Probe Thread
 * ============================================================================ */

/**
 * @brief Probe-thread main loop.
 *
 * @param arg Probe manager instance.
 * @return Always `NULL` when the thread exits.
 */
static void* probe_thread(void* arg)
{
    keel_probe_manager_t* mgr = (keel_probe_manager_t*)arg;

#if defined(__APPLE__) || defined(__FreeBSD__)
    /* On macOS/BSD pthread_setname_np sets the current thread's name only */
    pthread_setname_np("keel-probe");
#endif

    keel_server_pool_t* pool = mgr->pool;

    KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE,
            "probe: thread started — monitoring %zu servers "
            "(type=%s, interval=%ums, timeout=%ums, retries=%u)",
            pool->count, mgr->config.probe_type,
            mgr->config.interval_ms, mgr->config.timeout_ms,
            mgr->config.retries);

    mgr->running = true;

    while (!mgr->should_stop) {
        uint64_t cycle_start = clock_ns();

        for (size_t i = 0; i < pool->count && !mgr->should_stop; i++) {
            keel_backend_server_t* server = &pool->servers[i];
            keel_server_state_t*   state  = &mgr->states[i];
            void*                 ctx    = mgr->probe_ctxs[i];

            if (!ctx) continue;

            /* Execute probe */
            keel_probe_check_t result;
            keel_error_t err = mgr->ops->check(ctx, server, &result);
            (void)err;  /* check() returns KEEL_OK even when server is DOWN */

            uint64_t now = clock_ns();
            atomic_store_explicit(&state->last_check_ns, now, memory_order_relaxed);
            atomic_store_explicit(&state->last_latency_us, result.latency_us, memory_order_relaxed);
            atomic_fetch_add_explicit(&state->total_checks, 1, memory_order_relaxed);

            /* Health transition logic */
            int prev_health = atomic_load_explicit(&state->health, memory_order_relaxed);

            if (result.health == KEEL_HEALTH_UP) {
                /* Healthy — reset failure counter */
                atomic_store_explicit(&state->consecutive_failures, 0, memory_order_relaxed);

                if (prev_health != KEEL_HEALTH_UP) {
                    KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE,
                            "probe: server[%zu] %s:%u is UP (was %s) — %s",
                            i, server->host, server->port,
                            keel_health_status_str((keel_health_status_t)prev_health),
                            result.message);
                    server->healthy = true;
                    atomic_store_explicit(&state->health, KEEL_HEALTH_UP, memory_order_release);

                    /* Drain stale idle pool connections on DOWN→UP transition.
                     * When a backend restarts it resets all TCP connections, so
                     * every idle connection in KEEL's pool is now stale.  Drain
                     * them now — across all workers — so the pool refill timer
                     * establishes fresh connections before the next client borrow,
                     * instead of clients receiving "server closed the connection
                     * unexpectedly" on their first query to the recovered shard. */
                    if (prev_health == KEEL_HEALTH_DOWN && mgr->engine) {
                        uint32_t nworkers = keel_engine_get_num_workers(mgr->engine);
                        uint32_t drained_workers = 0;
                        for (uint32_t w = 0; w < nworkers; w++) {
                            keel_worker_t* worker = keel_engine_get_worker_mut(mgr->engine, w);
                            if (!worker) continue;
                            if (i < (size_t)worker->server_pool_count &&
                                worker->server_pools[i]) {
                                backend_pool_drain_idle(worker->server_pools[i]);
                                drained_workers++;
                            }
                        }
                        if (drained_workers > 0)
                            KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE,
                                    "probe: server[%zu] %s:%u — drained stale pool "
                                    "connections in %u/%u workers",
                                    i, server->host, server->port,
                                    drained_workers, nworkers);
                    }

                    /* === HOOK: ON_HEALTH_CHANGE (UP) ===
                     * Fires after a DOWN→UP or UNKNOWN→UP transition.
                     * Runs on the probe thread — callbacks must be non-blocking.
                     * Zero-cost when no hooks are registered for this point. */
                    struct keel_hook_registry* hreg = mgr->config.hook_registry;
                    if (hreg && KEEL_HOOK_FIRED_FOR(hreg, KEEL_HOOK_ON_HEALTH_CHANGE)) {
                        keel_hook_health_ctx_t hctx = {
                            .host           = server->host,
                            .port           = server->port,
                            .shard_index    = i,
                            .is_primary     = (server->role == KEEL_SERVER_ROLE_PRIMARY),
                            .prev_health    = prev_health,
                            .curr_health    = KEEL_HEALTH_UP,
                            .prev_health_str= keel_health_status_str((keel_health_status_t)prev_health),
                            .curr_health_str= keel_health_status_str(KEEL_HEALTH_UP),
                            .probe_latency_us = result.latency_us,
                            .error_detail   = NULL,
                            .suppress_log   = false,
                        };
                        keel_hook_ctx_t hook_ctx = { .ext = &hctx,
                                                      .hook_point = KEEL_HOOK_ON_HEALTH_CHANGE };
                        keel_hook_fire(hreg, KEEL_HOOK_ON_HEALTH_CHANGE, &hook_ctx);
                    }
                } else {
                    atomic_store_explicit(&state->health, KEEL_HEALTH_UP, memory_order_release);
                }
            } else {
                /* Unhealthy — increment failures */
                uint32_t fails = atomic_fetch_add_explicit(&state->consecutive_failures, 1,
                                                           memory_order_relaxed) + 1;
                atomic_fetch_add_explicit(&state->total_failures, 1, memory_order_relaxed);
                snprintf(state->last_error, sizeof(state->last_error), "%s", result.message);

                if (fails >= mgr->config.retries) {
                    if (prev_health != KEEL_HEALTH_DOWN) {
                        KEEL_LOG_WARN(KEEL_LOG_CAT_PROBE,
                                "probe: server[%zu] %s:%u is DOWN "
                                "after %u consecutive failures — %s",
                                i, server->host, server->port, fails, result.message);
                        server->healthy = false;
                        atomic_store_explicit(&state->health, KEEL_HEALTH_DOWN, memory_order_release);

                        /* === HOOK: ON_HEALTH_CHANGE (DOWN) ===
                         * Fires after a UP→DOWN or UNKNOWN→DOWN transition.
                         * Runs on the probe thread — callbacks must be non-blocking.
                         * Zero-cost when no hooks are registered for this point. */
                        struct keel_hook_registry* hreg = mgr->config.hook_registry;
                        if (hreg && KEEL_HOOK_FIRED_FOR(hreg, KEEL_HOOK_ON_HEALTH_CHANGE)) {
                            keel_hook_health_ctx_t hctx = {
                                .host           = server->host,
                                .port           = server->port,
                                .shard_index    = i,
                                .is_primary     = (server->role == KEEL_SERVER_ROLE_PRIMARY),
                                .prev_health    = prev_health,
                                .curr_health    = KEEL_HEALTH_DOWN,
                                .prev_health_str= keel_health_status_str((keel_health_status_t)prev_health),
                                .curr_health_str= keel_health_status_str(KEEL_HEALTH_DOWN),
                                .probe_latency_us = result.latency_us,
                                .error_detail   = result.message,
                                .suppress_log   = false,
                            };
                            keel_hook_ctx_t hook_ctx = { .ext = &hctx,
                                                          .hook_point = KEEL_HOOK_ON_HEALTH_CHANGE };
                            keel_hook_fire(hreg, KEEL_HOOK_ON_HEALTH_CHANGE, &hook_ctx);
                        }
                    } else {
                        atomic_store_explicit(&state->health, KEEL_HEALTH_DOWN, memory_order_release);
                    }
                } else {
                    KEEL_LOG_DEBUG(KEEL_LOG_CAT_PROBE,
                            "probe: server[%zu] %s:%u failure %u/%u — %s",
                            i, server->host, server->port, fails, mgr->config.retries,
                            result.message);
                }
            }

            /* Role change detection — skipped in multi-primary mode where all
             * servers are independent primaries (role failover not applicable). */
            if (!mgr->multi_primary_mode &&
                result.health == KEEL_HEALTH_UP &&
                result.detected_role != KEEL_SERVER_ROLE_AUTO)
            {
                int prev_role = atomic_load_explicit(&state->detected_role, memory_order_relaxed);
                atomic_store_explicit(&state->detected_role, result.detected_role,
                                      memory_order_release);

                if (prev_role != result.detected_role &&
                    prev_role != KEEL_SERVER_ROLE_AUTO)
                {
                    /* Role actually changed (not first detection) */
                    const char* old_str = prev_role == KEEL_SERVER_ROLE_RW ? "RW" :
                                          prev_role == KEEL_SERVER_ROLE_RO ? "RO" :
                                          prev_role == KEEL_SERVER_ROLE_WO ? "WO" : "AUTO";
                    const char* new_str = result.detected_role == KEEL_SERVER_ROLE_RW ? "RW" :
                                          result.detected_role == KEEL_SERVER_ROLE_RO ? "RO" :
                                          result.detected_role == KEEL_SERVER_ROLE_WO ? "WO" : "AUTO";
                    KEEL_LOG_WARN(KEEL_LOG_CAT_PROBE,
                            "probe: server[%zu] %s:%u role changed: %s → %s",
                            i, server->host, server->port, old_str, new_str);

                    /* If this server became RW and another server was RW, trigger failover */
                    if (result.detected_role == KEEL_SERVER_ROLE_RW) {
                        /* Find the current RW server (if any) */
                        size_t old_rw_idx = SIZE_MAX;
                        for (size_t s = 0; s < pool->count; s++) {
                            if (s != i && pool->servers[s].role == KEEL_SERVER_ROLE_RW) {
                                old_rw_idx = s;
                                break;
                            }
                        }

                        if (old_rw_idx != SIZE_MAX) {
                            /* Wait failover_delay before acting */
                            if (mgr->config.failover_delay_ms > 0) {
                                KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE,
                                        "probe: waiting %ums before failover",
                                        mgr->config.failover_delay_ms);
                                usleep(mgr->config.failover_delay_ms * 1000u);
                            }

                            /* Re-verify: check the candidate again */
                            keel_probe_check_t verify;
                            mgr->ops->check(ctx, server, &verify);
                            if (verify.health == KEEL_HEALTH_UP &&
                                verify.detected_role == KEEL_SERVER_ROLE_RW)
                            {
                                perform_failover(mgr, old_rw_idx, i);
                            } else {
                                KEEL_LOG_WARN(KEEL_LOG_CAT_PROBE,
                                        "probe: failover aborted — "
                                        "re-verify failed (health=%s, role=%d)",
                                        keel_health_status_str(verify.health),
                                        verify.detected_role);
                            }
                        } else {
                            /* No existing RW — just update the role and rebuild */
                            server->role = KEEL_SERVER_ROLE_RW;
                            keel_server_pool_rebuild_indices(pool);
                        }
                    } else {
                        /* Non-RW role change — update role and rebuild */
                        server->role = (keel_server_role_t)result.detected_role;
                        keel_server_pool_rebuild_indices(pool);
                    }
                } else if (prev_role == KEEL_SERVER_ROLE_AUTO) {
                    /* First detection — resolve AUTO to actual role */
                    const char* role_str = result.detected_role == KEEL_SERVER_ROLE_RW ? "RW" :
                                           result.detected_role == KEEL_SERVER_ROLE_RO ? "RO" :
                                           result.detected_role == KEEL_SERVER_ROLE_WO ? "WO" : "AUTO";
                    KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE,
                            "probe: server[%zu] %s:%u detected as %s",
                            i, server->host, server->port, role_str);
                    server->role = (keel_server_role_t)result.detected_role;

                    /* Critical: if this server is detected as RW but no other
                     * server is currently RW, just rebuild indices.  If another
                     * server is already RW (from config or from a previous
                     * resolve), do NOT trigger a failover during initial
                     * discovery — that causes AB-BA oscillation when the
                     * cluster has two primaries (split-brain or stale state).
                     * Demote this server to RO instead; an operator-driven
                     * topology change can promote it via a real role flip,
                     * which is handled by the normal role-change branch above. */
                    if (result.detected_role == KEEL_SERVER_ROLE_RW) {
                        size_t existing_rw_idx = SIZE_MAX;
                        for (size_t s = 0; s < pool->count; s++) {
                            if (s != i && pool->servers[s].role == KEEL_SERVER_ROLE_RW) {
                                existing_rw_idx = s;
                                break;
                            }
                        }

                        if (existing_rw_idx != SIZE_MAX) {
                            KEEL_LOG_WARN(KEEL_LOG_CAT_PROBE,
                                    "probe: server[%zu] %s:%u detected RW but "
                                    "server[%zu] %s:%u is already RW — keeping "
                                    "server[%zu] as RO (avoids AB-BA failover "
                                    "during initial discovery; investigate "
                                    "possible split-brain)",
                                    i, server->host, server->port,
                                    existing_rw_idx,
                                    pool->servers[existing_rw_idx].host,
                                    pool->servers[existing_rw_idx].port,
                                    i);
                            server->role = KEEL_SERVER_ROLE_RO;
                            keel_server_pool_rebuild_indices(pool);
                        } else {
                            /* No conflict — just rebuild indices */
                            keel_server_pool_rebuild_indices(pool);
                        }
                    } else {
                        /* Non-RW first detection — rebuild indices */
                        keel_server_pool_rebuild_indices(pool);
                    }
                }
            }
        }

        /* Sleep until next cycle */
        uint64_t elapsed_ms = (clock_ns() - cycle_start) / 1000000ULL;
        if (elapsed_ms < mgr->config.interval_ms && !mgr->should_stop) {
            uint32_t sleep_ms = mgr->config.interval_ms - (uint32_t)elapsed_ms;
            /* Use nanosleep for precision and interruptibility */
            struct timespec ts = {
                .tv_sec  = sleep_ms / 1000,
                .tv_nsec = (long)(sleep_ms % 1000) * 1000000L,
            };
            while (nanosleep(&ts, &ts) < 0 && !mgr->should_stop) {
                /* EINTR — check should_stop and retry */
            }
        }
    }

    mgr->running = false;
    KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE, "probe: thread stopped");
    return NULL;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Create a probe manager.
 *
 * Resolves the probe plugin from the registry, allocates per-server
 * probe contexts and health state arrays, and initialises everything
 * to UNKNOWN/AUTO.
 *
 * @param config  Probe timing/retry configuration
 * @param pool    Server pool to monitor (must have count > 0)
 * @param engine  Engine handle (used to access workers for failover)
 * @return New manager, or NULL on error (logged)
 */
keel_probe_manager_t* keel_probe_manager_create(
    const keel_probe_config_t* config,
    keel_server_pool_t*        pool,
    keel_engine_t*             engine)
{
    if (!config || !pool || !engine || pool->count == 0) return NULL;

    /* Resolve probe plugin */
    const keel_probe_ops_t* ops = keel_probe_lookup(config->probe_type);
    if (!ops) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_PROBE,
                "probe: unknown probe type '%s'", config->probe_type);
        return NULL;
    }

    keel_probe_manager_t* mgr = keel_calloc(1, sizeof(keel_probe_manager_t));
    if (!mgr) return NULL;

    mgr->config = *config;
    mgr->ops    = ops;
    mgr->pool   = pool;
    mgr->engine = engine;

    /* Allocate per-server arrays */
    mgr->probe_ctxs = keel_calloc(pool->count, sizeof(void*));
    mgr->states     = keel_calloc(pool->count, sizeof(keel_server_state_t));
    if (!mgr->probe_ctxs || !mgr->states) {
        keel_free(mgr->probe_ctxs);
        keel_free(mgr->states);
        keel_free(mgr);
        return NULL;
    }

    /* Detect multi-primary mode: when ALL servers are configured RW they are
     * independent shard primaries.  In this mode we suppress role-based
     * failover so the probe only tracks UP/DOWN health. */
    bool all_rw = (pool->count > 1);
    for (size_t s = 0; s < pool->count && all_rw; s++) {
        if (pool->servers[s].role != KEEL_SERVER_ROLE_RW)
            all_rw = false;
    }
    mgr->multi_primary_mode = all_rw;
    if (all_rw)
        KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE,
            "probe: multi-primary mode — role failover disabled (all %zu servers are RW)",
            pool->count);

    /* Create per-server probe contexts */
    for (size_t i = 0; i < pool->count; i++) {
        mgr->probe_ctxs[i] = ops->create(&pool->servers[i], config->probe_extra);
        if (!mgr->probe_ctxs[i]) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_PROBE,
                    "probe: failed to create context for server[%zu] %s:%u",
                    i, pool->servers[i].host, pool->servers[i].port);
        }
        /* Initialize state */
        atomic_store(&mgr->states[i].health, KEEL_HEALTH_UNKNOWN);
        atomic_store(&mgr->states[i].detected_role, KEEL_SERVER_ROLE_AUTO);
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_PROBE,
            "probe: manager created — type=%s, %zu servers, "
            "interval=%ums, timeout=%ums",
            ops->name, pool->count, config->interval_ms, config->timeout_ms);

    return mgr;
}

/**
 * @brief Start the probe thread.
 *
 * Creates a pthread named "keel-probe" that will begin health-checking
 * all servers in the pool.  Safe to call only once (idempotent if
 * already running).
 *
 * @param mgr  Probe manager (from keel_probe_manager_create)
 * @return KEEL_OK on success, error code on failure
 */
keel_error_t keel_probe_manager_start(keel_probe_manager_t* mgr)
{
    if (!mgr) return KEEL_ERR_INVALID_ARG;
    if (mgr->running) return KEEL_OK;

    mgr->should_stop = false;

    int rc = pthread_create(&mgr->thread, NULL, probe_thread, mgr);
    if (rc != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_PROBE,
                "probe: failed to create thread: %s", strerror(rc));
        return KEEL_ERR_UNKNOWN;
    }

#ifdef __linux__
    pthread_setname_np(mgr->thread, "keel-probe");
#endif
    return KEEL_OK;
}

/**
 * @brief Signal the probe thread to stop and wait for it to finish.
 *
 * Sets should_stop and joins the thread.  After this call the manager
 * is idle and can be destroyed.
 *
 * @param mgr  Probe manager (NULL-safe)
 */
void keel_probe_manager_stop(keel_probe_manager_t* mgr)
{
    if (!mgr) return;
    mgr->should_stop = true;

    if (mgr->running) {
        pthread_join(mgr->thread, NULL);
    }
}

/**
 * @brief Destroy the probe manager and free all resources.
 *
 * Stops the thread if running, destroys all per-server probe contexts
 * via ops->destroy(), and frees the manager struct.
 *
 * @param mgr  Probe manager (NULL-safe)
 */
void keel_probe_manager_destroy(keel_probe_manager_t* mgr)
{
    if (!mgr) return;
    keel_probe_manager_stop(mgr);

    /* Destroy per-server probe contexts */
    if (mgr->probe_ctxs && mgr->ops) {
        for (size_t i = 0; i < mgr->pool->count; i++) {
            if (mgr->probe_ctxs[i]) {
                mgr->ops->destroy(mgr->probe_ctxs[i]);
            }
        }
    }

    keel_free(mgr->probe_ctxs);
    keel_free(mgr->states);
    keel_free(mgr);
}

/**
 * @brief Get per-server health state (lock-free atomic read).
 *
 * Returns a pointer to the server's health state struct.  All fields
 * are atomics and can be read without locking.
 *
 * @param mgr         Probe manager
 * @param server_idx  Index into the server pool
 * @return Pointer to state, or NULL if out of range
 */
const keel_server_state_t* keel_probe_manager_get_state(
    const keel_probe_manager_t* mgr, size_t server_idx)
{
    if (!mgr || server_idx >= mgr->pool->count) return NULL;
    return &mgr->states[server_idx];
}

/**
 * @brief Update probe timing parameters at runtime.
 *
 * Non-zero values override the corresponding field in mgr->config.
 * Zero values for any parameter are silently ignored, so callers can
 * pass 0 for fields they do not want to change.
 *
 * @param mgr          Probe manager.
 * @param interval_ms  New probe interval in milliseconds (0 = keep current).
 * @param timeout_ms   New per-probe connection timeout in milliseconds (0 = keep current).
 * @param retries      New retry count before marking a server failed (0 = keep current).
 */
void keel_probe_manager_update_timing(keel_probe_manager_t* mgr,
                                       uint32_t interval_ms,
                                       uint32_t timeout_ms,
                                       uint32_t retries)
{
    if (!mgr) return;
    if (interval_ms > 0) mgr->config.interval_ms = interval_ms;
    if (timeout_ms > 0)  mgr->config.timeout_ms  = timeout_ms;
    if (retries > 0)     mgr->config.retries      = retries;
}
