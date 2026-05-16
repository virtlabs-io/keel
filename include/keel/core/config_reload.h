/**
 * @file config_reload.h
 * @brief Public API for live worker-group configuration reload during SIGHUP handling.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Extends the existing SIGHUP handler to apply all safe-to-change parameters
 * without restart. Parameters that require a restart (listen port, worker count,
 * protocol) are flagged clearly in the log.
 *
 * Safe-to-reload parameters:
 *   - Pool sizing: min_pool_size, max_pool_size, pool_max_waiting
 *   - Timeouts: idle_timeout_ms, connect_timeout_ms, pool_prune_interval_ms,
 *               pool_refill_interval_ms, pool_refill_backoff_ms
 *   - Routing: server weights
 *   - Probes: probe_interval, probe_timeout, probe_retries
 *   - Rebalancing: rebalance_enabled, rebalance_interval_ms,
 *                  rebalance_threshold_pct, rebalance_max_per_tick
 *   - TLS: certificates (existing)
 *   - Logging: log_level (existing)
 *
 * Restart-required parameters:
 *   - bind_addr, bind_port (listen socket already bound)
 *   - num_workers (thread pool already sized)
 *   - protocol (wire protocol selected at startup)
 *   - prepared_statement mode (cached PS state tied to mode)
 *   - mode / runtime_mode (tier gates applied at session init)
 *   - transaction_tracking (affects XID probe setup)
 */

#ifndef KEEL_CONFIG_RELOAD_H
#define KEEL_CONFIG_RELOAD_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
struct keel_config;
struct worker_group;
struct keel_engine;

/* ============================================================================
 * Reload Result
 * ============================================================================ */

/**
 * @brief Aggregated result counters from a single configuration reload pass.
 *
 * Populated by `keel_config_reload_worker_group()` and
 * `keel_config_reload_shard_rules()`.  Fields accumulate across multiple
 * reload calls so the caller can report totals at the end of a SIGHUP pass.
 */
typedef struct keel_reload_result {
    int     applied;        /**< Number of parameters successfully applied */
    int     skipped;        /**< Restart-required parameters that changed but were not applied */
    int     unchanged;      /**< Parameters that didn't change */
    int     errors;         /**< Parameters that failed to apply */
} keel_reload_result_t;

/* ============================================================================
 * Reload API
 * ============================================================================ */

/**
 * @brief Reload a single worker group's configuration from a parsed INI file.
 *
 * Re-reads the worker group section, diffs against the current runtime state,
 * and applies safe-to-change parameters to workers, pools, probes, and routing.
 * Restart-required changes are logged but not applied.
 *
 * This function is called from the main thread during SIGHUP processing.
 * Worker threads are NOT stopped — changes are applied in a lock-free manner
 * via atomic field updates that workers pick up on their next timer tick.
 *
 * @param config    Freshly parsed INI config
 * @param wg        Runtime worker group state
 * @param section   INI section name (e.g., "worker_group.myapp")
 * @param result    Output: reload statistics
 * @return
 */
void keel_config_reload_worker_group(
    struct keel_config* config,
    struct worker_group* wg,
    const char* section,
    keel_reload_result_t* result);

/* ============================================================================
 * Feature 13: Shard rule hot-reload
 * ============================================================================ */

struct keel_router;

/**
 * @brief Reload shard rules from a freshly parsed INI config into a live router.
 *
 * Reads every `[worker_group.<group_name>.shard_rule.<table>]` section from
 * `config`, calls `keel_router_add_shard_rule()` /
 * `keel_router_add_shard_rule_range()` for each, and removes rules that are
 * no longer present in the config.
 *
 * Shard rules are scoped to a specific worker group so that multiple groups
 * in the same keel instance can have independent, non-conflicting shard
 * topologies.
 *
 * Safe to call from the SIGHUP handler while the router is in active use:
 * individual rule updates are synchronous but brief, and the router never
 * holds a long-term lock on rules during dispatch.
 *
 * Rules with a changed `shard_count` trigger a `KEEL_LOG_WARN` (via the
 * change-detection logic added in Feature 10).
 *
 * @param config      Freshly parsed INI config.
 * @param group_name  Worker group name (e.g. "myapp").  Must not be NULL.
 * @param router      Live router to update.
 * @param result      Incremented for each rule applied/skipped/unchanged/errored.
 */
void keel_config_reload_shard_rules(
    const struct keel_config*  config,
    const char*                group_name,
    struct keel_router*        router,
    keel_reload_result_t*      result);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_CONFIG_RELOAD_H */
