/**
 * @file admin.h
 * @brief Public API for KEEL's administrative SQL endpoint and Prometheus exporter.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * The admin subsystem exposes KEEL's operational control plane through two
 * externally consumable surfaces:
 *
 * 1. A PostgreSQL-wire listener suitable for `psql`, scripts, and monitoring
 *    tools that prefer tabular SQL-like introspection.
 * 2. A minimal HTTP listener exposing Prometheus-compatible metrics and health
 *    endpoints.
 *
 * The implementation is intentionally isolated in a dedicated thread. That
 * keeps formatting, socket polling, and operator-facing command handling out of
 * the worker fast path while still allowing live runtime inspection.
 */
#ifndef KEEL_ADMIN_H
#define KEEL_ADMIN_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
struct keel_engine;
struct keel_stats_collector;
struct keel_cluster;
struct keel_router;
typedef struct keel_query_rules keel_query_rules_t;
typedef struct keel_throttle_rules keel_throttle_rules_t;
typedef struct keel_discovery keel_discovery_t;

/* ============================================================================
 * Admin configuration
 * ============================================================================ */

/**
 * @brief Runtime configuration for the admin subsystem.
 *
 * Fields are copied during `keel_admin_start()`, so the caller may release or
 * reuse the original structure after startup completes.
 */
typedef struct keel_admin_config {
    /* PostgreSQL-wire admin console configuration. */
    bool            admin_enabled;  /**< Enable the PostgreSQL-wire admin console listener. */
    const char     *admin_addr;     /**< Bind address for the admin console (default "127.0.0.1"). */
    uint16_t        admin_port;     /**< TCP port for the admin console (default 6433). */
    const char     *admin_users;    /**< Comma-separated list of users permitted to connect. */
    const char     *admin_password; /**< SCRAM-SHA-256 password; NULL enables trust authentication. */

    /* Prometheus/HTTP metrics exporter configuration. */
    bool            prom_enabled;   /**< Enable the Prometheus/HTTP metrics exporter. */
    const char     *prom_addr;      /**< Bind address for the Prometheus listener (default "0.0.0.0"). */
    uint16_t        prom_port;      /**< TCP port for the Prometheus listener (default 9101). */
    const char     *prom_path;      /**< HTTP path for the metrics endpoint (default "/metrics"). */
} keel_admin_config_t;

/**
 * @brief Default-initializer for `keel_admin_config_t`.
 *
 * Both the admin console and Prometheus exporter are disabled by default.
 * Override individual fields after applying this initializer.
 */
#define KEEL_ADMIN_CONFIG_DEFAULT { \
    .admin_enabled  = false,       \
    .admin_addr     = "127.0.0.1", \
    .admin_port     = 6433,        \
    .admin_users    = "admin",     \
    .admin_password = NULL,        \
    .prom_enabled   = false,       \
    .prom_addr      = "0.0.0.0",   \
    .prom_port      = 9101,        \
    .prom_path      = "/metrics",  \
}

/* ============================================================================
 * Admin server handle (opaque)
 * ============================================================================ */

/**
 * @brief Opaque handle for the admin subsystem instance.
 *
 * Obtained from `keel_admin_start()` and passed to all subsequent admin API
 * calls.  The internal structure is private to the admin implementation.
 */
typedef struct keel_admin keel_admin_t;

/**
 * @brief Create and start the admin subsystem.
 *
 * This allocates the opaque admin handle, copies the supplied configuration,
 * attempts to bind the configured listener sockets, and spawns the dedicated
 * admin thread.
 *
 * @param cfg Configuration copied into internal storage.
 * @param engine Engine handle used for runtime introspection and administrative control.
 * @return Opaque admin handle on success, or `NULL` when startup fails or the
 *         admin subsystem is effectively disabled.
 *
 * Possible failure causes:
 * - `cfg` is `NULL`.
 * - `engine` is `NULL`.
 * - Both admin surfaces are disabled.
 * - Memory allocation failure.
 * - Neither listener socket could be created.
 * - Thread creation failure.
 */
keel_admin_t *keel_admin_start(const keel_admin_config_t *cfg,
                             struct keel_engine *engine);

/**
 * @brief Stop the admin subsystem and free resources.
 *
 * Signals the admin thread to stop, waits for it to exit, closes any open
 * listeners, and releases the opaque admin handle.
 *
 * @param admin Admin handle returned by `keel_admin_start()`, or `NULL`.
 * @return
 *
 * @note Safe to call with `NULL`.
 */
void keel_admin_stop(keel_admin_t *admin);

/**
 * @brief Attach a cluster manager to the admin subsystem.
 *
 * When set, the admin console exposes cluster commands (SHOW CLUSTER,
 * ADD PEER, REMOVE PEER). Safe to call at any time; NULL disables cluster
 * commands.
 *
 * @param admin Admin handle.
 * @param cluster Cluster handle, or NULL.
 */
void keel_admin_set_cluster(keel_admin_t *admin, struct keel_cluster *cluster);

/**
 * @brief Attach a router to the admin subsystem.
 *
 * When set, the admin console exposes `SHOW SHARD RULES`,
 * `EXPLAIN SHARD PLAN FOR '<sql>'`, and
 * `SELECT * FROM shard_rules`.  Safe to call at any time after
 * `keel_admin_start()`; NULL disables shard-routing commands.
 *
 * @param admin   Admin handle.
 * @param router  Router handle, or NULL.
 */
void keel_admin_set_router(keel_admin_t *admin, struct keel_router *router);

/**
 * @brief Retrieve the router currently attached to the admin subsystem.
 *
 * @param admin Admin handle. May be NULL.
 * @return The attached router, or NULL.
 */
struct keel_router *keel_admin_get_router(keel_admin_t *admin);

/**
 * @brief Attach a declarative query rules list for SHOW QUERY RULES.
 *
 * Ownership is NOT transferred. The caller manages the lifetime.
 */
void keel_admin_set_query_rules(keel_admin_t *admin,
                                keel_query_rules_t *rules);

/**
 * @brief Attach a throttle rules set for SHOW THROTTLE RULES.
 *
 * Ownership is NOT transferred. The caller manages the lifetime.
 */
void keel_admin_set_throttle_rules(keel_admin_t *admin,
                                   keel_throttle_rules_t *throttle_rules);

/**
 * @brief Attach a discovery instance for SHOW TOPOLOGY.
 *
 * Ownership is NOT transferred. The caller manages the lifetime.
 */
void keel_admin_set_discovery(keel_admin_t *admin,
                              keel_discovery_t *discovery);

/**
 * @brief Return the TCP port the admin console is actually listening on.
 *
 * Useful when `admin_port = 0` was specified and the OS assigned an
 * ephemeral port.  Returns 0 if the admin listener is not open.
 */
uint16_t keel_admin_get_port(const keel_admin_t *admin);

/**
 * @brief Return the TCP port the Prometheus exporter is actually listening on.
 *
 * Useful when `prom_port = 0` was specified and the OS assigned an
 * ephemeral port.  Returns 0 if the Prometheus listener is not open.
 */
uint16_t keel_admin_get_prom_port(const keel_admin_t *admin);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ADMIN_H */
