/**
 * @file router_discovery.h
 * @brief Public API for topology discovery, role probing, and failover observation.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This module provides mechanisms to:
 * - Probe PostgreSQL servers to determine their role (primary/replica)
 * - Check if servers are read-only
 * - Integrate with cluster managers (Patroni, pg_auto_failover)
 * - Monitor replication lag
 *
 * Probe Strategy:
 * ===============
 * 
 * 1. **Direct SQL Probing**:
 *    - `pg_is_in_recovery()` - Returns true for replicas
 *    - `default_transaction_read_only` - Check read-only mode
 *    - `pg_last_wal_receive_lsn()` - Check replication position
 *
 * 2. **Patroni Integration**:
 *    - REST API at /cluster, /leader, /replica endpoints
 *    - Provides cluster-wide view of topology
 *    - Automatic failover detection
 *
 * 3. **pg_auto_failover Integration**:
 *    - Query pg_auto_failover.node_state table
 *    - Monitor state transitions
 *
 * Usage:
 * ======
 * @code
 * // Create discovery instance
 * keel_discovery_t* disc = keel_discovery_create(&config);
 *
 * // Probe a server
 * keel_server_info_t info;
 * keel_discovery_probe(disc, "db.host:5432", &info);
 * 
 * if (info.is_primary) {
 *     // Primary server
 * } else if (info.lag_bytes < MAX_LAG) {
 *     // Usable replica
 * }
 *
 * // Start background discovery
 * keel_discovery_start(disc, router);
 * @endcode
 */

#ifndef KEEL_ROUTER_DISCOVERY_H
#define KEEL_ROUTER_DISCOVERY_H

#include "keel_types.h"
#include "keel_error.h"
#include "keel/core/router.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Types
 * ============================================================================ */

/** Opaque discovery instance handle. */
typedef struct keel_discovery keel_discovery_t;

/**
 * @brief Mechanisms available for discovering topology and node roles.
 */
typedef enum keel_discovery_method {
    KEEL_DISCOVER_SQL = 0,       /**< Direct SQL probing */
    KEEL_DISCOVER_PATRONI,       /**< Patroni REST API */
    KEEL_DISCOVER_PGAUTOFAILOVER, /**< pg_auto_failover */
    KEEL_DISCOVER_CONSUL,        /**< Consul service discovery */
    KEEL_DISCOVER_ETCD,          /**< etcd-based discovery */
} keel_discovery_method_t;

/**
 * @brief Role, health, and replication details collected for one server.
 */
typedef struct keel_server_info {
    /* Identity */
    char                    name[64];       /**< Server name/identifier */
    char                    host[256];      /**< Hostname/IP */
    uint16_t                port;           /**< Port number */
    
    /* Role */
    bool                    is_primary;     /**< True if primary/leader */
    bool                    is_readonly;    /**< True if read-only mode */
    bool                    is_standby;     /**< True if standby */
    bool                    accepting_writes; /**< Can accept writes */
    
    /* Replication state */
    uint64_t                wal_lsn;        /**< Current WAL position */
    uint64_t                replay_lsn;     /**< Replay position (replica) */
    uint64_t                lag_bytes;      /**< Replication lag in bytes */
    double                  lag_seconds;    /**< Replication lag in seconds */
    
    /* Health */
    keel_server_health_t     health;         /**< Overall health */
    keel_time_t              probe_time;     /**< When probed */
    keel_duration_t          response_time;  /**< Probe response time */
    int                     probe_failures; /**< Consecutive failures */
    
    /* PostgreSQL info */
    int                     pg_major;       /**< PostgreSQL major version */
    int                     pg_minor;       /**< PostgreSQL minor version */
    
    /* Cluster info (if using cluster manager) */
    char                    cluster_name[64]; /**< Cluster name */
    int                     timeline;       /**< WAL timeline */
    
} keel_server_info_t;

/**
 * @brief Parameters forwarded to a probe_fn call.
 *
 * Captures the per-probe tuning that lives in keel_discovery_config_t so that
 * probe implementations do not need to reach back into the discovery struct.
 */
typedef struct keel_discovery_probe_params {
    uint32_t timeout_ms;       /**< Connect + query timeout */
    uint64_t max_lag_bytes;    /**< Replication lag threshold (bytes) → DEGRADED */
    double   max_lag_seconds;  /**< Replication lag threshold (seconds) → DEGRADED */
} keel_discovery_probe_params_t;

/**
 * @brief Backend-specific server probe callback.
 *
 * Implementations live in the probe layer (e.g. keel_pg_discovery_probe for
 * PostgreSQL). The core discovery module calls this function and remains
 * completely agnostic to any wire protocol.
 *
 * Contract:
 * - Must always return KEEL_OK; the health field in @p info reflects the
 *   actual outcome (KEEL_HEALTH_UP, KEEL_HEALTH_DOWN, KEEL_HEALTH_DEGRADED).
 * - @p info is zero-filled by the caller before the probe is invoked.
 */
typedef keel_error_t (*keel_discovery_probe_fn)(
    const char*                          host,
    uint16_t                             port,
    const char*                          user,
    const char*                          pass,
    const char*                          dbname,
    const keel_discovery_probe_params_t* params,
    keel_server_info_t*                  info
);

/**
 * @brief Configuration governing direct probes and cluster-manager integration.
 */
typedef struct keel_discovery_config {
    /* Method */
    keel_discovery_method_t  method;         /**< Discovery method */

    /* Probe callback — MUST be set by the caller; NULL → health stays UNKNOWN */
    keel_discovery_probe_fn  probe_fn;       /**< Backend-specific probe implementation */

    /**
     * @brief In-pool connection probe callback.
     *
     * When set, keel_discovery_probe_conn() delegates to this function instead
     * of returning KEEL_HEALTH_UNKNOWN.  The callback receives the opaque
     * connection handle supplied by the caller and fills `info`.
     *
     * @param conn    Opaque backend connection handle (caller-defined).
     * @param params  Probe tuning parameters (timeout, lag thresholds).
     * @param info    [out] Probe result.
     * @return KEEL_OK on success.
     */
    keel_error_t (*conn_probe_fn)(void* conn,
                                   const keel_discovery_probe_params_t* params,
                                   keel_server_info_t* info);

    /* Probe timing */
    keel_duration_t          probe_timeout;  /**< Probe timeout */
    int                      probe_retries;  /**< Retries before marking down */
    keel_duration_t          probe_interval; /**< Time between probes */

    /* Lag thresholds */
    uint64_t                 max_lag_bytes;   /**< Max acceptable lag (bytes) */
    double                   max_lag_seconds; /**< Max acceptable lag (seconds) */

    /* Patroni settings */
    const char*              patroni_url;    /**< Patroni REST API base URL */
    const char*              cluster_name;   /**< Cluster to discover */

    /* Role-flap dampening */
    uint32_t                 flap_dampening_window_s;   /**< Window for counting role flips (0=disabled) */
    uint32_t                 flap_dampening_threshold;  /**< Max flips in window before dampening kicks in */

    /* pg_auto_failover settings */
    const char*              monitor_connstr; /**< Monitor connection string */
    const char*              formation;       /**< Formation name */

    /* Consul/etcd settings */
    const char*              service_name;   /**< Service name in registry */
    const char*              consul_url;     /**< Consul HTTP API URL */
    const char*              etcd_endpoints; /**< etcd endpoints */

} keel_discovery_config_t;

/**
 * @brief Snapshot of the cluster topology produced by a discovery pass.
 */
typedef struct keel_cluster_topology {
    keel_server_info_t*      servers;        /**< Array of servers */
    size_t                  server_count;   /**< Number of servers */
    size_t                  primary_index;  /**< Index of primary (-1 if none) */
    keel_time_t              discovered_at;  /**< Discovery timestamp */
    const char*             cluster_name;   /**< Cluster name */
} keel_cluster_topology_t;

/**
 * @brief Description of a detected primary switch.
 */
typedef struct keel_failover_event {
    const char*             old_primary;    /**< Previous primary name */
    const char*             new_primary;    /**< New primary name */
    keel_time_t              detected_at;    /**< When detected */
    const char*             reason;         /**< Failover reason (if known) */
    int                     old_timeline;   /**< WAL timeline before promotion */
    int                     new_timeline;   /**< WAL timeline after promotion */
} keel_failover_event_t;

/**
 * @brief Callback signature invoked when discovery detects failover.
 */
typedef void (*keel_failover_callback_fn)(
    void* user_data,
    const keel_failover_event_t* event
);

/**
 * @brief Structured record for a single server role change.
 *
 * Emitted via @c keel_role_change_callback_fn whenever a server's role
 * transitions between primary and replica (or to unknown).  The @c flap_count
 * field accumulates over the lifetime of the discovery instance so a steadily
 * growing value indicates persistent instability.
 */
typedef struct keel_role_change_event {
    char                    server_name[64]; /**< Server identifier */
    keel_server_role_t       old_role;        /**< Previous role */
    keel_server_role_t       new_role;        /**< New role */
    int                     old_timeline;    /**< WAL timeline before change */
    int                     new_timeline;    /**< WAL timeline after change */
    keel_time_t              changed_at;      /**< Monotonic timestamp of detection */
    uint32_t                flap_count;      /**< Total role flips seen for this server */
    bool                    dampened;        /**< True: change suppressed by dampening */
} keel_role_change_event_t;

/**
 * @brief Callback invoked on every (undampened) server role change.
 */
typedef void (*keel_role_change_callback_fn)(
    void* user_data,
    const keel_role_change_event_t* event
);

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @brief Construct a discovery configuration populated with defaults.
 *
 * @return Default discovery configuration.
 */
keel_discovery_config_t keel_discovery_config_default(void);

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Create a discovery instance
 *
 * @param config Discovery configuration
 * @return Discovery instance, or `NULL` if allocation or setup fails.
 */
keel_discovery_t* keel_discovery_create(const keel_discovery_config_t* config);

/**
 * @brief Destroy a discovery instance and release any background resources.
 * @return
 */
void keel_discovery_destroy(keel_discovery_t* disc);

/* ============================================================================
 * Manual Probing
 * ============================================================================ */

/**
 * @brief Probe a single server
 *
 * Connects to the server and queries its state.
 *
 * @param disc   Discovery instance
 * @param host   Server hostname
 * @param port   Server port
 * @param user   Username for connection
 * @param pass   Password for connection
 * @param dbname Database to connect to
 * @param[out] info Server information
 * @return `KEEL_OK` on success or an error code if the server could not be probed.
 */
keel_error_t keel_discovery_probe(
    keel_discovery_t* disc,
    const char* host,
    uint16_t port,
    const char* user,
    const char* pass,
    const char* dbname,
    keel_server_info_t* info
);

/**
 * @brief Probe server using existing connection
 *
 * Uses a pre-established connection to probe server state.
 * This is more efficient when connection is already available.
 *
 * @param disc Discovery instance
 * @param conn Connection interface
 * @param[out] info Server information
 * @return `KEEL_OK` on success or an error code if the connection could not be queried.
 */
keel_error_t keel_discovery_probe_conn(
    keel_discovery_t* disc,
    void* conn,  /* Connection-specific type */
    keel_server_info_t* info
);

/* ============================================================================
 * Cluster Discovery
 * ============================================================================ */

/**
 * @brief Discover cluster topology
 *
 * For SQL discovery, probes all known servers.
 * For Patroni/etc, queries the cluster manager.
 *
 * @param disc     Discovery instance
 * @param[out] topology Cluster topology (caller must free with keel_topology_free)
 * @return `KEEL_OK` on success or an error code if topology discovery fails.
 */
keel_error_t keel_discovery_refresh(
    keel_discovery_t* disc,
    keel_cluster_topology_t** topology
);

/**
 * @brief Free a topology snapshot returned by the discovery subsystem.
 *
 * @param topology Topology snapshot to release.
 * @return
 */
void keel_topology_free(keel_cluster_topology_t* topology);

/**
 * @brief Apply topology to router
 *
 * Updates router's server list to match discovered topology.
 *
 * @param disc     Discovery instance
 * @param router   Router to update
 * @param topology Topology to apply
 * @return `KEEL_OK` on success or an error code if the topology cannot be applied.
 */
keel_error_t keel_discovery_apply(
    keel_discovery_t* disc,
    keel_router_t* router,
    const keel_cluster_topology_t* topology
);

/* ============================================================================
 * Background Discovery
 * ============================================================================ */

/**
 * @brief Start background discovery
 *
 * Spawns a thread that periodically discovers cluster topology
 * and updates the router.
 *
 * @param disc   Discovery instance
 * @param router Router to keep updated
 * @return `KEEL_OK` on success or an error code if the background worker cannot be started.
 */
keel_error_t keel_discovery_start(
    keel_discovery_t* disc,
    keel_router_t* router
);

/**
 * @brief Stop the background discovery loop if it is running.
 *
 * @param disc Discovery instance.
 * @return
 */
void keel_discovery_stop(keel_discovery_t* disc);

/**
 * @brief Check whether background discovery is currently active.
 *
 * @param disc Discovery instance.
 * @return `true` if the background discovery thread is running.
 */
bool keel_discovery_is_running(const keel_discovery_t* disc);

/* ============================================================================
 * Failover Detection
 * ============================================================================ */

/**
 * @brief Register failover callback
 *
 * Callback is invoked when a failover is detected.
 *
 * @param disc      Discovery instance
 * @param callback  Callback function
 * @param user_data User data for callback.
 * @return
 */
void keel_discovery_on_failover(
    keel_discovery_t* disc,
    keel_failover_callback_fn callback,
    void* user_data
);

/**
 * @brief Register a role-change callback.
 *
 * The callback fires on every role transition observed during topology
 * refresh.  Dampened transitions (within the flap window) still fire but
 * have @c event->dampened set to @c true so the caller can choose to ignore
 * them or count them separately.
 *
 * @param disc      Discovery instance.
 * @param callback  Callback function, or @c NULL to deregister.
 * @param user_data Opaque value forwarded to the callback.
 * @return
 */
void keel_discovery_on_role_change(
    keel_discovery_t*            disc,
    keel_role_change_callback_fn callback,
    void*                        user_data
);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ROUTER_DISCOVERY_H */
