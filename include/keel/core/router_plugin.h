/**
 * @file router_plugin.h
 * @brief Public API for pluggable routing policies and topology-aware router management.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This header defines the plugin interface for query routing, enabling:
 * - Custom routing logic per database or use case
 * - Metadata-aware routing (functions, views, materialized views)
 * - Integration with cluster managers (Patroni, pg_auto_failover)
 * - Per-database routing policies
 *
 * Architecture:
 * =============
 * 
 * ┌─────────────────────────────────────────────────────────────────┐
 * │                        Router Manager                          │
 * │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
 * │  │ Plugin: db1 │  │ Plugin: db2 │  │  Default    │             │
 * │  │ (custom)    │  │ (patroni)   │  │  Plugin     │             │
 * │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘             │
 * │         │                │                │                     │
 * │         └────────────────┴────────────────┘                     │
 * │                          │                                      │
 * │              ┌───────────▼────────────┐                         │
 * │              │   Metadata Cache       │                         │
 * │              │ - Write functions      │                         │
 * │              │ - View rules           │                         │
 * │              │ - Materialized views   │                         │
 * │              └────────────────────────┘                         │
 * └─────────────────────────────────────────────────────────────────┘
 *
 * Plugin Lifecycle:
 * =================
 * 1. Plugin created with keel_router_plugin_create()
 * 2. Plugin attached to router manager for specific databases
 * 3. Metadata cache populated on first query or explicit refresh
 * 4. Plugin's route() called for each query
 * 5. Plugin destroyed when router manager shuts down
 *
 * Example Custom Plugin:
 * ======================
 * @code
 * static keel_error_t my_route(keel_router_plugin_t* plugin,
 *                              const keel_router_ctx_t* ctx,
 *                              keel_route_decision_t* decision) {
 *     // Custom routing logic
 *     if (should_use_analytics_replica(ctx->qt)) {
 *         decision->server = get_analytics_replica(plugin);
 *     }
 *     return KEEL_OK;
 * }
 *
 * keel_router_plugin_ops_t my_ops = {
 *     .name = "analytics",
 *     .route = my_route,
 * };
 *
 * keel_router_plugin_t* plugin = keel_router_plugin_create(&my_ops, config);
 * keel_router_mgr_register(mgr, "analytics_db", plugin);
 * @endcode
 */

#ifndef KEEL_ROUTER_PLUGIN_H
#define KEEL_ROUTER_PLUGIN_H

#include "keel_types.h"
#include "keel_error.h"
#include "keel/core/router.h"
#include "keel/sql/query_tree.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
typedef struct keel_router_plugin keel_router_plugin_t;
typedef struct keel_router_mgr keel_router_mgr_t;
typedef struct keel_metadata_cache keel_metadata_cache_t;
typedef struct keel_metadata_conn keel_metadata_conn_t;

/* ============================================================================
 * Routing Context
 * ============================================================================ */

/**
 * @brief Classification of whether a referenced object can cause writes.
 */
typedef enum keel_object_write_type {
    KEEL_OBJ_WRITE_NONE = 0,         /**< Pure read operation */
    KEEL_OBJ_WRITE_ALWAYS,           /**< Always writes (VOLATILE function) */
    KEEL_OBJ_WRITE_POSSIBLE,         /**< May write (STABLE with side effects) */
    KEEL_OBJ_WRITE_TRIGGER,          /**< Has write trigger */
    KEEL_OBJ_WRITE_RULE,             /**< Has write rule (view) */
    KEEL_OBJ_WRITE_MATVIEW,          /**< Materialized view refresh needed */
} keel_object_write_type_t;

/**
 * @brief Minimal object metadata passed into plugin routing decisions.
 */
typedef struct keel_object_meta {
    const char*             schema;         /**< Schema name */
    const char*             name;           /**< Object name */
    char                    type;           /**< 'f'=function, 'v'=view, 'm'=matview, 'r'=table */
    keel_object_write_type_t write_type;     /**< Write behavior */
    bool                    is_security_definer; /**< Security definer function */
    bool                    returns_trigger;     /**< Function returns trigger */
} keel_object_meta_t;

/**
 * @brief Context bundle supplied to plugins for each routing decision.
 *
 * Contains all information needed to make a routing decision.
 */
typedef struct keel_router_ctx {
    /* Query information */
    const keel_qt_query_t*   qt;             /**< Parsed query tree */
    keel_str_t               sql;            /**< Original SQL text */
    
    /* Session state */
    const keel_route_session_t* session;     /**< Session state */
    const char*             database;       /**< Target database name */
    const char*             user;           /**< Authenticated user */
    
    /* Server topology */
    keel_router_t*           router;         /**< Server pool router */
    
    /* Metadata cache */
    keel_metadata_cache_t*   metadata;       /**< Database metadata cache */
    
    /* Referenced objects (populated by router manager) */
    const keel_object_meta_t* referenced_objects; /**< Objects in query */
    size_t                  referenced_count;    /**< Number of objects */
    
    /* Aggregate write analysis */
    bool                    has_write_function;   /**< Query calls write function */
    bool                    has_write_trigger;    /**< Query touches table with write trigger */
    bool                    has_write_rule;       /**< Query uses view with write rule */
    bool                    has_matview_refresh;  /**< Query needs matview sync */
    
} keel_router_ctx_t;

/* ============================================================================
 * Plugin Operations Interface
 * ============================================================================ */

/**
 * @brief Plugin initialization function
 *
 * Called when plugin is created. Can be used to initialize
 * plugin-specific resources.
 *
 * @param plugin Plugin instance
 * @param config Plugin configuration (plugin-specific)
 * @return `KEEL_OK` on success or an error code if initialization fails.
 */
typedef keel_error_t (*keel_plugin_init_fn)(
    keel_router_plugin_t* plugin,
    const void* config
);

/**
 * @brief Plugin cleanup callback invoked during plugin destruction.
 *
 * Called when plugin is destroyed.
 *
 * @param plugin Plugin instance.
 * @return
 */
typedef void (*keel_plugin_destroy_fn)(keel_router_plugin_t* plugin);

/**
 * @brief Main routing function
 *
 * Called for each query that needs routing. The plugin should
 * analyze the context and fill in the routing decision.
 *
 * @param plugin   Plugin instance
 * @param ctx      Routing context with query info, metadata, etc.
 * @param decision [out] Routing decision to fill
 * @return `KEEL_OK` on success, or an error code to indicate the manager
 *         should fall back to another plugin or routing path.
 */
typedef keel_error_t (*keel_plugin_route_fn)(
    keel_router_plugin_t* plugin,
    const keel_router_ctx_t* ctx,
    keel_route_decision_t* decision
);

/**
 * @brief Check if query can use replica
 *
 * Quick check without full routing. Used for fast-path decisions.
 *
 * @param plugin Plugin instance
 * @param ctx    Routing context
 * @return `true` if the query may safely execute on a replica, otherwise `false`.
 */
typedef bool (*keel_plugin_can_replica_fn)(
    keel_router_plugin_t* plugin,
    const keel_router_ctx_t* ctx
);

/**
 * @brief Probe server state
 *
 * Called to check if a server is primary/replica/read-only.
 * Used for topology discovery.
 *
 * @param plugin Plugin instance
 * @param server Server to probe
 * @param[out] is_primary true if server is primary
 * @param[out] is_readonly true if server is read-only
 * @return `KEEL_OK` on success or an error code if probing failed.
 */
typedef keel_error_t (*keel_plugin_probe_fn)(
    keel_router_plugin_t* plugin,
    keel_route_server_t* server,
    bool* is_primary,
    bool* is_readonly
);

/**
 * @brief Refresh cluster topology
 *
 * Called to discover/refresh cluster topology.
 * For Patroni, this would query the Patroni API.
 *
 * @param plugin Plugin instance
 * @param router Router to update with new topology
 * @return `KEEL_OK` on success or an error code if discovery or refresh failed.
 */
typedef keel_error_t (*keel_plugin_refresh_topology_fn)(
    keel_router_plugin_t* plugin,
    keel_router_t* router
);

/**
 * @brief Handle server failover
 *
 * Called when a failover is detected.
 *
 * @param plugin      Plugin instance
 * @param old_primary Name of old primary
 * @param new_primary Name of new primary.
 * @return
 */
typedef void (*keel_plugin_on_failover_fn)(
    keel_router_plugin_t* plugin,
    const char* old_primary,
    const char* new_primary
);

/**
 * @brief Virtual table implemented by concrete routing plugins.
 *
 * All fields are optional except 'name' and 'route'.
 */
typedef struct keel_router_plugin_ops {
    const char*                   name;           /**< Plugin name (required) */
    uint32_t                      version;        /**< Plugin API version */
    
    /* Lifecycle */
    keel_plugin_init_fn            init;           /**< Initialize plugin */
    keel_plugin_destroy_fn         destroy;        /**< Cleanup plugin */
    
    /* Routing */
    keel_plugin_route_fn           route;          /**< Route query (required) */
    keel_plugin_can_replica_fn     can_use_replica; /**< Quick replica check */
    
    /* Discovery */
    keel_plugin_probe_fn           probe_server;   /**< Probe server state */
    keel_plugin_refresh_topology_fn refresh_topology; /**< Refresh cluster */
    keel_plugin_on_failover_fn     on_failover;    /**< Handle failover */
    
} keel_router_plugin_ops_t;

/* Plugin API version */
#define KEEL_ROUTER_PLUGIN_API_VERSION 1

/* ============================================================================
 * Plugin Instance
 * ============================================================================ */

/**
 * @brief Mutable plugin instance registered with a router manager.
 */
struct keel_router_plugin {
    const keel_router_plugin_ops_t* ops;         /**< Operations table */
    void*                          user_data;   /**< Plugin-specific data */
    keel_router_mgr_t*              manager;     /**< Owning manager */
    
    /* Statistics */
    uint64_t                       routes_handled;
    uint64_t                       routes_delegated;
    uint64_t                       errors;
};

/**
 * @brief Create a router plugin
 *
 * @param ops    Plugin operations table
 * @param config Plugin-specific configuration
 * @return Plugin instance, or `NULL` if validation or initialization fails.
 */
keel_router_plugin_t* keel_router_plugin_create(
    const keel_router_plugin_ops_t* ops,
    const void* config
);

/**
 * @brief Destroy a router plugin and invoke its cleanup hook if present.
 * @return
 */
void keel_router_plugin_destroy(keel_router_plugin_t* plugin);

/* ============================================================================
 * Router Manager
 * ============================================================================ */

/**
 * @brief Configuration for the plugin manager and its background helpers.
 */
typedef struct keel_router_mgr_config {
    /* Metadata */
    bool                    metadata_enabled;     /**< Enable metadata caching */
    keel_duration_t          metadata_refresh;     /**< Metadata refresh interval */

    /**
     * @brief Optional connection provider for metadata refresh.
     *
     * When non-NULL, `keel_router_mgr_refresh_metadata()` will obtain a
     * temporary `keel_metadata_conn_t` by calling this function, drive the
     * refresh, and then call `release_conn_fn`.  If NULL the refresh is
     * skipped and only a log entry is emitted.
     *
     * @param database  Database name for which a connection is needed.
     * @param user_data The value stored in `get_conn_user_data`.
     * @return Heap-allocated (or pool-borrowed) connection, or NULL on failure.
     */
    keel_metadata_conn_t* (*get_conn_fn)(const char* database, void* user_data);

    /**
     * @brief Optional release function paired with `get_conn_fn`.
     *
     * Called after the refresh is complete (whether it succeeded or not).
     * May be NULL if the connection returned by `get_conn_fn` does not require
     * explicit release.
     */
    void (*release_conn_fn)(keel_metadata_conn_t* conn, void* user_data);

    /** Opaque pointer forwarded to `get_conn_fn` / `release_conn_fn`. */
    void*                   get_conn_user_data;

    /* Discovery */
    bool                    topology_discovery;   /**< Enable auto-discovery */
    keel_duration_t          topology_refresh;     /**< Topology refresh interval */
    
    /* Failover */
    bool                    auto_failover;        /**< React to failovers */
    keel_duration_t          failover_timeout;     /**< Failover detection timeout */
    
} keel_router_mgr_config_t;

/**
 * @brief Construct a default router-manager configuration.
 *
 * @return Default-initialized manager configuration.
 */
keel_router_mgr_config_t keel_router_mgr_config_default(void);

/**
 * @brief Create a router manager
 *
 * The manager coordinates multiple plugins and handles:
 * - Per-database plugin routing
 * - Metadata cache management
 * - Topology discovery
 *
 * @param config Manager configuration
 * @param router Base router for server pool
 * @return Manager instance, or `NULL` if setup fails.
 */
keel_router_mgr_t* keel_router_mgr_create(
    const keel_router_mgr_config_t* config,
    keel_router_t* router
);

/**
 * @brief Destroy the router manager, its plugins, and any owned caches.
 * @return
 */
void keel_router_mgr_destroy(keel_router_mgr_t* mgr);

/**
 * @brief Register a plugin for a database
 *
 * @param mgr      Manager instance
 * @param database Database name (NULL for default)
 * @param plugin   Plugin to register
 * @return `KEEL_OK` on success or an error code if registration fails.
 */
keel_error_t keel_router_mgr_register(
    keel_router_mgr_t* mgr,
    const char* database,
    keel_router_plugin_t* plugin
);

/**
 * @brief Set the default plugin
 *
 * The default plugin is used for databases without a specific plugin.
 *
 * @param mgr    Manager instance
 * @param plugin Default plugin.
 * @return
 */
void keel_router_mgr_set_default(
    keel_router_mgr_t* mgr,
    keel_router_plugin_t* plugin
);

/**
 * @brief Route a query through the plugin system
 *
 * This is the main entry point for routing. It:
 * 1. Looks up the plugin for the target database
 * 2. Populates the routing context with metadata
 * 3. Calls the plugin's route function
 *
 * @param mgr      Manager instance
 * @param sql      SQL query text
 * @param database Target database
 * @param session  Session state
 * @param decision [out] Routing decision
 * @return `KEEL_OK` on success or an error code if no plugin can route the query.
 */
keel_error_t keel_router_mgr_route(
    keel_router_mgr_t* mgr,
    keel_str_t sql,
    const char* database,
    const keel_route_session_t* session,
    keel_route_decision_t* decision
);

/**
 * @brief Route with pre-parsed query tree
 *
 * @param mgr      Manager instance
 * @param qt       Parsed query tree
 * @param sql      Original SQL (for fallback)
 * @param database Target database
 * @param session  Session state
 * @param decision [out] Routing decision
 * @return `KEEL_OK` on success or an error code if routing fails.
 */
keel_error_t keel_router_mgr_route_qt(
    keel_router_mgr_t* mgr,
    const keel_qt_query_t* qt,
    keel_str_t sql,
    const char* database,
    const keel_route_session_t* session,
    keel_route_decision_t* decision
);

/**
 * @brief Refresh metadata for a database
 *
 * @param mgr      Manager instance
 * @param database Database name (NULL for all)
 * @return `KEEL_OK` on success or an error code if refresh fails.
 */
keel_error_t keel_router_mgr_refresh_metadata(
    keel_router_mgr_t* mgr,
    const char* database
);

/**
 * @brief Trigger topology refresh
 *
 * @param mgr Manager instance
 * @return `KEEL_OK` on success or an error code if topology refresh fails.
 */
keel_error_t keel_router_mgr_refresh_topology(keel_router_mgr_t* mgr);

/**
 * @brief Get the metadata cache for a database
 *
 * @param mgr      Manager instance
 * @param database Database name
 * @return Metadata cache, or `NULL` if the database is unknown or metadata is disabled.
 */
keel_metadata_cache_t* keel_router_mgr_get_metadata(
    keel_router_mgr_t* mgr,
    const char* database
);

/* ============================================================================
 * Built-in Plugins
 * ============================================================================ */

/**
 * @brief Configuration for the built-in default routing plugin.
 */
typedef struct keel_plugin_default_config {
    double              primary_read_weight;  /**< Primary weight for reads */
    bool                respect_metadata;     /**< Use metadata for routing */
    bool                conservative_mode;    /**< When in doubt, use primary */
} keel_plugin_default_config_t;

/**
 * @brief Create the built-in default routing plugin.
 *
 * The default plugin implements:
 * - Weighted read/write splitting
 * - Metadata-aware routing (write functions, rules, etc.)
 * - Session pinning support
 * @param config Default-plugin configuration, or `NULL` for defaults.
 * @return Plugin instance, or `NULL` if creation fails.
 */
keel_router_plugin_t* keel_router_plugin_default_create(
    const keel_plugin_default_config_t* config
);

/**
 * @brief Configuration for the built-in Patroni-aware routing plugin.
 */
typedef struct keel_plugin_patroni_config {
    const char*         patroni_url;          /**< Patroni REST API URL */
    const char*         cluster_name;         /**< Cluster name */
    keel_duration_t      poll_interval;        /**< Polling interval */
    const char*         api_key;              /**< Optional API key */
} keel_plugin_patroni_config_t;

/**
 * @brief Create the built-in Patroni-integrated routing plugin.
 *
 * This plugin:
 * - Discovers cluster topology from Patroni API
 * - Reacts to leader elections
 * - Handles replica lag awareness
 * @param config Patroni plugin configuration.
 * @return Plugin instance, or `NULL` if creation fails.
 */
keel_router_plugin_t* keel_router_plugin_patroni_create(
    const keel_plugin_patroni_config_t* config
);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ROUTER_PLUGIN_H */
