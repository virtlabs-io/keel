/**
 * @file router_plugin.c
 * @brief Router plugin registry, manager, and built-in plugin implementations.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file layers pluggable routing policy on top of the base weighted router.
 * It maintains database-to-plugin mappings, optional metadata caches per
 * database, and built-in default and Patroni-aware plugins.
 *
 * Concurrency note:
 * - manager state is protected by a read/write lock
 * - routing takes the read lock while selecting the plugin and metadata cache
 * - registration/default-plugin changes take the write lock
 */

#include "keel/core/router_plugin.h"
#include "keel/core/router_metadata.h"
#include "keel/core/router.h"
#include "keel/core/router_discovery.h"
#include "keel/sql/sql.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"
#include "keel/util/util.h"

#include <string.h>
#include <stdlib.h>
#include <pthread.h>

/* Forward declaration: patroni_discover() is defined in router_discovery.c and
 * intentionally not in the public header — it is package-internal to src/core/. */
keel_error_t patroni_discover(const char* base_url, keel_cluster_topology_t* topo);

/* ============================================================================
 * Constants
 * ============================================================================ */

#define MAX_DATABASE_PLUGINS 64
#define DEFAULT_METADATA_REFRESH_MS (5 * 60 * 1000)  /* 5 minutes */
#define DEFAULT_TOPOLOGY_REFRESH_MS (30 * 1000)       /* 30 seconds */

/* ============================================================================
 * Plugin Instance
 * ============================================================================ */

/**
 * @brief Create and initialize a router plugin instance.
 *
 * @param ops Plugin operations table.
 * @param config Optional plugin-specific configuration blob.
 * @return Plugin instance on success, or `NULL` on validation/init/allocation failure.
 */
keel_router_plugin_t* keel_router_plugin_create(
    const keel_router_plugin_ops_t* ops,
    const void* config
) {
    if (!ops || !ops->name || !ops->route) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, "Invalid plugin ops: name and route required");
        return NULL;
    }
    
    keel_router_plugin_t* plugin = keel_calloc(1, sizeof(*plugin));
    if (!plugin) {
        return NULL;
    }
    
    plugin->ops = ops;
    
    /* Call init if provided */
    if (ops->init) {
        keel_error_t err = ops->init(plugin, config);
        if (err != KEEL_OK) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, "Plugin %s init failed: %d", 
                          ops->name, err);
            keel_free(plugin);
            return NULL;
        }
    }
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, "Created router plugin: %s (v%u)",
                 ops->name, ops->version);
    
    return plugin;
}

/**
 * @brief Destroy a plugin instance.
 *
 * @param plugin Plugin handle, or `NULL`.
 * @return
 */
void keel_router_plugin_destroy(keel_router_plugin_t* plugin) {
    if (!plugin) return;
    
    if (plugin->ops && plugin->ops->destroy) {
        plugin->ops->destroy(plugin);
    }
    
    keel_free(plugin);
}

/* ============================================================================
 * Router Manager
 * ============================================================================ */

/** Database to plugin mapping */
typedef struct {
    char*                   database;       /**< Database name */
    keel_router_plugin_t*    plugin;         /**< Associated plugin */
    keel_metadata_cache_t*   metadata;       /**< Metadata cache */
} db_plugin_entry_t;

/** Router manager structure */
struct keel_router_mgr {
    keel_router_mgr_config_t config;
    keel_router_t*           router;         /**< Base server pool router */
    
    /* Plugin registry */
    db_plugin_entry_t       plugins[MAX_DATABASE_PLUGINS];
    size_t                  plugin_count;
    keel_router_plugin_t*    default_plugin; /**< Default plugin */
    
    /* Thread safety */
    pthread_rwlock_t        lock;
    
    /* Query parsing arena */
    keel_arena_t*            arena;
    
    /* Statistics */
    uint64_t                total_routes;
    uint64_t                plugin_routes;
    uint64_t                default_routes;
};

/* ============================================================================
 * Manager Configuration
 * ============================================================================ */

/**
 * @brief Return the default router-manager configuration.
 *
 * @return Default manager configuration.
 */
keel_router_mgr_config_t keel_router_mgr_config_default(void) {
    return (keel_router_mgr_config_t){
        .metadata_enabled = true,
        .metadata_refresh = DEFAULT_METADATA_REFRESH_MS * 1000000ULL,
        .topology_discovery = true,
        .topology_refresh = DEFAULT_TOPOLOGY_REFRESH_MS * 1000000ULL,
        .auto_failover = true,
        .failover_timeout = 10 * 1000000000ULL,  /* 10 seconds */
    };
}

/* ============================================================================
 * Manager Lifecycle
 * ============================================================================ */

/**
 * @brief Create a router manager that owns plugin mappings over a base router.
 *
 * @param config Optional manager configuration.
 * @param router Base router used for fallback and final server selection.
 * @return Manager instance on success, or `NULL` on validation/allocation failure.
 */
keel_router_mgr_t* keel_router_mgr_create(
    const keel_router_mgr_config_t* config,
    keel_router_t* router
) {
    if (!router) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, "Router required for manager");
        return NULL;
    }
    
    keel_router_mgr_t* mgr = keel_calloc(1, sizeof(*mgr));
    if (!mgr) {
        return NULL;
    }
    
    if (config) {
        mgr->config = *config;
    } else {
        mgr->config = keel_router_mgr_config_default();
    }
    
    mgr->router = router;
    
    if (pthread_rwlock_init(&mgr->lock, NULL) != 0) {
        keel_free(mgr);
        return NULL;
    }
    
    mgr->arena = keel_arena_create(16 * 1024);
    if (!mgr->arena) {
        pthread_rwlock_destroy(&mgr->lock);
        keel_free(mgr);
        return NULL;
    }
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, 
                 "Created router manager (metadata=%s, topology=%s)",
                 mgr->config.metadata_enabled ? "on" : "off",
                 mgr->config.topology_discovery ? "on" : "off");
    
    return mgr;
}

/**
 * @brief Destroy a router manager and its owned metadata caches.
 *
 * @param mgr Manager handle, or `NULL`.
 * @return
 */
void keel_router_mgr_destroy(keel_router_mgr_t* mgr) {
    if (!mgr) return;
    
    /* Destroy all registered plugins */
    for (size_t i = 0; i < mgr->plugin_count; i++) {
        if (mgr->plugins[i].metadata) {
            keel_metadata_cache_destroy(mgr->plugins[i].metadata);
        }
        if (mgr->plugins[i].database) {
            keel_free(mgr->plugins[i].database);
        }
        /* Don't destroy plugins - they're owned by caller */
    }
    
    keel_arena_destroy(mgr->arena);
    pthread_rwlock_destroy(&mgr->lock);
    keel_free(mgr);
}

/* ============================================================================
 * Plugin Registration
 * ============================================================================ */

/**
 * @brief Register a plugin for a specific database or as a generic entry.
 *
 * @param mgr Manager handle.
 * @param database Database name, or `NULL` for a generic entry.
 * @param plugin Plugin instance.
 * @return `KEEL_OK` on success, or an error for invalid input, duplicates,
 *         overflow, or allocation failure.
 */
keel_error_t keel_router_mgr_register(
    keel_router_mgr_t* mgr,
    const char* database,
    keel_router_plugin_t* plugin
) {
    if (!mgr || !plugin) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    pthread_rwlock_wrlock(&mgr->lock);
    
    if (mgr->plugin_count >= MAX_DATABASE_PLUGINS) {
        pthread_rwlock_unlock(&mgr->lock);
        KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, "Maximum plugin count reached");
        return KEEL_ERR_OVERFLOW;
    }
    
    /* Check for duplicate */
    if (database) {
        for (size_t i = 0; i < mgr->plugin_count; i++) {
            if (mgr->plugins[i].database && 
                strcmp(mgr->plugins[i].database, database) == 0) {
                pthread_rwlock_unlock(&mgr->lock);
                KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, 
                              "Plugin already registered for database: %s", database);
                return KEEL_ERR_ALREADY_EXISTS;
            }
        }
    }
    
    db_plugin_entry_t* entry = &mgr->plugins[mgr->plugin_count++];
    entry->database = database ? keel_strdup(database) : NULL;
    entry->plugin = plugin;
    entry->metadata = NULL;
    
    /* Create metadata cache if enabled */
    if (mgr->config.metadata_enabled && database) {
        entry->metadata = keel_metadata_cache_create(database, NULL);
    }
    
    plugin->manager = mgr;
    
    pthread_rwlock_unlock(&mgr->lock);
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, 
                 "Registered plugin '%s' for database '%s'",
                 plugin->ops->name, database ? database : "(default)");
    
    return KEEL_OK;
}

/**
 * @brief Set the default plugin used when no database-specific entry matches.
 *
 * @param mgr Manager handle.
 * @param plugin Default plugin, or `NULL`.
 * @return
 */
void keel_router_mgr_set_default(
    keel_router_mgr_t* mgr,
    keel_router_plugin_t* plugin
) {
    if (!mgr) return;
    
    pthread_rwlock_wrlock(&mgr->lock);
    mgr->default_plugin = plugin;
    if (plugin) {
        plugin->manager = mgr;
    }
    pthread_rwlock_unlock(&mgr->lock);
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, "Set default plugin: %s",
                 plugin ? plugin->ops->name : "(none)");
}

/* ============================================================================
 * Plugin Lookup (internal, must hold lock)
 * ============================================================================ */

/**
 * @brief Find the plugin entry for a database.
 *
 * @note Caller must already hold the manager lock.
 */
static db_plugin_entry_t* find_plugin_entry(
    keel_router_mgr_t* mgr,
    const char* database
) {
    if (!database) {
        return NULL;
    }
    
    for (size_t i = 0; i < mgr->plugin_count; i++) {
        if (mgr->plugins[i].database &&
            strcmp(mgr->plugins[i].database, database) == 0) {
            return &mgr->plugins[i];
        }
    }
    
    return NULL;
}

/* ============================================================================
 * Routing
 * ============================================================================ */

/**
 * @brief Build the plugin-facing routing context from manager, query, and session state.
 *
 * @param mgr Manager handle.
 * @param[out] ctx Routing context to initialize.
 * @param qt Parsed query tree.
 * @param sql Original SQL text.
 * @param database Logical database name.
 * @param session Session routing context.
 * @param metadata Optional metadata cache.
 * @return
 */
static void build_routing_context(
    keel_router_mgr_t* mgr,
    keel_router_ctx_t* ctx,
    const keel_qt_query_t* qt,
    keel_str_t sql,
    const char* database,
    const keel_route_session_t* session,
    keel_metadata_cache_t* metadata
) {
    memset(ctx, 0, sizeof(*ctx));
    
    ctx->qt = qt;
    ctx->sql = sql;
    ctx->session = session;
    ctx->database = database;
    ctx->router = mgr->router;
    ctx->metadata = metadata;
    
    /* Analyze query for write objects if metadata available */
    if (metadata && qt) {
        keel_metadata_analyze_query(
            metadata, qt,
            &ctx->has_write_function,
            &ctx->has_write_trigger,
            &ctx->has_write_rule,
            NULL  /* needs_primary computed from these */
        );
    }
}

/**
 * @brief Parse raw SQL and route it through the plugin manager.
 *
 * @return Manager/router error code.
 */
keel_error_t keel_router_mgr_route(
    keel_router_mgr_t* mgr,
    keel_str_t sql,
    const char* database,
    const keel_route_session_t* session,
    keel_route_decision_t* decision
) {
    if (!mgr || !decision) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    /* Parse query */
    keel_arena_reset(mgr->arena);
    keel_qt_query_t* qt = keel_sql_analyze_full(sql, mgr->arena);
    
    return keel_router_mgr_route_qt(mgr, qt, sql, database, session, decision);
}

/**
 * @brief Route an already parsed query tree through the plugin manager.
 *
 * @return `KEEL_OK` on success, or a routing error.
 *
 * Behavior:
 * - select the plugin bound to the target database, else use the default plugin
 * - build metadata-enriched routing context
 * - call the plugin route hook
 * - fall back to the base router if the plugin errors
 */
keel_error_t keel_router_mgr_route_qt(
    keel_router_mgr_t* mgr,
    const keel_qt_query_t* qt,
    keel_str_t sql,
    const char* database,
    const keel_route_session_t* session,
    keel_route_decision_t* decision
) {
    if (!mgr || !decision) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    memset(decision, 0, sizeof(*decision));
    mgr->total_routes++;
    
    pthread_rwlock_rdlock(&mgr->lock);
    
    /* Find plugin for database */
    db_plugin_entry_t* entry = find_plugin_entry(mgr, database);
    keel_router_plugin_t* plugin = entry ? entry->plugin : mgr->default_plugin;
    keel_metadata_cache_t* metadata = entry ? entry->metadata : NULL;
    
    if (!plugin) {
        /* No plugin - use base router directly */
        pthread_rwlock_unlock(&mgr->lock);
        
        return keel_router_route(mgr->router, qt, session, decision);
    }
    
    /* Build context */
    keel_router_ctx_t ctx;
    build_routing_context(mgr, &ctx, qt, sql, database, session, metadata);
    
    /* Call plugin */
    keel_error_t err = plugin->ops->route(plugin, &ctx, decision);
    
    if (err == KEEL_OK) {
        plugin->routes_handled++;
        if (entry) {
            mgr->plugin_routes++;
        } else {
            mgr->default_routes++;
        }
    } else {
        plugin->errors++;
        
        /* Fallback to base router on error */
        pthread_rwlock_unlock(&mgr->lock);
        return keel_router_route(mgr->router, qt, session, decision);
    }
    
    pthread_rwlock_unlock(&mgr->lock);
    return err;
}

/**
 * @brief Request a metadata refresh for one database or all databases.
 *
 * @note Actual refresh still requires a live metadata connection implementation.
 */
keel_error_t keel_router_mgr_refresh_metadata(
    keel_router_mgr_t* mgr,
    const char* database
) {
    if (!mgr) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    pthread_rwlock_rdlock(&mgr->lock);
    
    if (database) {
        /* Refresh specific database */
        db_plugin_entry_t* entry = find_plugin_entry(mgr, database);
        if (!entry || !entry->metadata) {
            pthread_rwlock_unlock(&mgr->lock);
            return KEEL_ERR_NOT_FOUND;
        }

        if (mgr->config.get_conn_fn) {
            keel_metadata_conn_t* conn =
                mgr->config.get_conn_fn(database, mgr->config.get_conn_user_data);
            if (conn) {
                keel_error_t re = keel_metadata_cache_refresh(entry->metadata, conn);
                if (re != KEEL_OK) {
                    KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                        "Metadata refresh failed for '%s': %d", database, re);
                }
                if (mgr->config.release_conn_fn) {
                    mgr->config.release_conn_fn(conn,
                                                mgr->config.get_conn_user_data);
                }
            } else {
                KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                    "Metadata refresh: get_conn_fn returned NULL for '%s'", database);
            }
        } else {
            KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                "Metadata refresh requested for '%s' (no connection provider configured)",
                database);
        }
    } else {
        /* Refresh all */
        for (size_t i = 0; i < mgr->plugin_count; i++) {
            if (!mgr->plugins[i].metadata) {
                continue;
            }
            const char* db = mgr->plugins[i].database;
            if (mgr->config.get_conn_fn) {
                keel_metadata_conn_t* conn =
                    mgr->config.get_conn_fn(db, mgr->config.get_conn_user_data);
                if (conn) {
                    keel_error_t re =
                        keel_metadata_cache_refresh(mgr->plugins[i].metadata, conn);
                    if (re != KEEL_OK) {
                        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                            "Metadata refresh failed for '%s': %d", db, re);
                    }
                    if (mgr->config.release_conn_fn) {
                        mgr->config.release_conn_fn(conn,
                                                    mgr->config.get_conn_user_data);
                    }
                } else {
                    KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                        "Metadata refresh: get_conn_fn returned NULL for '%s'", db);
                }
            } else {
                KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                    "Metadata refresh requested for '%s' "
                    "(no connection provider configured)", db);
            }
        }
    }
    
    pthread_rwlock_unlock(&mgr->lock);
    return KEEL_OK;
}

/**
 * @brief Ask all plugins that support topology refresh to update router state.
 */
keel_error_t keel_router_mgr_refresh_topology(keel_router_mgr_t* mgr) {
    if (!mgr) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    pthread_rwlock_rdlock(&mgr->lock);
    
    /* Call refresh_topology on all plugins that support it */
    for (size_t i = 0; i < mgr->plugin_count; i++) {
        keel_router_plugin_t* plugin = mgr->plugins[i].plugin;
        if (plugin && plugin->ops->refresh_topology) {
            keel_error_t err = plugin->ops->refresh_topology(plugin, mgr->router);
            if (err != KEEL_OK) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                             "Topology refresh failed for plugin '%s': %d",
                             plugin->ops->name, err);
            }
        }
    }
    
    if (mgr->default_plugin && mgr->default_plugin->ops->refresh_topology) {
        mgr->default_plugin->ops->refresh_topology(mgr->default_plugin, mgr->router);
    }
    
    pthread_rwlock_unlock(&mgr->lock);
    return KEEL_OK;
}

/**
 * @brief Get the metadata cache bound to a database.
 *
 * @return Metadata cache, or `NULL` if none is configured.
 */
keel_metadata_cache_t* keel_router_mgr_get_metadata(
    keel_router_mgr_t* mgr,
    const char* database
) {
    if (!mgr || !database) {
        return NULL;
    }
    
    pthread_rwlock_rdlock(&mgr->lock);
    
    db_plugin_entry_t* entry = find_plugin_entry(mgr, database);
    keel_metadata_cache_t* metadata = entry ? entry->metadata : NULL;
    
    pthread_rwlock_unlock(&mgr->lock);
    return metadata;
}

/* ============================================================================
 * Default Plugin Implementation
 * ============================================================================ */

typedef struct {
    keel_plugin_default_config_t config;
} default_plugin_data_t;

/**
 * @brief Initialize the built-in default plugin.
 */
static keel_error_t default_init(
    keel_router_plugin_t* plugin,
    const void* config
) {
    default_plugin_data_t* data = keel_calloc(1, sizeof(*data));
    if (!data) {
        return KEEL_ERR_NOMEM;
    }
    
    if (config) {
        data->config = *(const keel_plugin_default_config_t*)config;
    } else {
        data->config = (keel_plugin_default_config_t){
            .primary_read_weight = 0.5,
            .respect_metadata = true,
            .conservative_mode = false,
        };
    }
    
    plugin->user_data = data;
    return KEEL_OK;
}

/**
 * @brief Destroy the built-in default plugin state.
 *
 * @param plugin Plugin handle.
 * @return
 */
static void default_destroy(keel_router_plugin_t* plugin) {
    if (plugin && plugin->user_data) {
        keel_free(plugin->user_data);
        plugin->user_data = NULL;
    }
}

/**
 * @brief Route using metadata-aware conservative checks before delegating to the base router.
 */
static keel_error_t default_route(
    keel_router_plugin_t* plugin,
    const keel_router_ctx_t* ctx,
    keel_route_decision_t* decision
) {
    default_plugin_data_t* data = plugin->user_data;
    
    /* Check metadata for write operations */
    if (data->config.respect_metadata && ctx->metadata) {
        if (ctx->has_write_function) {
            /* Query calls a VOLATILE function - must use primary */
            decision->reason = "volatile function call";
            decision->is_read = false;
            return keel_router_route(ctx->router, ctx->qt, ctx->session, decision);
        }
        
        if (ctx->has_write_trigger) {
            decision->reason = "table has write trigger";
            decision->is_read = false;
            return keel_router_route(ctx->router, ctx->qt, ctx->session, decision);
        }
        
        if (ctx->has_write_rule) {
            decision->reason = "view has write rule";
            decision->is_read = false;
            return keel_router_route(ctx->router, ctx->qt, ctx->session, decision);
        }
    }
    
    /* Note: Conservative mode for unknown functions is disabled until 
     * the Query Tree is extended to track function calls.
     * Currently, function_count is not part of keel_qt_query_t.
     */
    (void)data; /* Suppress unused warning when conservative mode is disabled */
    
    /* Delegate to base router */
    return keel_router_route(ctx->router, ctx->qt, ctx->session, decision);
}

/**
 * @brief Check whether the default plugin believes the query may use a replica.
 */
static bool default_can_replica(
    keel_router_plugin_t* plugin,
    const keel_router_ctx_t* ctx
) {
    default_plugin_data_t* data = plugin->user_data;
    
    /* Check metadata */
    if (data->config.respect_metadata) {
        if (ctx->has_write_function || 
            ctx->has_write_trigger || 
            ctx->has_write_rule) {
            return false;
        }
    }
    
    /* Delegate to base router check */
    return keel_router_can_use_replica(ctx->router, ctx->qt, ctx->session);
}

/**
 * @brief Probe hook for the default plugin using existing router role state.
 */
static keel_error_t default_probe(
    keel_router_plugin_t* plugin,
    keel_route_server_t* server,
    bool* is_primary,
    bool* is_readonly
) {
    (void)plugin;
    (void)server;
    
    /* Default probe just uses existing state */
    if (is_primary) {
        *is_primary = (server->role == KEEL_SERVER_PRIMARY);
    }
    if (is_readonly) {
        *is_readonly = (server->role != KEEL_SERVER_PRIMARY);
    }
    
    return KEEL_OK;
}

static const keel_router_plugin_ops_t default_plugin_ops = {
    .name = "default",
    .version = KEEL_ROUTER_PLUGIN_API_VERSION,
    .init = default_init,
    .destroy = default_destroy,
    .route = default_route,
    .can_use_replica = default_can_replica,
    .probe_server = default_probe,
    .refresh_topology = NULL,
    .on_failover = NULL,
};

/**
 * @brief Construct the built-in default router plugin.
 *
 * @return Plugin instance, or `NULL` on failure.
 */
keel_router_plugin_t* keel_router_plugin_default_create(
    const keel_plugin_default_config_t* config
) {
    return keel_router_plugin_create(&default_plugin_ops, config);
}

/* ============================================================================
 * Patroni Plugin Implementation
 * ============================================================================ */

typedef struct {
    keel_plugin_patroni_config_t config;
    char*                       patroni_url;
    char*                       cluster_name;
    keel_time_t                  last_poll;
} patroni_plugin_data_t;

/**
 * @brief Initialize the built-in Patroni-aware plugin.
 */
static keel_error_t patroni_init(
    keel_router_plugin_t* plugin,
    const void* config
) {
    const keel_plugin_patroni_config_t* cfg = config;
    if (!cfg || !cfg->patroni_url) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, "Patroni plugin requires URL");
        return KEEL_ERR_INVALID_ARG;
    }
    
    patroni_plugin_data_t* data = keel_calloc(1, sizeof(*data));
    if (!data) {
        return KEEL_ERR_NOMEM;
    }
    
    data->config = *cfg;
    data->patroni_url = keel_strdup(cfg->patroni_url);
    if (cfg->cluster_name) {
        data->cluster_name = keel_strdup(cfg->cluster_name);
    }
    
    plugin->user_data = data;
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, 
                 "Patroni plugin initialized: url=%s cluster=%s",
                 data->patroni_url, 
                 data->cluster_name ? data->cluster_name : "(auto)");
    
    return KEEL_OK;
}

/**
 * @brief Destroy the Patroni plugin state.
 *
 * @param plugin Plugin handle.
 * @return
 */
static void patroni_destroy(keel_router_plugin_t* plugin) {
    if (plugin && plugin->user_data) {
        patroni_plugin_data_t* data = plugin->user_data;
        keel_free(data->patroni_url);
        keel_free(data->cluster_name);
        keel_free(data);
        plugin->user_data = NULL;
    }
}

/**
 * @brief Route through the base router after Patroni topology synchronization.
 */
static keel_error_t patroni_route(
    keel_router_plugin_t* plugin,
    const keel_router_ctx_t* ctx,
    keel_route_decision_t* decision
) {
    /* Patroni plugin delegates to base router after topology updates */
    /* The topology is kept up-to-date via refresh_topology */
    return keel_router_route(ctx->router, ctx->qt, ctx->session, decision);
}

/**
 * @brief Refresh topology from Patroni REST API
 *
 * Patroni exposes endpoints:
 * - GET /cluster - cluster state
 * - GET /leader  - current leader
 * - GET /replica - replica info
 *
 * Response format:
 * {
 *   "members": [
 *     {"name": "node1", "role": "leader", "host": "...", "port": 5432, "state": "running"},
 *     {"name": "node2", "role": "replica", "host": "...", "port": 5432, "lag": 0}
 *   ]
 * }
 */
static keel_error_t patroni_refresh_topology(
    keel_router_plugin_t* plugin,
    keel_router_t* router
) {
    patroni_plugin_data_t* data = plugin->user_data;

    data->last_poll = keel_time_now();

    /* Query Patroni REST API (/cluster then /patroni fallback) */
    keel_cluster_topology_t topo;
    memset(&topo, 0, sizeof(topo));
    keel_error_t err = patroni_discover(data->patroni_url, &topo);
    if (err != KEEL_OK) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                     "Patroni topology refresh failed for %s: err=%d",
                     data->patroni_url, (int)err);
        return err;
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                 "Patroni: applying %zu server(s) from %s",
                 topo.server_count, data->patroni_url);

    /* Apply topology to the router: update health for known servers,
     * add newly discovered servers with the correct role. */
    const keel_server_info_t* primary_srv = NULL;
    for (size_t i = 0; i < topo.server_count; i++) {
        const keel_server_info_t* srv = &topo.servers[i];
        if (srv->is_primary && !primary_srv) {
            primary_srv = srv;
        }

        keel_route_server_t* existing = keel_router_get_server(router, srv->name);
        if (existing) {
            keel_router_set_server_health(router, srv->name, srv->health);
            existing->role = srv->is_primary ? KEEL_SERVER_PRIMARY : KEEL_SERVER_REPLICA;
            existing->timeline_id = (uint32_t)(srv->timeline > 0 ? srv->timeline : 0);
        } else {
            keel_route_server_t new_srv;
            memset(&new_srv, 0, sizeof(new_srv));
            new_srv.name   = srv->name;
            new_srv.host   = srv->host;
            new_srv.port   = srv->port;
            new_srv.role   = srv->is_primary ? KEEL_SERVER_PRIMARY
                                             : KEEL_SERVER_REPLICA;
            new_srv.timeline_id = (uint32_t)(srv->timeline > 0 ? srv->timeline : 0);
            new_srv.weight = 100;
            new_srv.health = srv->health;
            keel_error_t aerr = keel_router_add_server(router, &new_srv);
            if (aerr != KEEL_OK && aerr != KEEL_ERR_ALREADY_EXISTS) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                             "Patroni: failed to add server '%s': err=%d",
                             srv->name, (int)aerr);
            }
        }
    }

    keel_free(topo.servers);

    /* Failover-manager: feed the primary observation through. If patroni
     * reported a primary, observe_primary() will bump the epoch on flip
     * and fence any previous primary. If no primary was reported, signal
     * "lost" so the router enters degraded mode. */
    if (primary_srv) {
        keel_router_observe_primary(router, primary_srv->name,
            (uint32_t)(primary_srv->timeline > 0 ? primary_srv->timeline : 0));
    } else {
        keel_router_observe_primary(router, NULL, 0);
    }
    return KEEL_OK;
}

/**
 * @brief React to a failover notification by logging and forcing topology refresh.
 *
 * @param plugin Plugin handle.
 * @param old_primary Previous primary name, or `NULL`.
 * @param new_primary New primary name, or `NULL`.
 * @return
 */
static void patroni_on_failover(
    keel_router_plugin_t* plugin,
    const char* old_primary,
    const char* new_primary
) {
    patroni_plugin_data_t* data = plugin->user_data;
    (void)data;
    
    KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                 "Patroni failover detected: %s -> %s",
                 old_primary ? old_primary : "(none)",
                 new_primary ? new_primary : "(none)");
    
    /* Trigger immediate topology refresh */
    if (plugin->manager) {
        keel_router_mgr_refresh_topology(plugin->manager);
    }
}

static const keel_router_plugin_ops_t patroni_plugin_ops = {
    .name = "patroni",
    .version = KEEL_ROUTER_PLUGIN_API_VERSION,
    .init = patroni_init,
    .destroy = patroni_destroy,
    .route = patroni_route,
    .can_use_replica = NULL,  /* Use default */
    .probe_server = NULL,     /* Use Patroni API instead */
    .refresh_topology = patroni_refresh_topology,
    .on_failover = patroni_on_failover,
};

/**
 * @brief Construct the built-in Patroni plugin.
 *
 * @return Plugin instance, or `NULL` on failure.
 */
keel_router_plugin_t* keel_router_plugin_patroni_create(
    const keel_plugin_patroni_config_t* config
) {
    return keel_router_plugin_create(&patroni_plugin_ops, config);
}
