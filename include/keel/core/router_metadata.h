/**
 * @file router_metadata.h
 * @brief Public API for metadata-aware routing caches and query side-effect analysis.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This module provides caching of database object metadata to enable
 * intelligent routing decisions based on:
 *
 * 1. **Functions/Stored Procedures**:
 *    - VOLATILE functions that may write data
 *    - Functions with SECURITY DEFINER (may bypass RLS)
 *    - Functions that return triggers
 *
 * 2. **Views with Rules**:
 *    - Views with INSERT/UPDATE/DELETE rules
 *    - Views that trigger writes on SELECT
 *
 * 3. **Materialized Views**:
 *    - Matviews that need refresh consistency
 *    - Queries that should see refreshed data
 *
 * 4. **Tables with Triggers**:
 *    - Tables with AFTER triggers that write
 *    - Tables with INSTEAD OF triggers
 *
 * Cache Strategy:
 * ===============
 * - Cache is populated lazily on first query or explicit refresh
 * - Objects are looked up by schema.name
 * - Cache invalidated on DDL or periodic refresh
 * - Uses hash table for O(1) lookups
 *
 * Thread Safety:
 * ==============
 * - Cache is thread-safe with read-write locks
 * - Refresh operations acquire write lock
 * - Lookups acquire read lock
 *
 * Example:
 * ========
 * @code
 * // Create cache for database
 * keel_metadata_cache_t* cache = keel_metadata_cache_create("mydb");
 *
 * // Populate from connection
 * keel_metadata_cache_refresh(cache, conn);
 *
 * // Check if function writes
 * keel_object_meta_t* meta;
 * if (keel_metadata_cache_lookup(cache, "public", "my_function", 'f', &meta)) {
 *     if (meta->write_type != KEEL_OBJ_WRITE_NONE) {
 *         // Route to primary
 *     }
 * }
 * @endcode
 */

#ifndef KEEL_ROUTER_METADATA_H
#define KEEL_ROUTER_METADATA_H

#include "keel_types.h"
#include "keel_error.h"
#include "keel/core/router_plugin.h"
#include "keel/mem/mem.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Types
 * ============================================================================ */

/** Opaque metadata cache handle keyed by database name. */
typedef struct keel_metadata_cache keel_metadata_cache_t;

/** Abstract connection interface used to execute metadata refresh queries. */
typedef struct keel_metadata_conn keel_metadata_conn_t;

/**
 * @brief Aggregate statistics describing cache contents and refresh behavior.
 */
typedef struct keel_metadata_stats {
    size_t          total_objects;      /**< Total cached objects */
    size_t          functions;          /**< Cached functions */
    size_t          views;              /**< Cached views */
    size_t          matviews;           /**< Cached materialized views */
    size_t          tables;             /**< Cached tables (with triggers) */
    
    size_t          write_functions;    /**< Functions that write */
    size_t          write_rules;        /**< Views with write rules */
    size_t          write_triggers;     /**< Tables with write triggers */
    
    uint64_t        cache_hits;         /**< Lookup hits */
    uint64_t        cache_misses;       /**< Lookup misses */
    uint64_t        refreshes;          /**< Cache refreshes */
    
    keel_time_t      last_refresh;       /**< Last refresh time */
    keel_duration_t  last_refresh_duration; /**< Last refresh duration */
} keel_metadata_stats_t;

/**
 * @brief Configuration controlling cache size, scope, and filtering behavior.
 */
typedef struct keel_metadata_config {
    size_t          initial_capacity;   /**< Initial hash table capacity */
    bool            cache_all_objects;  /**< Cache all objects (not just problematic) */
    bool            track_dependencies; /**< Track object dependencies */
    const char**    schema_whitelist;   /**< Only cache these schemas (NULL=all) */
    size_t          schema_count;       /**< Number of whitelisted schemas */
    const char**    schema_blacklist;   /**< Skip these schemas */
    size_t          blacklist_count;    /**< Number of blacklisted schemas */
} keel_metadata_config_t;

/**
 * @brief Cached metadata stored for a database object referenced by queries.
 */
typedef struct keel_cached_object {
    /* Identity */
    char*                   schema;         /**< Schema name (owned) */
    char*                   name;           /**< Object name (owned) */
    uint32_t                oid;            /**< PostgreSQL OID */
    char                    type;           /**< Object type */
    
    /* Write behavior */
    keel_object_write_type_t write_type;     /**< Write classification */
    bool                    is_volatile;    /**< VOLATILE function */
    bool                    is_security_definer; /**< Security definer */
    bool                    returns_trigger;     /**< Returns trigger */
    bool                    has_side_effects;    /**< Known side effects */
    
    /* For functions */
    char*                   return_type;    /**< Return type name */
    int                     arg_count;      /**< Number of arguments */
    
    /* For views */
    bool                    has_insert_rule; /**< Has INSERT rule */
    bool                    has_update_rule; /**< Has UPDATE rule */
    bool                    has_delete_rule; /**< Has DELETE rule */
    bool                    is_updatable;    /**< Automatically updatable */
    
    /* For tables */
    bool                    has_insert_trigger; /**< Has INSERT trigger */
    bool                    has_update_trigger; /**< Has UPDATE trigger */
    bool                    has_delete_trigger; /**< Has DELETE trigger */
    bool                    has_truncate_trigger; /**< Has TRUNCATE trigger */
    
    /* Caching metadata */
    keel_time_t              cached_at;      /**< When cached */
    uint64_t                hit_count;      /**< Cache hit count */
    
    /* Hash table linkage */
    struct keel_cached_object* hash_next;    /**< Hash chain */
    
} keel_cached_object_t;

/* ============================================================================
 * Cache Lifecycle
 * ============================================================================ */

/**
 * @brief Construct a metadata-cache configuration populated with defaults.
 *
 * @return Default cache configuration.
 */
keel_metadata_config_t keel_metadata_config_default(void);

/**
 * @brief Create a metadata cache for a database
 *
 * @param database Database name
 * @param config   Cache configuration (NULL for defaults)
 * @return Cache instance, or `NULL` if allocation or initialization fails.
 */
keel_metadata_cache_t* keel_metadata_cache_create(
    const char* database,
    const keel_metadata_config_t* config
);

/**
 * @brief Destroy a metadata cache and free all cached objects.
 * @return
 */
void keel_metadata_cache_destroy(keel_metadata_cache_t* cache);

/**
 * @brief Remove all cached objects without destroying the cache handle itself.
 *
 * @param cache Cache instance to clear.
 * @return
 */
void keel_metadata_cache_clear(keel_metadata_cache_t* cache);

/* ============================================================================
 * Cache Population
 * ============================================================================ */

/**
 * @brief Connection interface for metadata queries
 *
 * This abstracts the connection to allow testing and
 * different connection backends.
 */
struct keel_metadata_conn {
    void* user_data;
    
    /**
     * @brief Execute a query and iterate results
     *
     * @param conn      Connection instance
     * @param query     SQL query
     * @param callback  Called for each row
     * @param user_data User data for callback
    * @return `KEEL_OK` on success or an error code if the query could not be executed.
     */
    keel_error_t (*query)(
        keel_metadata_conn_t* conn,
        const char* query,
        bool (*callback)(void* user_data, int col_count, 
                         const char** values, const char** names),
        void* user_data
    );
};

/**
 * @brief Refresh cache from database connection
 *
 * Queries the database for:
 * - pg_proc (functions with volatility, security definer)
 * - pg_rewrite (view rules)
 * - pg_trigger (table triggers)
 * - pg_matviews (materialized views)
 *
 * @param cache Cache instance
 * @param conn  Connection interface
 * @return `KEEL_OK` on success or an error code if any refresh query fails.
 */
keel_error_t keel_metadata_cache_refresh(
    keel_metadata_cache_t* cache,
    keel_metadata_conn_t* conn
);

/**
 * @brief Refresh cache entries for a restricted set of schemas.
 *
 * @param cache Cache instance.
 * @param conn Connection interface used to query metadata.
 * @param schemas Array of schema names to refresh.
 * @param schema_count Number of schema names in `schemas`.
 * @return `KEEL_OK` on success or an error code if refresh fails.
 */
keel_error_t keel_metadata_cache_refresh_schemas(
    keel_metadata_cache_t* cache,
    keel_metadata_conn_t* conn,
    const char** schemas,
    size_t schema_count
);

/**
 * @brief Add a single object to cache
 *
 * Used for incremental updates or testing.
 *
 * @param cache  Cache instance
 * @param object Object to add (copied)
 * @return `KEEL_OK` on success or an error code if the object could not be copied.
 */
keel_error_t keel_metadata_cache_add(
    keel_metadata_cache_t* cache,
    const keel_cached_object_t* object
);

/**
 * @brief Remove one cached object by identity.
 *
 * @param cache Cache instance.
 * @param schema Object schema name.
 * @param name Object name.
 * @param type Object type discriminator.
 * @return `KEEL_OK` on success or an error code if the object was not found.
 */
keel_error_t keel_metadata_cache_remove(
    keel_metadata_cache_t* cache,
    const char* schema,
    const char* name,
    char type
);

/* ============================================================================
 * Cache Lookup
 * ============================================================================ */

/**
 * @brief Look up an object in the cache
 *
 * @param cache  Cache instance
 * @param schema Schema name (NULL for search_path)
 * @param name   Object name
 * @param type   Object type ('f', 'v', 'm', 'r', or 0 for any)
 * @param[out] out Object metadata if found
 * @return `true` if the object was found and `out` was populated, otherwise `false`.
 */
bool keel_metadata_cache_lookup(
    keel_metadata_cache_t* cache,
    const char* schema,
    const char* name,
    char type,
    const keel_cached_object_t** out
);

/**
 * @brief Check if object has write side effects
 *
 * Quick check for routing decisions.
 *
 * @param cache  Cache instance
 * @param schema Schema name
 * @param name   Object name
 * @param type   Object type
 * @return Write classification, or `KEEL_OBJ_WRITE_NONE` if the object is absent
 *         or considered read-only.
 */
keel_object_write_type_t keel_metadata_cache_check_write(
    keel_metadata_cache_t* cache,
    const char* schema,
    const char* name,
    char type
);

/**
 * @brief Check multiple objects for write behavior
 *
 * Efficient batch check for queries with multiple objects.
 *
 * @param cache   Cache instance
 * @param schemas Array of schema names
 * @param names   Array of object names
 * @param types   Array of object types
 * @param count   Number of objects
 * @param[out] has_write Set to true if any object writes
 * @return `KEEL_OK` on success or an error code if analysis could not be completed.
 */
keel_error_t keel_metadata_cache_check_batch(
    keel_metadata_cache_t* cache,
    const char** schemas,
    const char** names,
    const char* types,
    size_t count,
    bool* has_write
);

/* ============================================================================
 * Query Analysis Integration
 * ============================================================================ */

/**
 * @brief Analyze query for write objects using cache
 *
 * Extracts objects from Query Tree and checks cache.
 *
 * @param cache   Cache instance
 * @param qt      Query tree to analyze
 * @param[out] has_write_function Query uses write function
 * @param[out] has_write_trigger  Query touches table with write trigger
 * @param[out] has_write_rule     Query uses view with write rule
 * @param[out] needs_primary      Query must go to primary
 * @return KEEL_OK on success
 */
keel_error_t keel_metadata_analyze_query(
    keel_metadata_cache_t* cache,
    const keel_qt_query_t* qt,
    bool* has_write_function,
    bool* has_write_trigger,
    bool* has_write_rule,
    bool* needs_primary
);

/**
 * @brief Get objects referenced by query
 *
 * Populates array with metadata for all objects in query.
 *
 * @param cache     Cache instance
 * @param qt        Query tree
 * @param[out] objects Array to fill (caller allocated)
 * @param max_objects Maximum objects to return
 * @return Number of referenced objects copied into `objects`.
 */
size_t keel_metadata_get_query_objects(
    keel_metadata_cache_t* cache,
    const keel_qt_query_t* qt,
    keel_object_meta_t* objects,
    size_t max_objects
);

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Copy cache statistics into a caller-provided structure.
 *
 * @param cache Cache instance.
 * @param stats Output statistics structure.
 * @return
 */
void keel_metadata_cache_get_stats(
    const keel_metadata_cache_t* cache,
    keel_metadata_stats_t* stats
);

/**
 * @brief Emit a human-readable dump of cache contents for debugging.
 *
 * @param cache Cache instance.
 * @param out Output stream that receives the dump.
 * @return
 */
void keel_metadata_cache_dump(
    const keel_metadata_cache_t* cache,
    FILE* out
);

/* ============================================================================
 * Introspection Queries
 * ============================================================================ */

/**
 * @brief SQL to query volatile/write functions
 *
 * Returns: schema, name, volatility, is_security_definer, returns_trigger
 */
extern const char* KEEL_SQL_QUERY_FUNCTIONS;

/**
 * @brief SQL to query views with rules
 *
 * Returns: schema, name, has_insert_rule, has_update_rule, has_delete_rule
 */
extern const char* KEEL_SQL_QUERY_VIEW_RULES;

/**
 * @brief SQL to query tables with write triggers
 *
 * Returns: schema, name, trigger_types (INSERT/UPDATE/DELETE/TRUNCATE)
 */
extern const char* KEEL_SQL_QUERY_TRIGGERS;

/**
 * @brief SQL to query materialized views
 *
 * Returns: schema, name, last_refresh
 */
extern const char* KEEL_SQL_QUERY_MATVIEWS;

/**
 * @brief SQL to check if server is primary
 *
 * Returns: is_in_recovery (false = primary)
 */
extern const char* KEEL_SQL_CHECK_PRIMARY;

/**
 * @brief SQL to check if server is read-only
 *
 * Returns: is_readonly
 */
extern const char* KEEL_SQL_CHECK_READONLY;

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ROUTER_METADATA_H */
