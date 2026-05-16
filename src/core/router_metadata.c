/**
 * @file router_metadata.c
 * @brief Metadata cache for write-sensitive database objects.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file maintains a per-database cache of functions, views, triggers,
 * tables, and materialized views that may force routing to primary nodes. The
 * cache is refreshed from PostgreSQL catalog queries and then consulted during
 * metadata-aware routing.
 */

#include "keel/core/router_metadata.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"
#include "keel/util/util.h"

#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>

/* ============================================================================
 * PostgreSQL Introspection Queries
 * ============================================================================ */

/**
 * Query to find functions that may write data.
 * 
 * We look for:
 * - VOLATILE functions (may have side effects)
 * - SECURITY DEFINER functions (may bypass RLS)
 * - Functions that return TRIGGER
 */
const char* KEEL_SQL_QUERY_FUNCTIONS = 
    "SELECT n.nspname AS schema, "
    "       p.proname AS name, "
    "       p.provolatile AS volatility, "
    "       p.prosecdef AS is_security_definer, "
    "       (p.prorettype = 'trigger'::regtype) AS returns_trigger, "
    "       p.pronargs AS arg_count, "
    "       pg_get_function_result(p.oid) AS return_type, "
    "       p.oid "
    "FROM pg_proc p "
    "JOIN pg_namespace n ON p.pronamespace = n.oid "
    "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') "
    "  AND (p.provolatile = 'v' "          /* VOLATILE */
    "       OR p.prosecdef = true "         /* SECURITY DEFINER */
    "       OR p.prorettype = 'trigger'::regtype) " /* Returns trigger */
    "ORDER BY n.nspname, p.proname";

/**
 * Query to find views with INSERT/UPDATE/DELETE rules.
 * 
 * Views with rules can cause writes when selected from.
 */
const char* KEEL_SQL_QUERY_VIEW_RULES =
    "SELECT n.nspname AS schema, "
    "       c.relname AS name, "
    "       EXISTS(SELECT 1 FROM pg_rewrite r "
    "              WHERE r.ev_class = c.oid AND r.ev_type = '2') AS has_insert_rule, "
    "       EXISTS(SELECT 1 FROM pg_rewrite r "
    "              WHERE r.ev_class = c.oid AND r.ev_type = '4') AS has_update_rule, "
    "       EXISTS(SELECT 1 FROM pg_rewrite r "
    "              WHERE r.ev_class = c.oid AND r.ev_type = '3') AS has_delete_rule, "
    "       pg_relation_is_updatable(c.oid, true) != 0 AS is_updatable, "
    "       c.oid "
    "FROM pg_class c "
    "JOIN pg_namespace n ON c.relnamespace = n.oid "
    "WHERE c.relkind = 'v' "
    "  AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
    "  AND EXISTS(SELECT 1 FROM pg_rewrite r "
    "             WHERE r.ev_class = c.oid "
    "               AND r.ev_type IN ('2', '3', '4') " /* INSERT, DELETE, UPDATE */
    "               AND r.is_instead) "
    "ORDER BY n.nspname, c.relname";

/**
 * Query to find tables with triggers that may write.
 * 
 * We look for AFTER triggers on INSERT/UPDATE/DELETE/TRUNCATE
 * that might cause additional writes.
 */
const char* KEEL_SQL_QUERY_TRIGGERS =
    "SELECT n.nspname AS schema, "
    "       c.relname AS name, "
    "       string_agg(DISTINCT "
    "           CASE t.tgtype & 0x03 "  /* Trigger type bits */
    "               WHEN 2 THEN 'I' "   /* INSERT */
    "               WHEN 4 THEN 'D' "   /* DELETE */
    "               WHEN 8 THEN 'U' "   /* UPDATE */
    "               WHEN 32 THEN 'T' "  /* TRUNCATE */
    "               ELSE '' END, '') AS trigger_types, "
    "       c.oid "
    "FROM pg_trigger t "
    "JOIN pg_class c ON t.tgrelid = c.oid "
    "JOIN pg_namespace n ON c.relnamespace = n.oid "
    "WHERE NOT t.tgisinternal "
    "  AND t.tgenabled = 'O' "  /* Origin-firing triggers */
    "  AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
    "  AND (t.tgtype & 0x01) = 0 "  /* AFTER triggers */
    "GROUP BY n.nspname, c.relname, c.oid "
    "ORDER BY n.nspname, c.relname";

/**
 * Query to list materialized views.
 */
const char* KEEL_SQL_QUERY_MATVIEWS =
    "SELECT n.nspname AS schema, "
    "       c.relname AS name, "
    "       c.oid "
    "FROM pg_class c "
    "JOIN pg_namespace n ON c.relnamespace = n.oid "
    "WHERE c.relkind = 'm' "
    "  AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
    "ORDER BY n.nspname, c.relname";

/**
 * Query to check if server is in recovery (replica).
 */
const char* KEEL_SQL_CHECK_PRIMARY =
    "SELECT NOT pg_is_in_recovery() AS is_primary";

/**
 * Query to check if server is read-only.
 */
const char* KEEL_SQL_CHECK_READONLY =
    "SELECT current_setting('default_transaction_read_only')::boolean "
    "       OR pg_is_in_recovery() AS is_readonly";

/* ============================================================================
 * Hash Table Implementation
 * ============================================================================ */

#define INITIAL_CAPACITY 256
#define LOAD_FACTOR 0.75

/**
 * @brief Compute the hash-table key for one cached object.
 *
 * @return 64-bit FNV-1a hash value.
 */
static uint64_t hash_object_key(const char* schema, const char* name, char type) {
    uint64_t hash = 14695981039346656037ULL;
    
    if (schema) {
        for (const char* p = schema; *p; p++) {
            hash ^= (uint64_t)(unsigned char)*p;
            hash *= 1099511628211ULL;
        }
    }
    hash ^= '.';
    hash *= 1099511628211ULL;
    
    for (const char* p = name; *p; p++) {
        hash ^= (uint64_t)(unsigned char)*p;
        hash *= 1099511628211ULL;
    }
    hash ^= type;
    hash *= 1099511628211ULL;
    
    return hash;
}

/* ============================================================================
 * Metadata Cache Structure
 * ============================================================================ */

struct keel_metadata_cache {
    char*                   database;       /**< Database name */
    keel_metadata_config_t   config;         /**< Configuration */
    
    /* Hash table */
    keel_cached_object_t**   buckets;        /**< Hash buckets */
    size_t                  capacity;       /**< Number of buckets */
    size_t                  count;          /**< Number of objects */
    
    /* Statistics */
    keel_metadata_stats_t    stats;
    
    /* Thread safety */
    pthread_rwlock_t        lock;
    
    /* Memory */
    keel_arena_t*            arena;          /**< Memory arena */
};

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @brief Return the default metadata-cache configuration.
 *
 * @return Default metadata configuration.
 */
keel_metadata_config_t keel_metadata_config_default(void) {
    return (keel_metadata_config_t){
        .initial_capacity = INITIAL_CAPACITY,
        .cache_all_objects = false,
        .track_dependencies = false,
        .schema_whitelist = NULL,
        .schema_count = 0,
        .schema_blacklist = NULL,
        .blacklist_count = 0,
    };
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Create a metadata cache for a specific database.
 *
 * @param database Database name.
 * @param config Optional configuration.
 * @return Cache instance on success, or `NULL` on allocation/init failure.
 */
keel_metadata_cache_t* keel_metadata_cache_create(
    const char* database,
    const keel_metadata_config_t* config
) {
    keel_metadata_cache_t* cache = keel_calloc(1, sizeof(*cache));
    if (!cache) {
        return NULL;
    }
    
    cache->database = keel_strdup(database);
    if (!cache->database) {
        keel_free(cache);
        return NULL;
    }
    
    if (config) {
        cache->config = *config;
    } else {
        cache->config = keel_metadata_config_default();
    }
    
    cache->capacity = cache->config.initial_capacity;
    cache->buckets = keel_calloc(cache->capacity, sizeof(keel_cached_object_t*));
    if (!cache->buckets) {
        keel_free(cache->database);
        keel_free(cache);
        return NULL;
    }
    
    cache->arena = keel_arena_create(64 * 1024);
    if (!cache->arena) {
        keel_free(cache->buckets);
        keel_free(cache->database);
        keel_free(cache);
        return NULL;
    }
    
    if (pthread_rwlock_init(&cache->lock, NULL) != 0) {
        keel_arena_destroy(cache->arena);
        keel_free(cache->buckets);
        keel_free(cache->database);
        keel_free(cache);
        return NULL;
    }
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL, 
                 "Created metadata cache for database '%s' (capacity=%zu)",
                 database, cache->capacity);
    
    return cache;
}

/**
 * @brief Destroy a metadata cache and all owned memory.
 *
 * @param cache Cache handle, or `NULL`.
 * @return
 */
void keel_metadata_cache_destroy(keel_metadata_cache_t* cache) {
    if (!cache) return;
    
    pthread_rwlock_destroy(&cache->lock);
    keel_arena_destroy(cache->arena);
    keel_free(cache->buckets);
    keel_free(cache->database);
    keel_free(cache);
}

/**
 * @brief Clear cached objects while retaining the cache container itself.
 *
 * @param cache Cache handle.
 * @return
 */
void keel_metadata_cache_clear(keel_metadata_cache_t* cache) {
    if (!cache) return;
    
    pthread_rwlock_wrlock(&cache->lock);
    
    /* Clear buckets */
    memset(cache->buckets, 0, cache->capacity * sizeof(keel_cached_object_t*));
    cache->count = 0;
    
    /* Reset arena */
    keel_arena_reset(cache->arena);
    
    /* Update stats */
    memset(&cache->stats, 0, sizeof(cache->stats));
    
    pthread_rwlock_unlock(&cache->lock);
}

/* ============================================================================
 * Hash Table Operations (Internal, must hold lock)
 * ============================================================================ */

/**
 * @brief Rehash the metadata table into a larger bucket array.
 *
 * @param cache Cache handle.
 * @return
 */
static void cache_grow(keel_metadata_cache_t* cache) {
    size_t new_capacity = cache->capacity * 2;
    keel_cached_object_t** new_buckets = keel_calloc(new_capacity, 
                                                    sizeof(keel_cached_object_t*));
    if (!new_buckets) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL, "Failed to grow metadata cache");
        return;
    }
    
    /* Rehash all objects */
    for (size_t i = 0; i < cache->capacity; i++) {
        keel_cached_object_t* obj = cache->buckets[i];
        while (obj) {
            keel_cached_object_t* next = obj->hash_next;
            
            uint64_t hash = hash_object_key(obj->schema, obj->name, obj->type);
            size_t idx = hash % new_capacity;
            
            obj->hash_next = new_buckets[idx];
            new_buckets[idx] = obj;
            
            obj = next;
        }
    }
    
    keel_free(cache->buckets);
    cache->buckets = new_buckets;
    cache->capacity = new_capacity;
}

/**
 * @brief Find one cached object by schema, name, and optional type.
 *
 * @note Caller must already hold the appropriate cache lock.
 */
static keel_cached_object_t* cache_find(
    keel_metadata_cache_t* cache,
    const char* schema,
    const char* name,
    char type
) {
    uint64_t hash = hash_object_key(schema, name, type);
    size_t idx = hash % cache->capacity;
    
    keel_cached_object_t* obj = cache->buckets[idx];
    while (obj) {
        bool schema_match = (schema == NULL && obj->schema == NULL) ||
                           (schema && obj->schema && strcmp(schema, obj->schema) == 0);
        
        if (schema_match && 
            strcmp(name, obj->name) == 0 &&
            (type == 0 || obj->type == type)) {
            return obj;
        }
        obj = obj->hash_next;
    }
    
    return NULL;
}

/**
 * @brief Insert one object into the metadata cache.
 *
 * @note Caller must already hold the write lock.
 */
static keel_error_t cache_insert(
    keel_metadata_cache_t* cache,
    const keel_cached_object_t* object
) {
    /* Check load factor */
    if ((double)cache->count / cache->capacity > LOAD_FACTOR) {
        cache_grow(cache);
    }
    
    /* Allocate object from arena */
    keel_cached_object_t* obj = keel_arena_alloc(cache->arena, sizeof(*obj));
    if (!obj) {
        return KEEL_ERR_NOMEM;
    }
    
    *obj = *object;
    
    /* Copy strings to arena */
    if (object->schema) {
        size_t len = strlen(object->schema) + 1;
        obj->schema = keel_arena_alloc(cache->arena, len);
        if (!obj->schema) return KEEL_ERR_NOMEM;
        memcpy(obj->schema, object->schema, len);
    }
    
    size_t len = strlen(object->name) + 1;
    obj->name = keel_arena_alloc(cache->arena, len);
    if (!obj->name) return KEEL_ERR_NOMEM;
    memcpy(obj->name, object->name, len);
    
    if (object->return_type) {
        len = strlen(object->return_type) + 1;
        obj->return_type = keel_arena_alloc(cache->arena, len);
        if (!obj->return_type) return KEEL_ERR_NOMEM;
        memcpy(obj->return_type, object->return_type, len);
    }
    
    /* Set cache time */
    obj->cached_at = keel_time_now();
    obj->hit_count = 0;
    
    /* Insert into hash table */
    uint64_t hash = hash_object_key(obj->schema, obj->name, obj->type);
    size_t idx = hash % cache->capacity;
    
    obj->hash_next = cache->buckets[idx];
    cache->buckets[idx] = obj;
    cache->count++;
    
    /* Update type-specific stats */
    cache->stats.total_objects++;
    switch (obj->type) {
        case 'f': 
            cache->stats.functions++;
            if (obj->write_type != KEEL_OBJ_WRITE_NONE) {
                cache->stats.write_functions++;
            }
            break;
        case 'v': 
            cache->stats.views++;
            if (obj->has_insert_rule || obj->has_update_rule || obj->has_delete_rule) {
                cache->stats.write_rules++;
            }
            break;
        case 'm': 
            cache->stats.matviews++;
            break;
        case 'r': 
            cache->stats.tables++;
            if (obj->has_insert_trigger || obj->has_update_trigger || 
                obj->has_delete_trigger) {
                cache->stats.write_triggers++;
            }
            break;
    }
    
    return KEEL_OK;
}

/* ============================================================================
 * Cache Population
 * ============================================================================ */

/* Callback context for query processing */
typedef struct {
    keel_metadata_cache_t* cache;
    char query_type;  /* 'f'=function, 'v'=view, 't'=trigger, 'm'=matview */
} refresh_ctx_t;

/**
 * @brief Convert one function catalog row into a cached-object entry.
 */
static bool process_function_row(
    void* user_data, 
    int col_count, 
    const char** values, 
    const char** names
) {
    refresh_ctx_t* ctx = user_data;
    (void)names;
    (void)col_count;
    
    keel_cached_object_t obj = {
        .schema = (char*)values[0],
        .name = (char*)values[1],
        .type = 'f',
        .is_volatile = (values[2] && values[2][0] == 'v'),
        .is_security_definer = (values[3] && values[3][0] == 't'),
        .returns_trigger = (values[4] && values[4][0] == 't'),
        .arg_count = values[5] ? atoi(values[5]) : 0,
        .return_type = (char*)values[6],
        .oid = values[7] ? (uint32_t)atoi(values[7]) : 0,
    };
    
    /* Classify write behavior */
    if (obj.is_volatile) {
        obj.write_type = KEEL_OBJ_WRITE_ALWAYS;
        obj.has_side_effects = true;
    } else if (obj.returns_trigger) {
        obj.write_type = KEEL_OBJ_WRITE_TRIGGER;
    } else if (obj.is_security_definer) {
        /* SECURITY DEFINER might bypass checks, be conservative */
        obj.write_type = KEEL_OBJ_WRITE_POSSIBLE;
    }
    
    cache_insert(ctx->cache, &obj);
    return true;  /* Continue iteration */
}

/**
 * @brief Convert one view-rule catalog row into a cached-object entry.
 */
static bool process_view_row(
    void* user_data, 
    int col_count, 
    const char** values, 
    const char** names
) {
    refresh_ctx_t* ctx = user_data;
    (void)names;
    (void)col_count;
    
    keel_cached_object_t obj = {
        .schema = (char*)values[0],
        .name = (char*)values[1],
        .type = 'v',
        .has_insert_rule = (values[2] && values[2][0] == 't'),
        .has_update_rule = (values[3] && values[3][0] == 't'),
        .has_delete_rule = (values[4] && values[4][0] == 't'),
        .is_updatable = (values[5] && values[5][0] == 't'),
        .oid = values[6] ? (uint32_t)atoi(values[6]) : 0,
    };
    
    if (obj.has_insert_rule || obj.has_update_rule || obj.has_delete_rule) {
        obj.write_type = KEEL_OBJ_WRITE_RULE;
    }
    
    cache_insert(ctx->cache, &obj);
    return true;
}

/**
 * @brief Convert one trigger catalog row into a cached-object entry.
 */
static bool process_trigger_row(
    void* user_data, 
    int col_count, 
    const char** values, 
    const char** names
) {
    refresh_ctx_t* ctx = user_data;
    (void)names;
    (void)col_count;
    
    const char* trigger_types = values[2] ? values[2] : "";
    
    keel_cached_object_t obj = {
        .schema = (char*)values[0],
        .name = (char*)values[1],
        .type = 'r',  /* Table */
        .has_insert_trigger = (strchr(trigger_types, 'I') != NULL),
        .has_update_trigger = (strchr(trigger_types, 'U') != NULL),
        .has_delete_trigger = (strchr(trigger_types, 'D') != NULL),
        .has_truncate_trigger = (strchr(trigger_types, 'T') != NULL),
        .oid = values[3] ? (uint32_t)atoi(values[3]) : 0,
    };
    
    if (obj.has_insert_trigger || obj.has_update_trigger || 
        obj.has_delete_trigger) {
        obj.write_type = KEEL_OBJ_WRITE_TRIGGER;
    }
    
    cache_insert(ctx->cache, &obj);
    return true;
}

/**
 * @brief Convert one materialized-view catalog row into a cached-object entry.
 */
static bool process_matview_row(
    void* user_data, 
    int col_count, 
    const char** values, 
    const char** names
) {
    refresh_ctx_t* ctx = user_data;
    (void)names;
    (void)col_count;
    
    keel_cached_object_t obj = {
        .schema = (char*)values[0],
        .name = (char*)values[1],
        .type = 'm',
        .write_type = KEEL_OBJ_WRITE_MATVIEW,
        .oid = values[2] ? (uint32_t)atoi(values[2]) : 0,
    };
    
    cache_insert(ctx->cache, &obj);
    return true;
}

/**
 * @brief Fully refresh the metadata cache from PostgreSQL catalog queries.
 *
 * @param cache Cache handle.
 * @param conn Metadata connection callback set.
 * @return `KEEL_OK` on success, or an error for invalid input.
 *
 * Corner cases:
 * - individual catalog query failures are logged but do not abort the whole refresh
 */
keel_error_t keel_metadata_cache_refresh(
    keel_metadata_cache_t* cache,
    keel_metadata_conn_t* conn
) {
    if (!cache || !conn) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    keel_time_t start = keel_time_now();
    
    pthread_rwlock_wrlock(&cache->lock);
    
    /* Clear existing cache */
    memset(cache->buckets, 0, cache->capacity * sizeof(keel_cached_object_t*));
    cache->count = 0;
    keel_arena_reset(cache->arena);
    memset(&cache->stats, 0, sizeof(cache->stats));
    
    refresh_ctx_t ctx = { .cache = cache };
    keel_error_t err;
    
    /* Query functions */
    ctx.query_type = 'f';
    err = conn->query(conn, KEEL_SQL_QUERY_FUNCTIONS, process_function_row, &ctx);
    if (err != KEEL_OK) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL, "Failed to query functions: %d", err);
    }
    
    /* Query views with rules */
    ctx.query_type = 'v';
    err = conn->query(conn, KEEL_SQL_QUERY_VIEW_RULES, process_view_row, &ctx);
    if (err != KEEL_OK) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL, "Failed to query view rules: %d", err);
    }
    
    /* Query tables with triggers */
    ctx.query_type = 't';
    err = conn->query(conn, KEEL_SQL_QUERY_TRIGGERS, process_trigger_row, &ctx);
    if (err != KEEL_OK) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL, "Failed to query triggers: %d", err);
    }
    
    /* Query materialized views */
    ctx.query_type = 'm';
    err = conn->query(conn, KEEL_SQL_QUERY_MATVIEWS, process_matview_row, &ctx);
    if (err != KEEL_OK) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL, "Failed to query matviews: %d", err);
    }
    
    keel_time_t end = keel_time_now();
    
    cache->stats.last_refresh = end;
    cache->stats.last_refresh_duration = end - start;
    cache->stats.refreshes++;
    
    pthread_rwlock_unlock(&cache->lock);
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                 "Metadata cache refreshed for '%s': %zu objects "
                 "(%zu functions, %zu views, %zu matviews, %zu tables) in %lums",
                 cache->database, cache->count,
                 cache->stats.functions, cache->stats.views,
                 cache->stats.matviews, cache->stats.tables,
                 (unsigned long)(cache->stats.last_refresh_duration / 1000000));
    
    return KEEL_OK;
}

/**
 * @brief Refresh only selected schemas.
 *
 * Builds filtered catalog queries by appending
 * `AND n.nspname IN ('s1','s2',...)` to each base query, clears the cache,
 * and then populates it with only the objects that belong to those schemas.
 * Objects from other schemas are not re-populated; callers that need
 * cross-schema data should use `keel_metadata_cache_refresh()` instead.
 */
keel_error_t keel_metadata_cache_refresh_schemas(
    keel_metadata_cache_t* cache,
    keel_metadata_conn_t* conn,
    const char** schemas,
    size_t schema_count
) {
    if (!cache || !conn) {
        return KEEL_ERR_INVALID_ARG;
    }

    /* Fall back to full refresh when no schema list is provided */
    if (!schemas || schema_count == 0) {
        return keel_metadata_cache_refresh(cache, conn);
    }

    /*
     * Build SQL IN clause: ('s1','s2',...)
     * Single-quote each schema name, doubling any internal single-quotes.
     */
    char in_clause[4096];
    size_t pos = 0;
    in_clause[pos++] = '(';
    for (size_t i = 0; i < schema_count && pos < sizeof(in_clause) - 8; i++) {
        if (i > 0) {
            in_clause[pos++] = ',';
        }
        in_clause[pos++] = '\'';
        for (const char* p = schemas[i]; *p && pos < sizeof(in_clause) - 8; p++) {
            if (*p == '\'') {
                in_clause[pos++] = '\'';
            }
            in_clause[pos++] = *p;
        }
        in_clause[pos++] = '\'';
    }
    in_clause[pos++] = ')';
    in_clause[pos] = '\0';

#define SCHEMA_FILTER_CLAUSE " AND n.nspname IN "

    /* Build one filtered query per catalog; 8 KiB is ample for the base SQL +
     * filter (base queries are ~600 bytes). */
    char sql_buf[8192];
    int n;

    pthread_rwlock_wrlock(&cache->lock);

    /* Clear the cache so filtered results are the only entries */
    memset(cache->buckets, 0, cache->capacity * sizeof(keel_cached_object_t*));
    cache->count = 0;
    keel_arena_reset(cache->arena);
    memset(&cache->stats, 0, sizeof(cache->stats));

    refresh_ctx_t ctx = { .cache = cache };
    keel_error_t err;

    n = snprintf(sql_buf, sizeof(sql_buf), "%s%s%s",
                 KEEL_SQL_QUERY_FUNCTIONS, SCHEMA_FILTER_CLAUSE, in_clause);
    if (n > 0 && (size_t)n < sizeof(sql_buf)) {
        ctx.query_type = 'f';
        err = conn->query(conn, sql_buf, process_function_row, &ctx);
        if (err != KEEL_OK) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                "Schema-filtered function query failed: %d", err);
        }
    }

    n = snprintf(sql_buf, sizeof(sql_buf), "%s%s%s",
                 KEEL_SQL_QUERY_VIEW_RULES, SCHEMA_FILTER_CLAUSE, in_clause);
    if (n > 0 && (size_t)n < sizeof(sql_buf)) {
        ctx.query_type = 'v';
        err = conn->query(conn, sql_buf, process_view_row, &ctx);
        if (err != KEEL_OK) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                "Schema-filtered view query failed: %d", err);
        }
    }

    n = snprintf(sql_buf, sizeof(sql_buf), "%s%s%s",
                 KEEL_SQL_QUERY_TRIGGERS, SCHEMA_FILTER_CLAUSE, in_clause);
    if (n > 0 && (size_t)n < sizeof(sql_buf)) {
        ctx.query_type = 't';
        err = conn->query(conn, sql_buf, process_trigger_row, &ctx);
        if (err != KEEL_OK) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                "Schema-filtered trigger query failed: %d", err);
        }
    }

    n = snprintf(sql_buf, sizeof(sql_buf), "%s%s%s",
                 KEEL_SQL_QUERY_MATVIEWS, SCHEMA_FILTER_CLAUSE, in_clause);
    if (n > 0 && (size_t)n < sizeof(sql_buf)) {
        ctx.query_type = 'm';
        err = conn->query(conn, sql_buf, process_matview_row, &ctx);
        if (err != KEEL_OK) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                "Schema-filtered matview query failed: %d", err);
        }
    }

#undef SCHEMA_FILTER_CLAUSE

    cache->stats.refreshes++;
    pthread_rwlock_unlock(&cache->lock);

    return KEEL_OK;
}

/**
 * @brief Insert one object into the cache under write lock.
 */
keel_error_t keel_metadata_cache_add(
    keel_metadata_cache_t* cache,
    const keel_cached_object_t* object
) {
    if (!cache || !object || !object->name) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    pthread_rwlock_wrlock(&cache->lock);
    keel_error_t err = cache_insert(cache, object);
    pthread_rwlock_unlock(&cache->lock);
    
    return err;
}

/**
 * @brief Remove one object from the cache.
 */
keel_error_t keel_metadata_cache_remove(
    keel_metadata_cache_t* cache,
    const char* schema,
    const char* name,
    char type
) {
    if (!cache || !name) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    pthread_rwlock_wrlock(&cache->lock);
    
    uint64_t hash = hash_object_key(schema, name, type);
    size_t idx = hash % cache->capacity;
    
    keel_cached_object_t** pp = &cache->buckets[idx];
    while (*pp) {
        keel_cached_object_t* obj = *pp;
        
        bool schema_match = (schema == NULL && obj->schema == NULL) ||
                           (schema && obj->schema && strcmp(schema, obj->schema) == 0);
        
        if (schema_match && 
            strcmp(name, obj->name) == 0 &&
            (type == 0 || obj->type == type)) {
            *pp = obj->hash_next;
            cache->count--;
            cache->stats.total_objects--;
            
            pthread_rwlock_unlock(&cache->lock);
            return KEEL_OK;
        }
        
        pp = &obj->hash_next;
    }
    
    pthread_rwlock_unlock(&cache->lock);
    return KEEL_ERR_NOT_FOUND;
}

/* ============================================================================
 * Cache Lookup
 * ============================================================================ */

/**
 * @brief Look up one object and update cache hit/miss counters.
 */
bool keel_metadata_cache_lookup(
    keel_metadata_cache_t* cache,
    const char* schema,
    const char* name,
    char type,
    const keel_cached_object_t** out
) {
    if (!cache || !name) {
        return false;
    }
    
    pthread_rwlock_rdlock(&cache->lock);
    
    keel_cached_object_t* obj = cache_find(cache, schema, name, type);
    
    if (obj) {
        obj->hit_count++;
        ((keel_metadata_cache_t*)cache)->stats.cache_hits++;
        
        if (out) {
            *out = obj;
        }
        
        pthread_rwlock_unlock(&cache->lock);
        return true;
    }
    
    ((keel_metadata_cache_t*)cache)->stats.cache_misses++;
    pthread_rwlock_unlock(&cache->lock);
    return false;
}

/**
 * @brief Return the cached write classification for one object.
 */
keel_object_write_type_t keel_metadata_cache_check_write(
    keel_metadata_cache_t* cache,
    const char* schema,
    const char* name,
    char type
) {
    const keel_cached_object_t* obj;
    if (keel_metadata_cache_lookup(cache, schema, name, type, &obj)) {
        return obj->write_type;
    }
    return KEEL_OBJ_WRITE_NONE;
}

/**
 * @brief Check a batch of object references for any write-sensitive object.
 */
keel_error_t keel_metadata_cache_check_batch(
    keel_metadata_cache_t* cache,
    const char** schemas,
    const char** names,
    const char* types,
    size_t count,
    bool* has_write
) {
    if (!cache || !names || !has_write) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    *has_write = false;
    
    pthread_rwlock_rdlock(&cache->lock);
    
    for (size_t i = 0; i < count; i++) {
        const char* schema = schemas ? schemas[i] : NULL;
        char type = types ? types[i] : 0;
        
        keel_cached_object_t* obj = cache_find(cache, schema, names[i], type);
        if (obj && obj->write_type != KEEL_OBJ_WRITE_NONE) {
            *has_write = true;
            break;
        }
    }
    
    pthread_rwlock_unlock(&cache->lock);
    return KEEL_OK;
}

/* ============================================================================
 * Query Analysis
 * ============================================================================ */

/**
 * @brief Analyze a parsed query tree against cached metadata.
 *
 * @return `KEEL_OK` on success, or an error for invalid input.
 */
keel_error_t keel_metadata_analyze_query(
    keel_metadata_cache_t* cache,
    const keel_qt_query_t* qt,
    bool* has_write_function,
    bool* has_write_trigger,
    bool* has_write_rule,
    bool* needs_primary
) {
    if (!cache || !qt) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    if (has_write_function) *has_write_function = false;
    if (has_write_trigger) *has_write_trigger = false;
    if (has_write_rule) *has_write_rule = false;
    if (needs_primary) *needs_primary = false;
    
    pthread_rwlock_rdlock(&cache->lock);
    
    /* Check functions referenced by this query against the metadata cache.
     * Any VOLATILE or SECURITY DEFINER function forces primary routing because
     * it may perform writes or bypass row-level security.  Functions that return
     * TRIGGER are also flagged for the same reason. */
    keel_qt_func_ref_t* fn = qt->functions;
    for (size_t i = 0; i < qt->func_count && fn; i++, fn = fn->next) {
        char schema_buf[128] = "";
        char name_buf[256] = "";

        if (fn->schema.len > 0 && fn->schema.len < sizeof(schema_buf)) {
            memcpy(schema_buf, fn->schema.data, fn->schema.len);
            schema_buf[fn->schema.len] = '\0';
        }
        if (fn->name.len > 0 && fn->name.len < sizeof(name_buf)) {
            memcpy(name_buf, fn->name.data, fn->name.len);
            name_buf[fn->name.len] = '\0';
        }

        const char* schema = schema_buf[0] ? schema_buf : "public";

        keel_cached_object_t* obj = cache_find(cache, schema, name_buf, 'f');
        if (!obj && schema_buf[0]) {
            /* Fallback: try unqualified lookup in 'public' schema */
            obj = cache_find(cache, "public", name_buf, 'f');
        }

        if (obj) {
            bool write_unsafe = obj->is_volatile ||
                                obj->is_security_definer ||
                                obj->returns_trigger;
            if (write_unsafe) {
                if (has_write_function) *has_write_function = true;
                if (needs_primary)     *needs_primary = true;
            }
        }
    }

    /* Check tables in query */
    keel_qt_table_ref_t* tbl = qt->tables;
    for (size_t i = 0; i < qt->table_count && tbl; i++, tbl = tbl->next) {
        /* Convert keel_str_t to C strings for lookup */
        char schema_buf[128] = "";
        char table_buf[128] = "";
        
        if (tbl->schema.len > 0 && tbl->schema.len < sizeof(schema_buf)) {
            memcpy(schema_buf, tbl->schema.data, tbl->schema.len);
            schema_buf[tbl->schema.len] = '\0';
        }
        if (tbl->table.len > 0 && tbl->table.len < sizeof(table_buf)) {
            memcpy(table_buf, tbl->table.data, tbl->table.len);
            table_buf[tbl->table.len] = '\0';
        }
        
        /* Use public schema if not specified */
        const char* schema = schema_buf[0] ? schema_buf : "public";
        
        /* Check for table triggers */
        keel_cached_object_t* obj = cache_find(cache, schema, table_buf, 'r');
        if (obj && obj->write_type == KEEL_OBJ_WRITE_TRIGGER) {
            /* Only triggers on modified tables matter for reads */
            if (qt->operation != KEEL_QT_OP_READ) {
                if (has_write_trigger) *has_write_trigger = true;
                if (needs_primary) *needs_primary = true;
            }
        }
        
        /* Check for view with rules */
        obj = cache_find(cache, schema, table_buf, 'v');
        if (obj && obj->write_type == KEEL_OBJ_WRITE_RULE) {
            if (has_write_rule) *has_write_rule = true;
            if (needs_primary) *needs_primary = true;
        }
    }
    
    pthread_rwlock_unlock(&cache->lock);
    return KEEL_OK;
}

/**
 * @brief Materialize cached metadata records for objects referenced by a query tree.
 *
 * @return Number of objects written to `objects`.
 */
size_t keel_metadata_get_query_objects(
    keel_metadata_cache_t* cache,
    const keel_qt_query_t* qt,
    keel_object_meta_t* objects,
    size_t max_objects
) {
    if (!cache || !qt || !objects || max_objects == 0) {
        return 0;
    }
    
    size_t count = 0;
    
    pthread_rwlock_rdlock(&cache->lock);
    
    /* Add tables from query tree */
    keel_qt_table_ref_t* tbl = qt->tables;
    for (size_t i = 0; i < qt->table_count && tbl && count < max_objects; i++, tbl = tbl->next) {
        /* Convert keel_str_t to C strings for lookup */
        char schema_buf[128] = "";
        char table_buf[128] = "";
        
        if (tbl->schema.len > 0 && tbl->schema.len < sizeof(schema_buf)) {
            memcpy(schema_buf, tbl->schema.data, tbl->schema.len);
            schema_buf[tbl->schema.len] = '\0';
        }
        if (tbl->table.len > 0 && tbl->table.len < sizeof(table_buf)) {
            memcpy(table_buf, tbl->table.data, tbl->table.len);
            table_buf[tbl->table.len] = '\0';
        }
        
        /* Use public schema if not specified */
        const char* schema = schema_buf[0] ? schema_buf : "public";
        
        keel_cached_object_t* obj = cache_find(cache, schema, table_buf, 0);
        if (obj) {
            objects[count++] = (keel_object_meta_t){
                .schema = obj->schema,
                .name = obj->name,
                .type = obj->type,
                .write_type = obj->write_type,
                .is_security_definer = obj->is_security_definer,
                .returns_trigger = obj->returns_trigger,
            };
        }
    }
    
    /* Note: Function tracking is not currently supported in the Query Tree.
     * To add function metadata, extend keel_qt_query_t or parse SQL text.
     */
    
    pthread_rwlock_unlock(&cache->lock);
    return count;
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Copy metadata-cache statistics under read lock.
 *
 * @param cache Cache handle.
 * @param[out] stats Destination stats structure.
 * @return
 */
void keel_metadata_cache_get_stats(
    const keel_metadata_cache_t* cache,
    keel_metadata_stats_t* stats
) {
    if (!cache || !stats) return;
    
    pthread_rwlock_rdlock((pthread_rwlock_t*)&cache->lock);
    *stats = cache->stats;
    pthread_rwlock_unlock((pthread_rwlock_t*)&cache->lock);
}

/**
 * @brief Dump a human-readable metadata cache summary.
 *
 * @param cache Cache handle.
 * @param out Destination stream.
 * @return
 */
void keel_metadata_cache_dump(
    const keel_metadata_cache_t* cache,
    FILE* out
) {
    if (!cache || !out) return;
    
    pthread_rwlock_rdlock((pthread_rwlock_t*)&cache->lock);
    
    fprintf(out, "=== Metadata Cache: %s ===\n", cache->database);
    fprintf(out, "Objects: %zu (capacity: %zu)\n", cache->count, cache->capacity);
    fprintf(out, "\nStatistics:\n");
    fprintf(out, "  Functions:     %zu (%zu write)\n", 
            cache->stats.functions, cache->stats.write_functions);
    fprintf(out, "  Views:         %zu (%zu with rules)\n",
            cache->stats.views, cache->stats.write_rules);
    fprintf(out, "  Matviews:      %zu\n", cache->stats.matviews);
    fprintf(out, "  Tables:        %zu (%zu with triggers)\n",
            cache->stats.tables, cache->stats.write_triggers);
    fprintf(out, "  Cache hits:    %lu\n", (unsigned long)cache->stats.cache_hits);
    fprintf(out, "  Cache misses:  %lu\n", (unsigned long)cache->stats.cache_misses);
    fprintf(out, "  Refreshes:     %lu\n", (unsigned long)cache->stats.refreshes);
    
    fprintf(out, "\nObjects:\n");
    for (size_t i = 0; i < cache->capacity; i++) {
        keel_cached_object_t* obj = cache->buckets[i];
        while (obj) {
            const char* type_str = "?";
            switch (obj->type) {
                case 'f': type_str = "func"; break;
                case 'v': type_str = "view"; break;
                case 'm': type_str = "matv"; break;
                case 'r': type_str = "tabl"; break;
            }
            
            const char* write_str = "read";
            switch (obj->write_type) {
                case KEEL_OBJ_WRITE_ALWAYS: write_str = "WRITE"; break;
                case KEEL_OBJ_WRITE_POSSIBLE: write_str = "maybe"; break;
                case KEEL_OBJ_WRITE_TRIGGER: write_str = "trig"; break;
                case KEEL_OBJ_WRITE_RULE: write_str = "rule"; break;
                case KEEL_OBJ_WRITE_MATVIEW: write_str = "matv"; break;
                default: break;
            }
            
            fprintf(out, "  [%s] %s.%s (%s)\n",
                    type_str,
                    obj->schema ? obj->schema : "public",
                    obj->name,
                    write_str);
            
            obj = obj->hash_next;
        }
    }
    
    pthread_rwlock_unlock((pthread_rwlock_t*)&cache->lock);
}
