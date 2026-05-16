/**
 * @file query_cache.h
 * @brief Query result caching for elimination of repeated backend round-trips.
 *
 * Provides an in-process LRU cache for SELECT query results, keyed by
 * normalized query digest (SHA-256). Supports per-rule TTL configuration,
 * automatic eviction on expiry, and table-level invalidation on writes.
 *
 * Cache is embedded in backend pools and shared across all connections to
 * that pool. Access is thread-safe via RWlock.
 *
 * @author KEEL Development Team
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_QUERY_CACHE_H
#define KEEL_QUERY_CACHE_H

#include "keel_types.h"
#include "keel_error.h"

#include <time.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Query Cache Statistics
 * ============================================================================ */

/**
 * @brief Query cache statistics snapshot.
 */
typedef struct keel_query_cache_stats {
    uint64_t hits;                  /**< Total cache hits */
    uint64_t misses;                /**< Total cache misses */
    uint64_t evictions;             /**< Entries evicted (size limit) */
    uint64_t expirations;           /**< Entries expired (TTL) */
    size_t   entries_count;         /**< Current entry count */
    size_t   memory_used_bytes;     /**< Current memory usage */
    size_t   memory_max_bytes;      /**< Maximum configured memory */
} keel_query_cache_stats_t;

/**
 * @brief Query cache instance (embedded in backend pool).
 *
 * This is an opaque type. Use keel_query_cache_* functions to interact.
 */
typedef struct keel_query_cache keel_query_cache_t;

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Create a new query cache instance.
 *
 * @param cache_out         Allocated cache instance (output)
 * @param default_ttl_ms    Default TTL in milliseconds
 * @param max_size_mb       Maximum cache size in megabytes (0 = unlimited)
 * @return                  KEEL_OK or error code
 */
keel_error_t keel_query_cache_create(keel_query_cache_t** cache_out,
                                     int default_ttl_ms,
                                     size_t max_size_mb);

/**
 * @brief Destroy a query cache instance.
 *
 * @param cache  Cache to destroy (NULL-safe)
 */
void keel_query_cache_destroy(keel_query_cache_t* cache);

/**
 * @brief Get a cached query result (if available and not expired).
 *
 * Returns the cached result if found. The returned pointer is valid until
 * the next cache operation (get, put, invalidate, flush).
 *
 * @param cache         Query cache
 * @param digest        SHA-256 digest of normalized query (32 bytes)
 * @param result_out    Cached result bytes (output, do NOT free)
 * @param result_len    Result length in bytes (output)
 * @return              KEEL_OK on hit, KEEL_CACHE_MISS on miss, error otherwise
 */
keel_error_t keel_query_cache_get(keel_query_cache_t* cache,
                                  const uint8_t digest[32],
                                  const uint8_t** result_out,
                                  size_t* result_len);

/**
 * @brief Store a query result in the cache.
 *
 * Result data is copied into the cache. If cache is full, LRU entry is
 * evicted. If result is larger than max_size_mb, insertion fails.
 *
 * @param cache       Query cache
 * @param digest      SHA-256 digest of normalized query (32 bytes)
 * @param result      Result bytes to cache (will be copied)
 * @param result_len  Result length
 * @param ttl_ms      Time-to-live in milliseconds (0 = use default)
 * @return            KEEL_OK or error code
 */
keel_error_t keel_query_cache_put(keel_query_cache_t* cache,
                                  const uint8_t digest[32],
                                  const uint8_t* result,
                                  size_t result_len,
                                  int ttl_ms);

/**
 * @brief Mark a cache entry as expired (force refresh on next access).
 *
 * Used to trigger refresh without invalidating completely. On next get(),
 * entry will be treated as expired and evicted.
 *
 * @param cache   Query cache
 * @param digest  SHA-256 digest of normalized query (32 bytes)
 * @return        KEEL_OK or KEEL_CACHE_MISS if entry not found
 */
keel_error_t keel_query_cache_expire(keel_query_cache_t* cache,
                                     const uint8_t digest[32]);

/**
 * @brief Invalidate all cache entries for a table.
 *
 * Called on INSERT/UPDATE/DELETE to maintain cache coherency. All entries
 * that read from the specified table are evicted immediately.
 *
 * @param cache  Query cache
 * @param table  Table name (case-insensitive)
 * @return       KEEL_OK or error code
 */
keel_error_t keel_query_cache_invalidate_table(keel_query_cache_t* cache,
                                               const char* table);

/**
 * @brief Flush all cached query results.
 *
 * Clears the cache completely. Used on deployment, configuration reload,
 * or manual admin command.
 *
 * @param cache  Query cache (NULL-safe)
 * @return       KEEL_OK or error code
 */
keel_error_t keel_query_cache_flush(keel_query_cache_t* cache);

/**
 * @brief Get current cache statistics.
 *
 * Snapshot statistics without taking exclusive lock. Numbers are
 * approximate in concurrent scenarios.
 *
 * @param cache      Query cache
 * @param stats_out  Statistics (allocated by caller)
 * @return           KEEL_OK or error code
 */
keel_error_t keel_query_cache_stats(keel_query_cache_t* cache,
                                    keel_query_cache_stats_t* stats_out);

/* ============================================================================
 * Query Digest Computation
 * ============================================================================ */

/**
 * @brief Compute SHA-256 digest of normalized query.
 *
 * Normalizes the query text (collapse whitespace, case-fold keywords,
 * replace literals with ?) and computes SHA-256 digest.
 *
 * Non-cacheable patterns (volatile functions, session state) are detected
 * and result in a unique digest that never matches the cache.
 *
 * @param query      Query text (NUL-terminated)
 * @param digest_out SHA-256 digest (output, 32 bytes)
 * @return           KEEL_OK if cacheable, KEEL_CACHE_NON_CACHEABLE if not,
 *                   error otherwise
 */
keel_error_t keel_query_cache_digest(const char* query,
                                     uint8_t digest_out[32]);

/**
 * @brief Check if a query is cacheable (without computing digest).
 *
 * Fast check for volatile functions, session state, and other
 * non-cacheable patterns.
 *
 * @param query  Query text (NUL-terminated)
 * @return       true if query is cacheable, false otherwise
 */
bool keel_query_cache_is_cacheable(const char* query);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_QUERY_CACHE_H */
