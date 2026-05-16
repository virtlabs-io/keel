/**
 * @file route_cache.h
 * @brief Worker-local L1 cache for query routing decisions.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Route classification can be expensive relative to a tiny read-only query, so
 * KEEL keeps a small per-worker cache that remembers previous query-to-route
 * decisions. The cache is intentionally narrow in scope:
 *
 * - it is thread-local, so lookups need no atomics or locks;
 * - it is bounded and uses short linear probe chains to keep hot-path latency
 *   predictable even under collisions;
 * - it stores only hash, length, and route class rather than the full query text,
 *   accepting a tiny collision risk in exchange for low memory footprint and no
 *   dynamic allocation.
 *
 * This is an optimization layer only. Callers must still ensure the session is in
 * a cache-eligible state, such as not being inside a transaction or carrying
 * unreleased session state that would invalidate a generic read route.
 */

#ifndef KEEL_ROUTE_CACHE_H
#define KEEL_ROUTE_CACHE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Constants
 * ============================================================================ */

/** Cache size — must be power of 2 for fast masking */
#define ROUTE_CACHE_SIZE        1024
#define ROUTE_CACHE_MASK        (ROUTE_CACHE_SIZE - 1)

/** Maximum probe distance before giving up */
#define ROUTE_CACHE_MAX_PROBE   8

/* ============================================================================
 * Cache Entry
 * ============================================================================ */

typedef struct route_cache_entry {
    uint64_t    query_hash;     /**< XXHash64 of the query text */
    uint32_t    query_len;      /**< Query length (for collision check) */
    uint8_t     route_type;     /**< Cached keel_route_type_t */
    uint8_t     _pad[3];
    uint64_t    timestamp;      /**< Monotonic tick when inserted */
} route_cache_entry_t;

_Static_assert(sizeof(route_cache_entry_t) == 24,
               "route_cache_entry_t should fit in 24 bytes (< 1 cache line)");

/* ============================================================================
 * Route Cache
 * ============================================================================ */

typedef struct route_cache {
    route_cache_entry_t entries[ROUTE_CACHE_SIZE];
    uint64_t            hits;       /**< Total cache hits */
    uint64_t            misses;     /**< Total cache misses */
    uint64_t            inserts;    /**< Total insertions */
    uint64_t            evictions;  /**< Total evictions */
    uint64_t            tick;       /**< Monotonic counter for LRU */
} route_cache_t;

/* ============================================================================
 * API
 * ============================================================================ */

/**
 * @brief Zero the route cache and reset all hit/miss statistics.
 *
 * @param cache Cache instance to initialize.
 * @return
 */
void route_cache_init(route_cache_t* cache);

/**
 * @brief Attempt to reuse a previously cached routing decision for a query.
 *
 * The lookup probes a bounded number of slots using the query hash as the start
 * index. A hit requires matching hash and query length; the cache does not retain
 * original query bytes, so this is a deliberate probabilistic optimization rather
 * than a perfect memoization table.
 *
 * @param cache Thread-local route cache.
 * @param query Query text used as the hash key.
 * @param query_len Query length in bytes.
 * @param route_out [out] Receives the cached route classification on hit.
 * @return `true` on cache hit, otherwise `false`.
 */
bool route_cache_lookup(route_cache_t* cache,
                        const char* query, size_t query_len,
                        uint8_t* route_out);

/**
 * @brief Insert or refresh a route-cache entry for a query.
 *
 * If the bounded probe chain finds an existing matching key, the entry is merely
 * refreshed. Otherwise the algorithm prefers an empty slot and falls back to
 * evicting the oldest entry seen in the probe window, approximating cheap local
 * LRU without global bookkeeping.
 *
 * @param cache Thread-local route cache.
 * @param query Query text used as the hash key.
 * @param query_len Query length in bytes.
 * @param route_type Route classification to cache.
 * @return
 */
void route_cache_insert(route_cache_t* cache,
                        const char* query, size_t query_len,
                        uint8_t route_type);

/**
 * @brief Invalidate all cached routing decisions while preserving counters.
 *
 * This is used when topology or routing policy changes make old decisions
 * unreliable. Statistics remain intact so operators can still inspect cache
 * effectiveness across invalidation events.
 *
 * @param cache Cache to flush.
 * @return
 */
void route_cache_flush(route_cache_t* cache);

/**
 * @brief Get cache statistics
 */
static inline double route_cache_hit_rate(const route_cache_t* cache)
{
    uint64_t total = cache->hits + cache->misses;
    return total > 0 ? (double)cache->hits / (double)total : 0.0;
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ROUTE_CACHE_H */
