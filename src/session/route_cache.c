/**
 * @file route_cache.c
 * @brief Worker-local route-decision cache with bounded linear probing.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This module implements a tiny L1 cache for query classification results. It is
 * purposely simple: fixed-size storage, no heap allocation, no locking, and a
 * short probe budget. The aim is to accelerate repeat traffic patterns on a
 * worker without turning routing into its own complex subsystem.
 */

#include "keel/session/route_cache.h"
#include "keel/util/xxhash.h"

#include <string.h>

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Reset cache storage and counters.
 */
void route_cache_init(route_cache_t* cache)
{
    memset(cache, 0, sizeof(*cache));
}

/**
 * @brief Probe the cache for a prior route decision.
 *
 * The cache key is deliberately compact: XXHash64 of the query text plus the
 * query length. KEEL does not store the original query string, accepting the very
 * small possibility of a false hit caused by collision in exchange for fixed
 * memory cost and zero allocations.
 *
 * @param cache Cache to query.
 * @param query Query bytes.
 * @param query_len Query length in bytes.
 * @param route_out [out] Receives the cached route on hit.
 * @return `true` if an entry was found within the bounded probe window.
 */
bool route_cache_lookup(route_cache_t* cache,
                        const char* query, size_t query_len,
                        uint8_t* route_out)
{
    if (!cache || !query || query_len == 0) return false;

    uint64_t hash = keel_xxh64(query, query_len, 0xCA00CE01ULL);
    uint32_t idx  = (uint32_t)(hash & ROUTE_CACHE_MASK);

    for (int probe = 0; probe < ROUTE_CACHE_MAX_PROBE; probe++) {
        uint32_t slot = (idx + (uint32_t)probe) & ROUTE_CACHE_MASK;
        route_cache_entry_t* e = &cache->entries[slot];

        if (e->query_hash == 0) {
            /* Empty slot — miss */
            cache->misses++;
            return false;
        }

        if (e->query_hash == hash && e->query_len == (uint32_t)query_len) {
            /* Touch the entry on hit so future insertions within this local probe
             * window prefer evicting colder neighbors. */
            if (route_out) *route_out = e->route_type;
            e->timestamp = ++cache->tick;  /* touch for freshness */
            cache->hits++;
            return true;
        }
    }

    /* Exhausted probe chain — miss */
    cache->misses++;
    return false;
}

/**
 * @brief Insert or refresh a cached route decision.
 *
 * The insertion policy is intentionally local rather than globally optimal: it
 * searches only the bounded probe chain, preferring an empty slot and otherwise
 * evicting the oldest entry in that chain. This keeps insertion cost predictable
 * and consistent with the limited lookup budget.
 *
 * @param cache Cache to mutate.
 * @param query Query bytes.
 * @param query_len Query length in bytes.
 * @param route_type Route classification to store.
 * @return
 */
void route_cache_insert(route_cache_t* cache,
                        const char* query, size_t query_len,
                        uint8_t route_type)
{
    if (!cache || !query || query_len == 0) return;

    uint64_t hash = keel_xxh64(query, query_len, 0xCA00CE01ULL);
    uint32_t idx  = (uint32_t)(hash & ROUTE_CACHE_MASK);

    /* Find best slot: empty, or oldest within probe chain */
    uint32_t best_slot = idx;
    uint64_t oldest_ts = UINT64_MAX;

    for (int probe = 0; probe < ROUTE_CACHE_MAX_PROBE; probe++) {
        uint32_t slot = (idx + (uint32_t)probe) & ROUTE_CACHE_MASK;
        route_cache_entry_t* e = &cache->entries[slot];

        if (e->query_hash == 0) {
            /* Empty slot — use it */
            best_slot = slot;
            break;
        }

        if (e->query_hash == hash && e->query_len == (uint32_t)query_len) {
            /* Already cached — update */
            e->route_type = route_type;
            e->timestamp = ++cache->tick;
            return;
        }

        if (e->timestamp < oldest_ts) {
            oldest_ts = e->timestamp;
            best_slot = slot;
        }
    }

    /* Insert (possibly evicting oldest) */
    route_cache_entry_t* target = &cache->entries[best_slot];
    if (target->query_hash != 0) {
        cache->evictions++;
    }

    target->query_hash = hash;
    target->query_len  = (uint32_t)query_len;
    target->route_type = route_type;
    target->timestamp  = ++cache->tick;

    cache->inserts++;
}

/**
 * @brief Drop all cached entries while retaining lifetime counters.
 */
void route_cache_flush(route_cache_t* cache)
{
    if (!cache) return;
    memset(cache->entries, 0, sizeof(cache->entries));
    /* Keep statistics, but reset entries */
}
