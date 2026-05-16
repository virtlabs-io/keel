/**
 * @file query_cache.c
 * @brief Query result caching implementation.
 *
 * In-process LRU cache with:
 * - SHA-256 query digest keys
 * - Hash table with linear probing (XXHash64 for quick distribution)
 * - LRU eviction list (doubly-linked)
 * - Per-entry TTL and size tracking
 * - Table-level invalidation tracking
 * - Thread-safe access via RWlock
 *
 * @author KEEL Development Team
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/core/query_cache.h"
#include "keel/mem/mem.h"
#include "keel/util/xxhash.h"
#include <pthread.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#include <openssl/evp.h>

/* ============================================================================
 * Cache Entry
 * ============================================================================ */

typedef struct keel_query_cache_entry {
    uint8_t         digest[32];         /* SHA-256 key */
    uint8_t*        result;             /* Cached result bytes */
    size_t          result_len;         /* Result length */
    time_t          expires_at;         /* Expiry time (UNIX epoch) */
    _Atomic uint64_t last_accessed;     /* Access counter for LRU (atomic: updated under rdlock) */
    
    /* Table tracking for invalidation */
    char**          tables;             /* NULL-terminated table names */
    size_t          num_tables;
    
    bool            valid;              /* Entry is valid (not evicted) */
} keel_query_cache_entry_t;

/* ============================================================================
 * Query Cache Instance
 * ============================================================================ */

struct keel_query_cache {
    keel_query_cache_entry_t* entries;  /* Hash table */
    size_t                    capacity;  /* Allocated slots */
    size_t                    size;      /* Current entries */
    
    size_t                    memory_used;
    size_t                    memory_max;
    
    int                       default_ttl_ms;
    
    /* Statistics — _Atomic so hits/misses can be updated under rdlock
     * without races when multiple threads call keel_query_cache_get() */
    _Atomic uint64_t hits;
    _Atomic uint64_t misses;
    _Atomic uint64_t evictions;
    _Atomic uint64_t expirations;
    
    /* LRU tracking — incremented atomically on every cache hit */
    _Atomic uint64_t access_counter;
    
    /* Thread safety */
    pthread_rwlock_t          lock;
};

/* ============================================================================
 * Hash Table Operations
 * ============================================================================ */

#define QUERY_CACHE_INITIAL_CAPACITY 1024
#define QUERY_CACHE_LOAD_FACTOR 0.75

/**
 * @brief Compute the bucket index from a SHA-256 digest.
 *
 * Applies XXHash64 to the 32-byte digest to obtain a fast,
 * well-distributed 64-bit hash value.
 *
 * @param digest  32-byte SHA-256 digest of the query.
 * @return        64-bit hash value; caller reduces modulo capacity.
 */
static uint64_t query_cache_hash(const uint8_t digest[32]) {
    /* Use XXHash64 on the digest */
    return keel_xxh64(digest, 32, 0);
}

/**
 * @brief Find the slot for a digest using linear probing.
 *
 * Scans the hash table starting at the hashed slot and advances
 * until an empty slot or a matching entry is found.
 *
 * @param cache      Cache instance.
 * @param digest     32-byte SHA-256 digest to look up.
 * @param out_index  Set to the resolved slot index, or SIZE_MAX if the
 *                   table is full with no matching or empty slot.
 * @return           Number of probe steps taken.
 */
static size_t query_cache_probe(keel_query_cache_t* cache,
                                const uint8_t digest[32],
                                size_t* out_index) {
    uint64_t hash = query_cache_hash(digest);
    size_t index = hash % cache->capacity;
    size_t probes = 0;

    /* Linear probing */
    while (probes < cache->capacity) {
        keel_query_cache_entry_t* entry = &cache->entries[index];

        if (!entry->valid || memcmp(entry->digest, digest, 32) == 0) {
            *out_index = index;
            return probes;
        }

        index = (index + 1) % cache->capacity;
        probes++;
    }

    /* Table full */
    *out_index = SIZE_MAX;
    return probes;
}

/* ============================================================================
 * Eviction (LRU)
 * ============================================================================ */

/**
 * @brief Evict the least-recently-used entry from the cache.
 *
 * Scans all valid entries and removes the one with the smallest
 * @c last_accessed counter.  Memory used by the evicted entry is
 * freed and the eviction counter is incremented.
 *
 * @param cache  Cache instance (caller must hold the write-lock).
 */
static void query_cache_evict_lru(keel_query_cache_t* cache) {
    if (cache->size == 0) return;

    /* Find oldest entry (minimum access_counter among valid entries) */
    size_t oldest_idx = SIZE_MAX;
    uint64_t oldest_counter = UINT64_MAX;

    for (size_t i = 0; i < cache->capacity; i++) {
        keel_query_cache_entry_t* entry = &cache->entries[i];
        if (entry->valid && atomic_load(&entry->last_accessed) < oldest_counter) {
            oldest_idx = i;
            oldest_counter = atomic_load(&entry->last_accessed);
        }
    }

    if (oldest_idx != SIZE_MAX) {
        keel_query_cache_entry_t* entry = &cache->entries[oldest_idx];
        cache->memory_used -= entry->result_len;

        keel_free(entry->result);
        for (size_t i = 0; i < entry->num_tables; i++) {
            keel_free(entry->tables[i]);
        }
        keel_free(entry->tables);

        entry->valid = false;
        cache->size--;
        atomic_fetch_add(&cache->evictions, 1);
    }
}

/* ============================================================================
 * Expiry Check
 * ============================================================================ */

/**
 * @brief Check whether a cache entry has exceeded its TTL.
 *
 * @param entry  Entry to test.
 * @return       @c true if the current wall-clock time is at or past
 *               the entry's @c expires_at timestamp.
 */
static bool query_cache_is_expired(keel_query_cache_entry_t* entry) {
    return time(NULL) >= entry->expires_at;
}

/* ============================================================================
 * Resize
 * ============================================================================ */

/**
 * @brief Grow the hash table to a new capacity and rehash all entries.
 *
 * Allocates a fresh entry array of @p new_capacity slots, re-inserts
 * every currently valid entry, and releases the old array.  Entries
 * that cannot be re-inserted are freed rather than leaked.
 *
 * @param cache        Cache instance (caller must hold the write-lock).
 * @param new_capacity Number of slots in the new hash table.
 * @return             @c KEEL_OK on success, @c KEEL_ERR_NOMEM if the
 *                     new array cannot be allocated.
 */
static keel_error_t query_cache_resize(keel_query_cache_t* cache,
                                       size_t new_capacity) {
    keel_query_cache_entry_t* old_entries = cache->entries;
    size_t old_capacity = cache->capacity;

    cache->entries = keel_calloc(new_capacity,
                                  sizeof(keel_query_cache_entry_t));
    if (!cache->entries) {
        cache->entries = old_entries;
        return KEEL_ERR_NOMEM;
    }

    cache->capacity = new_capacity;
    size_t old_size = cache->size;
    cache->size = 0;

    /* Rehash all entries */
    for (size_t i = 0; i < old_capacity; i++) {
        if (!old_entries[i].valid) continue;

        size_t new_idx;
        query_cache_probe(cache, old_entries[i].digest, &new_idx);

        if (new_idx != SIZE_MAX) {
            cache->entries[new_idx] = old_entries[i];
            cache->size++;
        } else {
            /* Hash table full during resize (shouldn't happen) */
            keel_free(old_entries[i].result);
            for (size_t j = 0; j < old_entries[i].num_tables; j++) {
                keel_free(old_entries[i].tables[j]);
            }
            keel_free(old_entries[i].tables);
        }
    }

    keel_free(old_entries);
    return KEEL_OK;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Allocate and initialize a new query cache instance.
 *
 * Creates an in-process LRU cache backed by an open-addressing hash
 * table.  The cache is thread-safe via an internal read/write lock.
 *
 * @param cache_out      Output pointer that receives the new cache.
 * @param default_ttl_ms Default entry lifetime in milliseconds;
 *                       values ≤ 0 fall back to 3 000 ms.
 * @param max_size_mb    Maximum total result data held in memory (MiB);
 *                       0 disables the memory cap.
 * @return               @c KEEL_OK on success, @c KEEL_ERR_INVALID_ARG
 *                       if @p cache_out is NULL, or @c KEEL_ERR_NOMEM on
 *                       allocation failure.
 */
keel_error_t keel_query_cache_create(keel_query_cache_t** cache_out,
                                     int default_ttl_ms,
                                     size_t max_size_mb) {
    if (!cache_out) return KEEL_ERR_INVALID_ARG;

    keel_query_cache_t* cache = keel_calloc(1, sizeof(*cache));
    if (!cache) return KEEL_ERR_NOMEM;

    cache->entries = keel_calloc(QUERY_CACHE_INITIAL_CAPACITY,
                                  sizeof(keel_query_cache_entry_t));
    if (!cache->entries) {
        keel_free(cache);
        return KEEL_ERR_NOMEM;
    }

    cache->capacity = QUERY_CACHE_INITIAL_CAPACITY;
    cache->size = 0;
    cache->memory_used = 0;
    cache->memory_max = max_size_mb * 1024 * 1024;  /* Convert to bytes */
    cache->default_ttl_ms = (default_ttl_ms > 0) ? default_ttl_ms : 3000;
    cache->access_counter = 0;

    if (pthread_rwlock_init(&cache->lock, NULL) != 0) {
        keel_free(cache->entries);
        keel_free(cache);
        return KEEL_ERR_AUTH;  /* Use generic error */
    }

    *cache_out = cache;
    return KEEL_OK;
}

/**
 * @brief Destroy a cache and free all resources.
 *
 * Frees every cached result, table-name array, the internal hash table,
 * and the cache struct itself.  The read/write lock is destroyed after
 * all entries are released.  Safe to call with @p cache == NULL.
 *
 * @param cache  Cache instance to destroy.
 */
void keel_query_cache_destroy(keel_query_cache_t* cache) {
    if (!cache) return;

    pthread_rwlock_wrlock(&cache->lock);

    for (size_t i = 0; i < cache->capacity; i++) {
        if (cache->entries[i].valid) {
            keel_free(cache->entries[i].result);
            for (size_t j = 0; j < cache->entries[i].num_tables; j++) {
                keel_free(cache->entries[i].tables[j]);
            }
            keel_free(cache->entries[i].tables);
        }
    }

    keel_free(cache->entries);
    pthread_rwlock_unlock(&cache->lock);
    pthread_rwlock_destroy(&cache->lock);
    keel_free(cache);
}

/**
 * @brief Look up a query result by its SHA-256 digest.
 *
 * Acquires a read-lock, probes the hash table, and on a hit returns a
 * read-only pointer into the entry's result buffer.  The pointer is
 * valid only until the next write operation on the cache.
 *
 * @param cache       Cache instance.
 * @param digest      32-byte SHA-256 digest of the query.
 * @param result_out  Set to the cached result data on a hit.
 * @param result_len  Set to the result length in bytes on a hit.
 * @return            @c KEEL_OK on hit, @c KEEL_CACHE_MISS on miss or
 *                    expiry, @c KEEL_ERR_INVALID_ARG on bad arguments.
 */
keel_error_t keel_query_cache_get(keel_query_cache_t* cache,
                                  const uint8_t digest[32],
                                  const uint8_t** result_out,
                                  size_t* result_len) {
    if (!cache || !digest || !result_out || !result_len) {
        return KEEL_ERR_INVALID_ARG;
    }

    pthread_rwlock_rdlock(&cache->lock);

    size_t index;
    query_cache_probe(cache, digest, &index);

    if (index == SIZE_MAX) {
        atomic_fetch_add(&cache->misses, 1);
        pthread_rwlock_unlock(&cache->lock);
        return KEEL_CACHE_MISS;
    }

    keel_query_cache_entry_t* entry = &cache->entries[index];

    if (!entry->valid || query_cache_is_expired(entry)) {
        atomic_fetch_add(&cache->misses, 1);
        if (!entry->valid) {
            atomic_fetch_add(&cache->expirations, 1);
        }
        pthread_rwlock_unlock(&cache->lock);
        return KEEL_CACHE_MISS;
    }

    atomic_fetch_add(&cache->hits, 1);
    atomic_store(&entry->last_accessed, atomic_fetch_add(&cache->access_counter, 1));

    *result_out = entry->result;
    *result_len = entry->result_len;

    pthread_rwlock_unlock(&cache->lock);
    return KEEL_OK;
}

/**
 * @brief Insert or replace a cache entry.
 *
 * Acquires a write-lock, evicts LRU entries if the memory cap would be
 * exceeded, resizes the hash table when the load factor threshold is
 * reached, then stores a copy of the result data.  An existing entry
 * with the same digest is silently replaced.
 *
 * @param cache       Cache instance.
 * @param digest      32-byte SHA-256 digest that identifies the entry.
 * @param result      Result bytes to cache (copied internally).
 * @param result_len  Length of @p result in bytes.
 * @param ttl_ms      Entry lifetime in milliseconds; values ≤ 0 use
 *                    the cache default.
 * @return            @c KEEL_OK on success, @c KEEL_ERR_INVALID_ARG on
 *                    bad arguments, or @c KEEL_ERR_NOMEM on allocation
 *                    failure or if the result exceeds the memory cap.
 */
keel_error_t keel_query_cache_put(keel_query_cache_t* cache,
                                  const uint8_t digest[32],
                                  const uint8_t* result,
                                  size_t result_len,
                                  int ttl_ms) {
    if (!cache || !digest || !result || result_len == 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    /* Check if result is too large */
    if (cache->memory_max > 0 && result_len > cache->memory_max) {
        return KEEL_ERR_NOMEM;
    }

    pthread_rwlock_wrlock(&cache->lock);

    /* Evict entries until there's space */
    while (cache->memory_used + result_len > cache->memory_max &&
           cache->memory_max > 0 && cache->size > 0) {
        query_cache_evict_lru(cache);
    }

    /* Check if we need to resize */
    if (cache->size >= (size_t)(cache->capacity * QUERY_CACHE_LOAD_FACTOR)) {
        size_t new_capacity = cache->capacity * 2;
        if (query_cache_resize(cache, new_capacity) != KEEL_OK) {
            pthread_rwlock_unlock(&cache->lock);
            return KEEL_ERR_NOMEM;
        }
    }

    size_t index;
    query_cache_probe(cache, digest, &index);

    if (index == SIZE_MAX) {
        pthread_rwlock_unlock(&cache->lock);
        return KEEL_ERR_NOMEM;
    }

    keel_query_cache_entry_t* entry = &cache->entries[index];

    /* If replacing an existing entry, subtract old size */
    if (entry->valid) {
        cache->memory_used -= entry->result_len;
        keel_free(entry->result);
        for (size_t i = 0; i < entry->num_tables; i++) {
            keel_free(entry->tables[i]);
        }
        keel_free(entry->tables);
    } else {
        /* New entry, increment count */
        cache->size++;
    }

    memcpy(entry->digest, digest, 32);
    entry->result = keel_malloc(result_len);
    if (!entry->result) {
        entry->valid = false;
        pthread_rwlock_unlock(&cache->lock);
        return KEEL_ERR_NOMEM;
    }

    memcpy(entry->result, result, result_len);
    entry->result_len = result_len;
    /* Convert milliseconds to seconds (ceiling division to preserve small TTLs) */
    int effective_ttl_ms = (ttl_ms > 0) ? ttl_ms : cache->default_ttl_ms;
    int ttl_sec = (effective_ttl_ms + 999) / 1000;  /* Ceiling division */
    entry->expires_at = time(NULL) + ttl_sec;
    atomic_store(&entry->last_accessed, atomic_fetch_add(&cache->access_counter, 1));
    entry->valid = true;
    entry->tables = NULL;
    entry->num_tables = 0;

    cache->memory_used += result_len;

    pthread_rwlock_unlock(&cache->lock);
    return KEEL_OK;
}

/**
 * @brief Immediately expire a single cache entry by its digest.
 *
 * Sets the entry's expiry timestamp to zero so the next
 * @c keel_query_cache_get call treats it as a miss.  The entry
 * remains in the table until it is evicted or the cache is flushed.
 *
 * @param cache   Cache instance.
 * @param digest  32-byte SHA-256 digest of the entry to expire.
 * @return        @c KEEL_OK on success, @c KEEL_CACHE_MISS if no
 *                matching valid entry exists, or
 *                @c KEEL_ERR_INVALID_ARG on bad arguments.
 */
keel_error_t keel_query_cache_expire(keel_query_cache_t* cache,
                                     const uint8_t digest[32]) {
    if (!cache || !digest) return KEEL_ERR_INVALID_ARG;

    pthread_rwlock_wrlock(&cache->lock);

    size_t index;
    query_cache_probe(cache, digest, &index);

    if (index == SIZE_MAX || !cache->entries[index].valid) {
        pthread_rwlock_unlock(&cache->lock);
        return KEEL_CACHE_MISS;
    }

    cache->entries[index].expires_at = 0;  /* Expired immediately */

    pthread_rwlock_unlock(&cache->lock);
    return KEEL_OK;
}

/**
 * @brief Invalidate all cache entries that reference a given table.
 *
 * Scans every valid entry and removes those whose table list contains
 * @p table (case-insensitive comparison).  The expirations counter is
 * incremented for each removed entry.
 *
 * @param cache  Cache instance.
 * @param table  Table name whose dependent entries should be removed.
 * @return       @c KEEL_OK on success, @c KEEL_ERR_INVALID_ARG if
 *               either argument is NULL.
 */
keel_error_t keel_query_cache_invalidate_table(keel_query_cache_t* cache,
                                               const char* table) {
    if (!cache || !table) return KEEL_ERR_INVALID_ARG;

    pthread_rwlock_wrlock(&cache->lock);

    for (size_t i = 0; i < cache->capacity; i++) {
        keel_query_cache_entry_t* entry = &cache->entries[i];
        if (!entry->valid) continue;

        /* Check if this entry references the table */
        for (size_t j = 0; j < entry->num_tables; j++) {
            if (strcasecmp(entry->tables[j], table) == 0) {
                /* Invalidate this entry */
                cache->memory_used -= entry->result_len;
                keel_free(entry->result);
                for (size_t k = 0; k < entry->num_tables; k++) {
                    keel_free(entry->tables[k]);
                }
                keel_free(entry->tables);
                entry->valid = false;
                cache->size--;
                atomic_fetch_add(&cache->expirations, 1);
                break;
            }
        }
    }

    pthread_rwlock_unlock(&cache->lock);
    return KEEL_OK;
}

/**
 * @brief Remove all entries from the cache.
 *
 * Frees every cached result and table-name array, then resets the
 * entry count and memory-used counter to zero.  The hash table
 * capacity is unchanged.  NULL-safe: a NULL @p cache is a no-op.
 *
 * @param cache  Cache instance to flush.
 * @return       @c KEEL_OK unconditionally.
 */
keel_error_t keel_query_cache_flush(keel_query_cache_t* cache) {
    if (!cache) return KEEL_OK;  /* NULL-safe */

    pthread_rwlock_wrlock(&cache->lock);

    for (size_t i = 0; i < cache->capacity; i++) {
        if (cache->entries[i].valid) {
            keel_free(cache->entries[i].result);
            for (size_t j = 0; j < cache->entries[i].num_tables; j++) {
                keel_free(cache->entries[i].tables[j]);
            }
            keel_free(cache->entries[i].tables);
            cache->entries[i].valid = false;
        }
    }

    cache->size = 0;
    cache->memory_used = 0;

    pthread_rwlock_unlock(&cache->lock);
    return KEEL_OK;
}

/**
 * @brief Read a consistent snapshot of cache statistics.
 *
 * Acquires a read-lock, atomically loads all counters, and populates
 * @p stats_out.  Does not modify any cache state.
 *
 * @param cache      Cache instance.
 * @param stats_out  Output structure to fill with current statistics.
 * @return           @c KEEL_OK on success, @c KEEL_ERR_INVALID_ARG if
 *                   either argument is NULL.
 */
keel_error_t keel_query_cache_stats(keel_query_cache_t* cache,
                                    keel_query_cache_stats_t* stats_out) {
    if (!cache || !stats_out) return KEEL_ERR_INVALID_ARG;

    pthread_rwlock_rdlock(&cache->lock);

    stats_out->hits = atomic_load(&cache->hits);
    stats_out->misses = atomic_load(&cache->misses);
    stats_out->evictions = atomic_load(&cache->evictions);
    stats_out->expirations = atomic_load(&cache->expirations);
    stats_out->entries_count = cache->size;
    stats_out->memory_used_bytes = cache->memory_used;
    stats_out->memory_max_bytes = cache->memory_max;

    pthread_rwlock_unlock(&cache->lock);
    return KEEL_OK;
}

/* ============================================================================
 * Query Normalization & Digest
 * ============================================================================ */

/**
 * @brief Compute SHA-256 digest using OpenSSL.
 */
static bool query_cache_sha256(const uint8_t* data, size_t data_len,
                               uint8_t out[32]) {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) return false;

    unsigned int out_len = 32;
    bool success = EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) &&
                   EVP_DigestUpdate(ctx, data, data_len) &&
                   EVP_DigestFinal_ex(ctx, out, &out_len);

    EVP_MD_CTX_free(ctx);
    return success;
}

/**
 * @brief Check whether a SQL query string references a volatile function.
 *
 * Performs case-insensitive substring searches for a hard-coded list of
 * function names and pseudo-columns known to return non-deterministic
 * values (e.g. @c NOW(), @c RANDOM(), @c SESSION_USER).
 *
 * @param query  NULL-terminated SQL query string.
 * @return       @c true if any volatile token is found; @c false otherwise.
 */
static bool query_has_volatile_function(const char* query) {
    const char* volatile_funcs[] = {
        "NOW()",
        "CURRENT_TIMESTAMP",
        "RANDOM()",
        "RAND()",
        "SESSION_USER",
        "CURRENT_USER",
        "UUID",
        NULL
    };

    for (int i = 0; volatile_funcs[i] != NULL; i++) {
        if (strcasestr(query, volatile_funcs[i]) != NULL) {
            return true;
        }
    }

    return false;
}

/**
 * @brief Decide whether a query is safe to cache.
 *
 * A query is considered cacheable only when it is a plain @c SELECT
 * that does not acquire row-level locks, does not reference volatile
 * functions, and does not use @c WITH or @c UNION constructs.
 *
 * @param query  NULL-terminated SQL query string.
 * @return       @c true if the query may be cached; @c false otherwise.
 */
bool keel_query_cache_is_cacheable(const char* query) {
    if (!query) return false;

    /* Non-cacheable: DML (INSERT, UPDATE, DELETE) */
    if (strncasecmp(query, "SELECT", 6) != 0) {
        return false;
    }

    /* Non-cacheable: contains FOR UPDATE/SHARE (locking reads) */
    if (strcasestr(query, "FOR UPDATE") || strcasestr(query, "FOR SHARE")) {
        return false;
    }

    /* Non-cacheable: contains volatile functions */
    if (query_has_volatile_function(query)) {
        return false;
    }

    /* Non-cacheable: contains CTE/subquery (conservative) */
    if (strcasestr(query, "WITH") || strcasestr(query, "UNION")) {
        return false;
    }

    return true;
}

/**
 * @brief Normalize a SQL query and compute its SHA-256 cache key.
 *
 * Verifies cacheability, collapses whitespace, converts keywords to
 * uppercase, strips line comments, then digests the normalized form
 * with SHA-256.
 *
 * @param query       NULL-terminated SQL query string.
 * @param digest_out  32-byte buffer that receives the SHA-256 digest.
 * @return            @c KEEL_OK on success,
 *                    @c KEEL_CACHE_NON_CACHEABLE if the query fails the
 *                    cacheability check, @c KEEL_ERR_INVALID_ARG on bad
 *                    arguments, or @c KEEL_ERR_UNKNOWN if SHA-256 fails.
 */
keel_error_t keel_query_cache_digest(const char* query,
                                     uint8_t digest_out[32]) {
    if (!query || !digest_out) return KEEL_ERR_INVALID_ARG;

    if (!keel_query_cache_is_cacheable(query)) {
        return KEEL_CACHE_NON_CACHEABLE;
    }

    /* Normalize query: collapse whitespace, lowercase keywords */
    char normalized[4096];
    size_t norm_len = 0;
    bool in_string = false;
    bool in_comment = false;
    bool prev_space = true;

    for (const char* p = query; *p && norm_len < sizeof(normalized) - 1; p++) {
        char c = *p;

        /* Handle string literals */
        if (c == '\'' && (p == query || *(p - 1) != '\\')) {
            in_string = !in_string;
            normalized[norm_len++] = c;
            prev_space = false;
            continue;
        }

        if (in_string) {
            normalized[norm_len++] = c;
            prev_space = false;
            continue;
        }

        /* Handle comments */
        if (c == '-' && *(p + 1) == '-') {
            while (*p && *p != '\n') p++;
            continue;
        }

        /* Whitespace: collapse multiple spaces to single space */
        if (isspace(c)) {
            if (!prev_space) {
                normalized[norm_len++] = ' ';
                prev_space = true;
            }
            continue;
        }

        /* Regular character */
        normalized[norm_len++] = toupper(c);
        prev_space = false;
    }

    normalized[norm_len] = '\0';

    /* Compute SHA-256 */
    if (!query_cache_sha256((const uint8_t*)normalized, norm_len, digest_out)) {
        return KEEL_ERR_UNKNOWN;
    }

    return KEEL_OK;
}
