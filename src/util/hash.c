/**
 * @file hash.c
 * @brief Small non-cryptographic hashes and container helpers.
 *
 * This file groups together three related pieces of infrastructure that share
 * the same design constraints: no external dependencies, predictable behavior,
 * and good enough performance for control-plane workloads.
 *
 * - FNV-1a provides a tiny baseline hash with minimal code size.
 * - MurmurHash3 provides better avalanche for structured keys when callers need
 *   more even distribution.
 * - The hashmap and hash-ring APIs provide lightweight containers/routing
 *   helpers for internal use where bringing in a richer generic container layer
 *   would be disproportionate.
 *
 * xxHash intentionally lives elsewhere because it serves a different niche:
 * longer inputs, streaming updates, and stable query fingerprinting.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel_types.h"
#include "keel/mem/mem.h"
#include "keel/util/util.h"
#include "keel_error.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ============================================================================
 * FNV-1a Hash (fast, good distribution)
 * ============================================================================ */

#define FNV1A_32_PRIME  0x01000193
#define FNV1A_32_OFFSET 0x811c9dc5

#define FNV1A_64_PRIME  0x00000100000001b3ULL
#define FNV1A_64_OFFSET 0xcbf29ce484222325ULL

/**
 * @brief Hash a byte range with 32-bit FNV-1a.
 *
 * FNV-1a is retained here because it is extremely small, branch-light, and
 * perfectly adequate for many short keys used in utility code.
 *
 * @param data Input bytes.
 * @param len Number of bytes to hash.
 * @return 32-bit digest.
 */
uint32_t keel_hash_fnv1a_32(const void* data, size_t len) {
    const uint8_t* bytes = (const uint8_t*)data;
    uint32_t hash = FNV1A_32_OFFSET;
    
    for (size_t i = 0; i < len; i++) {
        hash ^= bytes[i];
        hash *= FNV1A_32_PRIME;
    }
    
    return hash;
}

/**
 * @brief Hash a byte range with 64-bit FNV-1a.
 *
 * @param data Input bytes.
 * @param len Number of bytes to hash.
 * @return 64-bit digest.
 */
uint64_t keel_hash_fnv1a_64(const void* data, size_t len) {
    const uint8_t* bytes = (const uint8_t*)data;
    uint64_t hash = FNV1A_64_OFFSET;
    
    for (size_t i = 0; i < len; i++) {
        hash ^= bytes[i];
        hash *= FNV1A_64_PRIME;
    }
    
    return hash;
}

/**
 * @brief Hash a `keel_str_t` string with 32-bit FNV-1a.
 *
 * @param str Input string.
 * @return 32-bit digest.
 */
uint32_t keel_hash_fnv1a_str(keel_str_t str) {
    return keel_hash_fnv1a_32(str.data, str.len);
}

/* ============================================================================
 * MurmurHash3 32-bit (excellent distribution, fast)
 * ============================================================================ */

/**
 * @brief Rotate a 32-bit value left by @p r bits.
 *
 * @param x Value to rotate.
 * @param r Rotation count in bits.
 * @return Rotated value.
 */
static inline uint32_t rotl32(uint32_t x, int8_t r) {
    return (x << r) | (x >> (32 - r));
}

/**
 * @brief Hash a byte range with MurmurHash3 x86 32-bit.
 *
 * The implementation follows the standard body/tail/finalization structure.
 * `memcpy()` is used for block loads to avoid alignment assumptions while still
 * letting modern compilers optimize into efficient machine code.
 *
 * @param data Input bytes.
 * @param len Number of bytes to hash.
 * @param seed Seed used to separate hash domains.
 * @return 32-bit digest.
 */
uint32_t keel_hash_murmur3_32(const void* data, size_t len, uint32_t seed) {
    const uint8_t* bytes = (const uint8_t*)data;
    const int nblocks = (int)(len / 4);
    
    uint32_t h1 = seed;
    
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;
    
    /* Body */
    const uint32_t* blocks = (const uint32_t*)(bytes + (size_t)nblocks * 4);
    
    for (int i = -nblocks; i; i++) {
        uint32_t k1;
        memcpy(&k1, &blocks[i], sizeof(k1));
        
        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;
        
        h1 ^= k1;
        h1 = rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
    }
    
    /* Tail */
    const uint8_t* tail = (const uint8_t*)(bytes + (size_t)nblocks * 4);
    uint32_t k1 = 0;
    
    switch (len & 3) {
        case 3: k1 ^= (uint32_t)tail[2] << 16; /* fallthrough */
        case 2: k1 ^= (uint32_t)tail[1] << 8;  /* fallthrough */
        case 1: k1 ^= tail[0];
                k1 *= c1;
                k1 = rotl32(k1, 15);
                k1 *= c2;
                h1 ^= k1;
    }
    
    /* Finalization */
    h1 ^= (uint32_t)len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;
    
    return h1;
}

/* ============================================================================
 * Consistent Hash Ring (moved from hashring.c)
 * ============================================================================ */

struct keel_hash_ring {
    size_t virtual_nodes;
    char** node_ids;
    size_t node_count;
    size_t node_capacity;
};

/**
 * @brief Allocate a simplified hash-ring object.
 *
 * The structure keeps a dynamically sized array of node identifiers. The
 * `virtual_nodes` field is part of the public shape the routing layer wants,
 * even though the current implementation has not yet expanded to a true sorted
 * virtual-node ring.
 *
 * @param virtual_nodes Preferred virtual-node count, or zero for the default.
 * @return Newly allocated ring, or `NULL` on allocation failure.
 */
keel_hash_ring_t* keel_hash_ring_new(size_t virtual_nodes) {
    keel_hash_ring_t* ring = keel_calloc(1, sizeof(keel_hash_ring_t));
    if (!ring) return NULL;
    
    ring->virtual_nodes = virtual_nodes > 0 ? virtual_nodes : 150;
    ring->node_capacity = 16;
    ring->node_ids = keel_calloc(ring->node_capacity, sizeof(char*));
    if (!ring->node_ids) {
        keel_free(ring);
        return NULL;
    }
    
    return ring;
}

/**
 * @brief Release all resources owned by a hash ring.
 *
 * Frees each node identifier string and then the ring object itself.
 * Passing `NULL` is safe.
 *
 * @param ring Ring to free, or `NULL`.
 */
void keel_hash_ring_free(keel_hash_ring_t* ring) {
    if (!ring) return;
    
    for (size_t i = 0; i < ring->node_count; i++) {
        keel_free(ring->node_ids[i]);
    }
    keel_free(ring->node_ids);
    keel_free(ring);
}

/**
 * @brief Copy a node identifier into the ring's node list.
 *
 * Nodes are stored as owned strings so callers can pass transient input
 * buffers. Capacity grows geometrically to keep append amortized $O(1)$.
 *
 * @param ring Ring to modify.
 * @param node_id Node identifier bytes.
 * @param len Identifier length.
 * @return `KEEL_OK` on success or an error code on invalid input/allocation
 *         failure.
 */
keel_error_t keel_hash_ring_add(keel_hash_ring_t* ring, const char* node_id, size_t len) {
    if (!ring || !node_id) return KEEL_ERR_INVALID_ARG;
    
    if (ring->node_count >= ring->node_capacity) {
        size_t new_cap = ring->node_capacity * 2;
        char** new_ids = keel_realloc(ring->node_ids, new_cap * sizeof(char*));
        if (!new_ids) return KEEL_ERR_NOMEM;
        ring->node_ids = new_ids;
        ring->node_capacity = new_cap;
    }
    
    char* id_copy = keel_malloc(len + 1);
    if (!id_copy) return KEEL_ERR_NOMEM;
    memcpy(id_copy, node_id, len);
    id_copy[len] = '\0';
    
    ring->node_ids[ring->node_count++] = id_copy;
    
    return KEEL_OK;
}

/**
 * @brief Remove a node from the ring by identifier.
 *
 * Performs a linear scan and compacts the array after a match.
 *
 * @param ring Ring to modify.
 * @param node_id NUL-terminated identifier of the node to remove.
 * @return `KEEL_OK` if removed, `KEEL_ERR_NOT_FOUND` if absent, or
 *         `KEEL_ERR_INVALID_ARG` on bad input.
 */
keel_error_t keel_hash_ring_remove(keel_hash_ring_t* ring, const char* node_id) {
    if (!ring || !node_id) return KEEL_ERR_INVALID_ARG;
    
    for (size_t i = 0; i < ring->node_count; i++) {
        if (strcmp(ring->node_ids[i], node_id) == 0) {
            keel_free(ring->node_ids[i]);
            /* Shift remaining elements */
            for (size_t j = i; j < ring->node_count - 1; j++) {
                ring->node_ids[j] = ring->node_ids[j + 1];
            }
            ring->node_count--;
            return KEEL_OK;
        }
    }
    
    return KEEL_ERR_NOT_FOUND;
}

/**
 * @brief Select a node for a key using the current simplified policy.
 *
 * Despite the API name, this is presently not a full consistent-hashing ring
 * with sorted points and wraparound lookup. It hashes the key and uses modulo
 * over the registered node count. The benefit is simplicity; the drawback is
 * that adding or removing a node can remap a larger share of keys than a true
 * consistent-hashing implementation would.
 *
 * @param ring Ring to query.
 * @param key Key bytes.
 * @param key_len Number of bytes in `key`.
 * @param[out] node_data Selected node identifier.
 * @param[out] node_len Length of the selected identifier.
 * @return `KEEL_OK` on success or an error code on invalid input/empty ring.
 */
keel_error_t keel_hash_ring_get(keel_hash_ring_t* ring, const char* key, size_t key_len,
                               const void** node_data, size_t* node_len) {
    if (!ring || !key || !node_data || !node_len) return KEEL_ERR_INVALID_ARG;
    
    if (ring->node_count == 0) return KEEL_ERR_NOT_FOUND;
    
    /* Use FNV-1a to select node */
    uint64_t hash = keel_hash_fnv1a_64(key, key_len);
    size_t idx = hash % ring->node_count;
    
    *node_data = ring->node_ids[idx];
    *node_len = strlen(ring->node_ids[idx]);
    
    return KEEL_OK;
}

/* ============================================================================
 * Generic Hash Table (matches util.h API)
 * ============================================================================ */

typedef struct keel_hashmap_entry {
    void*    key;
    void*    value;
    uint64_t hash;
    bool     occupied;
} keel_hashmap_entry_t;

struct keel_hashmap {
    keel_hashmap_entry_t* entries;
    size_t               size;
    size_t               capacity;
    size_t               mask;
    
    keel_hash_fn  hash_fn;
    keel_eq_fn    eq_fn;
};

/**
 * @brief Default hash function for opaque pointer keys.
 *
 * @param key Pointer to hash.
 * @return 64-bit hash of the pointer value itself.
 */
static uint64_t ptr_hash(const void* key) {
    return keel_hash_fnv1a_64(&key, sizeof(void*));
}

/**
 * @brief Default equality function for opaque pointer keys.
 *
 * @param a First pointer.
 * @param b Second pointer.
 * @return true when both pointers are identical.
 */
static bool ptr_eq(const void* a, const void* b) {
    return a == b;
}

/**
 * @brief Allocate a small open-addressed hashmap.
 *
 * The table uses power-of-two capacities plus a mask for cheap index wrapping.
 * Default callbacks treat keys as opaque pointers, which is sufficient for many
 * internal maps keyed by stable object addresses.
 *
 * @param hash_fn Hash function, or `NULL` for pointer hashing.
 * @param eq_fn Equality function, or `NULL` for pointer equality.
 * @return Newly allocated hashmap, or `NULL` on allocation failure.
 */
keel_hashmap_t* keel_hashmap_new(keel_hash_fn hash_fn, keel_eq_fn eq_fn) {
    keel_hashmap_t* map = keel_calloc(1, sizeof(keel_hashmap_t));
    if (!map) {
        return NULL;
    }
    
    map->capacity = 16;
    map->mask = map->capacity - 1;
    map->hash_fn = hash_fn ? hash_fn : ptr_hash;
    map->eq_fn = eq_fn ? eq_fn : ptr_eq;
    
    map->entries = keel_calloc(map->capacity, sizeof(keel_hashmap_entry_t));
    if (!map->entries) {
        keel_free(map);
        return NULL;
    }
    
    return map;
}

/**
 * @brief Release a hashmap and its backing entry array.
 *
 * Caller is responsible for freeing any heap memory pointed to by stored
 * keys or values before calling this function. Passing `NULL` is safe.
 *
 * @param map Map to free, or `NULL`.
 */
void keel_hashmap_free(keel_hashmap_t* map) {
    if (!map) {
        return;
    }
    keel_free(map->entries);
    keel_free(map);
}

/**
 * @brief Double the hashmap capacity and reinsert existing entries.
 *
 * Rehashing discards the old probe layout, which is cheaper than trying to
 * preserve clusters in place and keeps the implementation compact.
 *
 * @param map Table to resize.
 * @return true on success, false on allocation failure.
 */
static bool keel_hashmap_grow(keel_hashmap_t* map) {
    size_t old_cap = map->capacity;
    keel_hashmap_entry_t* old_entries = map->entries;
    
    size_t new_cap = old_cap * 2;
    keel_hashmap_entry_t* new_entries = keel_calloc(new_cap, sizeof(keel_hashmap_entry_t));
    if (!new_entries) {
        return false;
    }
    
    map->entries = new_entries;
    map->capacity = new_cap;
    map->mask = new_cap - 1;
    map->size = 0;
    
    /* Rehash all entries */
    for (size_t i = 0; i < old_cap; i++) {
        if (old_entries[i].occupied) {
            keel_hashmap_set(map, old_entries[i].key, old_entries[i].value);
        }
    }
    
    keel_free(old_entries);
    return true;
}

/**
 * @brief Insert or update one key/value pair in the hashmap.
 *
 * Linear probing keeps the table implementation small and friendly to cache
 * locality for the modest table sizes typically used here.
 *
 * @param map Table to modify.
 * @param key Key pointer.
 * @param value Value pointer.
 * @return true on success, false on invalid input or allocation failure.
 */
bool keel_hashmap_set(keel_hashmap_t* map, void* key, void* value) {
    if (!map) {
        return false;
    }
    
    /* Grow if load factor > 0.75 */
    if (map->size * 4 >= map->capacity * 3) {
        if (!keel_hashmap_grow(map)) {
            return false;
        }
    }
    
    uint64_t hash = map->hash_fn(key);
    size_t idx = (size_t)(hash & map->mask);
    
    /* Linear probing */
    while (map->entries[idx].occupied) {
        if (map->entries[idx].hash == hash &&
            map->eq_fn(map->entries[idx].key, key)) {
            /* Update existing */
            map->entries[idx].value = value;
            return true;
        }
        idx = (idx + 1) & map->mask;
    }
    
    /* Insert new */
    map->entries[idx].key = key;
    map->entries[idx].value = value;
    map->entries[idx].hash = hash;
    map->entries[idx].occupied = true;
    map->size++;
    
    return true;
}

/**
 * @brief Look up a value by key.
 *
 * @param map Table to query.
 * @param key Lookup key.
 * @return Stored value pointer, or `NULL` when the key is absent.
 */
void* keel_hashmap_get(const keel_hashmap_t* map, const void* key) {
    if (!map || map->size == 0) {
        return NULL;
    }
    
    uint64_t hash = map->hash_fn(key);
    size_t idx = (size_t)(hash & map->mask);
    size_t start = idx;
    
    do {
        if (!map->entries[idx].occupied) {
            return NULL;
        }
        if (map->entries[idx].hash == hash &&
            map->eq_fn(map->entries[idx].key, key)) {
            return map->entries[idx].value;
        }
        idx = (idx + 1) & map->mask;
    } while (idx != start);
    
    return NULL;
}

/**
 * @brief Remove one entry from the hashmap.
 *
 * The current implementation clears the matched slot directly instead of
 * maintaining a dedicated tombstone state. That keeps the structure simple but
 * also means this container is best suited to small internal workloads rather
 * than heavy churn with long-lived probe chains.
 *
 * @param map Table to modify.
 * @param key Key to remove.
 * @return true when an entry was removed, false otherwise.
 */
bool keel_hashmap_remove(keel_hashmap_t* map, const void* key) {
    if (!map || map->size == 0) {
        return false;
    }
    
    uint64_t hash = map->hash_fn(key);
    size_t idx = (size_t)(hash & map->mask);
    size_t start = idx;
    
    do {
        if (!map->entries[idx].occupied) {
            return false;
        }
        if (map->entries[idx].hash == hash &&
            map->eq_fn(map->entries[idx].key, key)) {
            /* Mark as deleted (tombstone) */
            map->entries[idx].occupied = false;
            map->entries[idx].key = NULL;
            map->entries[idx].value = NULL;
            map->size--;
            return true;
        }
        idx = (idx + 1) & map->mask;
    } while (idx != start);
    
    return false;
}

/**
 * @brief Return the number of entries currently stored in the hashmap.
 *
 * @param map Map to query, or `NULL`.
 * @return Entry count, or zero when `map` is `NULL`.
 */
size_t keel_hashmap_size(const keel_hashmap_t* map) {
    return map ? map->size : 0;
}

/**
 * @brief Remove all entries from a hashmap without releasing memory.
 *
 * The backing array is zeroed so the map can be reused without reallocation.
 * Stored key/value pointers are not freed.
 *
 * @param map Map to clear, or `NULL`.
 */
void keel_hashmap_clear(keel_hashmap_t* map) {
    if (!map) {
        return;
    }
    memset(map->entries, 0, map->capacity * sizeof(keel_hashmap_entry_t));
    map->size = 0;
}
