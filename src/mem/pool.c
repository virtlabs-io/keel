/**
 * @file pool.c
 * @brief Fixed-size object-pool allocator with allocation-site auditing.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Pools target high-churn fixed-size objects where general-purpose malloc would
 * spend too much time in allocator bookkeeping. Each chunk contributes a batch of
 * slots threaded through a free list, giving O(1) allocation and return under the
 * caller's synchronization discipline. KEEL augments the classic pool design with
 * canaries and active-allocation lists so field debugging can still identify stale
 * or corrupted objects without abandoning the performance benefits of pooled reuse.
 */

#include "keel/mem/mem.h"
#include "keel/log/log.h"

#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

/** Default number of objects to allocate in initial chunk */
#define DEFAULT_INITIAL_COUNT 32

/** Minimum object size - must hold a pointer for free list linking */
#define MIN_OBJECT_SIZE sizeof(void*)

/** Pool block canaries */
#define KEEL_POOL_CANARY_HEAD 0xC001CAFEDEADBEEFULL
#define KEEL_POOL_CANARY_TAIL 0xF00DBAADCAFEBEEFULL

typedef enum pool_block_state {
    POOL_BLOCK_FREE = 0,
    POOL_BLOCK_ACTIVE = 1,
} pool_block_state_t;

typedef struct pool_audit_hdr {
    uint64_t                head_canary;
    const char*             alloc_file;
    int                     alloc_line;
    uint32_t                state;
    uint64_t                alloc_seq;
    struct pool_audit_hdr*  prev_active;
    struct pool_audit_hdr*  next_active;
} pool_audit_hdr_t;

/**
 * @brief Free list node (embedded in free objects)
 */
typedef struct keel_pool_node {
    struct keel_pool_node* next;
} keel_pool_node_t;

/**
 * @brief Pool chunk (block of objects)
 */
typedef struct keel_pool_chunk {
    struct keel_pool_chunk* next;    /**< Next chunk */
    size_t                 count;   /**< Objects in this chunk */
    uint8_t*               slots;   /**< Aligned object slot area */
    void*                  alloc_base; /**< Owning allocation pointer */
} keel_pool_chunk_t;

/**
 * @brief Pool allocator
 */
struct keel_pool {
    keel_pool_node_t*    free_list;      /**< Free object list */
    keel_pool_chunk_t*   chunks;         /**< All chunks */
    
    size_t              object_size;    /**< Size per object */
    size_t              object_align;   /**< Object alignment */
    size_t              user_offset;    /**< Raw-slot to user pointer offset */
    size_t              slot_size;      /**< Aligned slot size */
    size_t              initial_count;  /**< Initial objects per chunk */
    size_t              max_count;      /**< Maximum total objects (0=unlimited) */
    bool                zero_on_alloc;  /**< Zero memory on allocation */

    /* Pool audit metadata */
    uint64_t            alloc_seq;
    pool_audit_hdr_t*   active_head;
    size_t              active_audit_count;
    size_t              canary_failures;
    bool                audit_lock_init;
    pthread_mutex_t     audit_lock;
    struct keel_pool*   audit_next;
    
    /* Statistics */
    atomic_size_t       allocated;      /**< Currently allocated objects */
    atomic_size_t       available;      /**< Available objects in free list */
    atomic_size_t       total;          /**< Total objects in pool */
};

/* Global registry of pools for debug dumps */
static pthread_mutex_t g_pool_registry_lock = PTHREAD_MUTEX_INITIALIZER;
static keel_pool_t* g_pool_registry_head = NULL;

/**
 * @brief Recover the raw slot pointer from a user-visible object pointer.
 *
 * Subtracts the pool's `user_offset` (size of the audit header prepended
 * before the user area) to walk back to the start of the raw slot.
 *
 * @param pool     Pool the allocation belongs to.
 * @param user_ptr Pointer returned to the caller by a previous alloc.
 * @return Pointer to the first byte of the underlying raw slot.
 */
static inline uint8_t* pool_slot_raw_from_user(const keel_pool_t* pool, void* user_ptr) {
    return (uint8_t*)user_ptr - pool->user_offset;
}

/**
 * @brief Return the user-visible pointer for a raw slot.
 *
 * Adds `pool->user_offset` to skip past the `pool_audit_hdr_t` that
 * precedes every user region in the slot layout.
 *
 * @param pool Pool the slot belongs to.
 * @param raw  Pointer to the start of the raw slot.
 * @return User-visible pointer positioned after the audit header.
 */
static inline void* pool_user_from_slot_raw(const keel_pool_t* pool, uint8_t* raw) {
    return (void*)(raw + pool->user_offset);
}

/**
 * @brief Return the free-list node embedded inside a raw slot's user area.
 *
 * When a slot is on the free list, the first bytes of the user area hold a
 * `keel_pool_node_t` next-pointer.  This helper casts the user pointer to
 * that type.
 *
 * @param pool Pool the slot belongs to.
 * @param raw  Pointer to the start of the raw slot.
 * @return Pointer to the embedded `keel_pool_node_t`.
 */
static inline keel_pool_node_t* pool_node_from_slot_raw(const keel_pool_t* pool, uint8_t* raw) {
    return (keel_pool_node_t*)pool_user_from_slot_raw(pool, raw);
}

/**
 * @brief Return the raw slot pointer from a free-list node embedded in a slot.
 *
 * Inverse of `pool_node_from_slot_raw()`: the node lives at `user_offset`
 * inside the raw slot, so the raw start is `user_offset` bytes before `node`.
 *
 * @param pool Pool the slot belongs to.
 * @param node Pointer to the embedded `keel_pool_node_t`.
 * @return Pointer to the first byte of the underlying raw slot.
 */
static inline uint8_t* pool_slot_raw_from_node(const keel_pool_t* pool, keel_pool_node_t* node) {
    return pool_slot_raw_from_user(pool, (void*)node);
}

/**
 * @brief Return the audit header at the start of a raw slot.
 *
 * The `pool_audit_hdr_t` is placed at offset 0 of each raw slot, so this
 * is a simple pointer cast (via `void*` to suppress alignment warnings).
 *
 * @param raw Pointer to the first byte of a raw slot.
 * @return Pointer to the `pool_audit_hdr_t` embedded at the slot start.
 */
static inline pool_audit_hdr_t* pool_hdr_from_slot_raw(uint8_t* raw) {
    return (pool_audit_hdr_t*)(void*)raw;
}

/**
 * @brief Return the byte offset of the tail canary within a raw slot.
 *
 * The tail canary is written immediately after the user-data region:
 * `user_offset + object_size` bytes from the start of the raw slot.
 *
 * @param pool Pool whose slot layout is used.
 * @return Byte offset to the tail canary within any raw slot of this pool.
 */
static inline size_t pool_tail_offset(const keel_pool_t* pool) {
    return pool->user_offset + pool->object_size;
}

/**
 * @brief Write a 64-bit tail canary at the end of a raw slot's user region.
 *
 * Used at slot initialisation and on each allocation to detect buffer
 * overruns into the canary zone.  Copied via `memcpy` to avoid strict-
 * aliasing undefined behaviour on unaligned slot boundaries.
 *
 * @param pool Pool whose slot layout determines the canary offset.
 * @param raw  Pointer to the first byte of the raw slot.
 */
static inline void pool_write_tail_canary(const keel_pool_t* pool, uint8_t* raw) {
    uint64_t tail = KEEL_POOL_CANARY_TAIL;
    memcpy(raw + pool_tail_offset(pool), &tail, sizeof(tail));
}

/**
 * @brief Verify that the tail canary of a raw slot is intact.
 *
 * Reads the 64-bit value at the tail-canary offset via `memcpy` and
 * compares it to `KEEL_POOL_CANARY_TAIL`.
 *
 * @param pool Pool whose slot layout determines the canary offset.
 * @param raw  Pointer to the first byte of the raw slot.
 * @return `true` if the canary matches, `false` if overwritten.
 */
static inline bool pool_tail_canary_ok(const keel_pool_t* pool, const uint8_t* raw) {
    uint64_t tail = 0;
    memcpy(&tail, raw + pool_tail_offset(pool), sizeof(tail));
    return tail == KEEL_POOL_CANARY_TAIL;
}

/**
 * @brief Insert a pool into the process-wide pool registry.
 *
 * The registry is a singly-linked list protected by `g_pool_registry_lock`.
 * Registered pools are enumerated by `keel_pool_dump_active_allocations()`.
 *
 * @param pool Pool to register (must not already be in the list).
 */
static void pool_registry_add(keel_pool_t* pool) {
    pthread_mutex_lock(&g_pool_registry_lock);
    pool->audit_next = g_pool_registry_head;
    g_pool_registry_head = pool;
    pthread_mutex_unlock(&g_pool_registry_lock);
}

/**
 * @brief Remove a pool from the process-wide pool registry.
 *
 * Searches the registry list under `g_pool_registry_lock` and splices
 * the pool out.  Called by `keel_pool_destroy()` before freeing.
 *
 * @param pool Pool to deregister.
 */
static void pool_registry_remove(keel_pool_t* pool) {
    pthread_mutex_lock(&g_pool_registry_lock);
    keel_pool_t** prev = &g_pool_registry_head;
    while (*prev) {
        if (*prev == pool) {
            *prev = pool->audit_next;
            break;
        }
        prev = &(*prev)->audit_next;
    }
    pthread_mutex_unlock(&g_pool_registry_lock);
}

/**
 * @brief Dump all live pool allocations to stderr for leak investigation.
 *
 * Iterates every pool registered via `pool_registry_add()` and, for each
 * active slot, emits the allocation sequence number, pointer, and
 * call-site coordinates recorded by `keel_pool_alloc_debug()`.  Output is
 * truncated at 200 entries per pool.
 *
 * Notes:
 * - Acquires `g_pool_registry_lock` and each pool's `audit_lock`.
 * - Safe to call from a signal handler only if those mutexes are not already
 *   held by the interrupted thread; prefer calling from a quiesced context.
 */
void keel_pool_dump_active_allocations(void) {
    pthread_mutex_lock(&g_pool_registry_lock);

    fprintf(stderr, "\n=== KEEL Pool Active Allocation Dump ===\n");
    for (keel_pool_t* pool = g_pool_registry_head; pool != NULL; pool = pool->audit_next) {
        if (!pool->audit_lock_init) {
            continue;
        }

        pthread_mutex_lock(&pool->audit_lock);
        fprintf(stderr,
                "pool=%p object_size=%zu active=%zu canary_failures=%zu\n",
                (void*)pool,
                pool->object_size,
                pool->active_audit_count,
                pool->canary_failures);

        size_t shown = 0;
        for (pool_audit_hdr_t* h = pool->active_head; h != NULL; h = h->next_active) {
            if (shown >= 200) {
                fprintf(stderr, "  ... truncated after 200 active blocks\n");
                break;
            }
            fprintf(stderr,
                    "  #%llu ptr=%p alloc=%s:%d\n",
                    (unsigned long long)h->alloc_seq,
                    pool_user_from_slot_raw(pool, (uint8_t*)h),
                    h->alloc_file ? h->alloc_file : "<unknown>",
                    h->alloc_line);
            shown++;
        }
        pthread_mutex_unlock(&pool->audit_lock);
    }
    fprintf(stderr, "=== End Pool Dump ===\n\n");

    pthread_mutex_unlock(&g_pool_registry_lock);
}

/* ============================================================================
 * Chunk Management
 * ============================================================================ */

/**
 * @brief Create a new chunk with 'count' objects
 *
 * Allocates memory for the chunk header plus 'count' object slots.
 * All objects are immediately added to the pool's free list.
 *
 * Memory layout of a chunk:
 *   [keel_pool_chunk_t header]
 *   [object slot 0]
 *   [object slot 1]
 *   ...
 *   [object slot count-1]
 *
 * @param pool   Pool to add objects to
 * @param count  Number of objects in this chunk
 * @return New chunk, or NULL on allocation failure
 */
static keel_pool_chunk_t* pool_chunk_create(keel_pool_t* pool, size_t count) {
    size_t data_size = count * pool->slot_size;
    size_t align_slack = pool->object_align > 0 ? (pool->object_align - 1) : 0;
    size_t alloc_size = sizeof(keel_pool_chunk_t) + align_slack + data_size;
    void* alloc_base = keel_malloc(alloc_size);
    keel_pool_chunk_t* chunk = (keel_pool_chunk_t*)alloc_base;
    if (!chunk) {
        return NULL;
    }
    
    chunk->next = NULL;
    chunk->count = count;
    chunk->alloc_base = alloc_base;
    uintptr_t slots_base = (uintptr_t)(chunk + 1);
    uintptr_t aligned_slots = keel_align_up(slots_base, pool->object_align);
    chunk->slots = (uint8_t*)aligned_slots;
    
    /* Initialize free list within chunk */
    for (size_t i = 0; i < count; i++) {
        /* Use void* intermediate cast to avoid alignment warning - 
         * slot_size is already aligned in keel_pool_create() */
        void* slot = (void*)(chunk->slots + i * pool->slot_size);
        pool_audit_hdr_t* hdr = pool_hdr_from_slot_raw((uint8_t*)slot);
        hdr->head_canary = KEEL_POOL_CANARY_HEAD;
        hdr->alloc_file = NULL;
        hdr->alloc_line = 0;
        hdr->state = POOL_BLOCK_FREE;
        hdr->alloc_seq = 0;
        hdr->prev_active = NULL;
        hdr->next_active = NULL;
        pool_write_tail_canary(pool, (uint8_t*)slot);

        keel_pool_node_t* node = pool_node_from_slot_raw(pool, (uint8_t*)slot);
        node->next = pool->free_list;
        pool->free_list = node;
    }
    
    atomic_fetch_add(&pool->available, count);
    atomic_fetch_add(&pool->total, count);
    
    return chunk;
}

/**
 * @brief Destroy a chunk and free its memory
 *
 * @param chunk  Chunk to destroy
 */
static void pool_chunk_destroy(keel_pool_chunk_t* chunk) {
    keel_free(chunk->alloc_base);
}

/* ============================================================================
 * Pool Lifecycle
 * ============================================================================ */

/**
 * @brief Create a new fixed-size pool allocator
 *
 * Creates a pool for allocating objects of a specific size.
 * An initial chunk of objects is pre-allocated.
 *
 * Configuration options:
 *   - object_size:    Size of each object (required, > 0)
 *   - object_align:   Alignment requirement (0 = sizeof(void*))
 *   - initial_count:  Objects in initial chunk (0 = 32)
 *   - max_count:      Maximum objects allowed (0 = unlimited)
 *   - zero_on_alloc:  Whether to zero memory on allocation
 *
 * @code
 * keel_pool_config_t config = {
 *     .object_size = sizeof(my_struct_t),
 *     .initial_count = 100,
 *     .max_count = 1000
 * };
 * keel_pool_t* pool = keel_pool_create(&config);
 * @endcode
 *
 * @param config  Pool configuration (must have object_size > 0)
 * @return New pool, or NULL on failure
 */
keel_pool_t* keel_pool_create(const keel_pool_config_t* config) {
    if (!config || config->object_size == 0) {
        return NULL;
    }
    
    keel_pool_t* pool = keel_malloc(sizeof(keel_pool_t));
    if (!pool) {
        return NULL;
    }
    
    /* Calculate slot size with alignment */
    size_t obj_size = keel_max(config->object_size, MIN_OBJECT_SIZE);
    size_t align = config->object_align > 0 ? config->object_align : sizeof(void*);
    if (!keel_is_power_of_2(align)) {
        align = keel_next_power_of_2(align);
    }
    
    pool->object_size = config->object_size;
    pool->object_align = align;
    size_t audit_hdr_size = keel_align_up(sizeof(pool_audit_hdr_t), align);
    size_t raw_needed = audit_hdr_size + obj_size + sizeof(uint64_t);
    pool->user_offset = audit_hdr_size;
    pool->slot_size = keel_align_up(raw_needed, align);
    pool->initial_count = config->initial_count > 0 ? config->initial_count : DEFAULT_INITIAL_COUNT;
    pool->max_count = config->max_count;
    pool->zero_on_alloc = config->zero_on_alloc;
    
    pool->free_list = NULL;
    pool->chunks = NULL;
    pool->alloc_seq = 0;
    pool->active_head = NULL;
    pool->active_audit_count = 0;
    pool->canary_failures = 0;
    pool->audit_next = NULL;
    pool->audit_lock_init = (pthread_mutex_init(&pool->audit_lock, NULL) == 0);
    
    atomic_init(&pool->allocated, 0);
    atomic_init(&pool->available, 0);
    atomic_init(&pool->total, 0);
    
    /* Create initial chunk */
    keel_pool_chunk_t* chunk = pool_chunk_create(pool, pool->initial_count);
    if (!chunk) {
        keel_free(pool);
        return NULL;
    }
    
    pool->chunks = chunk;

    if (pool->audit_lock_init) {
        pool_registry_add(pool);
    }
    
    return pool;
}

/**
 * @brief Destroy a pool and free all its memory
 *
 * Releases all chunks and the pool structure itself.
 * All pointers obtained from this pool become invalid.
 *
 * @warning Do not call keel_pool_free() after this!
 *
 * @param pool  Pool to destroy (NULL is safe no-op)
 */
void keel_pool_destroy(keel_pool_t* pool) {
    if (!pool) {
        return;
    }
    
    if (pool->audit_lock_init) {
        pool_registry_remove(pool);
    }

    /* Free all chunks */
    keel_pool_chunk_t* chunk = pool->chunks;
    while (chunk) {
        keel_pool_chunk_t* next = chunk->next;
        pool_chunk_destroy(chunk);
        chunk = next;
    }
    
    if (pool->audit_lock_init) {
        pthread_mutex_destroy(&pool->audit_lock);
    }

    keel_free(pool);
}

/* ============================================================================
 * Allocation
 * ============================================================================ */

/**
 * @brief Allocate an object from the pool
 *
 * Returns a pointer to an object of the pool's configured size.
 * Operation is O(1) when objects are available in the free list.
 *
 * If the free list is empty, a new chunk is allocated (which may fail).
 * If max_count is set and reached, returns NULL.
 *
 * @code
 * connection_t* conn = keel_pool_alloc(conn_pool);
 * if (conn) {
 *     // Initialize and use connection
 * }
 * @endcode
 *
 * @param pool  Pool to allocate from
 * @return Pointer to object, or NULL if pool exhausted/error
 *
 * @note If zero_on_alloc was set, memory is zeroed before return
 */
/* mem.h maps keel_pool_alloc() -> keel_pool_alloc_debug() for callers.
 * Undefine here so we can define the concrete symbol bodies. */
#ifdef keel_pool_alloc
#undef keel_pool_alloc
#endif

/**
 * @brief Allocation back-end that records debug metadata at each call site.
 *
 * Identical to `keel_pool_alloc()` in behaviour but stamps the @p file and
 * @p line of the call site into the slot's `pool_audit_hdr_t` and inserts
 * the slot into the pool's active-allocation doubly-linked list.  Client
 * code should call through the `keel_pool_alloc()` macro which injects
 * `__FILE__` and `__LINE__` automatically.
 *
 * @param pool Pool to allocate from.
 * @param file Source file of the calling allocation site.
 * @param line Source line of the calling allocation site.
 * @return User-visible pointer to a zeroed or uninitialised object slot,
 *         or `NULL` if the pool is exhausted or `pool` is `NULL`.
 */
void* keel_pool_alloc_debug(keel_pool_t* pool, const char* file, int line) {
    if (!pool) {
        return NULL;
    }
    
    /* Try to get from free list */
    keel_pool_node_t* node = pool->free_list;
    
    if (!node) {
        /* Need to grow pool */
        size_t current = atomic_load(&pool->total);
        if (pool->max_count > 0 && current >= pool->max_count) {
            /* Pool exhausted */
            return NULL;
        }
        
        /* Grow by doubling, but respect max */
        size_t grow_count = pool->initial_count;
        if (pool->max_count > 0) {
            grow_count = keel_min(grow_count, pool->max_count - current);
        }
        
        keel_pool_chunk_t* chunk = pool_chunk_create(pool, grow_count);
        if (!chunk) {
            return NULL;
        }
        
        chunk->next = pool->chunks;
        pool->chunks = chunk;
        
        node = pool->free_list;
    }
    
    /* Remove from free list */
    pool->free_list = node->next;
    atomic_fetch_sub(&pool->available, 1);
    atomic_fetch_add(&pool->allocated, 1);
    
    uint8_t* raw = pool_slot_raw_from_node(pool, node);
    pool_audit_hdr_t* hdr = pool_hdr_from_slot_raw(raw);
    hdr->head_canary = KEEL_POOL_CANARY_HEAD;
    hdr->alloc_file = file;
    hdr->alloc_line = line;
    hdr->state = POOL_BLOCK_ACTIVE;
    hdr->alloc_seq = ++pool->alloc_seq;
    hdr->prev_active = NULL;
    hdr->next_active = NULL;
    pool_write_tail_canary(pool, raw);

    if (pool->audit_lock_init) {
        pthread_mutex_lock(&pool->audit_lock);
        hdr->next_active = pool->active_head;
        if (pool->active_head) {
            pool->active_head->prev_active = hdr;
        }
        pool->active_head = hdr;
        pool->active_audit_count++;
        pthread_mutex_unlock(&pool->audit_lock);
    }

    void* user_ptr = pool_user_from_slot_raw(pool, raw);

    /* Zero if requested */
    if (pool->zero_on_alloc) {
        memset(user_ptr, 0, pool->object_size);
    }
    
    return user_ptr;
}

/**
 * @brief Allocate an object from the pool without debug metadata.
 *
 * Thin wrapper around `keel_pool_alloc_debug()` with synthetic
 * `"<unknown>" / 0` call-site coordinates.  Present as a concrete symbol
 * for link-time use when the `keel_pool_alloc` macro is undefined (e.g.
 * from pre-compiled translation units that include the header before it is
 * macro-overridden).
 *
 * @param pool Pool to allocate from.
 * @return User-visible pointer, or `NULL` on exhaustion.
 */
void* keel_pool_alloc(keel_pool_t* pool) {
    return keel_pool_alloc_debug(pool, "<unknown>", 0);
}

/**
 * @brief Return an object to the pool
 *
 * Returns the object to the pool's free list for reuse.
 * Operation is O(1).
 *
 * The memory is not zeroed (that happens on next allocation if configured).
 *
 * @code
 * keel_pool_free(conn_pool, conn);
 * conn = NULL;  // Don't use after free!
 * @endcode
 *
 * @param pool  Pool the object belongs to
 * @param ptr   Object to return (NULL is safe no-op)
 *
 * @warning Caller must ensure ptr came from this pool!
 * @warning Do not use ptr after this call!
 */
void keel_pool_free(keel_pool_t* pool, void* ptr) {
    if (!pool || !ptr) {
        return;
    }
    
    uint8_t* raw = pool_slot_raw_from_user(pool, ptr);
    pool_audit_hdr_t* hdr = pool_hdr_from_slot_raw(raw);

    if (hdr->head_canary != KEEL_POOL_CANARY_HEAD || !pool_tail_canary_ok(pool, raw)) {
        pool->canary_failures++;
        KEEL_LOG_ERROR(KEEL_LOG_CAT_MEM,
                       "Pool canary violation: pool=%p ptr=%p alloc=%s:%d",
                       (void*)pool,
                       ptr,
                       hdr->alloc_file ? hdr->alloc_file : "<unknown>",
                       hdr->alloc_line);
        return;
    }

    if (hdr->state != POOL_BLOCK_ACTIVE) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_MEM,
                       "Pool invalid free (double-free or stale pointer): pool=%p ptr=%p state=%u alloc=%s:%d",
                       (void*)pool,
                       ptr,
                       (unsigned)hdr->state,
                       hdr->alloc_file ? hdr->alloc_file : "<unknown>",
                       hdr->alloc_line);
        return;
    }

    if (pool->audit_lock_init) {
        pthread_mutex_lock(&pool->audit_lock);
        if (hdr->prev_active) {
            hdr->prev_active->next_active = hdr->next_active;
        } else if (pool->active_head == hdr) {
            pool->active_head = hdr->next_active;
        }
        if (hdr->next_active) {
            hdr->next_active->prev_active = hdr->prev_active;
        }
        if (pool->active_audit_count > 0) {
            pool->active_audit_count--;
        }
        pthread_mutex_unlock(&pool->audit_lock);
    }

    hdr->state = POOL_BLOCK_FREE;
    hdr->prev_active = NULL;
    hdr->next_active = NULL;

    keel_pool_node_t* node = pool_node_from_slot_raw(pool, raw);
    
    /* Add to free list */
    node->next = pool->free_list;
    pool->free_list = node;
    
    atomic_fetch_add(&pool->available, 1);
    atomic_fetch_sub(&pool->allocated, 1);
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Get pool statistics
 *
 * Returns current allocation status of the pool:
 *   - allocated: Objects currently in use
 *   - available: Objects in free list, ready to allocate
 *   - total:     All objects ever created (allocated + available)
 *
 * @code
 * size_t used, free, cap;
 * keel_pool_stats(pool, &used, &free, &cap);
 * printf("Pool: %zu/%zu used (%zu free)\n", used, cap, free);
 * @endcode
 *
 * @param pool       Pool to query (NULL returns zeros)
 * @param allocated  [out] Objects currently allocated (may be NULL)
 * @param available  [out] Objects available in free list (may be NULL)
 * @param total      [out] Total objects in pool (may be NULL)
 */
void keel_pool_stats(const keel_pool_t* pool, size_t* allocated, size_t* available, size_t* total) {
    if (!pool) {
        if (allocated) *allocated = 0;
        if (available) *available = 0;
        if (total) *total = 0;
        return;
    }
    
    if (allocated) *allocated = atomic_load(&pool->allocated);
    if (available) *available = atomic_load(&pool->available);
    if (total) *total = atomic_load(&pool->total);
}
