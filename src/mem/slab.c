/**
 * @file slab.c
 * @brief Size-class allocator built on top of the fixed-size pool subsystem.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The slab allocator bridges the gap between a general allocator and the more
 * explicit pool API. Requests are rounded to a bounded set of size classes and
 * served by lazily created pools, preserving pool-like O(1) object recycling for
 * small and medium allocations while still giving callers a single variable-size
 * entry point. The explicit size-class header stored before the user pointer is
 * the key implementation tradeoff: it adds a small fixed overhead but makes free
 * operations deterministic without requiring the caller to resupply the size.
 */

#include "keel/mem/mem.h"
#include "keel/log/log.h"

#include <string.h>

/** Smallest size class (bytes) */
#define SLAB_MIN_SIZE       16

/** Largest size class (bytes) - larger allocations use malloc */
#define SLAB_MAX_SIZE       8192

/** Number of size classes (powers of 2 from MIN to MAX) */
#define SLAB_NUM_CLASSES    10

/** Default number of objects pre-allocated per size class */
#define SLAB_OBJECTS_PER_CLASS 64

/** Sentinel value stored in the header of malloc-backed large allocations */
#define SLAB_MALLOC_SENTINEL   ((uint64_t)0xFF)

/**
 * Header prepended to every slab allocation (8 bytes).
 * Stores the size-class index so keel_slab_free() can return
 * the slot to the correct pool without a size parameter.
 * Large (>8 KB) allocations use SLAB_MALLOC_SENTINEL.
 */
typedef uint64_t slab_hdr_t;

/**
 * @brief Slab allocator
 */
struct keel_slab {
    keel_pool_t* pools[SLAB_NUM_CLASSES];    /**< Pool per size class */
};

/* Size class lookup table - powers of 2 from 16 to 8192 */
static const size_t size_classes[SLAB_NUM_CLASSES] = {
    16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192
};

/**
 * @brief Determine which size class an allocation belongs to
 *
 * Finds the smallest size class that can hold 'size' bytes.
 * Uses simple if-else chain which compiles to efficient binary search
 * or jump table.
 *
 * @param size  Number of bytes requested
 * @return Index into size_classes[], or -1 if too large
 */
static int slab_size_class(size_t size) {
    if (size <= 16) return 0;
    if (size <= 32) return 1;
    if (size <= 64) return 2;
    if (size <= 128) return 3;
    if (size <= 256) return 4;
    if (size <= 512) return 5;
    if (size <= 1024) return 6;
    if (size <= 2048) return 7;
    if (size <= 4096) return 8;
    if (size <= 8192) return 9;
    return -1; /* Too large for slab */
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Create a new slab allocator
 *
 * Creates a slab allocator with lazy pool initialization.
 * Pools are only created when first allocation of that size class occurs.
 *
 * @code
 * keel_slab_t* slab = keel_slab_create();
 * void* p1 = keel_slab_alloc(slab, 100);  // Creates 128-byte pool
 * void* p2 = keel_slab_alloc(slab, 50);   // Creates 64-byte pool
 * @endcode
 *
 * @return New slab allocator, or NULL on failure
 */
keel_slab_t* keel_slab_create(void) {
    keel_slab_t* slab = keel_calloc(1, sizeof(keel_slab_t));
    if (!slab) {
        return NULL;
    }
    
    /* Create pools lazily on first allocation */
    
    return slab;
}

/**
 * @brief Destroy a slab allocator and all its pools
 *
 * Frees all pools and the slab structure.
 * All pointers from this slab become invalid.
 *
 * @param slab  Slab to destroy (NULL is safe no-op)
 */
void keel_slab_destroy(keel_slab_t* slab) {
    if (!slab) {
        return;
    }
    
    for (int i = 0; i < SLAB_NUM_CLASSES; i++) {
        if (slab->pools[i]) {
            keel_pool_destroy(slab->pools[i]);
        }
    }
    
    keel_free(slab);
}

/* ============================================================================
 * Allocation
 * ============================================================================ */

/**
 * @brief Allocate memory from the slab allocator
 *
 * Allocates 'size' bytes by:
 * 1. Finding the appropriate size class
 * 2. Creating the pool for that class if needed
 * 3. Allocating from that pool
 *
 * If size > SLAB_MAX_SIZE (8KB), falls back to regular malloc.
 *
 * @code
 * // Request 100 bytes, get 128-byte slot from class 3
 * void* p = keel_slab_alloc(slab, 100);
 * @endcode
 *
 * @param slab  Slab allocator
 * @param size  Number of bytes needed
 * @return Pointer to memory, or NULL on failure
 *
 * @note Memory is NOT zeroed; use memset if needed
 */
void* keel_slab_alloc(keel_slab_t* slab, size_t size) {
    if (!slab || size == 0) {
        return NULL;
    }
    
    int class_idx = slab_size_class(size);
    
    if (class_idx < 0) {
        /* Too large for slab — allocate with a header so free() can
         * distinguish this from a pool-backed allocation. */
        slab_hdr_t* raw = (slab_hdr_t*)keel_malloc(sizeof(slab_hdr_t) + size);
        if (!raw) return NULL;
        *raw = SLAB_MALLOC_SENTINEL;
        return raw + 1;
    }
    
    /* Create pool if needed.
     * Object size includes the 8-byte header so the pool slot can hold
     * both the class-index metadata and the user payload. */
    if (!slab->pools[class_idx]) {
        keel_pool_config_t config = {
            .object_size = size_classes[class_idx] + sizeof(slab_hdr_t),
            .object_align = 16,
            .initial_count = SLAB_OBJECTS_PER_CLASS,
            .max_count = 0, /* unlimited */
            .zero_on_alloc = false,
        };
        
        slab->pools[class_idx] = keel_pool_create(&config);
        if (!slab->pools[class_idx]) {
            return NULL;
        }
    }
    
    /* Write class index into header, return pointer past it */
    slab_hdr_t* raw = (slab_hdr_t*)keel_pool_alloc(slab->pools[class_idx]);
    if (!raw) return NULL;
    *raw = (slab_hdr_t)class_idx;
    return raw + 1;
}

/**
 * @brief Free memory allocated from slab allocator
 *
 * Attempts to return memory to the slab for reuse.
 *
 * LIMITATION: This implementation cannot determine which pool
 * an allocation came from without storing extra metadata.
 * Currently falls back to regular keel_free().
 *
 * For proper pool-based freeing, use keel_pool directly with
 * a known object size.
 *
 * @param slab  Slab allocator (NULL is safe no-op)
 * @param ptr   Pointer to free (NULL is safe no-op)
 *
 * @todo Store allocation size for proper pool return
 */
void keel_slab_free(keel_slab_t* slab, void* ptr) {
    if (!slab || !ptr) {
        return;
    }
    
    /* Walk back past the 8-byte header to find the raw slot start */
    slab_hdr_t* raw = (slab_hdr_t*)ptr - 1;
    slab_hdr_t  class_idx = *raw;
    
    if (class_idx == SLAB_MALLOC_SENTINEL) {
        /* Large allocation — free the whole block including the header */
        keel_free(raw);
        return;
    }
    
    /* Return the slot (header + payload) to the correct size-class pool */
    if ((int)class_idx < SLAB_NUM_CLASSES && slab->pools[class_idx]) {
        keel_pool_free(slab->pools[class_idx], raw);
    } else {
        /* Corrupt header or pool was destroyed — fall back safely */
        keel_free(raw);
    }
}
