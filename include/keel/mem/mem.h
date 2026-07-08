/**
 * @file mem.h
 * @brief Public memory-management surface for KEEL's allocator, arena, pool, and slab facilities.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This header defines the allocation abstractions used across the proxy. The
 * subsystem is intentionally layered so callers can choose the simplest tool that
 * matches an object's lifetime and concurrency profile instead of forcing every
 * path through one general-purpose allocator:
 *
 * - the core `keel_malloc` family wraps system allocation with statistics,
 *   corruption detection, and optional debug tracking;
 * - arenas trade individual frees for extremely fast bump allocation and bulk
 *   rollback/reset semantics;
 * - pools optimize fixed-size objects that churn frequently;
 * - slabs multiplex multiple fixed-size pools behind a single variable-size API.
 *
 * The design goal is operational clarity rather than allocator cleverness for
 * its own sake. Each abstraction advertises its ownership model explicitly so
 * higher layers can pick predictable lifetime semantics and avoid mixing APIs.
 */

#ifndef KEEL_MEM_H
#define KEEL_MEM_H

#include "keel_types.h"
#include "keel_error.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Memory Configuration
 * ============================================================================ */

/**
 * @brief Memory debug flags
 */
typedef enum keel_mem_debug_flags {
    KEEL_MEM_DEBUG_NONE          = 0,
    KEEL_MEM_DEBUG_TRACK_ALLOCS  = (1 << 0),  /**< Track all allocations */
    KEEL_MEM_DEBUG_FILL_ALLOC    = (1 << 1),  /**< Fill allocated memory (0xCD) */
    KEEL_MEM_DEBUG_FILL_FREE     = (1 << 2),  /**< Fill freed memory (0xDD) */
    KEEL_MEM_DEBUG_GUARD_PAGES   = (1 << 3),  /**< Use guard pages for overflow detection */
    KEEL_MEM_DEBUG_LEAK_CHECK    = (1 << 4),  /**< Check for leaks on shutdown */
    KEEL_MEM_DEBUG_DOUBLE_FREE   = (1 << 5),  /**< Detect double-free */
    KEEL_MEM_DEBUG_ALL           = 0x7FFFFFFF,  /**< All flags (use max signed int for portability) */
} keel_mem_debug_flags_t;

/**
 * @brief Memory configuration
 */
typedef struct keel_mem_config {
    uint32_t debug_flags;           /**< Debug flags (keel_mem_debug_flags_t) */
    size_t   page_size;             /**< System page size (0 = auto-detect) */
    size_t   default_arena_size;    /**< Default arena size (0 = 64KB) */
    size_t   max_cached_arenas;     /**< Max cached arenas (0 = 16) */

    /* Shared-buffers pool (Phase 2 — call keel_mem_pool_attach after config load) */
    size_t   shared_buffers_bytes;  /**< Pool size; 0 = disabled (libc fallback) */
    bool     mlock_pool;            /**< Lock pool pages into RAM (CAP_IPC_LOCK) */
    bool     use_huge_pages;        /**< Hint MAP_HUGETLB for TLB savings */
} keel_mem_config_t;

/**
 * @brief Construct the default memory-subsystem configuration.
 *
 * The defaults bias toward production-safe behavior with tracking facilities
 * available but not force-enabled. Callers can use this as a base and override
 * only the fields they care about.
 *
 * @return Initialized configuration structure.
 */
keel_mem_config_t keel_mem_config_default(void);

/**
 * @brief Initialize global allocator configuration and statistics tracking.
 *
 * Initialization is expected during early process bootstrap before worker threads
 * begin allocating aggressively. Passing `NULL` applies the default policy.
 *
 * @param config Optional configuration override. `NULL` selects defaults.
 * @return `KEEL_OK` on success, or an error code if the subsystem was already
 *         initialized or configuration could not be applied.
 */
keel_error_t keel_mem_init(const keel_mem_config_t* config);

/**
 * @brief Shut down the memory subsystem and emit any configured debug reports.
 *
 * If leak checking is enabled, outstanding tracked allocations are logged before
 * the subsystem is marked inactive.
 *
 * @return
 */
void keel_mem_shutdown(void);

/**
 * @brief Test-only: make the next N allocations fail.
 *
 * Sets a countdown so that `keel_malloc` / `keel_calloc` / `keel_strdup`
 * return NULL after @p n successful calls.  Pass 0 to fail immediately on
 * the very next call.  Pass -1 (or call once more after exhaustion) to
 * disable.  Not thread-safe — use only from a single test thread.
 */
void keel_mem_set_fail_countdown(int n);

/**
 * @brief Phase-2 memory pool initialisation (call after config is parsed).
 *
 * Maps a single contiguous region of @p bytes using MAP_ANONYMOUS and
 * initialises a dlmalloc mspace over it. After this call, all
 * `keel_malloc` / `keel_calloc` / `keel_realloc` / `keel_free` calls
 * draw from this pool instead of libc malloc.
 *
 * Allocations made during bootstrap (before this call, e.g. for config
 * parsing) remain in libc memory and are not migrated — they are
 * process-lifetime singletons and are not a problem in practice.
 *
 * @param bytes         Pool size. Must be at least 4 MiB.
 * @param mlock_pool    Lock pages into RAM (requires CAP_IPC_LOCK or sufficient
 *                      RLIMIT_MEMLOCK). Prevents swap latency.
 * @param use_huge_pages Hint MAP_HUGETLB for 2 MiB pages (Linux, reduces TLB
 *                      pressure; requires /proc/sys/vm/nr_hugepages > 0).
 * @return KEEL_OK, KEEL_ERR_INVALID (bad size), or KEEL_ERR_NOMEM.
 */
keel_error_t keel_mem_pool_attach(size_t bytes, bool mlock_pool, bool use_huge_pages);

/** @brief Returns true after a successful keel_mem_pool_attach(). */
bool   keel_mem_pool_active(void);

/** @brief Total bytes reserved by the pool (0 if inactive). */
size_t keel_mem_pool_total_bytes(void);

/** @brief Bytes currently committed inside the pool (footprint). */
size_t keel_mem_pool_used_bytes(void);

/** @brief Peak committed bytes since pool creation. */
size_t keel_mem_pool_peak_bytes(void);



/**
 * @brief Allocate memory
 *
 * @param size Number of bytes to allocate
 * @return Pointer to allocated memory, or NULL on failure
 */
KEEL_NODISCARD
void* keel_malloc(size_t size);

/**
 * @brief Allocate zeroed memory
 *
 * @param count Number of elements
 * @param size  Size of each element
 * @return Pointer to zeroed memory, or NULL on failure
 */
KEEL_NODISCARD
void* keel_calloc(size_t count, size_t size);

/**
 * @brief Reallocate memory
 *
 * @param ptr  Existing allocation (NULL is valid)
 * @param size New size
 * @return Pointer to reallocated memory, or NULL on failure
 */
KEEL_NODISCARD
void* keel_realloc(void* ptr, size_t size);

/**
 * @brief Free memory
 *
 * @param ptr Pointer to free (NULL is safe)
 */
void keel_free(void* ptr);

/**
 * @brief Allocate aligned memory
 *
 * @param alignment Alignment (must be power of 2)
 * @param size      Number of bytes
 * @return Aligned pointer, or NULL on failure
 */
KEEL_NODISCARD
void* keel_aligned_alloc(size_t alignment, size_t size);

/**
 * @brief Free aligned memory
 */
void keel_aligned_free(void* ptr);

/**
 * @brief Duplicate memory
 *
 * @param src  Source memory
 * @param size Number of bytes
 * @return New allocation with copied data, or NULL on failure
 */
KEEL_NODISCARD
void* keel_memdup(const void* src, size_t size);

/**
 * @brief Duplicate string
 *
 * @param str Null-terminated string
 * @return New allocation with copied string, or NULL on failure
 */
KEEL_NODISCARD
char* keel_strdup(const char* str);

/**
 * @brief Duplicate string with length limit
 *
 * @param str Null-terminated string
 * @param max Maximum characters to copy
 * @return New allocation with copied string, or NULL on failure
 */
KEEL_NODISCARD
char* keel_strndup(const char* str, size_t max);

/* ============================================================================
 * Debug Allocations (with source location tracking)
 * ============================================================================ */

#ifdef KEEL_MEM_DEBUG

/**
 * @brief Debug malloc with source-location tracking.
 *
 * @param size  Allocation size in bytes.
 * @param file  Caller's __FILE__.
 * @param line  Caller's __LINE__.
 * @return Allocated pointer, or NULL on failure.
 */
void* keel_malloc_debug(size_t size, const char* file, int line);

/**
 * @brief Debug calloc with source-location tracking.
 *
 * @param count  Number of elements.
 * @param size   Element size in bytes.
 * @param file   Caller's __FILE__.
 * @param line   Caller's __LINE__.
 * @return Zeroed allocation, or NULL on failure.
 */
void* keel_calloc_debug(size_t count, size_t size, const char* file, int line);

/**
 * @brief Debug realloc with source-location tracking.
 *
 * @param ptr   Previous allocation (NULL acts like malloc).
 * @param size  New size in bytes.
 * @param file  Caller's __FILE__.
 * @param line  Caller's __LINE__.
 * @return Reallocated pointer, or NULL on failure.
 */
void* keel_realloc_debug(void* ptr, size_t size, const char* file, int line);

/**
 * @brief Debug free with source-location tracking.
 *
 * @param ptr   Allocation to free (NULL is a no-op).
 * @param file  Caller's __FILE__.
 * @param line  Caller's __LINE__.
 */
void  keel_free_debug(void* ptr, const char* file, int line);

#define keel_malloc(size)        keel_malloc_debug((size), __FILE__, __LINE__)
#define keel_calloc(count, size) keel_calloc_debug((count), (size), __FILE__, __LINE__)
#define keel_realloc(ptr, size)  keel_realloc_debug((ptr), (size), __FILE__, __LINE__)
#define keel_free(ptr)           keel_free_debug((ptr), __FILE__, __LINE__)

#endif /* KEEL_MEM_DEBUG */

/* ============================================================================
 * Arena Allocator
 *
 * Fast bump allocator for temporary allocations.
 * All allocations freed at once when arena is reset/destroyed.
 * ============================================================================ */

/**
 * @brief Arena allocator handle
 */
typedef struct keel_arena keel_arena_t;

/**
 * @brief Create a new arena
 *
 * @param initial_size Initial arena size (0 = default)
 * @return Arena handle, or NULL on failure
 */
KEEL_NODISCARD
keel_arena_t* keel_arena_create(size_t initial_size);

/**
 * @brief Destroy an arena and all its allocations
 *
 * @param arena Arena to destroy
 */
void keel_arena_destroy(keel_arena_t* arena);

/**
 * @brief Allocate from arena
 *
 * @param arena Arena to allocate from
 * @param size  Number of bytes
 * @return Pointer to allocated memory, or NULL on failure
 */
KEEL_NODISCARD
void* keel_arena_alloc(keel_arena_t* arena, size_t size);

/**
 * @brief Allocate zeroed memory from arena
 */
KEEL_NODISCARD
void* keel_arena_calloc(keel_arena_t* arena, size_t count, size_t size);

/**
 * @brief Allocate aligned memory from arena
 */
KEEL_NODISCARD
void* keel_arena_alloc_aligned(keel_arena_t* arena, size_t size, size_t alignment);

/**
 * @brief Reset arena (free all allocations, keep memory)
 *
 * @param arena Arena to reset
 */
void keel_arena_reset(keel_arena_t* arena);

/**
 * @brief Report current arena usage.
 *
 * @param arena Arena to inspect.
 * @param[out] used Bytes currently consumed by allocations from the arena.
 * @param[out] committed Total bytes retained by the arena across all blocks.
 * @return
 */
void keel_arena_stats(const keel_arena_t* arena, size_t* used, size_t* committed);

/**
 * @brief Arena save point for partial rollback
 */
typedef struct keel_arena_mark {
    size_t pos;
} keel_arena_mark_t;

/**
 * @brief Save current arena position
 */
keel_arena_mark_t keel_arena_save(keel_arena_t* arena);

/**
 * @brief Restore arena to saved position
 */
void keel_arena_restore(keel_arena_t* arena, keel_arena_mark_t mark);

/**
 * @brief Duplicate string into arena
 */
KEEL_NODISCARD
char* keel_arena_strdup(keel_arena_t* arena, const char* str);

/**
 * @brief Duplicate at most max characters of a string into the arena
 */
KEEL_NODISCARD
char* keel_arena_strndup(keel_arena_t* arena, const char* str, size_t max);

/**
 * @brief Duplicate a memory region into the arena
 */
KEEL_NODISCARD

/**
 * @brief Return the global process-lifetime misc arena.
 *
 * Allocations from this arena are never individually freed; they are released
 * in bulk at keel_mem_shutdown(). Use for singletons, config strings, and any
 * process-lifetime data that does not have a natural request-scoped owner.
 *
 * @return Pointer to the global misc arena, or NULL if not initialised.
 */
KEEL_NODISCARD
keel_arena_t* keel_misc_arena(void);

/**
 * @brief Printf into arena
 */
KEEL_NODISCARD KEEL_PRINTF_FMT(2, 3)
char* keel_arena_sprintf(keel_arena_t* arena, const char* fmt, ...);

/* ============================================================================
 * Pool Allocator
 *
 * Fixed-size object allocator for frequently allocated/freed objects.
 * ============================================================================ */

/**
 * @brief Pool allocator handle
 */
typedef struct keel_pool keel_pool_t;

/* Optional allocation-site aware pool alloc API.
 * Most callers should use keel_pool_alloc() (macro-mapped to debug variant)
 * so active pool blocks retain caller file/line attribution for audits. */
KEEL_NODISCARD
void* keel_pool_alloc_debug(keel_pool_t* pool, const char* file, int line);

/* Dump all currently active blocks tracked by all pools.
 * Intended for debug signals (e.g. SIGUSR1) and test diagnostics. */
void keel_pool_dump_active_allocations(void);

/**
 * @brief Pool configuration
 */
typedef struct keel_pool_config {
    size_t object_size;     /**< Size of each object */
    size_t object_align;    /**< Alignment of each object (0 = default) */
    size_t initial_count;   /**< Initial number of objects (0 = 32) */
    size_t max_count;       /**< Maximum objects (0 = unlimited) */
    bool   zero_on_alloc;   /**< Zero memory on allocation */
} keel_pool_config_t;

/**
 * @brief Create a pool allocator
 *
 * @param config Pool configuration
 * @return Pool handle, or NULL on failure
 */
KEEL_NODISCARD
keel_pool_t* keel_pool_create(const keel_pool_config_t* config);

/**
 * @brief Destroy a pool
 */
void keel_pool_destroy(keel_pool_t* pool);

/**
 * @brief Allocate object from pool
 *
 * @param pool Pool to allocate from
 * @return Pointer to object, or NULL if pool exhausted
 */
KEEL_NODISCARD
void* keel_pool_alloc(keel_pool_t* pool);

/**
 * @brief Return object to pool
 *
 * @param pool Pool to return to
 * @param ptr  Object to return
 */
void keel_pool_free(keel_pool_t* pool, void* ptr);

#ifndef KEEL_POOL_ALLOC_NO_FILELINE
#define keel_pool_alloc(pool) keel_pool_alloc_debug((pool), __FILE__, __LINE__)
#endif

/**
 * @brief Report current pool occupancy.
 *
 * @param pool Pool to inspect.
 * @param[out] allocated Number of objects currently checked out by callers.
 * @param[out] available Number of objects sitting on the free list.
 * @param[out] total Total objects currently owned by the pool.
 * @return
 */
void keel_pool_stats(const keel_pool_t* pool, size_t* allocated, size_t* available, size_t* total);

/* ============================================================================
 * Memory Statistics
 * ============================================================================ */

/**
 * @brief Memory statistics
 */
typedef struct keel_mem_stats {
    /* Current state */
    size_t bytes_allocated;     /**< Currently allocated bytes */
    size_t bytes_committed;     /**< Memory committed from OS */
    size_t allocation_count;    /**< Current number of allocations */

    /* Lifetime totals */
    size_t total_allocations;   /**< Total allocations made */
    size_t total_frees;         /**< Total frees made */
    size_t total_bytes;         /**< Total bytes allocated (lifetime) */

    /* Peak usage */
    size_t peak_bytes;          /**< Peak bytes allocated */
    size_t peak_allocations;    /**< Peak allocation count */

    /* Arenas */
    size_t arena_count;         /**< Active arenas */
    size_t arena_bytes;         /**< Arena memory usage */

    /* Object pools */
    size_t pool_count;          /**< Active pools */
    size_t pool_bytes;          /**< Pool memory usage */

    /* Shared-buffers pool (populated when keel_mem_pool_active()) */
    size_t shared_pool_total;   /**< Total bytes reserved by the mmap pool */
    size_t shared_pool_used;    /**< Bytes committed (footprint) inside the pool */
    size_t shared_pool_peak;    /**< Peak footprint since pool creation */
} keel_mem_stats_t;

/**
 * @brief Snapshot global memory statistics.
 *
 * @param[out] stats Destination structure populated with current counters.
 * @return
 */
void keel_mem_stats_get(keel_mem_stats_t* stats);

/**
 * @brief Print memory statistics to log
 */
void keel_mem_stats_log(void);

/**
 * @brief Reset all lifetime statistics counters to zero.
 *
 * Useful for measuring deltas between operations. Does not affect
 * currently-allocated bytes or allocation-count gauges.
 */
void keel_mem_reset_stats(void);

/**
 * @brief Dump all allocations (debug mode only)
 *
 * Prints all current allocations with source locations.
 */
void keel_mem_dump_allocations(void);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_MEM_H */
