/**
 * @file mem.c
 * @brief Core allocator wrappers, global memory statistics, and debug tracking.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This module is the lowest-level allocation service used by the rest of the mem
 * subsystem. Its job is deliberately modest: wrap system allocation with enough
 * metadata to support accounting, cheap corruption checks, optional debug fills,
 * and leak reporting without trying to become a full custom heap. More specialized
 * allocators such as arenas, pools, and slabs are layered on top of these wrappers
 * so they inherit the same visibility and diagnostics.
 */

#include "keel/mem/mem.h"
#include "keel/log/log.h"

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <unistd.h>
#if __has_include(<execinfo.h>)
#  include <execinfo.h>
#  define KEEL_HAVE_EXECINFO 1
#else
#  define KEEL_HAVE_EXECINFO 0
#endif

/* ----------------------------------------------------------------------
 * Diagnostic backtrace helper used by allocator corruption logging.
 *
 * Captures up to 32 frames and emits one ERROR line per frame so that
 * each entry shows up next to the corruption report in `docker logs`.
 * Symbol resolution depends on -rdynamic / linkable symbol tables; for
 * stripped binaries we still get raw addresses, which can be passed to
 * `addr2line` / `eu-addr2line` against the matching build artefact.
 *
 * Kept out of the hot path: only invoked from corruption-detection
 * paths in keel_free / keel_realloc.
 * ---------------------------------------------------------------------- */
static void keel_mem_log_backtrace(keel_log_category_t cat, const char* label) {
#if KEEL_HAVE_EXECINFO
    void* frames[32];
    int   n = backtrace(frames, (int)(sizeof(frames) / sizeof(frames[0])));
    char** syms = backtrace_symbols(frames, n);
    if (!syms) {
        KEEL_LOG_ERROR(cat, "[%s] backtrace: <symbol resolution failed, n=%d>", label, n);
        return;
    }
    /* Skip frame 0 (this helper itself). */
    for (int i = 1; i < n; ++i) {
        KEEL_LOG_ERROR(cat, "[%s]   #%-2d %s", label, i - 1, syms[i]);
    }
    free(syms);  /* allocated by libc, not us */
#else
    KEEL_LOG_ERROR(cat, "[%s] backtrace: <not available on this platform>", label);
#endif
}

/* ============================================================================
 * Configuration
 * ============================================================================ */

static struct {
    bool              initialized;
    keel_mem_config_t  config;
    
    /* Statistics (atomic for thread safety) */
    atomic_size_t     bytes_allocated;
    atomic_size_t     allocation_count;
    atomic_size_t     total_allocations;
    atomic_size_t     total_frees;
    atomic_size_t     total_bytes;
    atomic_size_t     peak_bytes;
    atomic_size_t     peak_allocations;
    atomic_size_t     arena_count;
    atomic_size_t     arena_bytes;
    atomic_size_t     pool_count;
    atomic_size_t     pool_bytes;
} g_mem = {0};

/* ============================================================================
 * Test-only: allocation failure injection
 * ============================================================================
 * When g_alloc_fail_countdown >= 0, each call to keel_malloc_debug decrements
 * the counter and returns NULL at zero.  Set to -1 (default) to disable.
 * This is intentionally a plain global (not atomic) because failure injection
 * is always done from a single test thread.
 */
static int g_alloc_fail_countdown = -1;

/**
 * @brief Set the allocation failure-injection countdown for testing.
 *
 * When @p n is non-negative, the next @p n allocations through
 * `keel_malloc_debug()` succeed normally, and the @p n+1-th returns `NULL`.
 * Set to `-1` (the default) to disable failure injection entirely.
 *
 * @param n Number of successful allocations before the next one fails,
 *          or `-1` to disable injection.
 *
 * Notes:
 * - Not thread-safe; designed for single-threaded test setup.
 * - Has no effect when `KEEL_MEM_SAFETY` overrides the allocator.
 */
void keel_mem_set_fail_countdown(int n) { g_alloc_fail_countdown = n; }

/* Debug fill patterns */
#define KEEL_MEM_FILL_ALLOC 0xCD
#define KEEL_MEM_FILL_FREE  0xDD
#define KEEL_MEM_FILL_GUARD 0xFD

/* Allocation header for tracking */
typedef struct keel_alloc_header {
    size_t      size;       /* Requested size */
    uint32_t    magic;      /* Magic number for validation */
#ifdef KEEL_MEM_DEBUG
    const char* file;
    int         line;
    struct keel_alloc_header* next;
    struct keel_alloc_header* prev;
#endif
} keel_alloc_header_t;

/* Magic value: 0xDB + 0xA1 + 0x10 + 0x0C = signature
 * This unique value helps detect memory corruption:
 * - If magic != expected, the header was overwritten (buffer underflow)
 * - If magic == 0, the block was already freed (double-free)
 */
#define KEEL_ALLOC_MAGIC 0xDBA1100CU

#ifdef KEEL_MEM_DEBUG
/* Global allocation tracking list head - linked list of all active allocations */
static keel_alloc_header_t* g_alloc_list = NULL;
/* Spinlock for thread-safe access to allocation list */
static atomic_flag g_alloc_lock = ATOMIC_FLAG_INIT;

/**
 * @brief Acquire spinlock for allocation list access
 *
 * Uses atomic test-and-set for lock-free synchronization.
 * Spins until lock is acquired - suitable for short critical sections.
 */
static void alloc_list_lock(void) {
    while (atomic_flag_test_and_set(&g_alloc_lock)) {
        /* spin */
    }
}

/**
 * @brief Release spinlock for allocation list
 */
static void alloc_list_unlock(void) {
    atomic_flag_clear(&g_alloc_lock);
}
#endif

/* ============================================================================
 * Initialization
 * ============================================================================ */

/**
 * @brief Get default memory configuration
 *
 * Returns sensible defaults for the memory subsystem:
 * - No debug flags enabled (production mode)
 * - Auto-detect system page size
 * - 64KB default arena size (good for most workloads)
 * - Cache up to 16 arenas for reuse
 *
 * @return Default configuration structure
 */
keel_mem_config_t keel_mem_config_default(void) {
    return (keel_mem_config_t){
        .debug_flags = 0,
        .page_size = 0,
        .default_arena_size = 64 * 1024,
        .max_cached_arenas = 16,
    };
}

/**
 * @brief Initialize the memory subsystem
 *
 * Must be called before any other memory functions. Initializes:
 * - Global configuration from provided config or defaults
 * - System page size (auto-detected if not specified)
 * - Atomic statistics counters
 *
 * This function is NOT thread-safe and should be called once during
 * application startup, before spawning any threads.
 *
 * @param config  Configuration options, or NULL for defaults
 * @return KEEL_OK on success
 * @return KEEL_ERR_ALREADY_INITIALIZED if called more than once
 */
keel_error_t keel_mem_init(const keel_mem_config_t* config) {
    if (g_mem.initialized) {
        return KEEL_ERR_ALREADY_INITIALIZED;
    }
    
    if (config) {
        g_mem.config = *config;
    } else {
        g_mem.config = keel_mem_config_default();
    }
    
    /* Auto-detect page size */
    if (g_mem.config.page_size == 0) {
#ifdef _WIN32
        SYSTEM_INFO si;
        GetSystemInfo(&si);
        g_mem.config.page_size = si.dwPageSize;
#else
        g_mem.config.page_size = (size_t)sysconf(_SC_PAGESIZE);
#endif
    }
    
    /* Initialize atomics */
    atomic_init(&g_mem.bytes_allocated, 0);
    atomic_init(&g_mem.allocation_count, 0);
    atomic_init(&g_mem.total_allocations, 0);
    atomic_init(&g_mem.total_frees, 0);
    atomic_init(&g_mem.total_bytes, 0);
    atomic_init(&g_mem.peak_bytes, 0);
    atomic_init(&g_mem.peak_allocations, 0);
    atomic_init(&g_mem.arena_count, 0);
    atomic_init(&g_mem.arena_bytes, 0);
    atomic_init(&g_mem.pool_count, 0);
    atomic_init(&g_mem.pool_bytes, 0);
    
    g_mem.initialized = true;
    
    return KEEL_OK;
}

/**
 * @brief Shutdown the memory subsystem
 *
 * Performs cleanup and optionally reports memory leaks:
 * - If KEEL_MEM_DEBUG_LEAK_CHECK is enabled, dumps all unreleased allocations
 * - Marks subsystem as uninitialized
 *
 * After calling this, keel_mem_init() must be called again before
 * using any memory functions.
 *
 * @note Any allocations made after init but before shutdown are leaked
 */
void keel_mem_shutdown(void) {
    if (!g_mem.initialized) {
        return;
    }
    
#ifdef KEEL_MEM_DEBUG
    if (g_mem.config.debug_flags & KEEL_MEM_DEBUG_LEAK_CHECK) {
        keel_mem_dump_allocations();
    }
#endif
    
    g_mem.initialized = false;
}

/* ============================================================================
 * Basic Allocation
 * ============================================================================ */

/**
 * @brief Update allocation statistics after a successful allocation
 *
 * Thread-safe update of all allocation-related counters using atomic
 * operations. Also updates peak usage tracking using compare-and-swap
 * to handle concurrent updates correctly.
 *
 * @param size  Number of bytes allocated (user-requested size)
 */
static void update_stats_alloc(size_t size) {
    size_t current = atomic_fetch_add(&g_mem.bytes_allocated, size) + size;
    atomic_fetch_add(&g_mem.allocation_count, 1);
    atomic_fetch_add(&g_mem.total_allocations, 1);
    atomic_fetch_add(&g_mem.total_bytes, size);
    
    /* Update peak */
    size_t peak = atomic_load(&g_mem.peak_bytes);
    while (current > peak) {
        if (atomic_compare_exchange_weak(&g_mem.peak_bytes, &peak, current)) {
            break;
        }
    }
    
    size_t count = atomic_load(&g_mem.allocation_count);
    size_t peak_count = atomic_load(&g_mem.peak_allocations);
    while (count > peak_count) {
        if (atomic_compare_exchange_weak(&g_mem.peak_allocations, &peak_count, count)) {
            break;
        }
    }
}

/**
 * @brief Update allocation statistics after freeing memory
 *
 * Thread-safe decrement of allocation counters.
 *
 * @param size  Number of bytes being freed
 */
static void update_stats_free(size_t size) {
    atomic_fetch_sub(&g_mem.bytes_allocated, size);
    atomic_fetch_sub(&g_mem.allocation_count, 1);
    atomic_fetch_add(&g_mem.total_frees, 1);
}

#ifndef KEEL_MEM_DEBUG

/**
 * @brief Allocate memory (production version)
 *
 * Allocates memory with a prepended header for size tracking.
 * The header contains:
 * - Requested size (for statistics and realloc)
 * - Magic number (for corruption detection)
 *
 * If KEEL_MEM_DEBUG_FILL_ALLOC is enabled, fills memory with 0xCD
 * pattern to help identify use of uninitialized memory.
 *
 * Memory layout:
 *   [keel_alloc_header_t][user data...]
 *                       ^-- returned pointer
 *
 * @param size  Number of bytes to allocate
 * @return Pointer to allocated memory, or NULL on failure
 *
 * @note Returns NULL for size == 0
 * @see keel_free() to release allocated memory
 */
void* keel_malloc(size_t size) {
    if (size == 0) {
        return NULL;
    }

    /* Test-only failure injection */
    if (g_alloc_fail_countdown == 0) {
        return NULL;
    }
    if (g_alloc_fail_countdown > 0) {
        g_alloc_fail_countdown--;
    }

    /* Allocate with header */
    size_t total = sizeof(keel_alloc_header_t) + size;
    keel_alloc_header_t* header = malloc(total);
    if (!header) {
        return NULL;
    }
    
    header->size = size;
    header->magic = KEEL_ALLOC_MAGIC;
    
    void* ptr = header + 1;
    
    /* Fill with pattern if debugging */
    if (g_mem.config.debug_flags & KEEL_MEM_DEBUG_FILL_ALLOC) {
        memset(ptr, KEEL_MEM_FILL_ALLOC, size);
    }
    
    update_stats_alloc(size);
    
    return ptr;
}

/**
 * @brief Allocate zeroed memory
 *
 * Allocates count * size bytes and initializes to zero.
 * Equivalent to malloc + memset(0).
 *
 * @param count  Number of elements
 * @param size   Size of each element
 * @return Zeroed memory, or NULL on failure
 */
void* keel_calloc(size_t count, size_t size) {
    size_t total = count * size;
    void* ptr = keel_malloc(total);
    if (ptr) {
        memset(ptr, 0, total);
    }
    return ptr;
}

/**
 * @brief Reallocate memory block
 *
 * Changes the size of an allocation. Contents up to min(old_size, new_size)
 * are preserved. May return a different pointer.
 *
 * @param ptr   Current allocation (NULL = malloc)
 * @param size  New size (0 = free)
 * @return New pointer, or NULL on failure (original unchanged)
 */
void* keel_realloc(void* ptr, size_t size) {
    if (!ptr) {
        return keel_malloc(size);
    }
    
    if (size == 0) {
        keel_free(ptr);
        return NULL;
    }
    
    keel_alloc_header_t* header = ((keel_alloc_header_t*)ptr) - 1;
    
    if (header->magic != KEEL_ALLOC_MAGIC) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_MEM, "Invalid memory block in realloc");
        return NULL;
    }
    
    size_t old_size = header->size;
    
    size_t total = sizeof(keel_alloc_header_t) + size;
    keel_alloc_header_t* new_header = realloc(header, total);
    if (!new_header) {
        return NULL;
    }
    
    new_header->size = size;
    
    /* Update stats */
    atomic_fetch_sub(&g_mem.bytes_allocated, old_size);
    atomic_fetch_add(&g_mem.bytes_allocated, size);
    atomic_fetch_add(&g_mem.total_bytes, size);
    
    return new_header + 1;
}

/**
 * @brief Free allocated memory
 *
 * Releases memory allocated by keel_malloc/calloc/realloc.
 *
 * In debug mode:
 *   - Validates magic number to detect corruption
 *   - Fills with pattern to detect use-after-free
 *   - Invalidates header to detect double-free
 *
 * @param ptr  Memory to free (NULL is safe no-op)
 */
void keel_free(void* ptr) {
    if (!ptr) {
        return;
    }
    
    keel_alloc_header_t* header = ((keel_alloc_header_t*)ptr) - 1;
    
    if (header->magic != KEEL_ALLOC_MAGIC) {
        /* Dump enough state to fingerprint the corrupter:
         *  - header->size (offset 0..7): if also clobbered, the whole header
         *    was overwritten -> structured write; else only the magic field
         *    (offset 8..11) was hit -> 4-byte stray write.
         *  - first 32 bytes of user data: helps spot recurring patterns
         *    (e.g. a buffer left over from a previous use).
         *  - backtrace of the freer narrows down the *type* of allocation
         *    (each pool/cache calls keel_free from a distinct site). */
        const unsigned char* p = (const unsigned char*)ptr;
        KEEL_LOG_ERROR(
            KEEL_LOG_CAT_MEM,
            "Invalid memory block in free (possible double-free or corruption): "
            "ptr=%p header=%p magic=0x%08x expected=0x%08x size_field=%zu "
            "userdata[0..31]=%02x%02x%02x%02x%02x%02x%02x%02x "
            "%02x%02x%02x%02x%02x%02x%02x%02x "
            "%02x%02x%02x%02x%02x%02x%02x%02x "
            "%02x%02x%02x%02x%02x%02x%02x%02x",
            ptr,
            (void*)header,
            (unsigned)header->magic,
            (unsigned)KEEL_ALLOC_MAGIC,
            header->size,
            p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],
            p[8],p[9],p[10],p[11],p[12],p[13],p[14],p[15],
            p[16],p[17],p[18],p[19],p[20],p[21],p[22],p[23],
            p[24],p[25],p[26],p[27],p[28],p[29],p[30],p[31]
        );
        keel_mem_log_backtrace(KEEL_LOG_CAT_MEM, "corrupt-free");
        return;
    }
    
    size_t size = header->size;
    
    /* Fill with pattern if debugging */
    if (g_mem.config.debug_flags & KEEL_MEM_DEBUG_FILL_FREE) {
        memset(ptr, KEEL_MEM_FILL_FREE, size);
    }
    
    /* Invalidate magic to detect double-free */
    header->magic = 0;
    
    update_stats_free(size);
    
    free(header);
}

#else /* KEEL_MEM_DEBUG */

/**
 * @brief Allocate memory with debug source attribution.
 *
 * This variant keeps each live allocation on a tracked linked list so shutdown-
 * time leak reports can name the most recent allocation site.
 *
 * @param size Requested allocation size in bytes.
 * @param file Source file that requested the allocation.
 * @param line Source line that requested the allocation.
 * @return Pointer to user-visible memory, or `NULL` on allocation failure.
 */
void* keel_malloc_debug(size_t size, const char* file, int line) {
    if (size == 0) {
        return NULL;
    }

    /* Test-only failure injection: return NULL when countdown hits zero */
    if (g_alloc_fail_countdown == 0) {
        return NULL;
    }
    if (g_alloc_fail_countdown > 0) {
        g_alloc_fail_countdown--;
    }

    size_t total = sizeof(keel_alloc_header_t) + size;
    keel_alloc_header_t* header = malloc(total);
    if (!header) {
        return NULL;
    }
    
    header->size = size;
    header->magic = KEEL_ALLOC_MAGIC;
    header->file = file;
    header->line = line;
    
    /* Add to tracking list */
    alloc_list_lock();
    header->next = g_alloc_list;
    header->prev = NULL;
    if (g_alloc_list) {
        g_alloc_list->prev = header;
    }
    g_alloc_list = header;
    alloc_list_unlock();
    
    void* ptr = header + 1;
    
    if (g_mem.config.debug_flags & KEEL_MEM_DEBUG_FILL_ALLOC) {
        memset(ptr, KEEL_MEM_FILL_ALLOC, size);
    }
    
    update_stats_alloc(size);
    
    return ptr;
}

/**
 * @brief Debug-tracked calloc equivalent.
 *
 * @param count Number of elements.
 * @param size Size of each element.
 * @param file Allocation call-site file.
 * @param line Allocation call-site line.
 * @return Zero-initialized allocation, or `NULL` on failure.
 */
void* keel_calloc_debug(size_t count, size_t size, const char* file, int line) {
    size_t total = count * size;
    void* ptr = keel_malloc_debug(total, file, line);
    if (ptr) {
        memset(ptr, 0, total);
    }
    return ptr;
}

/**
 * @brief Debug-tracked realloc equivalent.
 *
 * The implementation removes the old block from the active list before calling
 * `realloc`, then reinserts the resulting block so the tracking list always
 * points at the live header address.
 *
 * @param ptr Existing allocation, or `NULL`.
 * @param size New requested size.
 * @param file Reallocation call-site file.
 * @param line Reallocation call-site line.
 * @return Reallocated block, `NULL` on failure, or `NULL` after freeing when
 *         `size == 0`.
 */
void* keel_realloc_debug(void* ptr, size_t size, const char* file, int line) {
    if (!ptr) {
        return keel_malloc_debug(size, file, line);
    }
    
    if (size == 0) {
        keel_free_debug(ptr, file, line);
        return NULL;
    }
    
    keel_alloc_header_t* header = ((keel_alloc_header_t*)ptr) - 1;
    
    if (header->magic != KEEL_ALLOC_MAGIC) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_MEM, "Invalid memory block in realloc at %s:%d", file, line);
        return NULL;
    }
    
    size_t old_size = header->size;
    
    /* Remove from list */
    alloc_list_lock();
    if (header->prev) {
        header->prev->next = header->next;
    } else {
        g_alloc_list = header->next;
    }
    if (header->next) {
        header->next->prev = header->prev;
    }
    alloc_list_unlock();
    
    size_t total = sizeof(keel_alloc_header_t) + size;
    keel_alloc_header_t* new_header = realloc(header, total);
    if (!new_header) {
        /* Put back in list */
        alloc_list_lock();
        header->next = g_alloc_list;
        header->prev = NULL;
        if (g_alloc_list) {
            g_alloc_list->prev = header;
        }
        g_alloc_list = header;
        alloc_list_unlock();
        return NULL;
    }
    
    new_header->size = size;
    new_header->file = file;
    new_header->line = line;
    
    /* Add back to list */
    alloc_list_lock();
    new_header->next = g_alloc_list;
    new_header->prev = NULL;
    if (g_alloc_list) {
        g_alloc_list->prev = new_header;
    }
    g_alloc_list = new_header;
    alloc_list_unlock();
    
    atomic_fetch_sub(&g_mem.bytes_allocated, old_size);
    atomic_fetch_add(&g_mem.bytes_allocated, size);
    atomic_fetch_add(&g_mem.total_bytes, size);
    
    return new_header + 1;
}

/**
 * @brief Debug-tracked free equivalent.
 *
 * @param ptr Allocation to release. `NULL` is accepted.
 * @param file Free call-site file.
 * @param line Free call-site line.
 * @return
 */
void keel_free_debug(void* ptr, const char* file, int line) {
    if (!ptr) {
        return;
    }
    
    keel_alloc_header_t* header = ((keel_alloc_header_t*)ptr) - 1;
    
    if (header->magic != KEEL_ALLOC_MAGIC) {
        KEEL_LOG_ERROR(
            KEEL_LOG_CAT_MEM,
            "Invalid memory block in free at %s:%d (possible double-free or corruption): ptr=%p header=%p magic=0x%08x expected=0x%08x",
            file,
            line,
            ptr,
            (void*)header,
            (unsigned)header->magic,
            (unsigned)KEEL_ALLOC_MAGIC
        );
        return;
    }
    
    size_t size = header->size;
    
    /* Remove from list */
    alloc_list_lock();
    if (header->prev) {
        header->prev->next = header->next;
    } else {
        g_alloc_list = header->next;
    }
    if (header->next) {
        header->next->prev = header->prev;
    }
    alloc_list_unlock();
    
    if (g_mem.config.debug_flags & KEEL_MEM_DEBUG_FILL_FREE) {
        memset(ptr, KEEL_MEM_FILL_FREE, size);
    }
    
    header->magic = 0;
    
    update_stats_free(size);
    
    free(header);
}

#endif /* KEEL_MEM_DEBUG */

/* ============================================================================
 * Aligned Allocation
 * ============================================================================ */

/**
 * @brief Allocate memory with specific alignment
 *
 * Useful for SIMD operations or cache line alignment.
 *
 * Implementation: Allocates extra space, adjusts pointer, and
 * stores the original pointer just before the aligned address.
 *
 * @param alignment  Required alignment (must be power of 2, >= sizeof(void*))
 * @param size       Number of bytes to allocate
 * @return Aligned pointer, or NULL on failure
 *
 * @note Must be freed with keel_aligned_free(), NOT keel_free()!
 */
void* keel_aligned_alloc(size_t alignment, size_t size) {
    if (!keel_is_power_of_2(alignment) || alignment < sizeof(void*)) {
        return NULL;
    }
    
    /* We need extra space for the original pointer and alignment */
    size_t total = size + alignment + sizeof(void*);
    void* raw = keel_malloc(total);
    if (!raw) {
        return NULL;
    }
    
    /* Calculate aligned pointer */
    uintptr_t addr = (uintptr_t)raw + sizeof(void*);
    uintptr_t aligned = keel_align_up(addr, alignment);
    
    /* Store original pointer before aligned address */
    ((void**)aligned)[-1] = raw;
    
    return (void*)aligned;
}

/**
 * @brief Free aligned memory
 *
 * Frees memory allocated by keel_aligned_alloc().
 *
 * @param ptr  Aligned pointer to free (NULL is safe no-op)
 */
void keel_aligned_free(void* ptr) {
    if (!ptr) {
        return;
    }
    
    /* Retrieve original pointer */
    void* raw = ((void**)ptr)[-1];
    keel_free(raw);
}

/* ============================================================================
 * String Operations
 * ============================================================================ */

/**
 * @brief Duplicate a memory block
 *
 * Allocates a new block and copies the contents.
 *
 * @param src   Source data
 * @param size  Number of bytes to copy
 * @return New allocation with copied data, or NULL
 */
void* keel_memdup(const void* src, size_t size) {
    if (!src || size == 0) {
        return NULL;
    }
    
    void* dst = keel_malloc(size);
    if (dst) {
        memcpy(dst, src, size);
    }
    return dst;
}

/**
 * @brief Duplicate a string
 *
 * Allocates strlen(str)+1 bytes and copies the string.
 *
 * @param str  String to duplicate
 * @return New string, or NULL
 */
char* keel_strdup(const char* str) {
    if (!str) {
        return NULL;
    }
    
    size_t len = strlen(str);
    char* dup = keel_malloc(len + 1);
    if (dup) {
        memcpy(dup, str, len + 1);
    }
    return dup;
}

/**
 * @brief Duplicate a string with maximum length
 *
 * Copies at most 'max' characters, always null-terminates.
 *
 * @param str  String to duplicate
 * @param max  Maximum characters to copy
 * @return New string, or NULL
 */
char* keel_strndup(const char* str, size_t max) {
    if (!str) {
        return NULL;
    }
    
    size_t len = 0;
    while (len < max && str[len]) {
        len++;
    }
    
    char* dup = keel_malloc(len + 1);
    if (dup) {
        memcpy(dup, str, len);
        dup[len] = '\0';
    }
    return dup;
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Get current memory statistics
 *
 * Fills in statistics structure with current allocation info.
 * All counters are atomically read for thread safety.
 *
 * @param stats  [out] Statistics structure to fill
 */
void keel_mem_stats_get(keel_mem_stats_t* stats) {
    if (!stats) {
        return;
    }
    
    stats->bytes_allocated = atomic_load(&g_mem.bytes_allocated);
    stats->allocation_count = atomic_load(&g_mem.allocation_count);
    stats->total_allocations = atomic_load(&g_mem.total_allocations);
    stats->total_frees = atomic_load(&g_mem.total_frees);
    stats->total_bytes = atomic_load(&g_mem.total_bytes);
    stats->peak_bytes = atomic_load(&g_mem.peak_bytes);
    stats->peak_allocations = atomic_load(&g_mem.peak_allocations);
    stats->arena_count = atomic_load(&g_mem.arena_count);
    stats->arena_bytes = atomic_load(&g_mem.arena_bytes);
    stats->pool_count = atomic_load(&g_mem.pool_count);
    stats->pool_bytes = atomic_load(&g_mem.pool_bytes);
    
    /* bytes_committed would require platform-specific query */
    stats->bytes_committed = stats->bytes_allocated;
}

/**
 * @brief Log memory statistics
 *
 * Logs current memory stats at INFO level.
 * Useful for debugging and monitoring.
 */
void keel_mem_stats_log(void) {
    keel_mem_stats_t stats;
    keel_mem_stats_get(&stats);
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_MEM, "Memory Statistics:");
    KEEL_LOG_INFO(KEEL_LOG_CAT_MEM, "  Allocated:    %zu bytes in %zu allocations",
                 stats.bytes_allocated, stats.allocation_count);
    KEEL_LOG_INFO(KEEL_LOG_CAT_MEM, "  Peak:         %zu bytes in %zu allocations",
                 stats.peak_bytes, stats.peak_allocations);
    KEEL_LOG_INFO(KEEL_LOG_CAT_MEM, "  Total:        %zu allocations, %zu frees, %zu bytes",
                 stats.total_allocations, stats.total_frees, stats.total_bytes);
    KEEL_LOG_INFO(KEEL_LOG_CAT_MEM, "  Arenas:       %zu using %zu bytes",
                 stats.arena_count, stats.arena_bytes);
    KEEL_LOG_INFO(KEEL_LOG_CAT_MEM, "  Pools:        %zu using %zu bytes",
                 stats.pool_count, stats.pool_bytes);
}

/**
 * @brief Dump all current allocations (leak detection)
 *
 * Only available when compiled with KEEL_MEM_DEBUG.
 * Logs each allocation with its size and source location.
 *
 * Call at shutdown to detect memory leaks.
 */
void keel_mem_dump_allocations(void) {
#ifdef KEEL_MEM_DEBUG
    alloc_list_lock();
    
    size_t count = 0;
    size_t total_bytes = 0;
    
    KEEL_LOG_WARN(KEEL_LOG_CAT_MEM, "=== Memory Leak Report ===");
    
    for (keel_alloc_header_t* h = g_alloc_list; h; h = h->next) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_MEM, "  %zu bytes at %s:%d",
                     h->size, h->file ? h->file : "unknown", h->line);
        count++;
        total_bytes += h->size;
    }
    
    if (count == 0) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_MEM, "  No memory leaks detected");
    } else {
        KEEL_LOG_WARN(KEEL_LOG_CAT_MEM, "=== Total: %zu leaks, %zu bytes ===", 
                     count, total_bytes);
    }
    
    alloc_list_unlock();
#else
    KEEL_LOG_INFO(KEEL_LOG_CAT_MEM, "Allocation dump not available (KEEL_MEM_DEBUG not enabled)");
#endif
}
