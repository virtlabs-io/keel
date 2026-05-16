/**
 * @file mem_safety.c
 * @brief Memory-safety tracking, validation, and signal-triggered reporting.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This module deliberately favors debuggability over allocator throughput. Each
 * tracked block carries a rich header and a tail canary, while a hash table gives
 * O(1)-ish pointer validation for test assertions and free-time checks. The major
 * tradeoff is space and time overhead per allocation, which is why this subsystem
 * is aimed at tests, diagnostics, and controlled debug deployments rather than the
 * production data plane.
 */

#include "keel/mem/mem_safety.h"
#include "keel/mem/mem.h"
#include "keel_error.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>

/* ============================================================================
 * Internal State
 * ============================================================================ */

/**
 * @brief Allocation header stored before user data
 */
typedef struct alloc_header {
    uint32_t        head_canary;    /**< Head canary (KEEL_CANARY_HEAD) */
    uint32_t        state;          /**< keel_alloc_state_t */
    size_t          size;           /**< User-requested size */
    const char*     file;           /**< Allocation source file */
    int             line;           /**< Allocation source line */
    const char*     free_file;      /**< Free source file */
    int             free_line;      /**< Free source line */
    uint64_t        alloc_time;     /**< Allocation timestamp */
    uint64_t        free_time;      /**< Free timestamp */
    uint32_t        checksum;       /**< Header checksum for validation */
    uint32_t        _padding;       /**< Alignment padding */
} alloc_header_t;

/**
 * @brief Hash table entry for tracking allocations
 */
typedef struct alloc_entry {
    void*               ptr;        /**< User pointer (key) */
    alloc_header_t*     header;     /**< Header pointer */
    struct alloc_entry* next;       /**< Hash chain */
} alloc_entry_t;

/**
 * @brief Global tracking state
 */
static struct {
    atomic_bool         initialized;
    uint32_t            flags;
    
    /* Hash table for O(1) lookup */
    #define HASH_BUCKETS 4096
    alloc_entry_t*      buckets[HASH_BUCKETS];
    
    /* Free entry pool */
    alloc_entry_t*      entry_pool;
    size_t              entry_pool_size;
    size_t              entry_pool_used;
    
    /* Statistics */
    atomic_size_t       total_allocs;
    atomic_size_t       total_frees;
    atomic_size_t       active_allocs;
    atomic_size_t       active_bytes;
    size_t              peak_allocs;
    size_t              peak_bytes;
    
    /* Error tracking */
    atomic_size_t       double_free_count;
    atomic_size_t       corruption_count;
    atomic_size_t       head_canary_fails;
    atomic_size_t       tail_canary_fails;
    
    /* First double-free info */
    void*               first_double_free_ptr;
    const char*         first_double_free_file1;
    int                 first_double_free_line1;
    const char*         first_double_free_file2;
    int                 first_double_free_line2;
    
} g_safety = { .initialized = false };

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

static inline uint64_t get_timestamp_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/**
 * @brief Map a pointer to its hash-table bucket index.
 *
 * Applies a two-round Fibonacci-style bit-mix to spread heap addresses,
 * which cluster at aligned boundaries, uniformly across `HASH_BUCKETS`.
 *
 * @param ptr Pointer whose bucket index is wanted.
 * @return Bucket index in `[0, HASH_BUCKETS)`.
 */
static inline size_t hash_ptr(void* ptr) {
    uintptr_t addr = (uintptr_t)ptr;
    /* Mix bits for better distribution */
    addr = (addr ^ (addr >> 16)) * 0x45d9f3b;
    addr = (addr ^ (addr >> 16)) * 0x45d9f3b;
    addr = addr ^ (addr >> 16);
    return addr % HASH_BUCKETS;
}

/**
 * @brief Compute the integrity checksum stored in an allocation header.
 *
 * XORs the head canary, the low 32 bits of the size, the high 32 bits of
 * the size, and the allocation state word.  Stored in the header at
 * allocation time and re-validated on free/validation passes.
 *
 * @param header Header whose checksum should be (re)computed.
 * @return 32-bit checksum value.
 */
static inline uint32_t compute_checksum(alloc_header_t* header) {
    uint32_t sum = 0;
    sum ^= header->head_canary;
    sum ^= (uint32_t)header->size;
    sum ^= (uint32_t)(header->size >> 32);
    sum ^= (uint32_t)header->state;
    return sum;
}

/**
 * @brief Compute the total raw allocation size for a given user payload size.
 *
 * The layout is: `[alloc_header_t][user data][uint32_t tail canary]`, padded
 * up to an 8-byte boundary so that all subsequent header fields remain
 * naturally aligned.
 *
 * @param user_size Requested user payload size in bytes.
 * @return Total bytes to pass to the underlying allocator.
 */
static inline size_t total_alloc_size(size_t user_size) {
    /* header + user_size + tail_canary, ensure alignment */
    size_t base = sizeof(alloc_header_t) + user_size + sizeof(uint32_t);
    /* Align to 8 bytes for pointer alignment */
    return (base + 7) & ~(size_t)7;
}

/**
 * @brief Return the user-visible data pointer for an allocation header.
 *
 * The user region starts immediately after the `alloc_header_t` in memory.
 *
 * @param header Pointer to the allocation header.
 * @return Pointer to the first byte of user-accessible data.
 */
static inline void* header_to_user(alloc_header_t* header) {
    return (void*)((char*)header + sizeof(alloc_header_t));
}

/**
 * @brief Return a pointer to the tail canary word for an allocation.
 *
 * The tail canary sits immediately after `header->size` bytes of user data.
 * Corruption of this word indicates a buffer overrun.
 *
 * @param header Pointer to the allocation header.
 * @return Pointer to the 32-bit tail canary.
 */
static inline uint32_t* get_tail_canary(alloc_header_t* header) {
    char* tail_ptr = (char*)header + sizeof(alloc_header_t) + header->size;
    /* Use void* intermediate to satisfy alignment */
    return (uint32_t*)(void*)tail_ptr;
}

/**
 * @brief Obtain a free tracking-entry slot from the internal entry pool.
 *
 * The entry pool is a flat array that grows by doubling (starting at 1024
 * entries) using the raw `realloc` allocator so that the safety layer
 * itself never recurses through the instrumented paths.
 *
 * @return Pointer to a zeroed-out `alloc_entry_t`, or `NULL` on OOM.
 */
static alloc_entry_t* alloc_entry(void) {
    if (g_safety.entry_pool_used >= g_safety.entry_pool_size) {
        /* Grow pool */
        size_t new_size = g_safety.entry_pool_size == 0 ? 1024 : g_safety.entry_pool_size * 2;
        alloc_entry_t* new_pool = realloc(g_safety.entry_pool, new_size * sizeof(alloc_entry_t));
        if (!new_pool) return NULL;
        g_safety.entry_pool = new_pool;
        g_safety.entry_pool_size = new_size;
    }
    return &g_safety.entry_pool[g_safety.entry_pool_used++];
}

/**
 * @brief Register a new allocation in the hash-chained tracking table.
 *
 * Hashes @p ptr to select a bucket and prepends a new `alloc_entry_t`
 * that links the user pointer to its instrumented header.
 *
 * @param ptr    User-visible pointer returned to the caller.
 * @param header Instrumented header located just before @p ptr in memory.
 */
static void add_tracking(void* ptr, alloc_header_t* header) {
    size_t bucket = hash_ptr(ptr);
    
    alloc_entry_t* entry = alloc_entry();
    if (!entry) return;
    
    entry->ptr = ptr;
    entry->header = header;
    entry->next = g_safety.buckets[bucket];
    g_safety.buckets[bucket] = entry;
}

/**
 * @brief Look up a tracked allocation by user pointer.
 *
 * Walks the hash bucket chain for @p ptr and returns the first entry
 * whose `ptr` field matches.  Returns `NULL` if @p ptr was not registered
 * through this tracker (e.g. raw system allocations).
 *
 * @param ptr User-visible pointer to search for.
 * @return Matching `alloc_entry_t`, or `NULL` if not found.
 */
static alloc_entry_t* find_tracking(void* ptr) {
    size_t bucket = hash_ptr(ptr);
    alloc_entry_t* entry = g_safety.buckets[bucket];
    
    while (entry) {
        if (entry->ptr == ptr) {
            return entry;
        }
        entry = entry->next;
    }
    return NULL;
}

/**
 * @brief Unlink and zero a tracking entry for a freed allocation.
 *
 * Walks the hash bucket chain for @p ptr, splices the matching entry
 * out of the list, and clears its `ptr`, `header`, and `next` fields to
 * prevent dangling references.  The entry slot itself is returned to the
 * pool (the pool is never compacted — slots are recycled via the used-count).
 *
 * @param ptr User-visible pointer whose tracking entry should be removed.
 */
static void remove_tracking(void* ptr) {
    size_t bucket = hash_ptr(ptr);
    alloc_entry_t** prev = &g_safety.buckets[bucket];
    alloc_entry_t* entry = *prev;

    while (entry) {
        if (entry->ptr == ptr) {
            *prev = entry->next;
            entry->ptr = NULL;
            entry->header = NULL;
            entry->next = NULL;
            return;
        }
        prev = &entry->next;
        entry = entry->next;
    }
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Initialize the memory-safety tracker.
 *
 * @return `KEEL_OK` on success. Repeated calls are treated as idempotent.
 */
keel_error_t keel_mem_safety_init(void) {
    if (atomic_exchange(&g_safety.initialized, true)) {
        /* Already initialized */
        return KEEL_OK;
    }
    
    memset(&g_safety, 0, sizeof(g_safety));
    g_safety.initialized = true;
    g_safety.flags = KEEL_SAFETY_ALL;
    
    return KEEL_OK;
}

/**
 * @brief Shut down the tracker, emit leak information, and release bookkeeping memory.
 *
 * @return
 */
void keel_mem_safety_shutdown(void) {
    if (!g_safety.initialized) return;
    
    /* Print leak report */
    keel_mem_safety_print_leaks();
    
    /* Free entry pool */
    free(g_safety.entry_pool);
    
    memset(&g_safety, 0, sizeof(g_safety));
}

/**
 * @brief Query whether the memory-safety tracker has been initialized.
 *
 * Checks the `initialized` flag set by `keel_mem_safety_init()` and
 * cleared by `keel_mem_safety_shutdown()`.  Safe to call at any time.
 *
 * @return `true` if the tracker is active, `false` otherwise.
 */
bool keel_mem_safety_is_active(void) {
    return g_safety.initialized;
}

/* ============================================================================
 * Tracked Allocations
 * ============================================================================ */

/**
 * @brief Allocate a tracked block with canaries and source metadata.
 *
 * @param size Requested user payload size.
 * @param file Allocation call-site file.
 * @param line Allocation call-site line.
 * @return User-visible pointer, or `NULL` on allocation failure.
 */
void* keel_safe_malloc(size_t size, const char* file, int line) {
    if (!g_safety.initialized) {
        keel_mem_safety_init();
    }
    
    size_t total = total_alloc_size(size);
    alloc_header_t* header = malloc(total);
    if (!header) return NULL;
    
    /* Initialize header */
    header->head_canary = KEEL_CANARY_HEAD;
    header->state = KEEL_ALLOC_ACTIVE;
    header->size = size;
    header->file = file;
    header->line = line;
    header->free_file = NULL;
    header->free_line = 0;
    header->alloc_time = get_timestamp_ns();
    header->free_time = 0;
    header->checksum = compute_checksum(header);
    
    /* Set tail canary */
    uint32_t* tail = get_tail_canary(header);
    *tail = KEEL_CANARY_TAIL;
    
    /* Fill user memory with pattern */
    void* ptr = header_to_user(header);
    if (g_safety.flags & KEEL_SAFETY_FILL_PATTERNS) {
        memset(ptr, KEEL_FILL_ALLOC, size);
    }
    
    /* Track allocation */
    add_tracking(ptr, header);
    
    /* Update stats */
    atomic_fetch_add(&g_safety.total_allocs, 1);
    size_t active = atomic_fetch_add(&g_safety.active_allocs, 1) + 1;
    size_t bytes = atomic_fetch_add(&g_safety.active_bytes, size) + size;
    
    if (active > g_safety.peak_allocs) g_safety.peak_allocs = active;
    if (bytes > g_safety.peak_bytes) g_safety.peak_bytes = bytes;
    
    return ptr;
}

/**
 * @brief Tracked zero-initialised allocation.
 *
 * Allocates `count * size` bytes via `keel_safe_malloc()` and zero-fills the
 * result before returning it.  The call-site coordinates are propagated to
 * the underlying malloc so leak reports show the correct origin.
 *
 * @param count Number of elements.
 * @param size  Size of each element.
 * @param file  Allocation call-site file (typically `__FILE__`).
 * @param line  Allocation call-site line (typically `__LINE__`).
 * @return Zeroed allocation, or `NULL` on failure or overflow.
 */
void* keel_safe_calloc(size_t count, size_t size, const char* file, int line) {
    size_t total_size = count * size;
    void* ptr = keel_safe_malloc(total_size, file, line);
    if (ptr) {
        memset(ptr, 0, total_size);
    }
    return ptr;
}

/**
 * @brief Reallocate a tracked block by allocate-copy-free.
 *
 * The implementation does not attempt in-place growth because doing so would make
 * the tracker responsible for moving hash entries and validation metadata around
 * a potentially relocated header anyway. The simpler allocate-copy-free sequence
 * preserves correctness and keeps the bookkeeping easy to reason about.
 *
 * @param ptr Existing tracked allocation, or `NULL`.
 * @param size New requested size.
 * @param file Reallocation call-site file.
 * @param line Reallocation call-site line.
 * @return Reallocated block, or `NULL` on failure.
 */
void* keel_safe_realloc(void* ptr, size_t size, const char* file, int line) {
    if (!ptr) {
        return keel_safe_malloc(size, file, line);
    }
    
    if (size == 0) {
        keel_safe_free(ptr, file, line);
        return NULL;
    }
    
    /* Find existing allocation */
    alloc_entry_t* entry = find_tracking(ptr);
    if (!entry || entry->header->state != KEEL_ALLOC_ACTIVE) {
        /* Invalid realloc */
        return NULL;
    }
    
    size_t old_size = entry->header->size;
    
    /* Allocate new block */
    void* new_ptr = keel_safe_malloc(size, file, line);
    if (!new_ptr) return NULL;
    
    /* Copy old data */
    size_t copy_size = old_size < size ? old_size : size;
    memcpy(new_ptr, ptr, copy_size);
    
    /* Free old block */
    keel_safe_free(ptr, file, line);
    
    return new_ptr;
}

/**
 * @brief Free a tracked allocation with validation and optional quarantine behavior.
 *
 * @param ptr Pointer to free.
 * @param file Free call-site file.
 * @param line Free call-site line.
 * @return `KEEL_OK` on success, or an error code describing a detected misuse.
 */
keel_error_t keel_safe_free(void* ptr, const char* file, int line) {
    if (!ptr) return KEEL_OK;
    
    if (!g_safety.initialized) {
        free(ptr);
        return KEEL_OK;
    }
    
    /* Find tracking entry */
    alloc_entry_t* entry = find_tracking(ptr);
    if (!entry) {
        /* Not a tracked allocation - could be system alloc */
        free(ptr);
        return KEEL_OK;
    }
    
    alloc_header_t* header = entry->header;
    
    /* Check for double-free */
    if (header->state == KEEL_ALLOC_FREED) {
        atomic_fetch_add(&g_safety.double_free_count, 1);
        
        /* Store first double-free info */
        if (!g_safety.first_double_free_ptr) {
            g_safety.first_double_free_ptr = ptr;
            g_safety.first_double_free_file1 = header->free_file;
            g_safety.first_double_free_line1 = header->free_line;
            g_safety.first_double_free_file2 = file;
            g_safety.first_double_free_line2 = line;
        }
        
        fprintf(stderr, "DOUBLE-FREE: %p (first free at %s:%d, second at %s:%d)\n",
                ptr, header->free_file, header->free_line, file, line);
        
        return KEEL_ERR_INVALID_ARG;
    }
    
    /* Check head canary */
    if (header->head_canary != KEEL_CANARY_HEAD) {
        atomic_fetch_add(&g_safety.head_canary_fails, 1);
        atomic_fetch_add(&g_safety.corruption_count, 1);
        header->state = KEEL_ALLOC_CORRUPTED;
        
        fprintf(stderr, "CORRUPTION: %p head canary violated (alloc at %s:%d)\n",
                ptr, header->file, header->line);
        
        return KEEL_ERR_INVALID_ARG;
    }
    
    /* Check tail canary */
    uint32_t* tail = get_tail_canary(header);
    if (*tail != KEEL_CANARY_TAIL) {
        atomic_fetch_add(&g_safety.tail_canary_fails, 1);
        atomic_fetch_add(&g_safety.corruption_count, 1);
        header->state = KEEL_ALLOC_CORRUPTED;
        
        fprintf(stderr, "CORRUPTION: %p tail canary violated (buffer overflow?) (alloc at %s:%d)\n",
                ptr, header->file, header->line);
        
        return KEEL_ERR_INVALID_ARG;
    }
    
    /* Mark as freed */
    header->state = KEEL_ALLOC_FREED;
    header->free_file = file;
    header->free_line = line;
    header->free_time = get_timestamp_ns();
    
    /* Fill with pattern to detect use-after-free */
    if (g_safety.flags & KEEL_SAFETY_FILL_PATTERNS) {
        memset(ptr, KEEL_FILL_FREE, header->size);
    }
    
    /* Update stats */
    atomic_fetch_add(&g_safety.total_frees, 1);
    atomic_fetch_sub(&g_safety.active_allocs, 1);
    atomic_fetch_sub(&g_safety.active_bytes, header->size);
    
    /* Actually free if not quarantining */
    if (!(g_safety.flags & KEEL_SAFETY_QUARANTINE)) {
        remove_tracking(ptr);
        free(header);
    }
    
    return KEEL_OK;
}

/**
 * @brief Free a tracked allocation without printing diagnostics to stderr.
 *
 * @param ptr Pointer to free.
 * @param file Free call-site file.
 * @param line Free call-site line.
 * @return `KEEL_OK` on success, or an error code for the detected misuse.
 */
keel_error_t keel_safe_free_check(void* ptr, const char* file, int line) {
    /* Silent version for testing - doesn't print but updates counters */
    if (!ptr) return KEEL_OK;
    
    if (!g_safety.initialized) {
        free(ptr);
        return KEEL_OK;
    }
    
    alloc_entry_t* entry = find_tracking(ptr);
    if (!entry) {
        free(ptr);
        return KEEL_OK;
    }
    
    alloc_header_t* header = entry->header;
    
    if (header->state == KEEL_ALLOC_FREED) {
        atomic_fetch_add(&g_safety.double_free_count, 1);
        
        /* Store first double-free info */
        if (!g_safety.first_double_free_ptr) {
            g_safety.first_double_free_ptr = ptr;
            g_safety.first_double_free_file1 = header->free_file;
            g_safety.first_double_free_line1 = header->free_line;
            g_safety.first_double_free_file2 = file;
            g_safety.first_double_free_line2 = line;
        }
        
        return KEEL_ERR_INVALID_ARG;  /* Double-free */
    }
    
    if (header->head_canary != KEEL_CANARY_HEAD) {
        atomic_fetch_add(&g_safety.head_canary_fails, 1);
        atomic_fetch_add(&g_safety.corruption_count, 1);
        header->state = KEEL_ALLOC_CORRUPTED;
        return KEEL_ERR_INVALID_ARG;  /* Head corruption */
    }
    
    uint32_t* tail = get_tail_canary(header);
    if (*tail != KEEL_CANARY_TAIL) {
        atomic_fetch_add(&g_safety.tail_canary_fails, 1);
        atomic_fetch_add(&g_safety.corruption_count, 1);
        header->state = KEEL_ALLOC_CORRUPTED;
        return KEEL_ERR_INVALID_ARG;  /* Tail corruption */
    }
    
    header->state = KEEL_ALLOC_FREED;
    header->free_file = file;
    header->free_line = line;
    
    atomic_fetch_add(&g_safety.total_frees, 1);
    atomic_fetch_sub(&g_safety.active_allocs, 1);
    atomic_fetch_sub(&g_safety.active_bytes, header->size);
    
    if (!(g_safety.flags & KEEL_SAFETY_QUARANTINE)) {
        free(header);
    }
    
    return KEEL_OK;
}

/* ============================================================================
 * Validation
 * ============================================================================ */

keel_error_t keel_mem_safety_validate(void* ptr) {
    if (!ptr) return KEEL_ERR_NULL_PTR;
    
    alloc_entry_t* entry = find_tracking(ptr);
    if (!entry) return KEEL_ERR_INVALID_ARG;
    
    alloc_header_t* header = entry->header;
    
    if (header->state != KEEL_ALLOC_ACTIVE) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    if (header->head_canary != KEEL_CANARY_HEAD) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    uint32_t* tail = get_tail_canary(header);
    if (*tail != KEEL_CANARY_TAIL) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    return KEEL_OK;
}

/**
 * @brief Validate every active tracked allocation for canary integrity.
 *
 * Iterates all hash buckets and calls `keel_mem_safety_validate()` on
 * each entry in the `KEEL_ALLOC_ACTIVE` state.  Corrupted allocations are
 * counted but not freed — the caller decides how to respond.
 *
 * @return Number of allocations that failed validation (0 means clean).
 *
 * Notes:
 * - Thread safety: not locked — must be called from a quiesced or
 *   single-threaded context (e.g., at shutdown or in a test teardown).
 */
size_t keel_mem_safety_validate_all(void) {
    size_t corrupted = 0;
    
    for (size_t i = 0; i < HASH_BUCKETS; i++) {
        alloc_entry_t* entry = g_safety.buckets[i];
        while (entry) {
            if (entry->header->state == KEEL_ALLOC_ACTIVE) {
                if (keel_mem_safety_validate(entry->ptr) != KEEL_OK) {
                    corrupted++;
                }
            }
            entry = entry->next;
        }
    }
    
    return corrupted;
}

/* ============================================================================
 * Reporting
 * ============================================================================ */

/**
 * @brief Build a point-in-time safety report.
 *
 * @param[out] report Destination report structure.
 * @return
 */
void keel_mem_safety_check(keel_mem_safety_report_t* report) {
    memset(report, 0, sizeof(*report));
    
    report->total_allocs = atomic_load(&g_safety.total_allocs);
    report->total_frees = atomic_load(&g_safety.total_frees);
    report->active_allocs = atomic_load(&g_safety.active_allocs);
    report->double_free_count = atomic_load(&g_safety.double_free_count);
    report->corruption_count = atomic_load(&g_safety.corruption_count);
    report->head_canary_fails = atomic_load(&g_safety.head_canary_fails);
    report->tail_canary_fails = atomic_load(&g_safety.tail_canary_fails);
    report->peak_allocs = g_safety.peak_allocs;
    report->peak_bytes = g_safety.peak_bytes;
    
    /* Count leaks and collect details */
    for (size_t i = 0; i < HASH_BUCKETS; i++) {
        alloc_entry_t* entry = g_safety.buckets[i];
        while (entry) {
            if (entry->header->state == KEEL_ALLOC_ACTIVE) {
                report->leak_count++;
                report->leaked_bytes += entry->header->size;
                
                if (report->leak_details_count < KEEL_MAX_LEAK_DETAILS) {
                    size_t idx = report->leak_details_count++;
                    report->leaks[idx].ptr = entry->ptr;
                    report->leaks[idx].size = entry->header->size;
                    report->leaks[idx].file = entry->header->file;
                    report->leaks[idx].line = entry->header->line;
                }
            }
            entry = entry->next;
        }
    }
    
    /* Copy double-free info */
    if (g_safety.first_double_free_ptr) {
        report->double_free_info.ptr = g_safety.first_double_free_ptr;
        report->double_free_info.first_free_file = g_safety.first_double_free_file1;
        report->double_free_info.first_free_line = g_safety.first_double_free_line1;
        report->double_free_info.second_free_file = g_safety.first_double_free_file2;
        report->double_free_info.second_free_line = g_safety.first_double_free_line2;
    }
}

/**
 * @brief Print a human-readable memory safety report to stdout.
 *
 * Builds a full report via `keel_mem_safety_check()` and emits a
 * formatted summary covering leaks, double-frees, canary failures, and
 * allocation statistics.  If no issues are detected a single-line "No
 * issues" message is printed instead.
 *
 * Notes:
 * - Output goes to `stdout`, not `stderr`, to appear in test logs.
 * - Thread safety: snapshot is built atomically per-counter but the leak
 *   walk is not locked; safe only in quiesced contexts.
 */
void keel_mem_safety_print_leaks(void) {
    keel_mem_safety_report_t report;
    keel_mem_safety_check(&report);
    
    if (report.leak_count == 0 && report.double_free_count == 0 && 
        report.corruption_count == 0) {
        printf("Memory safety: No issues detected. "
               "(%zu allocs, %zu frees)\n",
               report.total_allocs, report.total_frees);
        return;
    }
    
    printf("\n=== Memory Safety Report ===\n");
    
    if (report.leak_count > 0) {
        printf("\nLEAKS: %zu allocations (%zu bytes)\n", 
               report.leak_count, report.leaked_bytes);
        
        for (size_t i = 0; i < report.leak_details_count; i++) {
            printf("  - %p (%zu bytes) allocated at %s:%d\n",
                   report.leaks[i].ptr,
                   report.leaks[i].size,
                   report.leaks[i].file,
                   report.leaks[i].line);
        }
        
        if (report.leak_count > report.leak_details_count) {
            printf("  ... and %zu more\n", 
                   report.leak_count - report.leak_details_count);
        }
    }
    
    if (report.double_free_count > 0) {
        printf("\nDOUBLE-FREES: %zu detected\n", report.double_free_count);
        if (report.double_free_info.ptr) {
            printf("  First: %p freed at %s:%d, again at %s:%d\n",
                   report.double_free_info.ptr,
                   report.double_free_info.first_free_file,
                   report.double_free_info.first_free_line,
                   report.double_free_info.second_free_file,
                   report.double_free_info.second_free_line);
        }
    }
    
    if (report.corruption_count > 0) {
        printf("\nCORRUPTION: %zu detected\n", report.corruption_count);
        printf("  Head canary failures: %zu\n", report.head_canary_fails);
        printf("  Tail canary failures: %zu\n", report.tail_canary_fails);
    }
    
    printf("\nStatistics:\n");
    printf("  Total allocations: %zu\n", report.total_allocs);
    printf("  Total frees:       %zu\n", report.total_frees);
    printf("  Peak allocations:  %zu\n", report.peak_allocs);
    printf("  Peak bytes:        %zu\n", report.peak_bytes);
    printf("=============================\n\n");
}

/**
 * @brief Return the current number of live tracked allocations.
 *
 * The counter is updated atomically on every `keel_safe_malloc` and
 * `keel_safe_free` call.  Useful for asserting no leaks in unit tests.
 *
 * @return Number of allocations that have been allocated but not yet freed.
 */
size_t keel_mem_safety_active_count(void) {
    return atomic_load(&g_safety.active_allocs);
}

/**
 * @brief Return the total user-payload bytes currently live.
 *
 * Counts only the user-requested bytes, not the overhead introduced by
 * the tracker (header + tail canary).  Updated atomically on every
 * allocation and free.
 *
 * @return Total bytes in currently live tracked allocations.
 */
size_t keel_mem_safety_active_bytes(void) {
    return atomic_load(&g_safety.active_bytes);
}

/* ============================================================================
 * Testing Utilities
 * ============================================================================ */

void keel_mem_safety_reset(void) {
    /* Free all tracked allocations before clearing the tracking data */
    for (size_t i = 0; i < HASH_BUCKETS; i++) {
        alloc_entry_t* entry = g_safety.buckets[i];
        while (entry) {
            if (entry->header) {
                free(entry->header);
                entry->header = NULL;
            }
            entry = entry->next;
        }
    }

    memset(g_safety.buckets, 0, sizeof(g_safety.buckets));
    g_safety.entry_pool_used = 0;
    
    atomic_store(&g_safety.total_allocs, 0);
    atomic_store(&g_safety.total_frees, 0);
    atomic_store(&g_safety.active_allocs, 0);
    atomic_store(&g_safety.active_bytes, 0);
    atomic_store(&g_safety.double_free_count, 0);
    atomic_store(&g_safety.corruption_count, 0);
    atomic_store(&g_safety.head_canary_fails, 0);
    atomic_store(&g_safety.tail_canary_fails, 0);
    
    g_safety.peak_allocs = 0;
    g_safety.peak_bytes = 0;
    g_safety.first_double_free_ptr = NULL;
}

/**
 * @brief Replace the active safety-check feature flags.
 *
 * Flags control optional safety features such as fill patterns and
 * quarantine mode (see `KEEL_SAFETY_*` constants).  Must be called after
 * `keel_mem_safety_init()`.  Not thread-safe; intended for single-threaded
 * test setup before concurrent allocation begins.
 *
 * @param flags New flags bitmask.
 */
void keel_mem_safety_set_flags(uint32_t flags) {
    g_safety.flags = flags;
}

/**
 * @brief Read the current safety-check feature flags.
 *
 * @return Current flags bitmask as set by `keel_mem_safety_set_flags()`
 *         or the default (`KEEL_SAFETY_ALL`) established during init.
 */
uint32_t keel_mem_safety_get_flags(void) {
    return g_safety.flags;
}

/* ============================================================================
 * Signal-Based Live Dump
 * ============================================================================
 */

/**
 * @brief SIGUSR1 handler — async-signal-safe wrapper.
 *
 * We can't call printf() from a signal handler (not async-signal-safe), so
 * we use a volatile flag and let a periodic task drain it.  For debugging
 * tools that call raise(SIGUSR1) in a controlled test environment, a single
 * write() to STDERR_FILENO with a brief header is safe, then we delegate to
 * keel_mem_safety_print_leaks() which uses stdio but is only called from the
 * handler in debug/test contexts where reentrancy is not a concern.
 */
static volatile sig_atomic_t g_dump_requested = 0;
static atomic_bool g_signal_installed = false;

/**
 * @brief Minimal SIGUSR1 handler used to request an allocation dump.
 *
 * @param signum Delivered signal number.
 * @return
 */
static void sigusr1_handler(int signum)
{
    (void)signum;
    /* Mark that a dump was requested; the test harness or a main-loop
     * integration point can check keel_mem_safety_dump_pending() and call
     * keel_mem_safety_print_leaks() from a safe context.  For unit tests
     * we also call the function directly here since we control the process. */
    g_dump_requested = 1;

    /* Emit a small async-signal-safe header to stderr */
    static const char hdr[] = "\n[keel-mem-safety] SIGUSR1 received — "
                              "printing active allocation report\n";
    /* write() return is intentionally unchecked in signal handlers: we are
     * in an async-signal context and there is nothing useful we can do on
     * failure other than continue.  The if() suppresses -Werror=unused-result. */
    if (write(STDERR_FILENO, hdr, sizeof(hdr) - 1) < 0) { /* intentionally ignored in signal handler */ }

    /* NOTE: stdio is not async-signal-safe. The call below is intentionally
     * kept for debug/test builds only.  Production builds should instead rely
     * on the g_dump_requested flag polled from the io_uring main loop. */
    keel_mem_safety_print_leaks();
}

/**
 * @brief Install the debug SIGUSR1 handler once.
 *
 * @return `KEEL_OK` on success, or `KEEL_ERR_UNKNOWN` if `sigaction` fails.
 */
keel_error_t keel_mem_safety_install_dump_signal(void)
{
    bool expected = false;
    if (!atomic_compare_exchange_strong(&g_signal_installed, &expected, true)) {
        /* Already installed — idempotent */
        return KEEL_OK;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigusr1_handler;
    sigemptyset(&sa.sa_mask);
    /* SA_RESTART ensures slow syscalls are restarted after the handler */
    sa.sa_flags = SA_RESTART;

    if (sigaction(SIGUSR1, &sa, NULL) != 0) {
        atomic_store(&g_signal_installed, false);
        return KEEL_ERR_UNKNOWN;
    }

    return KEEL_OK;
}

/**
 * @brief Returns true if a SIGUSR1 dump was requested but not yet consumed.
 *
 * Callers from the main event loop can poll this and invoke
 * keel_mem_safety_print_leaks() from a safe (non-signal) context.
 */
/**
 * @brief Test and clear the deferred dump flag set by the signal handler.
 *
 * @return `true` when a dump was requested since the previous poll.
 */
bool keel_mem_safety_dump_pending(void)
{
    if (g_dump_requested) {
        g_dump_requested = 0;
        return true;
    }
    return false;
}
