/**
 * @file mem_safety.h
 * @brief Debug- and test-focused memory-safety tracking API.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This layer is intentionally heavier than the production allocator wrappers. It
 * wraps allocations with metadata, canaries, fill patterns, and optional signal-
 * driven reporting so tests and diagnostic builds can detect leaks, double frees,
 * invalid frees, and simple over/underflow corruption patterns. The tradeoff is
 * higher per-allocation cost and extra metadata retention, which is acceptable in
 * validation workflows but not ideal for steady-state hot paths.
 */

#ifndef KEEL_MEM_SAFETY_H
#define KEEL_MEM_SAFETY_H

#include "keel_types.h"
#include "keel_error.h"
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Constants
 * ============================================================================ */

/** Magic values for canary checking */
#define KEEL_CANARY_HEAD         0xDEADBEEF
#define KEEL_CANARY_TAIL         0xFEEDFACE
#define KEEL_FILL_ALLOC          0xCD    /**< Fill byte for allocated memory */
#define KEEL_FILL_FREE           0xDD    /**< Fill byte for freed memory */
#define KEEL_FILL_UNINIT         0xCC    /**< Fill byte for uninitialized */

/** Maximum tracked allocations (for bounded tracking) */
#define KEEL_MAX_TRACKED_ALLOCS  (64 * 1024)

/** Maximum backtrace depth */
#define KEEL_MAX_BACKTRACE       16

/* ============================================================================
 * Allocation States
 * ============================================================================ */

/**
 * @brief Allocation state for tracking
 */
typedef enum keel_alloc_state {
    KEEL_ALLOC_FREE = 0,         /**< Not allocated (in free pool) */
    KEEL_ALLOC_ACTIVE,           /**< Currently allocated */
    KEEL_ALLOC_FREED,            /**< Was freed (detecting double-free) */
    KEEL_ALLOC_CORRUPTED,        /**< Canary violated */
} keel_alloc_state_t;

/* ============================================================================
 * Allocation Info
 * ============================================================================ */

/**
 * @brief Information about a tracked allocation
 */
typedef struct keel_alloc_info {
    void*               ptr;            /**< User pointer */
    size_t              size;           /**< Requested size */
    size_t              actual_size;    /**< Actual allocated size with overhead */
    keel_alloc_state_t   state;          /**< Current state */
    
    /* Source location */
    const char*         alloc_file;     /**< File where allocated */
    int                 alloc_line;     /**< Line where allocated */
    const char*         free_file;      /**< File where freed (if freed) */
    int                 free_line;      /**< Line where freed */
    
    /* Timing */
    uint64_t            alloc_time;     /**< Timestamp of allocation */
    uint64_t            free_time;      /**< Timestamp of free */
    
    /* Backtrace (optional) */
    void*               backtrace[KEEL_MAX_BACKTRACE];
    int                 backtrace_depth;
    
    /* Linked list for hash bucket */
    struct keel_alloc_info* next;
} keel_alloc_info_t;

/* ============================================================================
 * Safety Report
 * ============================================================================ */

/**
 * @brief Memory safety check report
 */
typedef struct keel_mem_safety_report {
    /* Leak detection */
    size_t              leak_count;         /**< Number of leaked allocations */
    size_t              leaked_bytes;       /**< Total leaked bytes */
    
    /* Double-free detection */
    size_t              double_free_count;  /**< Double-frees detected */
    
    /* Corruption detection */
    size_t              corruption_count;   /**< Corrupted allocations */
    size_t              head_canary_fails;  /**< Head canary violations */
    size_t              tail_canary_fails;  /**< Tail canary violations */
    
    /* Statistics */
    size_t              total_allocs;       /**< Total allocations tracked */
    size_t              total_frees;        /**< Total frees tracked */
    size_t              active_allocs;      /**< Currently active */
    size_t              peak_allocs;        /**< Peak concurrent allocations */
    size_t              peak_bytes;         /**< Peak memory usage */
    
    /* Detailed leak info (first N leaks) */
    #define KEEL_MAX_LEAK_DETAILS 10
    struct {
        void*           ptr;
        size_t          size;
        const char*     file;
        int             line;
    } leaks[KEEL_MAX_LEAK_DETAILS];
    size_t              leak_details_count;
    
    /* First double-free info */
    struct {
        void*           ptr;
        const char*     first_free_file;
        int             first_free_line;
        const char*     second_free_file;
        int             second_free_line;
    } double_free_info;
    
} keel_mem_safety_report_t;

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Initialize memory safety tracking
 *
 * Must be called before any tracked allocations.
 * Safe to call multiple times.
 *
 * @return KEEL_OK on success
 */
keel_error_t keel_mem_safety_init(void);

/**
 * @brief Shutdown memory safety tracking
 *
 * Reports any remaining leaks and cleans up tracking data.
 * After shutdown, tracked allocations will not be monitored.
 */
void keel_mem_safety_shutdown(void);

/**
 * @brief Check if safety tracking is active
 */
bool keel_mem_safety_is_active(void);

/* ============================================================================
 * Tracked Allocation Functions
 * ============================================================================ */

/**
 * @brief Allocate memory with safety tracking
 *
 * @param size  Bytes to allocate
 * @param file  Source file (__FILE__)
 * @param line  Source line (__LINE__)
 * @return Pointer to user memory, or NULL on failure
 */
void* keel_safe_malloc(size_t size, const char* file, int line);

/**
 * @brief Allocate zeroed memory with safety tracking
 */
void* keel_safe_calloc(size_t count, size_t size, const char* file, int line);

/**
 * @brief Reallocate memory with safety tracking
 */
void* keel_safe_realloc(void* ptr, size_t size, const char* file, int line);

/**
 * @brief Free memory with safety tracking
 *
 * Detects:
 * - Double-free (same pointer freed twice)
 * - Invalid free (pointer not from tracked allocation)
 * - Corruption (canary violations)
 *
 * @param ptr   Pointer to free
 * @param file  Source file (__FILE__)
 * @param line  Source line (__LINE__)
 * @return KEEL_OK on success, error code on detected issue
 */
keel_error_t keel_safe_free(void* ptr, const char* file, int line);

/**
 * @brief Free and return error status silently (for testing)
 */
keel_error_t keel_safe_free_check(void* ptr, const char* file, int line);

/* ============================================================================
 * Convenience Macros
 * ============================================================================ */

#ifdef KEEL_MEM_SAFETY_ENABLED

#define KEEL_SAFE_MALLOC(size)       keel_safe_malloc((size), __FILE__, __LINE__)
#define KEEL_SAFE_CALLOC(n, size)    keel_safe_calloc((n), (size), __FILE__, __LINE__)
#define KEEL_SAFE_REALLOC(ptr, size) keel_safe_realloc((ptr), (size), __FILE__, __LINE__)
#define KEEL_SAFE_FREE(ptr)          keel_safe_free((ptr), __FILE__, __LINE__)

#else

#define KEEL_SAFE_MALLOC(size)       malloc(size)
#define KEEL_SAFE_CALLOC(n, size)    calloc(n, size)
#define KEEL_SAFE_REALLOC(ptr, size) realloc(ptr, size)
#define KEEL_SAFE_FREE(ptr)          free(ptr)

#endif

/* ============================================================================
 * Validation
 * ============================================================================ */

/**
 * @brief Validate a tracked allocation
 *
 * Checks canaries and state without freeing.
 *
 * @param ptr Pointer to validate
 * @return KEEL_OK if valid, error code otherwise
 */
keel_error_t keel_mem_safety_validate(void* ptr);

/**
 * @brief Validate all active allocations
 *
 * Scans all tracked allocations for corruption.
 *
 * @return Number of corrupted allocations found
 */
size_t keel_mem_safety_validate_all(void);

/* ============================================================================
 * Reporting
 * ============================================================================ */

/**
 * @brief Generate a structured report of the tracker state.
 *
 * @param[out] report Destination report structure.
 * @return
 */
void keel_mem_safety_check(keel_mem_safety_report_t* report);

/**
 * @brief Print human-readable leak report
 */
void keel_mem_safety_print_leaks(void);

/**
 * @brief Get current allocation count
 */
size_t keel_mem_safety_active_count(void);

/**
 * @brief Get current allocated bytes
 */
size_t keel_mem_safety_active_bytes(void);

/* ============================================================================
 * Testing Utilities
 * ============================================================================ */

/**
 * @brief Reset all tracking state (for testing)
 *
 * WARNING: Does not free tracked memory, only clears tracking.
 * Use only in controlled test scenarios.
 */
void keel_mem_safety_reset(void);

/**
 * @brief Set tracking mode flags
 */
typedef enum keel_mem_safety_flags {
    KEEL_SAFETY_TRACK_ALLOCS     = (1 << 0),  /**< Track allocations */
    KEEL_SAFETY_FILL_PATTERNS    = (1 << 1),  /**< Fill memory with patterns */
    KEEL_SAFETY_CANARY_CHECK     = (1 << 2),  /**< Use head/tail canaries */
    KEEL_SAFETY_BACKTRACE        = (1 << 3),  /**< Capture allocation backtrace */
    KEEL_SAFETY_QUARANTINE       = (1 << 4),  /**< Quarantine freed memory */
    KEEL_SAFETY_ALL              = 0x1F,
} keel_mem_safety_flags_t;

/**
 * @brief Set safety tracking flags
 *
 * @param flags Bitmask of keel_mem_safety_flags_t
 */
void keel_mem_safety_set_flags(uint32_t flags);

/**
 * @brief Get current safety flags
 */
uint32_t keel_mem_safety_get_flags(void);

/* ============================================================================
 * Signal-Based Live Dump
 * ============================================================================
 */
/**
 * @brief Install a SIGUSR1 handler that prints all active allocations.
 *
 * When the process receives SIGUSR1 (e.g., `kill -USR1 <pid>`) a full
 * memory safety report is printed to stderr.  This is the "debug signal"
 * described in the production-hardening spec and lets tooling introspect
 * pool state of a live proxy without stopping it.
 *
 * Safe to call multiple times – subsequent calls are no-ops.
 *
 * @return KEEL_OK on success, KEEL_ERR_UNKNOWN if sigaction(2) fails.
 */
keel_error_t keel_mem_safety_install_dump_signal(void);

/**
 * @brief Poll whether a SIGUSR1 dump was requested (and clear the flag).
 *
 * Call this from a safe, non-signal context (e.g., the io_uring main loop)
 * to decide when to invoke keel_mem_safety_print_leaks().
 *
 * @return true if a dump was requested since the last call, false otherwise.
 */
bool keel_mem_safety_dump_pending(void);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_MEM_SAFETY_H */
