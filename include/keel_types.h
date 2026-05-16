/**
 * @file types.h
 * @brief Core type definitions for KEEL
 *
 * This file defines the fundamental types used throughout KEEL.
 * Uses C23 features where available with fallbacks for compatibility.
 */

#ifndef KEEL_TYPES_H
#define KEEL_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Compiler and Language Feature Detection
 * ============================================================================ */

/* C23 [[nodiscard]] attribute */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 202311L
    #define KEEL_NODISCARD [[nodiscard]]
#elif defined(__GNUC__) || defined(__clang__)
    #define KEEL_NODISCARD __attribute__((warn_unused_result))
#else
    #define KEEL_NODISCARD
#endif

/* C23 [[maybe_unused]] attribute */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 202311L
    #define KEEL_UNUSED [[maybe_unused]]
#elif defined(__GNUC__) || defined(__clang__)
    #define KEEL_UNUSED __attribute__((unused))
#else
    #define KEEL_UNUSED
#endif

/* C23 [[deprecated]] attribute */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 202311L
    #define KEEL_DEPRECATED(msg) [[deprecated(msg)]]
#elif defined(__GNUC__) || defined(__clang__)
    #define KEEL_DEPRECATED(msg) __attribute__((deprecated(msg)))
#else
    #define KEEL_DEPRECATED(msg)
#endif

/* Force inline */
#if defined(__GNUC__) || defined(__clang__)
    #define KEEL_INLINE static inline __attribute__((always_inline))
#elif defined(_MSC_VER)
    #define KEEL_INLINE static __forceinline
#else
    #define KEEL_INLINE static inline
#endif

/* No inline */
#if defined(__GNUC__) || defined(__clang__)
    #define KEEL_NOINLINE __attribute__((noinline))
#elif defined(_MSC_VER)
    #define KEEL_NOINLINE __declspec(noinline)
#else
    #define KEEL_NOINLINE
#endif

/* Likely/Unlikely branch hints */
#if defined(__GNUC__) || defined(__clang__)
    #define KEEL_LIKELY(x)   __builtin_expect(!!(x), 1)
    #define KEEL_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
    #define KEEL_LIKELY(x)   (x)
    #define KEEL_UNLIKELY(x) (x)
#endif

/* Alignment */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
    #define KEEL_ALIGNAS(n) _Alignas(n)
    #define KEEL_ALIGNOF(t) _Alignof(t)
#elif defined(__GNUC__) || defined(__clang__)
    #define KEEL_ALIGNAS(n) __attribute__((aligned(n)))
    #define KEEL_ALIGNOF(t) __alignof__(t)
#else
    #define KEEL_ALIGNAS(n)
    #define KEEL_ALIGNOF(t) sizeof(t)
#endif

/* Cache line size (typical) */
#define KEEL_CACHE_LINE_SIZE 64

/* Cache-aligned type helper */
#define KEEL_CACHE_ALIGNED KEEL_ALIGNAS(KEEL_CACHE_LINE_SIZE)

/* Unreachable code marker */
#if defined(__GNUC__) || defined(__clang__)
    #define KEEL_UNREACHABLE() __builtin_unreachable()
#elif defined(_MSC_VER)
    #define KEEL_UNREACHABLE() __assume(0)
#else
    #define KEEL_UNREACHABLE() ((void)0)
#endif

/* Format string checking */
#if defined(__GNUC__) || defined(__clang__)
    #define KEEL_PRINTF_FMT(fmt_idx, first_arg) \
        __attribute__((format(printf, fmt_idx, first_arg)))
#else
    #define KEEL_PRINTF_FMT(fmt_idx, first_arg)
#endif

/* Non-null pointer checking */
#if defined(__GNUC__) || defined(__clang__)
    #define KEEL_NONNULL(...) __attribute__((nonnull(__VA_ARGS__)))
#else
    #define KEEL_NONNULL(...)
#endif

/* Pure function (no side effects, result depends only on args) */
#if defined(__GNUC__) || defined(__clang__)
    #define KEEL_PURE __attribute__((pure))
#else
    #define KEEL_PURE
#endif

/* Const function (pure + doesn't read global memory) */
#if defined(__GNUC__) || defined(__clang__)
    #define KEEL_CONST __attribute__((const))
#else
    #define KEEL_CONST
#endif

/* ============================================================================
 * Basic Types
 * ============================================================================ */

/** Signed size type */
typedef ptrdiff_t keel_ssize_t;

/** Handle types - opaque identifiers */
typedef uint64_t keel_handle_t;

/** Invalid handle value */
#define KEEL_INVALID_HANDLE ((keel_handle_t)0)

/** File descriptor type (platform-independent) */
typedef int keel_fd_t;

/** Invalid file descriptor */
#define KEEL_INVALID_FD ((keel_fd_t)-1)

/* ============================================================================
 * String View (Non-owning string reference)
 * ============================================================================ */

/**
 * @brief Non-owning reference to a string or binary data
 *
 * This is similar to C++17 string_view. The data may or may not be
 * null-terminated, so always use the length field.
 */
typedef struct keel_str {
    const char* data;   /**< Pointer to string data (may not be null-terminated) */
    size_t      len;    /**< Length in bytes (not including any null terminator) */
} keel_str_t;

/** Create a string view from a literal */
#define KEEL_STR(s) ((keel_str_t){ .data = (s), .len = sizeof(s) - 1 })

/** Create an empty string view */
#define KEEL_STR_EMPTY ((keel_str_t){ .data = NULL, .len = 0 })

/** Create a string view from a C string (computes length at runtime) */
KEEL_INLINE keel_str_t keel_str_from_cstr(const char* s) {
    if (s == NULL) {
        return KEEL_STR_EMPTY;
    }
    size_t len = 0;
    while (s[len] != '\0') len++;
    return (keel_str_t){ .data = s, .len = len };
}

/** Check if string view is empty */
KEEL_INLINE bool keel_str_is_empty(keel_str_t s) {
    return s.len == 0 || s.data == NULL;
}

/* ============================================================================
 * Buffer (Mutable byte buffer with capacity)
 * ============================================================================ */

/**
 * @brief Mutable byte buffer with separate length and capacity
 */
typedef struct keel_buf {
    uint8_t* data;      /**< Pointer to buffer data */
    size_t   len;       /**< Current length of data */
    size_t   cap;       /**< Total capacity of buffer */
} keel_buf_t;

/** Create an empty buffer */
#define KEEL_BUF_EMPTY ((keel_buf_t){ .data = NULL, .len = 0, .cap = 0 })

/* ============================================================================
 * Time Types
 * ============================================================================ */

/** Monotonic timestamp in nanoseconds */
typedef int64_t keel_time_t;

/** Duration in nanoseconds */
typedef int64_t keel_duration_t;

/* Time conversion macros */
#define KEEL_NS_PER_US   1000LL
#define KEEL_NS_PER_MS   1000000LL
#define KEEL_NS_PER_SEC  1000000000LL
#define KEEL_NS_PER_MIN  (60LL * KEEL_NS_PER_SEC)
#define KEEL_NS_PER_HOUR (60LL * KEEL_NS_PER_MIN)

#define KEEL_USEC(n) ((keel_duration_t)(n) * KEEL_NS_PER_US)
#define KEEL_MSEC(n) ((keel_duration_t)(n) * KEEL_NS_PER_MS)
#define KEEL_SEC(n)  ((keel_duration_t)(n) * KEEL_NS_PER_SEC)

/* ============================================================================
 * Container Helpers
 * ============================================================================ */

/**
 * @brief Get containing structure from member pointer
 *
 * This is the classic container_of macro, type-safe version.
 */
#define keel_container_of(ptr, type, member) \
    ((type*)((char*)(ptr) - offsetof(type, member)))

/**
 * @brief Array length (for static arrays only)
 */
#define keel_array_len(arr) (sizeof(arr) / sizeof((arr)[0]))

/**
 * @brief Minimum of two values
 */
#define keel_min(a, b) ((a) < (b) ? (a) : (b))

/**
 * @brief Maximum of two values
 */
#define keel_max(a, b) ((a) > (b) ? (a) : (b))

/**
 * @brief Clamp value to range [lo, hi]
 */
#define keel_clamp(x, lo, hi) keel_min(keel_max(x, lo), hi)

/**
 * @brief Check if value is power of 2
 */
KEEL_INLINE KEEL_CONST bool keel_is_power_of_2(size_t n) {
    return n != 0 && (n & (n - 1)) == 0;
}

/**
 * @brief Round up to next power of 2
 */
KEEL_INLINE KEEL_CONST size_t keel_next_power_of_2(size_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
#if SIZE_MAX > 0xFFFFFFFFU
    n |= n >> 32;
#endif
    return n + 1;
}

/**
 * @brief Align size up to alignment boundary
 */
KEEL_INLINE KEEL_CONST size_t keel_align_up(size_t size, size_t align) {
    return (size + align - 1) & ~(align - 1);
}

/* ============================================================================
 * Atomic Reference Counting
 * ============================================================================ */

/**
 * @brief Atomic reference counter
 */
typedef struct keel_refcount {
    atomic_size_t count;
} keel_refcount_t;

/** Initialize reference count to 1 */
KEEL_INLINE void keel_refcount_init(keel_refcount_t* rc) {
    atomic_init(&rc->count, 1);
}

/** Increment reference count (acquire) */
KEEL_INLINE void keel_refcount_inc(keel_refcount_t* rc) {
    atomic_fetch_add_explicit(&rc->count, 1, memory_order_relaxed);
}

/**
 * @brief Decrement reference count (release)
 * @return true if count reached zero (caller should free)
 */
KEEL_INLINE bool keel_refcount_dec(keel_refcount_t* rc) {
    if (atomic_fetch_sub_explicit(&rc->count, 1, memory_order_acq_rel) == 1) {
        atomic_thread_fence(memory_order_acquire);
        return true;
    }
    return false;
}

/** Get current reference count (for debugging) */
KEEL_INLINE size_t keel_refcount_get(const keel_refcount_t* rc) {
    /* Use a temporary to avoid casting away const */
    _Atomic size_t* cnt = (_Atomic size_t*)(uintptr_t)&rc->count;
    return atomic_load_explicit(cnt, memory_order_relaxed);
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_TYPES_H */
