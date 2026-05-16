/**
 * @file string_view.h
 * @brief Zero-copy string-view primitives for parsing and classification code.
 *
 * `keel_str_view_t` is KEEL's non-owning string slice. It lets protocol,
 * configuration, and SQL code point directly into an existing buffer instead
 * of allocating transient substrings. That keeps parsing hot paths cheaper,
 * especially when most tokens are inspected briefly and never need to become
 * owning strings.
 *
 * The main tradeoff is lifetime coupling: a view is only valid while the
 * underlying bytes remain alive and unmodified. The API therefore emphasizes
 * explicit construction and simple value semantics so callers can reason about
 * aliasing instead of hiding copies behind richer abstractions.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_STRING_VIEW_H
#define KEEL_STRING_VIEW_H

#include "keel_types.h"
#include <stddef.h>
#include <stdbool.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * String View Type
 * ============================================================================ */

/**
 * @brief Non-owning view into a contiguous character range.
 *
 * The slice may refer to a full string, a token inside a larger packet, or any
 * other byte range. No ownership is implied and the bytes are not required to
 * be null-terminated.
 */
typedef struct keel_str_view {
    const char* data;   /**< Pointer to string data (NOT null-terminated) */
    size_t      len;    /**< Length in bytes */
} keel_str_view_t;

/* ============================================================================
 * Construction
 * ============================================================================ */

/**
 * @brief Construct a view from explicit pointer and length.
 *
 * @param data Pointer to the first byte of the viewed range.
 * @param len Length in bytes.
 * @return Lightweight string view that aliases the original storage.
 */
KEEL_INLINE keel_str_view_t keel_str_view(const char* data, size_t len) {
    return (keel_str_view_t){ .data = data, .len = len };
}

/**
 * @brief Construct a view from a null-terminated C string.
 *
 * The length is computed eagerly with `strlen()`, so callers in hot loops may
 * prefer `keel_str_view()` when the length is already known.
 *
 * @param cstr Null-terminated source string.
 * @return View excluding the trailing null terminator.
 */
KEEL_INLINE keel_str_view_t keel_str_view_cstr(const char* cstr) {
    return (keel_str_view_t){ 
        .data = cstr, 
        .len = cstr ? strlen(cstr) : 0 
    };
}

/**
 * @brief Create an empty string view
 * @return Empty string view with data=NULL, len=0
 */
KEEL_INLINE keel_str_view_t keel_str_view_empty(void) {
    return (keel_str_view_t){ .data = NULL, .len = 0 };
}

/**
 * @brief Create a string view from a keel_str_t
 * 
 * @param str  keel_str_t structure
 * @return String view
 */
KEEL_INLINE keel_str_view_t keel_str_view_from_str(keel_str_t str) {
    return (keel_str_view_t){ .data = str.data, .len = str.len };
}

/* ============================================================================
 * Properties
 * ============================================================================ */

/**
 * @brief Check if string view is empty
 */
KEEL_INLINE bool keel_str_view_is_empty(keel_str_view_t sv) {
    return sv.len == 0;
}

/**
 * @brief Check whether the view points at some underlying storage.
 *
 * An empty-but-valid view may still have `len == 0` and non-`NULL` data. This
 * predicate is therefore about pointer presence, not semantic emptiness.
 *
 * @param sv View to inspect.
 * @return true when `sv.data` is non-`NULL`.
 */
KEEL_INLINE bool keel_str_view_is_valid(keel_str_view_t sv) {
    return sv.data != NULL;
}

/* ============================================================================
 * Comparison
 * ============================================================================ */

/**
 * @brief Compare two string views for equality
 *
 * @param a  First string view
 * @param b  Second string view
 * @return true if equal
 */
KEEL_INLINE bool keel_str_view_eq(keel_str_view_t a, keel_str_view_t b) {
    if (a.len != b.len) return false;
    if (a.len == 0) return true;
    if (a.data == b.data) return true;
    return memcmp(a.data, b.data, a.len) == 0;
}

/**
 * @brief Compare string view with C string
 *
 * @param sv   String view
 * @param cstr Null-terminated C string
 * @return true if equal
 */
KEEL_INLINE bool keel_str_view_eq_cstr(keel_str_view_t sv, const char* cstr) {
    if (!cstr) return sv.len == 0;
    size_t clen = strlen(cstr);
    if (sv.len != clen) return false;
    if (clen == 0) return true;
    return memcmp(sv.data, cstr, clen) == 0;
}

/**
 * @brief Compare two views case-insensitively using ASCII folding.
 *
 * This is intended for protocol and SQL keywords, not locale-sensitive text.
 *
 * @param a First view.
 * @param b Second view.
 * @return true if both views are equal after ASCII case folding.
 */
bool keel_str_view_eq_nocase(keel_str_view_t a, keel_str_view_t b);

/**
 * @brief Case-insensitive comparison with C string
 *
 * @param sv   String view
 * @param cstr Null-terminated C string
 * @return true if equal (case-insensitive)
 */
bool keel_str_view_eq_cstr_nocase(keel_str_view_t sv, const char* cstr);

/**
 * @brief Compare string views lexicographically
 *
 * @param a  First string view
 * @param b  Second string view
 * @return <0 if a<b, 0 if a==b, >0 if a>b
 */
KEEL_INLINE int keel_str_view_cmp(keel_str_view_t a, keel_str_view_t b) {
    size_t min_len = a.len < b.len ? a.len : b.len;
    int cmp = min_len > 0 ? memcmp(a.data, b.data, min_len) : 0;
    if (cmp != 0) return cmp;
    if (a.len < b.len) return -1;
    if (a.len > b.len) return 1;
    return 0;
}

/* ============================================================================
 * Slicing
 * ============================================================================ */

/**
 * @brief Get substring from start position
 *
 * @param sv    String view
 * @param start Start position (0-indexed)
 * @return Substring from start to end, or empty if out of bounds
 */
KEEL_INLINE keel_str_view_t keel_str_view_substr(keel_str_view_t sv, size_t start) {
    if (start >= sv.len) return keel_str_view_empty();
    return keel_str_view(sv.data + start, sv.len - start);
}

/**
 * @brief Get substring with start and length
 *
 * @param sv    String view
 * @param start Start position (0-indexed)
 * @param len   Maximum length
 * @return Substring, clamped to available length
 */
KEEL_INLINE keel_str_view_t keel_str_view_substr_len(keel_str_view_t sv, size_t start, size_t len) {
    if (start >= sv.len) return keel_str_view_empty();
    size_t remaining = sv.len - start;
    size_t actual_len = len < remaining ? len : remaining;
    return keel_str_view(sv.data + start, actual_len);
}

/**
 * @brief Get first N characters
 *
 * @param sv String view
 * @param n  Number of characters
 * @return Prefix, clamped to available length
 */
KEEL_INLINE keel_str_view_t keel_str_view_prefix(keel_str_view_t sv, size_t n) {
    size_t len = n < sv.len ? n : sv.len;
    return keel_str_view(sv.data, len);
}

/**
 * @brief Get last N characters
 *
 * @param sv String view
 * @param n  Number of characters
 * @return Suffix, clamped to available length
 */
KEEL_INLINE keel_str_view_t keel_str_view_suffix(keel_str_view_t sv, size_t n) {
    if (n >= sv.len) return sv;
    return keel_str_view(sv.data + sv.len - n, n);
}

/**
 * @brief Remove prefix from string view
 *
 * @param sv String view
 * @param n  Number of characters to remove
 * @return String view with prefix removed
 */
KEEL_INLINE keel_str_view_t keel_str_view_remove_prefix(keel_str_view_t sv, size_t n) {
    if (n >= sv.len) return keel_str_view_empty();
    return keel_str_view(sv.data + n, sv.len - n);
}

/**
 * @brief Remove suffix from string view
 *
 * @param sv String view
 * @param n  Number of characters to remove
 * @return String view with suffix removed
 */
KEEL_INLINE keel_str_view_t keel_str_view_remove_suffix(keel_str_view_t sv, size_t n) {
    if (n >= sv.len) return keel_str_view_empty();
    return keel_str_view(sv.data, sv.len - n);
}

/* ============================================================================
 * Searching
 * ============================================================================ */

/**
 * @brief Check if string view starts with prefix
 *
 * @param sv     String view
 * @param prefix Prefix to check
 * @return true if sv starts with prefix
 */
KEEL_INLINE bool keel_str_view_starts_with(keel_str_view_t sv, keel_str_view_t prefix) {
    if (prefix.len > sv.len) return false;
    if (prefix.len == 0) return true;
    return memcmp(sv.data, prefix.data, prefix.len) == 0;
}

/**
 * @brief Check if string view starts with C string
 */
KEEL_INLINE bool keel_str_view_starts_with_cstr(keel_str_view_t sv, const char* prefix) {
    return keel_str_view_starts_with(sv, keel_str_view_cstr(prefix));
}

/**
 * @brief Check if string view ends with suffix
 *
 * @param sv     String view
 * @param suffix Suffix to check
 * @return true if sv ends with suffix
 */
KEEL_INLINE bool keel_str_view_ends_with(keel_str_view_t sv, keel_str_view_t suffix) {
    if (suffix.len > sv.len) return false;
    if (suffix.len == 0) return true;
    return memcmp(sv.data + sv.len - suffix.len, suffix.data, suffix.len) == 0;
}

/**
 * @brief Find character in string view
 *
 * @param sv String view
 * @param c  Character to find
 * @return Index of first occurrence, or (size_t)-1 if not found
 */
KEEL_INLINE size_t keel_str_view_find_char(keel_str_view_t sv, char c) {
    const char* found = (const char*)memchr(sv.data, c, sv.len);
    return found ? (size_t)(found - sv.data) : (size_t)-1;
}

/**
 * @brief Find the first exact substring match within a view.
 *
 * The implementation favors a tiny, allocation-free $O(nm)$ scan because KEEL
 * typically searches short tokens inside already small slices. If future usage
 * shifts toward long haystacks and repeated searches, this API can keep the
 * same contract while swapping in a more sophisticated search strategy.
 *
 * @param sv View to search in.
 * @param needle Substring to locate.
 * @return Zero-based byte offset of the first match, or `(size_t)-1` when
 *         absent.
 */
size_t keel_str_view_find(keel_str_view_t sv, keel_str_view_t needle);

/* ============================================================================
 * Trimming
 * ============================================================================ */

/**
 * @brief Trim whitespace from both ends
 *
 * @param sv String view
 * @return Trimmed string view
 */
keel_str_view_t keel_str_view_trim(keel_str_view_t sv);

/**
 * @brief Trim whitespace from start
 *
 * @param sv String view
 * @return Left-trimmed string view
 */
keel_str_view_t keel_str_view_trim_left(keel_str_view_t sv);

/**
 * @brief Trim whitespace from end
 *
 * @param sv String view
 * @return Right-trimmed string view
 */
keel_str_view_t keel_str_view_trim_right(keel_str_view_t sv);

/* ============================================================================
 * Splitting
 * ============================================================================ */

/**
 * @brief Mutable iterator state for delimiter-based splitting.
 *
 * The iterator retains the remaining unconsumed suffix so tokenization can
 * proceed incrementally without allocating an array of results.
 */
typedef struct keel_str_view_split {
    keel_str_view_t remaining;   /**< Remaining string to split */
    char           delimiter;   /**< Split delimiter */
} keel_str_view_split_t;

/**
 * @brief Initialize split iterator
 *
 * @param sv        String view to split
 * @param delimiter Delimiter character
 * @return Split iterator
 */
KEEL_INLINE keel_str_view_split_t keel_str_view_split_init(keel_str_view_t sv, char delimiter) {
    return (keel_str_view_split_t){ .remaining = sv, .delimiter = delimiter };
}

/**
 * @brief Advance a split iterator and return the next token.
 *
 * Empty tokens are preserved, which makes the helper suitable for CSV-like
 * field extraction and configuration values where adjacent delimiters are
 * semantically meaningful.
 *
 * @param[in,out] split Iterator state to advance.
 * @param[out] token Next token slice.
 * @return true when a token was produced, false when the iterator is exhausted.
 */
bool keel_str_view_split_next(keel_str_view_split_t* split, keel_str_view_t* token);

/* ============================================================================
 * Conversion
 * ============================================================================ */

/**
 * @brief Copy string view to null-terminated buffer
 *
 * @param sv      String view
 * @param buf     Destination buffer
 * @param buf_len Buffer length (must include space for null terminator)
 * @return Number of characters copied (excluding null), or 0 if buffer too small
 */
KEEL_INLINE size_t keel_str_view_to_cstr(keel_str_view_t sv, char* buf, size_t buf_len) {
    if (!buf || buf_len == 0) return 0;
    size_t copy_len = sv.len < buf_len - 1 ? sv.len : buf_len - 1;
    if (copy_len > 0 && sv.data) {
        memcpy(buf, sv.data, copy_len);
    }
    buf[copy_len] = '\0';
    return copy_len;
}

/**
 * @brief Allocate and copy string view to null-terminated string
 *
 * @param sv String view
 * @return Newly allocated null-terminated string (caller must free)
 */
char* keel_str_view_dup(keel_str_view_t sv);

/**
 * @brief Parse string view as integer
 *
 * @param sv        String view
 * @param[out] out  Output value
 * @return true on success
 */
bool keel_str_view_to_int64(keel_str_view_t sv, int64_t* out);

/**
 * @brief Parse string view as unsigned integer
 *
 * @param sv        String view
 * @param[out] out  Output value
 * @return true on success
 */
bool keel_str_view_to_uint64(keel_str_view_t sv, uint64_t* out);

/**
 * @brief Parse string view as double
 *
 * @param sv        String view
 * @param[out] out  Output value
 * @return true on success
 */
bool keel_str_view_to_double(keel_str_view_t sv, double* out);

/* ============================================================================
 * Hashing
 * ============================================================================ */

/**
 * @brief Compute hash of string view
 *
 * Uses FNV-1a hash for speed. For cryptographic use, use XXHash.
 *
 * @param sv String view
 * @return 64-bit hash value
 */
uint64_t keel_str_view_hash(keel_str_view_t sv);

/**
 * @brief Compute case-insensitive hash
 *
 * @param sv String view
 * @return 64-bit hash value
 */
uint64_t keel_str_view_hash_nocase(keel_str_view_t sv);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_STRING_VIEW_H */
