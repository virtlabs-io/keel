/**
 * @file string_view.c
 * @brief Out-of-line helpers for zero-copy string views.
 *
 * The header keeps the cheapest operations inline so parsers can slice and
 * compare views without function-call overhead. This file contains the helpers
 * that are either more complex, more rarely used, or better kept out-of-line to
 * avoid bloating every translation unit that includes the header.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/util/string_view.h"
#include "keel/util/util.h"
#include "keel/mem/mem.h"

#include <ctype.h>
#include <stdlib.h>
#include <errno.h>

/* ============================================================================
 * Case-Insensitive Comparison
 * ============================================================================ */

/**
 * @brief Compare two string views with ASCII-only case folding.
 *
 * @param a First view.
 * @param b Second view.
 * @return true when both views are equal ignoring ASCII case.
 */
bool keel_str_view_eq_nocase(keel_str_view_t a, keel_str_view_t b) {
    if (a.len != b.len) return false;
    if (a.len == 0) return true;
    if (a.data == b.data) return true;
    
    for (size_t i = 0; i < a.len; i++) {
        if (tolower((unsigned char)a.data[i]) != tolower((unsigned char)b.data[i])) {
            return false;
        }
    }
    return true;
}

/**
 * @brief Compare a string view against a NUL-terminated C string, ignoring ASCII case.
 *
 * @param sv View to compare.
 * @param cstr NUL-terminated string to compare against. `NULL` is treated as empty.
 * @return true when both are equal ignoring ASCII case.
 */
bool keel_str_view_eq_cstr_nocase(keel_str_view_t sv, const char* cstr) {
    if (!cstr) return sv.len == 0;
    return keel_str_view_eq_nocase(sv, keel_str_view_cstr(cstr));
}

/* ============================================================================
 * Searching
 * ============================================================================ */

/**
 * @brief Find the first occurrence of one view inside another.
 *
 * A simple nested scan is sufficient here because callers generally use it on
 * short protocol fragments and token streams rather than large documents.
 *
 * @param sv Haystack view.
 * @param needle Needle view.
 * @return First match offset, or `(size_t)-1` when not found.
 */
size_t keel_str_view_find(keel_str_view_t sv, keel_str_view_t needle) {
    if (needle.len == 0) return 0;
    if (needle.len > sv.len) return (size_t)-1;
    
    /* Simple O(n*m) search - could use KMP for large needles */
    size_t limit = sv.len - needle.len + 1;
    for (size_t i = 0; i < limit; i++) {
        if (memcmp(sv.data + i, needle.data, needle.len) == 0) {
            return i;
        }
    }
    return (size_t)-1;
}

/* ============================================================================
 * Trimming
 * ============================================================================ */

/**
 * @brief Check whether a character is ASCII whitespace.
 *
 * @param c Character to test.
 * @return true when `c` is space, tab, newline, carriage-return, vertical-tab, or form-feed.
 */
static bool is_whitespace(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || 
           c == '\v' || c == '\f';
}

/**
 * @brief Remove leading ASCII whitespace from a string view.
 *
 * @param sv Source view.
 * @return View with the leading whitespace prefix removed.
 */
keel_str_view_t keel_str_view_trim_left(keel_str_view_t sv) {
    while (sv.len > 0 && is_whitespace(sv.data[0])) {
        sv.data++;
        sv.len--;
    }
    return sv;
}

/**
 * @brief Remove trailing ASCII whitespace from a string view.
 *
 * @param sv Source view.
 * @return View with the trailing whitespace suffix removed.
 */
keel_str_view_t keel_str_view_trim_right(keel_str_view_t sv) {
    while (sv.len > 0 && is_whitespace(sv.data[sv.len - 1])) {
        sv.len--;
    }
    return sv;
}

/**
 * @brief Remove both leading and trailing ASCII whitespace from a string view.
 *
 * @param sv Source view.
 * @return View with whitespace stripped from both ends.
 */
keel_str_view_t keel_str_view_trim(keel_str_view_t sv) {
    return keel_str_view_trim_right(keel_str_view_trim_left(sv));
}

/* ============================================================================
 * Splitting
 * ============================================================================ */

/**
 * @brief Advance a split iterator and emit the next token.
 *
 * The iterator uses a `NULL` data pointer as the exhausted sentinel so it can
 * still represent a final empty token distinctly from complete exhaustion.
 *
 * @param[in,out] split Split state to advance.
 * @param[out] token Next token.
 * @return true when a token was produced, false when iteration is exhausted.
 */
bool keel_str_view_split_next(keel_str_view_split_t* split, keel_str_view_t* token) {
    if (!split || !token) return false;
    if (split->remaining.len == 0 && !split->remaining.data) return false;
    
    /* Handle case where remaining is empty but valid (final empty token) */
    if (split->remaining.len == 0) {
        *token = keel_str_view_empty();
        split->remaining.data = NULL;  /* Mark as exhausted */
        return true;
    }
    
    /* Find delimiter */
    size_t pos = keel_str_view_find_char(split->remaining, split->delimiter);
    
    if (pos == (size_t)-1) {
        /* No more delimiters - return rest */
        *token = split->remaining;
        split->remaining = keel_str_view_empty();
        split->remaining.data = NULL;  /* Mark as exhausted */
    } else {
        /* Found delimiter */
        *token = keel_str_view_prefix(split->remaining, pos);
        split->remaining = keel_str_view_remove_prefix(split->remaining, pos + 1);
    }
    
    return true;
}

/* ============================================================================
 * Conversion
 * ============================================================================ */

/**
 * @brief Duplicate a string view into a new heap-allocated NUL-terminated string.
 *
 * @param sv View to copy.
 * @return Newly allocated C string, or `NULL` on allocation failure.
 */
char* keel_str_view_dup(keel_str_view_t sv) {
    char* result = keel_malloc(sv.len + 1);
    if (!result) return NULL;
    
    if (sv.len > 0 && sv.data) {
        memcpy(result, sv.data, sv.len);
    }
    result[sv.len] = '\0';
    return result;
}

/**
 * @brief Parse a signed integer from a string view.
 *
 * A small fixed stack buffer is used to materialize a temporary C string.
 * Oversized inputs are rejected instead of allocating dynamically so the helper
 * stays allocation-free on the success path.
 *
 * @param sv Source view.
 * @param[out] out Parsed integer.
 * @return true on success, false on invalid input or overflow.
 */
bool keel_str_view_to_int64(keel_str_view_t sv, int64_t* out) {
    if (!out) return false;
    if (sv.len == 0) return false;
    
    /* Copy to null-terminated buffer for strtoll */
    char buf[32];
    if (sv.len >= sizeof(buf)) return false;
    
    memcpy(buf, sv.data, sv.len);
    buf[sv.len] = '\0';
    
    char* end;
    errno = 0;
    long long val = strtoll(buf, &end, 10);
    
    if (errno != 0) return false;
    if (end != buf + sv.len) return false;  /* Trailing garbage */
    
    *out = (int64_t)val;
    return true;
}

/**
 * @brief Parse an unsigned 64-bit integer from a string view.
 *
 * Leading minus signs are rejected. Uses a fixed stack buffer; inputs longer
 * than 31 characters are rejected.
 *
 * @param sv Source view.
 * @param[out] out Parsed value.
 * @return true on success, false on invalid input, overflow, or negative input.
 */
bool keel_str_view_to_uint64(keel_str_view_t sv, uint64_t* out) {
    if (!out) return false;
    if (sv.len == 0) return false;
    
    /* Reject negative numbers */
    if (sv.len > 0 && sv.data[0] == '-') return false;
    
    char buf[32];
    if (sv.len >= sizeof(buf)) return false;
    
    memcpy(buf, sv.data, sv.len);
    buf[sv.len] = '\0';
    
    char* end;
    errno = 0;
    unsigned long long val = strtoull(buf, &end, 10);
    
    if (errno != 0) return false;
    if (end != buf + sv.len) return false;
    
    *out = (uint64_t)val;
    return true;
}

/**
 * @brief Parse a double-precision floating-point number from a string view.
 *
 * Uses a fixed stack buffer; inputs longer than 63 characters are rejected.
 *
 * @param sv Source view.
 * @param[out] out Parsed value.
 * @return true on success, false on invalid input or range error.
 */
bool keel_str_view_to_double(keel_str_view_t sv, double* out) {
    if (!out) return false;
    if (sv.len == 0) return false;
    
    char buf[64];
    if (sv.len >= sizeof(buf)) return false;
    
    memcpy(buf, sv.data, sv.len);
    buf[sv.len] = '\0';
    
    char* end;
    errno = 0;
    double val = strtod(buf, &end);
    
    if (errno != 0) return false;
    if (end != buf + sv.len) return false;
    
    *out = val;
    return true;
}

/* ============================================================================
 * Hashing
 * ============================================================================ */

/**
 * FNV-1a hash - fast and good distribution for short strings
 */
/**
 * @brief Hash a string view with 64-bit FNV-1a.
 *
 * This is a small, dependency-free hash for view keys when pulling in xxHash
 * would be unnecessary.
 *
 * @param sv Input view.
 * @return 64-bit hash value.
 */
uint64_t keel_str_view_hash(keel_str_view_t sv) {
    return keel_hash_fnv1a_64(sv.data, sv.len);
}

/**
 * @brief Hash a string view with 64-bit FNV-1a, folding to lower-case.
 *
 * Produces the same hash for strings that differ only in ASCII case.
 *
 * @param sv Input view.
 * @return 64-bit case-insensitive hash value.
 */
uint64_t keel_str_view_hash_nocase(keel_str_view_t sv) {
    /* FNV-1a 64-bit constants — same as keel_hash_fnv1a_64() in util/hash.c */
    uint64_t hash = 0xcbf29ce484222325ULL;
    for (size_t i = 0; i < sv.len; i++) {
        hash ^= (uint64_t)(unsigned char)tolower((unsigned char)sv.data[i]);
        hash *= 0x00000100000001b3ULL;
    }
    return hash;
}
