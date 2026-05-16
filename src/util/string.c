/**
 * @file string.c
 * @brief Small owning and non-owning string helpers built around `keel_str_t`.
 *
 * KEEL deliberately keeps its basic string type as a pointer-plus-length slice.
 * That avoids forcing null-terminated ownership on every call site while still
 * allowing selected helpers in this file to cross back into heap-owned C
 * strings when needed.
 *
 * The functions here favor simple, explicit behavior over aggressive
 * optimization. Most strings handled by utility, protocol, and SQL code are
 * short enough that straightforward loops and copies are easier to audit than
 * heavier abstractions.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel_types.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <sys/types.h>

/* ============================================================================
 * String creation and destruction
 * ============================================================================ */

/**
 * @brief Return an empty non-owning string slice.
 *
 * @return A zero-length slice pointing to a static empty C string.
 */
keel_str_t keel_str_empty(void) {
    return (keel_str_t){ .data = "", .len = 0 };
}

/* Note: keel_str_from_cstr is defined inline in types.h */

/**
 * @brief Construct a non-owning slice from a raw pointer and explicit length.
 *
 * @param data Pointer to the character data; NULL is treated as empty.
 * @param len Number of bytes in the slice.
 * @return Non-owning slice over the provided data.
 */
keel_str_t keel_str_from_parts(const char* data, size_t len) {
    return (keel_str_t){ .data = data ? data : "", .len = data ? len : 0 };
}

/* Internal function to duplicate as keel_str_t (for internal use) */
/**
 * @brief Duplicate a slice into owned storage while preserving `keel_str_t` form.
 *
 * This internal helper is used when KEEL wants an owning slice rather than a
 * raw C string. Allocation failure degrades to the empty string because many
 * utility callers treat emptiness as the safest failure sentinel.
 *
 * @param str Source slice.
 * @return Heap-backed slice on success, or the empty slice on failure.
 */
keel_str_t keel_str_dup_internal(keel_str_t str) {
    if (str.len == 0) {
        return keel_str_empty();
    }
    
    char* data = keel_malloc(str.len + 1);
    if (!data) {
        return keel_str_empty();
    }
    
    memcpy(data, str.data, str.len);
    data[str.len] = '\0';
    
    return (keel_str_t){ .data = data, .len = str.len };
}

/* Matches util.h: Duplicate string to heap-allocated C string */
/**
 * @brief Duplicate a slice into a freshly allocated null-terminated C string.
 *
 * @param str Source slice.
 * @return Heap-allocated null-terminated copy, or NULL on allocation failure.
 */
char* keel_str_dup(keel_str_t str) {
    if (str.len == 0) {
        char* empty = keel_malloc(1);
        if (empty) empty[0] = '\0';
        return empty;
    }
    
    char* data = keel_malloc(str.len + 1);
    if (!data) {
        return NULL;
    }
    
    memcpy(data, str.data, str.len);
    data[str.len] = '\0';
    
    return data;
}

/**
 * @brief Release the heap memory backing an owning slice and reset it to empty.
 *
 * @param str Pointer to the slice to free; safe to call with NULL or an empty slice.
 */
void keel_str_free(keel_str_t* str) {
    if (str && str->data && str->len > 0) {
        keel_free((void*)str->data);
        str->data = "";
        str->len = 0;
    }
}

/**
 * @brief Copy a slice into a freshly allocated null-terminated C string.
 *
 * Unlike `keel_str_dup`, the return type is a plain `char *` suitable for
 * APIs that require a null-terminated string.
 *
 * @param str Source slice.
 * @return Heap-allocated null-terminated copy, or NULL on allocation failure.
 */
char* keel_str_to_cstr(keel_str_t str) {
    char* cstr = keel_malloc(str.len + 1);
    if (!cstr) {
        return NULL;
    }
    
    memcpy(cstr, str.data, str.len);
    cstr[str.len] = '\0';
    
    return cstr;
}

/* ============================================================================
 * String comparison
 * ============================================================================ */

/**
 * @brief Compare two slices lexicographically.
 *
 * @param a First string.
 * @param b Second string.
 * @return Negative if a < b, zero if equal, positive if a > b.
 */
int keel_str_cmp(keel_str_t a, keel_str_t b) {
    size_t min_len = a.len < b.len ? a.len : b.len;
    int cmp = memcmp(a.data, b.data, min_len);
    
    if (cmp != 0) {
        return cmp;
    }
    
    if (a.len < b.len) return -1;
    if (a.len > b.len) return 1;
    return 0;
}

/**
 * @brief Compare a slice with a null-terminated C string lexicographically.
 *
 * @param str Slice operand.
 * @param cstr Null-terminated string operand.
 * @return Negative if str < cstr, zero if equal, positive if str > cstr.
 */
int keel_str_cmp_cstr(keel_str_t str, const char* cstr) {
    return keel_str_cmp(str, keel_str_from_cstr(cstr));
}

/**
 * @brief Test whether two slices have identical content.
 *
 * @param a First string.
 * @param b Second string.
 * @return true if equal, false otherwise.
 */
bool keel_str_eq(keel_str_t a, keel_str_t b) {
    return a.len == b.len && (a.len == 0 || memcmp(a.data, b.data, a.len) == 0);
}

/**
 * @brief Test whether a slice is equal to a null-terminated C string.
 *
 * @param str Slice operand.
 * @param cstr Null-terminated string operand.
 * @return true if equal, false otherwise.
 */
bool keel_str_eq_cstr(keel_str_t str, const char* cstr) {
    return keel_str_eq(str, keel_str_from_cstr(cstr));
}

/**
 * @brief Compare two slices case-insensitively using ASCII rules.
 *
 * @param a First string.
 * @param b Second string.
 * @return Negative if a < b, zero if equal, positive if a > b.
 */
int keel_str_casecmp(keel_str_t a, keel_str_t b) {
    size_t min_len = a.len < b.len ? a.len : b.len;
    
    for (size_t i = 0; i < min_len; i++) {
        int ca = tolower((unsigned char)a.data[i]);
        int cb = tolower((unsigned char)b.data[i]);
        if (ca != cb) {
            return ca - cb;
        }
    }
    
    if (a.len < b.len) return -1;
    if (a.len > b.len) return 1;
    return 0;
}

/**
 * @brief Test whether two slices are equal ignoring ASCII case.
 *
 * @param a First string.
 * @param b Second string.
 * @return true if case-insensitively equal, false otherwise.
 */
bool keel_str_case_eq(keel_str_t a, keel_str_t b) {
    return a.len == b.len && keel_str_casecmp(a, b) == 0;
}

/* Alias to match util.h declaration */
/**
 * @brief Alias for `keel_str_case_eq`; tests case-insensitive equality.
 *
 * @param a First string.
 * @param b Second string.
 * @return true if case-insensitively equal, false otherwise.
 */
bool keel_str_eq_nocase(keel_str_t a, keel_str_t b) {
    return keel_str_case_eq(a, b);
}

/* ============================================================================
 * String searching
 * ============================================================================ */

/**
 * @brief Test whether a slice begins with a given prefix.
 *
 * @param str String to examine.
 * @param prefix Required prefix.
 * @return true if str starts with prefix, false otherwise.
 */
bool keel_str_starts_with(keel_str_t str, keel_str_t prefix) {
    if (prefix.len > str.len) {
        return false;
    }
    return memcmp(str.data, prefix.data, prefix.len) == 0;
}

/**
 * @brief Test whether a slice ends with a given suffix.
 *
 * @param str String to examine.
 * @param suffix Required suffix.
 * @return true if str ends with suffix, false otherwise.
 */
bool keel_str_ends_with(keel_str_t str, keel_str_t suffix) {
    if (suffix.len > str.len) {
        return false;
    }
    return memcmp(str.data + str.len - suffix.len, suffix.data, suffix.len) == 0;
}

/**
 * @brief Find the first occurrence of needle in haystack.
 *
 * @param haystack String to search within.
 * @param needle Substring to search for.
 * @return Byte offset of the first match, or -1 if not found.
 */
keel_ssize_t keel_str_find(keel_str_t haystack, keel_str_t needle) {
    if (needle.len == 0) {
        return 0;
    }
    if (needle.len > haystack.len) {
        return -1;
    }
    
    for (size_t i = 0; i <= haystack.len - needle.len; i++) {
        if (memcmp(haystack.data + i, needle.data, needle.len) == 0) {
            return (keel_ssize_t)i;
        }
    }
    
    return -1;
}

/**
 * @brief Find the first occurrence of a byte value in a slice.
 *
 * @param str String to search.
 * @param c Byte value to find.
 * @return Byte offset of the first match, or -1 if not found.
 */
ssize_t keel_str_find_char(keel_str_t str, char c) {
    for (size_t i = 0; i < str.len; i++) {
        if (str.data[i] == c) {
            return (ssize_t)i;
        }
    }
    return -1;
}

/**
 * @brief Find the last occurrence of a byte value in a slice.
 *
 * @param str String to search.
 * @param c Byte value to find.
 * @return Byte offset of the last match, or -1 if not found.
 */
ssize_t keel_str_rfind_char(keel_str_t str, char c) {
    for (ssize_t i = (ssize_t)str.len - 1; i >= 0; i--) {
        if (str.data[(size_t)i] == c) {
            return i;
        }
    }
    return -1;
}

/**
 * @brief Test whether a slice contains a given substring.
 *
 * @param haystack String to search within.
 * @param needle Substring to search for.
 * @return true if needle is found, false otherwise.
 */
bool keel_str_contains(keel_str_t haystack, keel_str_t needle) {
    return keel_str_find(haystack, needle) >= 0;
}

/**
 * @brief Test whether a slice contains a specific byte value.
 *
 * @param str String to search.
 * @param c Byte value to look for.
 * @return true if the byte is found, false otherwise.
 */
bool keel_str_contains_char(keel_str_t str, char c) {
    return keel_str_find_char(str, c) >= 0;
}

/* ============================================================================
 * String slicing
 * ============================================================================ */

/**
 * @brief Extract a non-owning substring slice.
 *
 * @param str Source string.
 * @param start Zero-based start offset; clamped to the string length.
 * @param len Maximum number of bytes to include; clamped to available bytes.
 * @return Non-owning slice over the requested range.
 */
keel_str_t keel_str_substr(keel_str_t str, size_t start, size_t len) {
    if (start >= str.len) {
        return keel_str_empty();
    }
    
    size_t remaining = str.len - start;
    if (len > remaining) {
        len = remaining;
    }
    
    return keel_str_from_parts(str.data + start, len);
}

/**
 * @brief Return a non-owning slice with leading and trailing whitespace removed.
 *
 * @param str Source string.
 * @return Trimmed non-owning slice pointing into the original data.
 */
keel_str_t keel_str_trim(keel_str_t str) {
    size_t start = 0;
    size_t end = str.len;
    
    while (start < end && isspace((unsigned char)str.data[start])) {
        start++;
    }
    
    while (end > start && isspace((unsigned char)str.data[end - 1])) {
        end--;
    }
    
    return keel_str_from_parts(str.data + start, end - start);
}

/**
 * @brief Return a non-owning slice with leading whitespace removed.
 *
 * @param str Source string.
 * @return Slice with leading whitespace stripped; points into the original data.
 */
keel_str_t keel_str_trim_left(keel_str_t str) {
    size_t start = 0;
    
    while (start < str.len && isspace((unsigned char)str.data[start])) {
        start++;
    }
    
    return keel_str_from_parts(str.data + start, str.len - start);
}

/**
 * @brief Return a non-owning slice with trailing whitespace removed.
 *
 * @param str Source string.
 * @return Slice with trailing whitespace stripped; points into the original data.
 */
keel_str_t keel_str_trim_right(keel_str_t str) {
    size_t end = str.len;
    
    while (end > 0 && isspace((unsigned char)str.data[end - 1])) {
        end--;
    }
    
    return keel_str_from_parts(str.data, end);
}

/* ============================================================================
 * String splitting
 * ============================================================================ */

/* Iterator-style split: extracts one part at a time from remaining string.
 * Returns true if a part was extracted, false if no more parts.
 * 
 * Usage:
 *   keel_str_t remaining = keel_str_from_cstr("a,b,c");
 *   keel_str_t part;
 *   while (keel_str_split(&remaining, ',', &part)) {
 *       // process part
 *   }
 */
/**
 * @brief Produce the next token from a delimiter-separated slice.
 *
 * The function updates `remaining` in place so callers can iterate through a
 * string without allocating an intermediate token array. The final token is
 * returned even when no delimiter is found, mirroring common iterator-style
 * split APIs.
 *
 * @param[in,out] remaining Unconsumed suffix of the source string.
 * @param delim Delimiter byte.
 * @param[out] part Next token.
 * @return true when a token is produced, false when iteration is finished.
 */
bool keel_str_split(keel_str_t* remaining, char delim, keel_str_t* part) {
    if (!remaining || !part || remaining->len == 0) {
        if (part) *part = keel_str_empty();
        return false;
    }
    
    /* Find delimiter */
    for (size_t i = 0; i < remaining->len; i++) {
        if (remaining->data[i] == delim) {
            /* Found delimiter - extract part before it */
            *part = keel_str_from_parts(remaining->data, i);
            /* Advance remaining past delimiter */
            remaining->data = remaining->data + i + 1;
            remaining->len = remaining->len - i - 1;
            return true;
        }
    }
    
    /* No delimiter found - remaining string is the last part */
    *part = *remaining;
    remaining->data = remaining->data + remaining->len;
    remaining->len = 0;
    return true;
}

/* ============================================================================
 * String formatting
 * ============================================================================ */

/**
 * @brief Format a string into freshly allocated storage.
 *
 * The implementation uses the common two-pass `vsnprintf()` pattern: one pass
 * to size the result and one pass to write it. That costs an extra format walk
 * but avoids arbitrary fixed buffers and truncation surprises.
 *
 * @param fmt `printf`-style format string.
 * @param ... Format arguments.
 * @return Heap-backed formatted slice, or the empty slice on failure.
 */
keel_str_t keel_str_printf(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    
    va_list args_copy;
    va_copy(args_copy, args);
    
    int len = vsnprintf(NULL, 0, fmt, args_copy);
    va_end(args_copy);
    
    if (len < 0) {
        va_end(args);
        return keel_str_empty();
    }
    
    char* data = keel_malloc((size_t)len + 1);
    if (!data) {
        va_end(args);
        return keel_str_empty();
    }
    
    vsnprintf(data, (size_t)len + 1, fmt, args);
    va_end(args);
    
    return (keel_str_t){ .data = data, .len = (size_t)len };
}

/**
 * @brief Concatenate two slices into a newly allocated string.
 *
 * @param a First string.
 * @param b Second string.
 * @return Heap-backed slice containing a followed by b, or empty on failure.
 */
keel_str_t keel_str_concat(keel_str_t a, keel_str_t b) {
    size_t total = a.len + b.len;
    
    char* data = keel_malloc(total + 1);
    if (!data) {
        return keel_str_empty();
    }
    
    memcpy(data, a.data, a.len);
    memcpy(data + a.len, b.data, b.len);
    data[total] = '\0';
    
    return (keel_str_t){ .data = data, .len = total };
}

/**
 * @brief Join an array of slices with a separator into a newly allocated string.
 *
 * @param parts Array of slices to join.
 * @param count Number of elements in parts.
 * @param sep Separator inserted between consecutive parts.
 * @return Heap-backed joined string, or empty on allocation failure or count == 0.
 */
keel_str_t keel_str_join(const keel_str_t* parts, size_t count, keel_str_t sep) {
    if (count == 0) {
        return keel_str_empty();
    }
    
    /* Calculate total length */
    size_t total = 0;
    for (size_t i = 0; i < count; i++) {
        total += parts[i].len;
        if (i > 0) {
            total += sep.len;
        }
    }
    
    char* data = keel_malloc(total + 1);
    if (!data) {
        return keel_str_empty();
    }
    
    size_t offset = 0;
    for (size_t i = 0; i < count; i++) {
        if (i > 0 && sep.len > 0) {
            memcpy(data + offset, sep.data, sep.len);
            offset += sep.len;
        }
        memcpy(data + offset, parts[i].data, parts[i].len);
        offset += parts[i].len;
    }
    data[total] = '\0';
    
    return (keel_str_t){ .data = data, .len = total };
}

/* ============================================================================
 * String conversion
 * ============================================================================ */

/**
 * @brief Parse a signed 64-bit integer from a slice.
 *
 * The helper temporarily materializes a null-terminated buffer because the C
 * library number parsers operate on C strings. It rejects trailing garbage by
 * requiring the parse to consume the full slice.
 *
 * @param str Source slice.
 * @param[out] out Parsed integer on success.
 * @return true on success, false on allocation or parse failure.
 */
bool keel_str_to_int64(keel_str_t str, int64_t* out) {
    if (str.len == 0 || !out) {
        return false;
    }
    
    char* cstr = keel_str_to_cstr(str);
    if (!cstr) {
        return false;
    }
    
    char* endptr;
    long long val = strtoll(cstr, &endptr, 10);
    bool ok = (*endptr == '\0');
    
    keel_free(cstr);
    
    if (ok) {
        *out = (int64_t)val;
    }
    
    return ok;
}

/**
 * @brief Parse an unsigned 64-bit integer from a slice.
 *
 * @param str Source slice.
 * @param[out] out Parsed integer on success.
 * @return true on success, false on allocation or parse failure.
 */
bool keel_str_to_uint64(keel_str_t str, uint64_t* out) {
    if (str.len == 0 || !out) {
        return false;
    }
    
    char* cstr = keel_str_to_cstr(str);
    if (!cstr) {
        return false;
    }
    
    char* endptr;
    unsigned long long val = strtoull(cstr, &endptr, 10);
    bool ok = (*endptr == '\0');
    
    keel_free(cstr);
    
    if (ok) {
        *out = (uint64_t)val;
    }
    
    return ok;
}

/**
 * @brief Parse a floating-point value from a slice.
 *
 * @param str Source slice.
 * @param[out] out Parsed value on success.
 * @return true on success, false on allocation or parse failure.
 */
bool keel_str_to_double(keel_str_t str, double* out) {
    if (str.len == 0 || !out) {
        return false;
    }
    
    char* cstr = keel_str_to_cstr(str);
    if (!cstr) {
        return false;
    }
    
    char* endptr;
    double val = strtod(cstr, &endptr);
    bool ok = (*endptr == '\0');
    
    keel_free(cstr);
    
    if (ok) {
        *out = val;
    }
    
    return ok;
}

/* ============================================================================
 * Case conversion
 * ============================================================================ */

/**
 * @brief Convert a slice to lowercase, returning a newly allocated string.
 *
 * @param str Source string.
 * @return Heap-backed lowercase copy, or empty on allocation failure.
 */
keel_str_t keel_str_tolower(keel_str_t str) {
    char* data = keel_malloc(str.len + 1);
    if (!data) {
        return keel_str_empty();
    }
    
    for (size_t i = 0; i < str.len; i++) {
        data[i] = (char)tolower((unsigned char)str.data[i]);
    }
    data[str.len] = '\0';
    
    return (keel_str_t){ .data = data, .len = str.len };
}

/**
 * @brief Convert a slice to uppercase, returning a newly allocated string.
 *
 * @param str Source string.
 * @return Heap-backed uppercase copy, or empty on allocation failure.
 */
keel_str_t keel_str_toupper(keel_str_t str) {
    char* data = keel_malloc(str.len + 1);
    if (!data) {
        return keel_str_empty();
    }
    
    for (size_t i = 0; i < str.len; i++) {
        data[i] = (char)toupper((unsigned char)str.data[i]);
    }
    data[str.len] = '\0';
    
    return (keel_str_t){ .data = data, .len = str.len };
}

/* ============================================================================
 * Hex encoding
 * ============================================================================ */

static const char hex_chars[] = "0123456789abcdef";

/**
 * @brief Encode raw bytes as a lowercase hexadecimal string.
 *
 * @param data Pointer to the input bytes.
 * @param len Number of bytes to encode.
 * @return Heap-backed hex string of length len * 2, or empty on allocation failure.
 */
keel_str_t keel_str_to_hex(const uint8_t* data, size_t len) {
    char* hex = keel_malloc(len * 2 + 1);
    if (!hex) {
        return keel_str_empty();
    }
    
    for (size_t i = 0; i < len; i++) {
        hex[i * 2] = hex_chars[(data[i] >> 4) & 0x0f];
        hex[i * 2 + 1] = hex_chars[data[i] & 0x0f];
    }
    hex[len * 2] = '\0';
    
    return (keel_str_t){ .data = hex, .len = len * 2 };
}

/**
 * @brief Convert a single ASCII hexadecimal digit to its numeric value.
 *
 * @param c Hexadecimal character ('0'-'9', 'a'-'f', 'A'-'F').
 * @return Numeric value 0-15, or -1 if c is not a valid hex digit.
 */
static int hex_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/**
 * @brief Decode an ASCII hexadecimal string into caller-owned bytes.
 *
 * The function validates both input shape and output capacity before writing so
 * callers can use it safely with stack buffers.
 *
 * @param hex Hexadecimal input slice.
 * @param[out] out Destination byte buffer.
 * @param[in,out] out_len Capacity on entry, decoded byte count on success.
 * @return true on successful decode, false on malformed input or short output.
 */
bool keel_str_from_hex(keel_str_t hex, uint8_t* out, size_t* out_len) {
    if (hex.len % 2 != 0 || !out || !out_len) {
        return false;
    }
    
    size_t len = hex.len / 2;
    if (*out_len < len) {
        return false;
    }
    
    for (size_t i = 0; i < len; i++) {
        int high = hex_value(hex.data[i * 2]);
        int low = hex_value(hex.data[i * 2 + 1]);
        
        if (high < 0 || low < 0) {
            return false;
        }
        
        out[i] = (uint8_t)((high << 4) | low);
    }
    
    *out_len = len;
    return true;
}
