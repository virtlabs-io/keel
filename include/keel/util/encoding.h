/**
 * @file encoding.h
 * @brief Shared hex and JSON string encoding helpers.
 *
 * Previously duplicated as local static functions in trace.c, cloud_auth.c,
 * and log.c.  All callers should include this header and use keel_hex_encode()
 * and keel_json_escape() directly.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_UTIL_ENCODING_H
#define KEEL_UTIL_ENCODING_H

#include <stdint.h>
#include <stddef.h>

/* ============================================================================
 * Hex encoding
 * ========================================================================== */

/**
 * @brief Encode @p byte_count bytes from @p src into lowercase hex at @p dst.
 *
 * @p dst must be at least @p byte_count * 2 bytes.  No NUL terminator is
 * written — call sites that need one must append it themselves.
 */
static inline void keel_hex_encode(const uint8_t *src, char *dst, size_t byte_count) {
    static const char hex_chars[] = "0123456789abcdef";
    for (size_t i = 0; i < byte_count; i++) {
        dst[i * 2]     = hex_chars[(src[i] >> 4) & 0x0F];
        dst[i * 2 + 1] = hex_chars[src[i] & 0x0F];
    }
}

/* ============================================================================
 * JSON string escaping
 * ========================================================================== */

/**
 * @brief JSON-escape @p src into @p dst, writing at most @p dst_size bytes
 *        (including the NUL terminator).
 *
 * Handles NULL @p src (writes empty string), escapes the five JSON special
 * characters, and unicode-escapes remaining C0 control characters.
 *
 * @return Number of bytes written excluding the NUL terminator.
 */
size_t keel_json_escape(char *dst, size_t dst_size, const char *src);

#endif /* KEEL_UTIL_ENCODING_H */
