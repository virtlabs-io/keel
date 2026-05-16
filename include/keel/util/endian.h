/**
 * @file endian.h
 * @brief Portable big-endian integer read/write helpers.
 *
 * Previously duplicated as local static functions in admin.c, backend_connect_async.c,
 * and postgres_flow.c under names like wr16/wr32/rd32/be32/wr32be.  All callers
 * should now include this header and use the keel_be* names.
 *
 * All functions operate on raw byte arrays and avoid undefined behaviour from
 * type-punning by using byte-by-byte shifts — the compiler will optimise these
 * to a single bswap instruction on little-endian targets.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_UTIL_ENDIAN_H
#define KEEL_UTIL_ENDIAN_H

#include <stdint.h>

/* ============================================================================
 * 16-bit big-endian
 * ========================================================================== */

/** @brief Read a big-endian uint16 from @p p. */
static inline uint16_t keel_be16_get(const uint8_t *p) {
    return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}

/** @brief Write @p v as big-endian uint16 at @p p. */
static inline void keel_be16_put(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

/* ============================================================================
 * 32-bit big-endian
 * ========================================================================== */

/** @brief Read a big-endian uint32 from @p p. */
static inline uint32_t keel_be32_get(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16)
         | ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

/** @brief Write @p v as big-endian uint32 at @p p. */
static inline void keel_be32_put(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8);
    p[3] = (uint8_t)v;
}

/* ============================================================================
 * 64-bit big-endian
 * ========================================================================== */

/** @brief Read a big-endian uint64 from @p p. */
static inline uint64_t keel_be64_get(const uint8_t *p) {
    return ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48)
         | ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32)
         | ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16)
         | ((uint64_t)p[6] <<  8) |  (uint64_t)p[7];
}

/** @brief Write @p v as big-endian uint64 at @p p. */
static inline void keel_be64_put(uint8_t *p, uint64_t v) {
    p[0] = (uint8_t)(v >> 56);
    p[1] = (uint8_t)(v >> 48);
    p[2] = (uint8_t)(v >> 40);
    p[3] = (uint8_t)(v >> 32);
    p[4] = (uint8_t)(v >> 24);
    p[5] = (uint8_t)(v >> 16);
    p[6] = (uint8_t)(v >>  8);
    p[7] = (uint8_t)v;
}

#endif /* KEEL_UTIL_ENDIAN_H */
