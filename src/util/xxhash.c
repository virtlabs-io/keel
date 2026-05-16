/**
 * @file xxhash.c
 * @brief Portable XXH32 and XXH64 implementations used for KEEL fingerprints.
 *
 * This is an intentionally compact port of the xxHash algorithms. It preserves
 * the core round structure, tail handling, and avalanche behavior of the
 * reference design while avoiding architecture-specific intrinsics or external
 * dependencies.
 *
 * That tradeoff is deliberate: KEEL benefits more from a stable in-tree hash
 * implementation with predictable behavior across environments than from
 * chasing the absolute fastest upstream variant.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/util/xxhash.h"

#include <string.h>

/* ============================================================================
 * XXH32 Constants
 * ============================================================================ */

static const uint32_t XXH_PRIME32_1 = 0x9E3779B1U;
static const uint32_t XXH_PRIME32_2 = 0x85EBCA77U;
static const uint32_t XXH_PRIME32_3 = 0xC2B2AE3DU;
static const uint32_t XXH_PRIME32_4 = 0x27D4EB2FU;
static const uint32_t XXH_PRIME32_5 = 0x165667B1U;

/* ============================================================================
 * XXH64 Constants
 * ============================================================================ */

static const uint64_t XXH_PRIME64_1 = 0x9E3779B185EBCA87ULL;
static const uint64_t XXH_PRIME64_2 = 0xC2B2AE3D27D4EB4FULL;
static const uint64_t XXH_PRIME64_3 = 0x165667B19E3779F9ULL;
static const uint64_t XXH_PRIME64_4 = 0x85EBCA77C2B2AE63ULL;
static const uint64_t XXH_PRIME64_5 = 0x27D4EB2F165667C5ULL;

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/* Read 32-bit little-endian */
static uint32_t xxh_read32(const void* ptr) {
    const uint8_t* p = (const uint8_t*)ptr;
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

/* Read 64-bit little-endian */
static uint64_t xxh_read64(const void* ptr) {
    const uint8_t* p = (const uint8_t*)ptr;
    return (uint64_t)p[0] | ((uint64_t)p[1] << 8) |
           ((uint64_t)p[2] << 16) | ((uint64_t)p[3] << 24) |
           ((uint64_t)p[4] << 32) | ((uint64_t)p[5] << 40) |
           ((uint64_t)p[6] << 48) | ((uint64_t)p[7] << 56);
}

/* 32-bit rotate left */
/** @brief Rotate @p x left by @p r bits (32-bit). */
static uint32_t xxh_rotl32(uint32_t x, int r) {
    return (x << r) | (x >> (32 - r));
}

/* 64-bit rotate left */
/** @brief Rotate @p x left by @p r bits (64-bit). */
static uint64_t xxh_rotl64(uint64_t x, int r) {
    return (x << r) | (x >> (64 - r));
}

/* XXH32 round function */
/** @brief Perform one XXH32 mixing round. */
static uint32_t xxh32_round(uint32_t acc, uint32_t input) {
    acc += input * XXH_PRIME32_2;
    acc = xxh_rotl32(acc, 13);
    acc *= XXH_PRIME32_1;
    return acc;
}

/* XXH64 round function */
/** @brief Perform one XXH64 mixing round. */
static uint64_t xxh64_round(uint64_t acc, uint64_t input) {
    acc += input * XXH_PRIME64_2;
    acc = xxh_rotl64(acc, 31);
    acc *= XXH_PRIME64_1;
    return acc;
}

/* XXH64 merge round */
/** @brief Merge an accumulator lane into the XXH64 hash during finalisation. */
static uint64_t xxh64_merge_round(uint64_t acc, uint64_t val) {
    val = xxh64_round(0, val);
    acc ^= val;
    acc = acc * XXH_PRIME64_1 + XXH_PRIME64_4;
    return acc;
}

/* ============================================================================
 * XXH32 Implementation
 * ============================================================================ */

/**
 * @brief Compute a one-shot XXH32 digest.
 *
 * The algorithm processes 16-byte stripes when enough data is available, then
 * folds in remaining 4-byte and 1-byte tails before the avalanche stage.
 *
 * @param input Input bytes.
 * @param len Number of bytes to hash.
 * @param seed Seed value.
 * @return 32-bit digest.
 */
uint32_t keel_xxh32(const void* input, size_t len, uint32_t seed) {
    const uint8_t* p = (const uint8_t*)input;
    const uint8_t* const end = p + len;
    uint32_t h32;

    if (len >= 16) {
        const uint8_t* const limit = end - 16;
        uint32_t v1 = seed + XXH_PRIME32_1 + XXH_PRIME32_2;
        uint32_t v2 = seed + XXH_PRIME32_2;
        uint32_t v3 = seed + 0;
        uint32_t v4 = seed - XXH_PRIME32_1;

        do {
            v1 = xxh32_round(v1, xxh_read32(p)); p += 4;
            v2 = xxh32_round(v2, xxh_read32(p)); p += 4;
            v3 = xxh32_round(v3, xxh_read32(p)); p += 4;
            v4 = xxh32_round(v4, xxh_read32(p)); p += 4;
        } while (p <= limit);

        h32 = xxh_rotl32(v1, 1) + xxh_rotl32(v2, 7) +
              xxh_rotl32(v3, 12) + xxh_rotl32(v4, 18);
    } else {
        h32 = seed + XXH_PRIME32_5;
    }

    h32 += (uint32_t)len;

    /* Process remaining bytes */
    while (p + 4 <= end) {
        h32 += xxh_read32(p) * XXH_PRIME32_3;
        h32 = xxh_rotl32(h32, 17) * XXH_PRIME32_4;
        p += 4;
    }

    while (p < end) {
        h32 += (*p++) * XXH_PRIME32_5;
        h32 = xxh_rotl32(h32, 11) * XXH_PRIME32_1;
    }

    /* Avalanche */
    h32 ^= h32 >> 15;
    h32 *= XXH_PRIME32_2;
    h32 ^= h32 >> 13;
    h32 *= XXH_PRIME32_3;
    h32 ^= h32 >> 16;

    return h32;
}

void keel_xxh32_reset(keel_xxh32_state_t* state, uint32_t seed) {
    memset(state, 0, sizeof(*state));
    state->v1 = seed + XXH_PRIME32_1 + XXH_PRIME32_2;
    state->v2 = seed + XXH_PRIME32_2;
    state->v3 = seed + 0;
    state->v4 = seed - XXH_PRIME32_1;
}

/**
 * @brief Feed bytes into a streaming XXH32 computation.
 *
 * Partial stripes are buffered in `state->mem` until a full 16-byte block can
 * be processed. This keeps the streaming API behavior identical to the one-shot
 * algorithm regardless of chunk boundaries.
 *
 * @param state Streaming state.
 * @param input Input bytes.
 * @param len Number of bytes to incorporate.
 * @return
 */
void keel_xxh32_update(keel_xxh32_state_t* state, const void* input, size_t len) {
    const uint8_t* p = (const uint8_t*)input;
    const uint8_t* const end = p + len;

    state->total_len += len;

    if (state->memsize + len < 16) {
        /* Not enough data for a full block, buffer it */
        memcpy(((uint8_t*)state->mem) + state->memsize, input, len);
        state->memsize += (uint32_t)len;
        return;
    }

    if (state->memsize) {
        /* Fill the block */
        size_t fill = 16 - state->memsize;
        memcpy(((uint8_t*)state->mem) + state->memsize, p, fill);
        p += fill;

        /* Process the block */
        state->v1 = xxh32_round(state->v1, state->mem[0]);
        state->v2 = xxh32_round(state->v2, state->mem[1]);
        state->v3 = xxh32_round(state->v3, state->mem[2]);
        state->v4 = xxh32_round(state->v4, state->mem[3]);
        state->memsize = 0;
        state->large_len = 1;
    }

    if (p + 16 <= end) {
        const uint8_t* const limit = end - 16;
        uint32_t v1 = state->v1;
        uint32_t v2 = state->v2;
        uint32_t v3 = state->v3;
        uint32_t v4 = state->v4;

        do {
            v1 = xxh32_round(v1, xxh_read32(p)); p += 4;
            v2 = xxh32_round(v2, xxh_read32(p)); p += 4;
            v3 = xxh32_round(v3, xxh_read32(p)); p += 4;
            v4 = xxh32_round(v4, xxh_read32(p)); p += 4;
        } while (p <= limit);

        state->v1 = v1;
        state->v2 = v2;
        state->v3 = v3;
        state->v4 = v4;
        state->large_len = 1;
    }

    if (p < end) {
        memcpy(state->mem, p, (size_t)(end - p));
        state->memsize = (uint32_t)(end - p);
    }
}

/**
 * @brief Finalise a streaming XXH32 computation and return the digest.
 *
 * @param state  Streaming state populated by keel_xxh32_update().
 * @return 32-bit hash value.
 */
uint32_t keel_xxh32_digest(const keel_xxh32_state_t* state) {
    const uint8_t* p = (const uint8_t*)state->mem;
    const uint8_t* const end = p + state->memsize;
    uint32_t h32;

    if (state->large_len) {
        h32 = xxh_rotl32(state->v1, 1) + xxh_rotl32(state->v2, 7) +
              xxh_rotl32(state->v3, 12) + xxh_rotl32(state->v4, 18);
    } else {
        h32 = state->v3 /* seed */ + XXH_PRIME32_5;
    }

    h32 += (uint32_t)state->total_len;

    while (p + 4 <= end) {
        h32 += xxh_read32(p) * XXH_PRIME32_3;
        h32 = xxh_rotl32(h32, 17) * XXH_PRIME32_4;
        p += 4;
    }

    while (p < end) {
        h32 += (*p++) * XXH_PRIME32_5;
        h32 = xxh_rotl32(h32, 11) * XXH_PRIME32_1;
    }

    h32 ^= h32 >> 15;
    h32 *= XXH_PRIME32_2;
    h32 ^= h32 >> 13;
    h32 *= XXH_PRIME32_3;
    h32 ^= h32 >> 16;

    return h32;
}

/* ============================================================================
 * XXH64 Implementation
 * ============================================================================ */

/**
 * @brief Compute a one-shot XXH64 digest.
 *
 * XXH64 uses 32-byte stripes and a wider mixing schedule than XXH32, which is
 * why it is the preferred fingerprint size in most 64-bit KEEL code.
 *
 * @param input Input bytes.
 * @param len Number of bytes to hash.
 * @param seed Seed value.
 * @return 64-bit digest.
 */
uint64_t keel_xxh64(const void* input, size_t len, uint64_t seed) {
    const uint8_t* p = (const uint8_t*)input;
    const uint8_t* const end = p + len;
    uint64_t h64;

    if (len >= 32) {
        const uint8_t* const limit = end - 32;
        uint64_t v1 = seed + XXH_PRIME64_1 + XXH_PRIME64_2;
        uint64_t v2 = seed + XXH_PRIME64_2;
        uint64_t v3 = seed + 0;
        uint64_t v4 = seed - XXH_PRIME64_1;

        do {
            v1 = xxh64_round(v1, xxh_read64(p)); p += 8;
            v2 = xxh64_round(v2, xxh_read64(p)); p += 8;
            v3 = xxh64_round(v3, xxh_read64(p)); p += 8;
            v4 = xxh64_round(v4, xxh_read64(p)); p += 8;
        } while (p <= limit);

        h64 = xxh_rotl64(v1, 1) + xxh_rotl64(v2, 7) +
              xxh_rotl64(v3, 12) + xxh_rotl64(v4, 18);

        h64 = xxh64_merge_round(h64, v1);
        h64 = xxh64_merge_round(h64, v2);
        h64 = xxh64_merge_round(h64, v3);
        h64 = xxh64_merge_round(h64, v4);
    } else {
        h64 = seed + XXH_PRIME64_5;
    }

    h64 += (uint64_t)len;

    /* Process remaining 8-byte chunks */
    while (p + 8 <= end) {
        uint64_t k1 = xxh64_round(0, xxh_read64(p));
        h64 ^= k1;
        h64 = xxh_rotl64(h64, 27) * XXH_PRIME64_1 + XXH_PRIME64_4;
        p += 8;
    }

    /* Process remaining 4-byte chunk */
    if (p + 4 <= end) {
        h64 ^= (uint64_t)xxh_read32(p) * XXH_PRIME64_1;
        h64 = xxh_rotl64(h64, 23) * XXH_PRIME64_2 + XXH_PRIME64_3;
        p += 4;
    }

    /* Process remaining bytes */
    while (p < end) {
        h64 ^= (*p++) * XXH_PRIME64_5;
        h64 = xxh_rotl64(h64, 11) * XXH_PRIME64_1;
    }

    /* Avalanche */
    h64 ^= h64 >> 33;
    h64 *= XXH_PRIME64_2;
    h64 ^= h64 >> 29;
    h64 *= XXH_PRIME64_3;
    h64 ^= h64 >> 32;

    return h64;
}

/**
 * @brief Initialise (or re-initialise) a streaming XXH64 state.
 *
 * @param state  State to initialise.
 * @param seed   Hash seed (use 0 for unseeded).
 */
void keel_xxh64_reset(keel_xxh64_state_t* state, uint64_t seed) {
    memset(state, 0, sizeof(*state));
    state->v1 = seed + XXH_PRIME64_1 + XXH_PRIME64_2;
    state->v2 = seed + XXH_PRIME64_2;
    state->v3 = seed + 0;
    state->v4 = seed - XXH_PRIME64_1;
}

/**
 * @brief Feed bytes into a streaming XXH64 computation.
 *
 * May be called multiple times before keel_xxh64_digest().
 *
 * @param state  Streaming state.
 * @param input  Bytes to hash.
 * @param len    Number of bytes.
 */
void keel_xxh64_update(keel_xxh64_state_t* state, const void* input, size_t len) {
    const uint8_t* p = (const uint8_t*)input;
    const uint8_t* const end = p + len;

    state->total_len += len;

    if (state->memsize + len < 32) {
        memcpy(((uint8_t*)state->mem) + state->memsize, input, len);
        state->memsize += (uint32_t)len;
        return;
    }

    if (state->memsize) {
        size_t fill = 32 - state->memsize;
        memcpy(((uint8_t*)state->mem) + state->memsize, p, fill);
        p += fill;

        state->v1 = xxh64_round(state->v1, state->mem[0]);
        state->v2 = xxh64_round(state->v2, state->mem[1]);
        state->v3 = xxh64_round(state->v3, state->mem[2]);
        state->v4 = xxh64_round(state->v4, state->mem[3]);
        state->memsize = 0;
    }

    if (p + 32 <= end) {
        const uint8_t* const limit = end - 32;
        uint64_t v1 = state->v1;
        uint64_t v2 = state->v2;
        uint64_t v3 = state->v3;
        uint64_t v4 = state->v4;

        do {
            v1 = xxh64_round(v1, xxh_read64(p)); p += 8;
            v2 = xxh64_round(v2, xxh_read64(p)); p += 8;
            v3 = xxh64_round(v3, xxh_read64(p)); p += 8;
            v4 = xxh64_round(v4, xxh_read64(p)); p += 8;
        } while (p <= limit);

        state->v1 = v1;
        state->v2 = v2;
        state->v3 = v3;
        state->v4 = v4;
    }

    if (p < end) {
        memcpy(state->mem, p, (size_t)(end - p));
        state->memsize = (uint32_t)(end - p);
    }
}

/**
 * @brief Finalise a streaming XXH64 computation and return the digest.
 *
 * @param state  Streaming state populated by keel_xxh64_update().
 * @return 64-bit hash value.
 */
uint64_t keel_xxh64_digest(const keel_xxh64_state_t* state) {
    const uint8_t* p = (const uint8_t*)state->mem;
    const uint8_t* const end = p + state->memsize;
    uint64_t h64;

    if (state->total_len >= 32) {
        h64 = xxh_rotl64(state->v1, 1) + xxh_rotl64(state->v2, 7) +
              xxh_rotl64(state->v3, 12) + xxh_rotl64(state->v4, 18);

        h64 = xxh64_merge_round(h64, state->v1);
        h64 = xxh64_merge_round(h64, state->v2);
        h64 = xxh64_merge_round(h64, state->v3);
        h64 = xxh64_merge_round(h64, state->v4);
    } else {
        h64 = state->v3 /* seed */ + XXH_PRIME64_5;
    }

    h64 += state->total_len;

    while (p + 8 <= end) {
        uint64_t k1 = xxh64_round(0, xxh_read64(p));
        h64 ^= k1;
        h64 = xxh_rotl64(h64, 27) * XXH_PRIME64_1 + XXH_PRIME64_4;
        p += 8;
    }

    if (p + 4 <= end) {
        h64 ^= (uint64_t)xxh_read32(p) * XXH_PRIME64_1;
        h64 = xxh_rotl64(h64, 23) * XXH_PRIME64_2 + XXH_PRIME64_3;
        p += 4;
    }

    while (p < end) {
        h64 ^= (*p++) * XXH_PRIME64_5;
        h64 = xxh_rotl64(h64, 11) * XXH_PRIME64_1;
    }

    h64 ^= h64 >> 33;
    h64 *= XXH_PRIME64_2;
    h64 ^= h64 >> 29;
    h64 *= XXH_PRIME64_3;
    h64 ^= h64 >> 32;

    return h64;
}
