/**
 * @file xxhash.h
 * @brief Minimal XXHash declarations for fast, non-cryptographic fingerprints.
 *
 * KEEL uses xxHash where FNV-1a or Murmur would still work functionally but do
 * not provide the same throughput or avalanche characteristics for longer data
 * streams. Query fingerprinting is the main example: callers need a stable,
 * cheap checksum-like identifier, not cryptographic resistance.
 *
 * The implementation in this repository deliberately targets portability and a
 * small integration surface rather than every optimization from the upstream
 * library. That keeps the dependency story simple while still exposing the
 * one-shot and streaming interfaces most of KEEL needs.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_XXHASH_H
#define KEEL_XXHASH_H

#include "keel_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * XXH32 - 32-bit hash
 * ============================================================================ */

/**
 * @brief Compute the one-shot 32-bit xxHash digest of a byte range.
 *
 * @param data Input bytes.
 * @param len Length in bytes.
 * @param seed Seed value used to namespace hash domains.
 * @return 32-bit xxHash digest.
 */
uint32_t keel_xxh32(const void* data, size_t len, uint32_t seed);

/**
 * @brief Streaming XXH32 state
 */
typedef struct keel_xxh32_state {
    uint64_t total_len;
    uint32_t large_len;
    uint32_t v1;
    uint32_t v2;
    uint32_t v3;
    uint32_t v4;
    uint32_t mem[4];
    uint32_t memsize;
} keel_xxh32_state_t;

/**
 * @brief Initialize a streaming XXH32 state structure.
 *
 * @param state State object to reset.
 * @param seed Seed value used for this hash stream.
 * @return
 */
void keel_xxh32_reset(keel_xxh32_state_t* state, uint32_t seed);

/**
 * @brief Feed another byte range into a streaming XXH32 computation.
 *
 * @param state Streaming state to update.
 * @param data Bytes to incorporate.
 * @param len Number of bytes in `data`.
 * @return
 */
void keel_xxh32_update(keel_xxh32_state_t* state, const void* data, size_t len);

/**
 * @brief Finalize a streaming XXH32 computation without modifying the state.
 *
 * @param state Streaming state to digest.
 * @return Final 32-bit digest.
 */
uint32_t keel_xxh32_digest(const keel_xxh32_state_t* state);

/* ============================================================================
 * XXH64 - 64-bit hash
 * ============================================================================ */

/**
 * @brief Compute the one-shot 64-bit xxHash digest of a byte range.
 *
 * @param data Input bytes.
 * @param len Length in bytes.
 * @param seed Seed value used to namespace hash domains.
 * @return 64-bit xxHash digest.
 */
uint64_t keel_xxh64(const void* data, size_t len, uint64_t seed);

/**
 * @brief Streaming XXH64 state
 */
typedef struct keel_xxh64_state {
    uint64_t total_len;
    uint64_t v1;
    uint64_t v2;
    uint64_t v3;
    uint64_t v4;
    uint64_t mem[4];
    uint32_t memsize;
} keel_xxh64_state_t;

/**
 * @brief Initialize a streaming XXH64 state structure.
 *
 * @param state State object to reset.
 * @param seed Seed value used for this hash stream.
 * @return
 */
void keel_xxh64_reset(keel_xxh64_state_t* state, uint64_t seed);

/**
 * @brief Feed another byte range into a streaming XXH64 computation.
 *
 * @param state Streaming state to update.
 * @param data Bytes to incorporate.
 * @param len Number of bytes in `data`.
 * @return
 */
void keel_xxh64_update(keel_xxh64_state_t* state, const void* data, size_t len);

/**
 * @brief Finalize a streaming XXH64 computation without modifying the state.
 *
 * @param state Streaming state to digest.
 * @return Final 64-bit digest.
 */
uint64_t keel_xxh64_digest(const keel_xxh64_state_t* state);

/* ============================================================================
 * Convenience Functions for KEEL
 * ============================================================================ */

/**
 * @brief Hash a null-terminated string
 *
 * @param str   String to hash
 * @param seed  Hash seed
 * @return 64-bit hash
 */
KEEL_INLINE uint64_t keel_xxh64_str(const char* str, uint64_t seed) {
    size_t len = 0;
    const char* p = str;
    while (*p++) len++;
    return keel_xxh64(str, len, seed);
}

/**
 * @brief Hash a string view
 *
 * @param data  String data
 * @param len   String length
 * @param seed  Hash seed
 * @return 64-bit hash
 */
KEEL_INLINE uint64_t keel_xxh64_view(const char* data, size_t len, uint64_t seed) {
    return keel_xxh64(data, len, seed);
}

/**
 * @brief Create a query fingerprint
 *
 * This hashes a SQL query to create a fingerprint for caching/comparison.
 * Uses seed 0 for consistency.
 *
 * @param query     Query string
 * @param len       Query length
 * @return 64-bit fingerprint
 */
KEEL_INLINE uint64_t keel_query_fingerprint(const char* query, size_t len) {
    return keel_xxh64(query, len, 0);
}

/**
 * @brief Combine two hash values
 *
 * Useful for hashing composite keys (e.g., database + table).
 *
 * @param h1    First hash
 * @param h2    Second hash
 * @return Combined hash
 */
KEEL_INLINE uint64_t keel_xxh64_combine(uint64_t h1, uint64_t h2) {
    /* Mix the hashes using XXH64 prime */
    const uint64_t PRIME64_1 = 0x9E3779B185EBCA87ULL;
    return (h1 ^ h2) * PRIME64_1;
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_XXHASH_H */
