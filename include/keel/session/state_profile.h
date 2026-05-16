/**
 * @file state_profile.h
 * @brief Canonical session-configuration profiles and minimal sync-SQL generation.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Pooling only works if KEEL can tell whether a borrowed backend already matches
 * the frontend session's mutable configuration. Replaying every historical `SET`
 * command would be expensive and brittle, so the session subsystem compresses the
 * observable configuration into a canonical sorted profile of key/value pairs.
 *
 * That design gives the borrow/return path three useful properties:
 *
 * - equality checks are usually reduced to a count-plus-hash comparison;
 * - deterministic key ordering makes profile diffs reproducible and easy to
 *   compare across workers or logs;
 * - synchronization SQL can be generated as a minimal `SET`/`RESET` script
 *   instead of blasting the backend with a full reset-and-replay sequence.
 *
 * The implementation deliberately uses fixed-size arrays and bounded strings. The
 * goal is hot-path predictability, not unbounded configurability.
 */

#ifndef KEEL_STATE_PROFILE_H
#define KEEL_STATE_PROFILE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Constants
 * ============================================================================ */

#define STATE_PROFILE_MAX_PARAMS    64  /**< Max SET parameters tracked */
#define STATE_PROFILE_KEY_MAX       64  /**< Max key length */
#define STATE_PROFILE_VALUE_MAX     256 /**< Max value length */

/* ============================================================================
 * Key-Value Pair
 * ============================================================================ */

typedef struct state_kv_pair {
    char    key[STATE_PROFILE_KEY_MAX];
    char    value[STATE_PROFILE_VALUE_MAX];
} state_kv_pair_t;

/* ============================================================================
 * State Profile (Spec §5.1)
 * ============================================================================
 *
 * Canonical representation: sorted by key, deterministic hash.
 * Profiles are compared first by hash (fast path), then by content (if needed).
 */

typedef struct state_profile {
    state_kv_pair_t sorted_params[STATE_PROFILE_MAX_PARAMS];
    uint32_t        count;      /**< Number of active parameters */
    uint64_t        hash;       /**< XXHash64 of canonical form */
} state_profile_t;

/* ============================================================================
 * Profile Operations
 * ============================================================================ */

/**
 * @brief Initialize a profile to the canonical clean-connection state.
 *
 * A zeroed profile represents a backend with no tracked session-level settings.
 * That state is important because it is the target shape KEEL expects when a
 * backend is freshly opened or successfully reset before returning to the pool.
 *
 * @param profile Profile object to initialize.
 * @return
 */
void state_profile_init(state_profile_t* profile);

/**
 * @brief Set a parameter in the profile
 *
 * Inserts or updates the key-value pair, maintaining sorted order.
 * Recomputes the hash.
 *
 * @param profile Profile to update
 * @param key     Parameter name (e.g., "search_path")
 * @param value   Parameter value (e.g., "public")
 * @return 0 on success, -1 if profile is full
 */
int state_profile_set(state_profile_t* profile, const char* key, const char* value);

/**
 * @brief Remove a parameter from the profile (RESET)
 *
 * @param profile Profile to update
 * @param key     Parameter name to remove
 * @return 0 on success, -1 if key not found
 */
int state_profile_reset(state_profile_t* profile, const char* key);

/**
 * @brief Clear all parameters (DISCARD ALL / RESET ALL)
 */
void state_profile_clear(state_profile_t* profile);

/**
 * @brief Check if two profiles are identical (fast hash check)
 */
static inline bool state_profile_equal_fast(
    const state_profile_t* a,
    const state_profile_t* b)
{
    return a->hash == b->hash && a->count == b->count;
}

/**
 * @brief Perform a collision-safe equality comparison between two profiles.
 *
 * Hash and count are used as a fast reject path, but matching hashes are still
 * verified entry-by-entry so correctness never relies solely on the 64-bit hash.
 *
 * @param a First profile.
 * @param b Second profile.
 * @return `true` if both profiles contain identical canonical key/value pairs.
 */
bool state_profile_equal(const state_profile_t* a, const state_profile_t* b);

/**
 * @brief Clone one profile into another.
 *
 * The copy is structural and constant-time because profiles are fixed-size value
 * objects with no internal heap ownership.
 *
 * @param dst [out] Destination profile.
 * @param src Source profile to copy.
 * @return
 */
void state_profile_copy(state_profile_t* dst, const state_profile_t* src);

/**
 * @brief Lookup a tracked configuration parameter by key.
 *
 * The search uses the same case-insensitive ordering relation as insertion so
 * callers can treat protocol parameter names according to database semantics
 * rather than C string case sensitivity.
 *
 * @param profile Profile to inspect.
 * @param key Parameter name to search for.
 * @return Pointer to the stored value string, or `NULL` if the key is absent.
 */
const char* state_profile_get(const state_profile_t* profile, const char* key);

/* ============================================================================
 * State Diff Generator (Spec §5.2)
 * ============================================================================
 *
 * Generates minimal SQL to synchronize a backend from one profile to another.
 *
 * Algorithm:
 *   For each key in `from` not in `to`:  → RESET <key>
 *   For each key in `to` not in `from`:  → SET <key> = '<value>'
 *   For each key in both with different values: → SET <key> = '<value>'
 *   Keys with identical values: → no-op
 */

#define STATE_SYNC_SQL_MAX  4096  /**< Max generated SQL length */

typedef struct state_sync_result {
    char    sql[STATE_SYNC_SQL_MAX]; /**< Generated SQL commands (semicolon separated) */
    size_t  sql_len;                 /**< Length of generated SQL */
    int     set_count;               /**< Number of SET commands */
    int     reset_count;             /**< Number of RESET commands */
    bool    needs_sync;              /**< True if any SQL was generated */
} state_sync_result_t;

/**
 * @brief Generate the minimal SQL script needed to transform one profile into another.
 *
 * The algorithm performs a merge walk over two sorted arrays. This is cheaper and
 * more deterministic than replaying a full command history, while still emitting
 * only the changes needed to bring a borrowed backend into semantic alignment
 * with the frontend session.
 *
 * `NULL` profiles are treated as the clean baseline, which lets callers describe
 * transitions to or from a pristine backend without allocating temporary objects.
 *
 * @param from Current backend profile, or `NULL` for a clean backend.
 * @param to Desired session profile, or `NULL` for a clean target state.
 * @param result [out] Receives generated SQL text and per-command counters.
 * @return `0` on success, or `-1` if the generated script would overflow the fixed buffer.
 */
int generate_sync_sql(
    const state_profile_t* from,
    const state_profile_t* to,
    state_sync_result_t* result);

/* ============================================================================
 * Profile Debug/Logging
 * ============================================================================ */

/**
 * @brief Dump a profile to `stderr` for debugging and trace analysis.
 *
 * @param profile Profile to print.
 * @param label Optional human-readable label shown alongside the dump.
 * @return
 */
void state_profile_dump(const state_profile_t* profile, const char* label);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_STATE_PROFILE_H */
