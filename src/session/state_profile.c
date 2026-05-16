/**
 * @file state_profile.c
 * @brief Canonical profile maintenance and minimal configuration-sync generation.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This module turns session-level configuration drift into a compact value object
 * that the borrow path can compare cheaply. The two central operations are:
 *
 * - maintain a sorted canonical list of tracked parameters as `SET` and `RESET`
 *   statements are observed;
 * - generate the smallest practical `SET`/`RESET` script that transforms one
 *   canonical profile into another.
 *
 * The code favors determinism over absolute minimal CPU work. Hashes are
 * recomputed after each mutation rather than maintained incrementally, which is a
 * reasonable tradeoff because profiles are intentionally bounded and updates are
 * far less frequent than equality checks.
 */

#include "keel/session/state_profile.h"
#include "keel/util/xxhash.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ============================================================================
 * Internal: Helpers
 * ============================================================================ */

/**
 * @brief Compare parameter names using database-style case-insensitive ordering.
 *
 * @param a First key.
 * @param b Second key.
 * @return Negative, zero, or positive according to `strcasecmp()` ordering.
 */
static int key_compare(const char* a, const char* b)
{
    return strcasecmp(a, b);
}

/**
 * @brief Locate a key in the canonical sorted parameter array.
 *
 * Returning the insertion point in the traditional negative form lets callers use
 * one search result for both update and insert operations, avoiding a second scan.
 *
 * @param profile Profile to search.
 * @param key Parameter name to locate.
 * @return Existing index, or `-(insertion_point + 1)` when absent.
 */
static int find_key(const state_profile_t* profile, const char* key)
{
    int lo = 0, hi = (int)profile->count - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        int cmp = key_compare(profile->sorted_params[mid].key, key);
        if (cmp == 0) return mid;
        if (cmp < 0) lo = mid + 1;
        else hi = mid - 1;
    }
    return -(lo + 1);  /* not found, lo = insertion point */
}

/**
 * @brief Recompute the canonical XXHash64 digest for a profile.
 *
 * The canonical byte stream is the sorted concatenation of `key=value\0`
 * fragments. Rebuilding it on each mutation is simple and deterministic, and the
 * bounded profile size keeps the cost acceptable while avoiding tricky
 * incremental-hash invalidation bugs.
 *
 * @param profile Profile whose hash must be rebuilt.
 * @return
 */
static void recompute_hash(state_profile_t* profile)
{
    if (profile->count == 0) {
        profile->hash = 0;
        return;
    }

    /*
     * Build canonical representation in stack buffer.
     * Max size: STATE_PROFILE_MAX_PARAMS * (KEY_MAX + VALUE_MAX + 2)
     * = 64 * (64 + 256 + 2) = 20608 bytes — fits on stack.
     */
    char buf[STATE_PROFILE_MAX_PARAMS * (STATE_PROFILE_KEY_MAX + STATE_PROFILE_VALUE_MAX + 2)];
    size_t len = 0;

    for (uint32_t i = 0; i < profile->count; i++) {
        size_t klen = strlen(profile->sorted_params[i].key);
        size_t vlen = strlen(profile->sorted_params[i].value);

        memcpy(buf + len, profile->sorted_params[i].key, klen);
        len += klen;
        buf[len++] = '=';
        memcpy(buf + len, profile->sorted_params[i].value, vlen);
        len += vlen;
        buf[len++] = '\0';
    }

    profile->hash = keel_xxh64(buf, len, 0xDB057A7EULL);
}

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Reset a profile to the empty clean-state baseline.
 */
void state_profile_init(state_profile_t* profile)
{
    memset(profile, 0, sizeof(*profile));
}

/**
 * @brief Insert or update one canonical key/value entry.
 *
 * Updates happen in place when the key already exists; otherwise the sorted array
 * is shifted to preserve canonical ordering. Because the array is bounded, the
 * memmove cost is small and predictable.
 *
 * @param profile Profile to mutate.
 * @param key Parameter name.
 * @param value Parameter value.
 * @return `0` on success, or `-1` if input is invalid or capacity is exhausted.
 */
int state_profile_set(state_profile_t* profile, const char* key, const char* value)
{
    if (!profile || !key || !value) return -1;

    int idx = find_key(profile, key);

    if (idx >= 0) {
        /* Key exists — update value */
        strncpy(profile->sorted_params[idx].value, value, STATE_PROFILE_VALUE_MAX - 1);
        profile->sorted_params[idx].value[STATE_PROFILE_VALUE_MAX - 1] = '\0';
        recompute_hash(profile);
        return 0;
    }

    /* Key not found — insert at sorted position */
    if (profile->count >= STATE_PROFILE_MAX_PARAMS) return -1;

    int insert_at = -(idx + 1);

    /* Shift elements to make room */
    if ((uint32_t)insert_at < profile->count) {
        memmove(&profile->sorted_params[insert_at + 1],
                &profile->sorted_params[insert_at],
                (profile->count - (uint32_t)insert_at) * sizeof(state_kv_pair_t));
    }

    strncpy(profile->sorted_params[insert_at].key, key, STATE_PROFILE_KEY_MAX - 1);
    profile->sorted_params[insert_at].key[STATE_PROFILE_KEY_MAX - 1] = '\0';
    strncpy(profile->sorted_params[insert_at].value, value, STATE_PROFILE_VALUE_MAX - 1);
    profile->sorted_params[insert_at].value[STATE_PROFILE_VALUE_MAX - 1] = '\0';

    profile->count++;
    recompute_hash(profile);
    return 0;
}

/**
 * @brief Remove one tracked parameter from the canonical profile.
 *
 * @param profile Profile to mutate.
 * @param key Parameter name to remove.
 * @return `0` on success, or `-1` if the key is absent or arguments are invalid.
 */
int state_profile_reset(state_profile_t* profile, const char* key)
{
    if (!profile || !key) return -1;

    int idx = find_key(profile, key);
    if (idx < 0) return -1;

    /* Shift elements down */
    if ((uint32_t)(idx + 1) < profile->count) {
        memmove(&profile->sorted_params[idx],
                &profile->sorted_params[idx + 1],
                (profile->count - (uint32_t)idx - 1) * sizeof(state_kv_pair_t));
    }

    profile->count--;
    memset(&profile->sorted_params[profile->count], 0, sizeof(state_kv_pair_t));
    recompute_hash(profile);
    return 0;
}

/**
 * @brief Erase all tracked parameters and restore the empty-profile hash.
 */
void state_profile_clear(state_profile_t* profile)
{
    if (!profile) return;
    memset(profile->sorted_params, 0, sizeof(profile->sorted_params));
    profile->count = 0;
    profile->hash = 0;
}

/**
 * @brief Compare two profiles for semantic equality.
 *
 * Matching hashes and counts are necessary but not sufficient; the code still
 * performs a content comparison to guard against hash collisions.
 *
 * @param a First profile.
 * @param b Second profile.
 * @return `true` if the profiles contain the same canonical entries.
 */
bool state_profile_equal(const state_profile_t* a, const state_profile_t* b)
{
    if (!a || !b) return (a == b);
    if (a->hash != b->hash || a->count != b->count) return false;

    /* Hash match + count match — verify content */
    for (uint32_t i = 0; i < a->count; i++) {
        if (key_compare(a->sorted_params[i].key, b->sorted_params[i].key) != 0)
            return false;
        if (strcmp(a->sorted_params[i].value, b->sorted_params[i].value) != 0)
            return false;
    }
    return true;
}

/**
 * @brief Copy one fixed-size profile value into another.
 */
void state_profile_copy(state_profile_t* dst, const state_profile_t* src)
{
    if (!dst || !src) return;
    memcpy(dst, src, sizeof(state_profile_t));
}

/**
 * @brief Lookup a tracked value by parameter name.
 */
const char* state_profile_get(const state_profile_t* profile, const char* key)
{
    if (!profile || !key) return NULL;
    int idx = find_key(profile, key);
    if (idx < 0) return NULL;
    return profile->sorted_params[idx].value;
}

/* ============================================================================
 * Sync SQL Generation (Spec §5.2)
 * ============================================================================ */

/**
 * @brief Append one `SET` statement to the generated sync script.
 *
 * `search_path` is emitted specially because PostgreSQL expects an identifier list
 * rather than a quoted literal for that parameter. The function does not attempt
 * SQL escaping; callers are expected to feed it values that already represent the
 * exact backend state KEEL intends to replay.
 *
 * @param result [in,out] Accumulated sync-script result.
 * @param key Parameter name to set.
 * @param value Parameter value to emit.
 * @return Bytes appended, or `-1` on fixed-buffer overflow.
 */
static int append_set(state_sync_result_t* result, const char* key, const char* value)
{
    size_t remaining = STATE_SYNC_SQL_MAX - result->sql_len;

    int n;
    if (strcasecmp(key, "search_path") == 0) {
        /* PostgreSQL search_path expects a comma-separated identifier list,
         * not a single quoted string literal. */
        n = snprintf(result->sql + result->sql_len, remaining,
                     "SET %s = %s;", key, value);
    } else {
        /* Format: SET key = 'value'; */
        n = snprintf(result->sql + result->sql_len, remaining,
                     "SET %s = '%s';", key, value);
    }

    if (n < 0 || (size_t)n >= remaining) return -1;

    result->sql_len += (size_t)n;
    result->set_count++;
    return n;
}

/**
 * @brief Append one `RESET` statement to the generated sync script.
 *
 * @param result [in,out] Accumulated sync-script result.
 * @param key Parameter name to reset.
 * @return Bytes appended, or `-1` on fixed-buffer overflow.
 */
static int append_reset(state_sync_result_t* result, const char* key)
{
    size_t remaining = STATE_SYNC_SQL_MAX - result->sql_len;

    int n = snprintf(result->sql + result->sql_len, remaining,
                     "RESET %s;", key);

    if (n < 0 || (size_t)n >= remaining) return -1;

    result->sql_len += (size_t)n;
    result->reset_count++;
    return n;
}

int generate_sync_sql(
    const state_profile_t* from,
    const state_profile_t* to,
    state_sync_result_t* result)
{
    if (!result) return -1;

    /* Initialize result */
    memset(result, 0, sizeof(*result));

    /* Treat NULL as the clean baseline so callers can model transitions to or
     * from a pristine backend without first materializing a temporary profile. */
    const state_profile_t empty;
    if (!from) { state_profile_init((state_profile_t*)&empty); from = &empty; }
    if (!to)   { state_profile_init((state_profile_t*)&empty); to   = &empty; }

    /* Matching canonical profiles need no replay at all. */
    if (state_profile_equal(from, to)) {
        result->needs_sync = false;
        return 0;
    }

    /* Perform a classic merge walk over the two sorted arrays. This produces a
     * stable minimal diff in O(n) time without secondary hash tables. */
    uint32_t fi = 0, ti = 0;

    while (fi < from->count && ti < to->count) {
        int cmp = key_compare(from->sorted_params[fi].key,
                              to->sorted_params[ti].key);

        if (cmp == 0) {
            /* Key in both — check if value changed */
            if (strcmp(from->sorted_params[fi].value,
                       to->sorted_params[ti].value) != 0) {
                if (append_set(result,
                               to->sorted_params[ti].key,
                               to->sorted_params[ti].value) < 0)
                    return -1;
            }
            fi++; ti++;
        } else if (cmp < 0) {
            /* Key in `from` but not in `to` → RESET */
            if (append_reset(result, from->sorted_params[fi].key) < 0)
                return -1;
            fi++;
        } else {
            /* Key in `to` but not in `from` → SET */
            if (append_set(result,
                           to->sorted_params[ti].key,
                           to->sorted_params[ti].value) < 0)
                return -1;
            ti++;
        }
    }

    /* Remaining keys in `from` → RESET */
    while (fi < from->count) {
        if (append_reset(result, from->sorted_params[fi].key) < 0)
            return -1;
        fi++;
    }

    /* Remaining keys in `to` → SET */
    while (ti < to->count) {
        if (append_set(result,
                       to->sorted_params[ti].key,
                       to->sorted_params[ti].value) < 0)
            return -1;
        ti++;
    }

    result->needs_sync = (result->set_count + result->reset_count) > 0;
    return 0;
}

/* ============================================================================
 * Debug
 * ============================================================================ */

/**
 * @brief Print a debug representation of the canonical profile.
 */
void state_profile_dump(const state_profile_t* profile, const char* label)
{
    if (!profile) {
        fprintf(stderr, "[state_profile] %s: (null)\n", label ? label : "?");
        return;
    }

    fprintf(stderr, "[state_profile] %s: count=%u hash=0x%016lx\n",
            label ? label : "?", profile->count, (unsigned long)profile->hash);

    for (uint32_t i = 0; i < profile->count; i++) {
        fprintf(stderr, "  [%u] %s = %s\n",
                i, profile->sorted_params[i].key, profile->sorted_params[i].value);
    }
}
