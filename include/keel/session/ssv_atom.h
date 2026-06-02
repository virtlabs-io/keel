/**
 * @file ssv_atom.h
 * @brief Atom definitions and inline helpers for semantic state virtualization.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * SSV atoms are KEEL's compact intermediate representation for session state that
 * matters to multiplexing safety. Instead of forcing every engine decision to
 * inspect protocol-specific structures, adapters project meaningful state into a
 * small set of typed atoms: configuration digests, consistency tokens, opaque
 * side effects, and other domain-specific facts.
 *
 * The atom model encodes three dimensions at once:
 *
 * - domain: what kind of state this is;
 * - virtualization class: whether the state can be replayed or transferred;
 * - cost class: how expensive it is to honour that state when borrowing.
 *
 * The helpers in this header are all inline, allocation-free, and worker-local.
 * That constraint matters because they sit directly on the borrow/release hot
 * path and must not introduce locks or secondary heap traffic.
 */

#ifndef KEEL_SSV_ATOM_H
#define KEEL_SSV_ATOM_H

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "keel/plugin/plugin_types.h"  /* keel_consistency_token_t */

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Domain — what kind of session state does this atom represent?
 * ============================================================================ */

typedef enum keel_ssv_domain {
    KEEL_SSV_DOMAIN_CONFIG       = 0,  /**< SET variable (search_path, DateStyle, …) */
    KEEL_SSV_DOMAIN_EXEC_OBJECT  = 1,  /**< Prepared statement, cursor, portal */
    KEEL_SSV_DOMAIN_TXN          = 2,  /**< Transaction boundary state */
    KEEL_SSV_DOMAIN_NAMESPACE    = 3,  /**< Temp objects, schema search order */
    KEEL_SSV_DOMAIN_SECURITY     = 4,  /**< SET ROLE, session auth, advisory locks */
    KEEL_SSV_DOMAIN_CONSISTENCY  = 5,  /**< WAL LSN / GTID write-position token */
    KEEL_SSV_DOMAIN_OPAQUE       = 6,  /**< Unknown / unsupported state */
    KEEL_SSV_DOMAIN__COUNT       = 7
} keel_ssv_domain_t;

/* ============================================================================
 * Virtualization class — can the proxy transparently replay this atom?
 * ============================================================================ */

typedef enum keel_ssv_virt_class {
    KEEL_SSV_VIRT_FULL        = 0,  /**< Replay / restore on any backend */
    KEEL_SSV_VIRT_CONDITIONAL = 1,  /**< Replay possible under constraints */
    KEEL_SSV_VIRT_NONE        = 2,  /**< Cannot replay — hard-pin only */
    KEEL_SSV_VIRT_OPAQUE      = 3,  /**< Unknown — treat as NONE */
} keel_ssv_virt_class_t;

/* ============================================================================
 * Cost class — how expensive is the replay / check?
 * ============================================================================ */

typedef enum keel_ssv_cost_class {
    KEEL_SSV_COST_CHEAP      = 0,  /**< < ~100 CPU cycles (hash compare) */
    KEEL_SSV_COST_MODERATE    = 1,  /**< < ~1000 cycles (string compare, GUC lookup) */
    KEEL_SSV_COST_EXPENSIVE   = 2,  /**< Needs network I/O (replay Parse, LSN check).
                                     *   Gated to FULL runtime tier only. */
    KEEL_SSV_COST_PROHIBITIVE = 3,  /**< Don't attempt (e.g. temp-table re-creation) */
} keel_ssv_cost_class_t;

/* ============================================================================
 * Atom value union — protocol-agnostic storage
 * ============================================================================ */

typedef union keel_ssv_value {
    uint64_t u64;
    int64_t  i64;
    bool     flag;
    double   f64;
    char     str[KEEL_CONSISTENCY_TOKEN_MAX];
} keel_ssv_value_t;

/* ============================================================================
 * SSV Atom — one observable unit of session state
 * ============================================================================ */

typedef struct keel_ssv_atom {
    keel_ssv_domain_t     domain;
    keel_ssv_virt_class_t virt_class;
    keel_ssv_cost_class_t cost_class;
    uint16_t              key;        /**< Domain-specific sub-key */
    keel_ssv_value_t      value;
} keel_ssv_atom_t;

/* ============================================================================
 * Consistency Atom — specialised accessors for DOMAIN_CONSISTENCY
 *
 * The consistency atom carries the WAL LSN string from the last committed
 * write on the primary backend.  The borrow algorithm uses it to determine
 * whether a candidate replica has advanced past this LSN.
 *
 * Sub-keys for DOMAIN_CONSISTENCY:
 * ============================================================================ */

typedef enum keel_ssv_consistency_key {
    /** WAL LSN captured after last write/DDL commit (PG: pg_current_wal_lsn).
     *  Value: NUL-terminated LSN string in atom.value.str. */
    KEEL_SSV_CK_WRITE_LSN        = 0,

    /** Monotonic capture timestamp (CLOCK_MONOTONIC_COARSE nanoseconds).
     *  Value: atom.value.u64.  Used for TTL expiry of sticky-primary. */
    KEEL_SSV_CK_WRITE_LSN_TS     = 1,

    KEEL_SSV_CK__COUNT            = 2,
} keel_ssv_consistency_key_t;

/* ============================================================================
 * Opaque Atom — DOMAIN_OPAQUE sub-keys
 *
 * Unmodelled semantic dirtiness.  Anything classified here cannot be
 * virtualised and forces a full backend reset (DISCARD ALL) on pool return.
 * Deliberately separate from CONSISTENCY (which carries LSN / GTID obligations)
 * to keep domain semantics clean.
 * ============================================================================ */

typedef enum keel_ssv_opaque_key {
    /** Session has unmodelled state (DO $$, CALL, C-extension side effects).
     *  Value: atom.value.flag.
     *  When true, force DISCARD ALL on backend before pool return. */
    KEEL_SSV_OK_UNKNOWN_STATE     = 0,

    KEEL_SSV_OK__COUNT            = 1,
} keel_ssv_opaque_key_t;

/* ============================================================================
 * Config Atom — DOMAIN_CONFIG sub-keys
 *
 * Tracks session-level SET variable state as a single XXHash64 digest
 * (the "state profile hash").  This lets the borrow algorithm prefer
 * backends whose GUC configuration already matches the session, avoiding
 * a DISCARD ALL → SET replay round-trip.
 * ============================================================================ */

typedef enum keel_ssv_config_key {
    /** XXHash64 digest of the session's current GUC key-value pairs.
     *  Value: atom.value.u64.  Zero means "clean / no SET changes". */
    KEEL_SSV_CFG_PROFILE_HASH     = 0,

    KEEL_SSV_CFG__COUNT           = 1,
} keel_ssv_config_key_t;

/* ============================================================================
 * Consistency Atom — inline helpers (zero allocation, no locking)
 * ============================================================================ */

/**
 * @brief Initialise the consistency-domain atom array.
 *
 * @param atoms Array of at least `KEEL_SSV_CK__COUNT` atoms.
 */
static inline void keel_ssv_consistency_init(keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT])
{
    memset(atoms, 0, KEEL_SSV_CK__COUNT * sizeof(keel_ssv_atom_t));

    /* WRITE_LSN: conditional virtualisation, expensive (network I/O) */
    atoms[KEEL_SSV_CK_WRITE_LSN].domain     = KEEL_SSV_DOMAIN_CONSISTENCY;
    atoms[KEEL_SSV_CK_WRITE_LSN].virt_class = KEEL_SSV_VIRT_CONDITIONAL;
    atoms[KEEL_SSV_CK_WRITE_LSN].cost_class = KEEL_SSV_COST_EXPENSIVE;
    atoms[KEEL_SSV_CK_WRITE_LSN].key        = KEEL_SSV_CK_WRITE_LSN;

    /* WRITE_LSN_TS: timestamp — cheap compare */
    atoms[KEEL_SSV_CK_WRITE_LSN_TS].domain     = KEEL_SSV_DOMAIN_CONSISTENCY;
    atoms[KEEL_SSV_CK_WRITE_LSN_TS].virt_class = KEEL_SSV_VIRT_FULL;
    atoms[KEEL_SSV_CK_WRITE_LSN_TS].cost_class = KEEL_SSV_COST_CHEAP;
    atoms[KEEL_SSV_CK_WRITE_LSN_TS].key        = KEEL_SSV_CK_WRITE_LSN_TS;
}

/**
 * @brief Store a captured consistency token into the atom array.
 *
 * Copies the LSN string and timestamp from a keel_consistency_token_t into
 * the WRITE_LSN and WRITE_LSN_TS atoms respectively.
 *
 * @param atoms Consistency atom array.
 * @param token Token captured by the protocol adapter.
 */
static inline void keel_ssv_consistency_set_token(
    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT],
    const keel_consistency_token_t* token)
{
    memcpy(atoms[KEEL_SSV_CK_WRITE_LSN].value.str,
           token->value, KEEL_CONSISTENCY_TOKEN_MAX);
    atoms[KEEL_SSV_CK_WRITE_LSN_TS].value.u64 = token->captured_at_ns;
}

/**
 * @brief Store only the write timestamp for sticky-primary fallback.
 *
 * Used when a write has completed far enough to require read-your-writes
 * protection, but no exact LSN/GTID token has been captured yet.
 */
static inline void keel_ssv_consistency_set_write_ts(
    keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT],
    uint64_t captured_at_ns)
{
    atoms[KEEL_SSV_CK_WRITE_LSN_TS].value.u64 = captured_at_ns;
}

/**
 * @brief Read the LSN string from the consistency atom.
 *
 * @param atoms Consistency atom array.
 * @return Pointer to the stored token string, or an empty string if none is present.
 */
static inline const char* keel_ssv_consistency_get_lsn(
    const keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT])
{
    return atoms[KEEL_SSV_CK_WRITE_LSN].value.str;
}

/**
 * @brief Read the capture timestamp from the consistency atom.
 *
 * @param atoms Consistency atom array.
 * @return Monotonic nanosecond timestamp, or 0 if no write recorded.
 */
static inline uint64_t keel_ssv_consistency_get_ts(
    const keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT])
{
    return atoms[KEEL_SSV_CK_WRITE_LSN_TS].value.u64;
}

/**
 * @brief Check whether the session has a pending write-LSN requirement.
 *
 * @param atoms Consistency atom array.
 * @return true if a non-empty LSN string is stored.
 */
static inline bool keel_ssv_consistency_has_write_lsn(
    const keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT])
{
    return atoms[KEEL_SSV_CK_WRITE_LSN].value.str[0] != '\0';
}

/**
 * @brief Clear any stored consistency requirement from the atom array.
 */
static inline void keel_ssv_consistency_clear(keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT])
{
    atoms[KEEL_SSV_CK_WRITE_LSN].value.str[0] = '\0';
    atoms[KEEL_SSV_CK_WRITE_LSN_TS].value.u64 = 0;
}

/* ============================================================================
 * Opaque Atom — inline helpers (zero allocation, no locking)
 * ============================================================================ */

/**
 * @brief Initialise the opaque atom array.
 *
 * @param atoms Array of at least `KEEL_SSV_OK__COUNT` atoms.
 */
static inline void keel_ssv_opaque_init(keel_ssv_atom_t atoms[KEEL_SSV_OK__COUNT])
{
    memset(atoms, 0, KEEL_SSV_OK__COUNT * sizeof(keel_ssv_atom_t));

    atoms[KEEL_SSV_OK_UNKNOWN_STATE].domain     = KEEL_SSV_DOMAIN_OPAQUE;
    atoms[KEEL_SSV_OK_UNKNOWN_STATE].virt_class = KEEL_SSV_VIRT_NONE;
    atoms[KEEL_SSV_OK_UNKNOWN_STATE].cost_class = KEEL_SSV_COST_CHEAP;
    atoms[KEEL_SSV_OK_UNKNOWN_STATE].key        = KEEL_SSV_OK_UNKNOWN_STATE;
}

/**
 * @brief Mark the session as having unknown/unmodelled state.
 *
 * When set, the engine must issue DISCARD ALL on the backend before returning
 * it to the pool — the semantic state cannot be trusted.
 *
 * @param atoms Opaque atom array.
 */
static inline void keel_ssv_opaque_set_unknown(keel_ssv_atom_t atoms[KEEL_SSV_OK__COUNT])
{
    atoms[KEEL_SSV_OK_UNKNOWN_STATE].value.flag = true;
}

/**
 * @brief Check whether the session currently carries opaque unknown state.
 */
static inline bool keel_ssv_opaque_has_unknown(
    const keel_ssv_atom_t atoms[KEEL_SSV_OK__COUNT])
{
    return atoms[KEEL_SSV_OK_UNKNOWN_STATE].value.flag;
}

/**
 * @brief Clear opaque-state tracking after a successful reset or detach cleanup.
 */
static inline void keel_ssv_opaque_clear(keel_ssv_atom_t atoms[KEEL_SSV_OK__COUNT])
{
    atoms[KEEL_SSV_OK_UNKNOWN_STATE].value.flag = false;
}

/* ---- Legacy compatibility wrappers ---- */

/** @deprecated Use keel_ssv_opaque_set_unknown() instead. */
static inline void keel_ssv_consistency_set_unknown(keel_ssv_atom_t atoms[KEEL_SSV_OK__COUNT])
{ keel_ssv_opaque_set_unknown(atoms); }

/** @deprecated Use keel_ssv_opaque_has_unknown() instead. */
static inline bool keel_ssv_consistency_has_unknown(
    const keel_ssv_atom_t atoms[KEEL_SSV_OK__COUNT])
{ return keel_ssv_opaque_has_unknown(atoms); }

/* ============================================================================
 * Config Atom — inline helpers (zero allocation, no locking)
 * ============================================================================ */

/**
 * @brief Initialise the config atom array.
 *
 * @param atoms Array of at least `KEEL_SSV_CFG__COUNT` atoms.
 */
static inline void keel_ssv_config_init(keel_ssv_atom_t atoms[KEEL_SSV_CFG__COUNT])
{
    memset(atoms, 0, KEEL_SSV_CFG__COUNT * sizeof(keel_ssv_atom_t));

    atoms[KEEL_SSV_CFG_PROFILE_HASH].domain     = KEEL_SSV_DOMAIN_CONFIG;
    atoms[KEEL_SSV_CFG_PROFILE_HASH].virt_class = KEEL_SSV_VIRT_FULL;
    atoms[KEEL_SSV_CFG_PROFILE_HASH].cost_class = KEEL_SSV_COST_MODERATE;
    atoms[KEEL_SSV_CFG_PROFILE_HASH].key        = KEEL_SSV_CFG_PROFILE_HASH;
}

/**
 * @brief Store the session's current state-profile hash into the config atom.
 *
 * @param atoms Config atom array.
 * @param hash XXHash64 digest of tracked session configuration (`0` means clean).
 */
static inline void keel_ssv_config_set_profile_hash(
    keel_ssv_atom_t atoms[KEEL_SSV_CFG__COUNT],
    uint64_t hash)
{
    atoms[KEEL_SSV_CFG_PROFILE_HASH].value.u64 = hash;
}

/**
 * @brief Read the session's state-profile hash.
 *
 * @return Stored profile hash, or `0` if no configuration drift has been tracked.
 */
static inline uint64_t keel_ssv_config_get_profile_hash(
    const keel_ssv_atom_t atoms[KEEL_SSV_CFG__COUNT])
{
    return atoms[KEEL_SSV_CFG_PROFILE_HASH].value.u64;
}

/**
 * @brief Clear tracked configuration drift and return to the clean baseline.
 */
static inline void keel_ssv_config_clear(keel_ssv_atom_t atoms[KEEL_SSV_CFG__COUNT])
{
    atoms[KEEL_SSV_CFG_PROFILE_HASH].value.u64 = 0;
}

/**
 * @brief Check if a candidate replica satisfies the session's LSN requirement,
 *        using the sticky-primary TTL as a time-based fallback.
 *
 * Decision logic for legacy timestamp-only sticky-primary callers
 * (zero-allocation, no I/O):
 *   1. No write timestamp recorded → replica is fine (return true).
 *   2. TTL expired → timestamp-only fallback is no longer active (return true).
 *   3. TTL still active → caller must keep using primary (return false).
 *
 * @param atoms           Consistency atom array.
 * @param now_ns          Current CLOCK_MONOTONIC_COARSE nanoseconds.
 * @param ttl_ms          Sticky-primary TTL in milliseconds (0 = use default 100ms).
 * @return true if the sticky fallback window has expired. This does not prove
 *         a token-bearing replica is caught up; use keel_ssv_requires_primary()
 *         for the v0.5 conservative routing decision.
 */
static inline bool keel_ssv_consistency_ttl_ok(
    const keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT],
    uint64_t now_ns,
    uint32_t ttl_ms)
{
    uint64_t ts = keel_ssv_consistency_get_ts(atoms);
    if (ts == 0)
        return true;

    uint64_t ttl_ns = (uint64_t)(ttl_ms ? ttl_ms : 100) * 1000000ULL;
    return (now_ns - ts) >= ttl_ns;
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_SSV_ATOM_H */
