/**
 * @file ssv.c
 * @brief Small policy helpers for semantic state virtualization decisions.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * These routines deliberately keep no state of their own. They are pure or
 * nearly pure translations from flow-level facts into engine decisions about pin
 * exposure, backend-release eligibility, and primary-routing requirements.
 */

#include "keel/session/ssv.h"

#ifndef KEEL_STICKY_PRIMARY_TTL_MS
#define KEEL_STICKY_PRIMARY_TTL_MS 100
#endif

/**
 * @brief Translate internal flow pin bits into exported session pin reasons.
 */
keel_pin_reason_t keel_ssv_pin_reason_from_flow_pins(
    keel_flow_pin_reason_t pins)
{
    keel_pin_reason_t reasons = KEEL_PIN_NONE;

    if (pins & KEEL_FPIN_TEMP_TABLE)
        reasons |= KEEL_PIN_TEMP_TABLE;
    if (pins & KEEL_FPIN_LISTEN)
        reasons |= KEEL_PIN_LISTEN;
    if (pins & KEEL_FPIN_CURSOR)
        reasons |= KEEL_PIN_DECLARE_CURSOR;
    if (pins & KEEL_FPIN_COPY)
        reasons |= KEEL_PIN_COPY;
    if (pins & KEEL_FPIN_SET_ROLE)
        reasons |= KEEL_PIN_SET_ROLE;
    if (pins & KEEL_FPIN_ADVISORY_LOCK)
        reasons |= KEEL_PIN_ADVISORY_LOCK;
    if (pins & KEEL_FPIN_PREPARED_STMT)
        reasons |= KEEL_PIN_PREPARED_STMT;
    if (pins & KEEL_FPIN_GET_LOCK)
        reasons |= KEEL_PIN_GET_LOCK;
    if (pins & KEEL_FPIN_USER_VARIABLE)
        reasons |= KEEL_PIN_USER_VARIABLE;
    if (pins & KEEL_FPIN_LOCK_TABLE)
        reasons |= KEEL_PIN_LOCK_TABLE;
    if (pins & KEEL_FPIN_EXTENDED_PROTO)
        reasons |= KEEL_PIN_EXTENDED_PROTOCOL;
    if (pins & KEEL_FPIN_OSC)
        reasons |= KEEL_PIN_OSC;

    return reasons;
}

/**
 * @brief Remove pin bits that are replay-safe under the active PS mode.
 */
keel_flow_pin_reason_t keel_ssv_release_blocking_pins(
    keel_flow_pin_reason_t pins,
    keel_ps_mode_t ps_mode)
{
    /* OFF and PINNING both hard-pin the backend for the session lifetime:
     * PREPARED_STMT must remain a blocking pin so the backend is never
     * returned between transactions.  Any other pins are also blocking. */
    if (ps_mode == KEEL_PS_MODE_OFF || ps_mode == KEEL_PS_MODE_PINNING)
        return pins;

    return pins & ~(keel_flow_pin_reason_t)KEEL_FPIN_PREPARED_STMT;
}

/**
 * @brief Test whether a session may detach from its backend without losing state.
 */
bool keel_ssv_allows_backend_release(
    keel_flow_pin_reason_t pins,
    keel_ps_mode_t ps_mode)
{
    return keel_ssv_release_blocking_pins(pins, ps_mode) == KEEL_FPIN_NONE;
}

/**
 * @brief Detect the special case where prepared statements are the only pin source.
 */
bool keel_ssv_is_stmt_only_pin(
    keel_flow_pin_reason_t pins,
    keel_ps_mode_t ps_mode)
{
    if (pins != KEEL_FPIN_PREPARED_STMT)
        return false;
    /* VIRTUALIZE, TRACKING, and ANONYMOUS all support statement replay:
     * the engine may release the backend and re-acquire a fresh one,
     * replaying the saved PREPARE messages.  PINNING and OFF never
     * replay — the backend must remain hard-pinned. */
    return ps_mode == KEEL_PS_MODE_VIRTUALIZE
        || ps_mode == KEEL_PS_MODE_TRACKING
        || ps_mode == KEEL_PS_MODE_ANONYMOUS;
}

/**
 * @brief Decide whether recent-write consistency still forces primary routing.
 */
bool keel_ssv_requires_primary(
    const keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT],
    uint64_t now_ns,
    uint32_t ttl_ms)
{
    return keel_ssv_consistency_has_write_lsn(atoms) &&
           !keel_ssv_consistency_ttl_ok(atoms, now_ns,
                                        ttl_ms ? ttl_ms : KEEL_STICKY_PRIMARY_TTL_MS);
}

/**
 * @brief Check whether opaque unknown state requires a hard backend reset.
 */
bool keel_ssv_needs_discard(const keel_ssv_atom_t opaque_atoms[KEEL_SSV_OK__COUNT])
{
    return keel_ssv_opaque_has_unknown(opaque_atoms);
}