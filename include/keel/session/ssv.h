/**
 * @file ssv.h
 * @brief Protocol-neutral helpers for semantic state virtualization policy.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Semantic state virtualization (SSV) is the layer that decides whether a
 * frontend session may safely detach from one backend and continue on another.
 * Protocol adapters report low-level facts such as prepared statements, temp
 * tables, write-position tokens, or unknown side effects; these helpers turn that
 * raw information into engine-facing policy decisions.
 *
 * Keeping the logic here has two benefits:
 *
 * - the engine can ask high-level questions such as "may I release this backend?"
 *   without hard-coding PostgreSQL or MySQL details;
 * - protocol implementations stay focused on detection and token capture instead
 *   of duplicating release/primary-routing policy.
 */

#ifndef KEEL_SSV_H
#define KEEL_SSV_H

#include <stdbool.h>

#include "keel/protocol/protocol_flow.h"
#include "keel/session/hardpin.h"
#include "keel/session/ssv_atom.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Translate protocol-flow pin bits into session-level pin reasons.
 *
 * The flow layer uses its own compact internal bit space. This helper projects
 * those bits into the stable `keel_pin_reason_t` API used by session management,
 * observability, and migration eligibility checks.
 *
 * @param pins Flow-reported pin bitmask.
 * @return Equivalent session pin-reason bitmask.
 */
keel_pin_reason_t keel_ssv_pin_reason_from_flow_pins(
    keel_flow_pin_reason_t pins);

/**
 * @brief Filter a pin set down to the reasons that still block backend release.
 *
 * Prepared-statement pins are special: when prepared-statement replay is enabled,
 * a session that is pinned only because of prepared statements may still release a
 * backend because the state can be reconstructed later. In non-replay mode, the
 * same bit remains release-blocking.
 *
 * @param pins Flow pin bitmask under consideration.
 * @param ps_mode Prepared-statement handling mode.
 * @return Reduced bitmask containing only release-blocking pins.
 */
keel_flow_pin_reason_t keel_ssv_release_blocking_pins(
    keel_flow_pin_reason_t pins,
    keel_ps_mode_t ps_mode);

/**
 * @brief Test whether a session's current pin set permits backend detachment.
 *
 * @param pins Flow pin bitmask.
 * @param ps_mode Prepared-statement handling mode.
 * @return `true` if no remaining release-blocking pin exists.
 */
bool keel_ssv_allows_backend_release(
    keel_flow_pin_reason_t pins,
    keel_ps_mode_t ps_mode);

/**
 * @brief Detect the narrow case where prepared statements are the only pinning factor.
 *
 * This is useful for borrow heuristics that treat statement-replay-capable
 * sessions more leniently than sessions carrying non-virtualizable state.
 *
 * @param pins Flow pin bitmask.
 * @param ps_mode Prepared-statement handling mode.
 * @return `true` if prepared statements are the sole pin reason under a replay-capable mode.
 */
bool keel_ssv_is_stmt_only_pin(
    keel_flow_pin_reason_t pins,
    keel_ps_mode_t ps_mode);

/**
 * @brief Check whether this session's consistency atoms require primary routing.
 *
 * Returns true if the session carries a write-position token that has not been
 * proven satisfied by a replica.  v0.5-alpha uses conservative primary fallback:
 * a real LSN/GTID token does not expire by wall-clock time alone because lag can
 * outlive any configured sticky window.
 *
 * @param atoms  The session's consistency_atoms[3] array.
 * @param now_ns Current monotonic timestamp (nanoseconds).
 * @param ttl_ms Reserved for legacy timestamp-only sticky-primary callers.
 * @return true if primary routing is required for read-after-write safety.
 */
bool keel_ssv_requires_primary(
    const keel_ssv_atom_t atoms[KEEL_SSV_CK__COUNT],
    uint64_t now_ns,
    uint32_t ttl_ms);

/**
 * @brief Check whether the session carries opaque state that requires a hard reset.
 *
 * @param opaque_atoms Session opaque-state atom array.
 * @return `true` if the backend must be cleaned with `DISCARD ALL` before reuse.
 */
bool keel_ssv_needs_discard(const keel_ssv_atom_t opaque_atoms[KEEL_SSV_OK__COUNT]);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_SSV_H */
