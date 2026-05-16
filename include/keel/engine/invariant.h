/**
 * @file invariant.h
 * @brief Public API for cross-feature invariants and compatibility checks.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * KEEL has 10+ interacting state machines. The real threat is not any single
 * feature but cross-feature interaction bugs at their intersection points.
 *
 * This header defines:
 *
 *   1. **Feature Interaction Compatibility Matrix** — compile-time declaration
 *      of which features may be simultaneously active, and which combinations
 *      are forbidden (would violate protocol, leak state, or corrupt data).
 *
 *   2. **Session Invariant Checker** — runtime assertions that hold at every
 *      transition boundary (after on_fe_msg, after on_be_msg, after pool
 *      borrow, after pool return). Debug builds call these on every message;
 *      release builds can sample or disable.
 *
 *   3. **Pin Conflict Matrix** — which pin reasons are mutually compatible
 *      and which combinations signal a state machine desynchronization.
 *
 * ARCHITECTURE:
 *   The 10 state machines are:
 *     SM1  Frontend protocol state     (keel_fe_action_type_t)
 *     SM2  Backend protocol state      (keel_be_action_type_t)
 *     SM3  Transaction tracking        (keel_tx_status_t + commit_in_doubt)
 *     SM4  LSN capture state           (capture_lsn_pending + txn_had_writes)
 *     SM5  Prepared statement replay   (stmt_replay_* fields + keel_ps_mode_t)
 *     SM6  Session context / replay    (state_profile + quarantine_pending)
 *     SM7  TLS / kTLS data path        (handshake state + splice eligibility)
 *     SM8  Pool cleanup / dirty state  (backend_conn_state_t + DISCARD flow)
 *     SM9  Migration / rebalance       (ring serialization + eligibility)
 *     SM10 Drain / shutdown lifecycle  (keel_engine_state_t)
 *
 *   The 6 highest-risk intersections are:
 *     R1: SM5 × SM3 × SM8   — PS replay + failover + transaction pooling
 *     R2: SM4 × SM2         — Deferred LSN capture + protocol framing
 *     R3: SM7 × splice      — TLS/kTLS fallback + splice eligibility
 *     R4: Hooks × SM6 × pins — Hook mutation + routing + session pinning
 *     R5: SM9 × SM6 × copy  — Migration + session state + pending residuals
 *     R6: SM10 × SM3        — Drain/shutdown + commit-in-doubt recovery
 *
 * USAGE:
 *   In debug builds, call keel_invariant_check_session() at:
 *     - After every on_fe_msg dispatch in engine_flow
 *     - After every on_be_msg dispatch in engine_flow
 *     - After pool_borrow completes
 *     - After pool_return completes
 *     - After hook chain fires
 *   Each call is O(1) — purely field comparisons, no I/O, no allocation.
 */

#ifndef KEEL_ENGINE_INVARIANT_H
#define KEEL_ENGINE_INVARIANT_H

#include "keel/protocol/protocol_flow.h"
#include "keel/engine/engine_flow.h"
#include "keel/engine/backend_pool.h"

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * §1 — Feature Interaction Compatibility Matrix
 *
 * Each cell encodes whether two features may be simultaneously active.
 * The matrix is symmetric; we store the upper triangle.
 *
 * Legend:
 *   COMPAT    — features safely compose; tested and documented
 *   GUARDED   — features may compose but require dynamic invariant checks
 *   MUTEX     — features are mutually exclusive; co-activation is a BUG
 *   DEGRADED  — feature A disables optimization B (e.g., TLS disables splice)
 * ============================================================================ */

typedef enum keel_compat_level {
    KEEL_COMPAT         = 0,   /**< Safe composition, tested */
    KEEL_COMPAT_GUARDED = 1,   /**< Requires runtime invariant guards */
    KEEL_COMPAT_DEGRADED = 2,  /**< A disables optimization in B */
    KEEL_COMPAT_MUTEX    = 3,  /**< Mutual exclusion — co-activation is a BUG */
} keel_compat_level_t;

/**
 * Feature identifiers for the interaction matrix.
 * Must be contiguous and < KEEL_FEATURE_COUNT.
 */
typedef enum keel_feature_id {
    KEEL_FEAT_TRANSACTION     = 0,   /**< SM3: Active transaction (BEGIN..COMMIT) */
    KEEL_FEAT_PS_REPLAY       = 1,   /**< SM5: Prepared statement replay in progress */
    KEEL_FEAT_COPY_MODE       = 2,   /**< COPY IN/OUT active */
    KEEL_FEAT_EXTENDED_PROTO  = 3,   /**< PG extended query protocol (Parse/Bind/Execute) */
    KEEL_FEAT_COMMIT_IN_DOUBT = 4,   /**< SM3: XID status unknown after backend death */
    KEEL_FEAT_TLS_HANDSHAKE   = 5,   /**< SM7: TLS handshake in progress */
    KEEL_FEAT_SPLICE          = 6,   /**< Zero-copy splice active */
    KEEL_FEAT_DRAINING        = 7,   /**< SM10: Engine is draining */
    KEEL_FEAT_MIGRATION       = 8,   /**< SM9: Session migration in progress */
    KEEL_FEAT_LSN_CAPTURE     = 9,   /**< SM4: Deferred LSN capture pending */
    KEEL_FEAT_HARD_PIN        = 10,  /**< SM6: Hard-pinned (LISTEN, TEMP, CURSOR) */
    KEEL_FEAT_QUARANTINE      = 11,  /**< SM6: Quarantined (awaiting state confirm) */
    KEEL_FEAT_COUNT           = 12,
} keel_feature_id_t;

/**
 * @brief Static compatibility matrix.
 *
 * keel_compat_matrix[a][b] == level of compatibility between feature a and b.
 * Indexed by keel_feature_id_t. Symmetric: matrix[a][b] == matrix[b][a].
 *
 * Defined in invariant.c.
 */
extern const keel_compat_level_t keel_compat_matrix[KEEL_FEAT_COUNT][KEEL_FEAT_COUNT];

/**
 * @brief Check whether two features may be simultaneously active.
 *
 * @return true if the combination is safe (COMPAT or DEGRADED), false if
 *         GUARDED (caller must verify invariants) or MUTEX (BUG).
 */
static inline bool keel_features_compatible(keel_feature_id_t a, keel_feature_id_t b)
{
    if (a >= KEEL_FEAT_COUNT || b >= KEEL_FEAT_COUNT) return false;
    keel_compat_level_t l = keel_compat_matrix[a][b];
    return l == KEEL_COMPAT || l == KEEL_COMPAT_DEGRADED;
}

/* ============================================================================
 * §2 — Pin Conflict Rules
 *
 * Some pin flag combinations should never co-exist. If they do, a state
 * machine has desynchronized.
 * ============================================================================ */

/**
 * Pin combinations that are INVALID (represent state machine bugs):
 *
 *   COPY + EXTENDED_PROTO  — PG cannot be in COPY and extended query at once
 *   COPY + TRANSACTION clearing — COPY implicitly holds TX pin until CopyDone
 *   STREAMING + anything else — replication session should be exclusive
 *   AUTH + query-path pins  — auth phase cannot have query pins
 */
#define KEEL_PIN_CONFLICT_COPY_EXTENDED \
    (KEEL_FPIN_COPY | KEEL_FPIN_EXTENDED_PROTO)

#define KEEL_PIN_CONFLICT_STREAMING_MASK \
    (KEEL_FPIN_TRANSACTION | KEEL_FPIN_EXTENDED_PROTO | KEEL_FPIN_COPY | \
     KEEL_FPIN_PREPARED_STMT | KEEL_FPIN_QUARANTINE | KEEL_FPIN_OSC)

#define KEEL_PIN_CONFLICT_AUTH_MASK \
    (KEEL_FPIN_TRANSACTION | KEEL_FPIN_EXTENDED_PROTO | KEEL_FPIN_COPY | \
     KEEL_FPIN_PREPARED_STMT | KEEL_FPIN_CURSOR | KEEL_FPIN_LISTEN | \
     KEEL_FPIN_QUARANTINE | KEEL_FPIN_OSC)

/**
 * @brief Check pin flags for internal consistency.
 *
 * @return true if pins are consistent, false if a conflict is detected.
 */
static inline bool keel_pins_consistent(keel_flow_pin_reason_t pins)
{
    /* COPY + extended protocol are mutually exclusive in PG */
    if ((pins & KEEL_PIN_CONFLICT_COPY_EXTENDED) == KEEL_PIN_CONFLICT_COPY_EXTENDED)
        return false;

    /* Streaming replication is exclusive — no query-path pins */
    if ((pins & KEEL_FPIN_STREAMING) && (pins & KEEL_PIN_CONFLICT_STREAMING_MASK))
        return false;

    /* Auth phase pins cannot coexist with query-path pins */
    if ((pins & KEEL_FPIN_AUTH) && (pins & KEEL_PIN_CONFLICT_AUTH_MASK))
        return false;

    return true;
}

/* ============================================================================
 * §3 — Session Invariant Checker
 *
 * Call at state transition boundaries. Each check is O(1).
 * Returns a bitmask of violated invariants (0 = all OK).
 * ============================================================================ */

/**
 * @brief Invariant violation flags.
 *
 * Each bit identifies a specific cross-feature invariant that was violated.
 * Multiple violations can be reported simultaneously.
 */
typedef enum keel_invariant_violation {
    KEEL_INV_OK                          = 0,

    /* R1: PS replay × transaction × pool */
    KEEL_INV_REPLAY_WITHOUT_BACKEND      = (1 << 0),  /**< stmt_replay_len > 0 but no backend conn */
    KEEL_INV_REPLAY_IN_FAILED_TX         = (1 << 1),  /**< Replaying PS while tx == FAILED */
    KEEL_INV_REPLAY_DURING_COPY          = (1 << 2),  /**< Replaying PS while COPY active */

    /* R2: LSN capture × protocol framing */
    KEEL_INV_LSN_WITHOUT_TX_WRITE        = (1 << 3),  /**< capture_lsn_pending but no write in tx */
    KEEL_INV_LSN_IN_FAILED_TX            = (1 << 4),  /**< LSN capture deferred in failed tx */

    /* R3: TLS × splice */
    KEEL_INV_SPLICE_DURING_TLS_HS        = (1 << 5),  /**< splice_eligible set during TLS handshake */

    /* R4: Hooks × routing × pinning */
    KEEL_INV_ROUTE_AFTER_PIN             = (1 << 6),  /**< Route changed after hard-pin committed */

    /* R5: Migration × residuals */
    KEEL_INV_MIGRATE_WITH_PENDING        = (1 << 7),  /**< Migration with pending_msg or copy state */
    KEEL_INV_MIGRATE_WITH_REPLAY         = (1 << 8),  /**< Migration during PS replay */
    KEEL_INV_MIGRATE_IN_TRANSACTION      = (1 << 9),  /**< Migration while tx != IDLE */

    /* R6: Drain × commit-in-doubt */
    KEEL_INV_DRAIN_KILLS_INDOUBT         = (1 << 10), /**< Drain force-closing a commit_in_doubt session */

    /* Pin consistency */
    KEEL_INV_PIN_CONFLICT                = (1 << 11), /**< Incompatible pin flags set */

    /* Phase consistency */
    KEEL_INV_QUERY_IN_AUTH_PHASE         = (1 << 12), /**< Query action while phase == HANDSHAKE_AUTH */
    KEEL_INV_READY_WITH_BACKEND_SYNC     = (1 << 13), /**< READY phase but backend in SYNCING state */

    /* Pool ↔ session consistency */
    KEEL_INV_IDLE_BACKEND_WITH_TX        = (1 << 14), /**< Backend IDLE but session in transaction */
    KEEL_INV_NO_BACKEND_WITH_TX_PIN      = (1 << 15), /**< TRANSACTION pin set but no backend */

    /* Commit-in-doubt consistency */
    KEEL_INV_DOUBT_WITHOUT_TRACKING      = (1 << 16), /**< commit_in_doubt but txn_tracking == false */
    KEEL_INV_DOUBT_WITHOUT_XID           = (1 << 17), /**< commit_in_doubt but indoubt_xid == 0 */

    /* Copy state consistency */
    KEEL_INV_COPY_STATE_WITHOUT_PIN      = (1 << 18), /**< copy_skip/copy_hdr_len > 0 but no COPY pin */
    KEEL_INV_COPY_PIN_WITH_ZERO_STATE    = (1 << 19), /**< COPY pin set but no copy state and no be_fwd */
} keel_invariant_violation_t;

/**
 * @brief Check all session-level invariants.
 *
 * Examines the session flow state, pins, tx status, and backend connection
 * (if any) for cross-feature interaction violations.
 *
 * @param sf        Session flow state
 * @param session   Session (for backend_conn access), may be NULL
 * @param be_conn   Backend connection (if currently borrowed), may be NULL
 * @return Bitmask of violated invariants (0 = all OK)
 */
uint32_t keel_invariant_check_session(
    const keel_session_flow_t *sf,
    const keel_session_t      *session,
    const backend_conn_t      *be_conn);

/**
 * @brief Check pool-level invariants.
 *
 * Examines pool counters and connection states for consistency.
 *
 * @param pool  Backend pool
 * @return Bitmask of violated invariants (0 = all OK)
 */
uint32_t keel_invariant_check_pool(const backend_pool_t *pool);

/* ============================================================================
 * §4 — Invariant Enforcement Macros
 *
 * In debug/ASan builds: assert on violation, log the failing invariant ID.
 * In release builds: no-op (or optional sampling via stats counter).
 * ============================================================================ */

#ifdef NDEBUG
#  define KEEL_CHECK_INVARIANTS(sf, session, be) ((void)0)
#  define KEEL_CHECK_POOL_INVARIANTS(pool)       ((void)0)
#else
#  include <stdio.h>
#  include <stdlib.h>

#  define KEEL_CHECK_INVARIANTS(sf, session, be) do {                         \
       uint32_t _viol = keel_invariant_check_session((sf), (session), (be));  \
       if (__builtin_expect(_viol != 0, 0)) {                                \
           fprintf(stderr, "INVARIANT VIOLATION 0x%08x at %s:%d\n",          \
                   _viol, __FILE__, __LINE__);                                \
           __builtin_trap();                                                  \
       }                                                                      \
   } while (0)

#  define KEEL_CHECK_POOL_INVARIANTS(pool) do {                               \
       uint32_t _viol = keel_invariant_check_pool((pool));                    \
       if (__builtin_expect(_viol != 0, 0)) {                                \
           fprintf(stderr, "POOL INVARIANT VIOLATION 0x%08x at %s:%d\n",     \
                   _viol, __FILE__, __LINE__);                                \
           __builtin_trap();                                                  \
       }                                                                      \
   } while (0)
#endif

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ENGINE_INVARIANT_H */
