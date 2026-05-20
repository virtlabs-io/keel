/**
 * @file state_machine.h
 * @brief Public API for engine state contracts, transitions, and derived predicates.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This header defines the contract-driven state model for KEEL.
 * It adds three new state domains (backend binding, replay state,
 * CID state), a unified session contract, backend contract,
 * quarantine tracking, transition functions, derived predicates,
 * and an event journal for debug builds.
 *
 * DESIGN PRINCIPLES:
 *   - Existing enums (session_state, session_phase, backend_conn_state,
 *     tls_state, engine_state, flow_pin_reason, tx_status) are NOT
 *     duplicated.  This header adds what was missing.
 *   - Transition functions validate preconditions, update state,
 *     record journal events, and run invariant checks.
 *   - Derived predicates compute eligibility from contract fields —
 *     they are the ONLY source of truth for eligibility decisions.
 *   - The event journal is active only in debug/hardening builds.
 *
 * See docs/STATE_MODEL.md for the formal specification.
 */

#ifndef KEEL_ENGINE_STATE_MACHINE_H
#define KEEL_ENGINE_STATE_MACHINE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "keel/protocol/protocol_flow.h"
#include "keel/engine/engine.h"
#include "keel/engine/engine_flow.h"
#include "keel/engine/backend_pool.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * §1 — New State Domain Enums
 *
 * Three domains that were previously tracked via scattered booleans.
 * ============================================================================ */

/**
 * @brief Backend binding — session→backend ownership state.
 *
 * Separate from session phase. A session can be READY while UNBOUND;
 * it can be in QUERY while SHARED; it can be CLOSING with HARD_PINNED.
 */
typedef enum keel_backend_binding {
    KEEL_BIND_UNBOUND       = 0,  /**< No backend connection */
    KEEL_BIND_BORROW_PENDING,     /**< Queued in pool waiting list */
    KEEL_BIND_SHARED,             /**< Borrowed, returned on query complete */
    KEEL_BIND_PINNED_TXN,         /**< Pinned due to active transaction */
    KEEL_BIND_PINNED_STATE,       /**< Pinned due to session state (SET vars) */
    KEEL_BIND_PINNED_PS,          /**< Pinned due to PS mode == PINNING */
    KEEL_BIND_HARD_PINNED,        /**< Exclusive ownership (LISTEN, TEMP, CURSOR) */
    KEEL_BIND_CID_CHECK,          /**< Using check connection for txid_status() */
    KEEL_BIND_COUNT,
} keel_backend_binding_t;

/**
 * @brief Replay state — prepared-statement replay lifecycle.
 *
 * When a session with named prepared statements borrows a backend with
 * a stmt_set_hash mismatch, the engine must replay Parse messages before
 * forwarding the client's original Bind/Execute. This enum tracks that
 * multi-step process.
 */
typedef enum keel_replay_state {
    KEEL_REPLAY_NONE = 0,        /**< No replay needed */
    KEEL_REPLAY_CLEANUP_PENDING, /**< Backend needs plugin cleanup first */
    KEEL_REPLAY_CLEANUP_SENT,    /**< Cleanup sent, awaiting validated completion */
    KEEL_REPLAY_SENDING,         /**< Parse messages being written to backend */
    KEEL_REPLAY_WAITING,         /**< Waiting for ParseComplete responses */
    KEEL_REPLAY_RFQ_PENDING,     /**< All ParseComplete received, draining Sync RFQ */
    KEEL_REPLAY_COMPLETE,        /**< Replay finished, ready to forward original msg */
    KEEL_REPLAY_COUNT,
} keel_replay_state_t;

/**
 * @brief Commit-in-doubt state — replication uncertainty lifecycle.
 *
 * When transaction tracking is enabled, KEEL intercepts COMMIT to capture
 * the XID. If the backend dies mid-COMMIT, KEEL must determine the outcome
 * by querying txid_status() on a different connection. This enum tracks
 * the entire lifecycle.
 */
typedef enum keel_cid_state {
    KEEL_CID_NONE = 0,           /**< No commit uncertainty */
    KEEL_CID_TRACKING,           /**< txn_tracking active, XID will be captured */
    KEEL_CID_XID_CAPTURED,       /**< txid_current() captured, COMMIT forwarded */
    KEEL_CID_COMMIT_SENT,        /**< COMMIT in flight, waiting for response */
    KEEL_CID_BACKEND_LOST,       /**< Backend died while COMMIT in flight */
    KEEL_CID_CHECK_BORROWING,    /**< Borrowing clean connection for txid_status() */
    KEEL_CID_CHECK_SENT,         /**< txid_status() query sent to new primary */
    KEEL_CID_RESOLVED_COMMITTED, /**< XID confirmed committed */
    KEEL_CID_RESOLVED_ABORTED,   /**< XID confirmed aborted */
    KEEL_CID_RESOLVED_UNKNOWN,   /**< XID status could not be determined */
    KEEL_CID_COUNT,
} keel_cid_state_t;

/**
 * @brief Quarantine reason — why a backend was quarantined.
 *
 * Quarantined backends get a cleanup attempt then close. Never reused.
 */
typedef enum keel_quarantine_reason {
    KEEL_QUARANTINE_NONE = 0,        /**< Not quarantined */
    KEEL_QUARANTINE_DIRTY_STATE,     /**< Unknown SET/session state */
    KEEL_QUARANTINE_REPLAY_MISMATCH, /**< PS replay hash mismatch */
    KEEL_QUARANTINE_PROTOCOL_DESYNC, /**< Unexpected message sequence */
    KEEL_QUARANTINE_TLS_MISMATCH,    /**< TLS state inconsistency */
    KEEL_QUARANTINE_FAILED_CLEANUP,  /**< Cleanup failed or timed out */
    KEEL_QUARANTINE_FAILED_SYNC,     /**< State sync failed */
    KEEL_QUARANTINE_COUNT,
} keel_quarantine_reason_t;

/* ============================================================================
 * §2 — Session Contract
 *
 * Unified view of a session's state across all domains.
 * Synced from scattered fields before decision points.
 * ============================================================================ */

typedef struct keel_session_contract {
    keel_session_phase_t       phase;           /**< Engine-level phase */
    keel_backend_binding_t     binding;         /**< Backend ownership */
    keel_flow_pin_reason_t     pins;            /**< Active pin reasons (bitmask) */
    keel_tx_status_t           tx;              /**< Transaction status */
    keel_replay_state_t        replay;          /**< PS replay lifecycle */
    keel_cid_state_t           cid;             /**< Commit-in-doubt lifecycle */
    keel_engine_state_t        engine_state;    /**< Engine lifecycle snapshot */
    uint32_t                   invariant_flags; /**< Last invariant check result */
} keel_session_contract_t;

/* ============================================================================
 * §3 — Backend Contract
 *
 * Unified view of a backend connection's state.
 * ============================================================================ */

typedef struct keel_backend_contract {
    backend_conn_state_t       conn_state;      /**< Connection lifecycle */
    keel_quarantine_reason_t   quarantine;       /**< NONE or reason for quarantine */
    bool                       has_owner;        /**< pinned_session != NULL */
    bool                       has_stmts;        /**< stmt_set_hash != 0 */
    bool                       needs_sync;       /**< State sync required before use */
    bool                       in_transaction;   /**< Inside BEGIN..COMMIT */
} keel_backend_contract_t;

/* ============================================================================
 * §4 — Event Journal (debug/hardening builds only)
 *
 * Rolling ring buffer of state transitions for post-mortem analysis.
 * Each session and backend connection gets a journal in debug builds.
 * ============================================================================ */

/** State domain identifier for journal entries. */
typedef enum keel_state_domain {
    KEEL_DOMAIN_PHASE = 0,       /**< keel_session_phase_t */
    KEEL_DOMAIN_BINDING,         /**< keel_backend_binding_t */
    KEEL_DOMAIN_TX,              /**< keel_tx_status_t */
    KEEL_DOMAIN_REPLAY,          /**< keel_replay_state_t */
    KEEL_DOMAIN_CID,             /**< keel_cid_state_t */
    KEEL_DOMAIN_BACKEND_CONN,    /**< backend_conn_state_t */
    KEEL_DOMAIN_QUARANTINE,      /**< keel_quarantine_reason_t */
    KEEL_DOMAIN_PINS,            /**< Pin bitmask change */
    KEEL_DOMAIN_COUNT,
} keel_state_domain_t;

/** Single state transition event. */
typedef struct keel_state_event {
    uint64_t    ts_ns;           /**< Monotonic timestamp (clock_gettime) */
    uint32_t    entity_id;       /**< Session ID or backend conn index */
    uint8_t     domain;          /**< keel_state_domain_t */
    uint8_t     old_state;       /**< Previous state value */
    uint8_t     new_state;       /**< New state value */
    uint8_t     trigger;         /**< What caused the transition (event-specific) */
} keel_state_event_t;

/** Journal capacity — power of 2 for fast modulo. */
#define KEEL_STATE_JOURNAL_CAPACITY 64
#define KEEL_STATE_JOURNAL_MASK     (KEEL_STATE_JOURNAL_CAPACITY - 1)

/** Per-entity rolling journal. */
typedef struct keel_state_journal {
    keel_state_event_t events[KEEL_STATE_JOURNAL_CAPACITY];
    uint32_t           head;      /**< Next write index (wraps) */
    uint32_t           count;     /**< Total events recorded (for underflow) */
} keel_state_journal_t;

/* ============================================================================
 * §5 — String Conversion (all domains)
 * ============================================================================ */

/**
 * @brief Return a human-readable name for a backend binding state.
 *
 * @param b  Binding enum value.
 * @return Static string (e.g. "UNBOUND", "SHARED", "PINNED_TXN"), never NULL.
 */
const char* keel_backend_binding_name(keel_backend_binding_t b);

/**
 * @brief Return a human-readable name for a replay state.
 *
 * @param s  Replay state enum value.
 * @return Static string, never NULL.
 */
const char* keel_replay_state_name(keel_replay_state_t s);

/**
 * @brief Return a human-readable name for a CID (commit-ID) tracking state.
 *
 * @param s  CID state enum value.
 * @return Static string, never NULL.
 */
const char* keel_cid_state_name(keel_cid_state_t s);

/**
 * @brief Return a human-readable name for a quarantine reason.
 *
 * @param r  Quarantine reason enum value.
 * @return Static string, never NULL.
 */
const char* keel_quarantine_reason_name(keel_quarantine_reason_t r);

/**
 * @brief Return a human-readable name for a state domain.
 *
 * @param d  Domain enum value (PHASE, TX, BINDING, etc.).
 * @return Static string, never NULL.
 */
const char* keel_state_domain_name(keel_state_domain_t d);

/* ============================================================================
 * §6 — Journal API
 * ============================================================================ */

/** Initialize journal (zeroed). */
void keel_journal_init(keel_state_journal_t* j);

/** Record a state transition. In release builds, this is a no-op. */
#ifndef NDEBUG
void keel_journal_record(keel_state_journal_t* j,
                         uint32_t entity_id,
                         keel_state_domain_t domain,
                         uint8_t old_state,
                         uint8_t new_state,
                         uint8_t trigger);

/** Dump the journal to stderr for debugging. */
void keel_journal_dump(const keel_state_journal_t* j, const char* label);
#else
static inline void keel_journal_record(keel_state_journal_t* j,
                                       uint32_t entity_id,
                                       keel_state_domain_t domain,
                                       uint8_t old_state,
                                       uint8_t new_state,
                                       uint8_t trigger)
{
    (void)j; (void)entity_id; (void)domain;
    (void)old_state; (void)new_state; (void)trigger;
}
static inline void keel_journal_dump(const keel_state_journal_t* j, const char* label)
{
    (void)j; (void)label;
}
#endif

/* ============================================================================
 * §7 — Contract Sync
 *
 * Build a contract snapshot from the scattered runtime fields.
 * Call before decision points that need multiple state checks.
 * ============================================================================ */

/**
 * @brief Build a session contract from current runtime state.
 *
 * Reads the session, session_flow, and backend_conn to produce a
 * coherent snapshot. O(1) — just field reads and simple derivation.
 */
keel_session_contract_t keel_session_contract_sync(
    const keel_session_flow_t* sf,
    const keel_session_t* session,
    const keel_engine_state_t* engine_state);

/**
 * @brief Build a backend contract from a backend connection.
 */
keel_backend_contract_t keel_backend_contract_sync(
    const backend_conn_t* conn);

/* ============================================================================
 * §8 — Transition Functions
 *
 * Central state mutation with:
 *   - Precondition validation
 *   - Dependent field update
 *   - Journal event recording
 *   - Invariant check (debug builds)
 *
 * Returns 0 on success, -1 if precondition violated.
 * On violation: logs the failure and does NOT mutate state.
 * ============================================================================ */

/**
 * @brief Transition session phase.
 *
 * Validates that new_phase is a legal successor of the current phase.
 */
int keel_session_transition_phase(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_session_phase_t new_phase,
    keel_state_journal_t* journal);

/**
 * @brief Bind a backend to a session.
 *
 * Preconditions: binding must be UNBOUND or BORROW_PENDING.
 * Sets session->backend_conn, updates backend pinned_session,
 * transitions backend_conn_state from IDLE to ACTIVE.
 */
int keel_session_transition_bind(
    keel_session_flow_t* sf,
    keel_session_t* session,
    backend_conn_t* conn,
    keel_backend_binding_t bind_type,
    keel_state_journal_t* journal);

/**
 * @brief Unbind (return) a backend from a session.
 *
 * Preconditions: binding must not be UNBOUND.
 * Clears session->backend_conn, clears backend pinned_session,
 * transitions backend to IDLE (or CLEANING if dirty).
 */
int keel_session_transition_unbind(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_state_journal_t* journal);

/**
 * @brief Begin a transaction.
 *
 * Preconditions: binding must be SHARED or higher, tx must be IDLE.
 * Sets in_transaction on both session and backend, upgrades binding
 * to PINNED_TXN, sets KEEL_FPIN_TRANSACTION.
 */
int keel_session_transition_begin_txn(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_state_journal_t* journal);

/**
 * @brief End a transaction (commit or rollback).
 *
 * Preconditions: tx must be ACTIVE or FAILED.
 * Clears in_transaction, removes KEEL_FPIN_TRANSACTION,
 * downgrades binding to SHARED (unless other pins exist).
 */
int keel_session_transition_end_txn(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_tx_status_t new_tx_status,
    keel_state_journal_t* journal);

/**
 * @brief Transition replay state.
 *
 * Validates legal transitions between replay states.
 */
int keel_session_transition_replay(
    keel_session_flow_t* sf,
    keel_replay_state_t new_state,
    keel_state_journal_t* journal);

/**
 * @brief Transition CID state.
 *
 * Validates legal CID transitions. For BACKEND_LOST, sets commit_in_doubt.
 * For RESOLVED_*, clears commit_in_doubt.
 */
int keel_session_transition_cid(
    keel_session_flow_t* sf,
    keel_cid_state_t new_state,
    keel_state_journal_t* journal);

/**
 * @brief Set hard pin on session.
 *
 * Preconditions: must have a backend (binding >= SHARED).
 * Upgrades binding to HARD_PINNED, sets hard_pinned on session and backend.
 */
int keel_session_transition_hard_pin(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_state_journal_t* journal);

/**
 * @brief Quarantine a backend connection.
 *
 * Sets quarantine reason, pins the backend so it won't be reused,
 * and records the event for metrics.
 */
int keel_backend_transition_quarantine(
    backend_conn_t* conn,
    keel_quarantine_reason_t reason,
    keel_state_journal_t* journal);

/* ============================================================================
 * §9 — Derived Predicates
 *
 * These are the ONLY source of truth for eligibility decisions.
 * They derive from the contract / runtime state — never stored independently.
 * ============================================================================ */

/**
 * @brief Can this session be force-closed during drain?
 *
 * Returns false if session has a commit-in-doubt that must be resolved
 * before closing. Drain timeout must wait for CID resolution.
 */
bool keel_session_can_force_close(const keel_session_contract_t* c);

/**
 * @brief Can this session use zero-copy splice for the current message?
 *
 * Derives from: TLS state (must be kTLS or plaintext), no pending replay,
 * not in COPY parse mode, platform supports splice.
 */
bool keel_session_can_splice(const keel_session_flow_t* sf,
                             const keel_session_t* session);

/**
 * @brief Does this session need prepared-statement replay on the given backend?
 *
 * Compares session's stmt_replay_hash with backend's stmt_set_hash.
 */
bool keel_session_needs_replay(const keel_session_flow_t* sf,
                               const backend_conn_t* conn);

/**
 * @brief Can this backend connection be returned to the general pool?
 *
 * Checks: not in transaction, not quarantined, not hard-pinned,
 * connection state is appropriate.
 */
bool keel_backend_can_return_to_pool(const backend_conn_t* conn);

/**
 * @brief Derive the backend binding from current session/flow state.
 *
 * Used by contract_sync to compute binding from scattered booleans.
 * Also useful for assertions: actual binding should match derived.
 */
keel_backend_binding_t keel_derive_binding(
    const keel_session_flow_t* sf,
    const keel_session_t* session);

/**
 * @brief Derive the replay state from current session_flow fields.
 *
 * Used by contract_sync. Maps scattered stmt_replay_* fields to the
 * formal replay state enum.
 */
keel_replay_state_t keel_derive_replay_state(const keel_session_flow_t* sf);

/**
 * @brief Derive the CID state from current session_flow fields.
 *
 * Used by contract_sync. Maps scattered commit_in_flight/doubt/xid fields
 * to the formal CID state enum.
 */
keel_cid_state_t keel_derive_cid_state(const keel_session_flow_t* sf);

/* ============================================================================
 * §10 — Contract-Level Invariant Validation
 *
 * Extends the existing invariant checks (invariant.h) with contract-level
 * cross-domain validation. Returns a bitmask of violations (0 = OK).
 * ============================================================================ */

/** Contract-level invariant violations (extend keel_invariant_violation_t). */
typedef enum keel_contract_violation {
    KEEL_CONTRACT_OK = 0,

    /* Binding × Phase */
    KEEL_CONTRACT_QUERY_UNBOUND           = (1 << 0),  /**< In QUERY/SYNC phase but UNBOUND */
    KEEL_CONTRACT_READY_NON_IDLE_BINDING  = (1 << 1),  /**< In READY phase with SHARED binding */

    /* Binding × TX */
    KEEL_CONTRACT_TXN_PIN_NO_TX           = (1 << 2),  /**< PINNED_TXN but tx == IDLE */
    KEEL_CONTRACT_TX_ACTIVE_UNBOUND       = (1 << 3),  /**< tx != IDLE but UNBOUND */

    /* Replay × Binding */
    KEEL_CONTRACT_REPLAY_UNBOUND          = (1 << 4),  /**< Replay active but UNBOUND */

    /* CID × Binding */
    KEEL_CONTRACT_CID_CHECK_NO_CONN       = (1 << 5),  /**< CID_CHECK binding but no xid_check_conn */
    KEEL_CONTRACT_CID_DOUBT_NO_XID        = (1 << 6),  /**< CID in doubt but no indoubt_xid */

    /* CID × Engine */
    KEEL_CONTRACT_DRAIN_KILLS_CID         = (1 << 7),  /**< Engine draining + CID in doubt */

    /* Backend contract */
    KEEL_CONTRACT_IDLE_WITH_OWNER         = (1 << 8),  /**< Backend IDLE but has owner */
    KEEL_CONTRACT_ACTIVE_NO_OWNER         = (1 << 9),  /**< Backend ACTIVE but no owner */
    KEEL_CONTRACT_QUARANTINE_IN_POOL      = (1 << 10),  /**< Quarantined but in IDLE state */
} keel_contract_violation_t;

/**
 * @brief Validate session contract invariants.
 * O(1) — purely field comparisons.
 */
uint32_t keel_contract_check_session(const keel_session_contract_t* c,
                                     const keel_session_flow_t* sf);

/**
 * @brief Validate backend contract invariants.
 */
uint32_t keel_contract_check_backend(const keel_backend_contract_t* c);

/* ============================================================================
 * §11 — Contract Invariant Enforcement Macros
 * ============================================================================ */

#ifdef NDEBUG
#  define KEEL_CHECK_CONTRACT(c, sf)       ((void)0)
#  define KEEL_CHECK_BACKEND_CONTRACT(c)   ((void)0)
#else
#  include <stdio.h>
#  include <stdlib.h>

#  define KEEL_CHECK_CONTRACT(c, sf) do {                                      \
       uint32_t _cv = keel_contract_check_session((c), (sf));                  \
       if (__builtin_expect(_cv != 0, 0)) {                                    \
           fprintf(stderr, "CONTRACT VIOLATION 0x%08x at %s:%d"               \
                   " phase=%s bind=%s tx=%d replay=%s cid=%s\n",              \
                   _cv, __FILE__, __LINE__,                                    \
                   keel_session_phase_name((c)->phase),                        \
                   keel_backend_binding_name((c)->binding),                    \
                   (c)->tx,                                                    \
                   keel_replay_state_name((c)->replay),                        \
                   keel_cid_state_name((c)->cid));                             \
           __builtin_trap();                                                   \
       }                                                                       \
   } while (0)

#  define KEEL_CHECK_BACKEND_CONTRACT(c) do {                                  \
       uint32_t _cv = keel_contract_check_backend((c));                        \
       if (__builtin_expect(_cv != 0, 0)) {                                    \
           fprintf(stderr, "BACKEND CONTRACT VIOLATION 0x%08x at %s:%d\n",    \
                   _cv, __FILE__, __LINE__);                                   \
           __builtin_trap();                                                   \
       }                                                                       \
   } while (0)
#endif

/* We need the phase name for the macro above */
static inline const char* keel_session_phase_name(keel_session_phase_t p)
{
    switch (p) {
    case KEEL_PHASE_HANDSHAKE_AUTH: return "HANDSHAKE_AUTH";
    case KEEL_PHASE_READY:          return "READY";
    case KEEL_PHASE_QUERY:          return "QUERY";
    case KEEL_PHASE_BACKEND_SYNC:   return "BACKEND_SYNC";
    case KEEL_PHASE_BACKEND_CLEANING: return "BACKEND_CLEANING";
    case KEEL_PHASE_CLOSING:        return "CLOSING";
    default:                        return "UNKNOWN_PHASE";
    }
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ENGINE_STATE_MACHINE_H */
