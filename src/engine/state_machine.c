/**
 * @file state_machine.c
 * @brief State-contract derivation, transition validation, and predicate logic.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Implements: contract sync, transition functions, derived predicates,
 * contract-level invariant checks, event journal, and string conversions.
 */

#include "keel/engine/state_machine.h"
#include "keel/engine/engine_flow.h"
#include "keel/session/session.h"
#include "keel/util/util.h"

#include <string.h>
#include <stdio.h>
#include <time.h>

/* ============================================================================
 * §1 — String Conversion Tables
 * ============================================================================ */

static const char* const s_binding_names[] = {
    [KEEL_BIND_UNBOUND]       = "UNBOUND",
    [KEEL_BIND_BORROW_PENDING]= "BORROW_PENDING",
    [KEEL_BIND_SHARED]        = "SHARED",
    [KEEL_BIND_PINNED_TXN]   = "PINNED_TXN",
    [KEEL_BIND_PINNED_STATE]  = "PINNED_STATE",
    [KEEL_BIND_PINNED_PS]    = "PINNED_PS",
    [KEEL_BIND_HARD_PINNED]   = "HARD_PINNED",
    [KEEL_BIND_CID_CHECK]    = "CID_CHECK",
};

static const char* const s_replay_names[] = {
    [KEEL_REPLAY_NONE]            = "NONE",
    [KEEL_REPLAY_CLEANUP_PENDING] = "CLEANUP_PENDING",
    [KEEL_REPLAY_CLEANUP_SENT]    = "CLEANUP_SENT",
    [KEEL_REPLAY_SENDING]         = "SENDING",
    [KEEL_REPLAY_WAITING]         = "WAITING",
    [KEEL_REPLAY_RFQ_PENDING]     = "RFQ_PENDING",
    [KEEL_REPLAY_COMPLETE]        = "COMPLETE",
};

static const char* const s_cid_names[] = {
    [KEEL_CID_NONE]               = "NONE",
    [KEEL_CID_TRACKING]           = "TRACKING",
    [KEEL_CID_XID_CAPTURED]       = "XID_CAPTURED",
    [KEEL_CID_COMMIT_SENT]        = "COMMIT_SENT",
    [KEEL_CID_BACKEND_LOST]       = "BACKEND_LOST",
    [KEEL_CID_CHECK_BORROWING]    = "CHECK_BORROWING",
    [KEEL_CID_CHECK_SENT]         = "CHECK_SENT",
    [KEEL_CID_RESOLVED_COMMITTED] = "RESOLVED_COMMITTED",
    [KEEL_CID_RESOLVED_ABORTED]   = "RESOLVED_ABORTED",
    [KEEL_CID_RESOLVED_UNKNOWN]   = "RESOLVED_UNKNOWN",
};

static const char* const s_quarantine_names[] = {
    [KEEL_QUARANTINE_NONE]            = "NONE",
    [KEEL_QUARANTINE_DIRTY_STATE]     = "DIRTY_STATE",
    [KEEL_QUARANTINE_REPLAY_MISMATCH] = "REPLAY_MISMATCH",
    [KEEL_QUARANTINE_PROTOCOL_DESYNC] = "PROTOCOL_DESYNC",
    [KEEL_QUARANTINE_TLS_MISMATCH]    = "TLS_MISMATCH",
    [KEEL_QUARANTINE_FAILED_CLEANUP]  = "FAILED_CLEANUP",
    [KEEL_QUARANTINE_FAILED_SYNC]     = "FAILED_SYNC",
};

static const char* const s_domain_names[] = {
    [KEEL_DOMAIN_PHASE]        = "PHASE",
    [KEEL_DOMAIN_BINDING]      = "BINDING",
    [KEEL_DOMAIN_TX]           = "TX",
    [KEEL_DOMAIN_REPLAY]       = "REPLAY",
    [KEEL_DOMAIN_CID]          = "CID",
    [KEEL_DOMAIN_BACKEND_CONN] = "BACKEND_CONN",
    [KEEL_DOMAIN_QUARANTINE]   = "QUARANTINE",
    [KEEL_DOMAIN_PINS]         = "PINS",
};

/**
 * @brief Convert a backend-binding enum to a stable debug string.
 *
 * @param b Binding enum value.
 * @return Human-readable binding name, or `"UNKNOWN"` for invalid values.
 */
const char* keel_backend_binding_name(keel_backend_binding_t b)
{
    if (b >= KEEL_BIND_COUNT) return "UNKNOWN";
    return s_binding_names[b];
}

/**
 * @brief Convert a replay-state enum to a stable debug string.
 *
 * @param s Replay-state enum value.
 * @return Human-readable replay-state name, or `"UNKNOWN"` for invalid values.
 */
const char* keel_replay_state_name(keel_replay_state_t s)
{
    if (s >= KEEL_REPLAY_COUNT) return "UNKNOWN";
    return s_replay_names[s];
}

/**
 * @brief Convert a commit-in-doubt state enum to a stable debug string.
 *
 * @param s Commit-in-doubt state enum value.
 * @return Human-readable CID state name, or `"UNKNOWN"` for invalid values.
 */
const char* keel_cid_state_name(keel_cid_state_t s)
{
    if (s >= KEEL_CID_COUNT) return "UNKNOWN";
    return s_cid_names[s];
}

/**
 * @brief Convert a quarantine reason to a stable debug string.
 *
 * @param r Quarantine reason enum value.
 * @return Human-readable quarantine reason, or `"UNKNOWN"` for invalid values.
 */
const char* keel_quarantine_reason_name(keel_quarantine_reason_t r)
{
    if (r >= KEEL_QUARANTINE_COUNT) return "UNKNOWN";
    return s_quarantine_names[r];
}

/**
 * @brief Convert a state-domain enum to a stable debug string.
 *
 * @param d State-domain enum value.
 * @return Human-readable domain name, or `"UNKNOWN"` for invalid values.
 */
const char* keel_state_domain_name(keel_state_domain_t d)
{
    if (d >= KEEL_DOMAIN_COUNT) return "UNKNOWN";
    return s_domain_names[d];
}

/* ============================================================================
 * §2 — Timestamp Helper
 * ============================================================================ */

/**
 * @brief Read a monotonic nanosecond timestamp for journal events.
 *
 * @return Current monotonic time in nanoseconds.
 */
static uint64_t now_ns(void) { return (uint64_t)keel_time_now(); }

/* ============================================================================
 * §3 — Event Journal
 * ============================================================================ */

/**
 * @brief Reset a state journal to its empty initial state.
 *
 * @param j Journal buffer to initialize.
 * @return
 */
void keel_journal_init(keel_state_journal_t* j)
{
    memset(j, 0, sizeof(*j));
}

#ifndef NDEBUG
/**
 * @brief Append one transition event to a rolling journal buffer.
 *
 * The journal overwrites its oldest entries once capacity is reached. This is
 * only compiled in debug builds, where preserving recent transition history is
 * more valuable than maintaining an unbounded log.
 *
 * @param j Journal buffer.
 * @param entity_id Session or backend identifier associated with the event.
 * @param domain State domain that changed.
 * @param old_state Previous state value.
 * @param new_state New state value.
 * @param trigger Caller-defined trigger value explaining the transition.
 * @return
 */
void keel_journal_record(keel_state_journal_t* j,
                         uint32_t entity_id,
                         keel_state_domain_t domain,
                         uint8_t old_state,
                         uint8_t new_state,
                         uint8_t trigger)
{
    uint32_t idx = j->head & KEEL_STATE_JOURNAL_MASK;
    keel_state_event_t* e = &j->events[idx];
    e->ts_ns = now_ns();
    e->entity_id = entity_id;
    e->domain = (uint8_t)domain;
    e->old_state = old_state;
    e->new_state = new_state;
    e->trigger = trigger;
    j->head++;
    j->count++;
}

/**
 * @brief Print the contents of a state journal to `stderr`.
 *
 * @param j Journal buffer to dump.
 * @param label Human-readable label included in the output.
 * @return
 */
void keel_journal_dump(const keel_state_journal_t* j, const char* label)
{
    uint32_t n = j->count < KEEL_STATE_JOURNAL_CAPACITY
               ? j->count : KEEL_STATE_JOURNAL_CAPACITY;
    uint32_t start = (j->head - n) & KEEL_STATE_JOURNAL_MASK;

    fprintf(stderr, "=== Journal Dump: %s (%u events) ===\n", label, n);
    for (uint32_t i = 0; i < n; i++) {
        uint32_t idx = (start + i) & KEEL_STATE_JOURNAL_MASK;
        const keel_state_event_t* e = &j->events[idx];
        fprintf(stderr, "  [%3u] ts=%lu id=%u domain=%-13s %u -> %u trigger=%u\n",
                i, (unsigned long)e->ts_ns, e->entity_id,
                keel_state_domain_name((keel_state_domain_t)e->domain),
                e->old_state, e->new_state, e->trigger);
    }
    fprintf(stderr, "=== End Journal ===\n");
}
#endif

/* ============================================================================
 * §4 — Derived State Functions
 * ============================================================================ */

/**
 * @brief Derive the effective backend binding mode for a session.
 *
 * The binding is not stored directly as one field; it is inferred from the
 * presence of a backend, pool-wait state, pin flags, transaction state, and
 * commit-in-doubt check connection.
 *
 * @param sf Session flow state.
 * @param session Session runtime state.
 * @return Derived binding classification.
 */
keel_backend_binding_t keel_derive_binding(
    const keel_session_flow_t* sf,
    const keel_session_t* session)
{
    if (!session) return KEEL_BIND_UNBOUND;

    /* CID check connection is a special case */
    if (sf->xid_check_conn != NULL)
        return KEEL_BIND_CID_CHECK;

    if (session->backend_conn == NULL) {
        if (sf->queued_for_pool)
            return KEEL_BIND_BORROW_PENDING;
        return KEEL_BIND_UNBOUND;
    }

    /* Has a backend — check pin level (highest priority first) */
    if (session->hard_pinned)
        return KEEL_BIND_HARD_PINNED;

    /* PS pinning mode (KEEL_PS_MODE_PINNING hard-pins on first PREPARE) */
    if (sf->ps_mode == KEEL_PS_MODE_PINNING &&
        (sf->pins & KEEL_FPIN_PREPARED_STMT))
        return KEEL_BIND_PINNED_PS;

    if (session->in_transaction)
        return KEEL_BIND_PINNED_TXN;

    /* State-pinned: has state profile that would be lost on return */
    if (sf->pins & (KEEL_FPIN_TEMP_TABLE | KEEL_FPIN_LISTEN |
                    KEEL_FPIN_CURSOR | KEEL_FPIN_ADVISORY_LOCK |
                    KEEL_FPIN_SET_ROLE))
        return KEEL_BIND_PINNED_STATE;

    return KEEL_BIND_SHARED;
}

/**
 * @brief Derive the prepared-statement replay lifecycle state.
 *
 * Replay state is inferred from buffered replay payload, remaining expected
 * responses, setup-cleanup sequencing, and pending terminal drainage.
 *
 * @param sf Session flow state.
 * @return Derived replay state.
 */
keel_replay_state_t keel_derive_replay_state(const keel_session_flow_t* sf)
{
    if (sf->stmt_replay_len == 0)
        return KEEL_REPLAY_NONE;

    if (sf->stmt_replay_needs_cleanup) {
        /* Waiting for plugin cleanup to complete before replay */
        if (sf->stmt_replay_count == 0 && sf->stmt_replay_rfq_pending)
            return KEEL_REPLAY_CLEANUP_SENT;
        return KEEL_REPLAY_CLEANUP_PENDING;
    }

    if (sf->stmt_replay_rfq_pending)
        return KEEL_REPLAY_RFQ_PENDING;

    if (sf->stmt_replay_count > 0)
        return KEEL_REPLAY_WAITING;

    /* replay_len > 0 but count == 0, not needs_discard, not rfq_pending:
     * Parse messages buffered but not yet sent or all responses received */
    return KEEL_REPLAY_SENDING;
}

/**
 * @brief Derive the commit-in-doubt state from flow-level tracking fields.
 *
 * @param sf Session flow state.
 * @return Derived commit-in-doubt state.
 */
keel_cid_state_t keel_derive_cid_state(const keel_session_flow_t* sf)
{
    if (!sf->txn_tracking)
        return KEEL_CID_NONE;

    /* Resolved states (check_result set) */
    if (sf->indoubt_check_result == 1)
        return KEEL_CID_RESOLVED_COMMITTED;
    if (sf->indoubt_check_result == 2)
        return KEEL_CID_RESOLVED_ABORTED;

    /* In-doubt check in progress */
    if (sf->commit_in_doubt) {
        if (sf->xid_check_conn != NULL)
            return KEEL_CID_CHECK_SENT;
        if (sf->indoubt_xid != 0)
            return KEEL_CID_BACKEND_LOST;
        return KEEL_CID_RESOLVED_UNKNOWN;
    }

    /* COMMIT in flight */
    if (sf->commit_in_flight) {
        if (sf->pending_commit_xid != 0)
            return KEEL_CID_XID_CAPTURED;
        return KEEL_CID_COMMIT_SENT;
    }

    /* Tracking active but no commit in flight */
    if (sf->tx == KEEL_TX_ACTIVE)
        return KEEL_CID_TRACKING;

    return KEEL_CID_NONE;
}

/* ============================================================================
 * §5 — Contract Sync
 * ============================================================================ */

/**
 * @brief Build a coherent session contract snapshot from scattered runtime state.
 *
 * The returned contract is a read-only summary used by invariant checkers and
 * policy predicates so that multi-field decisions are made from one consistent
 * derived view.
 *
 * @param sf Session flow state.
 * @param session Session runtime state.
 * @param engine_state Optional engine lifecycle state snapshot.
 * @return Session contract snapshot.
 */
keel_session_contract_t keel_session_contract_sync(
    const keel_session_flow_t* sf,
    const keel_session_t* session,
    const keel_engine_state_t* engine_state)
{
    keel_session_contract_t c = {
        .phase       = sf->phase,
        .binding     = keel_derive_binding(sf, session),
        .pins        = sf->pins,
        .tx          = sf->tx,
        .replay      = keel_derive_replay_state(sf),
        .cid         = keel_derive_cid_state(sf),
        .engine_state = engine_state ? *engine_state : KEEL_ENGINE_STATE_CREATED,
        .invariant_flags = 0,
    };
    return c;
}

/**
 * @brief Build a backend contract snapshot from one backend connection.
 *
 * @param conn Backend connection, or `NULL`.
 * @return Backend contract snapshot summarizing borrowability and ownership.
 */
keel_backend_contract_t keel_backend_contract_sync(const backend_conn_t* conn)
{
    keel_quarantine_reason_t q = KEEL_QUARANTINE_NONE;
    if (!conn) {
        return (keel_backend_contract_t){
            .conn_state    = BACKEND_CONN_CLOSED,
            .quarantine    = KEEL_QUARANTINE_NONE,
            .has_owner     = false,
            .has_stmts     = false,
            .needs_sync    = false,
            .in_transaction = false,
        };
    }

    switch (conn->quarantine) {
    case BACKEND_QUARANTINE_NONE: q = KEEL_QUARANTINE_NONE; break;
    case BACKEND_QUARANTINE_DIRTY: q = KEEL_QUARANTINE_DIRTY_STATE; break;
    case BACKEND_QUARANTINE_SYNCING: q = KEEL_QUARANTINE_FAILED_SYNC; break;
    case BACKEND_QUARANTINE_REPLAYING: q = KEEL_QUARANTINE_REPLAY_MISMATCH; break;
    case BACKEND_QUARANTINE_PROTOCOL_DESYNC: q = KEEL_QUARANTINE_PROTOCOL_DESYNC; break;
    case BACKEND_QUARANTINE_CLEANUP_FAILED: q = KEEL_QUARANTINE_FAILED_CLEANUP; break;
    }

    return (keel_backend_contract_t){
        .conn_state    = atomic_load_explicit(&conn->state, memory_order_relaxed),
        .quarantine    = q,
        .has_owner     = (conn->active_owner != NULL) || (conn->pinned_session != NULL),
        .has_stmts     = conn->stmt_set_hash != 0,
        .needs_sync    = conn->needs_sync,
        .in_transaction = conn->in_transaction,
    };
}

/* ============================================================================
 * §6 — Transition Functions
 * ============================================================================ */

/* Phase transition validation matrix.
 * phase_transitions[current][new] == true means the transition is legal.
 * We use a compact representation: for each phase, list allowed successors. */
static const bool phase_transitions[6][6] = {
    /* From HANDSHAKE_AUTH → READY or CLOSING */
    [KEEL_PHASE_HANDSHAKE_AUTH] = {
        [KEEL_PHASE_READY] = true,
        [KEEL_PHASE_CLOSING] = true,
    },
    /* From READY → QUERY, BACKEND_SYNC, or CLOSING */
    [KEEL_PHASE_READY] = {
        [KEEL_PHASE_QUERY] = true,
        [KEEL_PHASE_BACKEND_SYNC] = true,
        [KEEL_PHASE_CLOSING] = true,
    },
    /* From QUERY → READY, BACKEND_SYNC, BACKEND_CLEANING, or CLOSING */
    [KEEL_PHASE_QUERY] = {
        [KEEL_PHASE_READY] = true,
        [KEEL_PHASE_BACKEND_SYNC] = true,
        [KEEL_PHASE_BACKEND_CLEANING] = true,
        [KEEL_PHASE_CLOSING] = true,
    },
    /* From BACKEND_SYNC → QUERY or CLOSING */
    [KEEL_PHASE_BACKEND_SYNC] = {
        [KEEL_PHASE_QUERY] = true,
        [KEEL_PHASE_CLOSING] = true,
    },
    /* From BACKEND_CLEANING → READY or CLOSING */
    [KEEL_PHASE_BACKEND_CLEANING] = {
        [KEEL_PHASE_READY] = true,
        [KEEL_PHASE_CLOSING] = true,
    },
    /* From CLOSING → nowhere (terminal, but allow re-entry for cleanup) */
    [KEEL_PHASE_CLOSING] = { 0 },
};

/**
 * @brief Validate and apply a session-phase transition.
 *
 * Only legal successor phases are accepted. Illegal transitions leave the
 * state untouched and return an error so callers cannot silently corrupt the
 * phase machine.
 *
 * @param sf Session flow state to update.
 * @param session Session used for journal attribution.
 * @param new_phase Requested successor phase.
 * @param journal Optional journal receiving the transition event.
 * @return `0` on success or `-1` if the transition is illegal.
 */
int keel_session_transition_phase(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_session_phase_t new_phase,
    keel_state_journal_t* journal)
{
    keel_session_phase_t old = sf->phase;

    if (old == new_phase) return 0; /* No-op */

    if (old >= 6 || new_phase >= 6) return -1;

    if (!phase_transitions[old][new_phase]) {
        fprintf(stderr, "ILLEGAL phase transition: %s -> %s\n",
                keel_session_phase_name(old),
                keel_session_phase_name(new_phase));
        return -1;
    }

    sf->phase = new_phase;

    if (journal) {
        keel_journal_record(journal,
                            session ? (uint32_t)session->id : 0,
                            KEEL_DOMAIN_PHASE,
                            (uint8_t)old, (uint8_t)new_phase, 0);
    }
    return 0;
}

/**
 * @brief Bind a backend connection to a session with optional immediate pinning.
 *
 * The bind is only legal from unbound or pool-waiting states. On success the
 * session and backend are cross-linked, socket ownership is updated, and the
 * backend is marked active.
 *
 * @param sf Session flow state.
 * @param session Session receiving the backend.
 * @param conn Backend connection to attach.
 * @param bind_type Target binding interpretation to journal.
 * @param journal Optional transition journal.
 * @return `0` on success or `-1` if preconditions are violated.
 */
int keel_session_transition_bind(
    keel_session_flow_t* sf,
    keel_session_t* session,
    backend_conn_t* conn,
    keel_backend_binding_t bind_type,
    keel_state_journal_t* journal)
{
    if (!session || !conn) return -1;

    keel_backend_binding_t old = keel_derive_binding(sf, session);

    /* Precondition: must be UNBOUND or BORROW_PENDING to bind */
    if (old != KEEL_BIND_UNBOUND && old != KEEL_BIND_BORROW_PENDING) {
        fprintf(stderr, "ILLEGAL bind: current binding=%s, cannot bind\n",
                keel_backend_binding_name(old));
        return -1;
    }

    /* Perform the bind */
    session->backend_conn = conn;
    session->backend_generation = conn ? conn->generation : 0;
    session->server_fd = conn->fd;
    conn->pinned_session = session;
    atomic_store_explicit(&conn->state, BACKEND_CONN_ACTIVE, memory_order_release);
    sf->queued_for_pool = false;

    /* If bind_type requests immediate pin escalation */
    if (bind_type == KEEL_BIND_PINNED_TXN) {
        session->in_transaction = true;
        conn->in_transaction = true;
        keel_session_flow_apply_pin_change(sf, session, session->worker,
                                           KEEL_FPIN_TRANSACTION,
                                           KEEL_FPIN_NONE);
    } else if (bind_type == KEEL_BIND_HARD_PINNED) {
        session->hard_pinned = true;
        conn->hard_pinned = true;
    }

    if (journal) {
        keel_journal_record(journal,
                            (uint32_t)session->id,
                            KEEL_DOMAIN_BINDING,
                            (uint8_t)old, (uint8_t)bind_type, 0);
    }
    return 0;
}

/**
 * @brief Detach a backend connection from a session.
 *
 * The function clears cross-links and local transaction flags but deliberately
 * leaves the final backend-list decision to the caller, because the backend may
 * need to return to the pool, enter cleaning, or be closed.
 *
 * @param sf Session flow state.
 * @param session Session releasing the backend.
 * @param journal Optional transition journal.
 * @return `0` on success or `-1` when `session` is invalid.
 */
int keel_session_transition_unbind(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_state_journal_t* journal)
{
    if (!session) return -1;

    keel_backend_binding_t old = keel_derive_binding(sf, session);
    if (old == KEEL_BIND_UNBOUND) return 0; /* already unbound */

    backend_conn_t* conn = session->backend_conn;
    if (conn) {
        conn->pinned_session = NULL;
        conn->hard_pinned = false;
        conn->in_transaction = false;
        /* Caller decides whether to IDLE or CLEANING — we clear the ref */
    }

    session->backend_conn = NULL;
    session->backend_generation = 0;
    session->server_fd = -1;
    session->in_transaction = false;
    session->hard_pinned = false;
    sf->queued_for_pool = false;

    if (journal) {
        keel_journal_record(journal,
                            (uint32_t)session->id,
                            KEEL_DOMAIN_BINDING,
                            (uint8_t)old, (uint8_t)KEEL_BIND_UNBOUND, 0);
    }
    return 0;
}

/**
 * @brief Transition a session into an active transaction.
 *
 * The session must already own a backend, and its transaction state must be
 * idle. Success upgrades the pin set so the backend cannot be returned to the
 * pool until the transaction is resolved.
 *
 * @param sf Session flow state.
 * @param session Session entering transaction scope.
 * @param journal Optional transition journal.
 * @return `0` on success or `-1` if preconditions fail.
 */
int keel_session_transition_begin_txn(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_state_journal_t* journal)
{
    if (!session) return -1;

    keel_backend_binding_t bind = keel_derive_binding(sf, session);

    /* Precondition: must have a backend (at least SHARED) */
    if (bind < KEEL_BIND_SHARED) {
        fprintf(stderr, "ILLEGAL begin_txn: binding=%s\n",
                keel_backend_binding_name(bind));
        return -1;
    }

    /* Precondition: tx must be IDLE */
    if (sf->tx != KEEL_TX_IDLE) {
        fprintf(stderr, "ILLEGAL begin_txn: tx=%d (not IDLE)\n", sf->tx);
        return -1;
    }

    keel_tx_status_t old_tx = sf->tx;
    sf->tx = KEEL_TX_ACTIVE;
    keel_session_flow_apply_pin_change(sf, session, session->worker,
                                       KEEL_FPIN_TRANSACTION,
                                       KEEL_FPIN_NONE);
    session->in_transaction = true;

    if (session->backend_conn)
        session->backend_conn->in_transaction = true;

    if (journal) {
        keel_journal_record(journal,
                            (uint32_t)session->id,
                            KEEL_DOMAIN_TX,
                            (uint8_t)old_tx, (uint8_t)KEEL_TX_ACTIVE, 0);
    }
    return 0;
}

/**
 * @brief End the current transaction and clear transaction pinning.
 *
 * @param sf Session flow state.
 * @param session Session leaving transaction scope.
 * @param new_tx_status Final transaction status reported by the backend.
 * @param journal Optional transition journal.
 * @return `0` on success or `-1` if the session was not in a transaction.
 */
int keel_session_transition_end_txn(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_tx_status_t new_tx_status,
    keel_state_journal_t* journal)
{
    if (!session) return -1;

    /* Precondition: tx must be ACTIVE or FAILED */
    if (sf->tx != KEEL_TX_ACTIVE && sf->tx != KEEL_TX_FAILED) {
        fprintf(stderr, "ILLEGAL end_txn: tx=%d (not ACTIVE/FAILED)\n", sf->tx);
        return -1;
    }

    keel_tx_status_t old_tx = sf->tx;
    sf->tx = new_tx_status;
    keel_session_flow_apply_pin_change(sf, session, session->worker,
                                       KEEL_FPIN_NONE,
                                       KEEL_FPIN_TRANSACTION);
    session->in_transaction = false;

    if (session->backend_conn)
        session->backend_conn->in_transaction = false;

    if (journal) {
        keel_journal_record(journal,
                            (uint32_t)session->id,
                            KEEL_DOMAIN_TX,
                            (uint8_t)old_tx, (uint8_t)new_tx_status, 0);
    }
    return 0;
}

/* Replay transition validation. */
static const bool replay_transitions[KEEL_REPLAY_COUNT][KEEL_REPLAY_COUNT] = {
    [KEEL_REPLAY_NONE] = {
        [KEEL_REPLAY_CLEANUP_PENDING] = true,
        [KEEL_REPLAY_SENDING] = true,
    },
    [KEEL_REPLAY_CLEANUP_PENDING] = {
        [KEEL_REPLAY_CLEANUP_SENT] = true,
        [KEEL_REPLAY_NONE] = true,  /* abort */
    },
    [KEEL_REPLAY_CLEANUP_SENT] = {
        [KEEL_REPLAY_SENDING] = true,
        [KEEL_REPLAY_NONE] = true,  /* abort */
    },
    [KEEL_REPLAY_SENDING] = {
        [KEEL_REPLAY_WAITING] = true,
        [KEEL_REPLAY_NONE] = true,  /* abort */
    },
    [KEEL_REPLAY_WAITING] = {
        [KEEL_REPLAY_RFQ_PENDING] = true,
        [KEEL_REPLAY_NONE] = true,  /* abort */
    },
    [KEEL_REPLAY_RFQ_PENDING] = {
        [KEEL_REPLAY_COMPLETE] = true,
        [KEEL_REPLAY_NONE] = true,  /* abort */
    },
    [KEEL_REPLAY_COMPLETE] = {
        [KEEL_REPLAY_NONE] = true,  /* finished */
    },
};

/**
 * @brief Validate a prepared-statement replay transition and journal it.
 *
 * Actual replay bookkeeping fields remain owned by the flow layer; this helper
 * enforces legal state progression so replay sequencing bugs are caught at the
 * boundary where they are introduced.
 *
 * @param sf Session flow state.
 * @param new_state Requested replay state.
 * @param journal Optional transition journal.
 * @return `0` on success or `-1` if the transition is illegal.
 */
int keel_session_transition_replay(
    keel_session_flow_t* sf,
    keel_replay_state_t new_state,
    keel_state_journal_t* journal)
{
    keel_replay_state_t old = keel_derive_replay_state(sf);

    if (old == new_state) return 0;

    if (old >= KEEL_REPLAY_COUNT || new_state >= KEEL_REPLAY_COUNT)
        return -1;

    if (!replay_transitions[old][new_state]) {
        fprintf(stderr, "ILLEGAL replay transition: %s -> %s\n",
                keel_replay_state_name(old),
                keel_replay_state_name(new_state));
        return -1;
    }

    /* Record transition. Actual field updates are done by
     * the engine flow code — this function validates the transition
     * and journals it. The caller mutates the sf fields. */
    if (journal) {
        keel_journal_record(journal, 0,
                            KEEL_DOMAIN_REPLAY,
                            (uint8_t)old, (uint8_t)new_state, 0);
    }
    return 0;
}

/* CID transition validation. */
static const bool cid_transitions[KEEL_CID_COUNT][KEEL_CID_COUNT] = {
    [KEEL_CID_NONE] = {
        [KEEL_CID_TRACKING] = true,
    },
    [KEEL_CID_TRACKING] = {
        [KEEL_CID_XID_CAPTURED] = true,
        [KEEL_CID_NONE] = true,    /* TX ended without COMMIT */
    },
    [KEEL_CID_XID_CAPTURED] = {
        [KEEL_CID_COMMIT_SENT] = true,
        [KEEL_CID_NONE] = true,    /* XID captured but TX rolled back */
    },
    [KEEL_CID_COMMIT_SENT] = {
        [KEEL_CID_NONE] = true,           /* Happy path: COMMIT succeeded */
        [KEEL_CID_BACKEND_LOST] = true,   /* Backend died mid-COMMIT */
    },
    [KEEL_CID_BACKEND_LOST] = {
        [KEEL_CID_CHECK_BORROWING] = true,
        [KEEL_CID_RESOLVED_UNKNOWN] = true, /* Can't resolve */
    },
    [KEEL_CID_CHECK_BORROWING] = {
        [KEEL_CID_CHECK_SENT] = true,
        [KEEL_CID_RESOLVED_UNKNOWN] = true, /* Borrow failed */
    },
    [KEEL_CID_CHECK_SENT] = {
        [KEEL_CID_RESOLVED_COMMITTED] = true,
        [KEEL_CID_RESOLVED_ABORTED] = true,
        [KEEL_CID_RESOLVED_UNKNOWN] = true,
    },
    [KEEL_CID_RESOLVED_COMMITTED] = {
        [KEEL_CID_NONE] = true,    /* Response sent, cleanup */
    },
    [KEEL_CID_RESOLVED_ABORTED] = {
        [KEEL_CID_NONE] = true,
    },
    [KEEL_CID_RESOLVED_UNKNOWN] = {
        [KEEL_CID_NONE] = true,
    },
};

/**
 * @brief Validate and apply a commit-in-doubt state transition.
 *
 * Unlike replay transitions, CID transitions also update a few dependent flags
 * such as `commit_in_doubt`, `commit_in_flight`, and resolved-check markers so
 * that later predicates see a coherent state.
 *
 * @param sf Session flow state.
 * @param new_state Requested CID state.
 * @param journal Optional transition journal.
 * @return `0` on success or `-1` if the transition is illegal.
 */
int keel_session_transition_cid(
    keel_session_flow_t* sf,
    keel_cid_state_t new_state,
    keel_state_journal_t* journal)
{
    keel_cid_state_t old = keel_derive_cid_state(sf);

    if (old == new_state) return 0;

    if (old >= KEEL_CID_COUNT || new_state >= KEEL_CID_COUNT)
        return -1;

    if (!cid_transitions[old][new_state]) {
        fprintf(stderr, "ILLEGAL CID transition: %s -> %s\n",
                keel_cid_state_name(old),
                keel_cid_state_name(new_state));
        return -1;
    }

    /* Update dependent fields based on transition */
    if (new_state == KEEL_CID_BACKEND_LOST)
        sf->commit_in_doubt = true;

    if (new_state == KEEL_CID_NONE) {
        sf->commit_in_doubt = false;
        sf->commit_in_flight = false;
        sf->indoubt_check_result = 0;
    }

    if (new_state >= KEEL_CID_RESOLVED_COMMITTED) {
        sf->commit_in_doubt = false;
        sf->commit_in_flight = false;
    }

    if (journal) {
        keel_journal_record(journal, 0,
                            KEEL_DOMAIN_CID,
                            (uint8_t)old, (uint8_t)new_state, 0);
    }
    return 0;
}

/**
 * @brief Escalate a session's backend ownership to hard-pinned.
 *
 * Hard pinning is used for features like LISTEN, temp tables, or other session
 * semantics that forbid backend reuse until disconnect.
 *
 * @param sf Session flow state.
 * @param session Session to hard-pin.
 * @param journal Optional transition journal.
 * @return `0` on success or `-1` if the session has no backend to pin.
 */
int keel_session_transition_hard_pin(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_state_journal_t* journal)
{
    if (!session) return -1;

    keel_backend_binding_t old = keel_derive_binding(sf, session);

    if (old < KEEL_BIND_SHARED) {
        fprintf(stderr, "ILLEGAL hard_pin: binding=%s (no backend)\n",
                keel_backend_binding_name(old));
        return -1;
    }

    if (old == KEEL_BIND_HARD_PINNED)
        return 0; /* already hard-pinned */

    session->hard_pinned = true;
    if (session->backend_conn)
        session->backend_conn->hard_pinned = true;

    if (journal) {
        keel_journal_record(journal,
                            (uint32_t)session->id,
                            KEEL_DOMAIN_BINDING,
                            (uint8_t)old, (uint8_t)KEEL_BIND_HARD_PINNED, 0);
    }
    return 0;
}

/**
 * @brief Move a backend into quarantine/cleaning state after unsafe behavior.
 *
 * Quarantined backends are never returned directly to the clean pool. They are
 * marked for cleaning and eventual disposal if recovery fails.
 *
 * @param conn Backend connection to quarantine.
 * @param reason Reason for quarantine.
 * @param journal Optional transition journal.
 * @return `0` on success or `-1` if inputs are invalid.
 */
int keel_backend_transition_quarantine(
    backend_conn_t* conn,
    keel_quarantine_reason_t reason,
    keel_state_journal_t* journal)
{
    if (!conn) return -1;
    if (reason == KEEL_QUARANTINE_NONE) return -1; /* can't quarantine with NONE */

    backend_conn_state_t old = atomic_load_explicit(&conn->state, memory_order_relaxed);

    /* Quarantine = move to CLEANING + record reason.
     * Backend will be closed after cleanup attempt. */
    atomic_store_explicit(&conn->state, BACKEND_CONN_CLEANING, memory_order_release);

    if (journal) {
        keel_journal_record(journal,
                            0,
                            KEEL_DOMAIN_QUARANTINE,
                            (uint8_t)KEEL_QUARANTINE_NONE,
                            (uint8_t)reason,
                            (uint8_t)old);
    }
    return 0;
}

/* ============================================================================
 * §7 — Derived Predicates
 * ============================================================================ */

/**
 * @brief Determine whether a session can be force-closed safely.
 *
 * Sessions actively resolving commit-in-doubt must be spared so their outcome
 * check can finish and the client can receive a correct transaction result.
 *
 * @param c Session contract snapshot.
 * @return `true` if force-close is allowed.
 */
bool keel_session_can_force_close(const keel_session_contract_t* c)
{
    /* Cannot force-close sessions in CID doubt states */
    switch (c->cid) {
    case KEEL_CID_BACKEND_LOST:
    case KEEL_CID_CHECK_BORROWING:
    case KEEL_CID_CHECK_SENT:
        return false;
    default:
        return true;
    }
}

/**
 * @brief Check whether the session is eligible for zero-copy splice bypass.
 *
 * The predicate rejects sessions with replay, partial-message forwarding, COPY
 * boundary tracking, or incompatible TLS state because those situations require
 * userspace framing awareness.
 *
 * @param sf Session flow state.
 * @param session Session runtime state.
 * @return `true` when splice bypass is safe.
 */
bool keel_session_can_splice(const keel_session_flow_t* sf,
                             const keel_session_t* session)
{
    if (!session) return false;

    /* Must not be in replay */
    if (sf->stmt_replay_len > 0) return false;

    /* Must not have pending deferred forward */
    if (sf->fe_fwd_remaining > 0 || sf->be_fwd_remaining > 0) return false;

    /* Must not be in COPY parse mode (need message boundary tracking) */
    if (sf->copy_hdr_len > 0 || sf->copy_skip > 0) return false;

    /* TLS: splice only with kTLS or plaintext */
    if (session->flags & KEEL_SESSION_FLAG_SSL) {
        /* If SSL is set, splice is only safe with kTLS (kernel handles crypto).
         * TLS state check: we'd need the TLS context here but the session flag
         * KEEL_SESSION_FLAG_SPLICE already represents this correctly at a higher
         * level. The predicate confirms no conflicting conditions exist. */
        if (!(session->flags & KEEL_SESSION_FLAG_SPLICE))
            return false;
    }

    return true;
}

/**
 * @brief Check whether a borrowed backend requires prepared-statement replay.
 *
 * @param sf Session flow state.
 * @param conn Borrowed backend connection.
 * @return `true` when the backend's statement hash differs from the session's replay hash.
 */
bool keel_session_needs_replay(const keel_session_flow_t* sf,
                               const backend_conn_t* conn)
{
    if (!conn) return false;
    if (sf->stmt_replay_len == 0) return false;

    /* Hash mismatch means replay is needed */
    return sf->stmt_replay_hash != conn->stmt_set_hash;
}

/**
 * @brief Check whether a backend can be safely returned to the shared pool.
 *
 * @param conn Backend connection.
 * @return `true` if the backend is open, not pinned, and not inside a transaction.
 */
bool keel_backend_can_return_to_pool(const backend_conn_t* conn)
{
    if (!conn) return false;

    backend_conn_state_t st = atomic_load_explicit(&conn->state, memory_order_relaxed);

    /* Must not be closed */
    if (st == BACKEND_CONN_CLOSED) return false;

    /* Must not be in transaction */
    if (conn->in_transaction) return false;

    /* Must not be hard-pinned */
    if (conn->hard_pinned) return false;

    return true;
}

/* ============================================================================
 * §8 — Contract-Level Invariant Checks
 * ============================================================================ */

/**
 * @brief Validate a session contract against cross-domain safety rules.
 *
 * The checker reports a bitmask rather than aborting so callers can aggregate,
 * log, or hard-fail depending on build mode and call site.
 *
 * @param c Session contract snapshot.
 * @param sf Session flow state used for a few additional derived checks.
 * @return Bitmask of contract violations, or `KEEL_CONTRACT_OK` when valid.
 */
uint32_t keel_contract_check_session(const keel_session_contract_t* c,
                                     const keel_session_flow_t* sf)
{
    uint32_t violations = KEEL_CONTRACT_OK;

    /* Binding × Phase: QUERY/SYNC phases require a backend */
    if ((c->phase == KEEL_PHASE_QUERY || c->phase == KEEL_PHASE_BACKEND_SYNC) &&
        c->binding == KEEL_BIND_UNBOUND) {
        /* Exception: BORROW_PENDING is OK during QUERY while waiting */
        if (c->binding != KEEL_BIND_BORROW_PENDING)
            violations |= KEEL_CONTRACT_QUERY_UNBOUND;
    }

    /* Binding × Phase: READY with SHARED is suspicious (should have returned) */
    if (c->phase == KEEL_PHASE_READY && c->binding == KEEL_BIND_SHARED) {
        /* Only allowed briefly during transition or if pins will keep it */
        if (c->pins == KEEL_FPIN_NONE)
            violations |= KEEL_CONTRACT_READY_NON_IDLE_BINDING;
    }

    /* Binding × TX: TXN pin must have active TX */
    if (c->binding == KEEL_BIND_PINNED_TXN &&
        c->tx == KEEL_TX_IDLE) {
        violations |= KEEL_CONTRACT_TXN_PIN_NO_TX;
    }

    /* TX × Binding: Active TX but no backend */
    if (c->tx != KEEL_TX_IDLE && c->binding == KEEL_BIND_UNBOUND) {
        violations |= KEEL_CONTRACT_TX_ACTIVE_UNBOUND;
    }

    /* Replay × Binding */
    if (c->replay != KEEL_REPLAY_NONE &&
        c->replay != KEEL_REPLAY_COMPLETE &&
        c->binding == KEEL_BIND_UNBOUND) {
        violations |= KEEL_CONTRACT_REPLAY_UNBOUND;
    }

    /* CID × Backend */
    if (c->binding == KEEL_BIND_CID_CHECK && sf->xid_check_conn == NULL) {
        violations |= KEEL_CONTRACT_CID_CHECK_NO_CONN;
    }

    /* CID doubt requires XID */
    if ((c->cid == KEEL_CID_BACKEND_LOST ||
         c->cid == KEEL_CID_CHECK_BORROWING ||
         c->cid == KEEL_CID_CHECK_SENT) &&
        sf->indoubt_xid == 0) {
        violations |= KEEL_CONTRACT_CID_DOUBT_NO_XID;
    }

    /* Drain × CID: engine draining + CID in doubt */
    if (c->engine_state == KEEL_ENGINE_STATE_DRAINING &&
        (c->cid == KEEL_CID_BACKEND_LOST ||
         c->cid == KEEL_CID_CHECK_BORROWING ||
         c->cid == KEEL_CID_CHECK_SENT)) {
        /* This is not a violation per se — it's a warning that
         * force-close must skip this session. We track it so the
         * drain code can be aware. */
        violations |= KEEL_CONTRACT_DRAIN_KILLS_CID;
    }

    return violations;
}

/**
 * @brief Validate a backend contract for ownership and quarantine consistency.
 *
 * @param c Backend contract snapshot.
 * @return Bitmask of backend contract violations, or `KEEL_CONTRACT_OK` when valid.
 */
uint32_t keel_contract_check_backend(const keel_backend_contract_t* c)
{
    uint32_t violations = KEEL_CONTRACT_OK;

    /* IDLE backend should not have an owner */
    if (c->conn_state == BACKEND_CONN_IDLE && c->has_owner)
        violations |= KEEL_CONTRACT_IDLE_WITH_OWNER;

    /* ACTIVE backend should have an owner */
    if (c->conn_state == BACKEND_CONN_ACTIVE && !c->has_owner)
        violations |= KEEL_CONTRACT_ACTIVE_NO_OWNER;

    /* Quarantined backend should not be IDLE (should be CLEANING) */
    if (c->quarantine != KEEL_QUARANTINE_NONE &&
        c->conn_state == BACKEND_CONN_IDLE)
        violations |= KEEL_CONTRACT_QUARANTINE_IN_POOL;

    return violations;
}
