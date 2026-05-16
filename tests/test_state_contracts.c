/**
 * @file test_state_contracts.c
 * @brief Comprehensive tests for the formal state machine model
 *
 * Tests all aspects of the contract-driven state model (state_machine.h/c):
 * - New enum string conversions
 * - Derived state functions (binding, replay, CID)
 * - Contract sync from scattered fields
 * - Transition functions (precondition validation, journaling)
 * - Derived predicates (force close, splice, replay, pool return)
 * - Contract-level invariant checks
 * - Event journal recording and dump
 * - Phase/replay/CID transition matrix validation
 * - Quarantine transitions
 */

#include "test_utils.h"
#include "keel/engine/state_machine.h"
#include "keel/session/session.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdatomic.h>

/* ============================================================================
 * Helpers — create zeroed structs for testing
 * ============================================================================ */

/**
 * @brief Create a zeroed session_flow in the READY / TX_IDLE state.
 *
 * This is the baseline "clean" flow that most contract-derivation
 * tests start from before mutating a single field.
 *
 * @return Value-initialised keel_session_flow_t.
 */
static keel_session_flow_t make_sf(void)
{
    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.phase = KEEL_PHASE_READY;
    sf.tx = KEEL_TX_IDLE;
    return sf;
}

/**
 * @brief Create a zeroed session with a stable id (42) and READY state.
 *
 * server_fd is set to -1 (unbound) so that binding-derivation tests
 * can verify the UNBOUND baseline.
 *
 * @return Value-initialised keel_session_t.
 */
static keel_session_t make_session(void)
{
    keel_session_t s;
    memset(&s, 0, sizeof(s));
    s.id = 42;
    s.state = KEEL_SESSION_READY;
    s.server_fd = -1;
    return s;
}

/**
 * @brief Create a zeroed backend_conn with fd=10 and IDLE state.
 *
 * The fd value (10) is arbitrary but non-negative so that
 * binding-derivation code treats the connection as live.
 *
 * @return Value-initialised backend_conn_t.
 */
static backend_conn_t make_backend(void)
{
    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = 10;
    atomic_store(&be.state, BACKEND_CONN_IDLE);
    return be;
}

/* ============================================================================
 * §1 — String Conversion Tests
 * ============================================================================ */

static void test_binding_names(void)
{
    TEST_BEGIN("binding enum names");

    TEST_ASSERT_STR_EQ(keel_backend_binding_name(KEEL_BIND_UNBOUND), "UNBOUND");
    TEST_ASSERT_STR_EQ(keel_backend_binding_name(KEEL_BIND_BORROW_PENDING), "BORROW_PENDING");
    TEST_ASSERT_STR_EQ(keel_backend_binding_name(KEEL_BIND_SHARED), "SHARED");
    TEST_ASSERT_STR_EQ(keel_backend_binding_name(KEEL_BIND_PINNED_TXN), "PINNED_TXN");
    TEST_ASSERT_STR_EQ(keel_backend_binding_name(KEEL_BIND_PINNED_STATE), "PINNED_STATE");
    TEST_ASSERT_STR_EQ(keel_backend_binding_name(KEEL_BIND_PINNED_PS), "PINNED_PS");
    TEST_ASSERT_STR_EQ(keel_backend_binding_name(KEEL_BIND_HARD_PINNED), "HARD_PINNED");
    TEST_ASSERT_STR_EQ(keel_backend_binding_name(KEEL_BIND_CID_CHECK), "CID_CHECK");
    TEST_ASSERT_STR_EQ(keel_backend_binding_name(KEEL_BIND_COUNT), "UNKNOWN");

    TEST_END();
}

static void test_replay_state_names(void)
{
    TEST_BEGIN("replay state names");

    TEST_ASSERT_STR_EQ(keel_replay_state_name(KEEL_REPLAY_NONE), "NONE");
    TEST_ASSERT_STR_EQ(keel_replay_state_name(KEEL_REPLAY_DISCARD_PENDING), "DISCARD_PENDING");
    TEST_ASSERT_STR_EQ(keel_replay_state_name(KEEL_REPLAY_DISCARD_SENT), "DISCARD_SENT");
    TEST_ASSERT_STR_EQ(keel_replay_state_name(KEEL_REPLAY_SENDING), "SENDING");
    TEST_ASSERT_STR_EQ(keel_replay_state_name(KEEL_REPLAY_WAITING), "WAITING");
    TEST_ASSERT_STR_EQ(keel_replay_state_name(KEEL_REPLAY_RFQ_PENDING), "RFQ_PENDING");
    TEST_ASSERT_STR_EQ(keel_replay_state_name(KEEL_REPLAY_COMPLETE), "COMPLETE");
    TEST_ASSERT_STR_EQ(keel_replay_state_name(KEEL_REPLAY_COUNT), "UNKNOWN");

    TEST_END();
}

static void test_cid_state_names(void)
{
    TEST_BEGIN("CID state names");

    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_NONE), "NONE");
    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_TRACKING), "TRACKING");
    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_XID_CAPTURED), "XID_CAPTURED");
    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_COMMIT_SENT), "COMMIT_SENT");
    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_BACKEND_LOST), "BACKEND_LOST");
    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_CHECK_BORROWING), "CHECK_BORROWING");
    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_CHECK_SENT), "CHECK_SENT");
    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_RESOLVED_COMMITTED), "RESOLVED_COMMITTED");
    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_RESOLVED_ABORTED), "RESOLVED_ABORTED");
    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_RESOLVED_UNKNOWN), "RESOLVED_UNKNOWN");
    TEST_ASSERT_STR_EQ(keel_cid_state_name(KEEL_CID_COUNT), "UNKNOWN");

    TEST_END();
}

static void test_quarantine_names(void)
{
    TEST_BEGIN("quarantine reason names");

    TEST_ASSERT_STR_EQ(keel_quarantine_reason_name(KEEL_QUARANTINE_NONE), "NONE");
    TEST_ASSERT_STR_EQ(keel_quarantine_reason_name(KEEL_QUARANTINE_DIRTY_STATE), "DIRTY_STATE");
    TEST_ASSERT_STR_EQ(keel_quarantine_reason_name(KEEL_QUARANTINE_REPLAY_MISMATCH), "REPLAY_MISMATCH");
    TEST_ASSERT_STR_EQ(keel_quarantine_reason_name(KEEL_QUARANTINE_PROTOCOL_DESYNC), "PROTOCOL_DESYNC");
    TEST_ASSERT_STR_EQ(keel_quarantine_reason_name(KEEL_QUARANTINE_TLS_MISMATCH), "TLS_MISMATCH");
    TEST_ASSERT_STR_EQ(keel_quarantine_reason_name(KEEL_QUARANTINE_FAILED_DISCARD), "FAILED_DISCARD");
    TEST_ASSERT_STR_EQ(keel_quarantine_reason_name(KEEL_QUARANTINE_FAILED_SYNC), "FAILED_SYNC");
    TEST_ASSERT_STR_EQ(keel_quarantine_reason_name(KEEL_QUARANTINE_COUNT), "UNKNOWN");

    TEST_END();
}

static void test_domain_names(void)
{
    TEST_BEGIN("state domain names");

    TEST_ASSERT_STR_EQ(keel_state_domain_name(KEEL_DOMAIN_PHASE), "PHASE");
    TEST_ASSERT_STR_EQ(keel_state_domain_name(KEEL_DOMAIN_BINDING), "BINDING");
    TEST_ASSERT_STR_EQ(keel_state_domain_name(KEEL_DOMAIN_TX), "TX");
    TEST_ASSERT_STR_EQ(keel_state_domain_name(KEEL_DOMAIN_REPLAY), "REPLAY");
    TEST_ASSERT_STR_EQ(keel_state_domain_name(KEEL_DOMAIN_CID), "CID");
    TEST_ASSERT_STR_EQ(keel_state_domain_name(KEEL_DOMAIN_BACKEND_CONN), "BACKEND_CONN");
    TEST_ASSERT_STR_EQ(keel_state_domain_name(KEEL_DOMAIN_QUARANTINE), "QUARANTINE");
    TEST_ASSERT_STR_EQ(keel_state_domain_name(KEEL_DOMAIN_PINS), "PINS");
    TEST_ASSERT_STR_EQ(keel_state_domain_name(KEEL_DOMAIN_COUNT), "UNKNOWN");

    TEST_END();
}

/* ============================================================================
 * §2 — Derived Binding Tests
 * ============================================================================ */

static void test_derive_binding_unbound(void)
{
    TEST_BEGIN("derive binding — unbound");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    s.backend_conn = NULL;
    sf.queued_for_pool = false;
    TEST_ASSERT_EQ(keel_derive_binding(&sf, &s), KEEL_BIND_UNBOUND);
    TEST_END();
}

static void test_derive_binding_borrow_pending(void)
{
    TEST_BEGIN("derive binding — borrow pending");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    s.backend_conn = NULL;
    sf.queued_for_pool = true;
    TEST_ASSERT_EQ(keel_derive_binding(&sf, &s), KEEL_BIND_BORROW_PENDING);
    TEST_END();
}

static void test_derive_binding_shared(void)
{
    TEST_BEGIN("derive binding — shared");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    s.backend_conn = &be;
    TEST_ASSERT_EQ(keel_derive_binding(&sf, &s), KEEL_BIND_SHARED);
    TEST_END();
}

static void test_derive_binding_txn_pinned(void)
{
    TEST_BEGIN("derive binding — txn pinned");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    s.backend_conn = &be;
    s.in_transaction = true;
    TEST_ASSERT_EQ(keel_derive_binding(&sf, &s), KEEL_BIND_PINNED_TXN);
    TEST_END();
}

static void test_derive_binding_hard_pinned(void)
{
    TEST_BEGIN("derive binding — hard pinned");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    s.backend_conn = &be;
    s.hard_pinned = true;
    TEST_ASSERT_EQ(keel_derive_binding(&sf, &s), KEEL_BIND_HARD_PINNED);
    TEST_END();
}

static void test_derive_binding_ps_pinned(void)
{
    TEST_BEGIN("derive binding — PS pinned");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    s.backend_conn = &be;
    sf.ps_mode = KEEL_PS_MODE_PINNING;
    sf.pins |= KEEL_FPIN_PREPARED_STMT;
    TEST_ASSERT_EQ(keel_derive_binding(&sf, &s), KEEL_BIND_PINNED_PS);
    TEST_END();
}

static void test_derive_binding_state_pinned(void)
{
    TEST_BEGIN("derive binding — state pinned (LISTEN)");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    s.backend_conn = &be;
    sf.pins |= KEEL_FPIN_LISTEN;
    TEST_ASSERT_EQ(keel_derive_binding(&sf, &s), KEEL_BIND_PINNED_STATE);
    TEST_END();
}

static void test_derive_binding_cid_check(void)
{
    TEST_BEGIN("derive binding — CID check");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t check_be = make_backend();
    sf.xid_check_conn = &check_be;
    TEST_ASSERT_EQ(keel_derive_binding(&sf, &s), KEEL_BIND_CID_CHECK);
    TEST_END();
}

static void test_derive_binding_null_session(void)
{
    TEST_BEGIN("derive binding — NULL session");
    keel_session_flow_t sf = make_sf();
    TEST_ASSERT_EQ(keel_derive_binding(&sf, NULL), KEEL_BIND_UNBOUND);
    TEST_END();
}

/* ============================================================================
 * §3 — Derived Replay State Tests
 * ============================================================================ */

static void test_derive_replay_none(void)
{
    TEST_BEGIN("derive replay — none");
    keel_session_flow_t sf = make_sf();
    TEST_ASSERT_EQ(keel_derive_replay_state(&sf), KEEL_REPLAY_NONE);
    TEST_END();
}

static void test_derive_replay_discard_pending(void)
{
    TEST_BEGIN("derive replay — discard pending");
    keel_session_flow_t sf = make_sf();
    sf.stmt_replay_len = 100;
    sf.stmt_replay_needs_discard = true;
    TEST_ASSERT_EQ(keel_derive_replay_state(&sf), KEEL_REPLAY_DISCARD_PENDING);
    TEST_END();
}

static void test_derive_replay_waiting(void)
{
    TEST_BEGIN("derive replay — waiting for ParseComplete");
    keel_session_flow_t sf = make_sf();
    sf.stmt_replay_len = 100;
    sf.stmt_replay_count = 3;
    TEST_ASSERT_EQ(keel_derive_replay_state(&sf), KEEL_REPLAY_WAITING);
    TEST_END();
}

static void test_derive_replay_rfq_pending(void)
{
    TEST_BEGIN("derive replay — RFQ pending");
    keel_session_flow_t sf = make_sf();
    sf.stmt_replay_len = 100;
    sf.stmt_replay_rfq_pending = true;
    TEST_ASSERT_EQ(keel_derive_replay_state(&sf), KEEL_REPLAY_RFQ_PENDING);
    TEST_END();
}

static void test_derive_replay_sending(void)
{
    TEST_BEGIN("derive replay — sending");
    keel_session_flow_t sf = make_sf();
    sf.stmt_replay_len = 100;
    TEST_ASSERT_EQ(keel_derive_replay_state(&sf), KEEL_REPLAY_SENDING);
    TEST_END();
}

/* ============================================================================
 * §4 — Derived CID State Tests
 * ============================================================================ */

static void test_derive_cid_none(void)
{
    TEST_BEGIN("derive CID — none (no tracking)");
    keel_session_flow_t sf = make_sf();
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_NONE);
    TEST_END();
}

static void test_derive_cid_tracking(void)
{
    TEST_BEGIN("derive CID — tracking active");
    keel_session_flow_t sf = make_sf();
    sf.txn_tracking = true;
    sf.tx = KEEL_TX_ACTIVE;
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_TRACKING);
    TEST_END();
}

static void test_derive_cid_xid_captured(void)
{
    TEST_BEGIN("derive CID — XID captured");
    keel_session_flow_t sf = make_sf();
    sf.txn_tracking = true;
    sf.commit_in_flight = true;
    sf.pending_commit_xid = 12345;
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_XID_CAPTURED);
    TEST_END();
}

static void test_derive_cid_commit_sent(void)
{
    TEST_BEGIN("derive CID — commit sent (no XID)");
    keel_session_flow_t sf = make_sf();
    sf.txn_tracking = true;
    sf.commit_in_flight = true;
    sf.pending_commit_xid = 0;
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_COMMIT_SENT);
    TEST_END();
}

static void test_derive_cid_backend_lost(void)
{
    TEST_BEGIN("derive CID — backend lost");
    keel_session_flow_t sf = make_sf();
    sf.txn_tracking = true;
    sf.commit_in_doubt = true;
    sf.indoubt_xid = 12345;
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_BACKEND_LOST);
    TEST_END();
}

static void test_derive_cid_check_sent(void)
{
    TEST_BEGIN("derive CID — check sent");
    keel_session_flow_t sf = make_sf();
    backend_conn_t check = make_backend();
    sf.txn_tracking = true;
    sf.commit_in_doubt = true;
    sf.xid_check_conn = &check;
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_CHECK_SENT);
    TEST_END();
}

static void test_derive_cid_resolved_committed(void)
{
    TEST_BEGIN("derive CID — resolved committed");
    keel_session_flow_t sf = make_sf();
    sf.txn_tracking = true;
    sf.indoubt_check_result = 1;
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_RESOLVED_COMMITTED);
    TEST_END();
}

static void test_derive_cid_resolved_aborted(void)
{
    TEST_BEGIN("derive CID — resolved aborted");
    keel_session_flow_t sf = make_sf();
    sf.txn_tracking = true;
    sf.indoubt_check_result = 2;
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_RESOLVED_ABORTED);
    TEST_END();
}

/* ============================================================================
 * §5 — Contract Sync Tests
 * ============================================================================ */

static void test_contract_sync_idle(void)
{
    TEST_BEGIN("contract sync — idle session");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    keel_engine_state_t es = KEEL_ENGINE_STATE_ACTIVE;

    keel_session_contract_t c = keel_session_contract_sync(&sf, &s, &es);

    TEST_ASSERT_EQ(c.phase, KEEL_PHASE_READY);
    TEST_ASSERT_EQ(c.binding, KEEL_BIND_UNBOUND);
    TEST_ASSERT_EQ(c.pins, KEEL_FPIN_NONE);
    TEST_ASSERT_EQ(c.tx, KEEL_TX_IDLE);
    TEST_ASSERT_EQ(c.replay, KEEL_REPLAY_NONE);
    TEST_ASSERT_EQ(c.cid, KEEL_CID_NONE);
    TEST_ASSERT_EQ(c.engine_state, KEEL_ENGINE_STATE_ACTIVE);
    TEST_END();
}

static void test_contract_sync_query_with_txn(void)
{
    TEST_BEGIN("contract sync — query with transaction");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_engine_state_t es = KEEL_ENGINE_STATE_ACTIVE;

    sf.phase = KEEL_PHASE_QUERY;
    sf.tx = KEEL_TX_ACTIVE;
    sf.pins = KEEL_FPIN_TRANSACTION;
    s.backend_conn = &be;
    s.in_transaction = true;

    keel_session_contract_t c = keel_session_contract_sync(&sf, &s, &es);

    TEST_ASSERT_EQ(c.phase, KEEL_PHASE_QUERY);
    TEST_ASSERT_EQ(c.binding, KEEL_BIND_PINNED_TXN);
    TEST_ASSERT(c.pins & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT_EQ(c.tx, KEEL_TX_ACTIVE);
    TEST_END();
}

static void test_contract_sync_null_engine_state(void)
{
    TEST_BEGIN("contract sync — NULL engine state defaults to CREATED");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();

    keel_session_contract_t c = keel_session_contract_sync(&sf, &s, NULL);

    TEST_ASSERT_EQ(c.engine_state, KEEL_ENGINE_STATE_CREATED);
    TEST_END();
}

static void test_backend_contract_sync(void)
{
    TEST_BEGIN("backend contract sync");
    backend_conn_t be = make_backend();
    keel_session_t s = make_session();
    be.pinned_session = &s;
    be.stmt_set_hash = 0xDEAD;
    be.needs_sync = true;
    be.in_transaction = true;
    atomic_store(&be.state, BACKEND_CONN_ACTIVE);

    keel_backend_contract_t bc = keel_backend_contract_sync(&be);

    TEST_ASSERT_EQ(bc.conn_state, BACKEND_CONN_ACTIVE);
    TEST_ASSERT(bc.has_owner);
    TEST_ASSERT(bc.has_stmts);
    TEST_ASSERT(bc.needs_sync);
    TEST_ASSERT(bc.in_transaction);
    TEST_END();
}

static void test_backend_contract_sync_null(void)
{
    TEST_BEGIN("backend contract sync — NULL conn");
    keel_backend_contract_t bc = keel_backend_contract_sync(NULL);

    TEST_ASSERT_EQ(bc.conn_state, BACKEND_CONN_CLOSED);
    TEST_ASSERT(!bc.has_owner);
    TEST_ASSERT(!bc.has_stmts);
    TEST_END();
}

/* ============================================================================
 * §6 — Phase Transition Tests
 * ============================================================================ */

static void test_phase_transitions_legal(void)
{
    TEST_BEGIN("phase transitions — legal paths");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    keel_state_journal_t j;
    keel_journal_init(&j);

    sf.phase = KEEL_PHASE_HANDSHAKE_AUTH;
    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, &j), 0);
    TEST_ASSERT_EQ(sf.phase, KEEL_PHASE_READY);

    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_QUERY, &j), 0);
    TEST_ASSERT_EQ(sf.phase, KEEL_PHASE_QUERY);

    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_BACKEND_SYNC, &j), 0);
    TEST_ASSERT_EQ(sf.phase, KEEL_PHASE_BACKEND_SYNC);

    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_QUERY, &j), 0);
    TEST_ASSERT_EQ(sf.phase, KEEL_PHASE_QUERY);

    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, &j), 0);
    TEST_ASSERT_EQ(sf.phase, KEEL_PHASE_READY);

    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_CLOSING, &j), 0);
    TEST_ASSERT_EQ(sf.phase, KEEL_PHASE_CLOSING);

#ifndef NDEBUG
    TEST_ASSERT_EQ(j.count, 6u);
#endif
    TEST_END();
}

static void test_phase_transitions_illegal(void)
{
    TEST_BEGIN("phase transitions — illegal paths blocked");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();

    /* HANDSHAKE → QUERY (must go through READY) */
    sf.phase = KEEL_PHASE_HANDSHAKE_AUTH;
    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_QUERY, NULL), -1);
    TEST_ASSERT_EQ(sf.phase, KEEL_PHASE_HANDSHAKE_AUTH);

    /* READY → HANDSHAKE (can't go backwards) */
    sf.phase = KEEL_PHASE_READY;
    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_HANDSHAKE_AUTH, NULL), -1);

    /* BACKEND_SYNC → READY (must go through QUERY) */
    sf.phase = KEEL_PHASE_BACKEND_SYNC;
    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, NULL), -1);

    /* CLOSING → READY (terminal state) */
    sf.phase = KEEL_PHASE_CLOSING;
    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, NULL), -1);

    TEST_END();
}

static void test_phase_transition_noop(void)
{
    TEST_BEGIN("phase transition — same phase is no-op");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    sf.phase = KEEL_PHASE_READY;
    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, NULL), 0);
    TEST_END();
}

/* ============================================================================
 * §7 — Bind/Unbind Transition Tests
 * ============================================================================ */

static void test_bind_shared(void)
{
    TEST_BEGIN("bind — shared");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_state_journal_t j;
    keel_journal_init(&j);

    TEST_ASSERT_EQ(keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, &j), 0);
    TEST_ASSERT_EQ(s.backend_conn, &be);
    TEST_ASSERT_EQ(be.pinned_session, &s);
    TEST_ASSERT_EQ(atomic_load(&be.state), BACKEND_CONN_ACTIVE);
#ifndef NDEBUG
    TEST_ASSERT_EQ(j.count, 1u);
#endif
    TEST_END();
}

static void test_bind_txn_pinned(void)
{
    TEST_BEGIN("bind — transaction pinned");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();

    TEST_ASSERT_EQ(keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_PINNED_TXN, NULL), 0);
    TEST_ASSERT(s.in_transaction);
    TEST_ASSERT(be.in_transaction);
    TEST_ASSERT(sf.pins & KEEL_FPIN_TRANSACTION);
    TEST_END();
}

static void test_bind_fails_when_already_bound(void)
{
    TEST_BEGIN("bind — fails if already bound");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be1 = make_backend();
    backend_conn_t be2 = make_backend();

    TEST_ASSERT_EQ(keel_session_transition_bind(&sf, &s, &be1, KEEL_BIND_SHARED, NULL), 0);
    TEST_ASSERT_EQ(keel_session_transition_bind(&sf, &s, &be2, KEEL_BIND_SHARED, NULL), -1);
    TEST_END();
}

static void test_unbind(void)
{
    TEST_BEGIN("unbind — returns to UNBOUND");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_state_journal_t j;
    keel_journal_init(&j);

    keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, &j);
    TEST_ASSERT_EQ(keel_session_transition_unbind(&sf, &s, &j), 0);
    TEST_ASSERT_NULL(s.backend_conn);
    TEST_ASSERT_NULL(be.pinned_session);
    TEST_ASSERT(!s.in_transaction);
    TEST_ASSERT(!s.hard_pinned);
    TEST_ASSERT_EQ(keel_derive_binding(&sf, &s), KEEL_BIND_UNBOUND);
#ifndef NDEBUG
    TEST_ASSERT_EQ(j.count, 2u);
#endif
    TEST_END();
}

static void test_unbind_noop_when_unbound(void)
{
    TEST_BEGIN("unbind — no-op when already unbound");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    TEST_ASSERT_EQ(keel_session_transition_unbind(&sf, &s, NULL), 0);
    TEST_END();
}

/* ============================================================================
 * §8 — Transaction Transition Tests
 * ============================================================================ */

static void test_begin_txn(void)
{
    TEST_BEGIN("begin txn — happy path");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_state_journal_t j;
    keel_journal_init(&j);

    keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, &j);
    TEST_ASSERT_EQ(keel_session_transition_begin_txn(&sf, &s, &j), 0);
    TEST_ASSERT_EQ(sf.tx, KEEL_TX_ACTIVE);
    TEST_ASSERT(sf.pins & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT(s.in_transaction);
    TEST_ASSERT(be.in_transaction);
    TEST_END();
}

static void test_begin_txn_fails_unbound(void)
{
    TEST_BEGIN("begin txn — fails when unbound");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    TEST_ASSERT_EQ(keel_session_transition_begin_txn(&sf, &s, NULL), -1);
    TEST_ASSERT_EQ(sf.tx, KEEL_TX_IDLE);
    TEST_END();
}

static void test_begin_txn_fails_not_idle(void)
{
    TEST_BEGIN("begin txn — fails when tx not IDLE");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, NULL);
    sf.tx = KEEL_TX_ACTIVE;
    TEST_ASSERT_EQ(keel_session_transition_begin_txn(&sf, &s, NULL), -1);
    TEST_END();
}

static void test_end_txn(void)
{
    TEST_BEGIN("end txn — commit to IDLE");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_state_journal_t j;
    keel_journal_init(&j);

    keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, &j);
    keel_session_transition_begin_txn(&sf, &s, &j);
    TEST_ASSERT_EQ(keel_session_transition_end_txn(&sf, &s, KEEL_TX_IDLE, &j), 0);
    TEST_ASSERT_EQ(sf.tx, KEEL_TX_IDLE);
    TEST_ASSERT(!(sf.pins & KEEL_FPIN_TRANSACTION));
    TEST_ASSERT(!s.in_transaction);
    TEST_ASSERT(!be.in_transaction);
    TEST_END();
}

static void test_end_txn_fails_when_idle(void)
{
    TEST_BEGIN("end txn — fails when already IDLE");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    TEST_ASSERT_EQ(keel_session_transition_end_txn(&sf, &s, KEEL_TX_IDLE, NULL), -1);
    TEST_END();
}

/* ============================================================================
 * §9 — Replay Transition Tests
 * ============================================================================ */

static void test_replay_transitions_legal(void)
{
    TEST_BEGIN("replay transitions — legal forward path");
    keel_session_flow_t sf = make_sf();
    keel_state_journal_t j;
    keel_journal_init(&j);

    /* NONE → SENDING */
    TEST_ASSERT_EQ(keel_session_transition_replay(&sf, KEEL_REPLAY_SENDING, &j), 0);

    /* Also test NONE → DISCARD_PENDING */
    keel_session_flow_t sf2 = make_sf();
    TEST_ASSERT_EQ(keel_session_transition_replay(&sf2, KEEL_REPLAY_DISCARD_PENDING, NULL), 0);
    TEST_END();
}

static void test_replay_transitions_illegal(void)
{
    TEST_BEGIN("replay transitions — illegal paths blocked");
    keel_session_flow_t sf = make_sf();

    /* NONE → WAITING (must go through SENDING first) */
    TEST_ASSERT_EQ(keel_session_transition_replay(&sf, KEEL_REPLAY_WAITING, NULL), -1);

    /* NONE → RFQ_PENDING (illegal) */
    TEST_ASSERT_EQ(keel_session_transition_replay(&sf, KEEL_REPLAY_RFQ_PENDING, NULL), -1);
    TEST_END();
}

/* ============================================================================
 * §10 — CID Transition Tests
 * ============================================================================ */

static void test_cid_happy_path(void)
{
    TEST_BEGIN("CID transitions — happy path (commit succeeds)");
    keel_session_flow_t sf = make_sf();
    sf.txn_tracking = true;
    sf.tx = KEEL_TX_ACTIVE;
    keel_state_journal_t j;
    keel_journal_init(&j);

    /* NONE → TRACKING */
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_TRACKING);
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_TRACKING, &j), 0);

    /* TRACKING → XID_CAPTURED */
    sf.commit_in_flight = true;
    sf.pending_commit_xid = 12345;
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_XID_CAPTURED, &j), 0);

    /* XID_CAPTURED → COMMIT_SENT */
    sf.pending_commit_xid = 0;
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_COMMIT_SENT, &j), 0);

    /* COMMIT_SENT → NONE (happy path — commit succeeded) */
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_NONE, &j), 0);
    TEST_ASSERT(!sf.commit_in_doubt);
    TEST_ASSERT(!sf.commit_in_flight);
    TEST_END();
}

static void test_cid_doubt_path(void)
{
    TEST_BEGIN("CID transitions — doubt path (backend lost)");
    keel_session_flow_t sf = make_sf();
    sf.txn_tracking = true;
    sf.commit_in_flight = true;
    sf.pending_commit_xid = 0; /* COMMIT_SENT state */

    /* COMMIT_SENT → BACKEND_LOST */
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_BACKEND_LOST, NULL), 0);
    TEST_ASSERT(sf.commit_in_doubt);

    /* BACKEND_LOST → CHECK_BORROWING */
    sf.indoubt_xid = 12345;
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_CHECK_BORROWING, NULL), 0);

    /* CHECK_BORROWING → CHECK_SENT */
    backend_conn_t check = make_backend();
    sf.xid_check_conn = &check;
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_CHECK_SENT, NULL), 0);

    /* CHECK_SENT → RESOLVED_COMMITTED
     * Note: don't set indoubt_check_result before the transition — it would
     * change the derived state to RESOLVED_COMMITTED prematurely (the derive
     * function checks indoubt_check_result first). The transition function
     * itself clears commit_in_doubt when target >= RESOLVED_COMMITTED. */
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_RESOLVED_COMMITTED, NULL), 0);
    TEST_ASSERT(!sf.commit_in_doubt);
    TEST_ASSERT(!sf.commit_in_flight);
    TEST_END();
}

static void test_cid_illegal_transition(void)
{
    TEST_BEGIN("CID transitions — illegal paths blocked");
    keel_session_flow_t sf = make_sf();
    sf.txn_tracking = true;

    /* NONE → BACKEND_LOST (can't skip COMMIT_SENT) */
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_BACKEND_LOST, NULL), -1);

    /* NONE → RESOLVED_COMMITTED (can't skip) */
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_RESOLVED_COMMITTED, NULL), -1);
    TEST_END();
}

/* ============================================================================
 * §11 — Hard Pin + Quarantine Tests
 * ============================================================================ */

static void test_hard_pin(void)
{
    TEST_BEGIN("hard pin — upgrades binding");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_state_journal_t j;
    keel_journal_init(&j);

    keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, &j);
    TEST_ASSERT_EQ(keel_session_transition_hard_pin(&sf, &s, &j), 0);
    TEST_ASSERT(s.hard_pinned);
    TEST_ASSERT(be.hard_pinned);
    TEST_ASSERT_EQ(keel_derive_binding(&sf, &s), KEEL_BIND_HARD_PINNED);
    TEST_END();
}

static void test_hard_pin_fails_unbound(void)
{
    TEST_BEGIN("hard pin — fails when unbound");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    TEST_ASSERT_EQ(keel_session_transition_hard_pin(&sf, &s, NULL), -1);
    TEST_END();
}

static void test_quarantine_backend(void)
{
    TEST_BEGIN("quarantine — sets CLEANING state");
    backend_conn_t be = make_backend();
    atomic_store(&be.state, BACKEND_CONN_ACTIVE);
    keel_state_journal_t j;
    keel_journal_init(&j);

    TEST_ASSERT_EQ(keel_backend_transition_quarantine(&be, KEEL_QUARANTINE_PROTOCOL_DESYNC, &j), 0);
    TEST_ASSERT_EQ(atomic_load(&be.state), BACKEND_CONN_CLEANING);
#ifndef NDEBUG
    TEST_ASSERT_EQ(j.count, 1u);
#endif
    TEST_END();
}

static void test_quarantine_none_fails(void)
{
    TEST_BEGIN("quarantine — NONE reason is rejected");
    backend_conn_t be = make_backend();
    TEST_ASSERT_EQ(keel_backend_transition_quarantine(&be, KEEL_QUARANTINE_NONE, NULL), -1);
    TEST_END();
}

/* ============================================================================
 * §12 — Derived Predicate Tests
 * ============================================================================ */

static void test_can_force_close_idle(void)
{
    TEST_BEGIN("can force close — idle session");
    keel_session_contract_t c = {0};
    c.cid = KEEL_CID_NONE;
    TEST_ASSERT(keel_session_can_force_close(&c));
    TEST_END();
}

static void test_cannot_force_close_cid_doubt(void)
{
    TEST_BEGIN("cannot force close — CID in doubt");
    keel_session_contract_t c = {0};

    c.cid = KEEL_CID_BACKEND_LOST;
    TEST_ASSERT(!keel_session_can_force_close(&c));

    c.cid = KEEL_CID_CHECK_BORROWING;
    TEST_ASSERT(!keel_session_can_force_close(&c));

    c.cid = KEEL_CID_CHECK_SENT;
    TEST_ASSERT(!keel_session_can_force_close(&c));

    /* Resolved states CAN be force-closed */
    c.cid = KEEL_CID_RESOLVED_COMMITTED;
    TEST_ASSERT(keel_session_can_force_close(&c));
    TEST_END();
}

static void test_backend_can_return_to_pool(void)
{
    TEST_BEGIN("backend can return to pool");
    backend_conn_t be = make_backend();
    atomic_store(&be.state, BACKEND_CONN_ACTIVE);

    TEST_ASSERT(keel_backend_can_return_to_pool(&be));

    be.in_transaction = true;
    TEST_ASSERT(!keel_backend_can_return_to_pool(&be));

    be.in_transaction = false;
    be.hard_pinned = true;
    TEST_ASSERT(!keel_backend_can_return_to_pool(&be));

    be.hard_pinned = false;
    atomic_store(&be.state, BACKEND_CONN_CLOSED);
    TEST_ASSERT(!keel_backend_can_return_to_pool(&be));

    TEST_ASSERT(!keel_backend_can_return_to_pool(NULL));
    TEST_END();
}

static void test_needs_replay(void)
{
    TEST_BEGIN("session needs replay");
    keel_session_flow_t sf = make_sf();
    backend_conn_t be = make_backend();

    TEST_ASSERT(!keel_session_needs_replay(&sf, &be));

    sf.stmt_replay_len = 100;
    sf.stmt_replay_hash = 0xABCD;
    be.stmt_set_hash = 0xABCD;
    TEST_ASSERT(!keel_session_needs_replay(&sf, &be));

    be.stmt_set_hash = 0x1234;
    TEST_ASSERT(keel_session_needs_replay(&sf, &be));
    TEST_END();
}

static void test_can_splice_basic(void)
{
    TEST_BEGIN("can splice — basic checks");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();

    /* Idle session: can splice */
    TEST_ASSERT(keel_session_can_splice(&sf, &s));

    /* In replay: cannot splice */
    sf.stmt_replay_len = 100;
    TEST_ASSERT(!keel_session_can_splice(&sf, &s));

    /* Reset replay, add forward remaining */
    sf.stmt_replay_len = 0;
    sf.fe_fwd_remaining = 42;
    TEST_ASSERT(!keel_session_can_splice(&sf, &s));

    /* Reset, add copy state */
    sf.fe_fwd_remaining = 0;
    sf.copy_hdr_len = 2;
    TEST_ASSERT(!keel_session_can_splice(&sf, &s));

    /* NULL session: cannot splice */
    TEST_ASSERT(!keel_session_can_splice(&sf, NULL));
    TEST_END();
}

/* ============================================================================
 * §13 — Contract Invariant Check Tests
 * ============================================================================ */

static void test_contract_check_clean_session(void)
{
    TEST_BEGIN("contract check — clean idle session");
    keel_session_flow_t sf = make_sf();
    keel_session_contract_t c = {
        .phase = KEEL_PHASE_READY,
        .binding = KEEL_BIND_UNBOUND,
        .pins = KEEL_FPIN_NONE,
        .tx = KEEL_TX_IDLE,
        .replay = KEEL_REPLAY_NONE,
        .cid = KEEL_CID_NONE,
        .engine_state = KEEL_ENGINE_STATE_ACTIVE,
    };
    TEST_ASSERT_EQ(keel_contract_check_session(&c, &sf), KEEL_CONTRACT_OK);
    TEST_END();
}

static void test_contract_check_txn_pin_no_tx(void)
{
    TEST_BEGIN("contract violation — TXN pin without active TX");
    keel_session_flow_t sf = make_sf();
    keel_session_contract_t c = {
        .phase = KEEL_PHASE_QUERY,
        .binding = KEEL_BIND_PINNED_TXN,
        .tx = KEEL_TX_IDLE,
    };
    uint32_t v = keel_contract_check_session(&c, &sf);
    TEST_ASSERT(v & KEEL_CONTRACT_TXN_PIN_NO_TX);
    TEST_END();
}

static void test_contract_check_tx_active_unbound(void)
{
    TEST_BEGIN("contract violation — TX active but unbound");
    keel_session_flow_t sf = make_sf();
    keel_session_contract_t c = {
        .phase = KEEL_PHASE_QUERY,
        .binding = KEEL_BIND_UNBOUND,
        .tx = KEEL_TX_ACTIVE,
    };
    uint32_t v = keel_contract_check_session(&c, &sf);
    TEST_ASSERT(v & KEEL_CONTRACT_TX_ACTIVE_UNBOUND);
    TEST_END();
}

static void test_contract_check_replay_unbound(void)
{
    TEST_BEGIN("contract violation — replay active but unbound");
    keel_session_flow_t sf = make_sf();
    keel_session_contract_t c = {
        .phase = KEEL_PHASE_QUERY,
        .binding = KEEL_BIND_UNBOUND,
        .replay = KEEL_REPLAY_WAITING,
    };
    uint32_t v = keel_contract_check_session(&c, &sf);
    TEST_ASSERT(v & KEEL_CONTRACT_REPLAY_UNBOUND);
    TEST_END();
}

static void test_contract_check_query_unbound(void)
{
    TEST_BEGIN("contract violation — QUERY phase but unbound");
    keel_session_flow_t sf = make_sf();
    keel_session_contract_t c = {
        .phase = KEEL_PHASE_QUERY,
        .binding = KEEL_BIND_UNBOUND,
        .tx = KEEL_TX_IDLE,
    };
    uint32_t v = keel_contract_check_session(&c, &sf);
    TEST_ASSERT(v & KEEL_CONTRACT_QUERY_UNBOUND);
    TEST_END();
}

static void test_contract_check_backend_idle_with_owner(void)
{
    TEST_BEGIN("backend contract violation — IDLE with owner");
    keel_backend_contract_t c = {
        .conn_state = BACKEND_CONN_IDLE,
        .has_owner = true,
    };
    uint32_t v = keel_contract_check_backend(&c);
    TEST_ASSERT(v & KEEL_CONTRACT_IDLE_WITH_OWNER);
    TEST_END();
}

static void test_contract_check_backend_active_no_owner(void)
{
    TEST_BEGIN("backend contract violation — ACTIVE without owner");
    keel_backend_contract_t c = {
        .conn_state = BACKEND_CONN_ACTIVE,
        .has_owner = false,
    };
    uint32_t v = keel_contract_check_backend(&c);
    TEST_ASSERT(v & KEEL_CONTRACT_ACTIVE_NO_OWNER);
    TEST_END();
}

/* ============================================================================
 * §14 — Event Journal Tests
 * ============================================================================ */

static void test_journal_init(void)
{
    TEST_BEGIN("journal init — zeroed");
    keel_state_journal_t j;
    keel_journal_init(&j);
    TEST_ASSERT_EQ(j.head, 0u);
    TEST_ASSERT_EQ(j.count, 0u);
    TEST_END();
}

static void test_journal_record_and_wrap(void)
{
    TEST_BEGIN("journal record + wrap");
    keel_state_journal_t j;
    keel_journal_init(&j);

    for (uint32_t i = 0; i < 65; i++) {
        keel_journal_record(&j, i, KEEL_DOMAIN_PHASE, 0, 1, (uint8_t)i);
    }

#ifndef NDEBUG
    TEST_ASSERT_EQ(j.count, 65u);
    TEST_ASSERT_EQ(j.head, 65u);

    uint32_t last_idx = (j.head - 1) & KEEL_STATE_JOURNAL_MASK;
    TEST_ASSERT_EQ(j.events[last_idx].entity_id, 64u);
    TEST_ASSERT_EQ(j.events[last_idx].trigger, 64u);
#endif
    TEST_END();
}

static void test_journal_dump_no_crash(void)
{
    TEST_BEGIN("journal dump — no crash");
    keel_state_journal_t j;
    keel_journal_init(&j);

    keel_journal_dump(&j, "empty");

    keel_journal_record(&j, 1, KEEL_DOMAIN_BINDING, 0, 2, 0);
    keel_journal_record(&j, 1, KEEL_DOMAIN_TX, 0, 1, 0);
    keel_journal_dump(&j, "two-events");

    TEST_ASSERT(true);
    TEST_END();
}

/* ============================================================================
 * §15 — Full Lifecycle Integration Test
 * ============================================================================ */

static void test_full_session_lifecycle(void)
{
    TEST_BEGIN("full session lifecycle — handshake to close");
    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_engine_state_t es = KEEL_ENGINE_STATE_ACTIVE;
    keel_state_journal_t j;
    keel_journal_init(&j);

    sf.phase = KEEL_PHASE_HANDSHAKE_AUTH;

    /* 1. Auth complete → READY */
    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, &j), 0);

    /* 2. Query arrives → QUERY */
    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_QUERY, &j), 0);

    /* 3. Borrow backend */
    TEST_ASSERT_EQ(keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, &j), 0);

    /* 4. Verify contract */
    keel_session_contract_t c = keel_session_contract_sync(&sf, &s, &es);
    TEST_ASSERT_EQ(c.phase, KEEL_PHASE_QUERY);
    TEST_ASSERT_EQ(c.binding, KEEL_BIND_SHARED);
    TEST_ASSERT_EQ(c.tx, KEEL_TX_IDLE);

    /* 5. BEGIN → txn */
    TEST_ASSERT_EQ(keel_session_transition_begin_txn(&sf, &s, &j), 0);
    c = keel_session_contract_sync(&sf, &s, &es);
    TEST_ASSERT_EQ(c.binding, KEEL_BIND_PINNED_TXN);

    /* 6. COMMIT → end txn */
    TEST_ASSERT_EQ(keel_session_transition_end_txn(&sf, &s, KEEL_TX_IDLE, &j), 0);
    c = keel_session_contract_sync(&sf, &s, &es);
    TEST_ASSERT_EQ(c.binding, KEEL_BIND_SHARED);
    TEST_ASSERT_EQ(c.tx, KEEL_TX_IDLE);

    /* 7. Return backend */
    TEST_ASSERT_EQ(keel_session_transition_unbind(&sf, &s, &j), 0);

    /* 8. Back to READY */
    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, &j), 0);

    /* 9. Close */
    TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_CLOSING, &j), 0);

    /* Final contract must be clean */
    c = keel_session_contract_sync(&sf, &s, &es);
    TEST_ASSERT_EQ(c.phase, KEEL_PHASE_CLOSING);
    TEST_ASSERT_EQ(c.binding, KEEL_BIND_UNBOUND);
    TEST_ASSERT_EQ(keel_contract_check_session(&c, &sf), KEEL_CONTRACT_OK);

#ifndef NDEBUG
    TEST_ASSERT(j.count >= 8);
#endif
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    printf("=== KEEL State Contract Tests ===\n\n");

    /* §1: String conversions */
    test_binding_names();
    test_replay_state_names();
    test_cid_state_names();
    test_quarantine_names();
    test_domain_names();

    /* §2: Derived binding */
    test_derive_binding_unbound();
    test_derive_binding_borrow_pending();
    test_derive_binding_shared();
    test_derive_binding_txn_pinned();
    test_derive_binding_hard_pinned();
    test_derive_binding_ps_pinned();
    test_derive_binding_state_pinned();
    test_derive_binding_cid_check();
    test_derive_binding_null_session();

    /* §3: Derived replay state */
    test_derive_replay_none();
    test_derive_replay_discard_pending();
    test_derive_replay_waiting();
    test_derive_replay_rfq_pending();
    test_derive_replay_sending();

    /* §4: Derived CID state */
    test_derive_cid_none();
    test_derive_cid_tracking();
    test_derive_cid_xid_captured();
    test_derive_cid_commit_sent();
    test_derive_cid_backend_lost();
    test_derive_cid_check_sent();
    test_derive_cid_resolved_committed();
    test_derive_cid_resolved_aborted();

    /* §5: Contract sync */
    test_contract_sync_idle();
    test_contract_sync_query_with_txn();
    test_contract_sync_null_engine_state();
    test_backend_contract_sync();
    test_backend_contract_sync_null();

    /* §6: Phase transitions */
    test_phase_transitions_legal();
    test_phase_transitions_illegal();
    test_phase_transition_noop();

    /* §7: Bind/unbind */
    test_bind_shared();
    test_bind_txn_pinned();
    test_bind_fails_when_already_bound();
    test_unbind();
    test_unbind_noop_when_unbound();

    /* §8: Transaction transitions */
    test_begin_txn();
    test_begin_txn_fails_unbound();
    test_begin_txn_fails_not_idle();
    test_end_txn();
    test_end_txn_fails_when_idle();

    /* §9: Replay transitions */
    test_replay_transitions_legal();
    test_replay_transitions_illegal();

    /* §10: CID transitions */
    test_cid_happy_path();
    test_cid_doubt_path();
    test_cid_illegal_transition();

    /* §11: Hard pin + quarantine */
    test_hard_pin();
    test_hard_pin_fails_unbound();
    test_quarantine_backend();
    test_quarantine_none_fails();

    /* §12: Derived predicates */
    test_can_force_close_idle();
    test_cannot_force_close_cid_doubt();
    test_backend_can_return_to_pool();
    test_needs_replay();
    test_can_splice_basic();

    /* §13: Contract invariant checks */
    test_contract_check_clean_session();
    test_contract_check_txn_pin_no_tx();
    test_contract_check_tx_active_unbound();
    test_contract_check_replay_unbound();
    test_contract_check_query_unbound();
    test_contract_check_backend_idle_with_owner();
    test_contract_check_backend_active_no_owner();

    /* §14: Journal */
    test_journal_init();
    test_journal_record_and_wrap();
    test_journal_dump_no_crash();

    /* §15: Integration */
    test_full_session_lifecycle();

    return test_summary();
}
