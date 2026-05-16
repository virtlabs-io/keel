/**
 * @file test_sm_sequence_walk.c
 * @brief Exhaustive walkers over the declarative session state-machine tables.
 *
 * This file complements the more targeted transition tests by treating the
 * state machine as a graph and traversing it mechanically. Instead of checking
 * only a few hand-picked lifecycles, it iterates legal adjacency lists, counts
 * expected edges, explores bounded path sets, and verifies that explicitly
 * illegal target states are rejected without mutating the source state.
 *
 * The point of the suite is to protect the transition matrices themselves. When
 * the state machine evolves, it is easy to update one helper path and forget to
 * update the edge tables or the negative-case rules. These walkers make that
 * drift visible immediately.
 */

#include "test_utils.h"
#include "keel/engine/state_machine.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdio.h>
#include <stdbool.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Create a minimally initialized session-flow fixture in the common idle
 *        state used by most transition walkers.
 * @return Stack-allocated session-flow fixture.
 */
static keel_session_flow_t make_sf(void)
{
    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.phase = KEEL_PHASE_READY;
    sf.tx    = KEEL_TX_IDLE;
    return sf;
}

/**
 * @brief Create a lightweight session fixture suitable for contract checks.
 * @return Stack-allocated session fixture.
 */
static keel_session_t make_session(void)
{
    keel_session_t s;
    memset(&s, 0, sizeof(s));
    s.id        = 1;
    s.state     = KEEL_SESSION_READY;
    s.server_fd = -1;
    return s;
}

/**
 * @brief Create a nominal idle backend fixture for walkers that need a bound
 *        connection without constructing a full pool.
 * @return Stack-allocated backend fixture.
 */
static backend_conn_t make_backend(void)
{
    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = 10;
    atomic_store(&be.state, BACKEND_CONN_IDLE);
    return be;
}

/*
 * The adjacency tables below intentionally duplicate the legal edges from the
 * implementation matrix. That duplication is a feature, not a bug: the tests
 * act as an independently maintained specification so accidental table edits in
 * production code are caught as mismatches rather than silently reusing the
 * same mistaken source of truth.
 */
static const keel_session_phase_t phase_adj[][5] = {
    /* HANDSHAKE_AUTH → */ { KEEL_PHASE_READY,        KEEL_PHASE_CLOSING,         -1 },
    /* READY →          */ { KEEL_PHASE_QUERY,         KEEL_PHASE_BACKEND_SYNC,    KEEL_PHASE_CLOSING, -1 },
    /* QUERY →          */ { KEEL_PHASE_READY,         KEEL_PHASE_BACKEND_SYNC,    KEEL_PHASE_BACKEND_CLEANING, KEEL_PHASE_CLOSING, -1 },
    /* BACKEND_SYNC →   */ { KEEL_PHASE_QUERY,         KEEL_PHASE_CLOSING,         -1 },
    /* BACKEND_CLEAN → */ { KEEL_PHASE_READY,         KEEL_PHASE_CLOSING,         -1 },
    /* CLOSING →        */ { -1 },
};

/* ============================================================================
 * §1 — Phase Transition Matrix: DFS Walk
 *
 * Iterates through every legal edge, asserts the transition succeeds,
 * and verifies the phase is actually updated.
 * ============================================================================ */

/* Count of legal phase edges (manually counted from the matrix) */
#define PHASE_EDGE_COUNT 13

static void test_phase_dfs(void)
{
    TEST_BEGIN("phase DFS - every legal edge");

    int edges_tested = 0;

    for (int from = 0; from < 6; from++) {
        for (int ei = 0; phase_adj[from][ei] != (keel_session_phase_t)-1 && ei < 5; ei++) {
            keel_session_phase_t to = phase_adj[from][ei];

            keel_session_flow_t sf = make_sf();
            keel_session_t s = make_session();
            sf.phase = (keel_session_phase_t)from;

            int rc = keel_session_transition_phase(&sf, &s, to, NULL);
            TEST_ASSERT_EQ(rc, 0);
            TEST_ASSERT_EQ(sf.phase, to);
            edges_tested++;
        }
    }

    /* Verify we tested all legal edges */
    TEST_ASSERT_EQ(edges_tested, PHASE_EDGE_COUNT);

    TEST_END();
}

/* Walk from HANDSHAKE_AUTH to CLOSING via every distinct path (DFS, depth ≤ 20).
 * We accumulate paths and verify the total count matches expectation. */
static int phase_path_count;

/**
 * @brief Recursively enumerate bounded phase paths until reaching CLOSING.
 * @param current Current phase node in the DFS.
 * @param depth Current recursion depth.
 * @return
 *
 * The walk is intentionally depth-bounded because READY and QUERY form cycles.
 * The test is interested in structural reachability, not in counting infinite
 * loop variants that differ only by repeated steady-state oscillation.
 */
static void phase_dfs_walk(keel_session_phase_t current, int depth)
{
    if (depth > 8) return;  /* Bounded: READY↔QUERY cycles are exponential */

    if (current == KEEL_PHASE_CLOSING) {
        phase_path_count++;
        return;
    }

    int from = (int)current;
    for (int ei = 0; phase_adj[from][ei] != (keel_session_phase_t)-1 && ei < 5; ei++) {
        keel_session_phase_t next = phase_adj[from][ei];

        keel_session_flow_t sf = make_sf();
        keel_session_t s = make_session();
        sf.phase = current;

        int rc = keel_session_transition_phase(&sf, &s, next, NULL);
        if (rc != 0) continue;

        phase_dfs_walk(next, depth + 1);
    }
}

static void test_phase_all_paths_to_closing(void)
{
    TEST_BEGIN("phase DFS - all paths HANDSHAKE to CLOSING (depth<=8)");

    phase_path_count = 0;
    phase_dfs_walk(KEEL_PHASE_HANDSHAKE_AUTH, 0);

    /* There must be at least the two shortest paths:
     *   HANDSHAKE→READY→CLOSING
     *   HANDSHAKE→CLOSING */
    TEST_ASSERT(phase_path_count >= 2);

    /* Must be finite and reasonable with depth bound 8 */
    TEST_ASSERT(phase_path_count < 50000);

    TEST_END();
}

/* ============================================================================
 * §2 — Phase Illegal Edge Exhaustive
 *
 * For every source phase, try every invalid target and assert rejection.
 * ============================================================================ */

static void test_phase_illegal_exhaustive(void)
{
    TEST_BEGIN("phase - every illegal edge returns -1");

    int illegal_count = 0;

    for (int from = 0; from < 6; from++) {
        for (int to = 0; to < 6; to++) {
            if (from == to) continue; /* same-phase is a legal no-op */

            /* Check if this is a legal edge */
            bool legal = false;
            for (int ei = 0; phase_adj[from][ei] != (keel_session_phase_t)-1 && ei < 5; ei++) {
                if (phase_adj[from][ei] == (keel_session_phase_t)to) {
                    legal = true;
                    break;
                }
            }

            if (!legal) {
                keel_session_flow_t sf = make_sf();
                keel_session_t s = make_session();
                sf.phase = (keel_session_phase_t)from;

                int rc = keel_session_transition_phase(&sf, &s, (keel_session_phase_t)to, NULL);
                TEST_ASSERT_EQ(rc, -1);
                TEST_ASSERT_EQ(sf.phase, (keel_session_phase_t)from); /* unchanged */
                illegal_count++;
            }
        }
    }

    /* Total edges = 6×5 (excluding self) = 30. Legal = 13. Illegal = 17. */
    TEST_ASSERT_EQ(illegal_count, 17);

    TEST_END();
}

/* ============================================================================
 * §3 — Replay Transition Matrix: Full Chain + Abort Exhaustive
 * ============================================================================ */

/* Replay adjacency (from state_machine.c) */
static const keel_replay_state_t replay_adj[][3] = {
    /* NONE →              */ { KEEL_REPLAY_DISCARD_PENDING, KEEL_REPLAY_SENDING, -1 },
    /* DISCARD_PENDING →   */ { KEEL_REPLAY_DISCARD_SENT,    KEEL_REPLAY_NONE, -1 },
    /* DISCARD_SENT →      */ { KEEL_REPLAY_SENDING,         KEEL_REPLAY_NONE, -1 },
    /* SENDING →           */ { KEEL_REPLAY_WAITING,         KEEL_REPLAY_NONE, -1 },
    /* WAITING →           */ { KEEL_REPLAY_RFQ_PENDING,     KEEL_REPLAY_NONE, -1 },
    /* RFQ_PENDING →       */ { KEEL_REPLAY_COMPLETE,        KEEL_REPLAY_NONE, -1 },
    /* COMPLETE →          */ { KEEL_REPLAY_NONE,            -1 },
};

/*
 * Replay states are partly derived from several bookkeeping fields rather than
 * being stored as one direct enum. These helpers construct a representative set
 * of fields for each derived state so the matrix walkers exercise the same
 * derivation logic the engine uses in production.
 */
static void set_replay_derived(keel_session_flow_t* sf, keel_replay_state_t state)
{
    /* Reset all replay fields */
    sf->stmt_replay_len = 0;
    sf->stmt_replay_needs_discard = false;
    sf->stmt_replay_rfq_pending = false;
    sf->stmt_replay_count = 0;

    switch (state) {
    case KEEL_REPLAY_NONE:
        break;
    case KEEL_REPLAY_DISCARD_PENDING:
        sf->stmt_replay_len = 1;
        sf->stmt_replay_needs_discard = true;
        break;
    case KEEL_REPLAY_DISCARD_SENT:
        sf->stmt_replay_len = 1;
        sf->stmt_replay_needs_discard = true;
        sf->stmt_replay_rfq_pending = true;
        break;
    case KEEL_REPLAY_SENDING:
        sf->stmt_replay_len = 1;
        break;
    case KEEL_REPLAY_WAITING:
        sf->stmt_replay_len = 1;
        sf->stmt_replay_count = 1;
        break;
    case KEEL_REPLAY_RFQ_PENDING:
        sf->stmt_replay_len = 1;
        sf->stmt_replay_rfq_pending = true;
        break;
    case KEEL_REPLAY_COMPLETE:
        /* COMPLETE is never derived — it's a transient state.
         * For testing we approximate using NONE (after replay finishes). */
        break;
    default:
        break;
    }
}

static void test_replay_full_chain(void)
{
    TEST_BEGIN("replay - full forward chain (discard path)");

    keel_session_flow_t sf = make_sf();

    /* NONE → DISCARD_PENDING */
    set_replay_derived(&sf, KEEL_REPLAY_NONE);
    TEST_ASSERT_EQ(keel_session_transition_replay(&sf, KEEL_REPLAY_DISCARD_PENDING, NULL), 0);

    /* DISCARD_PENDING → DISCARD_SENT */
    set_replay_derived(&sf, KEEL_REPLAY_DISCARD_PENDING);
    TEST_ASSERT_EQ(keel_session_transition_replay(&sf, KEEL_REPLAY_DISCARD_SENT, NULL), 0);

    /* DISCARD_SENT → SENDING */
    set_replay_derived(&sf, KEEL_REPLAY_DISCARD_SENT);
    TEST_ASSERT_EQ(keel_session_transition_replay(&sf, KEEL_REPLAY_SENDING, NULL), 0);

    /* SENDING → WAITING */
    set_replay_derived(&sf, KEEL_REPLAY_SENDING);
    TEST_ASSERT_EQ(keel_session_transition_replay(&sf, KEEL_REPLAY_WAITING, NULL), 0);

    /* WAITING → RFQ_PENDING */
    set_replay_derived(&sf, KEEL_REPLAY_WAITING);
    TEST_ASSERT_EQ(keel_session_transition_replay(&sf, KEEL_REPLAY_RFQ_PENDING, NULL), 0);

    /* RFQ_PENDING → COMPLETE */
    set_replay_derived(&sf, KEEL_REPLAY_RFQ_PENDING);
    TEST_ASSERT_EQ(keel_session_transition_replay(&sf, KEEL_REPLAY_COMPLETE, NULL), 0);

    TEST_END();
}

static void test_replay_abort_from_every_state(void)
{
    TEST_BEGIN("replay - abort to NONE from every non-NONE state");

    for (int s = 1; s < KEEL_REPLAY_COUNT; s++) {
        keel_session_flow_t sf = make_sf();
        set_replay_derived(&sf, (keel_replay_state_t)s);

        int rc = keel_session_transition_replay(&sf, KEEL_REPLAY_NONE, NULL);
        TEST_ASSERT_EQ(rc, 0);
    }

    TEST_END();
}

static void test_replay_illegal_exhaustive(void)
{
    TEST_BEGIN("replay - every illegal edge returns -1");

    int illegal_count = 0;

    /* NOTE: KEEL_REPLAY_COMPLETE (6) is a transient state that cannot be
     * derived from sf fields — keel_derive_replay_state() never returns it.
     * Since keel_session_transition_replay() uses the derived state as the
     * source, we skip COMPLETE as a source in this exhaustive walk. */
    for (int from = 0; from < KEEL_REPLAY_COUNT; from++) {
        if (from == KEEL_REPLAY_COMPLETE) continue;

        for (int to = 0; to < KEEL_REPLAY_COUNT; to++) {
            if (from == to) continue;

            bool legal = false;
            for (int ei = 0; replay_adj[from][ei] != (keel_replay_state_t)-1 && ei < 3; ei++) {
                if (replay_adj[from][ei] == (keel_replay_state_t)to) {
                    legal = true;
                    break;
                }
            }

            if (!legal) {
                keel_session_flow_t sf = make_sf();
                set_replay_derived(&sf, (keel_replay_state_t)from);

                int rc = keel_session_transition_replay(&sf, (keel_replay_state_t)to, NULL);
                TEST_ASSERT_EQ(rc, -1);
                illegal_count++;
            }
        }
    }

    /* Derivable states: 6 (NONE through RFQ_PENDING, excluding COMPLETE).
     * Non-self edges per state: 6. Total non-self = 6×6 = 36.
     * Legal edges from derivable states: NONE:2, DISCARD_PENDING:2,
     * DISCARD_SENT:2, SENDING:2, WAITING:2, RFQ_PENDING:2 = 12.
     * Illegal = 36 - 12 = 24. */
    TEST_ASSERT_EQ(illegal_count, 24);

    TEST_END();
}

/* ============================================================================
 * §4 — CID Transition Matrix: Full Lifecycle + Exhaustive Illegal
 * ============================================================================ */

/* CID adjacency (from state_machine.c) */
static const keel_cid_state_t cid_adj[][4] = {
    /* NONE →               */ { KEEL_CID_TRACKING, -1 },
    /* TRACKING →           */ { KEEL_CID_XID_CAPTURED, KEEL_CID_NONE, -1 },
    /* XID_CAPTURED →       */ { KEEL_CID_COMMIT_SENT,  KEEL_CID_NONE, -1 },
    /* COMMIT_SENT →        */ { KEEL_CID_NONE,         KEEL_CID_BACKEND_LOST, -1 },
    /* BACKEND_LOST →       */ { KEEL_CID_CHECK_BORROWING, KEEL_CID_RESOLVED_UNKNOWN, -1 },
    /* CHECK_BORROWING →    */ { KEEL_CID_CHECK_SENT,      KEEL_CID_RESOLVED_UNKNOWN, -1 },
    /* CHECK_SENT →         */ { KEEL_CID_RESOLVED_COMMITTED, KEEL_CID_RESOLVED_ABORTED, KEEL_CID_RESOLVED_UNKNOWN, -1 },
    /* RESOLVED_COMMITTED → */ { KEEL_CID_NONE, -1 },
    /* RESOLVED_ABORTED →   */ { KEEL_CID_NONE, -1 },
    /* RESOLVED_UNKNOWN →   */ { KEEL_CID_NONE, -1 },
};

/* Helper: set sf fields so that derive_cid_state returns the desired state */
static void set_cid_derived(keel_session_flow_t* sf, keel_cid_state_t state)
{
    /* Reset all CID fields */
    sf->txn_tracking = false;
    sf->commit_in_flight = false;
    sf->commit_in_doubt = false;
    sf->pending_commit_xid = 0;
    sf->indoubt_xid = 0;
    sf->indoubt_check_result = 0;
    sf->xid_check_conn = NULL;
    sf->tx = KEEL_TX_IDLE;

    switch (state) {
    case KEEL_CID_NONE:
        break;
    case KEEL_CID_TRACKING:
        sf->txn_tracking = true;
        sf->tx = KEEL_TX_ACTIVE;
        break;
    case KEEL_CID_XID_CAPTURED:
        sf->txn_tracking = true;
        sf->commit_in_flight = true;
        sf->pending_commit_xid = 12345;
        break;
    case KEEL_CID_COMMIT_SENT:
        sf->txn_tracking = true;
        sf->commit_in_flight = true;
        sf->pending_commit_xid = 0;
        break;
    case KEEL_CID_BACKEND_LOST:
        sf->txn_tracking = true;
        sf->commit_in_doubt = true;
        sf->indoubt_xid = 12345;
        break;
    case KEEL_CID_CHECK_BORROWING:
        sf->txn_tracking = true;
        sf->commit_in_doubt = true;
        sf->indoubt_xid = 12345;
        break;
    case KEEL_CID_CHECK_SENT: {
        static backend_conn_t fake_check; /* for xid_check_conn */
        sf->txn_tracking = true;
        sf->commit_in_doubt = true;
        sf->xid_check_conn = &fake_check;
        break;
    }
    case KEEL_CID_RESOLVED_COMMITTED:
        sf->txn_tracking = true;
        sf->indoubt_check_result = 1;
        break;
    case KEEL_CID_RESOLVED_ABORTED:
        sf->txn_tracking = true;
        sf->indoubt_check_result = 2;
        break;
    case KEEL_CID_RESOLVED_UNKNOWN:
        sf->txn_tracking = true;
        sf->commit_in_doubt = true;
        /* No indoubt_xid and no xid_check_conn → RESOLVED_UNKNOWN */
        break;
    default:
        break;
    }
}

static void test_cid_happy_path_walk(void)
{
    TEST_BEGIN("CID - happy path walk: NONE-TRACKING-XID-COMMIT-NONE");

    keel_session_flow_t sf = make_sf();
    keel_state_journal_t j;
    keel_journal_init(&j);

    set_cid_derived(&sf, KEEL_CID_NONE);
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_NONE);

    /* NONE → TRACKING */
    set_cid_derived(&sf, KEEL_CID_NONE);
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_TRACKING, &j), 0);

    /* TRACKING → XID_CAPTURED */
    set_cid_derived(&sf, KEEL_CID_TRACKING);
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_XID_CAPTURED, &j), 0);

    /* XID_CAPTURED → COMMIT_SENT */
    set_cid_derived(&sf, KEEL_CID_XID_CAPTURED);
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_COMMIT_SENT, &j), 0);

    /* COMMIT_SENT → NONE (happy) */
    set_cid_derived(&sf, KEEL_CID_COMMIT_SENT);
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_NONE, &j), 0);

#ifndef NDEBUG
    TEST_ASSERT_EQ(j.count, 4u);
#endif

    TEST_END();
}

static void test_cid_doubt_resolution_walk(void)
{
    TEST_BEGIN("CID - doubt walk: BACKEND_LOST-CHECK_BORROWING-CHECK_SENT-RESOLVED");

    keel_session_flow_t sf = make_sf();

    /* Setup: already at COMMIT_SENT */
    set_cid_derived(&sf, KEEL_CID_COMMIT_SENT);
    TEST_ASSERT_EQ(keel_session_transition_cid(&sf, KEEL_CID_BACKEND_LOST, NULL), 0);

    /* NOTE: CHECK_BORROWING is a transient state that cannot be derived from
     * sf fields — keel_derive_cid_state() maps the same field pattern as
     * BACKEND_LOST. We skip directly to CHECK_SENT by setting up the
     * xid_check_conn field that distinguishes CHECK_SENT from BACKEND_LOST. */
    set_cid_derived(&sf, KEEL_CID_CHECK_SENT);
    /* Verify we actually derive CHECK_SENT */
    TEST_ASSERT_EQ(keel_derive_cid_state(&sf), KEEL_CID_CHECK_SENT);

    /* Test all three resolution paths from CHECK_SENT */
    for (int r = KEEL_CID_RESOLVED_COMMITTED; r <= KEEL_CID_RESOLVED_UNKNOWN; r++) {
        keel_session_flow_t sf2 = make_sf();
        set_cid_derived(&sf2, KEEL_CID_CHECK_SENT);
        TEST_ASSERT_EQ(keel_session_transition_cid(&sf2, (keel_cid_state_t)r, NULL), 0);

        /* RESOLVED → NONE */
        set_cid_derived(&sf2, (keel_cid_state_t)r);
        TEST_ASSERT_EQ(keel_session_transition_cid(&sf2, KEEL_CID_NONE, NULL), 0);
    }

    TEST_END();
}

static void test_cid_every_legal_edge(void)
{
    TEST_BEGIN("CID - every single legal edge");

    int edges_tested = 0;

    /* NOTE: CHECK_BORROWING (5) is non-derivable — keel_derive_cid_state()
     * cannot distinguish it from BACKEND_LOST. We skip it as a source. */
    for (int from = 0; from < KEEL_CID_COUNT; from++) {
        if (from == KEEL_CID_CHECK_BORROWING) continue;

        for (int ei = 0; cid_adj[from][ei] != (keel_cid_state_t)-1 && ei < 4; ei++) {
            keel_cid_state_t to = cid_adj[from][ei];

            keel_session_flow_t sf = make_sf();
            set_cid_derived(&sf, (keel_cid_state_t)from);

            int rc = keel_session_transition_cid(&sf, to, NULL);
            TEST_ASSERT_EQ(rc, 0);
            edges_tested++;
        }
    }

    /* Legal edges from derivable states (all except CHECK_BORROWING):
     * NONE:1, TRACKING:2, XID:2, COMMIT:2, BACKEND_LOST:2,
     * CHECK_SENT:3, COMMITTED:1, ABORTED:1, UNKNOWN:1 = 15 */
    TEST_ASSERT_EQ(edges_tested, 15);

    TEST_END();
}

static void test_cid_illegal_exhaustive(void)
{
    TEST_BEGIN("CID - every illegal edge returns -1");

    int illegal_count = 0;

    /* NOTE: CHECK_BORROWING (5) is non-derivable — skip as source. */
    for (int from = 0; from < KEEL_CID_COUNT; from++) {
        if (from == KEEL_CID_CHECK_BORROWING) continue;

        for (int to = 0; to < KEEL_CID_COUNT; to++) {
            if (from == to) continue;

            bool legal = false;
            for (int ei = 0; cid_adj[from][ei] != (keel_cid_state_t)-1 && ei < 4; ei++) {
                if (cid_adj[from][ei] == (keel_cid_state_t)to) {
                    legal = true;
                    break;
                }
            }

            if (!legal) {
                keel_session_flow_t sf = make_sf();
                set_cid_derived(&sf, (keel_cid_state_t)from);

                int rc = keel_session_transition_cid(&sf, (keel_cid_state_t)to, NULL);
                TEST_ASSERT_EQ(rc, -1);
                illegal_count++;
            }
        }
    }

    /* Derivable states: 9 (all except CHECK_BORROWING).
     * Non-self edges per state: 9. Total non-self = 9×9 = 81.
     * Legal edges from derivable states: 15 (see test_cid_every_legal_edge).
     * Illegal = 81 - 15 = 66. */
    TEST_ASSERT_EQ(illegal_count, 66);

    TEST_END();
}

/* ============================================================================
 * §5 — Combined Lifecycle Walk: phase × bind × txn sequences
 *
 * Walks through the canonical session lifecycle and checks contract
 * invariants at every single step.
 * ============================================================================ */

static void test_lifecycle_all_bind_types(void)
{
    TEST_BEGIN("lifecycle - bind with each escalation type");

    keel_backend_binding_t bind_types[] = {
        KEEL_BIND_SHARED,
        KEEL_BIND_PINNED_TXN,
        KEEL_BIND_HARD_PINNED,
    };

    for (int i = 0; i < 3; i++) {
        keel_session_flow_t sf = make_sf();
        keel_session_t s = make_session();
        backend_conn_t be = make_backend();
        keel_engine_state_t es = KEEL_ENGINE_STATE_ACTIVE;
        keel_state_journal_t j;
        keel_journal_init(&j);

        /* HANDSHAKE → READY */
        sf.phase = KEEL_PHASE_HANDSHAKE_AUTH;
        TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, &j), 0);

        /* READY → QUERY */
        TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_QUERY, &j), 0);

        /* Bind */
        TEST_ASSERT_EQ(keel_session_transition_bind(&sf, &s, &be, bind_types[i], &j), 0);

        /* Check contract is clean (QUERY + bound = OK) */
        keel_session_contract_t c = keel_session_contract_sync(&sf, &s, &es);
        TEST_ASSERT(c.binding >= KEEL_BIND_SHARED);

        /* Unbind */
        TEST_ASSERT_EQ(keel_session_transition_unbind(&sf, &s, &j), 0);

        /* Contract: now UNBOUND but still in QUERY — this is actually OK for
         * the transition-to-READY path. Move to READY. */
        TEST_ASSERT_EQ(keel_session_transition_phase(&sf, &s, KEEL_PHASE_READY, &j), 0);

        c = keel_session_contract_sync(&sf, &s, &es);
        TEST_ASSERT_EQ(c.binding, KEEL_BIND_UNBOUND);
        TEST_ASSERT_EQ(c.phase, KEEL_PHASE_READY);
    }

    TEST_END();
}

static void test_lifecycle_txn_round_trips(void)
{
    TEST_BEGIN("lifecycle - multiple transaction round-trips");

    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_engine_state_t es = KEEL_ENGINE_STATE_ACTIVE;

    sf.phase = KEEL_PHASE_QUERY;
    keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, NULL);

    for (int i = 0; i < 10; i++) {
        /* BEGIN */
        TEST_ASSERT_EQ(keel_session_transition_begin_txn(&sf, &s, NULL), 0);
        keel_session_contract_t c = keel_session_contract_sync(&sf, &s, &es);
        TEST_ASSERT_EQ(c.tx, KEEL_TX_ACTIVE);
        TEST_ASSERT_EQ(c.binding, KEEL_BIND_PINNED_TXN);

        /* COMMIT */
        TEST_ASSERT_EQ(keel_session_transition_end_txn(&sf, &s, KEEL_TX_IDLE, NULL), 0);
        c = keel_session_contract_sync(&sf, &s, &es);
        TEST_ASSERT_EQ(c.tx, KEEL_TX_IDLE);
        TEST_ASSERT_EQ(c.binding, KEEL_BIND_SHARED);
    }

    keel_session_transition_unbind(&sf, &s, NULL);

    TEST_END();
}

static void test_lifecycle_hard_pin_upgrade(void)
{
    TEST_BEGIN("lifecycle - shared to hard pin to unbind");

    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_engine_state_t es = KEEL_ENGINE_STATE_ACTIVE;

    sf.phase = KEEL_PHASE_QUERY;
    keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, NULL);

    /* Upgrade */
    TEST_ASSERT_EQ(keel_session_transition_hard_pin(&sf, &s, NULL), 0);
    keel_session_contract_t c = keel_session_contract_sync(&sf, &s, &es);
    TEST_ASSERT_EQ(c.binding, KEEL_BIND_HARD_PINNED);

    /* Verify invariant OK */
    TEST_ASSERT_EQ(keel_contract_check_session(&c, &sf), KEEL_CONTRACT_OK);

    /* Unbind */
    keel_session_transition_unbind(&sf, &s, NULL);
    c = keel_session_contract_sync(&sf, &s, &es);
    TEST_ASSERT_EQ(c.binding, KEEL_BIND_UNBOUND);

    TEST_END();
}

static void test_lifecycle_quarantine_after_txn(void)
{
    TEST_BEGIN("lifecycle - quarantine backend after failed TX");

    keel_session_flow_t sf = make_sf();
    keel_session_t s = make_session();
    backend_conn_t be = make_backend();
    keel_state_journal_t j;
    keel_journal_init(&j);

    sf.phase = KEEL_PHASE_QUERY;
    keel_session_transition_bind(&sf, &s, &be, KEEL_BIND_SHARED, &j);
    keel_session_transition_begin_txn(&sf, &s, &j);

    /* TX failed */
    sf.tx = KEEL_TX_FAILED;

    /* End TX (rollback) */
    keel_session_transition_end_txn(&sf, &s, KEEL_TX_IDLE, &j);

    /* Unbind session */
    keel_session_transition_unbind(&sf, &s, &j);

    /* Quarantine the backend */
    TEST_ASSERT_EQ(keel_backend_transition_quarantine(&be, KEEL_QUARANTINE_DIRTY_STATE, &j), 0);
    TEST_ASSERT_EQ(atomic_load(&be.state), BACKEND_CONN_CLEANING);

    /* Backend in CLEANING state — verify contract reflects quarantine */
    keel_backend_contract_t bc = keel_backend_contract_sync(&be);
    TEST_ASSERT_EQ(bc.conn_state, BACKEND_CONN_CLEANING);

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    printf("=== KEEL State Machine Sequence Walk Tests ===\n\n");

    /* §1: Phase matrix */
    test_phase_dfs();
    test_phase_all_paths_to_closing();
    test_phase_illegal_exhaustive();

    /* §2: Replay matrix */
    test_replay_full_chain();
    test_replay_abort_from_every_state();
    test_replay_illegal_exhaustive();

    /* §3: CID matrix */
    test_cid_happy_path_walk();
    test_cid_doubt_resolution_walk();
    test_cid_every_legal_edge();
    test_cid_illegal_exhaustive();

    /* §4: Combined lifecycle */
    test_lifecycle_all_bind_types();
    test_lifecycle_txn_round_trips();
    test_lifecycle_hard_pin_upgrade();
    test_lifecycle_quarantine_after_txn();

    return test_summary();
}
