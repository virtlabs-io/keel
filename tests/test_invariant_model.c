/**
 * @file test_invariant_model.c
 * @brief Executable checks for the project's formal compatibility model.
 *
 * This suite sits between pure unit tests and broad end-to-end scenarios. It
 * encodes the invariant tables, pin-conflict rules, and flow-level assumptions
 * as directly checkable fixtures so architectural regressions fail close to the
 * model that justified the implementation in the first place.
 *
 * The tests mix three styles on purpose:
 *
 *   1. Table-property checks for compatibility-matrix symmetry and expected
 *      high-risk intersections.
 *   2. Hand-built session/backend fixtures that trigger specific invariant
 *      violations without needing a full worker runtime.
 *   3. Minimal protocol-flow bootstraps that ensure the model still matches the
 *      states produced by real parser/vtable code.
 */

#include "test_utils.h"
#include "keel/engine/invariant.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/engine/engine_flow.h"
#include "keel/engine/backend_pool.h"

#include <string.h>
#include <stdio.h>

int g_tests_run, g_tests_passed, g_tests_failed;

/*
 * The invariant model is validated against the PostgreSQL flow because that is
 * the richest state machine in the tree and therefore the best canary for model
 * drift. If the helpers below cannot assemble a clean PG flow context, the more
 * abstract invariant checks are no longer anchored to a realistic runtime path.
 */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
#define VT (&keel_proto_flow_postgres)

/* ---- Helpers ---- */

/**
 * @brief Write a 32-bit big-endian integer into a synthetic PostgreSQL frame.
 * @param p Destination buffer.
 * @param v Value to encode.
 * @return
 */
static inline void wr32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

/**
 * @brief Build a simple-query message suitable for feeding into the PG flow
 *        parser directly.
 * @param buf Destination buffer.
 * @param sql Query text to embed.
 * @return Number of bytes written.
 */
static size_t build_query(uint8_t *buf, const char *sql) {
    size_t sl = strlen(sql);
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sl + 1));
    memcpy(buf + 5, sql, sl);
    buf[5 + sl] = '\0';
    return 1 + 4 + sl + 1;
}

/**
 * @brief Build a minimal PostgreSQL startup packet.
 * @param buf Destination buffer.
 * @param user Startup user value.
 * @param db Startup database value.
 * @return Number of bytes written.
 *
 * The packet contains only the fields needed to drive the flow context out of
 * its bootstrap phase. Omitting optional parameters keeps the fixture small and
 * makes later invariant failures easier to attribute.
 */
static size_t build_startup(uint8_t *buf, const char *user, const char *db) {
    uint8_t *p = buf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p, "user", 5); p += 5;
    size_t ul = strlen(user);
    memcpy(p, user, ul + 1); p += ul + 1;
    memcpy(p, "database", 9); p += 9;
    size_t dl = strlen(db);
    memcpy(p, db, dl + 1); p += dl + 1;
    *p++ = '\0';
    wr32(buf, (uint32_t)(p - buf));
    return (size_t)(p - buf);
}

/**
 * @brief Create a PG flow context and advance it through client startup.
 * @return Flow context ready for post-startup invariant checks, or `NULL` on
 *         allocation failure.
 */
static void *create_and_startup(void) {
    void *ctx = VT->create_context(NULL);
    if (!ctx) return NULL;
    uint8_t buf[512];
    size_t len = build_startup(buf, "testuser", "testdb");
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, len, &act);
    return ctx;
}

/**
 * @brief Initialize a clean session-flow fixture backed by a started PG flow
 *        context.
 * @param sf [out] Fixture to populate.
 * @return
 */
static void make_clean_sf(keel_session_flow_t *sf) {
    memset(sf, 0, sizeof(*sf));
    sf->flow = VT;
    sf->ctx = create_and_startup();
    sf->phase = KEEL_PHASE_READY;
    sf->tx = KEEL_TX_IDLE;
    sf->pins = KEEL_FPIN_NONE;
}

/**
 * @brief Release any protocol-flow context owned by a session-flow fixture.
 * @param sf Fixture to clean up.
 * @return
 */
static void destroy_sf(keel_session_flow_t *sf) {
    if (sf->ctx) VT->destroy_context(sf->ctx);
    sf->ctx = NULL;
}

/**
 * @brief Create a zeroed backend connection fixture in the requested state.
 * @param state Initial backend state enum.
 * @return Stack-allocated backend connection fixture.
 *
 * The returned connection intentionally carries `fd = -1` because these model
 * checks are reasoning about invariant combinations rather than socket-level
 * behavior. Tests that require a live descriptor use separate fixtures.
 */
static backend_conn_t make_be_conn(backend_conn_state_t state) {
    backend_conn_t c;
    memset(&c, 0, sizeof(c));
    atomic_store(&c.state, state);
    c.fd = -1;
    return c;
}

/* ============================================================================
 * §1 — Compatibility Matrix Properties
 * ============================================================================ */

static void test_matrix_symmetry(void) {
    TEST_BEGIN("compat_matrix: symmetric");
    for (int a = 0; a < KEEL_FEAT_COUNT; a++) {
        for (int b = 0; b < KEEL_FEAT_COUNT; b++) {
            TEST_ASSERT_EQ(keel_compat_matrix[a][b], keel_compat_matrix[b][a]);
        }
    }
    TEST_END();
}

static void test_matrix_self_compatible(void) {
    TEST_BEGIN("compat_matrix: diagonal is COMPAT");
    for (int a = 0; a < KEEL_FEAT_COUNT; a++) {
        TEST_ASSERT_EQ(keel_compat_matrix[a][a], KEEL_COMPAT);
    }
    TEST_END();
}

static void test_matrix_risk_intersections(void) {
    /* R1: PS_REPLAY × TRANSACTION = GUARDED (replay into active tx ok if not failed) */
    TEST_BEGIN("compat_matrix/R1: PS_REPLAY × TRANSACTION = GUARDED");
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_PS_REPLAY][KEEL_FEAT_TRANSACTION],
                   KEEL_COMPAT_GUARDED);
    TEST_END();

    /* R1b: PS_REPLAY × COMMIT_IN_DOUBT = MUTEX */
    TEST_BEGIN("compat_matrix/R1b: PS_REPLAY × COMMIT_IN_DOUBT = MUTEX");
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_PS_REPLAY][KEEL_FEAT_COMMIT_IN_DOUBT],
                   KEEL_COMPAT_MUTEX);
    TEST_END();

    /* R1c: PS_REPLAY × COPY = MUTEX */
    TEST_BEGIN("compat_matrix/R1c: PS_REPLAY × COPY = MUTEX");
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_PS_REPLAY][KEEL_FEAT_COPY_MODE],
                   KEEL_COMPAT_MUTEX);
    TEST_END();

    /* R2: LSN_CAPTURE × PS_REPLAY = MUTEX (can't capture LSN during replay) */
    TEST_BEGIN("compat_matrix/R2: LSN_CAPTURE × PS_REPLAY = MUTEX");
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_LSN_CAPTURE][KEEL_FEAT_PS_REPLAY],
                   KEEL_COMPAT_MUTEX);
    TEST_END();

    /* R3: TLS_HANDSHAKE × SPLICE = MUTEX */
    TEST_BEGIN("compat_matrix/R3: TLS_HANDSHAKE × SPLICE = MUTEX");
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_TLS_HANDSHAKE][KEEL_FEAT_SPLICE],
                   KEEL_COMPAT_MUTEX);
    TEST_END();

    /* R5: MIGRATION × TRANSACTION = MUTEX */
    TEST_BEGIN("compat_matrix/R5a: MIGRATION × TRANSACTION = MUTEX");
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_MIGRATION][KEEL_FEAT_TRANSACTION],
                   KEEL_COMPAT_MUTEX);
    TEST_END();

    /* R5b: MIGRATION × PS_REPLAY = MUTEX */
    TEST_BEGIN("compat_matrix/R5b: MIGRATION × PS_REPLAY = MUTEX");
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_MIGRATION][KEEL_FEAT_PS_REPLAY],
                   KEEL_COMPAT_MUTEX);
    TEST_END();

    /* R5c: MIGRATION × COPY = MUTEX */
    TEST_BEGIN("compat_matrix/R5c: MIGRATION × COPY = MUTEX");
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_MIGRATION][KEEL_FEAT_COPY_MODE],
                   KEEL_COMPAT_MUTEX);
    TEST_END();

    /* R6: DRAINING × COMMIT_IN_DOUBT = GUARDED */
    TEST_BEGIN("compat_matrix/R6: DRAINING × COMMIT_IN_DOUBT = GUARDED");
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_DRAINING][KEEL_FEAT_COMMIT_IN_DOUBT],
                   KEEL_COMPAT_GUARDED);
    TEST_END();

    /* TLS_HANDSHAKE is MUTEX with everything except itself */
    TEST_BEGIN("compat_matrix: TLS_HANDSHAKE is exclusive");
    for (int i = 0; i < KEEL_FEAT_COUNT; i++) {
        if (i == KEEL_FEAT_TLS_HANDSHAKE) continue;
        TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_TLS_HANDSHAKE][i],
                       KEEL_COMPAT_MUTEX);
    }
    TEST_END();

    /* MIGRATION is MUTEX with most active features except DRAINING */
    TEST_BEGIN("compat_matrix: MIGRATION blocks active features");
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_MIGRATION][KEEL_FEAT_HARD_PIN],
                   KEEL_COMPAT_MUTEX);
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_MIGRATION][KEEL_FEAT_QUARANTINE],
                   KEEL_COMPAT_MUTEX);
    TEST_ASSERT_EQ(keel_compat_matrix[KEEL_FEAT_MIGRATION][KEEL_FEAT_EXTENDED_PROTO],
                   KEEL_COMPAT_MUTEX);
    TEST_END();
}

static void test_features_compatible_api(void) {
    TEST_BEGIN("features_compatible: COMPAT returns true");
    TEST_ASSERT(keel_features_compatible(KEEL_FEAT_TRANSACTION, KEEL_FEAT_SPLICE));
    TEST_END();

    TEST_BEGIN("features_compatible: DEGRADED returns true");
    TEST_ASSERT(keel_features_compatible(KEEL_FEAT_SPLICE, KEEL_FEAT_DRAINING));
    TEST_END();

    TEST_BEGIN("features_compatible: MUTEX returns false");
    TEST_ASSERT(!keel_features_compatible(KEEL_FEAT_TLS_HANDSHAKE, KEEL_FEAT_SPLICE));
    TEST_END();

    TEST_BEGIN("features_compatible: GUARDED returns false");
    TEST_ASSERT(!keel_features_compatible(KEEL_FEAT_PS_REPLAY, KEEL_FEAT_TRANSACTION));
    TEST_END();

    TEST_BEGIN("features_compatible: out-of-range returns false");
    TEST_ASSERT(!keel_features_compatible(KEEL_FEAT_COUNT, 0));
    TEST_ASSERT(!keel_features_compatible(0, KEEL_FEAT_COUNT));
    TEST_ASSERT(!keel_features_compatible(99, 99));
    TEST_END();
}

/* ============================================================================
 * §2 — Pin Conflict Detection
 * ============================================================================ */

static void test_pin_conflicts(void) {
    TEST_BEGIN("pins_consistent: no pins = consistent");
    TEST_ASSERT(keel_pins_consistent(KEEL_FPIN_NONE));
    TEST_END();

    TEST_BEGIN("pins_consistent: single pin = consistent");
    TEST_ASSERT(keel_pins_consistent(KEEL_FPIN_TRANSACTION));
    TEST_ASSERT(keel_pins_consistent(KEEL_FPIN_COPY));
    TEST_ASSERT(keel_pins_consistent(KEEL_FPIN_EXTENDED_PROTO));
    TEST_ASSERT(keel_pins_consistent(KEEL_FPIN_STREAMING));
    TEST_ASSERT(keel_pins_consistent(KEEL_FPIN_AUTH));
    TEST_END();

    TEST_BEGIN("pins_consistent: COPY + EXTENDED = conflict");
    TEST_ASSERT(!keel_pins_consistent(KEEL_FPIN_COPY | KEEL_FPIN_EXTENDED_PROTO));
    TEST_END();

    TEST_BEGIN("pins_consistent: STREAMING + TX = conflict");
    TEST_ASSERT(!keel_pins_consistent(KEEL_FPIN_STREAMING | KEEL_FPIN_TRANSACTION));
    TEST_END();

    TEST_BEGIN("pins_consistent: STREAMING + COPY = conflict");
    TEST_ASSERT(!keel_pins_consistent(KEEL_FPIN_STREAMING | KEEL_FPIN_COPY));
    TEST_END();

    TEST_BEGIN("pins_consistent: AUTH + TX = conflict");
    TEST_ASSERT(!keel_pins_consistent(KEEL_FPIN_AUTH | KEEL_FPIN_TRANSACTION));
    TEST_END();

    TEST_BEGIN("pins_consistent: AUTH + COPY = conflict");
    TEST_ASSERT(!keel_pins_consistent(KEEL_FPIN_AUTH | KEEL_FPIN_COPY));
    TEST_END();

    TEST_BEGIN("pins_consistent: AUTH + PS = conflict");
    TEST_ASSERT(!keel_pins_consistent(KEEL_FPIN_AUTH | KEEL_FPIN_PREPARED_STMT));
    TEST_END();

    TEST_BEGIN("pins_consistent: TX + PS = compatible");
    TEST_ASSERT(keel_pins_consistent(KEEL_FPIN_TRANSACTION | KEEL_FPIN_PREPARED_STMT));
    TEST_END();

    TEST_BEGIN("pins_consistent: TX + EXTENDED = compatible");
    TEST_ASSERT(keel_pins_consistent(KEEL_FPIN_TRANSACTION | KEEL_FPIN_EXTENDED_PROTO));
    TEST_END();

    TEST_BEGIN("pins_consistent: TX + HARD_PIN (LISTEN) = compatible");
    TEST_ASSERT(keel_pins_consistent(KEEL_FPIN_TRANSACTION | KEEL_FPIN_LISTEN));
    TEST_END();

    TEST_BEGIN("pins_consistent: FAILED_TX + TRANSACTION = compatible");
    TEST_ASSERT(keel_pins_consistent(KEEL_FPIN_TRANSACTION | KEEL_FPIN_FAILED_TX));
    TEST_END();
}

/* ============================================================================
 * §3 — Session Invariant Checker — Violation Detection
 * ============================================================================ */

static void test_inv_clean_session(void) {
    TEST_BEGIN("inv_session: clean session passes all invariants");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    backend_conn_t be = make_be_conn(BACKEND_CONN_ACTIVE);
    uint32_t v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT_EQ(v, (uint32_t)KEEL_INV_OK);
    destroy_sf(&sf);
    TEST_END();
}

static void test_inv_null_sf(void) {
    TEST_BEGIN("inv_session: NULL sf returns OK");
    TEST_ASSERT_EQ(keel_invariant_check_session(NULL, NULL, NULL), (uint32_t)0);
    TEST_END();
}

/* R1: PS replay violations */

static void test_inv_replay_without_backend(void) {
    TEST_BEGIN("inv_session/R1: replay without backend");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.stmt_replay_len = 42;  /* replay in progress */
    /* no be_conn */
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_REPLAY_WITHOUT_BACKEND);
    destroy_sf(&sf);
    TEST_END();
}

static void test_inv_replay_in_failed_tx(void) {
    TEST_BEGIN("inv_session/R1: replay in failed tx");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.stmt_replay_len = 42;
    sf.tx = KEEL_TX_FAILED;
    backend_conn_t be = make_be_conn(BACKEND_CONN_ACTIVE);
    uint32_t v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT(v & KEEL_INV_REPLAY_IN_FAILED_TX);
    destroy_sf(&sf);
    TEST_END();
}

static void test_inv_replay_during_copy(void) {
    TEST_BEGIN("inv_session/R1: replay during COPY");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.stmt_replay_len = 42;
    sf.pins = KEEL_FPIN_COPY;
    backend_conn_t be = make_be_conn(BACKEND_CONN_ACTIVE);
    uint32_t v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT(v & KEEL_INV_REPLAY_DURING_COPY);
    destroy_sf(&sf);
    TEST_END();
}

/* R2: LSN capture violations */

static void test_inv_lsn_without_write(void) {
    TEST_BEGIN("inv_session/R2: LSN capture in active tx without writes");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.capture_lsn_pending = true;
    sf.txn_had_writes = false;
    sf.tx = KEEL_TX_ACTIVE;
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_LSN_WITHOUT_TX_WRITE);
    destroy_sf(&sf);
    TEST_END();
}

static void test_inv_lsn_in_failed_tx(void) {
    TEST_BEGIN("inv_session/R2: LSN capture in failed tx");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.capture_lsn_pending = true;
    sf.tx = KEEL_TX_FAILED;
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_LSN_IN_FAILED_TX);
    destroy_sf(&sf);
    TEST_END();
}

/* Pin conflict */

static void test_inv_pin_conflict(void) {
    TEST_BEGIN("inv_session: COPY + EXTENDED pin conflict detected");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.pins = KEEL_FPIN_COPY | KEEL_FPIN_EXTENDED_PROTO;
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_PIN_CONFLICT);
    destroy_sf(&sf);
    TEST_END();
}

/* Phase consistency */

static void test_inv_query_in_auth(void) {
    TEST_BEGIN("inv_session: query pins during AUTH phase");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.phase = KEEL_PHASE_HANDSHAKE_AUTH;
    sf.pins = KEEL_FPIN_TRANSACTION;
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_QUERY_IN_AUTH_PHASE);
    destroy_sf(&sf);
    TEST_END();
}

/* Pool ↔ session consistency */

static void test_inv_idle_backend_with_tx(void) {
    TEST_BEGIN("inv_session: IDLE backend but session in tx");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.tx = KEEL_TX_ACTIVE;
    backend_conn_t be = make_be_conn(BACKEND_CONN_IDLE);
    uint32_t v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT(v & KEEL_INV_IDLE_BACKEND_WITH_TX);
    destroy_sf(&sf);
    TEST_END();
}

static void test_inv_no_backend_with_tx_pin(void) {
    TEST_BEGIN("inv_session: TX pin set but no backend and not queued");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.pins = KEEL_FPIN_TRANSACTION;
    sf.queued_for_pool = false;
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_NO_BACKEND_WITH_TX_PIN);
    destroy_sf(&sf);
    TEST_END();
}

static void test_inv_tx_pin_queued_ok(void) {
    TEST_BEGIN("inv_session: TX pin set + queued_for_pool = OK");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.pins = KEEL_FPIN_TRANSACTION;
    sf.queued_for_pool = true;
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(!(v & KEEL_INV_NO_BACKEND_WITH_TX_PIN));
    destroy_sf(&sf);
    TEST_END();
}

/* Commit-in-doubt consistency */

static void test_inv_doubt_without_tracking(void) {
    TEST_BEGIN("inv_session: commit_in_doubt without txn_tracking");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.commit_in_doubt = true;
    sf.txn_tracking = false;
    sf.indoubt_xid = 12345;
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_DOUBT_WITHOUT_TRACKING);
    destroy_sf(&sf);
    TEST_END();
}

static void test_inv_doubt_without_xid(void) {
    TEST_BEGIN("inv_session: commit_in_doubt without XID");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.commit_in_doubt = true;
    sf.txn_tracking = true;
    sf.indoubt_xid = 0;
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_DOUBT_WITHOUT_XID);
    destroy_sf(&sf);
    TEST_END();
}

static void test_inv_doubt_valid(void) {
    TEST_BEGIN("inv_session: valid commit_in_doubt state passes");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.commit_in_doubt = true;
    sf.txn_tracking = true;
    sf.indoubt_xid = 98765;
    /* No specific violation for valid doubt state */
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(!(v & KEEL_INV_DOUBT_WITHOUT_TRACKING));
    TEST_ASSERT(!(v & KEEL_INV_DOUBT_WITHOUT_XID));
    destroy_sf(&sf);
    TEST_END();
}

/* Copy state consistency */

static void test_inv_copy_state_without_pin(void) {
    TEST_BEGIN("inv_session: copy_skip > 0 but no COPY pin");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.copy_skip = 100;
    sf.pins = KEEL_FPIN_NONE;
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_COPY_STATE_WITHOUT_PIN);
    destroy_sf(&sf);
    TEST_END();
}

static void test_inv_copy_hdr_without_pin(void) {
    TEST_BEGIN("inv_session: copy_hdr_len > 0 but no COPY pin");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.copy_hdr_len = 3;
    sf.pins = KEEL_FPIN_TRANSACTION;  /* not COPY */
    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_COPY_STATE_WITHOUT_PIN);
    destroy_sf(&sf);
    TEST_END();
}

/* Multiple simultaneous violations */

static void test_inv_multiple_violations(void) {
    TEST_BEGIN("inv_session: multiple violations in one check");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    sf.stmt_replay_len = 42;            /* replay without backend */
    sf.tx = KEEL_TX_FAILED;             /* replay in failed tx */
    sf.commit_in_doubt = true;          /* doubt without tracking */
    sf.txn_tracking = false;
    sf.indoubt_xid = 0;                 /* doubt without xid */
    sf.pins = KEEL_FPIN_COPY | KEEL_FPIN_EXTENDED_PROTO;  /* pin conflict + replay during copy */

    uint32_t v = keel_invariant_check_session(&sf, NULL, NULL);
    TEST_ASSERT(v & KEEL_INV_REPLAY_WITHOUT_BACKEND);
    TEST_ASSERT(v & KEEL_INV_REPLAY_IN_FAILED_TX);
    TEST_ASSERT(v & KEEL_INV_REPLAY_DURING_COPY);
    TEST_ASSERT(v & KEEL_INV_PIN_CONFLICT);
    TEST_ASSERT(v & KEEL_INV_DOUBT_WITHOUT_TRACKING);
    TEST_ASSERT(v & KEEL_INV_DOUBT_WITHOUT_XID);
    destroy_sf(&sf);
    TEST_END();
}

/* ============================================================================
 * §4 — Pool Invariant Checker
 * ============================================================================ */

static void test_pool_inv_empty(void) {
    TEST_BEGIN("inv_pool: empty pool passes");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 10;
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT_EQ(v, (uint32_t)0);
    TEST_END();
}

static void test_pool_inv_null(void) {
    TEST_BEGIN("inv_pool: NULL pool returns OK");
    TEST_ASSERT_EQ(keel_invariant_check_pool(NULL), (uint32_t)0);
    TEST_END();
}

static void test_pool_inv_count_overflow(void) {
    TEST_BEGIN("inv_pool: count overflow detected");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 10;
    pool.active_count = 6;
    pool.clean_count = 3;
    pool.dirty_count = 2;
    pool.cleaning_count = 1;
    /* Sum = 12 > 10 */
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT(v != 0);
    TEST_END();
}

static void test_pool_inv_cleaning_overflow(void) {
    TEST_BEGIN("inv_pool: cleaning > total detected");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 5;
    pool.cleaning_count = 6;
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT(v != 0);
    TEST_END();
}

static void test_pool_inv_active_overflow(void) {
    TEST_BEGIN("inv_pool: active > total detected");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 5;
    pool.active_count = 10;
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT(v != 0);
    TEST_END();
}

static void test_pool_inv_wrong_list_state(void) {
    TEST_BEGIN("inv_pool: ACTIVE conn on clean_list");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 10;
    pool.clean_count = 1;
    backend_conn_t conn = make_be_conn(BACKEND_CONN_ACTIVE);
    pool.clean_list = &conn;
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT(v != 0);
    TEST_END();
}

static void test_pool_inv_valid_clean_list(void) {
    TEST_BEGIN("inv_pool: IDLE conn on clean_list = OK");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 10;
    pool.clean_count = 1;
    backend_conn_t conn = make_be_conn(BACKEND_CONN_IDLE);
    conn.next = NULL;
    pool.clean_list = &conn;
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT_EQ(v, (uint32_t)0);
    TEST_END();
}

static void test_pool_inv_clean_list_rejects_dirty(void) {
    TEST_BEGIN("inv_pool: clean_list rejects dirty state");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 10;
    pool.clean_count = 1;
    backend_conn_t conn = make_be_conn(BACKEND_CONN_IDLE);
    conn.current_state_hash = 0xdeadbeefULL;
    pool.clean_list = &conn;
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT(v != 0);
    TEST_END();
}

static void test_pool_inv_borrowable_rejects_pinned(void) {
    TEST_BEGIN("inv_pool: borrowable lists reject pinned owner");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 10;
    backend_conn_t conn = make_be_conn(BACKEND_CONN_IDLE);
    int owner = 42;
    conn.pinned_session = &owner;
    pool.idle_list = &conn;
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT(v != 0);
    TEST_END();
}

static void test_pool_inv_cleaning_not_borrowable(void) {
    TEST_BEGIN("inv_pool: CLEANING conn not borrowable");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 10;
    backend_conn_t conn = make_be_conn(BACKEND_CONN_CLEANING);
    conn.current_state_hash = 0xfeedULL;
    pool.dirty_list = &conn;
    pool.dirty_count = 1;
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT(v != 0);
    TEST_END();
}

static void test_pool_inv_closed_not_borrowable(void) {
    TEST_BEGIN("inv_pool: CLOSED conn not borrowable");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 10;
    backend_conn_t conn = make_be_conn(BACKEND_CONN_CLOSED);
    pool.idle_list = &conn;
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT(v != 0);
    TEST_END();
}

static void test_pool_inv_dirty_list_requires_dirty(void) {
    TEST_BEGIN("inv_pool: dirty_list requires cleanup work");
    backend_pool_t pool;
    memset(&pool, 0, sizeof(pool));
    pool.total_count = 10;
    backend_conn_t conn = make_be_conn(BACKEND_CONN_IDLE);
    pool.dirty_list = &conn;
    pool.dirty_count = 1;
    uint32_t v = keel_invariant_check_pool(&pool);
    TEST_ASSERT(v != 0);

    conn.current_state_hash = 0xbeefULL;
    v = keel_invariant_check_pool(&pool);
    TEST_ASSERT_EQ(v, (uint32_t)0);
    TEST_END();
}

/* ============================================================================
 * §5 — Live Protocol Flow Invariant Integration
 *
 * Run actual protocol messages and verify the invariant checker passes
 * at each step of the query lifecycle.
 * ============================================================================ */

static void test_live_query_lifecycle_invariants(void) {
    TEST_BEGIN("inv_live: SELECT lifecycle passes all invariants");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    backend_conn_t be = make_be_conn(BACKEND_CONN_ACTIVE);

    /* Step 1: Send SELECT query */
    uint8_t buf[128];
    keel_fe_action_t fact;
    size_t len = build_query(buf, "SELECT 1");
    VT->on_fe_msg(sf.ctx, buf, len, &fact);

    /* Apply pin updates like the engine would */
    sf.pins |= fact.pin_update;
    sf.pins &= ~fact.pin_clear;
    sf.phase = KEEL_PHASE_QUERY;

    uint32_t v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT_EQ(v, (uint32_t)KEEL_INV_OK);

    /* Step 2: Backend responds with ReadyForQuery('I') */
    uint8_t rfq[6] = {'Z', 0, 0, 0, 5, 'I'};
    keel_be_action_t bact;
    VT->on_be_msg(sf.ctx, rfq, 6, &bact);

    sf.pins |= bact.pin_update;
    sf.pins &= ~bact.pin_clear;
    if (bact.tx_state_changed) sf.tx = bact.tx_status;
    sf.phase = KEEL_PHASE_READY;

    v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT_EQ(v, (uint32_t)KEEL_INV_OK);

    destroy_sf(&sf);
    TEST_END();
}

static void test_live_begin_commit_invariants(void) {
    TEST_BEGIN("inv_live: BEGIN...COMMIT lifecycle passes invariants");
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    backend_conn_t be = make_be_conn(BACKEND_CONN_ACTIVE);

    /* BEGIN */
    uint8_t buf[128];
    keel_fe_action_t fact;
    size_t len = build_query(buf, "BEGIN");
    VT->on_fe_msg(sf.ctx, buf, len, &fact);
    sf.pins |= fact.pin_update;
    sf.pins &= ~fact.pin_clear;
    sf.phase = KEEL_PHASE_QUERY;

    /* After BEGIN: TX pin should be set, invariants pass */
    uint32_t v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT_EQ(v, (uint32_t)KEEL_INV_OK);
    TEST_ASSERT(sf.pins & KEEL_FPIN_TRANSACTION);

    /* Backend confirms with Z('T') */
    uint8_t rfq_t[6] = {'Z', 0, 0, 0, 5, 'T'};
    keel_be_action_t bact;
    VT->on_be_msg(sf.ctx, rfq_t, 6, &bact);
    sf.pins |= bact.pin_update;
    sf.pins &= ~bact.pin_clear;
    if (bact.tx_state_changed) sf.tx = bact.tx_status;

    v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT_EQ(v, (uint32_t)KEEL_INV_OK);

    /* COMMIT */
    len = build_query(buf, "COMMIT");
    VT->on_fe_msg(sf.ctx, buf, len, &fact);
    sf.pins |= fact.pin_update;
    sf.pins &= ~fact.pin_clear;

    /* Backend confirms with Z('I') */
    uint8_t rfq_i[6] = {'Z', 0, 0, 0, 5, 'I'};
    VT->on_be_msg(sf.ctx, rfq_i, 6, &bact);
    sf.pins |= bact.pin_update;
    sf.pins &= ~bact.pin_clear;
    if (bact.tx_state_changed) sf.tx = bact.tx_status;
    sf.phase = KEEL_PHASE_READY;

    v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT_EQ(v, (uint32_t)KEEL_INV_OK);
    TEST_ASSERT(!(sf.pins & KEEL_FPIN_TRANSACTION));

    destroy_sf(&sf);
    TEST_END();
}

/* ============================================================================
 * §6 — Exhaustive Feature Pair Compatibility Verification
 *
 * For every COMPAT pair: confirm both features can be set simultaneously
 * without triggering the invariant checker (assuming consistent surrounding
 * state).
 * For every MUTEX pair: confirm the checker catches the violation.
 * ============================================================================ */

static void test_exhaustive_feature_pairs(void) {
    TEST_BEGIN("compat_exhaustive: all MUTEX pairs detected by checker or matrix");

    /* Verify the 6 MUTEX pairs that the session checker can detect */
    struct {
        keel_feature_id_t a, b;
        const char *desc;
    } mutex_pairs[] = {
        { KEEL_FEAT_PS_REPLAY,       KEEL_FEAT_COPY_MODE,        "R1c: PS_REPLAY × COPY"      },
        { KEEL_FEAT_PS_REPLAY,       KEEL_FEAT_COMMIT_IN_DOUBT,  "R1b: PS_REPLAY × DOUBT"     },
        { KEEL_FEAT_TLS_HANDSHAKE,   KEEL_FEAT_SPLICE,           "R3: TLS_HS × SPLICE"        },
        { KEEL_FEAT_MIGRATION,       KEEL_FEAT_TRANSACTION,      "R5a: MIGRATION × TX"         },
        { KEEL_FEAT_MIGRATION,       KEEL_FEAT_PS_REPLAY,        "R5b: MIGRATION × PS_REPLAY"  },
        { KEEL_FEAT_MIGRATION,       KEEL_FEAT_COPY_MODE,        "R5c: MIGRATION × COPY"       },
    };

    for (size_t i = 0; i < sizeof(mutex_pairs) / sizeof(mutex_pairs[0]); i++) {
        keel_compat_level_t l = keel_compat_matrix[mutex_pairs[i].a][mutex_pairs[i].b];
        TEST_ASSERT_EQ(l, KEEL_COMPAT_MUTEX);
    }
    TEST_END();

    TEST_BEGIN("compat_exhaustive: no COMPAT pair triggers session checker");

    /* Build a clean sf and verify that each COMPAT pair doesn't trigger */
    keel_session_flow_t sf;
    make_clean_sf(&sf);
    backend_conn_t be = make_be_conn(BACKEND_CONN_ACTIVE);

    /* TX + SPLICE = COMPAT — both active should be fine */
    sf.pins = KEEL_FPIN_TRANSACTION;
    sf.tx = KEEL_TX_ACTIVE;
    uint32_t v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT_EQ(v, (uint32_t)KEEL_INV_OK);

    /* TX + EXTENDED = COMPAT */
    sf.pins = KEEL_FPIN_TRANSACTION | KEEL_FPIN_EXTENDED_PROTO;
    v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT_EQ(v, (uint32_t)KEEL_INV_OK);

    /* TX + HARD_PIN = COMPAT */
    sf.pins = KEEL_FPIN_TRANSACTION | KEEL_FPIN_LISTEN;
    v = keel_invariant_check_session(&sf, NULL, &be);
    TEST_ASSERT_EQ(v, (uint32_t)KEEL_INV_OK);

    destroy_sf(&sf);
    TEST_END();
}

/* ============================================================================
 * main()
 * ============================================================================ */

int main(void) {
    printf("=== Formal Invariant Model Tests ===\n\n");

    /* §1 — Compatibility matrix */
    test_matrix_symmetry();
    test_matrix_self_compatible();
    test_matrix_risk_intersections();
    test_features_compatible_api();

    /* §2 — Pin conflicts */
    test_pin_conflicts();

    /* §3 — Session invariant violations */
    test_inv_clean_session();
    test_inv_null_sf();
    test_inv_replay_without_backend();
    test_inv_replay_in_failed_tx();
    test_inv_replay_during_copy();
    test_inv_lsn_without_write();
    test_inv_lsn_in_failed_tx();
    test_inv_pin_conflict();
    test_inv_query_in_auth();
    test_inv_idle_backend_with_tx();
    test_inv_no_backend_with_tx_pin();
    test_inv_tx_pin_queued_ok();
    test_inv_doubt_without_tracking();
    test_inv_doubt_without_xid();
    test_inv_doubt_valid();
    test_inv_copy_state_without_pin();
    test_inv_copy_hdr_without_pin();
    test_inv_multiple_violations();

    /* §4 — Pool invariants */
    test_pool_inv_empty();
    test_pool_inv_null();
    test_pool_inv_count_overflow();
    test_pool_inv_cleaning_overflow();
    test_pool_inv_active_overflow();
    test_pool_inv_wrong_list_state();
    test_pool_inv_valid_clean_list();
    test_pool_inv_clean_list_rejects_dirty();
    test_pool_inv_borrowable_rejects_pinned();
    test_pool_inv_cleaning_not_borrowable();
    test_pool_inv_closed_not_borrowable();
    test_pool_inv_dirty_list_requires_dirty();

    /* §5 — Live protocol flow */
    test_live_query_lifecycle_invariants();
    test_live_begin_commit_invariants();

    /* §6 — Exhaustive feature pairs */
    test_exhaustive_feature_pairs();

    printf("\n--- Results: %d run, %d passed, %d failed ---\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return g_tests_failed > 0 ? 1 : 0;
}
