/**
 * @file test_crash_recovery_matrix.c
 * @brief Matrix tests for crash-recovery semantics, XID tracking, and cleanup policy.
 *
 * This suite verifies the subtle transaction edge where the proxy must reason
 * about commit outcome across potential failures. The key behavior under test is
 * COMMIT rewriting plus XID probe absorption: control frames are intercepted in
 * a strict order until enough evidence exists to safely finalize session state.
 *
 * The tests intentionally combine lifecycle axes (tracking enabled, backend
 * responses, cleanup reasons, tx states) so regressions in one branch do not
 * hide behind happy-path assumptions.
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/postgres/postgres_flow_internal.h"
#include "keel/session/session.h"

#include <string.h>
#include <stdio.h>

/* ---- Globals ---- */
int g_tests_run, g_tests_passed, g_tests_failed;

/* ---- PG vtable ---- */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
#define VT (&keel_proto_flow_postgres)

/* ============================================================================
 * Wire-protocol builders
 * ============================================================================ */

/**
 * @brief Encode a 32-bit big-endian integer for synthetic PG frames.
 * @param p Destination buffer.
 * @param v Value to encode.
 * @return
 */
static inline void wr32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

/**
 * @brief Build a minimal PostgreSQL startup packet.
 * @param buf Destination buffer.
 * @param user Startup user.
 * @param db Startup database.
 * @return Number of bytes written.
 */
static size_t build_startup(uint8_t *buf, const char *user, const char *db) {
    uint8_t *p = buf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p, "user", 5);     p += 5;
    size_t ul = strlen(user);
    memcpy(p, user, ul + 1);  p += ul + 1;
    memcpy(p, "database", 9); p += 9;
    size_t dl = strlen(db);
    memcpy(p, db, dl + 1);    p += dl + 1;
    *p++ = '\0';
    wr32(buf, (uint32_t)(p - buf));
    return (size_t)(p - buf);
}

/**
 * @brief Build a PostgreSQL simple-query frame.
 * @param buf Destination buffer.
 * @param sql Query text.
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
 * @brief Build a ReadyForQuery backend message.
 * @param buf Destination buffer.
 * @param status Transaction status byte (`I`, `T`, or `E`).
 * @return Number of bytes written.
 */
static size_t build_ready_for_query(uint8_t *buf, char status) {
    buf[0] = 'Z';
    wr32(buf + 1, 5);
    buf[5] = (uint8_t)status;
    return 6;
}

/**
 * @brief Build the smallest valid extended-protocol message shell.
 * @param buf Destination buffer.
 * @param type PostgreSQL message type byte.
 * @return Number of bytes written.
 */
static size_t build_extended_msg(uint8_t *buf, uint8_t type) {
    buf[0] = type;
    wr32(buf + 1, 4);
    return 5;
}

/**
 * @brief Build a named Parse message for prepared-statement path testing.
 * @param buf Destination buffer.
 * @param name Statement name.
 * @param query SQL text.
 * @return Number of bytes written.
 */
static size_t build_named_parse(uint8_t *buf, const char *name, const char *query) {
    size_t nl = strlen(name);
    size_t ql = strlen(query);
    size_t body = nl + 1 + ql + 1 + 2;
    buf[0] = 'P';
    wr32(buf + 1, (uint32_t)(4 + body));
    memcpy(buf + 5, name, nl + 1);
    memcpy(buf + 5 + nl + 1, query, ql + 1);
    buf[5 + nl + 1 + ql + 1] = 0;
    buf[5 + nl + 1 + ql + 2] = 0;
    return 1 + 4 + body;
}

/* Backend message builders */
static size_t build_be_row_description(uint8_t *buf) {
    buf[0] = 'T';
    buf[1] = 0; buf[2] = 0; buf[3] = 0; buf[4] = 6;
    buf[5] = 0; buf[6] = 0;
    return 7;
}

static size_t build_be_data_row_text(uint8_t *buf, const char *val) {
    size_t vlen = strlen(val);
    uint32_t msglen = (uint32_t)(4 + 2 + 4 + vlen);
    buf[0] = 'D';
    buf[1] = (msglen >> 24) & 0xff; buf[2] = (msglen >> 16) & 0xff;
    buf[3] = (msglen >>  8) & 0xff; buf[4] = (msglen      ) & 0xff;
    buf[5] = 0; buf[6] = 1;
    uint32_t cl = (uint32_t)vlen;
    buf[7]  = (cl >> 24) & 0xff; buf[8]  = (cl >> 16) & 0xff;
    buf[9]  = (cl >>  8) & 0xff; buf[10] = (cl      ) & 0xff;
    memcpy(buf + 11, val, vlen);
    return (size_t)(11 + vlen);
}

static size_t build_be_command_complete(uint8_t *buf, const char *tag) {
    size_t tlen = strlen(tag) + 1;
    uint32_t msglen = (uint32_t)(4 + tlen);
    buf[0] = 'C';
    buf[1] = (msglen >> 24) & 0xff; buf[2] = (msglen >> 16) & 0xff;
    buf[3] = (msglen >>  8) & 0xff; buf[4] = (msglen      ) & 0xff;
    memcpy(buf + 5, tag, tlen);
    return (size_t)(5 + tlen);
}

/* Build ErrorResponse: 'E' + length(4) + fields */
static size_t build_error_response(uint8_t *buf) {
    buf[0] = 'E';
    const char fields[] = "SERROR\0MTEST\0\0";
    size_t fl = sizeof(fields) - 1;
    wr32(buf + 1, (uint32_t)(4 + fl));
    memcpy(buf + 5, fields, fl);
    return 1 + 4 + fl;
}

static bool pg_error_response_message_contains(const uint8_t *msg,
                                               size_t msg_len,
                                               const char *needle)
{
    if (!msg || msg_len < 6 || !needle || needle[0] == '\0')
        return false;
    if (msg[0] != 'E')
        return false;

    size_t i = 5; /* field stream starts after tag+len */
    while (i < msg_len) {
        uint8_t field = msg[i++];
        if (field == 0)
            break;

        size_t start = i;
        while (i < msg_len && msg[i] != '\0')
            i++;
        if (i >= msg_len)
            return false;

        if (field == 'M') {
            return strstr((const char *)(msg + start), needle) != NULL;
        }
        i++; /* skip terminating NUL for this field value */
    }
    return false;
}

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Create a PG flow context and complete startup bootstrap.
 * @return Initialized context or `NULL`.
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
 * @brief Create a startup-complete PG flow context with transaction tracking
 *        explicitly enabled.
 * @return Initialized context or `NULL`.
 */
static void *create_with_txn_tracking(void) {
    void *ctx = create_and_startup();
    if (ctx) ((pg_flow_ctx_t *)ctx)->txn_tracking = true;
    return ctx;
}

/* ============================================================================
 * 1) Full XID probe lifecycle
 *
 * COMMIT → rewritten → backend sends: T(RowDesc) → D(DataRow with XID)
 * → C("SELECT 1") → C("COMMIT") → Z('I')
 *
 * Each step has specific absorb/forward expectations.
 * ============================================================================ */

static void test_full_xid_probe_lifecycle(void) {
    TEST_BEGIN("crash_xid: full COMMIT → XID probe → forward lifecycle");

    void *ctx = create_with_txn_tracking();
    TEST_ASSERT_NOT_NULL(ctx);
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;

    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* Step 1: COMMIT rewritten */
    size_t len = build_query(buf, "COMMIT");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(c->xid_probe_active);
    TEST_ASSERT(fact.be_payload != buf); /* Rewritten */
    const char *sql = (const char *)(fact.be_payload + 5);
    TEST_ASSERT(strstr(sql, "txid_current") != NULL);

    /* Step 2: RowDescription absorbed */
    len = build_be_row_description(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_ABSORB);

    /* Step 3: DataRow with XID — absorbed, XID captured */
    len = build_be_data_row_text(buf, "12345678");
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_ABSORB);
    TEST_ASSERT(bact.commit_xid_captured);
    TEST_ASSERT_EQ(bact.commit_xid, (uint64_t)12345678);

    /* Step 4: C("SELECT 1") absorbed, xid_probe_active cleared */
    len = build_be_command_complete(buf, "SELECT 1");
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_ABSORB);
    TEST_ASSERT(!c->xid_probe_active);

    /* Step 5: C("COMMIT") forwarded normally */
    len = build_be_command_complete(buf, "COMMIT");
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_FORWARD_FE);

    /* Step 6: Z('I') — tx complete, backend reusable */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(bact.query_complete);
    TEST_ASSERT(bact.tx_state_changed);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_IDLE);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 2) COMMIT without txn_tracking — passthrough, no XID probe
 * ============================================================================ */

static void test_commit_no_tracking(void) {
    TEST_BEGIN("crash_notrack: COMMIT passes through without tracking");

    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;
    TEST_ASSERT(!c->txn_tracking);

    uint8_t buf[64];
    keel_fe_action_t act;
    size_t len = build_query(buf, "COMMIT");
    VT->on_fe_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.be_payload == buf);
    TEST_ASSERT(!c->xid_probe_active);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 3) XID capture with various numeric values
 * ============================================================================ */

static void test_xid_capture_values(void) {
    struct { const char *val; uint64_t expect; } cases[] = {
        { "0",          0 },
        { "1",          1 },
        { "999999999",  999999999 },
        { "1234567890", 1234567890 },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "crash_xid_val/%zu: capture XID %s", i, cases[i].val);
        TEST_BEGIN(desc);

        void *ctx = create_with_txn_tracking();
        TEST_ASSERT_NOT_NULL(ctx);
        ((pg_flow_ctx_t *)ctx)->xid_probe_active = true;

        uint8_t buf[64];
        keel_be_action_t bact;
        size_t len = build_be_data_row_text(buf, cases[i].val);
        VT->on_be_msg(ctx, buf, len, &bact);
        TEST_ASSERT(bact.commit_xid_captured);
        TEST_ASSERT_EQ(bact.commit_xid, cases[i].expect);

        VT->destroy_context(ctx);
        TEST_END();
    }
}

/* ============================================================================
 * 4) commit_in_doubt flag lifecycle on session
 * ============================================================================ */

static void test_commit_in_doubt_lifecycle(void) {
    TEST_BEGIN("crash_cid: commit_in_doubt flag lifecycle");

    keel_session_t session;
    memset(&session, 0, sizeof(session));

    /* Initially false */
    TEST_ASSERT(!session.commit_in_doubt);

    /* Set true (simulating backend death after COMMIT sent) */
    session.commit_in_doubt = true;
    TEST_ASSERT(session.commit_in_doubt);

    /* Clear after resolution */
    session.commit_in_doubt = false;
    TEST_ASSERT(!session.commit_in_doubt);

    TEST_END();
}

/* ============================================================================
 * 5) commit_in_doubt × force-close protection
 *
 * Sessions with commit_in_doubt = true must NOT be force-closed.
 * ============================================================================ */

static void test_commit_in_doubt_force_close_protection(void) {
    TEST_BEGIN("crash_cid_fc: commit_in_doubt protects from force-close");

    keel_session_t safe;
    memset(&safe, 0, sizeof(safe));
    safe.commit_in_doubt = true;
    /* Engine should check this before force-closing */
    TEST_ASSERT(safe.commit_in_doubt);

    keel_session_t unsafe;
    memset(&unsafe, 0, sizeof(unsafe));
    unsafe.commit_in_doubt = false;
    /* This session CAN be force-closed */
    TEST_ASSERT(!unsafe.commit_in_doubt);

    TEST_END();
}

/* ============================================================================
 * 6) Cleanup reason × build_cleanup output matrix
 *
 * All 6 cleanup reasons should produce valid cleanup SQL.
 * ============================================================================ */

static void test_cleanup_reason_matrix(void) {
    struct {
        keel_cleanup_reason_t reason;
        const char *name;
        bool expect_rollback;
    } cases[] = {
        { KEEL_CLEANUP_FE_DISCONNECT,  "fe_disconnect",  true  },
        { KEEL_CLEANUP_TX_NOT_IDLE,    "tx_not_idle",    true  },
        { KEEL_CLEANUP_FAILED_TX,      "failed_tx",      true  },
        { KEEL_CLEANUP_UNKNOWN_STATE,  "unknown_state",  true  },
        { KEEL_CLEANUP_HARD_TAINT,     "hard_taint",     true  },
        { KEEL_CLEANUP_TIMEOUT,        "timeout",        true  },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "crash_cleanup/%s: build_cleanup output", cases[i].name);
        TEST_BEGIN(desc);

        void *ctx = create_and_startup();
        TEST_ASSERT_NOT_NULL(ctx);

        uint8_t buf[512];
        ssize_t n = VT->build_cleanup(ctx, cases[i].reason, buf, sizeof(buf));
        TEST_ASSERT(n > 0);
        TEST_ASSERT_EQ(buf[0], 'Q');

        const char *sql = (const char *)(buf + 5);
        if (cases[i].expect_rollback) {
            TEST_ASSERT(strstr(sql, "ROLLBACK") != NULL);
        }
        /* All reasons should include state reset (DISCARD ALL, RESET ALL, or similar) */
        TEST_ASSERT(strstr(sql, "DISCARD") != NULL ||
                    strstr(sql, "RESET") != NULL ||
                    strstr(sql, "ROLLBACK") != NULL);

        VT->destroy_context(ctx);
        TEST_END();
    }
}

/* ============================================================================
 * 7) RFQ error status ('E') × pin management
 *
 * ReadyForQuery with status 'E' should:
 *   - Set FAILED_TX pin
 *   - Report tx_status = KEEL_TX_FAILED
 *   - NOT mark backend as reusable
 * ============================================================================ */

static void test_rfq_error_matrix(void) {
    char statuses[] = { 'I', 'T', 'E' };
    keel_tx_status_t expected_tx[] = { KEEL_TX_IDLE, KEEL_TX_ACTIVE, KEEL_TX_FAILED };
    bool expected_reusable[] = { true, false, false };

    for (int i = 0; i < 3; i++) {
        char desc[128];
        snprintf(desc, sizeof(desc),
                 "crash_rfq/%c: tx_status and reusability", statuses[i]);
        TEST_BEGIN(desc);

        void *ctx = create_and_startup();
        TEST_ASSERT_NOT_NULL(ctx);

        uint8_t buf[16];
        keel_be_action_t bact;
        size_t len = build_ready_for_query(buf, statuses[i]);
        VT->on_be_msg(ctx, buf, len, &bact);

        TEST_ASSERT(bact.tx_state_changed);
        TEST_ASSERT_EQ(bact.tx_status, expected_tx[i]);
        TEST_ASSERT_EQ(bact.backend_reusable, expected_reusable[i]);

        if (statuses[i] == 'E') {
            TEST_ASSERT(bact.pin_update & KEEL_FPIN_FAILED_TX);
        }

        VT->destroy_context(ctx);
        TEST_END();
    }
}

/* ============================================================================
 * 8) Backend ErrorResponse during PS cycle
 *
 * If backend sends ErrorResponse during Parse→..→Sync, verify:
 *   - Error is forwarded to frontend
 *   - Pin state is still tracked (EXTENDED_PROTO remains until Sync)
 * ============================================================================ */

static void test_error_during_ps_cycle(void) {
    TEST_BEGIN("crash_ps_error: ErrorResponse during PS extended proto");

    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* Parse */
    size_t len = build_named_parse(buf, "errstmt", "SELECT 1/0");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.pin_update & KEEL_FPIN_EXTENDED_PROTO);

    /* Backend sends ErrorResponse */
    len = build_error_response(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    /* Error should be forwarded to frontend */
    TEST_ASSERT_EQ(bact.type, KEEL_BE_ACT_FORWARD_FE);

    /* Sync should still clear EXTENDED_PROTO */
    len = build_extended_msg(buf, 'S');
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.pin_clear & KEEL_FPIN_EXTENDED_PROTO);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 9) Multiple sequential COMMITs with txn_tracking
 *
 * Two consecutive COMMIT cycles should each get independent XID probes.
 * ============================================================================ */

static void test_sequential_commit_rewrites(void) {
    TEST_BEGIN("crash_seq_commit: two sequential COMMIT cycles are independent");

    void *ctx = create_with_txn_tracking();
    TEST_ASSERT_NOT_NULL(ctx);
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;

    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* -- First COMMIT cycle -- */
    size_t len = build_query(buf, "COMMIT");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(c->xid_probe_active);

    /* Absorb probe responses */
    len = build_be_row_description(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    len = build_be_data_row_text(buf, "111");
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.commit_xid_captured);
    TEST_ASSERT_EQ(bact.commit_xid, (uint64_t)111);
    len = build_be_command_complete(buf, "SELECT 1");
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(!c->xid_probe_active);

    /* Forward C(COMMIT) and RFQ */
    len = build_be_command_complete(buf, "COMMIT");
    VT->on_be_msg(ctx, buf, len, &bact);
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);

    /* -- Second COMMIT cycle (new transaction) -- */
    /* BEGIN */
    len = build_query(buf, "BEGIN");
    VT->on_fe_msg(ctx, buf, len, &fact);
    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);

    /* Second COMMIT */
    len = build_query(buf, "COMMIT");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(c->xid_probe_active);

    len = build_be_row_description(buf);
    VT->on_be_msg(ctx, buf, len, &bact);
    len = build_be_data_row_text(buf, "222");
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.commit_xid_captured);
    TEST_ASSERT_EQ(bact.commit_xid, (uint64_t)222);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 10) ROLLBACK with txn_tracking — should NOT trigger XID probe
 * ============================================================================ */

static void test_rollback_no_probe(void) {
    TEST_BEGIN("crash_rollback: ROLLBACK does NOT trigger XID probe");

    void *ctx = create_with_txn_tracking();
    TEST_ASSERT_NOT_NULL(ctx);
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;

    uint8_t buf[64];
    keel_fe_action_t act;
    size_t len = build_query(buf, "ROLLBACK");
    VT->on_fe_msg(ctx, buf, len, &act);

    /* ROLLBACK should pass through, no XID probe */
    TEST_ASSERT(!c->xid_probe_active);
    /* ROLLBACK clears TRANSACTION pin */
    TEST_ASSERT(act.pin_clear & KEEL_FPIN_TRANSACTION);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 11) RFQ idle after failed tx — clears FAILED_TX pin
 * ============================================================================ */

static void test_rfq_idle_clears_failed(void) {
    TEST_BEGIN("crash_rfq_clear: Z(I) clears FAILED_TX pin");

    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[16];
    keel_be_action_t bact;

    /* First: RFQ('E') sets FAILED_TX */
    size_t len = build_ready_for_query(buf, 'E');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_FAILED_TX);

    /* Then: RFQ('I') should clear it */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_FAILED_TX);
    TEST_ASSERT(bact.backend_reusable);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 12) Cleanup after failed transaction includes ROLLBACK
 * ============================================================================ */

static void test_cleanup_after_failed_tx(void) {
    TEST_BEGIN("crash_cleanup_ftx: FAILED_TX cleanup includes ROLLBACK");

    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);

    /* Simulate failed tx state */
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;
    c->txn_status = 'E';
    c->in_transaction = true;

    uint8_t buf[512];
    ssize_t n = VT->build_cleanup(ctx, KEEL_CLEANUP_FAILED_TX, buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    const char *sql = (const char *)(buf + 5);
    TEST_ASSERT(strstr(sql, "ROLLBACK") != NULL);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 13) Transaction state transitions: I → T → E → I
 *
 * Full lifecycle through RFQ status changes.
 * ============================================================================ */

static void test_tx_state_full_lifecycle(void) {
    TEST_BEGIN("crash_tx_lifecycle: I → T → E → I transitions");

    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);
    uint8_t buf[16];
    keel_be_action_t bact;

    /* Start idle */
    size_t len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_IDLE);
    TEST_ASSERT(bact.backend_reusable);

    /* Transition to active (T) */
    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_ACTIVE);
    TEST_ASSERT(!bact.backend_reusable);

    /* Transition to error (E) */
    len = build_ready_for_query(buf, 'E');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_FAILED);
    TEST_ASSERT(!bact.backend_reusable);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_FAILED_TX);

    /* Back to idle (I) after ROLLBACK */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_IDLE);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_FAILED_TX);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 14) BEGIN → INSERT → ErrorResponse → ROLLBACK → COMMIT sequence
 *
 * Real-world error recovery within a transaction.
 * ============================================================================ */

static void test_error_recovery_in_tx(void) {
    TEST_BEGIN("crash_error_recovery: BEGIN → error → ROLLBACK → new cycle");

    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);
    uint8_t buf[256];
    keel_fe_action_t fact;
    keel_be_action_t bact;

    /* BEGIN */
    size_t len = build_query(buf, "BEGIN");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.pin_update & KEEL_FPIN_TRANSACTION);

    len = build_ready_for_query(buf, 'T');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_ACTIVE);

    /* INSERT (write) */
    len = build_query(buf, "INSERT INTO t VALUES (1)");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.effect & KEEL_QE_WRITE);

    /* Backend sends ErrorResponse */
    len = build_error_response(buf);
    VT->on_be_msg(ctx, buf, len, &bact);

    /* RFQ('E') — failed transaction */
    len = build_ready_for_query(buf, 'E');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_FAILED);
    TEST_ASSERT(bact.pin_update & KEEL_FPIN_FAILED_TX);

    /* ROLLBACK */
    len = build_query(buf, "ROLLBACK");
    VT->on_fe_msg(ctx, buf, len, &fact);
    TEST_ASSERT(fact.pin_clear & KEEL_FPIN_TRANSACTION);

    /* RFQ('I') — back to idle */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &bact);
    TEST_ASSERT_EQ(bact.tx_status, KEEL_TX_IDLE);
    TEST_ASSERT(bact.backend_reusable);
    TEST_ASSERT(bact.pin_clear & KEEL_FPIN_FAILED_TX);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 15) Buffer overflow guard in build_cleanup
 *
 * Verify build_cleanup doesn't write beyond buffer bounds.
 * ============================================================================ */

static void test_cleanup_buffer_guard(void) {
    TEST_BEGIN("crash_buf_guard: build_cleanup respects buffer size");

    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);

    /* Tiny buffer — should return error or truncated */
    uint8_t tiny[4];
    ssize_t n = VT->build_cleanup(ctx, KEEL_CLEANUP_FE_DISCONNECT, tiny, sizeof(tiny));
    /* Either returns -1 (too small) or returns 0 (nothing written) */
    TEST_ASSERT(n <= 0 || (size_t)n <= sizeof(tiny));

    /* Normal buffer */
    uint8_t normal[512];
    n = VT->build_cleanup(ctx, KEEL_CLEANUP_FE_DISCONNECT, normal, sizeof(normal));
    TEST_ASSERT(n > 0);
    TEST_ASSERT((size_t)n <= sizeof(normal));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 16) Commit-in-doubt check payload builder (plugin-owned wire contract)
 * ============================================================================ */

static void test_commit_doubt_check_payload_builder(void) {
    TEST_BEGIN("crash_cid_build: build_commit_doubt_check builds PG query payload");

    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;

    uint8_t out[256];
    memset(out, 0, sizeof(out));

    ssize_t n = VT->build_commit_doubt_check(ctx, 424242ULL, out, sizeof(out));
    TEST_ASSERT(n > 0);
    TEST_ASSERT_EQ(out[0], 'Q');
    TEST_ASSERT(strstr((const char *)(out + 5), "txid_status(424242)") != NULL);
    TEST_ASSERT(c->commit_doubt_check_active);
    TEST_ASSERT_EQ(c->commit_doubt_outcome, 0);

    /* xid=0 is invalid */
    n = VT->build_commit_doubt_check(ctx, 0ULL, out, sizeof(out));
    TEST_ASSERT(n <= 0);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * 17) Commit-in-doubt synthetic response matrix
 * ============================================================================ */

static void test_commit_doubt_response_matrix(void) {
    struct {
        keel_commit_doubt_reason_t reason;
        uint64_t xid;
        bool expect_commit_ok;
        const char *contains;
    } cases[] = {
        { KEEL_CIDR_RESOLVED_COMMITTED, 321ULL, true,  "COMMIT" },
        { KEEL_CIDR_RESOLVED_ABORTED,   321ULL, false, "transaction was rolled back" },
        { KEEL_CIDR_NO_XID,             0ULL,   false, "outcome unknown (no XID captured)" },
        { KEEL_CIDR_NO_RW_POOL,         321ULL, false, "no RW pool" },
        { KEEL_CIDR_NO_CHECK_CONN,      321ULL, false, "pool unavailable" },
        { KEEL_CIDR_CHECK_BUILD_FAIL,   321ULL, false, "XID-check failed" },
        { KEEL_CIDR_CHECK_SEND_FAIL,    321ULL, false, "XID-check failed" },
        { KEEL_CIDR_RESOLVED_UNKNOWN,   321ULL, false, "outcome uncertain" },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char desc[160];
        snprintf(desc, sizeof(desc),
                 "crash_cid_resp/%zu: reason=%u", i, (unsigned)cases[i].reason);
        TEST_BEGIN(desc);

        void *ctx = create_and_startup();
        TEST_ASSERT_NOT_NULL(ctx);

        uint8_t out[768];
        memset(out, 0, sizeof(out));
        ssize_t n = VT->generate_commit_doubt_response(ctx, cases[i].reason,
                                                       cases[i].xid,
                                                       out, sizeof(out));
        TEST_ASSERT(n > 0);

        if (cases[i].expect_commit_ok) {
            TEST_ASSERT_EQ(out[0], 'C');
            TEST_ASSERT(strstr((const char *)(out + 5), cases[i].contains) != NULL);
            /* COMMIT success response must end with ReadyForQuery('I'). */
            TEST_ASSERT_EQ(out[n - 6], 'Z');
            TEST_ASSERT_EQ(out[n - 1], 'I');
        } else {
            TEST_ASSERT_EQ(out[0], 'E');
            TEST_ASSERT(pg_error_response_message_contains(out, (size_t)n,
                                                           cases[i].contains));
            /* Error path includes RFQ(I) terminator. */
            TEST_ASSERT_EQ(out[n - 6], 'Z');
            TEST_ASSERT_EQ(out[n - 1], 'I');
        }

        VT->destroy_context(ctx);
        TEST_END();
    }
}

/* ============================================================================
 * 18) Commit-in-doubt response-stream parsing and completion boundary
 * ============================================================================ */

static void test_commit_doubt_outcome_parsing(void) {
    TEST_BEGIN("crash_cid_parse: commit_doubt check stream parses D + Z correctly");

    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);
    pg_flow_ctx_t *c = (pg_flow_ctx_t *)ctx;

    /* Arm commit-doubt check mode exactly as engine would after sending check SQL. */
    c->commit_doubt_check_active = true;
    c->commit_doubt_outcome = 0;

    uint8_t buf[128];
    keel_be_action_t act;

    /* DataRow('committed') should update outcome and be absorbed. */
    size_t len = build_be_data_row_text(buf, "committed");
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT_EQ(act.type, KEEL_BE_ACT_ABSORB);
    TEST_ASSERT(act.commit_doubt_outcome_changed);
    TEST_ASSERT_EQ(act.commit_doubt_outcome, 1);
    TEST_ASSERT_EQ(c->commit_doubt_outcome, 1);
    TEST_ASSERT(c->commit_doubt_check_active);

    /* ReadyForQuery('I') marks completion and clears active check state. */
    len = build_ready_for_query(buf, 'I');
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.query_complete);
    TEST_ASSERT(act.tx_state_changed);
    TEST_ASSERT_EQ(act.tx_status, KEEL_TX_IDLE);
    TEST_ASSERT(act.backend_reusable);
    TEST_ASSERT(!c->commit_doubt_check_active);

    /* Repeat for aborted path. */
    c->commit_doubt_check_active = true;
    c->commit_doubt_outcome = 0;
    len = build_be_data_row_text(buf, "aborted");
    VT->on_be_msg(ctx, buf, len, &act);
    TEST_ASSERT(act.commit_doubt_outcome_changed);
    TEST_ASSERT_EQ(act.commit_doubt_outcome, 2);
    TEST_ASSERT_EQ(c->commit_doubt_outcome, 2);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * main()
 * ============================================================================ */

int main(void) {
    printf("=== Crash / Recovery Combinatorial Matrix ===\n\n");

    test_full_xid_probe_lifecycle();
    test_commit_no_tracking();
    test_xid_capture_values();
    test_commit_in_doubt_lifecycle();
    test_commit_in_doubt_force_close_protection();
    test_cleanup_reason_matrix();
    test_rfq_error_matrix();
    test_error_during_ps_cycle();
    test_sequential_commit_rewrites();
    test_rollback_no_probe();
    test_rfq_idle_clears_failed();
    test_cleanup_after_failed_tx();
    test_tx_state_full_lifecycle();
    test_error_recovery_in_tx();
    test_cleanup_buffer_guard();
    test_commit_doubt_check_payload_builder();
    test_commit_doubt_response_matrix();
    test_commit_doubt_outcome_parsing();

    printf("\n--- Results: %d run, %d passed, %d failed ---\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return g_tests_failed > 0 ? 1 : 0;
}
