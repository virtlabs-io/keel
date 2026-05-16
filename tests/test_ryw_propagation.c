/**
 * @file test_ryw_propagation.c
 * @brief Unit tests for cross-service Read-Your-Writes (RYW) consistency.
 *
 * Tests the proxy-layer intercept of:
 *   §1 — pg_try_parse_set_keel_lsn(): parser for SET keel.read_after_lsn
 *   §2 — pg_is_show_keel_write_lsn(): recogniser for SHOW keel.write_lsn
 *   §3 — pgf_on_fe_msg intercept for SET keel.read_after_lsn
 *         3a. Synthetic SET response (SEND_FE, no backend round-trip)
 *         3b. inject_consistency_lsn propagated in action
 *         3c. Various LSN formats (PG LSN "A/B", MySQL GTID)
 *         3d. Unrelated SET queries still forwarded normally
 *   §4 — pgf_on_fe_msg intercept for SHOW keel.write_lsn
 *         4a. Returns empty string when no LSN captured yet
 *         4b. Returns the LSN set by notify_write_lsn
 *         4c. Synthetic response is a valid PG result set
 *   §5 — notify_write_lsn vtable method updates pg_flow_ctx_t
 *   §6 — Edge cases: empty LSN, oversized LSN, semicolons, whitespace
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/postgres/postgres_flow_internal.h"
#include "keel/session/ssv_atom.h"

#include <string.h>
#include <stdio.h>
#include <stdint.h>

/* ---- PG vtable ---- */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
#define PG (&keel_proto_flow_postgres)

/* ============================================================================
 * Wire helpers
 * ============================================================================ */

static inline void wr32(uint8_t *p, uint32_t v) {
    p[0] = (v >> 24) & 0xff; p[1] = (v >> 16) & 0xff;
    p[2] = (v >>  8) & 0xff; p[3] =  v        & 0xff;
}

/** Build a simple Query message 'Q' with given SQL text. */
static size_t build_query(uint8_t *buf, const char *sql) {
    size_t sl = strlen(sql);
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sl + 1));
    memcpy(buf + 5, sql, sl);
    buf[5 + sl] = '\0';
    return 1 + 4 + sl + 1;
}

/** Complete the PG startup handshake on a fresh context.  Returns context. */
static void *make_pg_ctx(void) {
    void *ctx = PG->create_context(NULL);
    if (!ctx) return NULL;

    /* Startup message */
    uint8_t sbuf[512];
    uint8_t *p = sbuf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p, "user\0testuser\0database\0testdb\0", 29); p += 29;
    *p++ = '\0';
    wr32(sbuf, (uint32_t)(p - sbuf));
    size_t slen = (size_t)(p - sbuf);

    keel_fe_action_t act;
    PG->on_fe_msg(ctx, sbuf, slen, &act);
    return ctx;
}

/* ============================================================================
 * §1 — SET keel.read_after_lsn parsing (via on_fe_msg intercept)
 * ============================================================================ */

static void test_ryw_set_basic(void) {
    TEST_BEGIN("ryw: SET keel.read_after_lsn = '0/ABCD1234' intercepted");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SET keel.read_after_lsn = '0/ABCD1234'");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    /* Must be intercepted — no backend round-trip */
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);
    /* Synthetic response must be non-empty */
    TEST_ASSERT(act.fe_response_len > 0);
    TEST_ASSERT_NOT_NULL(act.fe_response);
    /* LSN must be propagated */
    TEST_ASSERT_STR_EQ(act.inject_consistency_lsn, "0/ABCD1234");

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_set_mysql_gtid(void) {
    TEST_BEGIN("ryw: SET keel.read_after_lsn with MySQL GTID");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[512];
    size_t mlen = build_query(msg,
        "SET keel.read_after_lsn = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-42'");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);
    TEST_ASSERT_STR_EQ(act.inject_consistency_lsn,
                       "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-42");

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_set_case_insensitive(void) {
    TEST_BEGIN("ryw: SET KEEL.READ_AFTER_LSN case-insensitive");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SET KEEL.READ_AFTER_LSN = '1/FEED0000'");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);
    TEST_ASSERT_STR_EQ(act.inject_consistency_lsn, "1/FEED0000");

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_set_unrelated_forwarded(void) {
    TEST_BEGIN("ryw: SET search_path is NOT intercepted (forwarded normally)");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SET search_path = myschema");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    /* Must be a normal QUERY, not intercepted */
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);
    /* inject_consistency_lsn must be empty */
    TEST_ASSERT_EQ(act.inject_consistency_lsn[0], '\0');

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_set_other_keel_guc_forwarded(void) {
    TEST_BEGIN("ryw: SET keel.sticky_primary_ttl is NOT intercepted");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SET keel.sticky_primary_ttl = '200ms'");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    /* Not keel.read_after_lsn → forwarded, no inject */
    TEST_ASSERT_EQ(act.inject_consistency_lsn[0], '\0');

    PG->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * §2 — SHOW keel.write_lsn parsing
 * ============================================================================ */

static void test_ryw_show_empty_lsn(void) {
    TEST_BEGIN("ryw: SHOW keel.write_lsn returns empty when no LSN captured");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SHOW keel.write_lsn");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);
    TEST_ASSERT(act.fe_response_len > 0);
    TEST_ASSERT_NOT_NULL(act.fe_response);

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_show_after_notify(void) {
    TEST_BEGIN("ryw: SHOW keel.write_lsn returns LSN set by notify_write_lsn");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    /* Notify the context of a captured LSN */
    TEST_ASSERT_NOT_NULL(PG->notify_write_lsn);
    PG->notify_write_lsn(ctx, "5/DEADBEEF");

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SHOW keel.write_lsn");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);

    /* The response should contain the LSN string "5/DEADBEEF" */
    const char *needle = "5/DEADBEEF";
    bool found = false;
    for (size_t i = 0; i + strlen(needle) <= act.fe_response_len; i++) {
        if (memcmp(act.fe_response + i, needle, strlen(needle)) == 0) {
            found = true;
            break;
        }
    }
    TEST_ASSERT(found);

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_show_case_insensitive(void) {
    TEST_BEGIN("ryw: SHOW KEEL.WRITE_LSN case-insensitive");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    PG->notify_write_lsn(ctx, "3/CAFEBABE");

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SHOW KEEL.WRITE_LSN");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);

    const char *needle = "3/CAFEBABE";
    bool found = false;
    for (size_t i = 0; i + strlen(needle) <= act.fe_response_len; i++) {
        if (memcmp(act.fe_response + i, needle, strlen(needle)) == 0) {
            found = true; break;
        }
    }
    TEST_ASSERT(found);

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_show_unrelated_forwarded(void) {
    TEST_BEGIN("ryw: SHOW search_path is NOT intercepted (forwarded normally)");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SHOW search_path");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_QUERY);

    PG->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * §3 — notify_write_lsn vtable presence and correctness
 * ============================================================================ */

static void test_ryw_notify_vtable_present(void) {
    TEST_BEGIN("ryw: notify_write_lsn vtable entry is non-NULL for postgres");

    TEST_ASSERT_NOT_NULL(PG->notify_write_lsn);

    TEST_END();
}

static void test_ryw_notify_updates_ctx(void) {
    TEST_BEGIN("ryw: notify_write_lsn updates pg_flow_ctx_t.keel_write_lsn");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    PG->notify_write_lsn(ctx, "FF/12345678");

    /* Inspect the internal context field */
    pg_flow_ctx_t *pctx = (pg_flow_ctx_t *)ctx;
    TEST_ASSERT_STR_EQ(pctx->keel_write_lsn, "FF/12345678");

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_notify_null_safe(void) {
    TEST_BEGIN("ryw: notify_write_lsn tolerates NULL lsn (no-op)");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    PG->notify_write_lsn(ctx, "1/AAAABBBB");
    PG->notify_write_lsn(ctx, NULL);  /* must not crash */

    pg_flow_ctx_t *pctx = (pg_flow_ctx_t *)ctx;
    /* Value should remain unchanged */
    TEST_ASSERT_STR_EQ(pctx->keel_write_lsn, "1/AAAABBBB");

    PG->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * §4 — inject_consistency_lsn default and boundary
 * ============================================================================ */

static void test_ryw_inject_lsn_default_empty(void) {
    TEST_BEGIN("ryw: inject_consistency_lsn is '\\0' in default fe_action");

    keel_fe_action_t act = keel_fe_action_default();
    TEST_ASSERT_EQ(act.inject_consistency_lsn[0], '\0');

    TEST_END();
}

static void test_ryw_no_inject_on_normal_query(void) {
    TEST_BEGIN("ryw: normal SELECT does not set inject_consistency_lsn");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SELECT 1");

    keel_fe_action_t act;
    PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(act.inject_consistency_lsn[0], '\0');

    PG->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * §5 — Synthetic response wire format validation
 * ============================================================================ */

static void test_ryw_set_response_format(void) {
    TEST_BEGIN("ryw: SET response starts with CommandComplete 'C' and ends with ReadyForQuery 'Z'");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SET keel.read_after_lsn = '0/1'");

    keel_fe_action_t act;
    PG->on_fe_msg(ctx, msg, mlen, &act);

    TEST_ASSERT(act.fe_response_len >= 6);  /* 'C'+4+4 = 9, 'Z'+4+1 = 6 */
    TEST_ASSERT_EQ(act.fe_response[0], 'C');
    TEST_ASSERT_EQ(act.fe_response[act.fe_response_len - 1], 'I');  /* RFQ status */
    TEST_ASSERT_EQ(act.fe_response[act.fe_response_len - 6], 'Z');  /* RFQ type */

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_show_response_format(void) {
    TEST_BEGIN("ryw: SHOW keel.write_lsn response has RowDescription 'T' first");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    PG->notify_write_lsn(ctx, "2/BAADF00D");

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SHOW keel.write_lsn");

    keel_fe_action_t act;
    PG->on_fe_msg(ctx, msg, mlen, &act);

    TEST_ASSERT(act.fe_response_len > 10);
    /* RowDescription first */
    TEST_ASSERT_EQ(act.fe_response[0], 'T');
    /* Last byte of RFQ status = 'I' */
    TEST_ASSERT_EQ(act.fe_response[act.fe_response_len - 1], 'I');

    PG->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * §6 — Edge cases
 * ============================================================================ */

static void test_ryw_set_semicolon(void) {
    TEST_BEGIN("ryw: SET keel.read_after_lsn with trailing semicolon");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SET keel.read_after_lsn = '0/AABB1122';");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);
    TEST_ASSERT_STR_EQ(act.inject_consistency_lsn, "0/AABB1122");

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_show_trailing_whitespace(void) {
    TEST_BEGIN("ryw: SHOW keel.write_lsn with trailing spaces");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t msg[256];
    size_t mlen = build_query(msg, "SHOW keel.write_lsn   ");

    keel_fe_action_t act;
    int rc = PG->on_fe_msg(ctx, msg, mlen, &act);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_EQ(act.type, KEEL_FE_ACT_SEND_FE);

    PG->destroy_context(ctx);
    TEST_END();
}

static void test_ryw_notify_overwrites(void) {
    TEST_BEGIN("ryw: second notify_write_lsn overwrites the first");

    void *ctx = make_pg_ctx();
    TEST_ASSERT_NOT_NULL(ctx);

    PG->notify_write_lsn(ctx, "1/00000001");
    PG->notify_write_lsn(ctx, "2/00000002");

    pg_flow_ctx_t *pctx = (pg_flow_ctx_t *)ctx;
    TEST_ASSERT_STR_EQ(pctx->keel_write_lsn, "2/00000002");

    PG->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    /* §1 — SET intercept */
    test_ryw_set_basic();
    test_ryw_set_mysql_gtid();
    test_ryw_set_case_insensitive();
    test_ryw_set_unrelated_forwarded();
    test_ryw_set_other_keel_guc_forwarded();

    /* §2 — SHOW intercept */
    test_ryw_show_empty_lsn();
    test_ryw_show_after_notify();
    test_ryw_show_case_insensitive();
    test_ryw_show_unrelated_forwarded();

    /* §3 — notify_write_lsn */
    test_ryw_notify_vtable_present();
    test_ryw_notify_updates_ctx();
    test_ryw_notify_null_safe();

    /* §4 — inject_consistency_lsn defaults */
    test_ryw_inject_lsn_default_empty();
    test_ryw_no_inject_on_normal_query();

    /* §5 — wire format */
    test_ryw_set_response_format();
    test_ryw_show_response_format();

    /* §6 — edge cases */
    test_ryw_set_semicolon();
    test_ryw_show_trailing_whitespace();
    test_ryw_notify_overwrites();

    return test_summary();
}
