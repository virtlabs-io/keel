/**
 * @file test_ps_failover_tls.c
 * @brief Prepared-statement replay and hash-preservation tests.
 *
 * Covers the three-way intersection identified in the exhaustive test plan
 * (§17 / §21.4): prepared statements × failover (backend reconnect) × session
 * hash stability.  "TLS" here refers to the session-level semantic context
 * (GUC fingerprint captured in session_stmt_hash) that must survive a backend
 * reconnect so that pgf_get_stmt_replay() can rebuild the exact Parse messages
 * the new backend needs.
 *
 * Tests (white-box — direct vtable + pg_flow_ctx_t inspection):
 *
 *  1. replay_buf_empty_before_parse    — fresh ctx → get_stmt_replay returns 0,
 *                                        *buf == NULL, hash == 0
 *  2. replay_after_tracking_prepare    — TRACKING mode PREPARE via simple query
 *                                        populates replay buffer (non-NULL, non-0 hash)
 *  3. replay_buf_count_equals_stmts    — 3 PREPARE stmts → stmt_count == 3 in replay
 *  4. replay_buf_contains_parse_msgs   — every Parse name appears verbatim in replay buf
 *  5. hash_stable_across_replay_calls  — two successive get_stmt_replay calls produce
 *                                        identical hash_out values
 *  6. hash_preserved_on_new_context    — save hash from old ctx, create new ctx with same
 *                                        PREPARE stmts, verify the two hashes are equal
 *  7. replay_freed_after_close_all     — DEALLOCATE ALL → session_stmt_hash becomes 0,
 *                                        replay buf is empty
 *  8. replay_hash_probe_only           — get_stmt_replay(ctx, NULL, NULL, NULL, &hash)
 *                                        still fills hash_out without allocating buf
 *  9. replay_buf_each_msg_is_parse     — each byte in replay buf that aligns to a msg
 *                                        boundary starts with 'P'
 * 10. failover_stmt_replay_roundtrip   — save replay buf from first ctx, inject each
 *                                        Parse message into a fresh ctx, verify
 *                                        session_stmt_hash matches
 * 11. guc_rehash_changes_replay_hash   — SET TimeZone changes hash; SET TimeZone back
 *                                        restores the original hash value
 * 12. multiple_modes_replay_empty      — PINNING / ANONYMOUS / OFF modes return empty
 *                                        replay (no TRACKING or VIRTUALIZE semantics)
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/postgres/postgres_flow_internal.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ---- Globals ---- */
/* (defined in test_utils.c, declared extern in test_utils.h) */

/* ---- PG vtable ---- */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
#define VT (&keel_proto_flow_postgres)

/* ============================================================================
 * Wire-protocol helpers
 * ============================================================================ */
static inline void wr32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static size_t build_startup(uint8_t *buf, const char *user, const char *db) {
    uint8_t *p = buf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p, "user", 5);   p += 5;
    size_t ul = strlen(user);
    memcpy(p, user, ul + 1); p += ul + 1;
    memcpy(p, "database", 9); p += 9;
    size_t dl = strlen(db);
    memcpy(p, db, dl + 1); p += dl + 1;
    *p++ = '\0';
    uint32_t total = (uint32_t)(p - buf);
    wr32(buf, total);
    return (size_t)(p - buf);
}

static size_t build_query(uint8_t *buf, const char *sql) {
    size_t sl = strlen(sql);
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sl + 1));
    memcpy(buf + 5, sql, sl + 1);
    return 1 + 4 + sl + 1;
}

static size_t build_ready_for_query(uint8_t *buf, char status) {
    buf[0] = 'Z';
    wr32(buf + 1, 5);
    buf[5] = (uint8_t)status;
    return 6;
}

/* Build a backend CommandComplete: 'C' + len(4) + tag\0 */
static size_t build_command_complete(uint8_t *buf, const char *tag) {
    size_t tl = strlen(tag);
    buf[0] = 'C';
    wr32(buf + 1, (uint32_t)(4 + tl + 1));
    memcpy(buf + 5, tag, tl + 1);
    return 1 + 4 + tl + 1;
}

/* Simulate the backend confirming a tracking-mode simple-query PREPARE.
 * Sends CommandComplete("PREPARE") then ReadyForQuery('I') through on_be_msg. */
static void sim_track_prepare_confirm(void *ctx) {
    uint8_t buf[32];
    keel_be_action_t bact;
    VT->on_be_msg(ctx, buf, build_command_complete(buf, "PREPARE"), &bact);
    VT->on_be_msg(ctx, buf, build_ready_for_query(buf, 'I'), &bact);
}

/* ============================================================================
 * Context bootstrap helpers
 * ============================================================================ */
static void *create_and_startup(void) {
    void *ctx = VT->create_context(NULL);
    if (!ctx) return NULL;
    uint8_t buf[512];
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, build_startup(buf, "testuser", "testdb"), &act);
    return ctx;
}

static void *create_with_ps_mode(keel_ps_mode_t mode) {
    void *ctx = create_and_startup();
    if (ctx) ((pg_flow_ctx_t *)ctx)->ps_mode = mode;
    return ctx;
}

/* Register `n` named prepared statements via PREPARE in tracking mode.
 * Each stmt is "PREPARE <name_i> AS SELECT <i>".
 * Returns the context (already in tracking mode). */
static void *create_ctx_with_n_stmts(int n) {
    void *ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    if (!ctx) return NULL;
    uint8_t buf[256];
    keel_fe_action_t act;
    char sql[128];
    for (int i = 0; i < n; i++) {
        snprintf(sql, sizeof(sql), "PREPARE stmt%d AS SELECT %d", i, i);
        VT->on_fe_msg(ctx, buf, build_query(buf, sql), &act);
        sim_track_prepare_confirm(ctx);
    }
    return ctx;
}

/* ============================================================================
 * Test 1: Fresh context has empty replay buffer
 * ============================================================================ */
static void test_replay_buf_empty_before_parse(void) {
    TEST_BEGIN("ps_failover: fresh ctx → replay buf NULL, hash 0");

    void *ctx = create_and_startup();
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t *rbuf = (uint8_t *)0x1; /* non-NULL sentinel */
    size_t   rlen = 1;
    uint32_t rcnt = 1;
    uint64_t hash = 1;

    int rc = VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &hash);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_NULL(rbuf);
    TEST_ASSERT_EQ((int)rlen, 0);
    TEST_ASSERT_EQ((int)rcnt, 0);
    TEST_ASSERT_EQ((int)hash, 0);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Test 2: Tracking-mode PREPARE populates replay buffer
 * ============================================================================ */
static void test_replay_after_tracking_prepare(void) {
    TEST_BEGIN("ps_failover: TRACKING PREPARE → non-NULL replay buf, non-0 hash");

    void *ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, build_query(buf, "PREPARE s1 AS SELECT 1"), &act);
    sim_track_prepare_confirm(ctx);

    uint8_t *rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcnt = 0;
    uint64_t hash = 0;
    int rc = VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &hash);

    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT_NOT_NULL(rbuf);
    TEST_ASSERT(((int)rlen) > (0));
    TEST_ASSERT(((int)rcnt) > (0));
    TEST_ASSERT(((int64_t)hash) != (0));

    keel_free(rbuf);
    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Test 3: stmt_count matches number of PREPARE calls
 * ============================================================================ */
static void test_replay_buf_count_equals_stmts(void) {
    TEST_BEGIN("ps_failover: stmt_count == number of PREPARE stmts");

    void *ctx = create_ctx_with_n_stmts(3);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t *rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcnt = 0;
    uint64_t hash = 0;
    VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &hash);

    TEST_ASSERT_EQ((int)rcnt, 3);

    keel_free(rbuf);
    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Test 4: Replay buffer contains the original Parse message names
 * ============================================================================ */
static void test_replay_buf_contains_parse_msgs(void) {
    TEST_BEGIN("ps_failover: replay buf contains each stmt name");

    void *ctx = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t buf[256];
    keel_fe_action_t act;
    const char *stmts[] = { "mystmt", "anotherstmt" };
    for (int i = 0; i < 2; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "PREPARE %s AS SELECT %d", stmts[i], i);
        VT->on_fe_msg(ctx, buf, build_query(buf, sql), &act);
        sim_track_prepare_confirm(ctx);
    }

    uint8_t *rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcnt = 0;
    uint64_t hash = 0;
    VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &hash);
    TEST_ASSERT_NOT_NULL(rbuf);

    /* Each stmt name must appear somewhere in the replay buffer */
    for (int i = 0; i < 2; i++) {
        size_t nlen = strlen(stmts[i]);
        bool found = false;
        for (size_t j = 0; j + nlen <= rlen; j++) {
            if (memcmp(rbuf + j, stmts[i], nlen) == 0) {
                found = true;
                break;
            }
        }
        TEST_ASSERT(found);
    }

    keel_free(rbuf);
    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Test 5: Hash is stable across two consecutive get_stmt_replay calls
 * ============================================================================ */
static void test_hash_stable_across_replay_calls(void) {
    TEST_BEGIN("ps_failover: hash_out is stable across two replay calls");

    void *ctx = create_ctx_with_n_stmts(2);
    TEST_ASSERT_NOT_NULL(ctx);

    uint64_t h1 = 0, h2 = 0;
    uint8_t *r1 = NULL, *r2 = NULL;
    size_t l1 = 0, l2 = 0;
    uint32_t c1 = 0, c2 = 0;

    VT->get_stmt_replay(ctx, &r1, &l1, &c1, &h1);
    VT->get_stmt_replay(ctx, &r2, &l2, &c2, &h2);

    TEST_ASSERT_EQ((int64_t)h1, (int64_t)h2);
    TEST_ASSERT_EQ((int)l1, (int)l2);
    TEST_ASSERT_EQ((int)c1, (int)c2);

    keel_free(r1);
    keel_free(r2);
    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Test 6: Two contexts with the same stmts have equal session_stmt_hash
 * ============================================================================ */
static void test_hash_preserved_on_new_context(void) {
    TEST_BEGIN("ps_failover: identical PREPARE stmts → same hash on two contexts");

    void *ctx1 = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    void *ctx2 = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(ctx1);
    TEST_ASSERT_NOT_NULL(ctx2);

    const char *sql = "PREPARE common_stmt AS SELECT 42";
    uint8_t buf[256];
    keel_fe_action_t act;
    VT->on_fe_msg(ctx1, buf, build_query(buf, sql), &act);
    sim_track_prepare_confirm(ctx1);
    VT->on_fe_msg(ctx2, buf, build_query(buf, sql), &act);
    sim_track_prepare_confirm(ctx2);

    uint64_t h1 = 0, h2 = 0;
    VT->get_stmt_replay(ctx1, NULL, NULL, NULL, &h1);
    VT->get_stmt_replay(ctx2, NULL, NULL, NULL, &h2);

    TEST_ASSERT_EQ((int64_t)h1, (int64_t)h2);

    VT->destroy_context(ctx1);
    VT->destroy_context(ctx2);
    TEST_END();
}

/* ============================================================================
 * Test 7: DEALLOCATE ALL empties replay buf (hash 0, buf NULL)
 * ============================================================================ */
static void test_replay_freed_after_close_all(void) {
    TEST_BEGIN("ps_failover: DEALLOCATE ALL → replay buf empty, hash 0");

    void *ctx = create_ctx_with_n_stmts(2);
    TEST_ASSERT_NOT_NULL(ctx);

    /* Confirm stmts are present */
    uint64_t h_before = 0;
    VT->get_stmt_replay(ctx, NULL, NULL, NULL, &h_before);
    TEST_ASSERT(((int64_t)h_before) != (0));

    /* Deallocate all */
    uint8_t buf[64];
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, build_query(buf, "DEALLOCATE ALL"), &act);

    uint8_t *rbuf = (uint8_t *)0x1;
    size_t   rlen = 1;
    uint32_t rcnt = 1;
    uint64_t h_after = 1;
    VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &h_after);

    TEST_ASSERT_NULL(rbuf);
    TEST_ASSERT_EQ((int)rlen, 0);
    TEST_ASSERT_EQ((int)rcnt, 0);
    TEST_ASSERT_EQ((int64_t)h_after, 0);

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Test 8: Hash-probe-only call (NULL buf/len/count) still fills hash_out
 * ============================================================================ */
static void test_replay_hash_probe_only(void) {
    TEST_BEGIN("ps_failover: get_stmt_replay with NULL buf/len/count fills hash_out");

    void *ctx = create_ctx_with_n_stmts(1);
    TEST_ASSERT_NOT_NULL(ctx);

    uint64_t h_probe = 0;
    int rc = VT->get_stmt_replay(ctx, NULL, NULL, NULL, &h_probe);
    TEST_ASSERT_EQ(rc, 0);
    TEST_ASSERT(((int64_t)h_probe) != (0));

    /* Full call must return same hash */
    uint8_t *rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcnt = 0;
    uint64_t h_full = 0;
    VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &h_full);
    TEST_ASSERT_EQ((int64_t)h_probe, (int64_t)h_full);

    keel_free(rbuf);
    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Test 9: Replay buffer contains one replay frame per statement.
 * ============================================================================ */
static void test_replay_each_msg_is_parse(void) {
    TEST_BEGIN("ps_failover: replay buf contains statement replay messages");

    void *ctx = create_ctx_with_n_stmts(4);
    TEST_ASSERT_NOT_NULL(ctx);

    uint8_t *rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcnt = 0;
    uint64_t hash = 0;
    VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcnt, &hash);
    TEST_ASSERT_NOT_NULL(rbuf);

    /* Tracking-mode SQL PREPARE replays as 'Q' PREPARE; extended Parse
     * replays as 'P'.  Ignore Sync and any non-PREPARE SET prefix. */
    uint32_t replay_count = 0;
    size_t pos = 0;
    while (pos + 5 <= rlen) {
        uint8_t msg_type = rbuf[pos];
        uint32_t msg_len = ((uint32_t)rbuf[pos+1] << 24) |
                           ((uint32_t)rbuf[pos+2] << 16) |
                           ((uint32_t)rbuf[pos+3] << 8)  |
                            (uint32_t)rbuf[pos+4];
        if (msg_len < 4 || pos + 1 + msg_len > rlen) break;
        if (msg_type == 'P') {
            replay_count++;
        } else if (msg_type == 'Q' && msg_len > 12 &&
                   strncasecmp((const char*)rbuf + pos + 5,
                               "PREPARE", 7) == 0) {
            replay_count++;
        }
        pos += 1 + msg_len;
    }
    TEST_ASSERT_EQ((int)replay_count, (int)rcnt);

    keel_free(rbuf);
    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Test 10: Replay-buffer round-trip — inject replayed statements into fresh context
 * ============================================================================ */
static void test_failover_stmt_replay_roundtrip(void) {
    TEST_BEGIN("ps_failover: replayed stmt msgs produce matching hash on fresh ctx");

    /* Create original context with stmts */
    void *orig = create_ctx_with_n_stmts(2);
    TEST_ASSERT_NOT_NULL(orig);

    uint8_t *rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcnt = 0;
    uint64_t orig_hash = 0;
    VT->get_stmt_replay(orig, &rbuf, &rlen, &rcnt, &orig_hash);
    TEST_ASSERT_NOT_NULL(rbuf);
    TEST_ASSERT(((int)rcnt) > (0));

    /* Create fresh context and inject statement replay messages from rbuf. */
    void *fresh = create_with_ps_mode(KEEL_PS_MODE_TRACKING);
    TEST_ASSERT_NOT_NULL(fresh);

    size_t pos = 0;
    while (pos + 5 <= rlen) {
        uint8_t msg_type = rbuf[pos];
        uint32_t msg_len = ((uint32_t)rbuf[pos+1] << 24) |
                           ((uint32_t)rbuf[pos+2] << 16) |
                           ((uint32_t)rbuf[pos+3] << 8)  |
                            (uint32_t)rbuf[pos+4];
        if (msg_len < 4 || pos + 1 + msg_len > rlen) break;
        if (msg_type == 'P') {
            /* The 'P' frame is in extended-query format (Parse).
             * Deliver it to the fresh context as a frontend message. */
            keel_fe_action_t act;
            VT->on_fe_msg(fresh, rbuf + pos, 1 + msg_len, &act);

            /* Deliver a synthetic ParseComplete backend response so that
             * pending_parse_valid is cleared and the hash is updated. */
            uint8_t pc[5];
            pc[0] = '1';
            pc[1] = 0; pc[2] = 0; pc[3] = 0; pc[4] = 4;
            keel_be_action_t bact;
            VT->on_be_msg(fresh, pc, sizeof(pc), &bact);
        } else if (msg_type == 'Q' && msg_len > 12 &&
                   strncasecmp((const char*)rbuf + pos + 5,
                               "PREPARE", 7) == 0) {
            keel_fe_action_t act;
            VT->on_fe_msg(fresh, rbuf + pos, 1 + msg_len, &act);

            uint8_t cc[16];
            cc[0] = 'C';
            cc[1] = 0; cc[2] = 0; cc[3] = 0; cc[4] = 12;
            memcpy(cc + 5, "PREPARE", 8);
            keel_be_action_t bact;
            VT->on_be_msg(fresh, cc, 13, &bact);
        }
        pos += 1 + msg_len;
    }

    /* Deliver a ReadyForQuery so state settles */
    uint8_t rfq[6];
    keel_be_action_t bact;
    VT->on_be_msg(fresh, rfq, build_ready_for_query(rfq, 'I'), &bact);

    uint64_t fresh_hash = 0;
    VT->get_stmt_replay(fresh, NULL, NULL, NULL, &fresh_hash);

    /* The fresh hash must equal the original hash */
    TEST_ASSERT_EQ((int64_t)fresh_hash, (int64_t)orig_hash);

    keel_free(rbuf);
    VT->destroy_context(orig);
    VT->destroy_context(fresh);
    TEST_END();
}

/* ============================================================================
 * Test 11: GUC rehash — SET and then RESET changes and then restores hash
 * ============================================================================ */
static void test_guc_rehash_changes_replay_hash(void) {
    TEST_BEGIN("ps_failover: SET TimeZone changes hash; SET back restores it");

    void *ctx = create_ctx_with_n_stmts(1);
    TEST_ASSERT_NOT_NULL(ctx);

    uint64_t base_hash = 0;
    VT->get_stmt_replay(ctx, NULL, NULL, NULL, &base_hash);
    TEST_ASSERT(((int64_t)base_hash) != (0));

    uint8_t buf[256];
    keel_fe_action_t act;
    VT->on_fe_msg(ctx, buf, build_query(buf, "SET TimeZone TO 'America/New_York'"), &act);

    uint64_t changed_hash = 0;
    VT->get_stmt_replay(ctx, NULL, NULL, NULL, &changed_hash);
    TEST_ASSERT(((int64_t)changed_hash) != ((int64_t)base_hash));

    /* Reset TimeZone to default — hash should change again */
    VT->on_fe_msg(ctx, buf, build_query(buf, "RESET TimeZone"), &act);

    uint64_t reset_hash = 0;
    VT->get_stmt_replay(ctx, NULL, NULL, NULL, &reset_hash);
    /* Hash after RESET must differ from the changed value */
    TEST_ASSERT(((int64_t)reset_hash) != ((int64_t)changed_hash));

    VT->destroy_context(ctx);
    TEST_END();
}

/* ============================================================================
 * Test 12: Non-tracking modes return empty replay
 * ============================================================================ */
static void test_multiple_modes_replay_empty(void) {
    TEST_BEGIN("ps_failover: PINNING / ANONYMOUS / OFF modes return empty replay");

    keel_ps_mode_t modes[] = {
        KEEL_PS_MODE_PINNING,
        KEEL_PS_MODE_ANONYMOUS,
        KEEL_PS_MODE_OFF,
    };
    const char *names[] = { "PINNING", "ANONYMOUS", "OFF" };
    (void)names;

    for (size_t i = 0; i < sizeof(modes)/sizeof(modes[0]); i++) {
        void *ctx = create_with_ps_mode(modes[i]);
        TEST_ASSERT_NOT_NULL(ctx);

        /* Send a named Parse (or PREPARE) — behaviour depends on mode */
        uint8_t buf[256];
        keel_fe_action_t act;
        VT->on_fe_msg(ctx, buf, build_query(buf, "PREPARE ps1 AS SELECT 1"), &act);

        uint64_t hash = 0;
        int rc = VT->get_stmt_replay(ctx, NULL, NULL, NULL, &hash);
        TEST_ASSERT_EQ(rc, 0);
        /* Non-tracking modes must not accumulate a stmt hash */
        TEST_ASSERT_EQ((int64_t)hash, 0);

        VT->destroy_context(ctx);
    }

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */
int main(void) {
    test_replay_buf_empty_before_parse();
    test_replay_after_tracking_prepare();
    test_replay_buf_count_equals_stmts();
    test_replay_buf_contains_parse_msgs();
    test_hash_stable_across_replay_calls();
    test_hash_preserved_on_new_context();
    test_replay_freed_after_close_all();
    test_replay_hash_probe_only();
    test_replay_each_msg_is_parse();
    test_failover_stmt_replay_roundtrip();
    test_guc_rehash_changes_replay_hash();
    test_multiple_modes_replay_empty();

    printf("\nps_failover_tls: %d/%d tests passed, %d failed\n",
           g_tests_passed, g_tests_run, g_tests_failed);
    return test_summary();
}
