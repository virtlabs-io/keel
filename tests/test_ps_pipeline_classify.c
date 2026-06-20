/**
 * @file test_ps_pipeline_classify.c
 * @brief Regression: pipelined Parse messages stashed in pre-query follow
 *        buffer must be classified into the per-session PS cache.
 *
 * Bug history: when state_sync / stmt_replay / deferred BEGIN / full
 * cleanup was triggered by the *first* FE message in a pipelined batch,
 * the engine stashed the entire tail (`data + pos`, length `len - pos`)
 * verbatim for later forwarding to the backend.  Only the trigger
 * message was passed through `on_fe_msg`; any Parse(S_N) further inside
 * the same TCP segment was forwarded to the backend without ever
 * touching KEEL's stmt cache.  On the next pool borrow + DISCARD ALL +
 * replay, KEEL had no record of S_N, so pgjdbc's subsequent Bind(S_N)
 * hit "prepared statement S_N does not exist" (SQLSTATE 26000).
 *
 * This file pins the contract that the pre-classify helper walks all
 * remaining frames in the follow buffer and feeds each to on_fe_msg,
 * leaving the cache populated for replay before the bytes ever reach
 * the backend.
 */

#include "test_utils.h"

#include "keel/engine/engine_flow.h"
#include "keel/engine/backend_pool.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/postgres/postgres_flow_internal.h"
#include "keel/session/session.h"
#include "keel/mem/mem.h"

#include <stdint.h>
#include <string.h>

extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
#define VT (&keel_proto_flow_postgres)

/* Forward declaration of the test-exposed pre-classify helper from
 * src/engine/engine_flow.c.  Not part of the public engine_flow API. */
extern void keel_engine_flow_prefetch_classify_follow_buf(
    keel_session_flow_t* sf,
    const uint8_t* buf,
    size_t buf_len,
    size_t skip_bytes);

/* ---- Tiny PG wire encoders (kept local; full set lives in
 * test_pg_protocol_flow.c / test_pre_query_replay.c). ---- */

static void wr32(uint8_t* p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)(v);
}

static size_t build_startup(uint8_t* buf, const char* user, const char* db)
{
    uint8_t* p = buf + 4;
    wr32(p, 0x00030000); p += 4;
    memcpy(p, "user", 5); p += 5;
    size_t ul = strlen(user); memcpy(p, user, ul + 1); p += ul + 1;
    memcpy(p, "database", 9); p += 9;
    size_t dl = strlen(db); memcpy(p, db, dl + 1); p += dl + 1;
    *p++ = '\0';
    wr32(buf, (uint32_t)(p - buf));
    return (size_t)(p - buf);
}

static size_t build_parse(uint8_t* buf, const char* name, const char* sql)
{
    size_t nl = strlen(name);
    size_t sl = strlen(sql);
    size_t body = nl + 1 + sl + 1 + 2;
    buf[0] = 'P';
    wr32(buf + 1, (uint32_t)(4 + body));
    memcpy(buf + 5, name, nl + 1);
    memcpy(buf + 5 + nl + 1, sql, sl + 1);
    buf[5 + nl + 1 + sl + 1] = 0;
    buf[5 + nl + 1 + sl + 2] = 0;
    return 1 + 4 + body;
}

static size_t build_bind(uint8_t* buf, const char* portal, const char* stmt)
{
    size_t pl = strlen(portal);
    size_t sl = strlen(stmt);
    size_t body = pl + 1 + sl + 1 + 2 + 2 + 2;
    buf[0] = 'B';
    wr32(buf + 1, (uint32_t)(4 + body));
    uint8_t* p = buf + 5;
    memcpy(p, portal, pl + 1); p += pl + 1;
    memcpy(p, stmt, sl + 1);   p += sl + 1;
    p[0] = 0; p[1] = 0; p += 2;
    p[0] = 0; p[1] = 0; p += 2;
    p[0] = 0; p[1] = 0;
    return 1 + 4 + body;
}

static size_t build_execute(uint8_t* buf, const char* portal)
{
    size_t pl = strlen(portal);
    size_t body = pl + 1 + 4;
    buf[0] = 'E';
    wr32(buf + 1, (uint32_t)(4 + body));
    memcpy(buf + 5, portal, pl + 1);
    wr32(buf + 5 + pl + 1, 0);
    return 1 + 4 + body;
}

static size_t build_sync(uint8_t* buf)
{
    buf[0] = 'S';
    wr32(buf + 1, 4);
    return 5;
}

/* Build minimal backend ParseComplete: '1' + len(4). */
static size_t build_parse_complete(uint8_t* buf)
{
    buf[0] = '1';
    wr32(buf + 1, 4);
    return 5;
}

/* Build minimal backend ReadyForQuery: 'Z' + len(5) + status(1). */
static size_t build_rfq(uint8_t* buf, char status)
{
    buf[0] = 'Z';
    wr32(buf + 1, 5);
    buf[5] = (uint8_t)status;
    return 6;
}

/* Build a Simple Query ('Q') message: 'Q' + len(4) + sql + NUL. */
static size_t build_query(uint8_t* buf, const char* sql)
{
    size_t sl = strlen(sql);
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sl + 1));
    memcpy(buf + 5, sql, sl + 1);
    return 1 + 4 + sl + 1;
}

/* Build a backend CommandComplete ('C') message with a tag. */
static size_t build_command_complete(uint8_t* buf, const char* tag)
{
    size_t tl = strlen(tag);
    buf[0] = 'C';
    wr32(buf + 1, (uint32_t)(4 + tl + 1));
    memcpy(buf + 5, tag, tl + 1);
    return 1 + 4 + tl + 1;
}

/* Build a backend ErrorResponse ('E') with SQLSTATE 42601 (syntax_error)
 * and a human-readable message.  Used to simulate a failed Parse. */
static size_t build_error_response(uint8_t* buf, const char* msg)
{
    buf[0] = 'E';
    uint8_t* p = buf + 5;
    *p++ = 'S'; const char* sev = "ERROR"; size_t sl = strlen(sev);
    memcpy(p, sev, sl + 1); p += sl + 1;
    *p++ = 'C'; const char* code = "42601"; size_t cl = strlen(code);
    memcpy(p, code, cl + 1); p += cl + 1;
    *p++ = 'M'; size_t ml = strlen(msg);
    memcpy(p, msg, ml + 1); p += ml + 1;
    *p++ = '\0';
    size_t body = (size_t)(p - buf) - 1;
    wr32(buf + 1, (uint32_t)body);
    return 1 + body;
}

/* Return true if @p ctx has any cache entry (valid, any confirmed state)
 * whose name matches @p name. */
static bool stmt_cache_has(const pg_flow_ctx_t* ctx, const char* name)
{
    for (size_t i = 0; i < PG_STMT_CACHE_SIZE; ++i) {
        if (ctx->stmt_cache[i].valid &&
            strcmp(ctx->stmt_cache[i].name, name) == 0)
            return true;
    }
    return false;
}

static void* startup_pg_ctx(void)
{
    void* ctx = VT->create_context(NULL);
    TEST_ASSERT_NOT_NULL(ctx);
    uint8_t buf[256];
    keel_fe_action_t act = keel_fe_action_default();
    size_t n = build_startup(buf, "testuser", "testdb");
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, n, &act), 0);
    return ctx;
}

/* -------------------------------------------------------------------------
 * Test 1: prefetch_classify_follow_buf populates the cache for *all*
 *         pipelined Parse messages following the trigger.
 *
 * Pipeline layout: [trigger Parse(unnamed,"SELECT 1")] [Parse("S_1",...)]
 *                  [Parse("S_2",...)] [Bind("S_1")] [Execute] [Sync]
 *
 * We call the helper with skip_bytes set to the length of the trigger
 * (mirroring the engine_flow contract: trigger was already classified).
 * Expect: S_1 and S_2 land in the stmt cache.
 * ------------------------------------------------------------------------- */
static void test_prefetch_classifies_all_named_parses(void)
{
    TEST_BEGIN("ps pipeline: prefetch classifies every named Parse in stash");

    void* ctx = startup_pg_ctx();

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.ctx  = ctx;

    /* Build the FULL pipelined batch.  The engine's classification of the
     * trigger frame is simulated by the explicit on_fe_msg call below. */
    uint8_t buf[1024];
    size_t  off = 0;
    size_t  trigger_len = build_parse(buf + off, "", "SELECT 1");
    off += trigger_len;
    off += build_parse  (buf + off, "S_1",
                         "INSERT INTO t (a) VALUES ($1)");
    off += build_parse  (buf + off, "S_2",
                         "SELECT a FROM t WHERE a = $1");
    off += build_bind   (buf + off, "", "S_1");
    off += build_execute(buf + off, "");
    off += build_sync   (buf + off);

    /* Simulate the engine_flow contract: trigger frame already classified
     * via on_fe_msg before stashing the tail. */
    keel_fe_action_t act = keel_fe_action_default();
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, trigger_len, &act), 0);

    /* Pre-fix behaviour: tail forwarded raw, S_1 / S_2 never classified.
     * Post-fix behaviour: the helper walks the tail and classifies each
     * Parse, so both names land in the cache. */
    keel_engine_flow_prefetch_classify_follow_buf(&sf, buf, off, trigger_len);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    TEST_ASSERT(stmt_cache_has(c, "S_1"));
    TEST_ASSERT(stmt_cache_has(c, "S_2"));

    if (VT->destroy_context) VT->destroy_context(ctx);
    TEST_END();
}

/* -------------------------------------------------------------------------
 * Test 2: helper is a no-op when skip_bytes covers the whole buffer or
 *         the buffer is empty / NULL.  Guards against re-classifying the
 *         trigger or walking past the end.
 * ------------------------------------------------------------------------- */
static void test_prefetch_noop_on_empty_or_fully_skipped(void)
{
    TEST_BEGIN("ps pipeline: prefetch is no-op when nothing left to classify");

    void* ctx = startup_pg_ctx();

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.ctx  = ctx;

    uint8_t buf[64];
    size_t  n = build_parse(buf, "S_only", "SELECT 1");

    /* Whole buffer skipped → nothing to do, and S_only must NOT enter
     * the cache (caller's contract: skip_bytes were already classified). */
    keel_engine_flow_prefetch_classify_follow_buf(&sf, buf, n, n);
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    TEST_ASSERT(!stmt_cache_has(c, "S_only"));

    /* NULL / zero-length are quietly ignored. */
    keel_engine_flow_prefetch_classify_follow_buf(&sf, NULL, 0, 0);
    keel_engine_flow_prefetch_classify_follow_buf(&sf, buf, 0, 0);

    if (VT->destroy_context) VT->destroy_context(ctx);
    TEST_END();
}

/* -------------------------------------------------------------------------
 * Test 3: helper tolerates a truncated trailing frame without writing
 *         past the buffer or partially classifying.  Mirrors the case
 *         where the FE TCP segment ends mid-message.
 * ------------------------------------------------------------------------- */
static void test_prefetch_stops_on_truncated_frame(void)
{
    TEST_BEGIN("ps pipeline: prefetch stops cleanly on truncated trailing frame");

    void* ctx = startup_pg_ctx();

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.ctx  = ctx;

    uint8_t buf[256];
    size_t  off = 0;
    size_t  trigger_len = build_parse(buf + off, "", "SELECT 1");
    off += trigger_len;
    size_t  s1_len = build_parse(buf + off, "S_1", "SELECT 2");
    off += s1_len;
    /* Truncate the next message header (claim 'P' frame of 100 bytes
     * but only deliver 3 bytes after the type byte). */
    buf[off++] = 'P';
    buf[off++] = 0;
    buf[off++] = 0;

    keel_fe_action_t act = keel_fe_action_default();
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, trigger_len, &act), 0);

    keel_engine_flow_prefetch_classify_follow_buf(&sf, buf, off, trigger_len);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    /* S_1 fits fully and must land in the cache. */
    TEST_ASSERT(stmt_cache_has(c, "S_1"));

    if (VT->destroy_context) VT->destroy_context(ctx);
    TEST_END();
}

/* -------------------------------------------------------------------------
 * Test 4: stmt-replay snapshot taken BEFORE prefetch survives a
 *         cache-clearing classification inside the follow buffer.
 *
 * Engine contract: when state_sync / stmt_replay / deferred BEGIN /
 * full cleanup is about to stash the follow buffer, the engine MUST
 * capture get_stmt_replay() before invoking the pre-classify helper.
 * Otherwise a DISCARD ALL message inside the tail clears the per-
 * session stmt cache via pg_stmt_clear_all() and the later replay
 * would be empty — even though the borrowed backend still needs
 * Parse(S_N) so the tail's Bind(S_N) can succeed.
 *
 * review_20260620_01.md RC-3 changed DDL/DISCARD-PLANS to preserve
 * the cache (matching vanilla PostgreSQL semantics); DISCARD ALL is
 * now the only statement that wipes the named-PS set in the proxy
 * cache.  This test was updated to use DISCARD ALL as the trigger.
 * ------------------------------------------------------------------------- */
static void test_replay_snapshot_survives_ddl_prefetch(void)
{
    TEST_BEGIN("ps pipeline: stmt-replay snapshot taken pre-prefetch survives cache clear");

    void* ctx = startup_pg_ctx();

    /* Prime the cache: Parse(S_1) + ParseComplete → S_1 confirmed. */
    uint8_t  buf[256];
    keel_fe_action_t fa = keel_fe_action_default();
    keel_be_action_t ba;
    size_t   n;

    n = build_parse(buf, "S_1", "INSERT INTO t (a) VALUES ($1)");
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, n, &fa), 0);
    n = build_parse_complete(buf);
    memset(&ba, 0, sizeof(ba));
    TEST_ASSERT_EQ(VT->on_be_msg(ctx, buf, n, &ba), 0);
    /* RFQ to settle protocol state. */
    n = build_rfq(buf, 'I');
    memset(&ba, 0, sizeof(ba));
    (void)VT->on_be_msg(ctx, buf, n, &ba);

    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;
    TEST_ASSERT(stmt_cache_has(c, "S_1"));

    /* Snapshot replay BEFORE any cache-clearing classification. */
    uint8_t* snap_buf   = NULL;
    size_t   snap_len   = 0;
    uint32_t snap_count = 0;
    uint64_t snap_hash  = 0;
    TEST_ASSERT_EQ(VT->get_stmt_replay(ctx, &snap_buf, &snap_len,
                                       &snap_count, &snap_hash), 0);
    TEST_ASSERT_NOT_NULL(snap_buf);
    TEST_ASSERT(snap_len > 0);
    TEST_ASSERT_EQ(snap_count, 1u);

    /* Now prefetch a follow buffer that contains a DISCARD ALL Parse.
     * The DISCARD ALL path inside the protocol plugin invokes
     * pg_stmt_clear_all(), wiping S_1 from the cache. */
    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.ctx  = ctx;

    uint8_t  pipe[512];
    size_t   off = 0;
    size_t   trigger_len = build_parse(pipe + off, "", "SELECT 1");
    off += trigger_len;
    /* DISCARD ALL must be Parse + Bind + Execute — the cache clear
     * fires on Execute (mirrors Simple Query semantics), not on Parse
     * alone. */
    off += build_parse  (pipe + off, "",
                         "DISCARD ALL");
    off += build_bind   (pipe + off, "", "");
    off += build_execute(pipe + off, "");
    off += build_bind   (pipe + off, "", "S_1");
    off += build_sync   (pipe + off);

    /* Trigger frame is "classified" by engine_flow's normal on_fe_msg
     * call before stashing — simulate that. */
    fa = keel_fe_action_default();
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, pipe, trigger_len, &fa), 0);

    keel_engine_flow_prefetch_classify_follow_buf(&sf, pipe, off, trigger_len);

    /* DISCARD ALL must have cleared the cache. */
    TEST_ASSERT(!stmt_cache_has(c, "S_1"));

    /* Re-querying replay post-prefetch yields nothing — proves that
     * snapshotting AFTER prefetch (the buggy ordering) would have
     * stranded the stmt-replay branch with rcount=0. */
    uint8_t* post_buf   = NULL;
    size_t   post_len   = 0;
    uint32_t post_count = 999;
    uint64_t post_hash  = 0;
    (void)VT->get_stmt_replay(ctx, &post_buf, &post_len,
                              &post_count, &post_hash);
    TEST_ASSERT_EQ(post_count, 0u);
    if (post_buf) keel_free(post_buf);

    /* The snapshot taken pre-prefetch is unaffected by the clear: it's
     * an independent heap buffer the engine can hand to the backend so
     * the next Bind(S_1) succeeds. */
    TEST_ASSERT_EQ(snap_count, 1u);
    TEST_ASSERT(snap_len > 0);
    TEST_ASSERT_NOT_NULL(snap_buf);

    keel_free(snap_buf);
    if (VT->destroy_context) VT->destroy_context(ctx);
    TEST_END();
}

/* -------------------------------------------------------------------------
 * Test 5 (review_20260620_01.md RC-1 / TG-8): a default-configured
 * VIRTUALIZE session that issues a simple-query PREPARE must have the
 * statement entered into the per-session cache.  Before the fix, only
 * TRACKING mode intercepted Q-PREPARE; VIRTUALIZE forwarded it verbatim
 * and silently lost track of the named PS, corrupting the pool.
 * ------------------------------------------------------------------------- */
static void test_virtualize_intercepts_q_prepare(void)
{
    TEST_BEGIN("ps pipeline: virtualize intercepts simple-query PREPARE (RC-1)");

    void* ctx = startup_pg_ctx();
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    /* Confirm default mode is VIRTUALIZE. */
    TEST_ASSERT_EQ(c->ps_mode, KEEL_PS_MODE_VIRTUALIZE);

    /* Issue Q("PREPARE myq AS SELECT $1::int"). */
    uint8_t buf[256];
    keel_fe_action_t fa = keel_fe_action_default();
    size_t n = build_query(buf, "PREPARE myq AS SELECT $1::int");
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, n, &fa), 0);

    /* The session cache must contain an entry for "myq" (unconfirmed
     * until backend CommandComplete("PREPARE") arrives). */
    TEST_ASSERT(stmt_cache_has(c, "myq"));

    /* HARD_PIN must be stripped in VIRTUALIZE so the backend stays
     * pool-reusable. */
    TEST_ASSERT(!(fa.effect & KEEL_QE_HARD_PIN));

    /* Backend confirms the PREPARE. */
    keel_be_action_t ba;
    memset(&ba, 0, sizeof(ba));
    n = build_command_complete(buf, "PREPARE");
    TEST_ASSERT_EQ(VT->on_be_msg(ctx, buf, n, &ba), 0);

    /* RFQ to settle. */
    memset(&ba, 0, sizeof(ba));
    n = build_rfq(buf, 'I');
    (void)VT->on_be_msg(ctx, buf, n, &ba);

    /* Session hash must be non-zero (the confirmed entry contributes). */
    TEST_ASSERT(c->session_stmt_hash != 0);

    /* get_stmt_replay must yield a single statement (the Q-PREPARE
     * wire bytes are stored and replayable). */
    uint8_t* rbuf = NULL;
    size_t   rlen = 0;
    uint32_t rcount = 0;
    uint64_t rhash = 0;
    TEST_ASSERT_EQ(VT->get_stmt_replay(ctx, &rbuf, &rlen, &rcount, &rhash), 0);
    TEST_ASSERT_EQ(rcount, 1u);
    TEST_ASSERT(rhash != 0);
    if (rbuf) keel_free(rbuf);

    if (VT->destroy_context) VT->destroy_context(ctx);
    TEST_END();
}

/* -------------------------------------------------------------------------
 * Test 6 (review_20260620_01.md RC-3 / TG-3): a session that has a
 * confirmed named PS, then issues DDL, must STILL have the named PS in
 * cache afterwards.  Vanilla PostgreSQL keeps named PS across DDL; the
 * proxy must match that so a subsequent Bind on a freshly-borrowed
 * backend can replay the Parse and succeed.
 * ------------------------------------------------------------------------- */
static void test_ddl_preserves_named_ps_cache(void)
{
    TEST_BEGIN("ps pipeline: DDL preserves named-PS cache (RC-3)");

    void* ctx = startup_pg_ctx();
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    /* Prime the cache. */
    uint8_t buf[256];
    keel_fe_action_t fa = keel_fe_action_default();
    keel_be_action_t ba;
    size_t n;

    n = build_parse(buf, "p1", "SELECT $1::int");
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, n, &fa), 0);
    memset(&ba, 0, sizeof(ba));
    n = build_parse_complete(buf);
    TEST_ASSERT_EQ(VT->on_be_msg(ctx, buf, n, &ba), 0);
    memset(&ba, 0, sizeof(ba));
    n = build_rfq(buf, 'I');
    (void)VT->on_be_msg(ctx, buf, n, &ba);

    TEST_ASSERT(stmt_cache_has(c, "p1"));
    uint64_t hash_before = c->session_stmt_hash;
    TEST_ASSERT(hash_before != 0);
    uint32_t epoch_before = c->stmt_schema_epoch;

    /* Issue DDL via Simple Query. */
    fa = keel_fe_action_default();
    n = build_query(buf, "ALTER TABLE t ADD COLUMN x int");
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, n, &fa), 0);

    /* The cache entry must still be present (RC-3).  The session hash
     * may change because pg_stmt_restamp_context incorporates
     * stmt_schema_epoch into every entry's per-stmt hash, but the
     * NUMBER of confirmed entries must be unchanged. */
    TEST_ASSERT(stmt_cache_has(c, "p1"));
    /* Schema epoch must bump. */
    TEST_ASSERT(c->stmt_schema_epoch > epoch_before);

    uint64_t hash_after_ddl = c->session_stmt_hash;
    uint32_t epoch_after_ddl = c->stmt_schema_epoch;

    /* Same for DISCARD PLANS — cache preserved. */
    fa = keel_fe_action_default();
    n = build_query(buf, "DISCARD PLANS");
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, n, &fa), 0);
    TEST_ASSERT(stmt_cache_has(c, "p1"));
    TEST_ASSERT(c->stmt_schema_epoch > epoch_after_ddl);

    /* DISCARD ALL must clear the cache (true backend-side drop). */
    fa = keel_fe_action_default();
    n = build_query(buf, "DISCARD ALL");
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, n, &fa), 0);
    TEST_ASSERT(!stmt_cache_has(c, "p1"));
    TEST_ASSERT_EQ(c->session_stmt_hash, 0ULL);

    if (VT->destroy_context) VT->destroy_context(ctx);
    TEST_END();
}

/* -------------------------------------------------------------------------
 * Test 7 (review_20260620_01.md RC-5 / TG-5): after a failed unnamed
 * Parse(''), the unnamed_stmt_valid flag must be false, and the next
 * Bind('') must trigger a re-Parse using the cached wire bytes.  After
 * a successful Parse(''), unnamed_stmt_valid must be true.
 * ------------------------------------------------------------------------- */
static void test_unnamed_parse_validity_tracking(void)
{
    TEST_BEGIN("ps pipeline: unnamed Parse validity tracking (RC-5)");

    void* ctx = startup_pg_ctx();
    pg_flow_ctx_t* c = (pg_flow_ctx_t*)ctx;

    /* Initial state: unnamed slot is invalid until first successful Parse. */
    TEST_ASSERT(!c->unnamed_stmt_valid);
    TEST_ASSERT(!c->unnamed_parse_in_flight);

    /* Issue Parse("") → sets in_flight, clears valid. */
    uint8_t buf[256];
    keel_fe_action_t fa = keel_fe_action_default();
    size_t n = build_parse(buf, "", "SELECT $1::int");
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, n, &fa), 0);
    TEST_ASSERT(!c->unnamed_stmt_valid);
    TEST_ASSERT(c->unnamed_parse_in_flight);

    /* Backend returns ParseComplete → valid=true, in_flight=false. */
    keel_be_action_t ba;
    memset(&ba, 0, sizeof(ba));
    n = build_parse_complete(buf);
    TEST_ASSERT_EQ(VT->on_be_msg(ctx, buf, n, &ba), 0);
    TEST_ASSERT(c->unnamed_stmt_valid);
    TEST_ASSERT(!c->unnamed_parse_in_flight);

    /* Issue another Parse("") → in_flight=true, valid=false. */
    fa = keel_fe_action_default();
    n = build_parse(buf, "", "SELECT broken syntax {{{");
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, buf, n, &fa), 0);
    TEST_ASSERT(!c->unnamed_stmt_valid);
    TEST_ASSERT(c->unnamed_parse_in_flight);

    /* Backend returns ErrorResponse → valid=false, in_flight=false. */
    memset(&ba, 0, sizeof(ba));
    n = build_error_response(buf, "syntax error in SQL");
    TEST_ASSERT_EQ(VT->on_be_msg(ctx, buf, n, &ba), 0);
    TEST_ASSERT(!c->unnamed_stmt_valid);
    TEST_ASSERT(!c->unnamed_parse_in_flight);

    /* Now issue Bind("").  Because unnamed_stmt_valid is false and the
     * cached wire_msg exists, the proxy must rewrite the payload to
     * Parse("") + Bind("") (act.be_payload points to a heap buffer). */
    fa = keel_fe_action_default();
    uint8_t bind_buf[64];
    size_t bn = build_bind(bind_buf, "", "");
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, bind_buf, bn, &fa), 0);

    /* The act.be_payload must be different from the original bind_buf
     * (rewritten to Parse+Bind) — i.e., be_payload_len exceeds the
     * original bind length. */
    TEST_ASSERT(fa.be_payload != NULL);
    TEST_ASSERT(fa.be_payload_len > bn);
    /* First byte of rewritten payload must be 'P' (Parse). */
    TEST_ASSERT_EQ(fa.be_payload[0], (uint8_t)'P');

    if (VT->destroy_context) VT->destroy_context(ctx);
    TEST_END();
}

/* -------------------------------------------------------------------------
 * Test 8 (review_20260620_01.md RC-4 / TG-4): two sessions whose
 * stmt_set_hash XOR folds collide but whose underlying statement sets
 * differ must NOT be considered compatible.  The hash vector equality
 * in backend_pool_stmt_compatible must reject the false match.
 *
 * Constructing a true fnv1a XOR collision is non-trivial; instead we
 * verify the mechanism directly: build two profiles with matching
 * stmt_set_hash but differing stmt_hashes[] arrays, and confirm
 * backend_pool_stmt_compatible returns false.
 * ------------------------------------------------------------------------- */
static void test_hash_vector_rejects_collision(void)
{
    TEST_BEGIN("ps pipeline: hash vector rejects XOR collision (RC-4)");

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.stmt_set_hash = 0xABCD1234;
    be.stmt_profile.stmt_set_hash = 0xABCD1234;
    be.stmt_profile.stmt_hash_count = 2;
    be.stmt_profile.stmt_hashes[0] = 0x1111;
    be.stmt_profile.stmt_hashes[1] = 0xBABC2123; /* XOR = 0xABCD1234 */

    keel_stmt_compat_profile_t req;
    memset(&req, 0, sizeof(req));
    req.stmt_set_hash = 0xABCD1234;  /* same XOR */
    req.stmt_hash_count = 2;
    req.stmt_hashes[0] = 0x2222;
    req.stmt_hashes[1] = 0x99EF33E6; /* XOR = 0xABCD1234, but different set */

    /* stmt_set_hash matches but the vectors differ — must reject. */
    TEST_ASSERT(!backend_pool_stmt_compatible(&req, &be));

    /* Sanity: identical vectors must accept. */
    keel_stmt_compat_profile_t req2 = req;
    req2.stmt_hashes[0] = be.stmt_profile.stmt_hashes[0];
    req2.stmt_hashes[1] = be.stmt_profile.stmt_hashes[1];
    TEST_ASSERT(backend_pool_stmt_compatible(&req2, &be));

    /* Sanity: different counts must reject even if first hash matches. */
    keel_stmt_compat_profile_t req3 = req2;
    req3.stmt_hash_count = 1;
    TEST_ASSERT(!backend_pool_stmt_compatible(&req3, &be));

    TEST_END();
}

int main(void)
{
    test_prefetch_classifies_all_named_parses();
    test_prefetch_noop_on_empty_or_fully_skipped();
    test_prefetch_stops_on_truncated_frame();
    test_replay_snapshot_survives_ddl_prefetch();
    test_virtualize_intercepts_q_prepare();
    test_ddl_preserves_named_ps_cache();
    test_unnamed_parse_validity_tracking();
    test_hash_vector_rejects_collision();
    return test_summary();
}
