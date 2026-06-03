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
 * Test 4: stmt-replay snapshot taken BEFORE prefetch survives a DDL
 *         classification inside the follow buffer.
 *
 * Engine contract: when state_sync / stmt_replay / deferred BEGIN /
 * full cleanup is about to stash the follow buffer, the engine MUST
 * capture get_stmt_replay() before invoking the pre-classify helper.
 * Otherwise a DDL message inside the tail (CREATE/ALTER/DROP) clears
 * the per-session stmt cache via pg_stmt_clear_all() and the later
 * replay would be empty — even though the borrowed backend still needs
 * Parse(S_N) so the tail's Bind(S_N) can succeed.
 *
 * This test pins the snapshot semantics: a buffer captured pre-DDL
 * contains S_1, and re-querying post-DDL returns no entries — so the
 * fix is materially required and the snapshot is materially useful.
 * ------------------------------------------------------------------------- */
static void test_replay_snapshot_survives_ddl_prefetch(void)
{
    TEST_BEGIN("ps pipeline: stmt-replay snapshot taken pre-prefetch survives DDL clear");

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

    /* Snapshot replay BEFORE any DDL classification. */
    uint8_t* snap_buf   = NULL;
    size_t   snap_len   = 0;
    uint32_t snap_count = 0;
    uint64_t snap_hash  = 0;
    TEST_ASSERT_EQ(VT->get_stmt_replay(ctx, &snap_buf, &snap_len,
                                       &snap_count, &snap_hash), 0);
    TEST_ASSERT_NOT_NULL(snap_buf);
    TEST_ASSERT(snap_len > 0);
    TEST_ASSERT_EQ(snap_count, 1u);

    /* Now prefetch a follow buffer that contains a DDL Parse.  The DDL
     * path inside the protocol plugin invokes pg_stmt_clear_all(),
     * wiping S_1 from the cache. */
    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.ctx  = ctx;

    uint8_t  pipe[512];
    size_t   off = 0;
    size_t   trigger_len = build_parse(pipe + off, "", "SELECT 1");
    off += trigger_len;
    /* DDL pipeline must be Parse + Bind + Execute — the cache clear
     * fires on Execute (mirrors Simple Query semantics), not on Parse
     * alone. */
    off += build_parse  (pipe + off, "",
                         "CREATE TABLE IF NOT EXISTS t (a int)");
    off += build_bind   (pipe + off, "", "");
    off += build_execute(pipe + off, "");
    off += build_bind   (pipe + off, "", "S_1");
    off += build_sync   (pipe + off);

    /* Trigger frame is "classified" by engine_flow's normal on_fe_msg
     * call before stashing — simulate that. */
    fa = keel_fe_action_default();
    TEST_ASSERT_EQ(VT->on_fe_msg(ctx, pipe, trigger_len, &fa), 0);

    keel_engine_flow_prefetch_classify_follow_buf(&sf, pipe, off, trigger_len);

    /* DDL must have cleared the cache. */
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

int main(void)
{
    test_prefetch_classifies_all_named_parses();
    test_prefetch_noop_on_empty_or_fully_skipped();
    test_prefetch_stops_on_truncated_frame();
    test_replay_snapshot_survives_ddl_prefetch();
    return test_summary();
}
