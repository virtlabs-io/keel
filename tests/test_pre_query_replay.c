/**
 * @file test_pre_query_replay.c
 * @brief Unit tests for async deferred-BEGIN pre-query replay (PR #4).
 */

#include "test_utils.h"

#include "keel/engine/engine_flow.h"
#include "keel/engine/backend_pool.h"
#include "keel/session/session.h"
#include "keel/session/ssv_atom.h"
#include "keel/session/state_profile.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/mem/mem.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ---- PG vtable ---- */
extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
#define VT (&keel_proto_flow_postgres)

static void wr32(uint8_t* p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)(v);
}

static size_t build_command_complete_tag(uint8_t* buf, const char* tag)
{
    size_t tlen = strlen(tag) + 1; /* include NUL */
    buf[0] = 'C';
    wr32(buf + 1, (uint32_t)(4 + tlen));
    memcpy(buf + 5, tag, tlen);
    return 1 + 4 + tlen;
}

static size_t build_command_complete_begin(uint8_t* buf)
{
    return build_command_complete_tag(buf, "BEGIN");
}

static size_t build_ready_for_query(uint8_t* buf, char status)
{
    buf[0] = 'Z';
    wr32(buf + 1, 5);
    buf[5] = (uint8_t)status;
    return 6;
}

static size_t build_simple_query(uint8_t* buf, const char* sql)
{
    size_t sql_len = strlen(sql) + 1;
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(4 + sql_len));
    memcpy(buf + 5, sql, sql_len);
    return 1 + 4 + sql_len;
}

static size_t build_startup(uint8_t* buf, const char* user, const char* db)
{
    uint8_t* p = buf + 4;
    wr32(p, 0x00030000);
    p += 4;
    memcpy(p, "user", 5);
    p += 5;
    size_t user_len = strlen(user);
    memcpy(p, user, user_len + 1);
    p += user_len + 1;
    memcpy(p, "database", 9);
    p += 9;
    size_t db_len = strlen(db);
    memcpy(p, db, db_len + 1);
    p += db_len + 1;
    *p++ = '\0';
    wr32(buf, (uint32_t)(p - buf));
    return (size_t)(p - buf);
}

static size_t build_named_parse(uint8_t* buf, const char* name, const char* sql)
{
    size_t name_len = strlen(name);
    size_t sql_len = strlen(sql);
    size_t body_len = name_len + 1 + sql_len + 1 + 2;
    buf[0] = 'P';
    wr32(buf + 1, (uint32_t)(4 + body_len));
    memcpy(buf + 5, name, name_len + 1);
    memcpy(buf + 5 + name_len + 1, sql, sql_len + 1);
    buf[5 + name_len + 1 + sql_len + 1] = 0;
    buf[5 + name_len + 1 + sql_len + 2] = 0;
    return 1 + 4 + body_len;
}

static size_t build_bind(uint8_t* buf, const char* portal, const char* stmt)
{
    size_t portal_len = strlen(portal);
    size_t stmt_len = strlen(stmt);
    size_t body_len = portal_len + 1 + stmt_len + 1 + 2 + 2 + 2;
    uint8_t* p = buf + 5;
    buf[0] = 'B';
    wr32(buf + 1, (uint32_t)(4 + body_len));
    memcpy(p, portal, portal_len + 1);
    p += portal_len + 1;
    memcpy(p, stmt, stmt_len + 1);
    p += stmt_len + 1;
    p[0] = 0; p[1] = 0; p += 2;
    p[0] = 0; p[1] = 0; p += 2;
    p[0] = 0; p[1] = 0;
    return 1 + 4 + body_len;
}

static size_t build_execute(uint8_t* buf, const char* portal)
{
    size_t portal_len = strlen(portal);
    size_t body_len = portal_len + 1 + 4;
    buf[0] = 'E';
    wr32(buf + 1, (uint32_t)(4 + body_len));
    memcpy(buf + 5, portal, portal_len + 1);
    wr32(buf + 5 + portal_len + 1, 0);
    return 1 + 4 + body_len;
}

static size_t build_extended_msg(uint8_t* buf, uint8_t type)
{
    buf[0] = type;
    wr32(buf + 1, 4);
    return 5;
}

static size_t build_parse_complete(uint8_t* buf)
{
    buf[0] = '1';
    wr32(buf + 1, 4);
    return 5;
}

static size_t build_error_response(uint8_t* buf, const char* message)
{
    size_t mlen = strlen(message) + 1; /* include NUL */
    size_t body_len = 1 + mlen + 1;    /* 'M' + message + terminator */
    buf[0] = 'E';
    wr32(buf + 1, (uint32_t)(4 + body_len));
    buf[5] = 'M';
    memcpy(buf + 6, message, mlen);
    buf[6 + mlen] = 0;
    return 1 + 4 + body_len;
}

static size_t build_notice_response(uint8_t* buf, const char* message)
{
    size_t mlen = strlen(message) + 1; /* include NUL */
    size_t body_len = 1 + mlen + 1;    /* 'M' + message + terminator */
    buf[0] = 'N';
    wr32(buf + 1, (uint32_t)(4 + body_len));
    buf[5] = 'M';
    memcpy(buf + 6, message, mlen);
    buf[6 + mlen] = 0;
    return 1 + 4 + body_len;
}

static size_t build_parameter_status(uint8_t* buf,
                                     const char* key,
                                     const char* value)
{
    size_t klen = strlen(key) + 1;
    size_t vlen = strlen(value) + 1;
    buf[0] = 'S';
    wr32(buf + 1, (uint32_t)(4 + klen + vlen));
    memcpy(buf + 5, key, klen);
    memcpy(buf + 5 + klen, value, vlen);
    return 1 + 4 + klen + vlen;
}

static int make_socketpair(int sv[2])
{
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0)
        return -1;

    int f0 = fcntl(sv[0], F_GETFL, 0);
    int f1 = fcntl(sv[1], F_GETFL, 0);
    if (f0 >= 0) (void)fcntl(sv[0], F_SETFL, f0 | O_NONBLOCK);
    if (f1 >= 0) (void)fcntl(sv[1], F_SETFL, f1 | O_NONBLOCK);
    return 0;
}

static void close_pair(int sv[2])
{
    if (sv[0] >= 0) close(sv[0]);
    if (sv[1] >= 0) close(sv[1]);
    sv[0] = sv[1] = -1;
}

static bool buf_contains_bytes(const uint8_t* haystack, size_t hay_len,
                               const char* needle, size_t needle_len)
{
    if (!haystack || !needle || needle_len == 0 || hay_len < needle_len)
        return false;
    for (size_t i = 0; i + needle_len <= hay_len; i++) {
        if (memcmp(haystack + i, needle, needle_len) == 0)
            return true;
    }
    return false;
}

typedef struct fake_replay_ctx {
    const uint8_t* replay_buf;
    size_t replay_len;
    uint32_t replay_count;
    uint64_t replay_hash;
    int cleanup_mode;
    size_t cleanup_calls;
    keel_consistency_token_t capture_token;
    int capture_rc;
    size_t capture_calls;
    size_t notify_calls;
    char notified_lsn[KEEL_CONSISTENCY_TOKEN_MAX];
    keel_query_effect_flags_t fe_effect;
    bool fe_no_response;
    size_t fe_calls;
} fake_replay_ctx_t;

enum {
    FAKE_CLEANUP_COMPLETE = 0,
    FAKE_CLEANUP_MORE_THEN_COMPLETE = 1,
    FAKE_CLEANUP_ERROR = 2,
    FAKE_CLEANUP_BAD_CONSUMED = 3,
    FAKE_CLEANUP_CONSUME_FIRST_FRAME = 4,
};

static ssize_t fake_build_cleanup(void* ctx, keel_cleanup_reason_t reason,
                                  uint8_t* buf, size_t buf_len)
{
    (void)ctx;
    (void)reason;
    static const uint8_t kCleanup[] = { 'Q', 0, 0, 0, 5, 0 };
    if (!buf || buf_len < sizeof(kCleanup))
        return -1;
    memcpy(buf, kCleanup, sizeof(kCleanup));
    return (ssize_t)sizeof(kCleanup);
}

static keel_proto_drain_result_t fake_drain_cleanup_response(
    void* ctx,
    keel_proto_drain_state_t* state,
    const uint8_t* data,
    size_t len,
    size_t* consumed_out)
{
    (void)state;
    (void)data;
    fake_replay_ctx_t* rctx = (fake_replay_ctx_t*)ctx;
    if (!rctx)
        return KEEL_PROTO_DRAIN_ERROR;

    rctx->cleanup_calls++;
    if (rctx->cleanup_mode == FAKE_CLEANUP_ERROR) {
        if (consumed_out)
            *consumed_out = 0;
        return KEEL_PROTO_DRAIN_ERROR;
    }
    if (rctx->cleanup_mode == FAKE_CLEANUP_BAD_CONSUMED) {
        if (consumed_out)
            *consumed_out = len + 1;
        return KEEL_PROTO_DRAIN_COMPLETE;
    }
    if (rctx->cleanup_mode == FAKE_CLEANUP_CONSUME_FIRST_FRAME) {
        if (len < 5) {
            if (consumed_out)
                *consumed_out = 0;
            return KEEL_PROTO_DRAIN_MORE;
        }
        uint32_t mlen = ((uint32_t)data[1] << 24) |
                        ((uint32_t)data[2] << 16) |
                        ((uint32_t)data[3] << 8)  |
                        (uint32_t)data[4];
        if (mlen < 4 || (1u + mlen) > len) {
            if (consumed_out)
                *consumed_out = 0;
            return KEEL_PROTO_DRAIN_MORE;
        }
        if (consumed_out)
            *consumed_out = 1u + mlen;
        return KEEL_PROTO_DRAIN_COMPLETE;
    }
    if (rctx->cleanup_mode == FAKE_CLEANUP_MORE_THEN_COMPLETE &&
        rctx->cleanup_calls == 1) {
        if (consumed_out)
            *consumed_out = len;
        return KEEL_PROTO_DRAIN_MORE;
    }
    if (consumed_out)
        *consumed_out = len;
    return KEEL_PROTO_DRAIN_COMPLETE;
}

static ssize_t fake_frame_len(void* ctx, const uint8_t* data, size_t len, int dir)
{
    (void)ctx;
    (void)dir;
    if (!data || len < 5)
        return 0;
    uint32_t ml = ((uint32_t)data[1] << 24) |
                  ((uint32_t)data[2] << 16) |
                  ((uint32_t)data[3] << 8)  |
                  (uint32_t)data[4];
    if (ml < 4)
        return -1;
    return (size_t)(1u + ml) <= len ? (ssize_t)(1u + ml) : 0;
}

static int fake_on_be_msg(void* ctx,
                          const uint8_t* data,
                          size_t len,
                          keel_be_action_t* act)
{
    (void)ctx;
    *act = keel_be_action_default();
    if (!data || len < 5) {
        act->type = KEEL_BE_ACT_ERROR;
        return -1;
    }

    act->type = KEEL_BE_ACT_FORWARD_FE;
    act->fe_payload = data;
    act->fe_payload_len = len;

    switch (data[0]) {
    case '1':
        act->stmt_replay_accepted = true;
        break;
    case 'E':
        act->type = KEEL_BE_ACT_ERROR;
        break;
    case 'Z':
        if (len >= 6) {
            act->tx_state_changed = true;
            act->query_complete = true;
            if (data[5] == 'I') {
                act->tx_status = KEEL_TX_IDLE;
                act->backend_reusable = true;
            } else if (data[5] == 'T') {
                act->tx_status = KEEL_TX_ACTIVE;
            } else {
                act->tx_status = KEEL_TX_FAILED;
            }
        }
        break;
    default:
        break;
    }
    return 0;
}

static int fake_on_fe_msg(void* ctx,
                          const uint8_t* data,
                          size_t len,
                          keel_fe_action_t* act)
{
    fake_replay_ctx_t* rctx = (fake_replay_ctx_t*)ctx;
    if (!rctx || !data || len == 0 || !act)
        return -1;

    rctx->fe_calls++;
    *act = keel_fe_action_default();
    act->type = KEEL_FE_ACT_QUERY;
    act->msg_kind = KEEL_MSG_KIND_SQL;
    act->effect = rctx->fe_effect;
    act->route_hint = (rctx->fe_effect & (KEEL_QE_WRITE | KEEL_QE_DDL))
        ? KEEL_FROUTE_WRITE
        : KEEL_FROUTE_READ;
    act->be_payload = data;
    act->be_payload_len = len;
    act->no_response = rctx->fe_no_response;
    if (len > 6 && data[0] == 'Q') {
        act->sql_view = (const char*)data + 5;
        act->sql_view_len = strlen((const char*)data + 5);
    }
    return 0;
}

static void fake_get_info(keel_plugin_info_t* out)
{
    if (!out)
        return;
    memset(out, 0, sizeof(*out));
    out->name = "fake";
    out->api_version = KEEL_PLUGIN_API_V1;
    out->capabilities = KEEL_PCAP_CONSISTENCY_TOKEN;
}

static int fake_get_stmt_replay(void* ctx,
                                uint8_t** out_buf,
                                size_t* out_len,
                                uint32_t* out_count,
                                uint64_t* out_hash)
{
    fake_replay_ctx_t* rctx = (fake_replay_ctx_t*)ctx;
    if (!rctx)
        return -1;
    if (out_hash)
        *out_hash = rctx->replay_hash;
    if (!out_buf || !out_len || !out_count)
        return 0;

    uint8_t* cp = (uint8_t*)keel_malloc(rctx->replay_len);
    if (!cp)
        return -1;
    memcpy(cp, rctx->replay_buf, rctx->replay_len);
    *out_buf = cp;
    *out_len = rctx->replay_len;
    *out_count = rctx->replay_count;
    return 0;
}

static int fake_capture_consistency_token(void* ctx, int be_fd,
                                          keel_consistency_token_t* out)
{
    fake_replay_ctx_t* rctx = (fake_replay_ctx_t*)ctx;
    if (!rctx || !out || be_fd < 0)
        return -1;
    rctx->capture_calls++;
    if (rctx->capture_rc != 0)
        return rctx->capture_rc;
    *out = rctx->capture_token;
    return 0;
}

static void fake_notify_write_lsn(void* ctx, const char* lsn)
{
    fake_replay_ctx_t* rctx = (fake_replay_ctx_t*)ctx;
    if (!rctx || !lsn)
        return;
    rctx->notify_calls++;
    snprintf(rctx->notified_lsn, sizeof(rctx->notified_lsn), "%s", lsn);
}

static const keel_proto_flow_vtable_t s_fake_replay_vt = {
    .frame_len = fake_frame_len,
    .on_fe_msg = fake_on_fe_msg,
    .on_be_msg = fake_on_be_msg,
    .get_info = fake_get_info,
    .build_cleanup = fake_build_cleanup,
    .drain_cleanup_response = fake_drain_cleanup_response,
    .get_stmt_replay = fake_get_stmt_replay,
    .capture_consistency_token = fake_capture_consistency_token,
    .notify_write_lsn = fake_notify_write_lsn,
};

static void assert_fe_effect_stamps_tokenless_write_ts(
    keel_query_effect_flags_t effect,
    const char* sql,
    bool expect_capture_pending)
{
    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.client_fd = -1;

    fake_replay_ctx_t rctx;
    memset(&rctx, 0, sizeof(rctx));
    rctx.fe_effect = effect;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = &s_fake_replay_vt;
    sf.ctx = &rctx;
    sf.phase = KEEL_PHASE_READY;
    sf.mode = expect_capture_pending ? KEEL_TIER_FULL : KEEL_TIER_SMART;

    uint8_t msg[128];
    size_t msg_len = build_simple_query(msg, sql);

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, msg, msg_len);
    TEST_ASSERT(r == KEEL_FLOW_WAIT_BACKEND || r == KEEL_FLOW_LINKED_SEND);
    TEST_ASSERT_EQ(rctx.fe_calls, 1u);
    TEST_ASSERT(sf.last_write_ns != 0);
    TEST_ASSERT_EQ(keel_ssv_consistency_get_ts(sf.consistency_atoms),
                   sf.last_write_ns);
    TEST_ASSERT(!keel_ssv_consistency_has_write_lsn(sf.consistency_atoms));
    TEST_ASSERT(!keel_ssv_consistency_ttl_ok(sf.consistency_atoms,
                                             sf.last_write_ns,
                                             100));
    TEST_ASSERT_EQ(sf.capture_lsn_pending, expect_capture_pending);

    if (session.server_fd < 0)
        be_sv[0] = -1;
    close_pair(be_sv);
}

static void test_fe_tokenless_write_stamps_sticky_primary(void)
{
    TEST_BEGIN("ryw: FE write stamps tokenless sticky-primary timestamp");

    assert_fe_effect_stamps_tokenless_write_ts(KEEL_QE_WRITE,
                                               "UPDATE t SET v = 1",
                                               false);

    TEST_END();
}

static void test_fe_tokenless_ddl_stamps_sticky_primary(void)
{
    TEST_BEGIN("ryw: FE DDL stamps tokenless sticky-primary timestamp");

    assert_fe_effect_stamps_tokenless_write_ts(KEEL_QE_DDL,
                                               "ALTER TABLE t ADD COLUMN v int",
                                               false);

    TEST_END();
}

static void test_fe_write_capture_pending_preserves_tokenless_stickiness(void)
{
    TEST_BEGIN("ryw: FE write awaiting capture still has sticky-primary timestamp");

    assert_fe_effect_stamps_tokenless_write_ts(KEEL_QE_WRITE,
                                               "INSERT INTO t VALUES (1)",
                                               true);

    TEST_END();
}

static void test_fe_read_does_not_stamp_sticky_primary(void)
{
    TEST_BEGIN("ryw: FE read does not stamp sticky-primary timestamp");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.client_fd = -1;

    fake_replay_ctx_t rctx;
    memset(&rctx, 0, sizeof(rctx));
    rctx.fe_effect = KEEL_QE_READONLY;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = &s_fake_replay_vt;
    sf.ctx = &rctx;
    sf.phase = KEEL_PHASE_READY;
    sf.mode = KEEL_TIER_SMART;

    uint8_t msg[128];
    size_t msg_len = build_simple_query(msg, "SELECT * FROM t");

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, msg, msg_len);
    TEST_ASSERT(r == KEEL_FLOW_WAIT_BACKEND || r == KEEL_FLOW_LINKED_SEND);
    TEST_ASSERT_EQ(rctx.fe_calls, 1u);
    TEST_ASSERT_EQ(sf.last_write_ns, 0ULL);
    TEST_ASSERT_EQ(keel_ssv_consistency_get_ts(sf.consistency_atoms), 0ULL);
    TEST_ASSERT(!keel_ssv_consistency_has_write_lsn(sf.consistency_atoms));
    TEST_ASSERT_EQ(sf.capture_lsn_pending, false);

    close_pair(be_sv);
    TEST_END();
}

static void test_extended_sync_does_not_use_linked_send(void)
{
    TEST_BEGIN("extended protocol: Sync waits for backend without linked send");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 17;
    worker.ps_mode = KEEL_PS_MODE_TRACKING;
    worker.runtime_mode = KEEL_TIER_POOL;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.client_fd = -1;

    keel_session_flow_t sf;
    TEST_ASSERT_EQ(keel_session_flow_init(&sf, VT, &session), 0);

    uint8_t startup[256];
    keel_fe_action_t startup_act;
    size_t startup_len = build_startup(startup, "testuser", "testdb");
    TEST_ASSERT_EQ(VT->on_fe_msg(sf.ctx, startup, startup_len, &startup_act), KEEL_OK);
    sf.phase = KEEL_PHASE_READY;
    session.state = KEEL_SESSION_READY;

    uint8_t msg[512];
    size_t n = 0;
    n += build_named_parse(msg + n, "stmtcache_deadbeef", "SELECT 1");
    n += build_bind(msg + n, "", "stmtcache_deadbeef");
    n += build_execute(msg + n, "");
    n += build_extended_msg(msg + n, 'S');

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, msg, n);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.linked_send_len, 0u);

    uint8_t got[512];
    ssize_t nr = recv(be_sv[1], got, sizeof(got), 0);
    TEST_ASSERT_EQ(nr, (ssize_t)n);
    TEST_ASSERT(memcmp(got, msg, n) == 0);

    if (VT->destroy_context)
        VT->destroy_context(sf.ctx);
    session.plugin_state = NULL;
    close_pair(be_sv);
    TEST_END();
}

static void test_post_write_capture_success_updates_ssv(void)
{
    TEST_BEGIN("ryw: post-write capture stores token in session state");

    int be_sv[2] = { -1, -1 };
    int fe_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);
    TEST_ASSERT(make_socketpair(fe_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.client_fd = fe_sv[0];

    fake_replay_ctx_t rctx;
    memset(&rctx, 0, sizeof(rctx));
    snprintf(rctx.capture_token.value, sizeof(rctx.capture_token.value),
             "0/CAFEBABE");
    rctx.capture_token.captured_at_ns = 123456789ULL;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = &s_fake_replay_vt;
    sf.ctx = &rctx;
    sf.capture_lsn_pending = true;

    uint8_t bebuf[64];
    size_t n = 0;
    n += build_command_complete_tag(bebuf + n, "UPDATE 1");
    n += build_ready_for_query(bebuf + n, 'I');

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, bebuf, n);
    TEST_ASSERT_EQ(r, KEEL_FLOW_OK);
    TEST_ASSERT_EQ(sf.capture_lsn_pending, false);
    TEST_ASSERT_EQ(rctx.capture_calls, 1u);
    TEST_ASSERT_EQ(rctx.notify_calls, 1u);
    TEST_ASSERT_STR_EQ(rctx.notified_lsn, "0/CAFEBABE");
    TEST_ASSERT_STR_EQ(sf.last_write_token.value, "0/CAFEBABE");
    TEST_ASSERT_STR_EQ(keel_ssv_consistency_get_lsn(sf.consistency_atoms),
                       "0/CAFEBABE");
    TEST_ASSERT_EQ(keel_ssv_consistency_get_ts(sf.consistency_atoms),
                   123456789ULL);

    if (session.server_fd < 0)
        be_sv[0] = -1;
    close_pair(be_sv);
    close_pair(fe_sv);
    TEST_END();
}

static void test_post_write_capture_failure_fails_closed(void)
{
    TEST_BEGIN("ryw: post-write capture failure fails closed");

    int be_sv[2] = { -1, -1 };
    int fe_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);
    TEST_ASSERT(make_socketpair(fe_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.client_fd = fe_sv[0];

    fake_replay_ctx_t rctx;
    memset(&rctx, 0, sizeof(rctx));
    rctx.capture_rc = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = &s_fake_replay_vt;
    sf.ctx = &rctx;
    sf.capture_lsn_pending = true;

    uint8_t bebuf[64];
    size_t n = 0;
    n += build_command_complete_tag(bebuf + n, "UPDATE 1");
    n += build_ready_for_query(bebuf + n, 'I');

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, bebuf, n);
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);
    TEST_ASSERT_EQ(sf.capture_lsn_pending, false);
    TEST_ASSERT_EQ(rctx.capture_calls, 1u);
    TEST_ASSERT_EQ(rctx.notify_calls, 0u);
    TEST_ASSERT_EQ(sf.last_write_token.value[0], '\0');
    TEST_ASSERT_EQ(keel_ssv_consistency_has_write_lsn(sf.consistency_atoms), false);
    TEST_ASSERT_EQ(session.server_fd, -1);

    be_sv[0] = -1;
    close_pair(be_sv);
    close_pair(fe_sv);
    TEST_END();
}

static void test_flag_lifecycle_and_forward_on_rfq(void)
{
    TEST_BEGIN("pre_query: flag lifecycle + forward on RFQ");

    int sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 1;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = sv[0];

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_BEGIN_REPLAY;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    sf.pending_pre_query_absorbed = 0;

    const uint8_t follow[] = { 'Q', 0, 0, 0, 5, 0 };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    uint8_t bebuf[64];
    size_t n = 0;
    n += build_command_complete_begin(bebuf + n);
    n += build_ready_for_query(bebuf + n, 'T');

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, bebuf, n);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(sf.pending_pre_query_len, 0u);
    TEST_ASSERT_EQ(sf.pending_pre_query_absorbed, 0u);

    uint8_t got[32];
    ssize_t rr = recv(sv[1], got, sizeof(got), 0);
    TEST_ASSERT_EQ(rr, (ssize_t)sizeof(follow));
    TEST_ASSERT(memcmp(got, follow, sizeof(follow)) == 0);

    close_pair(sv);
    TEST_END();
}

static void test_absorb_without_rfq(void)
{
    TEST_BEGIN("pre_query: absorb without RFQ");

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_BEGIN_REPLAY;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;

    uint8_t bebuf[32];
    size_t n = build_command_complete_begin(bebuf);

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, bebuf, n);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_BEGIN_REPLAY);
    TEST_ASSERT(sf.pending_pre_query_absorbed >= n);

    TEST_END();
}

static void test_partial_then_completion(void)
{
    TEST_BEGIN("pre_query: split completion across recv calls");

    int sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 2;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = sv[0];

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_BEGIN_REPLAY;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;

    const uint8_t follow[] = { 'Q', 0, 0, 0, 8, 'x', ';', 0 };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    uint8_t cmsg[16];
    size_t csz = build_command_complete_begin(cmsg);

    /* First recv has only CommandComplete; replay should remain armed. */
    keel_flow_result_t r1 = keel_engine_flow_on_be_data(&sf, &session, cmsg, csz);
    TEST_ASSERT_EQ(r1, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_BEGIN_REPLAY);

    /* Second recv delivers ReadyForQuery and should release replay. */
    uint8_t zmsg[8];
    size_t zsz = build_ready_for_query(zmsg, 'T');
    keel_flow_result_t r2 = keel_engine_flow_on_be_data(&sf, &session, zmsg, zsz);
    TEST_ASSERT_EQ(r2, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);

    uint8_t got[32];
    ssize_t rr = recv(sv[1], got, sizeof(got), 0);
    TEST_ASSERT_EQ(rr, (ssize_t)sizeof(follow));
    TEST_ASSERT(memcmp(got, follow, sizeof(follow)) == 0);

    close_pair(sv);
    TEST_END();
}

static void test_disconnect_during_absorb(void)
{
    TEST_BEGIN("pre_query: disconnect during absorb");

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_BEGIN_REPLAY;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    sf.pending_pre_query_len = 4;

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, NULL, 0);
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(sf.pending_pre_query_len, 0u);

    TEST_END();
}

static void test_stash_overflow_on_resume(void)
{
    TEST_BEGIN("pre_query: stash overflow on resume_from_pool");

    int sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 3;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.begin_deferred = true;

    static const uint8_t begin_msg[] = {
        'Q', 0, 0, 0, 11,
        'B', 'E', 'G', 'I', 'N', ';', 0
    };
    memcpy(sf.begin_deferred_payload, begin_msg, sizeof(begin_msg));
    sf.begin_deferred_payload_len = sizeof(begin_msg);

    uint8_t* big = (uint8_t*)malloc(KEEL_PRE_QUERY_REPLAY_BUFSZ + 1);
    TEST_ASSERT_NOT_NULL(big);
    memset(big, 'A', KEEL_PRE_QUERY_REPLAY_BUFSZ + 1);

    sf.pending_msg = big;
    sf.pending_msg_len = KEEL_PRE_QUERY_REPLAY_BUFSZ + 1;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = sv[0];

    keel_flow_result_t r = keel_engine_flow_resume_from_pool(&sf, &session, &be);
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(sf.begin_deferred_payload_len, 0u);

    free(big);
    close_pair(sv);
    TEST_END();
}

static void test_runaway_absorption_guard(void)
{
    TEST_BEGIN("pre_query: runaway absorption guard");

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_BEGIN_REPLAY;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;

    size_t big_len = 64u * 1024u + 1u;
    uint8_t* blob = (uint8_t*)malloc(big_len);
    TEST_ASSERT_NOT_NULL(blob);
    memset(blob, 0, big_len);

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, blob, big_len);
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);

    free(blob);
    TEST_END();
}

static void test_state_sync_absorbs_setup_stream(void)
{
    TEST_BEGIN("pre_query: state sync absorbs setup stream");

    int be_sv[2] = { -1, -1 };
    int fe_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);
    TEST_ASSERT(make_socketpair(fe_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 4;

    state_profile_t be_profile;
    state_profile_t session_profile;
    state_profile_init(&be_profile);
    state_profile_init(&session_profile);
    TEST_ASSERT(state_profile_set(&be_profile, "search_path", "public") == 0);
    TEST_ASSERT(state_profile_set(&session_profile, "search_path", "tenant_a") == 0);

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];
    be.profile = &be_profile;
    be.current_state_hash = be_profile.hash;
    be.needs_sync = true;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.client_fd = fe_sv[0];
    session.backend_conn = &be;
    session.state_profile = &session_profile;
    session.state_hash = session_profile.hash;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_STATE_SYNC;
    sf.pending_state_sync_hash = session.state_hash;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;

    const uint8_t follow[] = { 'Q', 0, 0, 0, 13, 'S','E','L','E','C','T',' ','1', 0 };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    uint8_t bebuf[64];
    size_t n = 0;
    n += build_command_complete_tag(bebuf + n, "SET");
    n += build_ready_for_query(bebuf + n, 'I');

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, bebuf, n);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(be.current_state_hash, session.state_hash);
    TEST_ASSERT_EQ(be.needs_sync, false);
    TEST_ASSERT(state_profile_equal(&be_profile, &session_profile));

    uint8_t got[32];
    ssize_t rr = recv(be_sv[1], got, sizeof(got), 0);
    TEST_ASSERT_EQ(rr, (ssize_t)sizeof(follow));
    TEST_ASSERT(memcmp(got, follow, sizeof(follow)) == 0);

    uint8_t leaked[32];
    ssize_t cr = recv(fe_sv[1], leaked, sizeof(leaked), 0);
    TEST_ASSERT(cr < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    close_pair(be_sv);
    close_pair(fe_sv);
    TEST_END();
}

static void test_state_sync_waits_until_rfq(void)
{
    TEST_BEGIN("pre_query: state sync waits until RFQ");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 5;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];
    be.current_state_hash = 0x11;
    be.needs_sync = true;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.backend_conn = &be;
    session.state_hash = 0x22;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_STATE_SYNC;
    sf.pending_state_sync_hash = session.state_hash;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;

    const uint8_t follow[] = { 'Q', 0, 0, 0, 5, 0 };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    uint8_t cmsg[16];
    size_t csz = build_command_complete_tag(cmsg, "SET");
    keel_flow_result_t r1 = keel_engine_flow_on_be_data(&sf, &session, cmsg, csz);
    TEST_ASSERT_EQ(r1, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_STATE_SYNC);
    TEST_ASSERT_EQ(be.current_state_hash, 0x11ULL);

    uint8_t got[16];
    ssize_t rr = recv(be_sv[1], got, sizeof(got), 0);
    TEST_ASSERT(rr < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    uint8_t zmsg[8];
    size_t zsz = build_ready_for_query(zmsg, 'I');
    keel_flow_result_t r2 = keel_engine_flow_on_be_data(&sf, &session, zmsg, zsz);
    TEST_ASSERT_EQ(r2, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(be.current_state_hash, 0x22ULL);

    rr = recv(be_sv[1], got, sizeof(got), 0);
    TEST_ASSERT_EQ(rr, (ssize_t)sizeof(follow));

    close_pair(be_sv);
    TEST_END();
}

static void test_resume_from_pool_sends_state_sync_first(void)
{
    TEST_BEGIN("pre_query: resume_from_pool sends state sync first");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 6;

    state_profile_t be_profile;
    state_profile_t session_profile;
    state_profile_init(&be_profile);
    state_profile_init(&session_profile);
    TEST_ASSERT(state_profile_set(&be_profile, "search_path", "public") == 0);
    TEST_ASSERT(state_profile_set(&session_profile, "search_path", "tenant_b") == 0);

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];
    be.profile = &be_profile;
    be.current_state_hash = be_profile.hash;
    be.needs_sync = true;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.state_profile = &session_profile;
    session.state_hash = session_profile.hash;

    static const uint8_t follow[] = {
        'Q', 0, 0, 0, 14,
        'S','E','L','E','C','T',' ','4','2', 0
    };

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.mode = KEEL_TIER_FULL;
    sf.pending_msg = follow;
    sf.pending_msg_len = sizeof(follow);

    keel_flow_result_t r = keel_engine_flow_resume_from_pool(&sf, &session, &be);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_STATE_SYNC);
    TEST_ASSERT_EQ(sf.pending_pre_query_len, sizeof(follow));
    TEST_ASSERT_EQ(sf.pending_state_sync_hash, session.state_hash);
    TEST_ASSERT_EQ(session.backend_conn, &be);
    TEST_ASSERT_EQ(session.server_fd, be_sv[0]);

    uint8_t sync_msg[256];
    ssize_t sr = recv(be_sv[1], sync_msg, sizeof(sync_msg), 0);
    TEST_ASSERT(sr > 0);
    TEST_ASSERT_EQ(sync_msg[0], (uint8_t)'Q');
    TEST_ASSERT(buf_contains_bytes(sync_msg, (size_t)sr, "tenant_b", 8));

    uint8_t bebuf[64];
    size_t n = 0;
    n += build_command_complete_tag(bebuf + n, "SET");
    n += build_ready_for_query(bebuf + n, 'I');
    r = keel_engine_flow_on_be_data(&sf, &session, bebuf, n);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(be.current_state_hash, session.state_hash);
    TEST_ASSERT_EQ(be.needs_sync, false);
    TEST_ASSERT(state_profile_equal(&be_profile, &session_profile));

    uint8_t got[32];
    ssize_t rr = recv(be_sv[1], got, sizeof(got), 0);
    TEST_ASSERT_EQ(rr, (ssize_t)sizeof(follow));
    TEST_ASSERT(memcmp(got, follow, sizeof(follow)) == 0);

    close_pair(be_sv);
    TEST_END();
}

static void test_resume_from_pool_sequences_state_sync_then_begin(void)
{
    TEST_BEGIN("pre_query: resume_from_pool queues state sync then deferred BEGIN");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 8;

    state_profile_t be_profile;
    state_profile_t session_profile;
    state_profile_init(&be_profile);
    state_profile_init(&session_profile);
    TEST_ASSERT(state_profile_set(&be_profile, "search_path", "public") == 0);
    TEST_ASSERT(state_profile_set(&session_profile, "search_path", "tenant_c") == 0);

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];
    be.profile = &be_profile;
    be.current_state_hash = be_profile.hash;
    be.needs_sync = true;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.state_profile = &session_profile;
    session.state_hash = session_profile.hash;

    static const uint8_t follow[] = {
        'Q', 0, 0, 0, 14,
        'S','E','L','E','C','T',' ','7','7', 0
    };
    static const uint8_t begin_msg[] = {
        'Q', 0, 0, 0, 11,
        'B', 'E', 'G', 'I', 'N', ';', 0
    };

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.mode = KEEL_TIER_FULL;
    sf.pending_msg = follow;
    sf.pending_msg_len = sizeof(follow);
    sf.begin_deferred = true;
    memcpy(sf.begin_deferred_payload, begin_msg, sizeof(begin_msg));
    sf.begin_deferred_payload_len = sizeof(begin_msg);

    keel_flow_result_t r = keel_engine_flow_resume_from_pool(&sf, &session, &be);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_STATE_SYNC);
    TEST_ASSERT_EQ(sf.pre_query_count, 1u); /* deferred BEGIN queued */

    uint8_t sent1[256];
    ssize_t n1 = recv(be_sv[1], sent1, sizeof(sent1), 0);
    TEST_ASSERT(n1 > 0);
    TEST_ASSERT_EQ(sent1[0], (uint8_t)'Q');
    TEST_ASSERT(buf_contains_bytes(sent1, (size_t)n1, "tenant_c", 8));

    uint8_t bebuf1[64];
    size_t m1 = 0;
    m1 += build_command_complete_tag(bebuf1 + m1, "SET");
    m1 += build_ready_for_query(bebuf1 + m1, 'I');
    r = keel_engine_flow_on_be_data(&sf, &session, bebuf1, m1);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_BEGIN_REPLAY);
    TEST_ASSERT_EQ(sf.pre_query_count, 0u);

    uint8_t sent2[64];
    ssize_t n2 = recv(be_sv[1], sent2, sizeof(sent2), 0);
    TEST_ASSERT_EQ(n2, (ssize_t)sizeof(begin_msg));
    TEST_ASSERT(memcmp(sent2, begin_msg, sizeof(begin_msg)) == 0);

    uint8_t bebuf2[64];
    size_t m2 = 0;
    m2 += build_command_complete_tag(bebuf2 + m2, "BEGIN");
    m2 += build_ready_for_query(bebuf2 + m2, 'T');
    r = keel_engine_flow_on_be_data(&sf, &session, bebuf2, m2);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);

    uint8_t sent3[64];
    ssize_t n3 = recv(be_sv[1], sent3, sizeof(sent3), 0);
    TEST_ASSERT_EQ(n3, (ssize_t)sizeof(follow));
    TEST_ASSERT(memcmp(sent3, follow, sizeof(follow)) == 0);

    close_pair(be_sv);
    TEST_END();
}

static void test_resume_from_pool_cleanup_then_stmt_replay_queue(void)
{
    TEST_BEGIN("pre_query: resume_from_pool queues cleanup then stmt replay");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 9;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];
    be.needs_full_cleanup = true;
    be.stmt_set_hash = 0xBADC0FFEEULL;

    static const uint8_t replay_msg[] = {
        'P', 0, 0, 0, 9, 0, 0, 0, 0, 0,
        'S', 0, 0, 0, 4
    };
    fake_replay_ctx_t rctx = {
        .replay_buf = replay_msg,
        .replay_len = sizeof(replay_msg),
        .replay_count = 1,
        .replay_hash = 0xAA55AA55ULL,
    };

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;

    static const uint8_t follow[] = {
        'Q', 0, 0, 0, 14,
        'S','E','L','E','C','T',' ','8','8', 0
    };

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = &s_fake_replay_vt;
    sf.ctx = &rctx;
    sf.mode = KEEL_TIER_FULL;
    sf.pins = KEEL_FPIN_PREPARED_STMT;
    sf.pending_msg = follow;
    sf.pending_msg_len = sizeof(follow);

    keel_flow_result_t r = keel_engine_flow_resume_from_pool(&sf, &session, &be);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_STMT_REPLAY);
    TEST_ASSERT(sf.stmt_replay_needs_cleanup);
    TEST_ASSERT_EQ(sf.pre_query_count, 1u); /* stmt replay queued */

    uint8_t sent_cleanup[64];
    ssize_t n1 = recv(be_sv[1], sent_cleanup, sizeof(sent_cleanup), 0);
    TEST_ASSERT(n1 > 0);
    TEST_ASSERT_EQ(sent_cleanup[0], (uint8_t)'Q');

    uint8_t cleanup_resp[8];
    size_t cleanup_len = build_ready_for_query(cleanup_resp, 'I');
    r = keel_engine_flow_on_be_data(&sf, &session, cleanup_resp, cleanup_len);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_STMT_REPLAY);
    TEST_ASSERT_EQ(sf.stmt_replay_needs_cleanup, false);
    TEST_ASSERT_EQ(sf.pre_query_count, 0u);
    TEST_ASSERT_EQ(sf.stmt_replay_count, 1u);

    uint8_t sent_replay[64];
    ssize_t n2 = recv(be_sv[1], sent_replay, sizeof(sent_replay), 0);
    TEST_ASSERT_EQ(n2, (ssize_t)sizeof(replay_msg));
    TEST_ASSERT(memcmp(sent_replay, replay_msg, sizeof(replay_msg)) == 0);

    uint8_t replay_resp[16];
    size_t rr = 0;
    rr += build_parse_complete(replay_resp + rr);
    rr += build_ready_for_query(replay_resp + rr, 'I');
    r = keel_engine_flow_on_be_data(&sf, &session, replay_resp, rr);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.stmt_replay_count, 0u);
    TEST_ASSERT_EQ(sf.stmt_replay_rfq_pending, false);
    TEST_ASSERT_EQ(be.stmt_set_hash, rctx.replay_hash);

    uint8_t sent_follow[64];
    ssize_t n3 = recv(be_sv[1], sent_follow, sizeof(sent_follow), 0);
    TEST_ASSERT_EQ(n3, (ssize_t)sizeof(follow));
    TEST_ASSERT(memcmp(sent_follow, follow, sizeof(follow)) == 0);

    close_pair(be_sv);
    TEST_END();
}

static void test_cleanup_more_then_complete_keeps_follow_held(void)
{
    TEST_BEGIN("pre_query: cleanup MORE keeps follow held until complete");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 10;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];
    be.needs_full_cleanup = true;
    be.stmt_set_hash = 0x9999ULL;

    static const uint8_t replay_msg[] = {
        'P', 0, 0, 0, 9, 0, 0, 0, 0, 0,
        'S', 0, 0, 0, 4
    };
    fake_replay_ctx_t rctx = {
        .replay_buf = replay_msg,
        .replay_len = sizeof(replay_msg),
        .replay_count = 1,
        .replay_hash = 0x2222ULL,
        .cleanup_mode = FAKE_CLEANUP_MORE_THEN_COMPLETE,
    };

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;

    static const uint8_t follow[] = {
        'Q', 0, 0, 0, 14,
        'S','E','L','E','C','T',' ','9','9', 0
    };

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = &s_fake_replay_vt;
    sf.ctx = &rctx;
    sf.mode = KEEL_TIER_FULL;
    sf.pins = KEEL_FPIN_PREPARED_STMT;
    sf.pending_msg = follow;
    sf.pending_msg_len = sizeof(follow);

    keel_flow_result_t r = keel_engine_flow_resume_from_pool(&sf, &session, &be);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_STMT_REPLAY);
    TEST_ASSERT(sf.stmt_replay_needs_cleanup);

    uint8_t sent_cleanup[64];
    ssize_t n1 = recv(be_sv[1], sent_cleanup, sizeof(sent_cleanup), 0);
    TEST_ASSERT(n1 > 0);
    TEST_ASSERT_EQ(sent_cleanup[0], (uint8_t)'Q');

    uint8_t cleanup_part1[4] = { 0 };
    r = keel_engine_flow_on_be_data(&sf, &session, cleanup_part1, sizeof(cleanup_part1));
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_STMT_REPLAY);
    TEST_ASSERT(sf.stmt_replay_needs_cleanup);
    TEST_ASSERT_EQ(sf.pre_query_count, 1u);

    uint8_t nothing[16];
    ssize_t n_none = recv(be_sv[1], nothing, sizeof(nothing), 0);
    TEST_ASSERT(n_none < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    uint8_t cleanup_part2[4] = { 0 };
    r = keel_engine_flow_on_be_data(&sf, &session, cleanup_part2, sizeof(cleanup_part2));
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_STMT_REPLAY);
    TEST_ASSERT_EQ(sf.stmt_replay_needs_cleanup, false);
    TEST_ASSERT_EQ(sf.stmt_replay_count, 1u);

    uint8_t sent_replay[64];
    ssize_t n2 = recv(be_sv[1], sent_replay, sizeof(sent_replay), 0);
    TEST_ASSERT_EQ(n2, (ssize_t)sizeof(replay_msg));

    close_pair(be_sv);
    TEST_END();
}

static void test_cleanup_and_replay_coalesced_response_preserved(void)
{
    TEST_BEGIN("pre_query: coalesced cleanup terminal + replay responses preserved");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 12;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];
    be.needs_full_cleanup = true;
    be.stmt_set_hash = 0x7070ULL;

    static const uint8_t replay_msg[] = {
        'P', 0, 0, 0, 9, 0, 0, 0, 0, 0,
        'S', 0, 0, 0, 4
    };
    fake_replay_ctx_t rctx = {
        .replay_buf = replay_msg,
        .replay_len = sizeof(replay_msg),
        .replay_count = 1,
        .replay_hash = 0x3030ULL,
        .cleanup_mode = FAKE_CLEANUP_CONSUME_FIRST_FRAME,
    };

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.backend_conn = &be;

    static const uint8_t follow[] = {
        'Q', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '5', 0
    };

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = &s_fake_replay_vt;
    sf.ctx = &rctx;
    sf.mode = KEEL_TIER_FULL;
    sf.pins = KEEL_FPIN_PREPARED_STMT;
    sf.pending_msg = follow;
    sf.pending_msg_len = sizeof(follow);

    keel_flow_result_t r = keel_engine_flow_resume_from_pool(&sf, &session, &be);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_STMT_REPLAY);
    TEST_ASSERT(sf.stmt_replay_needs_cleanup);
    TEST_ASSERT_EQ(sf.pre_query_count, 1u);

    uint8_t sent_cleanup[64];
    ssize_t n1 = recv(be_sv[1], sent_cleanup, sizeof(sent_cleanup), 0);
    TEST_ASSERT(n1 > 0);
    TEST_ASSERT_EQ(sent_cleanup[0], (uint8_t)'Q');

    uint8_t combo[64];
    size_t cl = 0;
    cl += build_ready_for_query(combo + cl, 'I');
    cl += build_parse_complete(combo + cl);
    cl += build_ready_for_query(combo + cl, 'I');

    r = keel_engine_flow_on_be_data(&sf, &session, combo, cl);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_STMT_REPLAY);
    TEST_ASSERT_EQ(sf.stmt_replay_needs_cleanup, false);
    TEST_ASSERT_EQ(sf.stmt_replay_count, 1u);

    uint8_t sent_replay[64];
    ssize_t n2 = recv(be_sv[1], sent_replay, sizeof(sent_replay), 0);
    TEST_ASSERT_EQ(n2, (ssize_t)sizeof(replay_msg));
    TEST_ASSERT(memcmp(sent_replay, replay_msg, sizeof(replay_msg)) == 0);

    size_t tail_len = keel_residual_len(&session.server_residual);
    TEST_ASSERT_EQ(tail_len, cl - 6u);
    uint8_t tail[64];
    size_t consumed = keel_residual_consume(&session.server_residual, tail, tail_len);
    TEST_ASSERT_EQ(consumed, tail_len);

    r = keel_engine_flow_on_be_data(&sf, &session, tail, tail_len);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(be.stmt_set_hash, rctx.replay_hash);

    uint8_t sent_follow[64];
    ssize_t n3 = recv(be_sv[1], sent_follow, sizeof(sent_follow), 0);
    TEST_ASSERT_EQ(n3, (ssize_t)sizeof(follow));
    TEST_ASSERT(memcmp(sent_follow, follow, sizeof(follow)) == 0);

    close_pair(be_sv);
    TEST_END();
}

static void test_cleanup_error_never_forwards_follow(void)
{
    TEST_BEGIN("pre_query: cleanup ERROR never forwards follow");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    fake_replay_ctx_t rctx;
    memset(&rctx, 0, sizeof(rctx));
    rctx.cleanup_mode = FAKE_CLEANUP_ERROR;

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = &s_fake_replay_vt;
    sf.ctx = &rctx;
    sf.stmt_replay_needs_cleanup = true;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    static const uint8_t follow[] = { 'Q', 0, 0, 0, 5, 0 };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    uint8_t cleanup_resp[6] = { 'Z', 0, 0, 0, 5, 'I' };
    keel_flow_result_t r = keel_engine_flow_on_be_data(
        &sf, &session, cleanup_resp, sizeof(cleanup_resp));
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);

    uint8_t got[32];
    ssize_t nr = recv(be_sv[1], got, sizeof(got), 0);
    TEST_ASSERT(nr < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    close_pair(be_sv);
    TEST_END();
}

static void test_cleanup_bad_consumed_never_forwards_follow(void)
{
    TEST_BEGIN("pre_query: cleanup bad-consumed never forwards follow");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    fake_replay_ctx_t rctx;
    memset(&rctx, 0, sizeof(rctx));
    rctx.cleanup_mode = FAKE_CLEANUP_BAD_CONSUMED;

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = &s_fake_replay_vt;
    sf.ctx = &rctx;
    sf.stmt_replay_needs_cleanup = true;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    static const uint8_t follow[] = { 'Q', 0, 0, 0, 5, 0 };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    uint8_t cleanup_resp[6] = { 'Z', 0, 0, 0, 5, 'I' };
    keel_flow_result_t r = keel_engine_flow_on_be_data(
        &sf, &session, cleanup_resp, sizeof(cleanup_resp));
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);

    uint8_t got[32];
    ssize_t nr = recv(be_sv[1], got, sizeof(got), 0);
    TEST_ASSERT(nr < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    close_pair(be_sv);
    TEST_END();
}

static void test_replay_error_response_never_forwards_follow(void)
{
    TEST_BEGIN("pre_query: replay ErrorResponse never forwards follow");

    int be_sv[2] = { -1, -1 };
    int fe_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);
    TEST_ASSERT(make_socketpair(fe_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 11;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];
    be.stmt_set_hash = 0xABCDULL;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.client_fd = fe_sv[0];
    session.backend_conn = &be;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.stmt_replay_count = 1;
    sf.stmt_replay_hash = 0xDEADULL;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    static const uint8_t follow[] = { 'Q', 0, 0, 0, 5, 0 };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    uint8_t emsg[128];
    size_t elen = build_error_response(emsg, "duplicate prepared statement");
    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, emsg, elen);
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);
    TEST_ASSERT_EQ(sf.stmt_replay_count, 0u);
    TEST_ASSERT_EQ(sf.stmt_replay_rfq_pending, false);
    TEST_ASSERT_EQ(sf.pending_pre_query_len, 0u);
    TEST_ASSERT_EQ(sf.stmt_replay_hash, 0ULL);
    TEST_ASSERT_EQ(be.stmt_set_hash, 0ULL);

    uint8_t be_out[64];
    ssize_t n1 = recv(be_sv[1], be_out, sizeof(be_out), 0);
    TEST_ASSERT(n1 < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    uint8_t fe_out[256];
    ssize_t n2 = recv(fe_sv[1], fe_out, sizeof(fe_out), 0);
    TEST_ASSERT_EQ(n2, (ssize_t)elen);
    TEST_ASSERT(memcmp(fe_out, emsg, elen) == 0);

    ssize_t n3 = recv(be_sv[1], be_out, sizeof(be_out), 0);
    TEST_ASSERT(n3 < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    close_pair(be_sv);
    close_pair(fe_sv);
    TEST_END();
}

static void test_replay_parsecomplete_waits_for_rfq_before_forward(void)
{
    TEST_BEGIN("pre_query: replay waits for RFQ before forwarding follow");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.backend_conn = &be;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.stmt_replay_count = 1;
    sf.stmt_replay_hash = 0x1010ULL;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    static const uint8_t follow[] = {
        'Q', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '1', 0
    };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    uint8_t parse_ok[8];
    size_t p = build_parse_complete(parse_ok);
    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, parse_ok, p);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_STMT_REPLAY);
    TEST_ASSERT_EQ(sf.stmt_replay_count, 0u);
    TEST_ASSERT_EQ(sf.stmt_replay_rfq_pending, true);

    uint8_t out[64];
    ssize_t n1 = recv(be_sv[1], out, sizeof(out), 0);
    TEST_ASSERT(n1 < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    uint8_t rfq[8];
    size_t z = build_ready_for_query(rfq, 'I');
    r = keel_engine_flow_on_be_data(&sf, &session, rfq, z);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.stmt_replay_rfq_pending, false);
    TEST_ASSERT_EQ(be.stmt_set_hash, 0x1010ULL);

    ssize_t n2 = recv(be_sv[1], out, sizeof(out), 0);
    TEST_ASSERT_EQ(n2, (ssize_t)sizeof(follow));
    TEST_ASSERT(memcmp(out, follow, sizeof(follow)) == 0);

    close_pair(be_sv);
    TEST_END();
}

static void test_replay_discards_notice_and_parameter_status(void)
{
    TEST_BEGIN("pre_query: replay drains Notice/ParameterStatus before RFQ");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.backend_conn = &be;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.stmt_replay_count = 1;
    sf.stmt_replay_hash = 0x8888ULL;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    static const uint8_t follow[] = {
        'Q', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '6', 0
    };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    uint8_t replay_resp[128];
    size_t n = 0;
    n += build_parse_complete(replay_resp + n);
    n += build_notice_response(replay_resp + n, "replay notice");
    n += build_parameter_status(replay_resp + n, "application_name", "keel");
    n += build_ready_for_query(replay_resp + n, 'I');

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, replay_resp, n);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.stmt_replay_count, 0u);
    TEST_ASSERT_EQ(sf.stmt_replay_rfq_pending, false);
    TEST_ASSERT_EQ(be.stmt_set_hash, 0x8888ULL);

    uint8_t out[64];
    ssize_t nr = recv(be_sv[1], out, sizeof(out), 0);
    TEST_ASSERT_EQ(nr, (ssize_t)sizeof(follow));
    TEST_ASSERT(memcmp(out, follow, sizeof(follow)) == 0);

    close_pair(be_sv);
    TEST_END();
}

static void test_replay_partial_rfq_header_is_preserved(void)
{
    TEST_BEGIN("pre_query: replay preserves partial RFQ header via residual");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.backend_conn = &be;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.stmt_replay_count = 1;
    sf.stmt_replay_hash = 0x4545ULL;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    static const uint8_t follow[] = {
        'Q', 0, 0, 0, 13, 'S', 'E', 'L', 'E', 'C', 'T', ' ', '7', 0
    };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    uint8_t first[16];
    size_t n1 = 0;
    n1 += build_parse_complete(first + n1);
    first[n1++] = 'Z';
    first[n1++] = 0;
    first[n1++] = 0;

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, first, n1);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_STMT_REPLAY);
    TEST_ASSERT_EQ(sf.stmt_replay_count, 0u);
    TEST_ASSERT_EQ(sf.stmt_replay_rfq_pending, true);
    TEST_ASSERT_EQ(keel_residual_len(&session.server_residual), 3u);

    uint8_t tail[16];
    size_t residual_len = keel_residual_len(&session.server_residual);
    size_t got = keel_residual_consume(&session.server_residual, tail, residual_len);
    TEST_ASSERT_EQ(got, residual_len);
    tail[got++] = 0;
    tail[got++] = 5;
    tail[got++] = 'I';

    r = keel_engine_flow_on_be_data(&sf, &session, tail, got);
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.stmt_replay_rfq_pending, false);
    TEST_ASSERT_EQ(be.stmt_set_hash, 0x4545ULL);

    uint8_t out[64];
    ssize_t nr = recv(be_sv[1], out, sizeof(out), 0);
    TEST_ASSERT_EQ(nr, (ssize_t)sizeof(follow));
    TEST_ASSERT(memcmp(out, follow, sizeof(follow)) == 0);

    close_pair(be_sv);
    TEST_END();
}

static void test_state_sync_rejects_non_idle_rfq(void)
{
    TEST_BEGIN("pre_query: state sync rejects non-idle RFQ");

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 7;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.current_state_hash = 0x1111;
    be.needs_sync = true;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.backend_conn = &be;
    session.state_hash = 0x2222;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_STATE_SYNC;
    sf.pending_state_sync_hash = session.state_hash;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    sf.pending_pre_query_len = 4;

    uint8_t zmsg[8];
    size_t zsz = build_ready_for_query(zmsg, 'T');
    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, zmsg, zsz);
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(sf.pending_state_sync_hash, 0ULL);
    TEST_ASSERT_EQ(be.current_state_hash, 0x1111ULL);
    TEST_ASSERT_EQ(be.needs_sync, true);

    TEST_END();
}

static void test_state_sync_rejects_malformed_frame(void)
{
    TEST_BEGIN("pre_query: state sync rejects malformed frame");

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_STATE_SYNC;
    sf.pending_state_sync_hash = 0x1234;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;

    uint8_t bad[] = { 'C', 0, 0, 0, 3 };
    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, bad, sizeof(bad));
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(sf.pending_state_sync_hash, 0ULL);

    TEST_END();
}

static void test_deferred_begin_error_response_aborts(void)
{
    TEST_BEGIN("pre_query: deferred BEGIN ErrorResponse aborts and does not forward follow");

    int fe_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(fe_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 10;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.backend_conn = &be;
    session.client_fd = fe_sv[0];

    const uint8_t follow[] = { 'Q', 0, 0, 0, 13, 'S','E','L','E','C','T',' ','1', 0 };

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_BEGIN_REPLAY;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    /* Backend rejects BEGIN — e.g. already in a transaction or server error. */
    uint8_t emsg[128];
    size_t elen = build_error_response(emsg, "cannot execute BEGIN in a transaction block");

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, emsg, elen);
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);

    /* Pre-query state must be cleared; the buffered follow payload must not survive. */
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(sf.pending_pre_query_len, 0U);

    close_pair(fe_sv);
    TEST_END();
}

static void test_state_sync_error_response_aborts_sync(void)
{
    TEST_BEGIN("pre_query: state sync ErrorResponse aborts without stamping hash");

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 8;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.current_state_hash = 0xAAAA;
    be.needs_sync = true;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.backend_conn = &be;
    session.state_hash = 0xBBBB;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_STATE_SYNC;
    sf.pending_state_sync_hash = session.state_hash;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    sf.pending_pre_query_len = 4; /* simulate a held client payload */

    /* Backend responds to a failed SET: ErrorResponse + ReadyForQuery(I).
     * The absorber must detect the ErrorResponse, abort, and NOT stamp the
     * backend hash or clear needs_sync. */
    uint8_t bebuf[128];
    size_t n = 0;
    n += build_error_response(bebuf + n, "invalid value for parameter");
    n += build_ready_for_query(bebuf + n, 'I');

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, bebuf, n);
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);

    /* Hash must NOT have been stamped; backend remains dirty. */
    TEST_ASSERT_EQ(be.current_state_hash, 0xAAAAULL);
    TEST_ASSERT_EQ(be.needs_sync, true);

    /* Pre-query state must be fully cleared. */
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(sf.pending_state_sync_hash, 0ULL);
    TEST_ASSERT_EQ(sf.pending_pre_query_len, 0U);

    TEST_END();
}

static void test_state_sync_error_response_only(void)
{
    TEST_BEGIN("pre_query: state sync bare ErrorResponse (no trailing RFQ) aborts");

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 9;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.current_state_hash = 0x1111;
    be.needs_sync = true;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.backend_conn = &be;
    session.state_hash = 0x2222;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query = KEEL_PRE_QUERY_STATE_SYNC;
    sf.pending_state_sync_hash = session.state_hash;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    sf.pending_pre_query_len = 4;

    /* Only ErrorResponse, no ReadyForQuery yet — must still abort immediately. */
    uint8_t emsg[128];
    size_t elen = build_error_response(emsg, "SET rejected");

    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, emsg, elen);
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);
    TEST_ASSERT_EQ(be.current_state_hash, 0x1111ULL);
    TEST_ASSERT_EQ(be.needs_sync, true);
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);

    TEST_END();
}

/* §15.3 fault injection: backend kill (EOF) while STATE_SYNC is waiting for RFQ.
 *
 * The engine sends a SET command to the backend and waits for RFQ.  If the
 * backend socket closes (recv returns 0, represented here as data=NULL len=0)
 * before RFQ arrives the absorber must:
 *   - Return KEEL_FLOW_ERROR immediately.
 *   - Clear all pending pre-query state.
 *   - NOT stamp the new hash onto the backend (hash remains stale/dirty).
 *   - Leave needs_sync=true so the slot is marked for cleanup or close.
 *
 * This is distinct from test_disconnect_during_absorb which tests EOF during
 * KEEL_PRE_QUERY_BEGIN_REPLAY, not during KEEL_PRE_QUERY_STATE_SYNC.
 */
static void test_backend_eof_during_state_sync(void)
{
    TEST_BEGIN("pre_query: backend EOF during state sync aborts without stamping hash");

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 20;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.current_state_hash = 0xDEAD1111ULL;
    be.needs_sync = true;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.backend_conn = &be;
    session.state_hash = 0xBEEF2222ULL;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.pending_pre_query      = KEEL_PRE_QUERY_STATE_SYNC;
    sf.pending_state_sync_hash = session.state_hash;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    sf.pending_pre_query_len   = 6; /* simulate a queued follow message */

    /* Backend closes socket mid-sync: data=NULL, len=0. */
    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, NULL, 0);

    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);

    /* Hash must NOT be stamped — backend is still dirty. */
    TEST_ASSERT_EQ(be.current_state_hash, 0xDEAD1111ULL);
    TEST_ASSERT_EQ(be.needs_sync, true);

    /* All pre-query tracking must be cleared. */
    TEST_ASSERT_EQ(sf.pending_pre_query, KEEL_PRE_QUERY_NONE);
    TEST_ASSERT_EQ(sf.pending_state_sync_hash, 0ULL);
    TEST_ASSERT_EQ(sf.pending_pre_query_len, 0U);

    TEST_END();
}

/* §15.3 fault injection: backend EOF while stmt replay is waiting for ParseComplete + RFQ.
 *
 * The engine has sent a Parse message to replay a prepared statement.  The
 * backend closes the connection before ParseComplete arrives.  The absorber must:
 *   - Return KEEL_FLOW_ERROR.
 *   - NOT forward the held follow payload to the backend.
 *   - Clear stmt_replay state (count, hash, rfq_pending).
 *   - NOT update be.stmt_set_hash.
 */
static void test_backend_eof_during_stmt_replay(void)
{
    TEST_BEGIN("pre_query: backend EOF during stmt replay clears replay state");

    int be_sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(be_sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.id = 21;

    backend_conn_t be;
    memset(&be, 0, sizeof(be));
    be.fd = be_sv[0];
    be.stmt_set_hash = 0xCAFEBABEULL;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = &worker;
    session.server_fd = be_sv[0];
    session.backend_conn = &be;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow = VT;
    sf.stmt_replay_count  = 2;
    sf.stmt_replay_hash   = 0xFACEFEEDULL;
    sf.stmt_replay_rfq_pending = false;
    sf.pending_pre_query_resume = KEEL_FLOW_WAIT_BACKEND;
    static const uint8_t follow[] = { 'Q', 0, 0, 0, 5, 0 };
    memcpy(sf.pending_pre_query_buf, follow, sizeof(follow));
    sf.pending_pre_query_len = sizeof(follow);

    /* Backend closes socket while we are waiting for ParseComplete. */
    keel_flow_result_t r = keel_engine_flow_on_be_data(&sf, &session, NULL, 0);

    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);

    /* stmt_replay tracking must be cleared. */
    TEST_ASSERT_EQ(sf.stmt_replay_count, 0u);
    TEST_ASSERT_EQ(sf.stmt_replay_rfq_pending, false);
    TEST_ASSERT_EQ(sf.pending_pre_query_len, 0u);

    /* stmt_set_hash on the backend must NOT be updated. */
    TEST_ASSERT_EQ(be.stmt_set_hash, 0xCAFEBABEULL);

    /* The follow message must NOT have been forwarded to the backend. */
    uint8_t out[64];
    ssize_t nr = recv(be_sv[1], out, sizeof(out), 0);
    TEST_ASSERT(nr < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));

    close_pair(be_sv);
    TEST_END();
}

int main(void)
{
    printf("=== Async Pre-Query Replay Tests ===\n");

    test_fe_tokenless_write_stamps_sticky_primary();
    test_fe_tokenless_ddl_stamps_sticky_primary();
    test_fe_write_capture_pending_preserves_tokenless_stickiness();
    test_fe_read_does_not_stamp_sticky_primary();
    test_extended_sync_does_not_use_linked_send();
    test_post_write_capture_success_updates_ssv();
    test_post_write_capture_failure_fails_closed();
    test_flag_lifecycle_and_forward_on_rfq();
    test_absorb_without_rfq();
    test_partial_then_completion();
    test_disconnect_during_absorb();
    test_stash_overflow_on_resume();
    test_runaway_absorption_guard();
    test_state_sync_absorbs_setup_stream();
    test_state_sync_waits_until_rfq();
    test_resume_from_pool_sends_state_sync_first();
    test_resume_from_pool_sequences_state_sync_then_begin();
    test_resume_from_pool_cleanup_then_stmt_replay_queue();
    test_cleanup_more_then_complete_keeps_follow_held();
    test_cleanup_and_replay_coalesced_response_preserved();
    test_cleanup_error_never_forwards_follow();
    test_cleanup_bad_consumed_never_forwards_follow();
    test_replay_error_response_never_forwards_follow();
    test_replay_parsecomplete_waits_for_rfq_before_forward();
    test_replay_discards_notice_and_parameter_status();
    test_replay_partial_rfq_header_is_preserved();
    test_state_sync_rejects_non_idle_rfq();
    test_state_sync_rejects_malformed_frame();
    test_deferred_begin_error_response_aborts();
    test_state_sync_error_response_aborts_sync();
    test_state_sync_error_response_only();
    test_backend_eof_during_state_sync();
    test_backend_eof_during_stmt_replay();

    return test_summary();
}
