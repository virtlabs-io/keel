/**
 * @file test_pre_query_replay.c
 * @brief Unit tests for async deferred-BEGIN pre-query replay (PR #4).
 */

#include "test_utils.h"

#include "keel/engine/engine_flow.h"
#include "keel/engine/backend_pool.h"
#include "keel/session/session.h"
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
} fake_replay_ctx_t;

enum {
    FAKE_CLEANUP_COMPLETE = 0,
    FAKE_CLEANUP_MORE_THEN_COMPLETE = 1,
    FAKE_CLEANUP_ERROR = 2,
    FAKE_CLEANUP_BAD_CONSUMED = 3,
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

static const keel_proto_flow_vtable_t s_fake_replay_vt = {
    .build_cleanup = fake_build_cleanup,
    .drain_cleanup_response = fake_drain_cleanup_response,
    .get_stmt_replay = fake_get_stmt_replay,
};

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
    TEST_ASSERT_EQ(r, KEEL_FLOW_WAIT_BACKEND);
    TEST_ASSERT_EQ(sf.stmt_replay_count, 0u);
    TEST_ASSERT_EQ(sf.stmt_replay_rfq_pending, false);
    TEST_ASSERT_EQ(sf.pending_pre_query_len, 0u);
    TEST_ASSERT_EQ(sf.stmt_replay_hash, 0ULL);
    TEST_ASSERT_EQ(be.stmt_set_hash, 0ULL);

    uint8_t be_out[64];
    ssize_t n1 = recv(be_sv[1], be_out, sizeof(be_out), 0);
    TEST_ASSERT_EQ(n1, 5);
    TEST_ASSERT_EQ(be_out[0], (uint8_t)'S'); /* Sync */

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

int main(void)
{
    printf("=== Async Pre-Query Replay Tests ===\n");

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
    test_cleanup_error_never_forwards_follow();
    test_cleanup_bad_consumed_never_forwards_follow();
    test_replay_error_response_never_forwards_follow();
    test_replay_parsecomplete_waits_for_rfq_before_forward();
    test_state_sync_rejects_non_idle_rfq();
    test_state_sync_rejects_malformed_frame();

    return test_summary();
}
