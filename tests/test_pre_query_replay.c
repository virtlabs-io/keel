/**
 * @file test_pre_query_replay.c
 * @brief Unit tests for async deferred-BEGIN pre-query replay (PR #4).
 */

#include "test_utils.h"

#include "keel/engine/engine_flow.h"
#include "keel/engine/backend_pool.h"
#include "keel/session/session.h"
#include "keel/protocol/protocol_flow.h"

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

static size_t build_command_complete_begin(uint8_t* buf)
{
    const char* tag = "BEGIN";
    size_t tlen = strlen(tag) + 1; /* include NUL */
    buf[0] = 'C';
    wr32(buf + 1, (uint32_t)(4 + tlen));
    memcpy(buf + 5, tag, tlen);
    return 1 + 4 + tlen;
}

static size_t build_ready_for_query(uint8_t* buf, char status)
{
    buf[0] = 'Z';
    wr32(buf + 1, 5);
    buf[5] = (uint8_t)status;
    return 6;
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

int main(void)
{
    printf("=== Async Pre-Query Replay Tests ===\n");

    test_flag_lifecycle_and_forward_on_rfq();
    test_absorb_without_rfq();
    test_partial_then_completion();
    test_disconnect_during_absorb();
    test_stash_overflow_on_resume();
    test_runaway_absorption_guard();

    return test_summary();
}
