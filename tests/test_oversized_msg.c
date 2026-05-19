/**
 * @file test_oversized_msg.c
 * @brief Oversized-message enforcement tests (§15.2 / §10 follow-up).
 *
 * Verifies that the engine correctly enforces session_max_buffered_bytes
 * and backend_max_replay_bytes when set, and allows all messages through
 * when those limits are zero (unlimited).
 *
 * §1  FE message limit — basic block and allow
 * §2  FE message limit — boundary conditions
 * §3  FE message limit — multiple messages: oversized rejects, normal passes
 * §4  FE message limit — jumbo continuation: limit set on first chunk
 * §5  PS replay limit — backend_max_replay_bytes block and allow
 */

#include "test_utils.h"

#include "keel/engine/engine_flow.h"
#include "keel/engine/backend_pool.h"
#include "keel/session/session.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/mem/mem.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ---- helpers ---- */

static void wr32(uint8_t* p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)(v);
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

/* ---- minimal fake vtable for FE message tests ---- */

static ssize_t fake_fe_frame_len(void* ctx, const uint8_t* data, size_t len, int dir)
{
    (void)ctx;
    (void)dir;
    /* PG wire format: type (1 byte) + length (4 bytes, includes itself) + body */
    if (len < 5)
        return 0;
    uint32_t ml = ((uint32_t)data[1] << 24) |
                  ((uint32_t)data[2] << 16) |
                  ((uint32_t)data[3] << 8)  |
                  (uint32_t)data[4];
    if (ml < 4)
        return -1;
    return (ssize_t)(1u + ml);
}

static int fake_fe_on_fe_msg(void* ctx,
                             const uint8_t* data,
                             size_t len,
                             keel_fe_action_t* act)
{
    (void)ctx;
    *act = keel_fe_action_default();
    if (!data || len < 1) {
        act->type = KEEL_FE_ACT_ERROR;
        return -1;
    }
    /* For 'Q' (simple query), return a forward-to-backend action */
    if (data[0] == 'Q') {
        act->type        = KEEL_FE_ACT_QUERY;
        act->be_payload  = data;
        act->be_payload_len = len;
    } else {
        act->type = KEEL_FE_ACT_IGNORE;
    }
    return 0;
}

static const keel_proto_flow_vtable_t s_fake_fe_vt = {
    .frame_len  = fake_fe_frame_len,
    .on_fe_msg  = fake_fe_on_fe_msg,
};

/* ---- fake vtable for PS replay tests ---- */

static uint8_t* s_replay_buf  = NULL;
static size_t   s_replay_len  = 0;
static uint32_t s_replay_count = 0;
static uint64_t s_replay_hash  = 0;

static int fake_get_stmt_replay(void* ctx,
                                uint8_t** out_buf,
                                size_t*   out_len,
                                uint32_t* out_count,
                                uint64_t* out_hash)
{
    (void)ctx;
    if (out_hash)
        *out_hash = s_replay_hash;
    if (!out_buf || !out_len || !out_count)
        return 0;
    if (!s_replay_buf || s_replay_len == 0) {
        *out_buf   = NULL;
        *out_len   = 0;
        *out_count = 0;
        return 0;
    }
    uint8_t* cp = (uint8_t*)keel_malloc(s_replay_len);
    if (!cp)
        return -1;
    memcpy(cp, s_replay_buf, s_replay_len);
    *out_buf   = cp;
    *out_len   = s_replay_len;
    *out_count = s_replay_count;
    return 0;
}

static ssize_t fake_replay_frame_len(void* ctx, const uint8_t* data, size_t len, int dir)
{
    (void)ctx;
    (void)dir;
    if (len < 5)
        return 0;
    uint32_t ml = ((uint32_t)data[1] << 24) |
                  ((uint32_t)data[2] << 16) |
                  ((uint32_t)data[3] << 8)  |
                  (uint32_t)data[4];
    if (ml < 4)
        return -1;
    return (ssize_t)(1u + ml) <= (ssize_t)len ? (ssize_t)(1u + ml) : 0;
}

static int fake_replay_on_fe_msg(void* ctx,
                                 const uint8_t* data,
                                 size_t len,
                                 keel_fe_action_t* act)
{
    (void)ctx;
    *act = keel_fe_action_default();
    if (!data || len < 1) {
        act->type = KEEL_FE_ACT_ERROR;
        return -1;
    }
    if (data[0] == 'B') {
        /* Bind — triggers PS replay path in SMART/FULL tier */
        act->type          = KEEL_FE_ACT_QUERY;
        act->be_payload    = data;
        act->be_payload_len = len;
        act->needs_stmt_replay = true;
    } else if (data[0] == 'Q') {
        act->type          = KEEL_FE_ACT_QUERY;
        act->be_payload    = data;
        act->be_payload_len = len;
    } else {
        act->type = KEEL_FE_ACT_IGNORE;
    }
    return 0;
}

static int fake_replay_build_cleanup(void* ctx, keel_cleanup_reason_t r,
                                     uint8_t* buf, size_t blen)
{
    (void)ctx;
    (void)r;
    /* minimal Q cleanup */
    static const uint8_t kq[] = { 'Q', 0, 0, 0, 5, 0 };
    if (!buf || blen < sizeof(kq))
        return -1;
    memcpy(buf, kq, sizeof(kq));
    return (int)sizeof(kq);
}

static const keel_proto_flow_vtable_t s_fake_replay_vt = {
    .frame_len          = fake_replay_frame_len,
    .on_fe_msg          = fake_replay_on_fe_msg,
    .get_stmt_replay    = fake_get_stmt_replay,
    .build_cleanup      = fake_replay_build_cleanup,
};

/* ============================================================================
 * §1  FE message limit — basic block and allow
 * ============================================================================ */

/* Build a PG 'Q' header declaring @total_frame total bytes. */
static void build_query_header(uint8_t* buf, size_t total_frame)
{
    /* total_frame = 1 (type) + length_field
     * length_field includes itself (4 bytes), so payload = total_frame - 5 */
    uint32_t length_field = (uint32_t)(total_frame - 1);
    buf[0] = 'Q';
    wr32(buf + 1, length_field);
}

static void test_limit_blocks_oversized_fe_message(void)
{
    TEST_BEGIN("oversized_msg: session_max_buffered_bytes blocks too-large FE message");

    /* 8 KiB limit, message declares 64 KiB (way over) */
    const size_t limit        = 8192;
    const size_t frame_total  = 65536;   /* declared total, well above limit */

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker     = &worker;
    session.server_fd  = -1;  /* no backend needed — error before send */
    session.client_fd  = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow                       = &s_fake_fe_vt;
    sf.phase                      = KEEL_PHASE_READY;
    sf.mode                       = KEEL_TIER_POOL;
    sf.session_max_buffered_bytes = limit;

    /* Send only the 5-byte header but with a large declared length */
    uint8_t buf[5];
    build_query_header(buf, frame_total);

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, buf, sizeof(buf));
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);

    TEST_END();
}

static void test_limit_zero_allows_large_fe_message(void)
{
    TEST_BEGIN("oversized_msg: session_max_buffered_bytes=0 allows large FE message (jumbo streaming)");

    /* No limit (0 = unlimited), send a large-declared message through a real socket */
    const size_t frame_total = 65536;

    int sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker    = &worker;
    session.server_fd = sv[0];   /* backend side */
    session.client_fd = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow                       = &s_fake_fe_vt;
    sf.phase                      = KEEL_PHASE_READY;
    sf.mode                       = KEEL_TIER_POOL;
    sf.session_max_buffered_bytes = 0;   /* unlimited */

    uint8_t buf[5];
    build_query_header(buf, frame_total);

    /* The engine should NOT return error for size (limit = 0).
     * With a valid server_fd and QUERY action, it'll try to send the 5-byte
     * prefix and set fe_fwd_remaining for the jumbo tail. */
    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, buf, sizeof(buf));

    /* Not KEEL_FLOW_ERROR from the limit check; may be WAIT_BACKEND or OK */
    TEST_ASSERT(r != KEEL_FLOW_ERROR);
    /* The jumbo remaining should be set (jumbo streaming started) */
    TEST_ASSERT_EQ(sf.fe_fwd_remaining, frame_total - sizeof(buf));

    close_pair(sv);
    TEST_END();
}

/* ============================================================================
 * §2  FE message limit — boundary conditions
 * ============================================================================ */

static void test_limit_boundary_at_limit_passes(void)
{
    TEST_BEGIN("oversized_msg: message exactly at limit passes through");

    /* Limit = 100, message declares exactly 100 bytes — should pass */
    const size_t limit       = 100;
    const size_t frame_total = 100;   /* at the limit: should pass */

    int sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker    = &worker;
    session.server_fd = sv[0];
    session.client_fd = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow                       = &s_fake_fe_vt;
    sf.phase                      = KEEL_PHASE_READY;
    sf.mode                       = KEEL_TIER_POOL;
    sf.session_max_buffered_bytes = limit;

    uint8_t buf[5];
    build_query_header(buf, frame_total);

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, buf, sizeof(buf));
    /* At the limit: should NOT be rejected by the size check */
    TEST_ASSERT(r != KEEL_FLOW_ERROR);

    close_pair(sv);
    TEST_END();
}

static void test_limit_boundary_one_over_limit_blocked(void)
{
    TEST_BEGIN("oversized_msg: message one byte over limit is blocked");

    const size_t limit       = 100;
    const size_t frame_total = 101;   /* one over — must be blocked */

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker    = &worker;
    session.server_fd = -1;
    session.client_fd = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow                       = &s_fake_fe_vt;
    sf.phase                      = KEEL_PHASE_READY;
    sf.mode                       = KEEL_TIER_POOL;
    sf.session_max_buffered_bytes = limit;

    uint8_t buf[5];
    build_query_header(buf, frame_total);

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, buf, sizeof(buf));
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);

    TEST_END();
}

static void test_limit_minimum_config_value_enforced(void)
{
    TEST_BEGIN("oversized_msg: minimum config value (4096) blocks messages above it");

    const size_t limit       = 4096;
    const size_t frame_total = 4097;   /* one over minimum */

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker    = &worker;
    session.server_fd = -1;
    session.client_fd = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow                       = &s_fake_fe_vt;
    sf.phase                      = KEEL_PHASE_READY;
    sf.mode                       = KEEL_TIER_POOL;
    sf.session_max_buffered_bytes = limit;

    uint8_t buf[5];
    build_query_header(buf, frame_total);

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, buf, sizeof(buf));
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);

    TEST_END();
}

/* ============================================================================
 * §3  FE message limit — small normal message still passes
 * ============================================================================ */

static void test_small_message_passes_within_limit(void)
{
    TEST_BEGIN("oversized_msg: small normal message passes within limit");

    /* A 20-byte message with a 1 MiB limit should always pass */
    const size_t limit       = 1024 * 1024;
    const size_t frame_total = 20;   /* well under the limit */

    /* Send all 20 bytes (no jumbo) */
    uint8_t buf[20];
    memset(buf, 0, sizeof(buf));
    buf[0] = 'Q';
    wr32(buf + 1, (uint32_t)(sizeof(buf) - 1)); /* length = 19 */
    /* body: query string "x\0" */
    buf[5] = 'x'; buf[6] = '\0';

    int sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker    = &worker;
    session.server_fd = sv[0];
    session.client_fd = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow                       = &s_fake_fe_vt;
    sf.phase                      = KEEL_PHASE_READY;
    sf.mode                       = KEEL_TIER_POOL;
    sf.session_max_buffered_bytes = limit;

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, buf, sizeof(buf));
    /* Small message must NOT be blocked */
    TEST_ASSERT(r != KEEL_FLOW_ERROR);
    /* No jumbo continuation needed */
    TEST_ASSERT_EQ(sf.fe_fwd_remaining, 0u);

    close_pair(sv);
    TEST_END();
}

static void test_oversized_blocks_even_with_valid_socket(void)
{
    TEST_BEGIN("oversized_msg: oversized message blocked even with valid server_fd");

    const size_t limit       = 8192;
    const size_t frame_total = 1024 * 1024;   /* 1 MiB declared */

    /* Use a real socketpair to make server_fd valid */
    int sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker    = &worker;
    session.server_fd = sv[0];
    session.client_fd = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow                       = &s_fake_fe_vt;
    sf.phase                      = KEEL_PHASE_READY;
    sf.mode                       = KEEL_TIER_POOL;
    sf.session_max_buffered_bytes = limit;

    uint8_t buf[5];
    build_query_header(buf, frame_total);

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, buf, sizeof(buf));
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);
    /* fe_fwd_remaining must NOT be set (message was rejected) */
    TEST_ASSERT_EQ(sf.fe_fwd_remaining, 0u);

    close_pair(sv);
    TEST_END();
}

/* ============================================================================
 * §4  FE message limit — field propagation through session_flow
 * ============================================================================ */

static void test_limit_propagated_from_worker(void)
{
    TEST_BEGIN("oversized_msg: session_max_buffered_bytes propagated from worker to sf");

    const size_t limit = 16384;

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));
    worker.session_max_buffered_bytes = limit;
    worker.backend_max_replay_bytes   = limit * 2;

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker    = &worker;
    session.server_fd = -1;
    session.client_fd = -1;

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));

    /* engine_flow_init copies worker fields into sf */
    extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
    int rc = keel_engine_flow_init(&sf, &keel_proto_flow_postgres, &session);
    TEST_ASSERT_EQ(rc, 0);

    TEST_ASSERT_EQ(sf.session_max_buffered_bytes, limit);
    TEST_ASSERT_EQ(sf.backend_max_replay_bytes, limit * 2);

    keel_engine_flow_cleanup(&sf, &session);
    TEST_END();
}

static void test_limit_zero_when_no_worker(void)
{
    TEST_BEGIN("oversized_msg: session_max_buffered_bytes defaults to 0 (unlimited) with no worker");

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));

    /* init without a session/worker */
    extern const keel_proto_flow_vtable_t keel_proto_flow_postgres;
    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker = NULL;

    int rc = keel_engine_flow_init(&sf, &keel_proto_flow_postgres, &session);
    TEST_ASSERT_EQ(rc, 0);

    TEST_ASSERT_EQ(sf.session_max_buffered_bytes, 0u);
    TEST_ASSERT_EQ(sf.backend_max_replay_bytes, 0u);

    keel_engine_flow_cleanup(&sf, &session);
    TEST_END();
}

/* ============================================================================
 * §5  PS replay limit — backend_max_replay_bytes block and allow
 * ============================================================================ */

/* Build a minimal PG Parse message (P) for a named statement */
static size_t build_parse_msg(uint8_t* buf, size_t bufsz,
                               const char* name, const char* query)
{
    size_t nlen   = strlen(name) + 1;
    size_t qlen   = strlen(query) + 1;
    /* Parse: 'P' + int32(len) + name_cstr + query_cstr + int16(0) */
    size_t body   = nlen + qlen + 2;
    size_t total  = 1 + 4 + body;
    if (total > bufsz)
        return 0;
    buf[0] = 'P';
    wr32(buf + 1, (uint32_t)(4 + body));
    size_t pos = 5;
    memcpy(buf + pos, name, nlen);   pos += nlen;
    memcpy(buf + pos, query, qlen);  pos += qlen;
    buf[pos]   = 0; buf[pos+1] = 0;  /* int16 param count = 0 */
    return total;
}

static void test_replay_limit_blocks_oversized_replay(void)
{
    TEST_BEGIN("oversized_msg: backend_max_replay_bytes blocks oversized PS replay");

    /* Build a Parse replay buffer larger than the allowed limit */
    const size_t replay_limit = 64;   /* very small limit */

    /* Build a Parse message for "s1" with a long query — clearly > 64 bytes */
    uint8_t parse_buf[256];
    size_t parse_len = build_parse_msg(parse_buf, sizeof(parse_buf),
                                       "s1",
                                       "SELECT * FROM very_long_table_name_for_testing_t1 "
                                       "WHERE id = $1");
    TEST_ASSERT(parse_len > replay_limit);

    /* Set up the global replay state (returned by fake_get_stmt_replay) */
    s_replay_buf   = parse_buf;
    s_replay_len   = parse_len;
    s_replay_count = 1;
    s_replay_hash  = 0xdeadbeef12345678ULL;

    int sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker    = &worker;
    session.server_fd = sv[0];
    session.client_fd = sv[1];

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow                     = &s_fake_replay_vt;
    sf.phase                    = KEEL_PHASE_READY;
    sf.mode                     = KEEL_TIER_SMART;   /* replay requires SMART+ */
    sf.ps_mode                  = KEEL_PS_MODE_VIRTUALIZE;
    sf.backend_max_replay_bytes = replay_limit;
    /* Signal that this session has prepared statements (hash != 0) */
    sf.stmt_replay_hash         = s_replay_hash;
    /* Simulate a backend where hash mismatch will trigger replay */
    sf.pins                     |= KEEL_FPIN_PREPARED_STMT;

    /* Build a Bind message that will trigger the replay path */
    uint8_t bind_buf[32];
    bind_buf[0] = 'B';
    wr32(bind_buf + 1, 20);    /* length */
    memset(bind_buf + 5, 0, 20); /* empty bind body */

    /* Also set stmt_replay_needs_cleanup = false so we go directly to replay */
    sf.stmt_replay_needs_cleanup = false;

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, bind_buf, 25);
    /* The replay buffer exceeds the limit — engine must return error */
    TEST_ASSERT_EQ(r, KEEL_FLOW_ERROR);

    /* Clean up */
    s_replay_buf = NULL; s_replay_len = 0; s_replay_count = 0;
    close_pair(sv);
    TEST_END();
}

static void test_replay_limit_zero_allows_large_replay(void)
{
    TEST_BEGIN("oversized_msg: backend_max_replay_bytes=0 allows large PS replay");

    /* Build a Parse replay buffer */
    uint8_t parse_buf[256];
    size_t parse_len = build_parse_msg(parse_buf, sizeof(parse_buf),
                                       "s1", "SELECT 1");

    s_replay_buf   = parse_buf;
    s_replay_len   = parse_len;
    s_replay_count = 1;
    s_replay_hash  = 0xdeadbeef12345678ULL;

    int sv[2] = { -1, -1 };
    TEST_ASSERT(make_socketpair(sv) == 0);

    keel_worker_t worker;
    memset(&worker, 0, sizeof(worker));

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    session.worker    = &worker;
    session.server_fd = sv[0];
    session.client_fd = sv[1];

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));
    sf.flow                     = &s_fake_replay_vt;
    sf.phase                    = KEEL_PHASE_READY;
    sf.mode                     = KEEL_TIER_SMART;
    sf.ps_mode                  = KEEL_PS_MODE_VIRTUALIZE;
    sf.backend_max_replay_bytes = 0;      /* unlimited */
    sf.stmt_replay_hash         = s_replay_hash;
    sf.pins                     |= KEEL_FPIN_PREPARED_STMT;
    sf.stmt_replay_needs_cleanup = false;

    uint8_t bind_buf[32];
    bind_buf[0] = 'B';
    wr32(bind_buf + 1, 20);
    memset(bind_buf + 5, 0, 20);

    keel_flow_result_t r = keel_engine_flow_on_fe_data(&sf, &session, bind_buf, 25);
    /* With limit=0, the size check is skipped; replay should start */
    TEST_ASSERT(r != KEEL_FLOW_ERROR || sf.stmt_replay_len > 0);

    /* Clean up */
    s_replay_buf = NULL; s_replay_len = 0; s_replay_count = 0;
    keel_engine_flow_cleanup(&sf, &session);
    close_pair(sv);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    /* §1 — basic block and allow */
    test_limit_blocks_oversized_fe_message();
    test_limit_zero_allows_large_fe_message();

    /* §2 — boundary conditions */
    test_limit_boundary_at_limit_passes();
    test_limit_boundary_one_over_limit_blocked();
    test_limit_minimum_config_value_enforced();

    /* §3 — small message passes, oversized blocked with valid socket */
    test_small_message_passes_within_limit();
    test_oversized_blocks_even_with_valid_socket();

    /* §4 — field propagation */
    test_limit_propagated_from_worker();
    test_limit_zero_when_no_worker();

    /* §5 — PS replay limit */
    test_replay_limit_blocks_oversized_replay();
    test_replay_limit_zero_allows_large_replay();

    printf("\noversized_msg: %d/%d tests passed, %d failed\n",
           test_pass_count, test_pass_count + test_fail_count,
           test_fail_count);
    return test_fail_count > 0 ? 1 : 0;
}
