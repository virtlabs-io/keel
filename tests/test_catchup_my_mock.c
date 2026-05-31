/**
 * @file test_catchup_my_mock.c
 * @brief Socketpair-mock end-to-end test of the MySQL catch-up probe
 *        state machine (Phase 2c).
 *
 * Mirror of test_catchup_pg_mock.c for MySQL. Drives a real
 * `keel_catchup_manager_t` + `keel_reactor_t` through one full MySQL
 * probe round (QUERY_SEND → QUERY_RECV → release) over a connected
 * `socketpair(AF_UNIX)`, bypassing the real CONNECT + handshake via
 * the test-only hook `keel_catchup_my_test_inject_ready`.
 *
 * On one end of the socketpair sits the production probe SM. On the
 * other end the test impersonates a MySQL backend: it reads the
 * COM_QUERY packet the SM produced (asserting framing + SQL), then
 * writes a synthetic 1-column result set (col-count, col-def, EOF,
 * row, EOF). The SM parses the row's '0'/'1' value, calls
 * `keel_catchup_release_satisfied`, and parked waiters fire.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"

#include "../src/worker/worker_catchup_internal.h"
#include "../src/worker/worker_catchup_my_helpers.h"

#include "keel/engine/catchup.h"
#include "keel/engine/worker.h"
#include "keel/plugin/plugin_types.h"
#include "keel/reactor/reactor.h"
#include "keel/util/util.h"

#include <errno.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int dummy_session_sentinel;

typedef struct {
    keel_catchup_outcome_t outcome;
    int                    call_count;
} resume_capture_t;

static void capture_cb(struct keel_session* s,
                       keel_catchup_outcome_t outcome,
                       void* userdata)
{
    (void)s;
    resume_capture_t* cap = (resume_capture_t*)userdata;
    cap->outcome = outcome;
    cap->call_count++;
}

static keel_consistency_token_t make_token(const char* v, uint32_t tl)
{
    keel_consistency_token_t t = {0};
    if (v) strncpy(t.value, v, sizeof t.value - 1);
    t.timeline_id = tl;
    return t;
}

static size_t write_pkt(uint8_t* out, uint8_t seq,
                        const uint8_t* body, size_t body_len)
{
    out[0] = (uint8_t)(body_len      );
    out[1] = (uint8_t)(body_len >>  8);
    out[2] = (uint8_t)(body_len >> 16);
    out[3] = seq;
    if (body && body_len) memcpy(out + 4, body, body_len);
    return 4 + body_len;
}

static size_t make_resultset(uint8_t* out, char value)
{
    size_t off = 0;
    uint8_t cc = 0x01;
    off += write_pkt(out + off, 1, &cc, 1);
    uint8_t col_def[16] = "def\0\0\0\0c\0\0\0\0\0\0\0\0";
    off += write_pkt(out + off, 2, col_def, sizeof col_def);
    uint8_t eof1[5] = {0xFE, 0, 0, 2, 0};
    off += write_pkt(out + off, 3, eof1, sizeof eof1);
    uint8_t row[2] = {0x01, (uint8_t)value};
    off += write_pkt(out + off, 4, row, sizeof row);
    uint8_t eof2[5] = {0xFE, 0, 0, 2, 0};
    off += write_pkt(out + off, 5, eof2, sizeof eof2);
    return off;
}

static int reactor_tick(keel_reactor_t* r, int timeout_ms)
{
    keel_reactor_submit(r);
    int n = keel_reactor_wait(r, timeout_ms);
    if (n <= 0) return n;
    return keel_reactor_process(r);
}

/* ==========================================================================
 * Test 1: happy path — probe reports REACHED, waiter released
 * ==========================================================================*/
static void test_mock_reached_releases_waiter(void)
{
    TEST_BEGIN("my probe mock: '0' row releases parked waiter");

    keel_reactor_t* r = keel_reactor_create(NULL);
    TEST_ASSERT_NOT_NULL(r);

    keel_worker_t w = {0};
    w.reactor = r;

    keel_catchup_manager_t* m = keel_catchup_manager_create(&w, NULL);
    TEST_ASSERT_NOT_NULL(m);

    int sp[2];
    int rc = socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sp);
    TEST_ASSERT_EQ(rc, 0);

    TEST_ASSERT_EQ(keel_catchup_my_test_inject_ready(m, 0, sp[0]), 0);

    const char* gtid = "11111111-2222-3333-4444-555555555555:1-42";
    resume_capture_t cap = {0};
    keel_consistency_token_t tok = make_token(gtid, 1);
    TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &tok,
        60000, capture_cb, &cap));

    keel_catchup_my_drive(m, 0, (uint64_t)keel_time_now());

    /* Flush send. */
    for (int i = 0; i < 5; i++) reactor_tick(r, 10);

    /* Read COM_QUERY from peer end. */
    uint8_t qbuf[1024];
    ssize_t got = 0;
    for (int i = 0; i < 50 && got == 0; i++) {
        got = recv(sp[1], qbuf, sizeof qbuf, MSG_DONTWAIT);
        if (got < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            reactor_tick(r, 10);
            got = 0;
            continue;
        }
        break;
    }
    TEST_ASSERT(got > 5);
    /* Header: 3-byte LE length + 1-byte seq_id (0) + COM_QUERY (0x03). */
    uint32_t payload_len = (uint32_t)qbuf[0]
                         | ((uint32_t)qbuf[1] << 8)
                         | ((uint32_t)qbuf[2] << 16);
    TEST_ASSERT_EQ((size_t)(4 + payload_len), (size_t)got);
    TEST_ASSERT_EQ(qbuf[3], (uint8_t)0);
    TEST_ASSERT_EQ(qbuf[4], (uint8_t)0x03);
    char sql[1024];
    size_t sql_len = payload_len - 1;
    memcpy(sql, qbuf + 5, sql_len);
    sql[sql_len] = '\0';
    TEST_ASSERT(strstr(sql, "WAIT_FOR_EXECUTED_GTID_SET") != NULL);
    TEST_ASSERT(strstr(sql, gtid) != NULL);

    /* Write a positive result set (row value '0' = reached). */
    uint8_t reply[256];
    size_t reply_len = make_resultset(reply, '0');
    TEST_ASSERT_EQ(send(sp[1], reply, reply_len, 0), (ssize_t)reply_len);

    /* Drive the reactor until the waiter has fired. */
    bool done = false;
    for (int i = 0; i < 50 && !done; i++) {
        reactor_tick(r, 10);
        done = cap.call_count > 0;
    }
    TEST_ASSERT(done);
    TEST_ASSERT_EQ(cap.outcome, KEEL_CATCHUP_REACHED);
    TEST_ASSERT_EQ(cap.call_count, 1);

    /* Probe-result cache should serve subsequent enqueue inline. */
    resume_capture_t fast = {0};
    keel_consistency_token_t same = make_token(gtid, 1);
    keel_catchup_waiter_t* w2 = keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &same,
        60000, capture_cb, &fast);
    TEST_ASSERT_NULL(w2);
    TEST_ASSERT_EQ(fast.call_count, 1);
    TEST_ASSERT_EQ(fast.outcome, KEEL_CATCHUP_REACHED);

    keel_catchup_stats_snapshot_t snap;
    keel_catchup_manager_snapshot(m, &snap);
    TEST_ASSERT_EQ(snap.probes_succeeded_total, (uint64_t)1);
    TEST_ASSERT(snap.cache_hits_total >= 1);

    keel_catchup_manager_destroy(m);
    close(sp[1]);  /* NOLINT(keel-syscall) */
    keel_reactor_destroy(r);
    TEST_END();
}

/* ==========================================================================
 * Test 2: negative reply ('1') keeps waiter parked, no backoff
 * ==========================================================================*/
static void test_mock_not_reached_keeps_waiter(void)
{
    TEST_BEGIN("my probe mock: '1' row leaves waiter parked, no backoff");

    keel_reactor_t* r = keel_reactor_create(NULL);
    TEST_ASSERT_NOT_NULL(r);
    keel_worker_t w = {0};
    w.reactor = r;
    keel_catchup_manager_t* m = keel_catchup_manager_create(&w, NULL);
    TEST_ASSERT_NOT_NULL(m);

    int sp[2];
    TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sp), 0);
    TEST_ASSERT_EQ(keel_catchup_my_test_inject_ready(m, 0, sp[0]), 0);

    resume_capture_t cap = {0};
    keel_consistency_token_t tok = make_token("aabb:1-99", 1);
    TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &tok,
        60000, capture_cb, &cap));

    keel_catchup_my_drive(m, 0, (uint64_t)keel_time_now());

    uint8_t qbuf[512];
    ssize_t got = 0;
    for (int i = 0; i < 50 && got == 0; i++) {
        reactor_tick(r, 10);
        got = recv(sp[1], qbuf, sizeof qbuf, MSG_DONTWAIT);
        if (got < 0) got = 0;
    }
    TEST_ASSERT(got > 5);

    /* Reply with '1' = timeout / not reached. */
    uint8_t reply[256];
    size_t reply_len = make_resultset(reply, '1');
    TEST_ASSERT_EQ(send(sp[1], reply, reply_len, 0), (ssize_t)reply_len);

    for (int i = 0; i < 30; i++) reactor_tick(r, 5);

    TEST_ASSERT_EQ(cap.call_count, 0);

    keel_catchup_stats_snapshot_t snap;
    keel_catchup_manager_snapshot(m, &snap);
    TEST_ASSERT_EQ(snap.probes_succeeded_total, (uint64_t)1);
    TEST_ASSERT_EQ(snap.probes_failed_total, (uint64_t)0);
    TEST_ASSERT_EQ(snap.waiters_active, (size_t)1);

    keel_catchup_manager_destroy(m);
    close(sp[1]);  /* NOLINT(keel-syscall) */
    keel_reactor_destroy(r);
    TEST_END();
}

/* ==========================================================================
 * Test 3: ERR packet closes socket and applies backoff
 * ==========================================================================*/
static void test_mock_err_packet_triggers_backoff(void)
{
    TEST_BEGIN("my probe mock: ERR packet counts as failure + backoff");

    keel_reactor_t* r = keel_reactor_create(NULL);
    TEST_ASSERT_NOT_NULL(r);
    keel_worker_t w = {0};
    w.reactor = r;
    keel_catchup_manager_t* m = keel_catchup_manager_create(&w, NULL);
    TEST_ASSERT_NOT_NULL(m);

    int sp[2];
    TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sp), 0);
    TEST_ASSERT_EQ(keel_catchup_my_test_inject_ready(m, 0, sp[0]), 0);

    resume_capture_t cap = {0};
    keel_consistency_token_t tok = make_token("aabb:1-7", 1);
    TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &tok,
        60000, capture_cb, &cap));

    keel_catchup_my_drive(m, 0, (uint64_t)keel_time_now());

    uint8_t qbuf[512];
    ssize_t got = 0;
    for (int i = 0; i < 50 && got == 0; i++) {
        reactor_tick(r, 10);
        got = recv(sp[1], qbuf, sizeof qbuf, MSG_DONTWAIT);
        if (got < 0) got = 0;
    }
    TEST_ASSERT(got > 5);

    /* Send a single ERR packet (header 0xFF, sqlstate, message). */
    uint8_t err_body[16] = {0xFF, 0x34, 0x12, '#', 'H', 'Y', '0', '0', '0',
                            'o', 'o', 'p', 's'};
    uint8_t err[32];
    size_t  elen = write_pkt(err, 1, err_body, sizeof err_body);
    TEST_ASSERT_EQ(send(sp[1], err, elen, 0), (ssize_t)elen);

    for (int i = 0; i < 30; i++) reactor_tick(r, 5);

    TEST_ASSERT_EQ(cap.call_count, 0);
    keel_catchup_stats_snapshot_t snap;
    keel_catchup_manager_snapshot(m, &snap);
    TEST_ASSERT_EQ(snap.probes_succeeded_total, (uint64_t)0);
    TEST_ASSERT(snap.probes_failed_total >= 1);
    TEST_ASSERT_EQ(snap.waiters_active, (size_t)1);

    keel_catchup_manager_destroy(m);
    close(sp[1]);  /* NOLINT(keel-syscall) */
    keel_reactor_destroy(r);
    TEST_END();
}

int main(void)
{
    test_mock_reached_releases_waiter();
    test_mock_not_reached_keeps_waiter();
    test_mock_err_packet_triggers_backoff();
    return test_summary();
}
