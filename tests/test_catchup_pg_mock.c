/**
 * @file test_catchup_pg_mock.c
 * @brief Socketpair-mock end-to-end test of the PostgreSQL catch-up
 *        probe state machine (Phase 2b-test, part 3).
 *
 * Drives a real `keel_catchup_manager_t` + `keel_reactor_t` through one
 * full PG probe round (QUERY_SEND → QUERY_RECV → release) over a
 * connected `socketpair(AF_UNIX)`, bypassing the real CONNECT + SCRAM
 * auth via the test-only hook `keel_catchup_pg_test_inject_ready`.
 *
 * On one end of the socketpair sits the production probe SM driving
 * `keel_reactor_send`/`keel_reactor_recv`. On the other end the test
 * impersonates a PostgreSQL backend: it reads the 'Q' Query message
 * the SM produced (asserting framing + SQL), then writes a synthetic
 * `T` + `D` + `C` + `Z` reply. The SM parses the reply, calls
 * `keel_catchup_release_satisfied`, and three parked waiters fire
 * with `KEEL_CATCHUP_REACHED`.
 *
 * The full TCP + SCRAM-SHA-256 + TLS handshake is owned by
 * `backend_async_start` (already covered by its own test suite); this
 * file proves the catch-up-specific layer on top of it works end-to-end.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"

#include "../src/worker/worker_catchup_internal.h"
#include "../src/worker/worker_catchup_pg_helpers.h"

#include "keel/engine/catchup.h"
#include "keel/engine/worker.h"
#include "keel/plugin/plugin_types.h"
#include "keel/reactor/reactor.h"
#include "keel/util/util.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* Sentinel — the manager never dereferences the session pointer. */
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

static size_t make_msg(uint8_t* out, uint8_t type,
                       const uint8_t* body, size_t body_len)
{
    out[0] = type;
    uint32_t len_field = (uint32_t)(body_len + 4);
    out[1] = (uint8_t)(len_field >> 24);
    out[2] = (uint8_t)(len_field >> 16);
    out[3] = (uint8_t)(len_field >>  8);
    out[4] = (uint8_t)(len_field      );
    if (body && body_len) memcpy(out + 5, body, body_len);
    return 1 + 4 + body_len;
}

static size_t make_datarow_bool(uint8_t* out, char value)
{
    uint8_t body[2 + 4 + 1];
    body[0] = 0; body[1] = 1;
    body[2] = 0; body[3] = 0; body[4] = 0; body[5] = 1;
    body[6] = (uint8_t)value;
    return make_msg(out, 'D', body, sizeof body);
}

/** Tick the reactor once with a small timeout. */
static int reactor_tick(keel_reactor_t* r, int timeout_ms)
{
    keel_reactor_submit(r);
    int n = keel_reactor_wait(r, timeout_ms);
    if (n <= 0) return n;
    return keel_reactor_process(r);
}

/** Drive the reactor until either @p done becomes true or @p budget_ticks
 *  reactor ticks have elapsed. */
static bool drive_reactor_until(keel_reactor_t* r, bool* done, int budget_ticks)
{
    for (int i = 0; i < budget_ticks; i++) {
        reactor_tick(r, 20);
        if (*done) return true;
    }
    return *done;
}

/* ==========================================================================
 * Test 1: happy path — probe reports REACHED, three waiters released
 * ==========================================================================*/
static void test_mock_reached_releases_waiters(void)
{
    TEST_BEGIN("pg probe mock: T+D(t)+C+Z releases all eligible waiters");

    keel_reactor_t* r = keel_reactor_create(NULL);
    TEST_ASSERT_NOT_NULL(r);

    /* Minimal worker shim — the catchup manager only touches `reactor`
     * on this code path. server_pools stays NULL because the test
     * inject hook bypasses the CONNECT path that would dereference it. */
    keel_worker_t w = {0};
    w.reactor = r;

    keel_catchup_manager_t* m = keel_catchup_manager_create(&w, NULL);
    TEST_ASSERT_NOT_NULL(m);

    int sp[2];
    int rc = socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sp);
    TEST_ASSERT_EQ(rc, 0);

    TEST_ASSERT_EQ(keel_catchup_pg_test_inject_ready(m, /*server*/ 0, sp[0]), 0);

    /* Park three waiters at increasing LSNs — the SM will pick the
     * highest (0/300) to probe; a positive reply releases all three. */
    resume_capture_t caps[3] = {0};
    const char* lsns[3] = { "0/100", "0/200", "0/300" };
    for (int i = 0; i < 3; i++) {
        keel_consistency_token_t tok = make_token(lsns[i], 1);
        TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
            m, (struct keel_session*)&dummy_session_sentinel, 0, &tok,
            /*max_wait_ms*/ 60000, capture_cb, &caps[i]));
    }

    /* Kick the SM: state==READY → pick highest token → encode 'Q' →
     * keel_reactor_send. */
    keel_catchup_pg_drive(m, 0, (uint64_t)keel_time_now());

    /* Tick the reactor a few times to flush the send completion. */
    for (int i = 0; i < 5; i++) reactor_tick(r, 10);

    /* Read the 'Q' message off the peer end and assert framing + payload. */
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
    TEST_ASSERT_EQ(qbuf[0], (uint8_t)'Q');
    /* The SQL string starts at offset 5 and contains the highest LSN. */
    const char* sql = (const char*)(qbuf + 5);
    TEST_ASSERT(strstr(sql, "'0/300'::pg_lsn") != NULL);
    TEST_ASSERT(strstr(sql, "pg_last_wal_replay_lsn() IS NULL") != NULL);

    /* Write the synthetic reply: T (RowDescription) + D ('t') + C + Z. */
    uint8_t reply[512];
    size_t  off = 0;
    uint8_t T_body[7] = {0,1, 0,0,0,0, 0};   /* minimal RowDescription stub */
    off += make_msg(reply + off, 'T', T_body, sizeof T_body);
    off += make_datarow_bool(reply + off, 't');
    off += make_msg(reply + off, 'C', (const uint8_t*)"SELECT 1", 9);
    off += make_msg(reply + off, 'Z', (const uint8_t*)"I", 1);
    ssize_t sent = send(sp[1], reply, off, 0);
    TEST_ASSERT_EQ(sent, (ssize_t)off);

    /* Drive the reactor until all three waiters have fired. */
    bool done = false;
    for (int i = 0; i < 50 && !done; i++) {
        reactor_tick(r, 10);
        done = caps[0].call_count > 0 &&
               caps[1].call_count > 0 &&
               caps[2].call_count > 0;
    }
    TEST_ASSERT(done);
    TEST_ASSERT_EQ(caps[0].outcome, KEEL_CATCHUP_REACHED);
    TEST_ASSERT_EQ(caps[1].outcome, KEEL_CATCHUP_REACHED);
    TEST_ASSERT_EQ(caps[2].outcome, KEEL_CATCHUP_REACHED);
    TEST_ASSERT_EQ(caps[0].call_count, 1);
    TEST_ASSERT_EQ(caps[1].call_count, 1);
    TEST_ASSERT_EQ(caps[2].call_count, 1);

    /* The probe-result cache should have a fresh hit for this token. */
    keel_consistency_token_t same = make_token("0/300", 1);
    resume_capture_t fast = {0};
    keel_catchup_waiter_t* w2 = keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &same,
        60000, capture_cb, &fast);
    TEST_ASSERT_NULL(w2);              /* inline cache hit, no park */
    TEST_ASSERT_EQ(fast.call_count, 1);
    TEST_ASSERT_EQ(fast.outcome, KEEL_CATCHUP_REACHED);

    keel_catchup_stats_snapshot_t snap;
    keel_catchup_manager_snapshot(m, &snap);
    TEST_ASSERT_EQ(snap.probes_succeeded_total, (uint64_t)1);
    TEST_ASSERT(snap.cache_hits_total >= 1);

    keel_catchup_manager_destroy(m);
    /* sp[0] was owned by the manager and closed by destroy; sp[1] is ours. */
    close(sp[1]);  /* NOLINT(keel-syscall) */
    keel_reactor_destroy(r);
    TEST_END();
    (void)drive_reactor_until;  /* silence unused-fn warning if any */
}

/* ==========================================================================
 * Test 2: negative reply ('f') keeps waiters parked, no release, no backoff
 * ==========================================================================*/
static void test_mock_not_reached_keeps_waiters(void)
{
    TEST_BEGIN("pg probe mock: D(f) leaves waiters parked, no backoff");

    keel_reactor_t* r = keel_reactor_create(NULL);
    TEST_ASSERT_NOT_NULL(r);
    keel_worker_t w = {0};
    w.reactor = r;
    keel_catchup_manager_t* m = keel_catchup_manager_create(&w, NULL);
    TEST_ASSERT_NOT_NULL(m);

    int sp[2];
    TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sp), 0);
    TEST_ASSERT_EQ(keel_catchup_pg_test_inject_ready(m, 0, sp[0]), 0);

    resume_capture_t cap = {0};
    keel_consistency_token_t tok = make_token("0/500", 1);
    TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &tok,
        60000, capture_cb, &cap));

    keel_catchup_pg_drive(m, 0, (uint64_t)keel_time_now());

    /* Wait for the 'Q' to arrive on the peer end. */
    uint8_t qbuf[512];
    ssize_t got = 0;
    for (int i = 0; i < 50 && got == 0; i++) {
        reactor_tick(r, 10);
        got = recv(sp[1], qbuf, sizeof qbuf, MSG_DONTWAIT);
        if (got < 0) got = 0;
    }
    TEST_ASSERT(got > 5);

    /* Reply: not-reached. */
    uint8_t reply[512]; size_t off = 0;
    uint8_t T_body[7] = {0,1, 0,0,0,0, 0};
    off += make_msg(reply + off, 'T', T_body, sizeof T_body);
    off += make_datarow_bool(reply + off, 'f');
    off += make_msg(reply + off, 'C', (const uint8_t*)"SELECT 1", 9);
    off += make_msg(reply + off, 'Z', (const uint8_t*)"I", 1);
    TEST_ASSERT_EQ(send(sp[1], reply, off, 0), (ssize_t)off);

    /* Drain probe round. */
    for (int i = 0; i < 30; i++) reactor_tick(r, 5);

    /* Waiter is still parked — the probe answered 'f', no release. */
    TEST_ASSERT_EQ(cap.call_count, 0);

    keel_catchup_stats_snapshot_t snap;
    keel_catchup_manager_snapshot(m, &snap);
    TEST_ASSERT_EQ(snap.probes_succeeded_total, (uint64_t)1);
    /* A clean negative answer is NOT a failure — no backoff. */
    TEST_ASSERT_EQ(snap.probes_failed_total, (uint64_t)0);
    TEST_ASSERT_EQ(snap.waiters_active, (size_t)1);

    keel_catchup_manager_destroy(m);
    close(sp[1]);  /* NOLINT(keel-syscall) */
    keel_reactor_destroy(r);
    TEST_END();
}

/* ==========================================================================
 * Test 3: ErrorResponse closes socket and applies backoff
 * ==========================================================================*/
static void test_mock_error_response_triggers_backoff(void)
{
    TEST_BEGIN("pg probe mock: ErrorResponse counts as failure + backoff");

    keel_reactor_t* r = keel_reactor_create(NULL);
    TEST_ASSERT_NOT_NULL(r);
    keel_worker_t w = {0};
    w.reactor = r;
    keel_catchup_manager_t* m = keel_catchup_manager_create(&w, NULL);
    TEST_ASSERT_NOT_NULL(m);

    int sp[2];
    TEST_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sp), 0);
    TEST_ASSERT_EQ(keel_catchup_pg_test_inject_ready(m, 0, sp[0]), 0);

    resume_capture_t cap = {0};
    keel_consistency_token_t tok = make_token("0/700", 1);
    TEST_ASSERT_NOT_NULL(keel_catchup_enqueue(
        m, (struct keel_session*)&dummy_session_sentinel, 0, &tok,
        60000, capture_cb, &cap));

    keel_catchup_pg_drive(m, 0, (uint64_t)keel_time_now());

    /* Drain 'Q' off the peer. */
    uint8_t qbuf[512];
    ssize_t got = 0;
    for (int i = 0; i < 50 && got == 0; i++) {
        reactor_tick(r, 10);
        got = recv(sp[1], qbuf, sizeof qbuf, MSG_DONTWAIT);
        if (got < 0) got = 0;
    }
    TEST_ASSERT(got > 5);

    /* Write an ErrorResponse — the SM must treat as transport failure
     * and apply backoff (waiter stays parked, no release). */
    uint8_t e_body[16] = "SFATAL\0Coops\0\0";
    uint8_t err[64];
    size_t  elen = make_msg(err, 'E', e_body, sizeof e_body);
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
    test_mock_reached_releases_waiters();
    test_mock_not_reached_keeps_waiters();
    test_mock_error_response_triggers_backoff();
    return test_summary();
}
