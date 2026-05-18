/**
 * @file test_pool_correctness.c
 * @brief White-box regression tests for backend-pool lifecycle invariants.
 *
 * This suite targets the parts of pool management that are easy to regress when
 * optimizing for throughput: deciding whether a returned connection is clean,
 * accounting for pinned versus reusable slots, and ensuring cleanup generations
 * advance monotonically so stale observations cannot be mistaken for current
 * state.
 *
 * The tests are deliberately narrow and structural. They do not talk to a real
 * PostgreSQL server and they do not attempt to validate the full wire cleanup
 * conversation. Instead they manufacture backend_conn_t objects around local
 * socketpairs and assert that the pool's linked lists, state enums, and quota
 * counters evolve in a way the worker runtime can trust.
 */

#include "test_utils.h"
#include "keel/protocol/protocol_flow.h"  /* KEEL_FPIN_QUARANTINE, KEEL_QE_POTENTIALLY_STATEFUL */
#include "keel/engine/engine_flow.h"    /* keel_session_flow_t */
#include "keel/reactor/reactor.h"
#include "keel/mem/mem.h"

#include <stdatomic.h>
#include <fcntl.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/*
 * This file is intentionally white-box. The public engine API does not expose
 * enough internal pool bookkeeping to assert list membership, cleanup state, or
 * pin counters directly, so the tests include the internal header and inspect
 * the structures the runtime itself relies on.
 */
#include "keel/engine/backend_pool.h"

/* ============================================================================
 * Helpers
 * ============================================================================ */

static int g_wait_order[8];
static int g_wait_count;
static int g_timeout_count;

static void reset_wait_probe(void)
{
    memset(g_wait_order, 0, sizeof(g_wait_order));
    g_wait_count = 0;
    g_timeout_count = 0;
}

static void wait_probe_cb(void* session, void* userdata)
{
    if (!userdata) {
        g_timeout_count++;
        return;
    }
    if (g_wait_count < (int)(sizeof(g_wait_order) / sizeof(g_wait_order[0])))
        g_wait_order[g_wait_count++] = *(int*)session;
}

static int reactor_tick(keel_reactor_t* r, int timeout_ms)
{
    keel_reactor_submit(r);
    int n = keel_reactor_wait(r, timeout_ms);
    if (n <= 0) return n;
    return keel_reactor_process(r);
}

static void make_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/**
 * @brief Assemble a synthetic pool populated with `n` immediately borrowable
 *        connections.
 * @param n Number of slots to create.
 * @param backend_fds [out] Receives the peer fd for each socketpair-backed
 *                          connection.
 * @return Heap-allocated pool fixture.
 *
 * The fixture bypasses real backend connect/auth because the tests care about
 * pool bookkeeping, not wire protocol. `socketpair()` is sufficient to provide
 * valid descriptors so send/recv-based cleanup logic behaves as if a transport
 * exists, while still keeping the tests deterministic and fast.
 */
static backend_pool_t* make_test_pool(size_t n, int backend_fds[])
{
    backend_pool_config_t cfg = {
        .host = "127.0.0.1",
        .port = 5432,
        .user = "test",
        .password = "test",
        .database = "test",
        .protocol = "postgres",
        .min_connections = n,
        .max_connections = n,
        .max_waiting = 4,
    };

    /* Allocate pool manually (bypass actual TCP connect) */
    backend_pool_t* pool = keel_calloc(1, sizeof(backend_pool_t));
    pool->config = cfg;
    pool->flow_vt = keel_proto_flow_get(cfg.protocol);
    TEST_ASSERT_NOT_NULL(pool->flow_vt);
    {
        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
        pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
        pthread_mutex_init(&pool->lock, &attr);
        pthread_mutexattr_destroy(&attr);
    }
    {
        keel_reactor_config_t rcfg = KEEL_REACTOR_CONFIG_DEFAULT;
        rcfg.type = KEEL_REACTOR_EPOLL;
        rcfg.max_fds = 128;
        pool->reactor = keel_reactor_create(&rcfg);
        TEST_ASSERT_NOT_NULL(pool->reactor);
    }
    pool->connections = keel_calloc(n, sizeof(backend_conn_t));
    pool->total_count = n;

    for (size_t i = 0; i < n; i++) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
            /* If socketpair fails, use fd = -1 (test will note it) */
            pool->connections[i].fd = -1;
            backend_fds[i] = -1;
            continue;
        }
        make_nonblocking(sv[0]);
        make_nonblocking(sv[1]);
        pool->connections[i].fd = sv[0];
        backend_fds[i] = sv[1];
        atomic_store(&pool->connections[i].state, BACKEND_CONN_IDLE);
        pool->connections[i].pool = pool;
        pool->connections[i].next = (i > 0) ? NULL : NULL;
    }

    /* Put all connections on clean_list */
    pool->clean_list = NULL;
    for (size_t i = 0; i < n; i++) {
        if (pool->connections[i].fd >= 0) {
            pool->connections[i].next = pool->clean_list;
            pool->clean_list = &pool->connections[i];
            pool->clean_count++;
        }
    }

    return pool;
}

/**
 * @brief Tear down a synthetic pool created by make_test_pool().
 * @param pool Pool fixture to destroy.
 * @param backend_fds Peer descriptors paired with each backend slot.
 * @param n Number of connection slots in the fixture.
 * @return
 */
static void destroy_test_pool(backend_pool_t* pool, int backend_fds[], size_t n)
{
    if (pool->reactor) {
        keel_reactor_destroy(pool->reactor);
        pool->reactor = NULL;
    }
    for (size_t i = 0; i < n; i++) {
        if (pool->connections[i].fd >= 0) close(pool->connections[i].fd);
        if (backend_fds[i] >= 0) close(backend_fds[i]);
    }
    keel_free(pool->connections);
    pthread_mutex_destroy(&pool->lock);
    keel_free(pool);
}

/* ============================================================================
 * Test: CLEANING State Transition
 * ============================================================================ */

static void test_cleaning_state_transition(void)
{
    TEST_BEGIN("CLEANING state transition on return of dirty connection");

    int be_fds[2];
    backend_pool_t* pool = make_test_pool(2, be_fds);

    /* Borrow a connection */
    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_ACTIVE);

    /* Simulate the connection having state (dirty) */
    conn->current_state_hash = 0xBEEF;

    /* Return it — should enter CLEANING since it has state */
    backend_pool_return(pool, conn, false);

    /* The connection is not borrowable: cleanup is now reactor-owned and starts
     * in SEND, waiting for the reactor to flush DISCARD ALL. */
    backend_conn_state_t st = atomic_load(&conn->state);
    TEST_ASSERT_EQ(st, BACKEND_CONN_CLEANING);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_SEND);
    TEST_ASSERT(conn->cleanup_io_armed);
    TEST_ASSERT_EQ(pool->cleaning_count, 1U);

    /* Verify it's not on any list (borrowers can't see it) */
    bool found_on_clean = false;
    for (backend_conn_t* c = pool->clean_list; c; c = c->next) {
        if (c == conn) found_on_clean = true;
    }
    TEST_ASSERT(!found_on_clean);

    destroy_test_pool(pool, be_fds, 2);
    TEST_END();
}

/* ============================================================================
 * Test: Clean Connection Returns Directly to IDLE
 * ============================================================================ */

static void test_clean_return_bypasses_discard(void)
{
    TEST_BEGIN("Clean connection returns directly to IDLE (no DISCARD ALL)");

    int be_fds[2];
    backend_pool_t* pool = make_test_pool(2, be_fds);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    /* Connection has no state → should return directly to clean_list */
    conn->current_state_hash = 0;
    conn->profile = NULL;  /* No profile → clean */

    size_t clean_before = pool->clean_count;
    backend_pool_return(pool, conn, false);

    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_IDLE);
    TEST_ASSERT(pool->clean_count > clean_before);

    destroy_test_pool(pool, be_fds, 2);
    TEST_END();
}

/* ============================================================================
 * Test: clean_gen Bumps on Return
 * ============================================================================ */

static void test_clean_gen_increments(void)
{
    TEST_BEGIN("clean_gen increments on every return");

    int be_fds[2];
    backend_pool_t* pool = make_test_pool(2, be_fds);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    uint64_t gen0 = conn->clean_gen;

    backend_pool_return(pool, conn, false);
    uint64_t gen1 = conn->clean_gen;
    TEST_ASSERT(gen1 > gen0);

    /* Borrow and return again */
    conn = backend_pool_borrow(pool, 0);
    if (conn) {
        backend_pool_return(pool, conn, false);
        TEST_ASSERT(conn->clean_gen > gen1);
    }

    destroy_test_pool(pool, be_fds, 2);
    TEST_END();
}

static void test_backend_can_borrow_predicate_matrix(void)
{
    TEST_BEGIN("backend_can_borrow rejects illegal lifecycle states");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);
    backend_conn_t* conn = &pool->connections[0];

    atomic_store(&conn->state, BACKEND_CONN_IDLE);
    conn->pinned_session = NULL;
    conn->quarantine = BACKEND_QUARANTINE_NONE;
    conn->in_transaction = false;
    conn->syncing = false;
    conn->replay_active = false;
    conn->protocol_desync = false;
    conn->needs_full_cleanup = false;
    TEST_ASSERT(backend_pool_can_borrow(conn));

    atomic_store(&conn->state, BACKEND_CONN_CLEANING);
    TEST_ASSERT(!backend_pool_can_borrow(conn));
    atomic_store(&conn->state, BACKEND_CONN_IDLE);

    conn->pinned_session = (void*)0x1;
    TEST_ASSERT(!backend_pool_can_borrow(conn));
    conn->pinned_session = NULL;

    conn->quarantine = BACKEND_QUARANTINE_DIRTY;
    TEST_ASSERT(!backend_pool_can_borrow(conn));
    conn->quarantine = BACKEND_QUARANTINE_NONE;

    conn->in_transaction = true;
    TEST_ASSERT(!backend_pool_can_borrow(conn));
    conn->in_transaction = false;

    conn->syncing = true;
    TEST_ASSERT(!backend_pool_can_borrow(conn));
    conn->syncing = false;

    conn->replay_active = true;
    TEST_ASSERT(!backend_pool_can_borrow(conn));
    conn->replay_active = false;

    conn->protocol_desync = true;
    TEST_ASSERT(!backend_pool_can_borrow(conn));
    conn->protocol_desync = false;

    conn->needs_full_cleanup = true;
    TEST_ASSERT(!backend_pool_can_borrow(conn));

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

static void test_backend_generation_validation(void)
{
    TEST_BEGIN("backend generation validation rejects stale references");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);
    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    uint64_t g = conn->generation;
    TEST_ASSERT(backend_pool_validate_generation(conn, g));

    backend_pool_close_connection(pool, conn, BACKEND_CLOSE_REASON_IO_ERROR);
    TEST_ASSERT(!backend_pool_validate_generation(conn, g));

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

static void test_close_reason_once(void)
{
    TEST_BEGIN("backend close reason is emitted once per close transition");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);
    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    backend_pool_close_connection(pool, conn, BACKEND_CLOSE_REASON_CLIENT_DISCONNECT);
    TEST_ASSERT_EQ(conn->close_reason, BACKEND_CLOSE_REASON_CLIENT_DISCONNECT);
    uint64_t gen_after_first = conn->generation;

    backend_pool_close_connection(pool, conn, BACKEND_CLOSE_REASON_CLEANUP_ERROR);
    TEST_ASSERT_EQ(conn->close_reason, BACKEND_CLOSE_REASON_CLIENT_DISCONNECT);
    TEST_ASSERT_EQ(conn->generation, gen_after_first);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

/* ============================================================================
 * Test: Admission Control (max_pinned)
 * ============================================================================ */

static void test_admission_control_max_pinned(void)
{
    TEST_BEGIN("admission control: max_pinned limits pinned borrows");

    int be_fds[4];
    backend_pool_t* pool = make_test_pool(4, be_fds);
    pool->max_pinned = 2;  /* Allow only 2 pinned connections */

    /* Fake sessions */
    int session1 = 1, session2 = 2, session3 = 3;

    /* Pin 1 */
    backend_conn_t* c1 = backend_pool_borrow_pinned(pool, &session1);
    TEST_ASSERT_NOT_NULL(c1);
    TEST_ASSERT_EQ(pool->pinned_count, 1);

    /* Pin 2 */
    backend_conn_t* c2 = backend_pool_borrow_pinned(pool, &session2);
    TEST_ASSERT_NOT_NULL(c2);
    TEST_ASSERT_EQ(pool->pinned_count, 2);

    /* Pin 3 — should fail (at max) */
    backend_conn_t* c3 = backend_pool_borrow_pinned(pool, &session3);
    TEST_ASSERT_NULL(c3);
    TEST_ASSERT_EQ(pool->pinned_count, 2);

    /* Return one — should allow new pin */
    backend_pool_return(pool, c1, false);
    TEST_ASSERT_EQ(pool->pinned_count, 1);

    c3 = backend_pool_borrow_pinned(pool, &session3);
    TEST_ASSERT_NOT_NULL(c3);
    TEST_ASSERT_EQ(pool->pinned_count, 2);

    /* Cleanup */
    backend_pool_return(pool, c2, false);
    backend_pool_return(pool, c3, false);

    destroy_test_pool(pool, be_fds, 4);
    TEST_END();
}

/* ============================================================================
 * Test: Quarantine Pin Flag Definitions
 * ============================================================================ */

static void test_quarantine_pin_flags(void)
{
    TEST_BEGIN("quarantine pin flag definitions and bit operations");

    /* Verify QUARANTINE is a distinct bit */
    TEST_ASSERT(KEEL_FPIN_QUARANTINE != 0);
    TEST_ASSERT((KEEL_FPIN_QUARANTINE & KEEL_FPIN_TRANSACTION) == 0);
    TEST_ASSERT((KEEL_FPIN_QUARANTINE & KEEL_FPIN_COPY) == 0);

    /* Verify POTENTIALLY_STATEFUL query effect */
    TEST_ASSERT(KEEL_QE_POTENTIALLY_STATEFUL != 0);

    /* Test quarantine set/clear logic */
    keel_flow_pin_reason_t pins = KEEL_FPIN_NONE;
    pins |= KEEL_FPIN_QUARANTINE;
    TEST_ASSERT(pins != KEEL_FPIN_NONE);
    TEST_ASSERT(pins & KEEL_FPIN_QUARANTINE);

    /* Clearing only quarantine should make pins == NONE */
    pins &= ~(uint32_t)KEEL_FPIN_QUARANTINE;
    TEST_ASSERT_EQ(pins, KEEL_FPIN_NONE);

    /* Multiple pins: quarantine + transaction */
    pins = KEEL_FPIN_TRANSACTION | KEEL_FPIN_QUARANTINE;
    /* Clearing quarantine leaves TRANSACTION */
    pins &= ~(uint32_t)KEEL_FPIN_QUARANTINE;
    TEST_ASSERT(pins & KEEL_FPIN_TRANSACTION);
    TEST_ASSERT(!(pins & KEEL_FPIN_QUARANTINE));

    TEST_END();
}

/* ============================================================================
 * Test: Session Flow Sticky-Primary Fields
 * ============================================================================ */

static void test_sticky_primary_fields(void)
{
    TEST_BEGIN("session flow sticky-primary fields initialization");

    keel_session_flow_t sf;
    memset(&sf, 0, sizeof(sf));

    TEST_ASSERT_EQ(sf.last_write_ns, 0ULL);
    TEST_ASSERT_EQ(sf.sticky_primary_ttl_ms, 0U);
    TEST_ASSERT_EQ(sf.quarantine_pending, 0U);

    /* Simulate write timestamp */
    sf.last_write_ns = 1234567890ULL;
    TEST_ASSERT(sf.last_write_ns > 0);

    /* Simulate TTL */
    sf.sticky_primary_ttl_ms = 100;
    TEST_ASSERT_EQ(sf.sticky_primary_ttl_ms, 100U);

    TEST_END();
}

/* ============================================================================
 * Test: CLEANING State Rejected by Borrow CAS
 * ============================================================================ */

static void test_borrow_rejects_cleaning(void)
{
    TEST_BEGIN("borrow CAS rejects CLEANING connections");

    int be_fds[2];
    backend_pool_t* pool = make_test_pool(2, be_fds);

    /* Borrow both connections */
    backend_conn_t* c1 = backend_pool_borrow(pool, 0);
    backend_conn_t* c2 = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(c1);
    TEST_ASSERT_NOT_NULL(c2);

    /* Manually put c1 into CLEANING state and back on clean_list. This is an
     * intentionally-invalid fixture used to prove borrow's CAS will not take a
     * non-IDLE entry even if list membership is wrong. Do not call pool_return
     * while the invalid fixture is present because production invariant checks
     * should trap on it. */
    atomic_store(&c1->state, BACKEND_CONN_CLEANING);
    atomic_store(&c2->state, BACKEND_CONN_IDLE);
    c1->active_owner = NULL;
    c2->active_owner = NULL;
    pool->active_count = 0;
    c1->next = c2;
    c2->next = NULL;
    pool->clean_list = c1;
    pool->clean_count = 2;

    /* Now borrow — should get c2 (IDLE), NOT c1 (CLEANING) */
    backend_conn_t* borrowed = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(borrowed);
    TEST_ASSERT(borrowed != c1);  /* Must not get the CLEANING connection */
    TEST_ASSERT_EQ(atomic_load(&borrowed->state), BACKEND_CONN_ACTIVE);

    /* Cleanup the intentionally invalid fixture manually. */
    if (borrowed && borrowed->fd >= 0) {
        close(borrowed->fd);
        borrowed->fd = -1;
    }
    atomic_store(&borrowed->state, BACKEND_CONN_CLOSED);
    atomic_store(&c1->state, BACKEND_CONN_CLOSED);
    pool->clean_list = NULL;
    pool->clean_count = 0;

    destroy_test_pool(pool, be_fds, 2);
    TEST_END();
}

/* ============================================================================
 * Test: Drain Cleaning with Simulated Response
 * ============================================================================ */

static void test_drain_cleaning_reclaims(void)
{
    TEST_BEGIN("reactor cleanup reclaims connections with ReadyForQuery response");

    int be_fds[2];
    backend_pool_t* pool = make_test_pool(2, be_fds);

    /* Borrow a connection and make it dirty */
    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    conn->current_state_hash = 0xDEAD;

    /* Find which backend fd corresponds to this connection */
    int be_fd = -1;
    for (size_t i = 0; i < pool->total_count; i++) {
        if (&pool->connections[i] == conn) {
            be_fd = be_fds[i];
            break;
        }
    }
    TEST_ASSERT(be_fd >= 0);

    /* Return it — cleanup should enter SEND and stay owned by the reactor. */
    backend_pool_return(pool, conn, false);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLEANING);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_SEND);

    /* Drive the reactor until the DISCARD ALL command is written. */
    uint8_t discard_buf[64];
    ssize_t nr = -1;
    for (int i = 0; i < 10 && nr <= 0; i++) {
        reactor_tick(pool->reactor, 10);
        nr = recv(be_fd, discard_buf, sizeof(discard_buf), MSG_DONTWAIT);
    }
    TEST_ASSERT(nr > 0);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLEANING);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_DRAIN);
    TEST_ASSERT(conn->cleanup_io_armed);

    /* Simulate backend response: CommandComplete + ReadyForQuery('I') */
    /* CommandComplete: 'C' + length(4) + "DISCARD ALL\0" = 'C' + 16 bytes */
    uint8_t response[] = {
        'C', 0, 0, 0, 16,
        'D','I','S','C','A','R','D',' ','A','L','L', 0,
        'Z', 0, 0, 0, 5, 'I'
    };
    ssize_t sw = send(be_fd, response, sizeof(response), MSG_NOSIGNAL);
    TEST_ASSERT(sw == (ssize_t)sizeof(response));

    /* Reactor recv callback should parse the response and reclaim it. */
    for (int i = 0; i < 10 &&
         atomic_load(&conn->state) == BACKEND_CONN_CLEANING; i++) {
        reactor_tick(pool->reactor, 10);
    }
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_IDLE);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_NONE);
    TEST_ASSERT_EQ(conn->current_state_hash, 0ULL);
    TEST_ASSERT_EQ(conn->stmt_set_hash, 0ULL);
    TEST_ASSERT_EQ(pool->cleaning_count, 0U);

    destroy_test_pool(pool, be_fds, 2);
    TEST_END();
}

static void test_cleanup_notification_closes(void)
{
    TEST_BEGIN("cleanup parser closes on async notification bytes");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    conn->current_state_hash = 0x1234;

    backend_pool_return(pool, conn, false);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLEANING);

    uint8_t discard_buf[64];
    ssize_t nr = -1;
    for (int i = 0; i < 10 && nr <= 0; i++) {
        reactor_tick(pool->reactor, 10);
        nr = recv(be_fds[0], discard_buf, sizeof(discard_buf), MSG_DONTWAIT);
    }
    TEST_ASSERT(nr > 0);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_DRAIN);

    /* NotificationResponse carries meaningful async payload with no session
     * owner. The pool must not discard it and reuse the backend. */
    uint8_t notification[] = {
        'A', 0, 0, 0, 12,
        0, 0, 0, 1,
        'c', 0,
        'p', 0
    };
    ssize_t sw = send(be_fds[0], notification, sizeof(notification), MSG_NOSIGNAL);
    TEST_ASSERT(sw == (ssize_t)sizeof(notification));

    for (int i = 0; i < 10 &&
         atomic_load(&conn->state) == BACKEND_CONN_CLEANING; i++) {
        reactor_tick(pool->reactor, 10);
    }
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLOSED);
    TEST_ASSERT_EQ(pool->cleaning_count, 0U);
    TEST_ASSERT_EQ(pool->clean_count, 0U);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

static void test_cleanup_timeout_closes_backend(void)
{
    TEST_BEGIN("cleanup timeout closes backend and never returns to idle");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    conn->current_state_hash = 0x123456;
    backend_pool_return(pool, conn, false);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLEANING);

    /* Force timeout eligibility and run cleaner supervision. */
    conn->cleanup_started_ms = 1;
    size_t closed = backend_pool_drain_cleaning(pool);
    TEST_ASSERT_EQ(closed, 1U);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLOSED);
    TEST_ASSERT_EQ(conn->close_reason, BACKEND_CLOSE_REASON_CLEANUP_TIMEOUT);
    TEST_ASSERT_EQ(conn->cleanup_last_result, BACKEND_CLEANUP_RESULT_TIMEOUT);
    TEST_ASSERT_EQ(pool->cleaning_count, 0U);
    TEST_ASSERT_EQ(pool->clean_count, 0U);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

static void test_backend_eof_during_cleanup_closes(void)
{
    TEST_BEGIN("backend EOF during cleanup closes backend");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    conn->current_state_hash = 0xCAFE;
    backend_pool_return(pool, conn, false);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLEANING);

    uint8_t discard_buf[64];
    ssize_t nr = -1;
    for (int i = 0; i < 10 && nr <= 0; i++) {
        reactor_tick(pool->reactor, 10);
        nr = recv(be_fds[0], discard_buf, sizeof(discard_buf), MSG_DONTWAIT);
    }
    TEST_ASSERT(nr > 0);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_DRAIN);

    /* Backend disappears while cleanup drain is waiting. */
    close(be_fds[0]);
    be_fds[0] = -1;

    for (int i = 0; i < 20 &&
         atomic_load(&conn->state) == BACKEND_CONN_CLEANING; i++) {
        reactor_tick(pool->reactor, 10);
    }
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLOSED);
    TEST_ASSERT_EQ(conn->cleanup_last_result, BACKEND_CLEANUP_RESULT_BACKEND_EOF);
    TEST_ASSERT_EQ(pool->cleaning_count, 0U);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

static void test_waiters_wake_only_when_cleaning_backend_becomes_idle(void)
{
    TEST_BEGIN("pool waiters wake only when a cleaning backend returns idle");

    int be_fds[3];
    backend_pool_t* pool = make_test_pool(3, be_fds);
    backend_pool_set_wait_callback(pool, wait_probe_cb);
    reset_wait_probe();

    backend_conn_t* c0 = backend_pool_borrow(pool, 0);
    backend_conn_t* c1 = backend_pool_borrow(pool, 0);
    backend_conn_t* c2 = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(c0);
    TEST_ASSERT_NOT_NULL(c1);
    TEST_ASSERT_NOT_NULL(c2);

    c0->current_state_hash = 0x10;
    c1->current_state_hash = 0x20;
    c2->current_state_hash = 0x30;
    backend_pool_return(pool, c0, false);
    backend_pool_return(pool, c1, false);
    backend_pool_return(pool, c2, false);
    TEST_ASSERT_EQ(pool->cleaning_count, 3U);

    int waiter = 42;
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &waiter, pool), 0);
    TEST_ASSERT_EQ(g_wait_count, 0);

    /* Flush cleanup command on one backend and feed successful cleanup response. */
    uint8_t discard_buf[64];
    ssize_t nr = -1;
    for (int i = 0; i < 10 && nr <= 0; i++) {
        reactor_tick(pool->reactor, 10);
        nr = recv(be_fds[0], discard_buf, sizeof(discard_buf), MSG_DONTWAIT);
    }
    TEST_ASSERT(nr > 0);

    uint8_t response[] = {
        'C', 0, 0, 0, 16,
        'D','I','S','C','A','R','D',' ','A','L','L', 0,
        'Z', 0, 0, 0, 5, 'I'
    };
    ssize_t sw = send(be_fds[0], response, sizeof(response), MSG_NOSIGNAL);
    TEST_ASSERT(sw == (ssize_t)sizeof(response));

    for (int i = 0; i < 20 && g_wait_count == 0; i++)
        reactor_tick(pool->reactor, 10);

    TEST_ASSERT_EQ(g_wait_count, 1);
    TEST_ASSERT_EQ(g_wait_order[0], 42);
    TEST_ASSERT_EQ(pool->wait_queue_size, 0U);

    destroy_test_pool(pool, be_fds, 3);
    TEST_END();
}

/* ============================================================================
 * §6.8.2 — Cleanup closes backend on non-idle ReadyForQuery status
 * ============================================================================ */

/*
 * pgf_drain_cleanup_response returns KEEL_PROTO_DRAIN_ERROR when it sees a
 * ReadyForQuery with status 'E' (error/failed transaction) or 'T' (open
 * transaction) instead of the expected 'I' (idle).  The pool must respond by
 * closing the backend, not recycling it.
 */
static void test_cleanup_rfq_transaction_error_closes(void)
{
    TEST_BEGIN("cleanup closes backend when ReadyForQuery reports non-idle status");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    conn->current_state_hash = 0x5678;
    backend_pool_return(pool, conn, false);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLEANING);

    /* Drain the cleanup command (DISCARD ALL) sent to the backend. */
    uint8_t discard_buf[64];
    ssize_t nr = -1;
    for (int i = 0; i < 10 && nr <= 0; i++) {
        reactor_tick(pool->reactor, 10);
        nr = recv(be_fds[0], discard_buf, sizeof(discard_buf), MSG_DONTWAIT);
    }
    TEST_ASSERT(nr > 0);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_DRAIN);

    /* Respond with ReadyForQuery('E') — transaction error state.
     * pgf_drain_cleanup_response returns KEEL_PROTO_DRAIN_ERROR for non-'I'. */
    uint8_t response[] = { 'Z', 0, 0, 0, 5, 'E' };
    ssize_t sw = send(be_fds[0], response, sizeof(response), MSG_NOSIGNAL);
    TEST_ASSERT(sw == (ssize_t)sizeof(response));

    for (int i = 0; i < 10 &&
         atomic_load(&conn->state) == BACKEND_CONN_CLEANING; i++) {
        reactor_tick(pool->reactor, 10);
    }
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLOSED);
    TEST_ASSERT_EQ(pool->cleaning_count, 0U);
    TEST_ASSERT_EQ(pool->clean_count, 0U);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

/* ============================================================================
 * §6.8.7 — Cleanup drain handles response split across multiple recv calls
 * ============================================================================ */

/*
 * The cleanup response may arrive in fragments.  pgf_drain_cleanup_response
 * preserves per-message parse state (pg_cleanup_drain_state_t) between calls
 * so a response header split in the middle does not corrupt the drain or cause
 * a spurious close.  The backend must reach IDLE only after the complete valid
 * response sequence is received.
 */
static void test_cleanup_split_response(void)
{
    TEST_BEGIN("cleanup drain handles response split across two reactor recv calls");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    conn->current_state_hash = 0xABCD;
    backend_pool_return(pool, conn, false);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLEANING);

    /* Drain the cleanup command sent to the backend. */
    uint8_t discard_buf[64];
    ssize_t nr = -1;
    for (int i = 0; i < 10 && nr <= 0; i++) {
        reactor_tick(pool->reactor, 10);
        nr = recv(be_fds[0], discard_buf, sizeof(discard_buf), MSG_DONTWAIT);
    }
    TEST_ASSERT(nr > 0);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_DRAIN);

    /* Valid response: CommandComplete("DISCARD ALL") + ReadyForQuery('I').
     * Split after the first 3 bytes — the 5-byte PG message header is
     * intentionally incomplete so the drain returns KEEL_PROTO_DRAIN_MORE
     * and re-arms recv before the full message is processed. */
    uint8_t full_response[] = {
        'C', 0, 0, 0, 16,
        'D','I','S','C','A','R','D',' ','A','L','L', 0,
        'Z', 0, 0, 0, 5, 'I'
    };

    /* Part 1: first 3 bytes — incomplete header. */
    ssize_t sw = send(be_fds[0], full_response, 3, MSG_NOSIGNAL);
    TEST_ASSERT(sw == 3);

    /* Tick: drain consumes partial header, returns DRAIN_MORE, re-arms recv. */
    for (int i = 0; i < 10; i++)
        reactor_tick(pool->reactor, 10);

    /* Backend must still be cleaning — partial data is not enough to reclaim. */
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLEANING);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_DRAIN);

    /* Part 2: remainder of response. */
    sw = send(be_fds[0], full_response + 3, sizeof(full_response) - 3, MSG_NOSIGNAL);
    TEST_ASSERT(sw == (ssize_t)(sizeof(full_response) - 3));

    for (int i = 0; i < 10 &&
         atomic_load(&conn->state) == BACKEND_CONN_CLEANING; i++) {
        reactor_tick(pool->reactor, 10);
    }
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_IDLE);
    TEST_ASSERT_EQ(pool->cleaning_count, 0U);
    TEST_ASSERT_EQ(conn->current_state_hash, 0ULL);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

/* ============================================================================
 * §6.8.8 — Cleanup failure wakes pool waiters to prevent starvation
 * ============================================================================ */

/*
 * When a backend under cleanup fails (protocol error, EOF, non-idle RFQ, etc.)
 * the slot is closed without becoming idle.  Any session queued in the wait
 * queue must be woken so it can react (attempt to borrow, fail fast, or
 * escalate an error) rather than waiting forever for a slot that will never
 * arrive.
 */
static void test_cleanup_failure_wakes_waiter(void)
{
    TEST_BEGIN("cleanup failure wakes waiting session to prevent starvation");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);
    backend_pool_set_wait_callback(pool, wait_probe_cb);
    reset_wait_probe();

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    conn->current_state_hash = 0x9999;

    /* Return dirty — backend enters CLEANING, no idle connections remain. */
    backend_pool_return(pool, conn, false);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLEANING);
    TEST_ASSERT_EQ(pool->clean_count, 0U);

    /* Queue a waiter — must block until cleanup finishes. */
    int session = 77;
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &session, pool), 0);
    TEST_ASSERT_EQ(g_wait_count, 0);

    /* Drain the cleanup command. */
    uint8_t discard_buf[64];
    ssize_t nr = -1;
    for (int i = 0; i < 10 && nr <= 0; i++) {
        reactor_tick(pool->reactor, 10);
        nr = recv(be_fds[0], discard_buf, sizeof(discard_buf), MSG_DONTWAIT);
    }
    TEST_ASSERT(nr > 0);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_DRAIN);

    /* Respond with RFQ('T') — open transaction, not idle.
     * Cleanup fails; backend is closed without becoming idle.
     * The waiter must be woken despite no idle slot being available,
     * preventing permanent starvation. */
    uint8_t response[] = { 'Z', 0, 0, 0, 5, 'T' };
    ssize_t sw = send(be_fds[0], response, sizeof(response), MSG_NOSIGNAL);
    TEST_ASSERT(sw == (ssize_t)sizeof(response));

    for (int i = 0; i < 20 && g_wait_count == 0; i++)
        reactor_tick(pool->reactor, 10);

    TEST_ASSERT_EQ(g_wait_count, 1);
    TEST_ASSERT_EQ(g_wait_order[0], 77);
    TEST_ASSERT_EQ(pool->wait_queue_size, 0U);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLOSED);
    TEST_ASSERT_EQ(pool->cleaning_count, 0U);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

/* ============================================================================
 * Test: conn_is_alive — dead connection is discarded by borrow
 * ============================================================================ */

/*
 * When the peer end of a socketpair is closed before borrow is called the
 * liveness peek (MSG_PEEK | MSG_DONTWAIT) returns 0 (EOF), which must cause
 * the borrow to discard that slot and return NULL (no other connection
 * available).
 */
static void test_borrow_skips_dead_connection(void)
{
    TEST_BEGIN("borrow skips connection whose peer fd was closed (conn_is_alive=false)");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);

    /* Close the peer end — the connection is now dead */
    close(be_fds[0]);
    be_fds[0] = -1;

    /* The single connection slot is on clean_list.  borrow should detect EOF
     * via conn_is_alive, discard the slot, and return NULL. */
    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT(conn == NULL);

    /* The slot should have been marked CLOSED */
    bool all_closed = true;
    for (size_t i = 0; i < pool->total_count; i++) {
        if (atomic_load(&pool->connections[i].state) != BACKEND_CONN_CLOSED)
            all_closed = false;
    }
    TEST_ASSERT(all_closed);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

/*
 * When two connections are in the pool and one is dead, borrow must skip
 * the dead one and return the alive one.
 */
static void test_borrow_returns_alive_when_one_dead(void)
{
    TEST_BEGIN("borrow returns the alive connection when one of two slots is dead");

    int be_fds[2];
    backend_pool_t* pool = make_test_pool(2, be_fds);

    /* Close the peer for the first connection only */
    close(be_fds[0]);
    be_fds[0] = -1;

    /* borrow should skip the dead slot and return the alive one */
    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    /* The returned connection must have a valid fd */
    TEST_ASSERT(conn->fd >= 0);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_ACTIVE);

    backend_pool_return(pool, conn, true);

    destroy_test_pool(pool, be_fds, 2);
    TEST_END();
}

/* ============================================================================
 * Test: backend_pool_drain_idle closes all idle lists
 * ============================================================================ */

/*
 * backend_pool_drain_idle is called on DOWN→UP probe transitions to flush
 * stale connections.  It must close every fd on clean_list, idle_list, and
 * dirty_list, reset those lists to NULL, and return the count of closed slots.
 */
static void test_drain_idle_closes_all_lists(void)
{
    TEST_BEGIN("drain_idle closes all idle connections and resets list pointers");

    int be_fds[3];
    backend_pool_t* pool = make_test_pool(3, be_fds);

    /* All 3 connections start on clean_list */
    TEST_ASSERT(pool->clean_list != NULL);
    TEST_ASSERT_EQ(pool->clean_count, 3U);

    size_t drained = backend_pool_drain_idle(pool);
    TEST_ASSERT(drained >= 3U);
    TEST_ASSERT(pool->clean_list == NULL);
    TEST_ASSERT(pool->idle_list  == NULL);
    TEST_ASSERT(pool->dirty_list == NULL);
    TEST_ASSERT_EQ(pool->clean_count, 0U);

    /* All slots must be CLOSED; their fd fields have been closed by drain */
    for (size_t i = 0; i < pool->total_count; i++) {
        TEST_ASSERT_EQ(atomic_load(&pool->connections[i].state), BACKEND_CONN_CLOSED);
        pool->connections[i].fd = -1; /* already closed by drain, prevent double-close */
    }
    /* Peer fds are still open — close them here */
    for (size_t i = 0; i < 3; i++) {
        if (be_fds[i] >= 0) { close(be_fds[i]); be_fds[i] = -1; }
    }

    destroy_test_pool(pool, be_fds, 3);
    TEST_END();
}

/* ============================================================================
 * Test: backend_pool_discard
 * ============================================================================ */

/*
 * backend_pool_discard() must atomically transition a borrowed connection from
 * ACTIVE to CLOSED and decrement pool->active_count.  Calling it a second time
 * (double-discard) must be a safe no-op.
 */
static void test_discard_decrements_active_count(void)
{
    TEST_BEGIN("discard transitions ACTIVE→CLOSED and decrements active_count");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    TEST_ASSERT_EQ(pool->active_count, 1U);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_ACTIVE);

    /* Simulate an error: close the fd and discard */
    close(conn->fd);
    conn->fd = -1;
    backend_pool_discard(pool, conn);

    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLOSED);
    TEST_ASSERT_EQ(pool->active_count, 0U);

    /* Second discard must be a safe no-op (state is already CLOSED) */
    backend_pool_discard(pool, conn);
    TEST_ASSERT_EQ(pool->active_count, 0U);

    /* Peer fd still open; fd in slot was already closed above */
    be_fds[0] = be_fds[0]; /* suppress unused warning */
    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

/* ============================================================================
 * Test: backend_pool_get_stats
 * ============================================================================ */

static void test_get_stats_counts_connections(void)
{
    TEST_BEGIN("get_stats returns accurate idle/active/total counts");

    int be_fds[3];
    backend_pool_t* pool = make_test_pool(3, be_fds);

    backend_pool_stats_t stats = {0};
    backend_pool_get_stats(pool, &stats);

    TEST_ASSERT_EQ(stats.total_connections,  3U);
    TEST_ASSERT_EQ(stats.idle_connections,   3U);
    TEST_ASSERT_EQ(stats.clean_connections,  3U);
    TEST_ASSERT_EQ(stats.stateful_connections, 0U);
    TEST_ASSERT_EQ(stats.dirty_connections,  0U);
    TEST_ASSERT_EQ(stats.closed_connections, 0U);
    TEST_ASSERT_EQ(stats.active_connections, 0U);
    TEST_ASSERT_EQ(stats.waiting_sessions,   0U);

    /* Borrow one — active_connections should increment */
    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    memset(&stats, 0, sizeof stats);
    backend_pool_get_stats(pool, &stats);
    TEST_ASSERT_EQ(stats.active_connections, 1U);
    TEST_ASSERT_EQ(stats.idle_connections,   2U);

    backend_pool_return(pool, conn, false);
    destroy_test_pool(pool, be_fds, 3);
    TEST_END();
}

/* ============================================================================
 * Test: backend_pool_has_available
 * ============================================================================ */

static void test_has_available_reflects_clean_list(void)
{
    TEST_BEGIN("has_available returns true when idle connections exist");

    int be_fds[2];
    backend_pool_t* pool = make_test_pool(2, be_fds);

    TEST_ASSERT(backend_pool_has_available(pool));
    TEST_ASSERT(!backend_pool_has_available(NULL));

    /* Borrow both; pool should still report available = false */
    backend_conn_t* c1 = backend_pool_borrow(pool, 0);
    backend_conn_t* c2 = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(c1);
    TEST_ASSERT_NOT_NULL(c2);
    TEST_ASSERT(!backend_pool_has_available(pool));

    backend_pool_return(pool, c1, false);
    backend_pool_return(pool, c2, false);
    destroy_test_pool(pool, be_fds, 2);
    TEST_END();
}

/* ============================================================================
 * Test: backend_pool_mark_transaction / backend_pool_update_state_hash
 * ============================================================================ */

static void test_mark_transaction_transitions_state(void)
{
    TEST_BEGIN("mark_transaction sets/clears TXN_PINNED state correctly");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    /* begin=true: must flip to TXN_PINNED */
    backend_pool_mark_transaction(pool, conn, true);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_TXN_PINNED);
    TEST_ASSERT(conn->in_transaction);

    /* begin=false with no pinned_session: must flip back to ACTIVE */
    backend_pool_mark_transaction(pool, conn, false);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_ACTIVE);
    TEST_ASSERT(!conn->in_transaction);

    backend_pool_return(pool, conn, false);
    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

static void test_update_state_hash_pins_connection(void)
{
    TEST_BEGIN("update_state_hash transitions to STATE_PINNED on non-zero hash");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    backend_pool_update_state_hash(pool, conn, 0xdeadbeefULL);
    TEST_ASSERT_EQ(conn->current_state_hash, 0xdeadbeefULL);
    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_STATE_PINNED);

    /* Reset to zero — state stays STATE_PINNED (caller must return/discard) */
    backend_pool_update_state_hash(pool, conn, 0);
    TEST_ASSERT_EQ(conn->current_state_hash, 0U);

    /* Discard instead of return because state is no longer ACTIVE */
    close(conn->fd);
    conn->fd = -1;
    backend_pool_discard(pool, conn);
    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

/* ============================================================================
 * Test: per-user connection accounting
 * ============================================================================ */

static void test_user_conn_accounting(void)
{
    TEST_BEGIN("user_can_acquire / acquire / release track per-user active count");

    int be_fds[2];
    backend_pool_t* pool = make_test_pool(2, be_fds);

    /* With max_user_connections == 0 (default), can_acquire always returns true */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "alice"));
    TEST_ASSERT(backend_pool_user_can_acquire(NULL, "alice"));

    /* Set a per-user limit of 1 */
    pool->config.max_user_connections = 1;

    TEST_ASSERT(backend_pool_user_can_acquire(pool, "alice"));
    backend_pool_user_conn_acquire(pool, "alice");
    /* Now at limit: must return false */
    TEST_ASSERT(!backend_pool_user_can_acquire(pool, "alice"));
    /* Different user must still be allowed */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "bob"));

    backend_pool_user_conn_release(pool, "alice");
    /* After release: alice can acquire again */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "alice"));

    /* Extra release must not underflow (should be a safe no-op) */
    backend_pool_user_conn_release(pool, "alice");
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "alice"));

    destroy_test_pool(pool, be_fds, 2);
    TEST_END();
}

/* ============================================================================
 * Test: wait queue backpressure and cancellation
 * ============================================================================ */

static void test_wait_queue_is_bounded(void)
{
    TEST_BEGIN("wait queue is bounded by max_waiting");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);
    pool->config.max_waiting = 2;

    int s1 = 1, s2 = 2, s3 = 3;
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s1, pool), 0);
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s2, pool), 0);
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s3, pool), -1);
    TEST_ASSERT_EQ(pool->wait_queue_size, 2U);

    TEST_ASSERT_EQ(backend_pool_cancel_wait(pool, &s1), 1U);
    TEST_ASSERT_EQ(backend_pool_cancel_wait(pool, &s2), 1U);
    TEST_ASSERT_EQ(pool->wait_queue_size, 0U);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

static void test_wait_queue_cancel_removes_dead_session(void)
{
    TEST_BEGIN("wait queue cancellation removes disconnected session");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);
    pool->config.max_waiting = 4;

    int s1 = 1, s2 = 2, s3 = 3;
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s1, pool), 0);
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s2, pool), 0);
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s3, pool), 0);

    TEST_ASSERT_EQ(backend_pool_cancel_wait(pool, &s2), 1U);
    TEST_ASSERT_EQ(pool->wait_queue_size, 2U);
    TEST_ASSERT(pool->wait_queue_head->session == &s1);
    TEST_ASSERT(pool->wait_queue_tail->session == &s3);

    TEST_ASSERT_EQ(backend_pool_cancel_wait(pool, &s1), 1U);
    TEST_ASSERT_EQ(backend_pool_cancel_wait(pool, &s3), 1U);
    TEST_ASSERT_EQ(pool->wait_queue_size, 0U);
    TEST_ASSERT(pool->wait_queue_head == NULL);
    TEST_ASSERT(pool->wait_queue_tail == NULL);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

static void test_wait_queue_timeout_no_leak(void)
{
    TEST_BEGIN("wait queue timeout removes all expired waiters");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);
    pool->config.max_waiting = 4;
    pool->config.wait_timeout_ms = 1;
    backend_pool_set_wait_callback(pool, wait_probe_cb);
    reset_wait_probe();

    int s1 = 1, s2 = 2;
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s1, pool), 0);
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s2, pool), 0);
    for (pool_waiter_t* w = pool->wait_queue_head; w; w = w->next)
        w->enqueue_time_ms = 0;

    size_t expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 2U);
    TEST_ASSERT_EQ(pool->wait_queue_size, 0U);
    TEST_ASSERT_EQ(g_timeout_count, 2);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

static void test_wait_queue_fifo_resume_order(void)
{
    TEST_BEGIN("wait queue resumes waiters in FIFO order");

    int be_fds[1];
    backend_pool_t* pool = make_test_pool(1, be_fds);
    pool->config.max_waiting = 4;
    backend_pool_set_wait_callback(pool, wait_probe_cb);
    reset_wait_probe();

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    int s1 = 1, s2 = 2, s3 = 3;
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s1, pool), 0);
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s2, pool), 0);
    TEST_ASSERT_EQ(backend_pool_queue_wait(pool, &s3, pool), 0);

    backend_pool_return(pool, conn, false);
    conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    backend_pool_return(pool, conn, false);
    conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    backend_pool_return(pool, conn, false);

    TEST_ASSERT_EQ(g_wait_count, 3);
    TEST_ASSERT_EQ(g_wait_order[0], 1);
    TEST_ASSERT_EQ(g_wait_order[1], 2);
    TEST_ASSERT_EQ(g_wait_order[2], 3);
    TEST_ASSERT_EQ(pool->wait_queue_size, 0U);

    destroy_test_pool(pool, be_fds, 1);
    TEST_END();
}

static void test_stmt_semantic_compatibility_predicate(void)
{
    TEST_BEGIN("stmt semantic profile compatibility predicate");

    backend_conn_t conn;
    memset(&conn, 0, sizeof(conn));
    conn.stmt_set_hash = 0xABCDEFULL;
    conn.stmt_profile.stmt_set_hash = 0xABCDEFULL;
    conn.stmt_profile.semantic_profile_hash = 0x1111ULL;
    conn.stmt_profile.schema_epoch = 3;
    conn.stmt_profile.role_hash = 0x2222ULL;
    conn.stmt_profile.search_path_hash = 0x3333ULL;
    conn.stmt_profile.guc_hash = 0x4444ULL;
    conn.stmt_profile.semantic_unknown = false;

    keel_stmt_compat_profile_t req = conn.stmt_profile;
    req.stmt_set_hash = conn.stmt_set_hash;
    TEST_ASSERT(backend_pool_stmt_compatible(&req, &conn));

    req.semantic_profile_hash ^= 0x1ULL;
    TEST_ASSERT(!backend_pool_stmt_compatible(&req, &conn));
    req = conn.stmt_profile;
    req.stmt_set_hash = conn.stmt_set_hash;

    req.role_hash ^= 0x1ULL;
    TEST_ASSERT(!backend_pool_stmt_compatible(&req, &conn));
    req = conn.stmt_profile;
    req.stmt_set_hash = conn.stmt_set_hash;

    req.semantic_unknown = true;
    TEST_ASSERT(!backend_pool_stmt_compatible(&req, &conn));

    conn.stmt_profile.semantic_unknown = true;
    req.semantic_unknown = false;
    req.stmt_set_hash = conn.stmt_set_hash;
    TEST_ASSERT(!backend_pool_stmt_compatible(&req, &conn));

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void)
{
    printf("=== Pool Correctness Tests ===\n\n");

    test_cleaning_state_transition();
    test_clean_return_bypasses_discard();
    test_clean_gen_increments();
    test_backend_can_borrow_predicate_matrix();
    test_backend_generation_validation();
    test_close_reason_once();
    test_admission_control_max_pinned();
    test_quarantine_pin_flags();
    test_sticky_primary_fields();
    test_borrow_rejects_cleaning();
    test_drain_cleaning_reclaims();
    test_cleanup_notification_closes();
    test_cleanup_timeout_closes_backend();
    test_backend_eof_during_cleanup_closes();
    test_waiters_wake_only_when_cleaning_backend_becomes_idle();
    test_cleanup_rfq_transaction_error_closes();
    test_cleanup_split_response();
    test_cleanup_failure_wakes_waiter();
    test_borrow_skips_dead_connection();
    test_borrow_returns_alive_when_one_dead();
    test_drain_idle_closes_all_lists();
    test_discard_decrements_active_count();
    test_get_stats_counts_connections();
    test_has_available_reflects_clean_list();
    test_mark_transaction_transitions_state();
    test_update_state_hash_pins_connection();
    test_user_conn_accounting();
    test_wait_queue_is_bounded();
    test_wait_queue_cancel_removes_dead_session();
    test_wait_queue_timeout_no_leak();
    test_wait_queue_fifo_resume_order();
    test_stmt_semantic_compatibility_predicate();

    printf("\n");
    return test_summary();
}
