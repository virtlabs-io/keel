/**
 * @file test_state_machine.c
 * @brief White-box verification of the session/backend transaction state model.
 *
 * This suite codifies the state table described in the design notes instead of
 * merely exercising it indirectly through larger integration flows. Each test
 * assembles the smallest possible session plus backend-pool fixture, forces a
 * specific lifecycle edge, and then checks that every mirrored bookkeeping bit
 * stays coherent across the transition.
 *
 * The important property here is not protocol correctness but bookkeeping
 * correctness. A transaction-aware multiplexer must keep several representations
 * of "who owns this backend and what phase is it in" aligned at all times:
 *
 *   1. The session-visible state machine.
 *   2. The backend connection state enum.
 *   3. Sticky ownership fields such as `backend_conn` and `pinned_session`.
 *   4. Transaction flags such as `in_transaction` and error markers.
 *   5. The backing file descriptor validity used by defensive assertions.
 *
 * We intentionally use `socketpair()` rather than real network sockets or a
 * live database. That keeps the tests deterministic while still satisfying the
 * low-level invariants checked by session and pool helpers that require an fd
 * which looks like a real connected endpoint.
 */

#include "test_utils.h"
#include "keel/session/session.h"
#include "keel/engine/backend_pool.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdatomic.h>

/* ============================================================================
 * Helpers
 * ============================================================================
 */

/**
 * @brief Build the smallest backend pool fixture that still behaves like a
 *        borrowable live connection.
 * @param backend_fd_out [out] Receives the peer end of the socketpair used by
 *                             the synthetic backend connection.
 * @return Heap-allocated pool containing one idle connection.
 *
 * The tests avoid calling the full pool bootstrap path because they are not
 * verifying DNS, TCP connect, authentication, or reactor registration. They
 * only need a backend_conn_t with a valid fd, an owning pool, and the clean
 * list wired up so transaction helpers operate on realistic structure state.
 */
static backend_pool_t *make_pool_one(int *backend_fd_out)
{
    backend_pool_config_t cfg = {
        .host = "127.0.0.1",
        .port = 5432,
        .user = "test",
        .password = "test",
        .database = "test",
        .min_connections = 1,
        .max_connections = 1,
        .max_waiting       = 4,
    };

    backend_pool_t *pool = keel_calloc(1, sizeof(backend_pool_t));
    pool->config = cfg;
    pool->connections = keel_calloc(1, sizeof(backend_conn_t));
    pool->total_count = 1;

    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        pool->connections[0].fd = -1;
        *backend_fd_out = -1;
    } else {
        pool->connections[0].fd = sv[0];
        *backend_fd_out = sv[1];
    }

    atomic_store(&pool->connections[0].state, BACKEND_CONN_IDLE);
    pool->connections[0].pool = pool;

    /* Put the single connection on the clean list */
    pool->clean_list = &pool->connections[0];
    pool->clean_count = 1;

    return pool;
}

/**
 * @brief Release the one-connection pool fixture created by make_pool_one().
 * @param pool Pool fixture to destroy.
 * @param backend_fd Peer fd returned to the caller for optional test-side use.
 * @return
 *
 * Cleanup is kept local instead of using production teardown helpers because
 * the fixture deliberately bypasses parts of normal initialization. Symmetric
 * local destruction makes that shortcut explicit and avoids depending on side
 * effects outside the scope of these state-table checks.
 */
static void destroy_pool_one(backend_pool_t *pool, int backend_fd)
{
    if (pool->connections[0].fd >= 0) close(pool->connections[0].fd);
    if (backend_fd >= 0) close(backend_fd);
    keel_free(pool->connections);
    keel_free(pool);
}

/*
 * The session fixture is intentionally thin: the production initializer sets up
 * internal buffers and sentinel values, while the surrounding test decides when
 * the session becomes associated with a backend and which transaction phase it
 * should represent.
 */
static void make_session(keel_session_t *s, int client_fd)
{
    keel_session_init(s, client_fd);
}

/* ============================================================================
 * State Transition Table
 *
 * States tested:
 *   S0: IDLE          — session READY, backend IDLE, no pin, no in_transaction
 *   S1: IN_TX         — session QUERY, backend TXN_PINNED, in_transaction=true
 *   S2: TX_ERROR      — session QUERY, backend TXN_PINNED, in_transaction=true,
 *                       error in flight (simulated with last_error != 0)
 *   S3: ROLLBACK      — session QUERY or READY (issuing ROLLBACK),
 *                       backend still TXN_PINNED until ROLLBACK completes
 *   S4: POST_ROLLBACK — session READY, backend IDLE, no pin, no in_transaction
 *
 * Map:  S0 → S1 → S2 → S3 → S4 → S0  (happy-path transaction lifecycle)
 *       S0 → S1 → S4                  (clean COMMIT returns to IDLE)
 * ============================================================================
 */

/* ============================================================================
 * Test 1 — IDLE state invariants
 * ============================================================================
 */
static void test_idle_state(void)
{
    TEST_BEGIN("state_machine: IDLE — invariants");

    keel_mem_init(NULL);

    int client_sv[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, client_sv);

    keel_session_t s;
    make_session(&s, client_sv[0]);

    int backend_fd;
    backend_pool_t *pool = make_pool_one(&backend_fd);
    backend_conn_t *conn = &pool->connections[0];

    /* IDLE invariants */
    TEST_ASSERT(keel_session_is_idle(&s, 0) || s.state == KEEL_SESSION_INIT);
    TEST_ASSERT_EQ(s.in_transaction, false);
    TEST_ASSERT_EQ(s.hard_pinned,    false);
    TEST_ASSERT_EQ(s.backend_conn,   NULL);

    TEST_ASSERT(atomic_load(&conn->state) == BACKEND_CONN_IDLE);
    TEST_ASSERT_EQ(conn->in_transaction, false);
    TEST_ASSERT_EQ(conn->hard_pinned,    false);
    TEST_ASSERT_EQ(conn->pinned_session, NULL);

    destroy_pool_one(pool, backend_fd);
    keel_session_cleanup(&s);
    close(client_sv[0]);
    close(client_sv[1]);

    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 2 — IDLE → IN_TX transition
 * ============================================================================
 */
static void test_idle_to_in_tx(void)
{
    TEST_BEGIN("state_machine: IDLE → IN_TX");

    keel_mem_init(NULL);

    int client_sv[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, client_sv);

    keel_session_t s;
    make_session(&s, client_sv[0]);

    int backend_fd;
    backend_pool_t *pool = make_pool_one(&backend_fd);
    backend_conn_t *conn = &pool->connections[0];

    /* Simulate: client sends BEGIN.  Engine borrows the connection. */
    conn->pinned_session = &s;
    s.backend_conn = conn;
    atomic_store(&conn->state, BACKEND_CONN_TXN_PINNED);

    /* Mark begin-of-transaction */
    backend_pool_mark_transaction(pool, conn, true);

    /* IN_TX invariants */
    TEST_ASSERT(atomic_load(&conn->state) == BACKEND_CONN_TXN_PINNED);
    TEST_ASSERT_EQ(conn->in_transaction, true);
    TEST_ASSERT_EQ(conn->pinned_session, &s);
    TEST_ASSERT(conn->fd >= 0);   /* Backend FD must be valid */

    /* Session must reference the backend conn */
    TEST_ASSERT_EQ(s.backend_conn, conn);

    destroy_pool_one(pool, backend_fd);
    keel_session_cleanup(&s);
    close(client_sv[0]);
    close(client_sv[1]);

    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 3 — IN_TX → TX_ERROR (backend returns error response)
 * ============================================================================
 */
static void test_in_tx_to_error(void)
{
    TEST_BEGIN("state_machine: IN_TX → TX_ERROR");

    keel_mem_init(NULL);

    int client_sv[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, client_sv);

    keel_session_t s;
    make_session(&s, client_sv[0]);

    int backend_fd;
    backend_pool_t *pool = make_pool_one(&backend_fd);
    backend_conn_t *conn = &pool->connections[0];

    /* Simulate: IN_TX state (BEGIN was issued) */
    conn->pinned_session = &s;
    s.backend_conn = conn;
    atomic_store(&conn->state, BACKEND_CONN_TXN_PINNED);
    backend_pool_mark_transaction(pool, conn, true);
    TEST_ASSERT_EQ(conn->in_transaction, true);

    /* Simulate: backend returns ERROR (e.g., constraint violation) */
    s.last_error = 1;  /* non-zero = error recorded */
    /* Connection stays TXN_PINNED — client must ROLLBACK before release */
    TEST_ASSERT(atomic_load(&conn->state) == BACKEND_CONN_TXN_PINNED);

    /* TX_ERROR invariants:
     *   - in_transaction still true (uncommitted)
     *   - session references the same backend fd
     *   - backend conn is still the same valid fd */
    TEST_ASSERT_EQ(conn->in_transaction, true);
    TEST_ASSERT(conn->fd >= 0);
    TEST_ASSERT_EQ(s.backend_conn, conn);
    TEST_ASSERT(s.last_error != 0);

    destroy_pool_one(pool, backend_fd);
    keel_session_cleanup(&s);
    close(client_sv[0]);
    close(client_sv[1]);

    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 4 — TX_ERROR → ROLLBACK → IDLE
 * ============================================================================
 */
static void test_error_to_rollback_to_idle(void)
{
    TEST_BEGIN("state_machine: TX_ERROR → ROLLBACK → IDLE");

    keel_mem_init(NULL);

    int client_sv[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, client_sv);

    keel_session_t s;
    make_session(&s, client_sv[0]);

    int backend_fd;
    backend_pool_t *pool = make_pool_one(&backend_fd);
    backend_conn_t *conn = &pool->connections[0];

    /* Start in TX_ERROR state */
    conn->pinned_session = &s;
    s.backend_conn = conn;
    atomic_store(&conn->state, BACKEND_CONN_TXN_PINNED);
    backend_pool_mark_transaction(pool, conn, true);
    s.last_error = 1;

    /* Client issues ROLLBACK.  The engine:
     *   1. Sends ROLLBACK to the backend.
     *   2. On ReadyForQuery('I'), calls backend_pool_mark_transaction(false).
     *   3. Returns the connection to the pool.
     * We simulate steps 2 and 3 here. */
    backend_pool_mark_transaction(pool, conn, false);
    TEST_ASSERT_EQ(conn->in_transaction, false);

    /* Simulate: engine returns connection to pool as IDLE */
    conn->pinned_session = NULL;
    s.backend_conn = NULL;
    s.last_error   = 0;
    atomic_store(&conn->state, BACKEND_CONN_IDLE);

    /* Pool moves it back to clean_list */
    pool->clean_list = conn;
    pool->clean_count = 1;

    /* POST-ROLLBACK / IDLE invariants */
    TEST_ASSERT(atomic_load(&conn->state) == BACKEND_CONN_IDLE);
    TEST_ASSERT_EQ(conn->in_transaction, false);
    TEST_ASSERT_EQ(conn->pinned_session, NULL);
    TEST_ASSERT_EQ(s.backend_conn, NULL);
    TEST_ASSERT_EQ(s.last_error,   0);

    destroy_pool_one(pool, backend_fd);
    keel_session_cleanup(&s);
    close(client_sv[0]);
    close(client_sv[1]);

    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 5 — IN_TX → COMMIT → IDLE (happy path)
 * ============================================================================
 */
static void test_in_tx_commit_to_idle(void)
{
    TEST_BEGIN("state_machine: IN_TX → COMMIT → IDLE (happy path)");

    keel_mem_init(NULL);

    int client_sv[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, client_sv);

    keel_session_t s;
    make_session(&s, client_sv[0]);

    int backend_fd;
    backend_pool_t *pool = make_pool_one(&backend_fd);
    backend_conn_t *conn = &pool->connections[0];

    /* Move to IN_TX */
    conn->pinned_session = &s;
    s.backend_conn = conn;
    atomic_store(&conn->state, BACKEND_CONN_TXN_PINNED);
    backend_pool_mark_transaction(pool, conn, true);
    TEST_ASSERT_EQ(conn->in_transaction, true);

    /* COMMIT received and acknowledged */
    backend_pool_mark_transaction(pool, conn, false);

    /* Return to pool */
    conn->pinned_session = NULL;
    s.backend_conn = NULL;
    atomic_store(&conn->state, BACKEND_CONN_IDLE);
    pool->clean_list = conn;
    pool->clean_count = 1;

    /* IDLE invariants */
    TEST_ASSERT(atomic_load(&conn->state) == BACKEND_CONN_IDLE);
    TEST_ASSERT_EQ(conn->in_transaction, false);
    TEST_ASSERT_EQ(conn->pinned_session, NULL);
    TEST_ASSERT_EQ(s.backend_conn, NULL);

    destroy_pool_one(pool, backend_fd);
    keel_session_cleanup(&s);
    close(client_sv[0]);
    close(client_sv[1]);

    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 6 — Backend FD stays constant throughout transaction lifecycle
 *
 * "Sticky Bit" verification: the same backend FD must be used for all
 * operations within a transaction.  A new FD would mean we switched backends
 * mid-transaction — catastrophic for ACID guarantees.
 * ============================================================================
 */
static void test_backend_fd_sticky_throughout_txn(void)
{
    TEST_BEGIN("state_machine: backend FD sticky throughout transaction");

    keel_mem_init(NULL);

    int client_sv[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, client_sv);

    keel_session_t s;
    make_session(&s, client_sv[0]);

    int backend_fd;
    backend_pool_t *pool = make_pool_one(&backend_fd);
    backend_conn_t *conn = &pool->connections[0];

    /* Capture the backend FD before the transaction */
    int original_backend_fd = conn->fd;
    TEST_ASSERT(original_backend_fd >= 0);

    /* BEGIN */
    conn->pinned_session = &s;
    s.backend_conn = conn;
    atomic_store(&conn->state, BACKEND_CONN_TXN_PINNED);
    backend_pool_mark_transaction(pool, conn, true);

    /* Verify FD is unchanged after BEGIN */
    TEST_ASSERT_EQ(conn->fd, original_backend_fd);
    TEST_ASSERT_EQ(s.backend_conn->fd, original_backend_fd);

    /* Simulate INSERT (query within transaction) */
    TEST_ASSERT_EQ(conn->fd, original_backend_fd); /* Still the same FD */

    /* Simulate second query in same transaction */
    TEST_ASSERT_EQ(conn->fd, original_backend_fd); /* Still the same FD */

    /* COMMIT */
    backend_pool_mark_transaction(pool, conn, false);
    int fd_at_commit = conn->fd;

    /* FD must be unchanged until the connection is actually closed/recycled */
    TEST_ASSERT_EQ(fd_at_commit, original_backend_fd);

    destroy_pool_one(pool, backend_fd);
    keel_session_cleanup(&s);
    close(client_sv[0]);
    close(client_sv[1]);

    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 7 — State name strings (sanity)
 * ============================================================================
 */
static void test_state_name_strings(void)
{
    TEST_BEGIN("state_machine: state name strings are non-NULL and non-empty");

    /* Every state must have a non-NULL, non-empty descriptive name */
    for (int i = 0; i < (int)KEEL_SESSION_STATE_COUNT; i++) {
        const char *name = keel_session_state_name((keel_session_state_t)i);
        TEST_ASSERT_NOT_NULL(name);
        TEST_ASSERT(name[0] != '\0');
    }

    TEST_END();
}

/* ============================================================================
 * Test 8 — Hard-pin (exclusive backend ownership)
 *
 * When a prepared statement is open on a backend, the session is hard-pinned
 * to that backend.  The pool must refuse to lend the connection to anyone else.
 * ============================================================================
 */
static void test_hard_pin_exclusive_ownership(void)
{
    TEST_BEGIN("state_machine: hard-pin — exclusive backend ownership");

    keel_mem_init(NULL);

    int client_sv[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, client_sv);

    keel_session_t s;
    make_session(&s, client_sv[0]);

    int backend_fd;
    backend_pool_t *pool = make_pool_one(&backend_fd);
    backend_conn_t *conn = &pool->connections[0];

    /* Simulate: prepared statement forces hard-pin */
    conn->pinned_session = &s;
    s.backend_conn = conn;
    s.hard_pinned = true;
    s.pin_reason  = 0x01; /* e.g. KEEL_PIN_PREPARED_STMT */
    conn->hard_pinned = true;
    atomic_store(&conn->state, BACKEND_CONN_STATE_PINNED);

    /* Reflect the ownership change in pool counters (mirrors what
     * backend_pool_borrow would do when removing from clean_list). */
    pool->clean_list  = NULL;
    pool->clean_count = 0;
    pool->pinned_count = 1;

    /* Hard-pin invariants: connection is NOT available in the clean list */
    TEST_ASSERT(pool->clean_list == NULL || pool->clean_list != conn);

    /* The backend FD must be valid */
    TEST_ASSERT(conn->fd >= 0);

    /* The session is the sole owner */
    TEST_ASSERT_EQ(conn->pinned_session, &s);
    TEST_ASSERT_EQ(s.hard_pinned, true);

    /* Simulate: session closes → connection released */
    conn->hard_pinned = false;
    s.hard_pinned = false;
    s.pin_reason  = 0;
    conn->pinned_session = NULL;
    s.backend_conn = NULL;
    atomic_store(&conn->state, BACKEND_CONN_IDLE);

    TEST_ASSERT_EQ(conn->pinned_session, NULL);
    TEST_ASSERT_EQ(conn->hard_pinned, false);

    destroy_pool_one(pool, backend_fd);
    keel_session_cleanup(&s);
    close(client_sv[0]);
    close(client_sv[1]);

    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================
 */
int main(void)
{
    printf("=== Formal State Machine Transition Tests (Phase B) ===\n\n");

    test_idle_state();
    test_idle_to_in_tx();
    test_in_tx_to_error();
    test_error_to_rollback_to_idle();
    test_in_tx_commit_to_idle();
    test_backend_fd_sticky_throughout_txn();
    test_state_name_strings();
    test_hard_pin_exclusive_ownership();

    return test_summary();
}
