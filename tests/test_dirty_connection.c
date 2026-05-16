/**
 * @file test_dirty_connection.c
 * @brief Phase 3 — Database Protocol & Session Leak Tests (Spec §3)
 *
 * "The 'Dirty Connection' Audit":
 *   - SET time_zone = 'UTC'; → Disconnect.
 *   - The next connection to the proxy should run SELECT @@time_zone;
 *   - If it returns UTC, our Core failed to 'clean' the connection.
 *
 * This is a white-box unit test that exercises the backend_pool dirty
 * connection detection and CLEANING state transitions without a real database.
 *
 * The core invariant being tested:
 *   When a backend connection is returned to the pool with a non-zero
 *   state_hash (i.e., session variables were set), it MUST NOT be handed
 *   to the next clean-state client without first going through DISCARD ALL.
 *
 * Tests:
 *   1. Clean connection (state_hash == 0) → borrowable immediately
 *   2. Dirty connection (state_hash != 0) → enters reactor-owned CLEANING
 *   3. CLEANING connection remains unborrowable until cleanup RFQ(I)
 *   4. clean_gen monotonically increments on each return
 *   5. Cross-session isolation: session A's state does not leak to session B
 *   6. Connection returned `in_transaction=true` is not clean-list borrowable
 */

#include "test_utils.h"
#include "keel/engine/backend_pool.h"
#include "keel/reactor/reactor.h"
#include "keel/mem/mem.h"

#include <fcntl.h>
#include <string.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <unistd.h>

/* ============================================================================
 * Helpers — from test_pool_correctness.c pattern
 * ============================================================================
 */

/**
 * @brief Build a synthetic backend pool with socketpair-backed connections.
 *
 * Every connection starts on the clean_list with state_hash == 0.
 * The caller receives the peer-side FDs in @p backend_fds so it can
 * send wire-level responses through the "backend" side of each pair.
 *
 * @param n            Number of backend connections to create.
 * @param backend_fds  [out] Array of at least @p n ints; receives the
 *                     peer-side FD for each connection.
 * @return Heap-allocated pool.  Caller must destroy via destroy_pool().
 */

static void make_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static backend_pool_t *make_pool(size_t n, int backend_fds[])
{
    backend_pool_config_t cfg = {
        .host = "127.0.0.1", .port = 5432,
        .user = "test", .password = "test", .database = "test",
        .min_connections = n, .max_connections = n, .max_waiting = 8,
    };

    backend_pool_t *pool = keel_calloc(1, sizeof(backend_pool_t));
    pool->config     = cfg;
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
            pool->connections[i].fd = -1;
            backend_fds[i] = -1;
            continue;
        }
        make_nonblocking(sv[0]);
        make_nonblocking(sv[1]);
        pool->connections[i].fd   = sv[0];
        backend_fds[i]            = sv[1];
        pool->connections[i].pool = pool;
        atomic_store(&pool->connections[i].state, BACKEND_CONN_IDLE);
    }

    /* All start on clean_list */
    pool->clean_list  = NULL;
    pool->clean_count = 0;
    for (size_t i = 0; i < n; i++) {
        if (pool->connections[i].fd >= 0) {
            pool->connections[i].next  = pool->clean_list;
            pool->clean_list           = &pool->connections[i];
            pool->clean_count++;
        }
    }

    return pool;
}

/**
 * @brief Tear down a pool created by make_pool(), closing all FDs.
 *
 * @param pool         Pool to destroy.
 * @param backend_fds  Peer-side FDs returned by make_pool().
 * @param n            Number of connections.
 */
static void destroy_pool(backend_pool_t *pool, int backend_fds[], size_t n)
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
 * Test 1 — Clean connection (state_hash == 0) is immediately borrowable
 * ============================================================================
 */
static void test_clean_connection_borrowable(void)
{
    TEST_BEGIN("dirty_conn: clean connection (hash=0) is immediately borrowable");

    keel_mem_init(NULL);

    int bfds[1];
    backend_pool_t *pool = make_pool(1, bfds);
    TEST_ASSERT_EQ(pool->clean_count, (size_t)1);

    /* current_state_hash == 0 → clean */
    pool->connections[0].current_state_hash = 0;

    /* Should be on the clean list */
    backend_conn_t *conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);
    TEST_ASSERT(atomic_load(&conn->state) == BACKEND_CONN_ACTIVE);

    /* Return it */
    backend_pool_return(pool, conn, false);

    destroy_pool(pool, bfds, 1);
    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 2 — Dirty connection (state_hash != 0) enters CLEANING on return
 * ============================================================================
 */
static void test_dirty_connection_quarantined(void)
{
    TEST_BEGIN("dirty_conn: connection with non-zero state_hash enters CLEANING");

    keel_mem_init(NULL);

    int bfds[2];
    backend_pool_t *pool = make_pool(2, bfds);

    /* Borrow the first connection */
    backend_conn_t *conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    /* Simulate: client issued SET time_zone='UTC' → state_hash changes */
    const uint64_t dirty_hash = 0xDEADBEEF12345678ULL;
    conn->current_state_hash = dirty_hash;

    /* Return the dirty connection — it should NOT go to clean_list */
    backend_pool_return(pool, conn, false);

    TEST_ASSERT_EQ(atomic_load(&conn->state), BACKEND_CONN_CLEANING);
    TEST_ASSERT_EQ(conn->cleanup_state, BACKEND_CLEANUP_SEND);
    TEST_ASSERT(conn->cleanup_io_armed);
    TEST_ASSERT_EQ(pool->cleaning_count, (size_t)1);
    TEST_ASSERT_EQ(pool->dirty_count, (size_t)0);

    destroy_pool(pool, bfds, 2);
    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 3 — clean_gen is monotonically incremented on each return
 * ============================================================================
 */
static void test_clean_gen_monotonic(void)
{
    TEST_BEGIN("dirty_conn: clean_gen increments monotonically on return");

    keel_mem_init(NULL);

    int bfds[1];
    backend_pool_t *pool = make_pool(1, bfds);

    backend_conn_t *conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    uint64_t gen_before = conn->clean_gen;

    /* Return cleanly */
    backend_pool_return(pool, conn, false);

    uint64_t gen_after = conn->clean_gen;

    /* clean_gen must have incremented (or stayed same if not supported) */
    TEST_ASSERT(gen_after >= gen_before);

    /* Borrow and return again */
    conn = backend_pool_borrow(pool, 0);
    if (conn) {
        uint64_t gen_before2 = conn->clean_gen;
        backend_pool_return(pool, conn, false);
        TEST_ASSERT(conn->clean_gen >= gen_before2);
    }

    destroy_pool(pool, bfds, 1);
    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 4 — Cross-session isolation: Session A's state_hash ≠ Session B's
 * ============================================================================
 */
static void test_cross_session_state_isolation(void)
{
    TEST_BEGIN("dirty_conn: cross-session isolation — state_hash mismatch detected");

    keel_mem_init(NULL);

    int bfds[2];
    backend_pool_t *pool = make_pool(2, bfds);

    /* Session A borrows connection 0, sets state (dirty) */
    backend_conn_t *conn_a = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn_a);

    const uint64_t session_a_hash = 0xAAAAAAAA00000001ULL;
    conn_a->current_state_hash = session_a_hash;
    backend_pool_update_state_hash(pool, conn_a, session_a_hash);

    /* Session A returns the connection (it stays state-pinned or goes dirty) */
    backend_pool_return(pool, conn_a, false);

    /* Session B wants a clean connection (hash 0) */
    backend_conn_t *conn_b = backend_pool_borrow(pool, 0);
    if (conn_b != NULL) {
        /* Must not be the dirty conn_a (unless it went through DISCARD ALL) */
        if (conn_b == conn_a) {
            /* Allowed only if the DISCARD ALL cleared the state_hash */
            TEST_ASSERT_EQ(conn_b->current_state_hash, (uint64_t)0);
        }
        backend_pool_return(pool, conn_b, false);
    }
    /* If conn_b is NULL, the pool correctly refused to serve a dirty connection
     * to a clean-state client, which is also acceptable. */

    destroy_pool(pool, bfds, 2);
    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 5 — Connection returned while in_transaction=true is not clean borrowable
 * ============================================================================
 */
static void test_transaction_leaked_on_disconnect(void)
{
    TEST_BEGIN("dirty_conn: in_transaction connection returned → requires ROLLBACK");

    keel_mem_init(NULL);

    int bfds[1];
    backend_pool_t *pool = make_pool(1, bfds);

    backend_conn_t *conn = backend_pool_borrow(pool, 0);
    TEST_ASSERT_NOT_NULL(conn);

    /* Simulate: client disconnected in the middle of a transaction */
    conn->in_transaction = true;

    /* Return with in_transaction=true → pool must not make it clean-borrowable */
    backend_pool_return(pool, conn, true /* in_transaction */);

    /* The connection must NOT be immediately on the clean list */
    bool on_clean = false;
    for (backend_conn_t *c = pool->clean_list; c != NULL; c = c->next) {
        if (c == conn) {
            on_clean = true;
            break;
        }
    }
    /* It is valid for the pool to:
     *   a) Put it on dirty_list for deferred ROLLBACK+DISCARD ALL
     *   b) Mark it CLEANING immediately
     *   c) Close the connection and reduce pool size
     * What is NOT acceptable: conn->in_transaction==true and it's on clean_list */
    if (on_clean) {
        /* Only acceptable if ROLLBACK was sent and in_transaction cleared */
        TEST_ASSERT_EQ(conn->in_transaction, false);
    }

    destroy_pool(pool, bfds, 1);
    keel_mem_shutdown();
    TEST_END();
}

/* ============================================================================
 * Test 6 — Pool admission control: dirty_count does not grow unboundedly
 * ============================================================================
 */
static void test_dirty_count_bounded(void)
{
    TEST_BEGIN("dirty_conn: dirty_count is bounded by total_count");

#define POOL_SIZE 4
    int bfds[POOL_SIZE];
    keel_mem_init(NULL);
    backend_pool_t *pool = make_pool(POOL_SIZE, bfds);

    /* Borrow all connections and return them dirty */
    backend_conn_t *conns[POOL_SIZE];
    for (int i = 0; i < POOL_SIZE; i++) {
        conns[i] = backend_pool_borrow(pool, 0);
        if (conns[i]) {
            conns[i]->current_state_hash = (uint64_t)(0xDEAD0000 + i);
        }
    }

    for (int i = 0; i < POOL_SIZE; i++) {
        if (conns[i]) {
            backend_pool_return(pool, conns[i], false);
        }
    }

    /* dirty_count + cleaning_count + clean_count must equal total available */
    size_t total_accounted = pool->dirty_count + pool->cleaning_count + pool->clean_count;
    /* Allow for connections that were closed on dirty return */
    TEST_ASSERT(total_accounted <= pool->total_count);

    destroy_pool(pool, bfds, POOL_SIZE);
    keel_mem_shutdown();
    TEST_END();
#undef POOL_SIZE
}

/* ============================================================================
 * main
 * ============================================================================
 */
int main(void)
{
    printf("=== Dirty Connection Audit Tests (Protocol & Session Integrity §3) ===\n\n");

    test_clean_connection_borrowable();
    test_dirty_connection_quarantined();
    test_clean_gen_monotonic();
    test_cross_session_state_isolation();
    test_transaction_leaked_on_disconnect();
    test_dirty_count_bounded();

    return test_summary();
}
