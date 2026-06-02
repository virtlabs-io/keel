/**
 * @file test_pool_observer_race.c
 * @brief Regression torture: backend_pool waiter wakeup under observer-thread pressure.
 *
 * Pins the scenario that exposed POOL INVARIANT VIOLATION 0x00000800
 * (KEEL_PINV_ACTIVE_WITHOUT_OWNER, bit 11 from keel/engine/invariant.h)
 * when adding a non-mutating observer thread alongside heavy
 * borrow/return traffic with waiters present.
 *
 * The check `KEEL_CHECK_POOL_INVARIANTS(pool)` after
 * `backend_pool_wake_one_locked` in src/worker/backend_pool.c traps via
 * __builtin_trap → SIGILL when the invariant fails, so CTest reports
 * the failure as the process dying. A clean run = no invariant trip
 * across the full stress window.
 *
 * If this test ever starts failing, do NOT widen the timeout or reduce
 * the iteration count. The invariant exists for a reason; investigate
 * the wake_one_locked path and the conn->state / conn->active_owner
 * ordering instead.
 */

#include "test_utils.h"
#include "keel/engine/backend_pool.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/reactor/reactor.h"
#include "keel/mem/mem.h"

#include <pthread.h>
#include <stdatomic.h>
#include <fcntl.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <semaphore.h>

int g_tests_run    = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) {
    return (g_tests_failed == 0) ? 0 : 1;
}

/* ============================================================================
 * Fixture (mirrors tests/test_pool_correctness.c::make_test_pool)
 * ============================================================================ */

static void make_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static backend_pool_t* make_pool(size_t n, size_t max_waiting, int backend_fds[])
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
        .max_waiting = max_waiting,
    };

    backend_pool_t* pool = keel_calloc(1, sizeof(backend_pool_t));
    pool->config = cfg;
    pool->flow_vt = keel_proto_flow_get(cfg.protocol);
    TEST_ASSERT_NOT_NULL(pool->flow_vt);

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&pool->lock, &attr);
    pthread_mutexattr_destroy(&attr);

    keel_reactor_config_t rcfg = KEEL_REACTOR_CONFIG_DEFAULT;
    rcfg.type = KEEL_REACTOR_EPOLL;
    rcfg.max_fds = 128;
    pool->reactor = keel_reactor_create(&rcfg);
    TEST_ASSERT_NOT_NULL(pool->reactor);

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
        pool->connections[i].fd = sv[0];
        backend_fds[i] = sv[1];
        atomic_store(&pool->connections[i].state, BACKEND_CONN_IDLE);
        pool->connections[i].pool = pool;
        pool->connections[i].next = NULL;
    }

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

static void destroy_pool(backend_pool_t* pool, int backend_fds[], size_t n)
{
    if (pool->reactor) keel_reactor_destroy(pool->reactor);
    for (size_t i = 0; i < n; i++) {
        if (pool->connections[i].fd >= 0) close(pool->connections[i].fd);
        if (backend_fds[i] >= 0) close(backend_fds[i]);
    }
    keel_free(pool->connections);
    pthread_mutex_destroy(&pool->lock);
    keel_free(pool);
}

/* ============================================================================
 * Worker threads
 * ============================================================================ */

#define POOL_SIZE        2
#define MAX_WAITING      8
#define BORROWER_THREADS 6
#define ITERATIONS       50000  /* per borrower; ~2s wall on typical hw */
#define WORK_SPINS       512    /* widen the borrowed window so waiters
                                   queue and the wake_one_locked path
                                   is exercised every iteration */

typedef struct {
    backend_pool_t* pool;
    sem_t           wake_sem;
    _Atomic int     id;
} borrower_ctx_t;

static void wait_cb(void* session, void* userdata)
{
    (void)session;
    sem_t* sem = (sem_t*)userdata;
    if (sem) sem_post(sem);
}

static void* borrower_main(void* arg)
{
    borrower_ctx_t* ctx = (borrower_ctx_t*)arg;
    backend_pool_t* pool = ctx->pool;
    int self = atomic_load(&ctx->id);

    for (int it = 0; it < ITERATIONS; it++) {
        backend_conn_t* conn = backend_pool_borrow(pool, 0);
        if (!conn) {
            /* Pool exhausted — queue a waiter and spin briefly. The pool
             * uses a callback-based wakeup; the callback here just signals
             * our semaphore. We then retry borrow. */
            int rc = backend_pool_queue_wait(pool, &self, &ctx->wake_sem);
            if (rc != 0) {
                /* Wait queue full — yield and retry */
                struct timespec ts = {0, 50 * 1000}; /* 50us */
                nanosleep(&ts, NULL);
                continue;
            }
            struct timespec deadline;
            clock_gettime(CLOCK_REALTIME, &deadline);
            deadline.tv_nsec += 100 * 1000 * 1000; /* +100ms */
            if (deadline.tv_nsec >= 1000000000L) {
                deadline.tv_sec++;
                deadline.tv_nsec -= 1000000000L;
            }
            sem_timedwait(&ctx->wake_sem, &deadline);
            backend_pool_release_session(pool, &self);
            conn = backend_pool_borrow(pool, 0);
            if (!conn) continue;
        }

        /* Tiny "work" to widen the borrowed window so other borrowers
         * are more likely to queue as waiters → exercises wake path. */
        for (volatile int spin = 0; spin < WORK_SPINS; spin++) { }

        backend_pool_return(pool, conn, false);
    }
    return NULL;
}

/* ============================================================================
 * Observer thread — reproduces the failover_manager pattern that exposed
 * the race: a separate thread reading pool topology fields concurrently.
 * ============================================================================ */

typedef struct {
    backend_pool_t*  pool;
    _Atomic int      stop;
    _Atomic uint64_t reads;
} observer_ctx_t;

static void* observer_main(void* arg)
{
    observer_ctx_t* ctx = (observer_ctx_t*)arg;
    backend_pool_t* pool = ctx->pool;
    uint64_t reads = 0;

    while (!atomic_load(&ctx->stop)) {
        /* Same access pattern as failover_manager::detector_tick —
         * non-mutating reads of per-server / per-pool fields without
         * holding pool->lock. These reads must not corrupt the
         * wake_one_locked invariants. */
        (void)pool->total_count;
        (void)pool->active_count;
        (void)pool->clean_count;
        (void)pool->wait_queue_size;
        for (size_t i = 0; i < pool->total_count; i++) {
            (void)atomic_load(&pool->connections[i].state);
        }
        reads++;
    }
    atomic_store(&ctx->reads, reads);
    return NULL;
}

/* ============================================================================
 * Test: stress wake_one_locked path with observer interference
 * ============================================================================ */

static void test_wake_under_observer_pressure(void)
{
    TEST_BEGIN("backend_pool wake_one_locked stays invariant-clean under observer thread");

    int be_fds[POOL_SIZE];
    backend_pool_t* pool = make_pool(POOL_SIZE, MAX_WAITING, be_fds);
    backend_pool_set_wait_callback(pool, wait_cb);

    borrower_ctx_t bctx[BORROWER_THREADS];
    pthread_t      bth[BORROWER_THREADS];
    for (int i = 0; i < BORROWER_THREADS; i++) {
        bctx[i].pool = pool;
        atomic_store(&bctx[i].id, i + 1);
        sem_init(&bctx[i].wake_sem, 0, 0);
    }

    observer_ctx_t octx = { .pool = pool };
    atomic_store(&octx.stop, 0);
    atomic_store(&octx.reads, 0);
    pthread_t oth;
    pthread_create(&oth, NULL, observer_main, &octx);

    for (int i = 0; i < BORROWER_THREADS; i++) {
        pthread_create(&bth[i], NULL, borrower_main, &bctx[i]);
    }

    for (int i = 0; i < BORROWER_THREADS; i++) {
        pthread_join(bth[i], NULL);
    }

    atomic_store(&octx.stop, 1);
    pthread_join(oth, NULL);

    /* Final invariant check — process is still alive, so no trip happened. */
    TEST_ASSERT(atomic_load(&octx.reads) > 0);
    TEST_ASSERT_EQ(pool->active_count, 0U);
    TEST_ASSERT_EQ(pool->wait_queue_size, 0U);

    for (int i = 0; i < BORROWER_THREADS; i++) sem_destroy(&bctx[i].wake_sem);
    destroy_pool(pool, be_fds, POOL_SIZE);

    TEST_END();
}

int main(void)
{
    test_wake_under_observer_pressure();
    return test_summary();
}
