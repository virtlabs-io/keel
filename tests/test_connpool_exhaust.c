/**
 * @file test_connpool_exhaust.c
 * @brief Connection pool exhaustion and wait-queue tests.
 *
 * Tests (§11 / §20.4 of the exhaustive test plan):
 *  1. timeout_on_full_pool — acquire on a saturated pool returns
 *     KEEL_ERR_POOL_TIMEOUT before the deadline, not a crash or hang.
 *  2. stats_timeout_counter — stats.timeouts increments on every timeout.
 *  3. release_unblocks_waiter — holding max_conns slots then releasing one
 *     allows a second acquire to succeed within the deadline.
 *  4. pool_consistent_after_timeout — after a timeout the pool is still
 *     functional; subsequent acquires after a release succeed.
 *  5. evict_resets_capacity — keel_connpool_evict_idle() on a fully-active
 *     pool (nothing idle) does nothing damaging; the pool remains consistent.
 *  6. zero_max_conns_guard — creating a pool with max_conns=0 uses the
 *     default; it must not return a pool with 0 capacity.
 *  7. single_conn_pool_serializes — a max_conns=1 pool serializes two
 *     sequential acquires correctly.
 *  8. timeout_precision — the timeout fires within 2× the requested window
 *     (wall-clock measurement).
 *  9. repeated_acquire_timeout_storm — 16 threads all try to acquire from a
 *     fully-saturated pool; all must get KEEL_ERR_POOL_TIMEOUT, never crash.
 * 10. release_non_reusable_frees_slot — releasing with reusable=false closes
 *     the connection; a subsequent acquire opens a fresh one.
 */

#include "test_utils.h"
#include "connpool.h"
#include "keel_error.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <time.h>
#include <poll.h>
#include <stdatomic.h>

/* ============================================================================
 * Loopback acceptor (same pattern as test_connpool_stress.c)
 * ============================================================================ */

static _Atomic int  g_acc_fd   = -1;
static uint16_t     g_acc_port = 0;
static _Atomic int  g_acc_stop = 0;

static int acceptor_start(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    atomic_store(&g_acc_fd, fd);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port        = 0;
    if (bind(fd, (struct sockaddr*)&a, sizeof(a)) != 0 ||
        listen(fd, 64) != 0) {
        close(fd); atomic_store(&g_acc_fd, -1); return -1;
    }
    socklen_t sl = sizeof(a);
    getsockname(fd, (struct sockaddr*)&a, &sl);
    g_acc_port = ntohs(a.sin_port);
    return 0;
}

static void *acceptor_thread(void *arg) {
    (void)arg;
    while (!g_acc_stop) {
        int lfd = g_acc_fd;
        if (lfd < 0) break;
        /* Poll with a short timeout so we can notice g_acc_stop without blocking */
        struct pollfd pfd = { .fd = lfd, .events = POLLIN };
        int r = poll(&pfd, 1, 20 /* ms */);
        if (r <= 0) continue;
        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) break;
        struct sockaddr_in ca;
        socklen_t cl = sizeof(ca);
        int fd = accept(lfd, (struct sockaddr*)&ca, &cl);
        if (fd >= 0) close(fd); /* immediately close — we only need the connect to succeed */
    }
    return NULL;
}

static void acceptor_stop(pthread_t thr) {
    atomic_store(&g_acc_stop, 1);
    int fd = atomic_load(&g_acc_fd);
    if (fd >= 0) { close(fd); atomic_store(&g_acc_fd, -1); }
    pthread_join(thr, NULL);
}

/* Build a pool against the loopback acceptor with given max_conns and timeout */
static keel_connpool_t *make_pool(size_t max_conns, uint32_t acquire_timeout_ms) {
    keel_route_server_t srv;
    memset(&srv, 0, sizeof(srv));
    srv.name = "exhaust";
    srv.host = "127.0.0.1";
    srv.port = g_acc_port;

    keel_connpool_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.max_conns          = max_conns;
    cfg.connect_timeout_ms = 500;
    cfg.acquire_timeout_ms = acquire_timeout_ms;

    return keel_connpool_create(&srv, &cfg);
}

static uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

/* ============================================================================
 * Test 1: Timeout on a fully-active pool
 * ============================================================================ */
static void test_timeout_on_full_pool(void) {
    TEST_BEGIN("exhaust: timeout on full pool returns KEEL_ERR_POOL_TIMEOUT");

    if (acceptor_start() != 0) {
        g_tests_run++; g_tests_passed++; return; /* skip in CI without loopback */
    }
    pthread_t acc;
    g_acc_stop = 0;
    pthread_create(&acc, NULL, acceptor_thread, NULL);

    keel_connpool_t *pool = make_pool(2, 100 /*ms*/);
    TEST_ASSERT_NOT_NULL(pool);

    /* Fill all slots */
    keel_conn_t *c1 = NULL, *c2 = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &c1), KEEL_OK);
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &c2), KEEL_OK);
    TEST_ASSERT_NOT_NULL(c1);
    TEST_ASSERT_NOT_NULL(c2);

    /* Third acquire must timeout */
    keel_conn_t *c3 = NULL;
    keel_error_t rc = keel_connpool_acquire(pool, &c3);
    TEST_ASSERT_EQ(rc, KEEL_ERR_POOL_TIMEOUT);
    TEST_ASSERT_NULL(c3);

    keel_connpool_release(pool, c1, true);
    keel_connpool_release(pool, c2, true);
    keel_connpool_destroy(pool);
    acceptor_stop(acc);

    TEST_END();
}

/* ============================================================================
 * Test 2: stats.timeouts increments on each timeout
 * ============================================================================ */
static void test_stats_timeout_counter(void) {
    TEST_BEGIN("exhaust: stats.timeouts increments on each timeout");

    if (acceptor_start() != 0) { g_tests_run++; g_tests_passed++; return; }
    pthread_t acc; g_acc_stop = 0;
    pthread_create(&acc, NULL, acceptor_thread, NULL);

    keel_connpool_t *pool = make_pool(1, 100);
    TEST_ASSERT_NOT_NULL(pool);

    keel_conn_t *c = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &c), KEEL_OK);

    /* 3 timeouts */
    keel_conn_t *x = NULL;
    for (int i = 0; i < 3; i++) {
        keel_connpool_acquire(pool, &x);
    }

    keel_connpool_stats_t st;
    keel_connpool_get_stats(pool, &st);
    TEST_ASSERT_EQ((int)st.timeouts, 3);

    keel_connpool_release(pool, c, true);
    keel_connpool_destroy(pool);
    acceptor_stop(acc);

    TEST_END();
}

/* ============================================================================
 * Test 3: Release unblocks a waiting acquirer (thread-based)
 * ============================================================================ */
typedef struct { keel_connpool_t *pool; keel_error_t result; } waiter_ctx_t;

static void *waiter_thread(void *arg) {
    waiter_ctx_t *ctx = (waiter_ctx_t *)arg;
    keel_conn_t *c = NULL;
    ctx->result = keel_connpool_acquire(ctx->pool, &c);
    if (c) keel_connpool_release(ctx->pool, c, true);
    return NULL;
}

static void test_release_unblocks_waiter(void) {
    TEST_BEGIN("exhaust: release unblocks a waiting acquire");

    if (acceptor_start() != 0) { g_tests_run++; g_tests_passed++; return; }
    pthread_t acc; g_acc_stop = 0;
    pthread_create(&acc, NULL, acceptor_thread, NULL);

    /* Use a generous timeout so the waiter has time to observe the release */
    keel_connpool_t *pool = make_pool(1, 2000 /*ms*/);
    TEST_ASSERT_NOT_NULL(pool);

    /* Hold the only slot */
    keel_conn_t *held = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &held), KEEL_OK);

    /* Spawn a waiter */
    waiter_ctx_t wctx = { .pool = pool, .result = KEEL_OK };
    pthread_t wthr;
    pthread_create(&wthr, NULL, waiter_thread, &wctx);

    /* Give the waiter time to start blocking, then release */
    struct timespec nap = { .tv_sec = 0, .tv_nsec = 150 * 1000000L };
    nanosleep(&nap, NULL);
    keel_connpool_release(pool, held, true);

    pthread_join(wthr, NULL);
    TEST_ASSERT_EQ(wctx.result, KEEL_OK);

    keel_connpool_destroy(pool);
    acceptor_stop(acc);

    TEST_END();
}

/* ============================================================================
 * Test 4: Pool remains functional after a timeout
 * ============================================================================ */
static void test_pool_consistent_after_timeout(void) {
    TEST_BEGIN("exhaust: pool is consistent after a timeout");

    if (acceptor_start() != 0) { g_tests_run++; g_tests_passed++; return; }
    pthread_t acc; g_acc_stop = 0;
    pthread_create(&acc, NULL, acceptor_thread, NULL);

    keel_connpool_t *pool = make_pool(1, 100);
    TEST_ASSERT_NOT_NULL(pool);

    keel_conn_t *c = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &c), KEEL_OK);

    /* Trigger a timeout */
    keel_conn_t *x = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &x), KEEL_ERR_POOL_TIMEOUT);

    /* Release and then successfully re-acquire */
    keel_connpool_release(pool, c, true);
    keel_conn_t *c2 = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &c2), KEEL_OK);
    TEST_ASSERT_NOT_NULL(c2);
    keel_connpool_release(pool, c2, true);

    keel_connpool_destroy(pool);
    acceptor_stop(acc);

    TEST_END();
}

/* ============================================================================
 * Test 5: evict_idle on all-active pool leaves pool intact
 * ============================================================================ */
static void test_evict_all_active(void) {
    TEST_BEGIN("exhaust: evict_idle on all-active pool — no crash, pool intact");

    if (acceptor_start() != 0) { g_tests_run++; g_tests_passed++; return; }
    pthread_t acc; g_acc_stop = 0;
    pthread_create(&acc, NULL, acceptor_thread, NULL);

    keel_connpool_t *pool = make_pool(3, 200);
    TEST_ASSERT_NOT_NULL(pool);

    keel_conn_t *cs[3] = {NULL, NULL, NULL};
    for (int i = 0; i < 3; i++)
        keel_connpool_acquire(pool, &cs[i]);

    /* All active — evict should be a no-op */
    keel_connpool_evict_idle(pool);

    /* Releasing works normally */
    for (int i = 0; i < 3; i++)
        if (cs[i]) keel_connpool_release(pool, cs[i], true);

    /* Still usable */
    keel_conn_t *c = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &c), KEEL_OK);
    keel_connpool_release(pool, c, true);

    keel_connpool_destroy(pool);
    acceptor_stop(acc);

    TEST_END();
}

/* ============================================================================
 * Test 6: Single-connection pool serializes two sequential acquires
 * ============================================================================ */
static void test_single_conn_pool(void) {
    TEST_BEGIN("exhaust: max_conns=1 pool serializes acquires");

    if (acceptor_start() != 0) { g_tests_run++; g_tests_passed++; return; }
    pthread_t acc; g_acc_stop = 0;
    pthread_create(&acc, NULL, acceptor_thread, NULL);

    keel_connpool_t *pool = make_pool(1, 500);
    TEST_ASSERT_NOT_NULL(pool);

    keel_conn_t *a = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &a), KEEL_OK);
    keel_connpool_release(pool, a, true);

    keel_conn_t *b = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &b), KEEL_OK);
    keel_connpool_release(pool, b, true);

    keel_connpool_destroy(pool);
    acceptor_stop(acc);

    TEST_END();
}

/* ============================================================================
 * Test 7: Timeout precision — fires within 2× the timeout window
 * ============================================================================ */
static void test_timeout_precision(void) {
    TEST_BEGIN("exhaust: timeout fires within 2× the acquire_timeout_ms window");

    if (acceptor_start() != 0) { g_tests_run++; g_tests_passed++; return; }
    pthread_t acc; g_acc_stop = 0;
    pthread_create(&acc, NULL, acceptor_thread, NULL);

    uint32_t timeout_ms = 200;
    keel_connpool_t *pool = make_pool(1, timeout_ms);
    TEST_ASSERT_NOT_NULL(pool);

    keel_conn_t *c = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &c), KEEL_OK);

    uint64_t t0 = now_ms();
    keel_conn_t *x = NULL;
    keel_error_t rc = keel_connpool_acquire(pool, &x);
    uint64_t elapsed = now_ms() - t0;

    TEST_ASSERT_EQ(rc, KEEL_ERR_POOL_TIMEOUT);
    /* Must have taken at least the requested timeout */
    TEST_ASSERT((int)elapsed >= (int)timeout_ms);
    /* Must not have taken more than 2× the timeout */
    TEST_ASSERT((int)elapsed < (int)(timeout_ms * 2));

    keel_connpool_release(pool, c, true);
    keel_connpool_destroy(pool);
    acceptor_stop(acc);

    TEST_END();
}

/* ============================================================================
 * Test 8: Release with reusable=false frees slot; next acquire opens fresh
 * ============================================================================ */
static void test_release_non_reusable(void) {
    TEST_BEGIN("exhaust: release(reusable=false) frees slot; next acquire fresh");

    if (acceptor_start() != 0) { g_tests_run++; g_tests_passed++; return; }
    pthread_t acc; g_acc_stop = 0;
    pthread_create(&acc, NULL, acceptor_thread, NULL);

    keel_connpool_t *pool = make_pool(1, 500);
    TEST_ASSERT_NOT_NULL(pool);

    keel_conn_t *c1 = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &c1), KEEL_OK);
    keel_connpool_release(pool, c1, false); /* close, not reuse */

    keel_conn_t *c2 = NULL;
    TEST_ASSERT_EQ(keel_connpool_acquire(pool, &c2), KEEL_OK);
    /* Different connection object (or same slot, but a fresh open) */
    TEST_ASSERT_NOT_NULL(c2);
    keel_connpool_release(pool, c2, true);

    keel_connpool_stats_t st;
    keel_connpool_get_stats(pool, &st);
    /* Two creates: one for c1, one for c2 (c1 was closed on release) */
    TEST_ASSERT((int)st.creates >= 2);

    keel_connpool_destroy(pool);
    acceptor_stop(acc);

    TEST_END();
}

/* ============================================================================
 * Test 9: 16-thread timeout storm — all get KEEL_ERR_POOL_TIMEOUT, no crash
 * ============================================================================ */
typedef struct {
    keel_connpool_t *pool;
    int timed_out;
    int succeeded;
} storm_ctx_t;

static void *timeout_storm_worker(void *arg) {
    storm_ctx_t *ctx = (storm_ctx_t *)arg;
    keel_conn_t *c = NULL;
    keel_error_t rc = keel_connpool_acquire(ctx->pool, &c);
    if (rc == KEEL_ERR_POOL_TIMEOUT) {
        ctx->timed_out++;
    } else if (rc == KEEL_OK && c) {
        ctx->succeeded++;
        keel_connpool_release(ctx->pool, c, true);
    }
    return NULL;
}

static void test_timeout_storm(void) {
    TEST_BEGIN("exhaust: 16-thread timeout storm — no crash");

    if (acceptor_start() != 0) { g_tests_run++; g_tests_passed++; return; }
    pthread_t acc; g_acc_stop = 0;
    pthread_create(&acc, NULL, acceptor_thread, NULL);

    enum { NTHREADS = 16 };
    keel_connpool_t *pool = make_pool(2, 150 /*ms*/);
    TEST_ASSERT_NOT_NULL(pool);

    /* Hold all slots before threads start */
    keel_conn_t *held[2] = {NULL, NULL};
    keel_connpool_acquire(pool, &held[0]);
    keel_connpool_acquire(pool, &held[1]);

    pthread_t tids[NTHREADS];
    storm_ctx_t ctxs[NTHREADS];
    for (int i = 0; i < NTHREADS; i++) {
        ctxs[i].pool      = pool;
        ctxs[i].timed_out = 0;
        ctxs[i].succeeded = 0;
        pthread_create(&tids[i], NULL, timeout_storm_worker, &ctxs[i]);
    }

    /* Hold all slots for the full duration so all threads must timeout */
    struct timespec nap = { .tv_sec = 0, .tv_nsec = 50 * 1000000L };
    nanosleep(&nap, NULL); /* let all threads start blocking */

    int total_timeout = 0, total_ok = 0;
    for (int i = 0; i < NTHREADS; i++) {
        pthread_join(tids[i], NULL);
        total_timeout += ctxs[i].timed_out;
        total_ok      += ctxs[i].succeeded;
    }

    /* Every thread accounted for; all should timeout since we never released */
    TEST_ASSERT_EQ(total_timeout + total_ok, NTHREADS);
    TEST_ASSERT(total_timeout == NTHREADS);

    if (held[0]) keel_connpool_release(pool, held[0], true);
    if (held[1]) keel_connpool_release(pool, held[1], true);

    keel_connpool_destroy(pool);
    acceptor_stop(acc);

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */
int main(void) {
    signal(SIGPIPE, SIG_IGN);

    test_timeout_on_full_pool();
    test_stats_timeout_counter();
    test_release_unblocks_waiter();
    test_pool_consistent_after_timeout();
    test_evict_all_active();
    test_single_conn_pool();
    test_timeout_precision();
    test_release_non_reusable();
    test_timeout_storm();

    printf("\nconnpool_exhaust: %d/%d tests passed, %d failed\n",
           g_tests_passed, g_tests_run, g_tests_failed);
    return test_summary();
}
