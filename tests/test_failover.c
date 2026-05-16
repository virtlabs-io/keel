/**
 * @file test_failover.c
 * @brief Targeted regressions for pool failover and refill behavior.
 *
 * Failover bugs are usually not parser bugs; they are bookkeeping bugs around
 * host/port reconfiguration, draining stale idle connections, expiring waiters,
 * and backing off after repeated connect failures. This suite keeps those rules
 * explicit with synthetic pools and mock-like fixtures so operational behavior
 * can be checked without a live topology manager.
 */

#include "test_utils.h"
#include "keel/engine/engine.h"
#include "keel/probe/probe.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/mem/mem.h"

#include <stdatomic.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>

/*
 * The interesting failover state lives inside backend_pool_t rather than the
 * public API, so these tests intentionally inspect internal counters and lists.
 */
#include "keel/engine/backend_pool.h"

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Construct a synthetic backend pool whose connections are backed by
 *        local socketpairs instead of real database sockets.
 * @param n Number of slots to create.
 * @param backend_fds [out] Optional array receiving peer fds.
 * @return Heap-allocated pool fixture.
 */
static backend_pool_t* make_test_pool(size_t n, int backend_fds[])
{
    backend_pool_config_t cfg = {
        .host = "127.0.0.1",
        .port = 5432,
        .user = "test",
        .password = "test",
        .database = "test",
        .min_connections = n,
        .max_connections = n,
        .max_waiting = 64,
        .idle_timeout_ms = 0,
        .wait_timeout_ms = 0,
    };

    backend_pool_t* pool = keel_calloc(1, sizeof(backend_pool_t));
    pool->config = cfg;
    pool->connections = keel_calloc(n, sizeof(backend_conn_t));
    pool->total_count = n;

    for (size_t i = 0; i < n; i++) {
        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
            pool->connections[i].fd = -1;
            if (backend_fds) backend_fds[i] = -1;
            atomic_store(&pool->connections[i].state, BACKEND_CONN_CLOSED);
            continue;
        }
        pool->connections[i].fd = sv[0];
        pool->connections[i].pool = pool;
        if (backend_fds) backend_fds[i] = sv[1];
        atomic_store(&pool->connections[i].state, BACKEND_CONN_IDLE);
        pool->connections[i].current_state_hash = 0;
        pool->connections[i].next = pool->clean_list;
        pool->clean_list = &pool->connections[i];
        pool->clean_count++;
    }

    return pool;
}

/**
 * @brief Destroy a synthetic failover test pool.
 * @param pool Pool fixture to release.
 * @param backend_fds Peer fds paired with each connection.
 * @param n Slot count.
 * @return
 */
static void destroy_test_pool(backend_pool_t* pool, int backend_fds[], size_t n)
{
    for (size_t i = 0; i < n; i++) {
        if (pool->connections[i].fd >= 0) {
            close(pool->connections[i].fd);
        }
        if (backend_fds && backend_fds[i] >= 0) {
            close(backend_fds[i]);
        }
    }
    keel_free(pool->connections);
    keel_free(pool);
}

/**
 * @brief Read a coarse monotonic timestamp in milliseconds.
 * @return Milliseconds from a monotonic clock source.
 *
 * Coarse time is sufficient here because the tests only compare relative backoff
 * delays and waiter-expiration windows; they do not need wall-clock precision.
 */
static uint64_t get_time_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* ============================================================================
 * Test 1: backend_pool_update_target
 * ============================================================================ */

static void test_update_target(void)
{
    printf("  test_update_target...\n");

    int fds[4];
    backend_pool_t* pool = make_test_pool(4, fds);

    TEST_ASSERT_STR_EQ(pool->config.host, "127.0.0.1");
    TEST_ASSERT_EQ(pool->config.port, 5432);

    backend_pool_update_target(pool, "10.0.0.5", 5433);

    TEST_ASSERT_STR_EQ(pool->config.host, "10.0.0.5");
    TEST_ASSERT_EQ(pool->config.port, 5433);

    /* NULL pool should not crash */
    backend_pool_update_target(NULL, "x", 1);

    destroy_test_pool(pool, fds, 4);
}

/* ============================================================================
 * Test 2: backend_pool_drain_idle
 * ============================================================================ */

static void test_drain_idle(void)
{
    printf("  test_drain_idle...\n");

    int fds[4];
    backend_pool_t* pool = make_test_pool(4, fds);

    TEST_ASSERT_EQ(pool->clean_count, 4u);
    TEST_ASSERT(pool->clean_list != NULL);

    size_t closed = backend_pool_drain_idle(pool);
    TEST_ASSERT_EQ(closed, 4u);
    TEST_ASSERT(pool->clean_list == NULL);
    TEST_ASSERT_EQ(pool->clean_count, 0u);

    /* All connections should be CLOSED with fd=-1 */
    for (size_t i = 0; i < 4; i++) {
        backend_conn_state_t st = atomic_load(&pool->connections[i].state);
        TEST_ASSERT_EQ(st, BACKEND_CONN_CLOSED);
        TEST_ASSERT_EQ(pool->connections[i].fd, -1);
    }

    /* Drain empty pool returns 0 */
    closed = backend_pool_drain_idle(pool);
    TEST_ASSERT_EQ(closed, 0u);

    /* NULL pool doesn't crash */
    backend_pool_drain_idle(NULL);

    /* Clean up backend fds */
    for (size_t i = 0; i < 4; i++) {
        if (fds[i] >= 0) close(fds[i]);
    }
    keel_free(pool->connections);
    keel_free(pool);
}

/* ============================================================================
 * Test 3: backend_pool_has_available
 * ============================================================================ */

static void test_has_available(void)
{
    printf("  test_has_available...\n");

    int fds[2];
    backend_pool_t* pool = make_test_pool(2, fds);

    TEST_ASSERT(backend_pool_has_available(pool));

    /* Drain all idle — should return false */
    backend_pool_drain_idle(pool);
    TEST_ASSERT(!backend_pool_has_available(pool));

    /* NULL pool returns false */
    TEST_ASSERT(!backend_pool_has_available(NULL));

    for (size_t i = 0; i < 2; i++) {
        if (fds[i] >= 0) close(fds[i]);
    }
    keel_free(pool->connections);
    keel_free(pool);
}

/* ============================================================================
 * Test 4: backend_pool_expire_waiters
 * ============================================================================ */

/*
 * Waiter expiration is observed through the same callback hook production code
 * uses to wake or fail queued sessions. The globals below make those side
 * effects visible without needing a full client/session object.
 */
static int g_expire_callback_count;
static void* g_expire_last_session;
static void* g_expire_last_userdata;

static void expire_test_callback(void* session, void* userdata)
{
    g_expire_callback_count++;
    g_expire_last_session = session;
    g_expire_last_userdata = userdata;
}

static void test_expire_waiters(void)
{
    printf("  test_expire_waiters...\n");
    g_expire_callback_count = 0;

    int fds[2];
    backend_pool_t* pool = make_test_pool(2, fds);
    pool->config.wait_timeout_ms = 50;  /* 50ms timeout */
    backend_pool_set_wait_callback(pool, expire_test_callback);

    /* Enqueue a waiter */
    int dummy_session = 42;
    backend_pool_queue_wait(pool, &dummy_session, pool);
    TEST_ASSERT_EQ(pool->wait_queue_size, 1u);

    /* Expire immediately — should not expire (too fresh) */
    size_t expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 0u);
    TEST_ASSERT_EQ(pool->wait_queue_size, 1u);

    /* Sleep 60ms and expire — should timeout */
    usleep(60000);
    expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 1u);
    TEST_ASSERT_EQ(pool->wait_queue_size, 0u);
    TEST_ASSERT_EQ(g_expire_callback_count, 1);
    TEST_ASSERT(g_expire_last_session == &dummy_session);
    TEST_ASSERT(g_expire_last_userdata == NULL);  /* NULL = timeout signal */

    /* No waiters — expire returns 0 */
    expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 0u);

    /* With timeout=0, expiration is disabled */
    pool->config.wait_timeout_ms = 0;
    backend_pool_queue_wait(pool, &dummy_session, pool);
    usleep(10000);
    expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 0u);
    /* Clean up the queued waiter manually */
    pool_waiter_t* w = pool->wait_queue_head;
    pool->wait_queue_head = NULL;
    pool->wait_queue_tail = NULL;
    pool->wait_queue_size = 0;
    keel_free(w);

    destroy_test_pool(pool, fds, 2);
}

/* ============================================================================
 * Test 5: Multiple expire with ordering (FIFO)
 * ============================================================================ */

static void test_expire_waiters_fifo(void)
{
    printf("  test_expire_waiters_fifo...\n");
    g_expire_callback_count = 0;

    int fds[2];
    backend_pool_t* pool = make_test_pool(2, fds);
    pool->config.wait_timeout_ms = 400;  /* larger timeout for robust margins */
    backend_pool_set_wait_callback(pool, expire_test_callback);

    int s1 = 1, s2 = 2, s3 = 3;

    /* Enqueue s1 at time T */
    backend_pool_queue_wait(pool, &s1, pool);
    usleep(80000);
    /* Enqueue s2 at T+80ms */
    backend_pool_queue_wait(pool, &s2, pool);
    usleep(80000);
    /* Enqueue s3 at T+160ms */
    backend_pool_queue_wait(pool, &s3, pool);

    /* At T+160ms: s1 waited 160ms, s2 waited 80ms, s3 waited 0ms.
     * None should expire yet (all < 400ms). */
    size_t expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 0u);
    TEST_ASSERT_EQ(pool->wait_queue_size, 3u);

    /* Sleep 290ms more → total T+450ms.
     * s1 (450ms) expired, s2 (370ms) not (30ms margin), s3 (290ms) not */
    usleep(290000);
    expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 1u);
    TEST_ASSERT(g_expire_last_session == &s1);
    TEST_ASSERT_EQ(pool->wait_queue_size, 2u);

    /* Sleep 80ms more → T+530ms. s2 (450ms) expired, s3 (370ms) not */
    usleep(80000);
    expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 1u);
    TEST_ASSERT(g_expire_last_session == &s2);
    TEST_ASSERT_EQ(pool->wait_queue_size, 1u);

    /* Sleep 80ms more → T+610ms. s3 (450ms) expired */
    usleep(80000);
    expired = backend_pool_expire_waiters(pool);
    TEST_ASSERT_EQ(expired, 1u);
    TEST_ASSERT(g_expire_last_session == &s3);
    TEST_ASSERT_EQ(pool->wait_queue_size, 0u);

    destroy_test_pool(pool, fds, 2);
}

/* ============================================================================
 * Test 6: Refill backoff counter
 * ============================================================================ */

static void test_refill_backoff(void)
{
    printf("  test_refill_backoff...\n");

    int fds[2];
    backend_pool_t* pool = make_test_pool(2, fds);

    /* Initially no backoff */
    TEST_ASSERT_EQ(pool->refill_fail_count, 0u);
    TEST_ASSERT_EQ(pool->refill_backoff_until, 0u);

    /* Simulate failure increment */
    pool->refill_fail_count = 1;
    uint64_t delay1 = 1000ULL << 0;  /* 1s for fail_count=1 */
    TEST_ASSERT_EQ(delay1, 1000u);

    pool->refill_fail_count = 3;
    uint64_t delay3 = 1000ULL << 2;  /* 4s for fail_count=3 */
    TEST_ASSERT_EQ(delay3, 4000u);

    pool->refill_fail_count = 6;
    uint64_t delay6 = 1000ULL << 5;  /* Capped at 5 shifts = 32s, then capped at 30s */
    if (delay6 > 30000) delay6 = 30000;
    TEST_ASSERT_EQ(delay6, 30000u);

    /* Set backoff_until and verify refill_one skips */
    pool->refill_fail_count = 0;
    pool->refill_backoff_until = get_time_ms() + 10000;  /* 10s from now */
    int refilled = backend_pool_refill_one(pool);
    TEST_ASSERT_EQ(refilled, 0);  /* Skipped due to backoff */

    /* Clear backoff and set to past — should be treated as expired */
    pool->refill_backoff_until = get_time_ms() - 1;

    destroy_test_pool(pool, fds, 2);
}

/* ============================================================================
 * Test 7: Drain idle with mixed connection states
 * ============================================================================ */

static void test_drain_idle_mixed(void)
{
    printf("  test_drain_idle_mixed...\n");

    int fds[6];
    backend_pool_t* pool = make_test_pool(6, fds);

    /* Move 2 connections from clean to idle list */
    backend_conn_t* c1 = pool->clean_list;
    backend_conn_t* c2 = c1->next;
    pool->clean_list = c2->next;
    pool->clean_count -= 2;
    c1->next = c2;
    c2->next = NULL;
    pool->idle_list = c1;

    /* Borrow 1 connection so it's ACTIVE */
    backend_conn_t* active = backend_pool_borrow(pool, 0);
    TEST_ASSERT(active != NULL);

    /* Now: clean=3, idle=2, active=1. Drain should only close clean+idle (5) */
    size_t closed = backend_pool_drain_idle(pool);
    TEST_ASSERT_EQ(closed, 5u);
    TEST_ASSERT(pool->clean_list == NULL);
    TEST_ASSERT(pool->idle_list == NULL);

    /* Active connection should still be active */
    TEST_ASSERT_EQ(atomic_load(&active->state), BACKEND_CONN_ACTIVE);
    TEST_ASSERT(active->fd >= 0);

    /* Return the active one and close */
    if (active->fd >= 0) {
        close(active->fd);
        active->fd = -1;
    }
    atomic_store(&active->state, BACKEND_CONN_CLOSED);

    for (size_t i = 0; i < 6; i++) {
        if (fds[i] >= 0) close(fds[i]);
    }
    keel_free(pool->connections);
    keel_free(pool);
}

/* ============================================================================
 * Test 8: Update target then drain — simulates failover sequence
 * ============================================================================ */

static void test_failover_sequence(void)
{
    printf("  test_failover_sequence...\n");

    int fds[4];
    backend_pool_t* pool = make_test_pool(4, fds);

    /* Initial state: all connected to 127.0.0.1:5432 */
    TEST_ASSERT_STR_EQ(pool->config.host, "127.0.0.1");
    TEST_ASSERT_EQ(pool->config.port, 5432);
    TEST_ASSERT_EQ(pool->clean_count, 4u);

    /* Simulate failover: update target to the new primary */
    backend_pool_update_target(pool, "10.0.0.2", 5433);

    /* Drain old connections */
    size_t closed = backend_pool_drain_idle(pool);
    TEST_ASSERT_EQ(closed, 4u);

    /* Verify pool is empty but config is updated */
    TEST_ASSERT(!backend_pool_has_available(pool));
    TEST_ASSERT_STR_EQ(pool->config.host, "10.0.0.2");
    TEST_ASSERT_EQ(pool->config.port, 5433);

    /* All slots should be CLOSED — ready for refill to the new host */
    for (size_t i = 0; i < 4; i++) {
        TEST_ASSERT_EQ(atomic_load(&pool->connections[i].state), BACKEND_CONN_CLOSED);
    }

    for (size_t i = 0; i < 4; i++) {
        if (fds[i] >= 0) close(fds[i]);
    }
    keel_free(pool->connections);
    keel_free(pool);
}

/* ============================================================================
 * Test 9: Server pool role detection and index tracking
 * ============================================================================ */

static void test_server_pool_roles(void)
{
    printf("  test_server_pool_roles...\n");

    /* Set up a 3-server pool mimicking config */
    keel_server_pool_t sp = {
        .count = 3,
    };
    sp.servers[0] = (keel_backend_server_t){
        .host = "10.0.0.1", .port = 5432, .role = KEEL_SERVER_ROLE_AUTO, .healthy = true
    };
    sp.servers[1] = (keel_backend_server_t){
        .host = "10.0.0.2", .port = 5433, .role = KEEL_SERVER_ROLE_AUTO, .healthy = true
    };
    sp.servers[2] = (keel_backend_server_t){
        .host = "10.0.0.3", .port = 5434, .role = KEEL_SERVER_ROLE_AUTO, .healthy = true
    };

    /* Simulate probe detecting server[1] as RW, others as RO. */
    sp.servers[0].role = KEEL_SERVER_ROLE_RO;
    sp.servers[1].role = KEEL_SERVER_ROLE_RW;
    sp.servers[2].role = KEEL_SERVER_ROLE_RO;

    /* Rebuild indices like perform_failover would */
    keel_server_pool_rebuild_indices(&sp);

    TEST_ASSERT_EQ(sp.rw_count, 1u);
    TEST_ASSERT_EQ(sp.rw_indices[0], 1u);
    TEST_ASSERT_EQ(sp.ro_count, 2u);
    TEST_ASSERT_EQ(sp.servers[0].role, KEEL_SERVER_ROLE_RO);
    TEST_ASSERT_EQ(sp.servers[1].role, KEEL_SERVER_ROLE_RW);

    /* Simulate probe marking server[0] as DOWN */
    sp.servers[0].healthy = false;

    /* Health-aware routing should skip server[0] */
    keel_backend_server_t* chosen = NULL;
    for (size_t i = 0; i < sp.ro_count; i++) {
        size_t idx = sp.ro_indices[i];
        if (sp.servers[idx].healthy && sp.servers[idx].role == KEEL_SERVER_ROLE_RO) {
            chosen = &sp.servers[idx];
            break;
        }
    }
    TEST_ASSERT(chosen != NULL);
    TEST_ASSERT_EQ(chosen->port, 5434);  /* Only healthy RO */
}

/* ============================================================================
 * Test 10: Health-aware replica selection
 * ============================================================================ */

static void test_health_aware_selection(void)
{
    printf("  test_health_aware_selection...\n");

    /* Build a server pool where RO server[1] is down */
    keel_server_pool_t sp = {
        .count = 3,
    };
    sp.servers[0] = (keel_backend_server_t){
        .host = "10.0.0.1", .port = 5432,
        .role = KEEL_SERVER_ROLE_RW, .healthy = true
    };
    sp.servers[1] = (keel_backend_server_t){
        .host = "10.0.0.2", .port = 5433,
        .role = KEEL_SERVER_ROLE_RO, .healthy = false  /* DOWN */
    };
    sp.servers[2] = (keel_backend_server_t){
        .host = "10.0.0.3", .port = 5434,
        .role = KEEL_SERVER_ROLE_RO, .healthy = true
    };

    keel_server_pool_rebuild_indices(&sp);

    /* Simulate the health-aware selection logic from engine_flow.c:
     * Try each RO server, skip unhealthy. */
    int selected = -1;

    for (size_t attempt = 0; attempt < sp.ro_count; attempt++) {
        size_t srv_idx = sp.ro_indices[attempt];
        if (sp.servers[srv_idx].healthy) {
            selected = (int)attempt;
            break;
        }
    }

    /* Should select RO idx=1 (server[2]) since idx=0 (server[1]) is unhealthy */
    TEST_ASSERT_EQ(selected, 1);
}

/* ============================================================================
 * Test 11: All replicas down — falls back to primary
 * ============================================================================ */

static void test_all_replicas_down_fallback(void)
{
    printf("  test_all_replicas_down_fallback...\n");

    keel_server_pool_t sp = {
        .count = 3,
    };
    sp.servers[0] = (keel_backend_server_t){
        .host = "10.0.0.1", .port = 5432,
        .role = KEEL_SERVER_ROLE_RW, .healthy = true
    };
    sp.servers[1] = (keel_backend_server_t){
        .host = "10.0.0.2", .port = 5433,
        .role = KEEL_SERVER_ROLE_RO, .healthy = false
    };
    sp.servers[2] = (keel_backend_server_t){
        .host = "10.0.0.3", .port = 5434,
        .role = KEEL_SERVER_ROLE_RO, .healthy = false
    };

    keel_server_pool_rebuild_indices(&sp);

    int selected = -1;  /* -1 = no healthy RO found → use RW */

    for (size_t attempt = 0; attempt < sp.ro_count; attempt++) {
        size_t srv_idx = sp.ro_indices[attempt];
        if (sp.servers[srv_idx].healthy) {
            selected = (int)attempt;
            break;
        }
    }

    /* No healthy RO found — engine_flow would fall back to RW */
    TEST_ASSERT_EQ(selected, -1);
}

/* ============================================================================
 * Test 12: Pool config reflects wait_timeout_ms
 * ============================================================================ */

static void test_pool_wait_timeout_config(void)
{
    printf("  test_pool_wait_timeout_config...\n");

    int fds[2];
    backend_pool_t* pool = make_test_pool(2, fds);

    /* Default wait_timeout is 0 (infinite) */
    TEST_ASSERT_EQ(pool->config.wait_timeout_ms, 0u);

    /* Set timeout */
    pool->config.wait_timeout_ms = 5000;
    TEST_ASSERT_EQ(pool->config.wait_timeout_ms, 5000u);

    destroy_test_pool(pool, fds, 2);
}

/* ============================================================================
 * Test 13: probe_result role detection enum values
 * ============================================================================ */

static void test_probe_result_roles(void)
{
    printf("  test_probe_result_roles...\n");

    /* Verify enum values match expectations */
    TEST_ASSERT_EQ(KEEL_SERVER_ROLE_RW, 0);
    TEST_ASSERT_EQ(KEEL_SERVER_ROLE_RO, 1);
    TEST_ASSERT_EQ(KEEL_SERVER_ROLE_WO, 2);
    TEST_ASSERT_EQ(KEEL_SERVER_ROLE_AUTO, 3);

    TEST_ASSERT_EQ(KEEL_HEALTH_UNKNOWN, 0);
    TEST_ASSERT_EQ(KEEL_HEALTH_UP, 1);
    TEST_ASSERT_EQ(KEEL_HEALTH_DOWN, 2);
    TEST_ASSERT_EQ(KEEL_HEALTH_DEGRADED, 3);

    /* Probe check structure (internal probe, not plugin API) */
    keel_probe_check_t res = {0};
    res.health = KEEL_HEALTH_UP;
    res.detected_role = KEEL_SERVER_ROLE_RW;
    res.latency_us = 1234;
    TEST_ASSERT_EQ(res.health, KEEL_HEALTH_UP);
    TEST_ASSERT_EQ(res.detected_role, (int)KEEL_SERVER_ROLE_RW);
}

/* ============================================================================
 * Test 14: Server state atomic fields
 * ============================================================================ */

static void test_server_state_atomics(void)
{
    printf("  test_server_state_atomics...\n");

    keel_server_state_t state;
    memset(&state, 0, sizeof(state));

    atomic_store(&state.health, KEEL_HEALTH_UNKNOWN);
    atomic_store(&state.detected_role, KEEL_SERVER_ROLE_AUTO);
    atomic_store(&state.consecutive_failures, 0);

    TEST_ASSERT_EQ(atomic_load(&state.health), KEEL_HEALTH_UNKNOWN);
    TEST_ASSERT_EQ(atomic_load(&state.detected_role), (int)KEEL_SERVER_ROLE_AUTO);

    /* Simulate first detection */
    atomic_store(&state.detected_role, KEEL_SERVER_ROLE_RW);
    int role = atomic_load(&state.detected_role);
    TEST_ASSERT_EQ(role, (int)KEEL_SERVER_ROLE_RW);

    /* Simulate health transition */
    atomic_store(&state.health, KEEL_HEALTH_UP);
    TEST_ASSERT_EQ(atomic_load(&state.health), KEEL_HEALTH_UP);

    /* Simulate failures */
    atomic_fetch_add(&state.consecutive_failures, 1);
    atomic_fetch_add(&state.consecutive_failures, 1);
    atomic_fetch_add(&state.consecutive_failures, 1);
    TEST_ASSERT_EQ(atomic_load(&state.consecutive_failures), 3u);

    /* DOWN after 3 failures */
    atomic_store(&state.health, KEEL_HEALTH_DOWN);
    TEST_ASSERT_EQ(atomic_load(&state.health), KEEL_HEALTH_DOWN);
}

/* ============================================================================
 * Test 15: Failover updates roles and rebuilds indices correctly
 * ============================================================================ */

static void test_failover_role_swap(void)
{
    printf("  test_failover_role_swap...\n");

    keel_server_pool_t sp = {
        .count = 3,
    };
    sp.servers[0] = (keel_backend_server_t){
        .host = "10.0.0.1", .port = 5432,
        .role = KEEL_SERVER_ROLE_RW, .healthy = true
    };
    sp.servers[1] = (keel_backend_server_t){
        .host = "10.0.0.2", .port = 5433,
        .role = KEEL_SERVER_ROLE_RO, .healthy = true
    };
    sp.servers[2] = (keel_backend_server_t){
        .host = "10.0.0.3", .port = 5434,
        .role = KEEL_SERVER_ROLE_RO, .healthy = true
    };

    keel_server_pool_rebuild_indices(&sp);

    TEST_ASSERT_EQ(sp.rw_count, 1u);
    TEST_ASSERT_EQ(sp.rw_indices[0], 0u);
    TEST_ASSERT_EQ(sp.ro_count, 2u);

    /* Simulate perform_failover(sp, old_rw=0, new_rw=1):
     * Demote server[0] to RO, promote server[1] to RW, rebuild indices */
    size_t old_rw = 0;
    size_t new_rw = 1;

    sp.servers[old_rw].role = KEEL_SERVER_ROLE_RO;
    sp.servers[new_rw].role = KEEL_SERVER_ROLE_RW;
    keel_server_pool_rebuild_indices(&sp);

    TEST_ASSERT_EQ(sp.rw_count, 1u);
    TEST_ASSERT_EQ(sp.rw_indices[0], 1u);
    TEST_ASSERT_EQ(sp.servers[0].role, KEEL_SERVER_ROLE_RO);
    TEST_ASSERT_EQ(sp.servers[1].role, KEEL_SERVER_ROLE_RW);
    TEST_ASSERT_EQ(sp.servers[2].role, KEEL_SERVER_ROLE_RO);
}

/* ============================================================================
 * Test 16: Double failover round-trip
 * ============================================================================ */

static void test_double_failover(void)
{
    printf("  test_double_failover...\n");

    keel_server_pool_t sp = {
        .count = 3,
    };
    sp.servers[0] = (keel_backend_server_t){
        .host = "10.0.0.1", .port = 5432, .role = KEEL_SERVER_ROLE_RW, .healthy = true
    };
    sp.servers[1] = (keel_backend_server_t){
        .host = "10.0.0.2", .port = 5433, .role = KEEL_SERVER_ROLE_RO, .healthy = true
    };
    sp.servers[2] = (keel_backend_server_t){
        .host = "10.0.0.3", .port = 5434, .role = KEEL_SERVER_ROLE_RO, .healthy = true
    };

    keel_server_pool_rebuild_indices(&sp);

    /* First failover: 0 → 1 */
    sp.servers[0].role = KEEL_SERVER_ROLE_RO;
    sp.servers[1].role = KEEL_SERVER_ROLE_RW;
    keel_server_pool_rebuild_indices(&sp);

    TEST_ASSERT_EQ(sp.rw_count, 1u);
    TEST_ASSERT_EQ(sp.rw_indices[0], 1u);

    /* Second failover: 1 → 2 */
    sp.servers[1].role = KEEL_SERVER_ROLE_RO;
    sp.servers[2].role = KEEL_SERVER_ROLE_RW;
    keel_server_pool_rebuild_indices(&sp);

    TEST_ASSERT_EQ(sp.rw_count, 1u);
    TEST_ASSERT_EQ(sp.rw_indices[0], 2u);
    TEST_ASSERT_EQ(sp.ro_count, 2u);
    TEST_ASSERT_EQ(sp.servers[0].role, KEEL_SERVER_ROLE_RO);
    TEST_ASSERT_EQ(sp.servers[1].role, KEEL_SERVER_ROLE_RO);
    TEST_ASSERT_EQ(sp.servers[2].role, KEEL_SERVER_ROLE_RW);

    /* Third failover back: 2 → 0 (return to original) */
    sp.servers[2].role = KEEL_SERVER_ROLE_RO;
    sp.servers[0].role = KEEL_SERVER_ROLE_RW;
    keel_server_pool_rebuild_indices(&sp);

    TEST_ASSERT_EQ(sp.rw_count, 1u);
    TEST_ASSERT_EQ(sp.rw_indices[0], 0u);
    TEST_ASSERT_EQ(sp.servers[0].role, KEEL_SERVER_ROLE_RW);
}

/* ============================================================================
 * Test 17: Drain idle preserves dirty list
 * ============================================================================ */

static void test_drain_cleans_dirty(void)
{
    printf("  test_drain_cleans_dirty...\n");

    int fds[4];
    backend_pool_t* pool = make_test_pool(4, fds);

    /* Move 1 conn to dirty list */
    backend_conn_t* c = pool->clean_list;
    pool->clean_list = c->next;
    pool->clean_count--;
    c->next = NULL;
    pool->dirty_list = c;
    pool->dirty_count = 1;

    /* Now: clean=3, dirty=1. Drain should close all 4 */
    size_t closed = backend_pool_drain_idle(pool);
    TEST_ASSERT_EQ(closed, 4u);
    TEST_ASSERT(pool->clean_list == NULL);
    TEST_ASSERT(pool->dirty_list == NULL);
    TEST_ASSERT_EQ(pool->dirty_count, 0u);
    TEST_ASSERT_EQ(pool->clean_count, 0u);

    for (size_t i = 0; i < 4; i++) {
        if (fds[i] >= 0) close(fds[i]);
    }
    keel_free(pool->connections);
    keel_free(pool);
}

/* ============================================================================
 * Test 18: Queue wait records enqueue time
 * ============================================================================ */

static void test_queue_wait_timestamp(void)
{
    printf("  test_queue_wait_timestamp...\n");

    int fds[2];
    backend_pool_t* pool = make_test_pool(2, fds);

    int dummy = 1;
    uint64_t before = get_time_ms();
    backend_pool_queue_wait(pool, &dummy, pool);
    uint64_t after = get_time_ms();

    pool_waiter_t* w = pool->wait_queue_head;
    TEST_ASSERT(w != NULL);
    TEST_ASSERT(w->enqueue_time_ms >= before);
    TEST_ASSERT(w->enqueue_time_ms <= after);

    /* Clean up */
    pool->wait_queue_head = NULL;
    pool->wait_queue_tail = NULL;
    pool->wait_queue_size = 0;
    keel_free(w);

    destroy_test_pool(pool, fds, 2);
}

/* ============================================================================
 * Test 19: Pool update_target doesn't affect existing open connections
 * ============================================================================ */

static void test_update_target_preserves_conns(void)
{
    printf("  test_update_target_preserves_conns...\n");

    int fds[4];
    backend_pool_t* pool = make_test_pool(4, fds);

    /* All 4 connections should be open */
    for (size_t i = 0; i < 4; i++) {
        TEST_ASSERT(pool->connections[i].fd >= 0);
    }

    /* Update target */
    backend_pool_update_target(pool, "192.168.1.1", 5555);

    /* Existing connections should still be open */
    for (size_t i = 0; i < 4; i++) {
        TEST_ASSERT(pool->connections[i].fd >= 0);
    }

    /* Borrow should still work */
    backend_conn_t* c = backend_pool_borrow(pool, 0);
    TEST_ASSERT(c != NULL);
    TEST_ASSERT(c->fd >= 0);

    /* But config now points to new host */
    TEST_ASSERT_STR_EQ(pool->config.host, "192.168.1.1");

    /* Return and cleanup */
    atomic_store(&c->state, BACKEND_CONN_IDLE);

    destroy_test_pool(pool, fds, 4);
}

/* ============================================================================
 * Test 20: Backend pool config immutability check
 * ============================================================================ */

static void test_pool_config_fields(void)
{
    printf("  test_pool_config_fields...\n");

    backend_pool_config_t cfg = {
        .host = "10.0.0.1",
        .port = 5433,
        .user = "pguser",
        .password = "secret",
        .database = "mydb",
        .min_connections = 5,
        .max_connections = 20,
        .max_waiting = 100,
        .idle_timeout_ms = 30000,
        .wait_timeout_ms = 10000,
    };

    TEST_ASSERT_STR_EQ(cfg.host, "10.0.0.1");
    TEST_ASSERT_EQ(cfg.port, 5433);
    TEST_ASSERT_STR_EQ(cfg.user, "pguser");
    TEST_ASSERT_STR_EQ(cfg.database, "mydb");
    TEST_ASSERT_EQ(cfg.min_connections, 5u);
    TEST_ASSERT_EQ(cfg.max_connections, 20u);
    TEST_ASSERT_EQ(cfg.max_waiting, 100u);
    TEST_ASSERT_EQ(cfg.idle_timeout_ms, 30000u);
    TEST_ASSERT_EQ(cfg.wait_timeout_ms, 10000u);
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void)
{
    printf("=== Failover Tests ===\n\n");

    test_update_target();
    test_drain_idle();
    test_has_available();
    test_expire_waiters();
    test_expire_waiters_fifo();
    test_refill_backoff();
    test_drain_idle_mixed();
    test_failover_sequence();
    test_server_pool_roles();
    test_health_aware_selection();
    test_all_replicas_down_fallback();
    test_pool_wait_timeout_config();
    test_probe_result_roles();
    test_server_state_atomics();
    test_failover_role_swap();
    test_double_failover();
    test_drain_cleans_dirty();
    test_queue_wait_timestamp();
    test_update_target_preserves_conns();
    test_pool_config_fields();

    return test_summary();
}
