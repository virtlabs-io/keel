/**
 * @file test_conn_lifecycle.c
 * @brief Unit tests for connection lifecycle management.
 *
 * Exercises the connection lifecycle features:
 *   §1 — Idle timeout pruning: prune_idle closes expired idle connections.
 *   §2 — Max connection age: prune_aged closes connections older than max_age.
 *   §3 — Min connections respected: pruning stops at min_connections.
 *   §4 — Per-user connection limits: acquire/release tracking.
 *   §5 — Per-user limit enforcement: can_acquire returns false at limit.
 *   §6 — Edge cases: NULL pool, zero config values, empty usernames.
 *   §7 — User map capacity: fills the fixed hash map.
 *   §8 — created_at field is set on backend_conn_t.
 *
 * @author Generated for KEEL P2 roadmap
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/engine/backend_pool.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * Create a minimal pool with `n` connections for unit testing.
 * Connections are placed on clean_list in IDLE state.
 * No real file descriptors or sockets are opened.
 */
static backend_pool_t* make_test_pool(size_t n)
{
    backend_pool_t* pool = keel_calloc(1, sizeof(*pool));
    if (!pool) return NULL;

    pool->connections = keel_calloc(n, sizeof(backend_conn_t));
    if (!pool->connections) {
        keel_free(pool);
        return NULL;
    }

    pool->total_count = n;
    pool->config.min_connections = 0;  /* default: no minimum */
    pool->config.max_connections = n;
    pthread_mutex_init(&pool->lock, NULL);

    /* Wire connections onto clean_list as IDLE */
    for (size_t i = 0; i < n; i++) {
        backend_conn_t* c = &pool->connections[i];
        c->fd = -1;  /* No real fd */
        c->pool = pool;
        atomic_store(&c->state, BACKEND_CONN_IDLE);
        c->last_used = 0;
        c->created_at = 0;
        c->next = pool->clean_list;
        pool->clean_list = c;
        pool->clean_count++;
    }

    return pool;
}

static void free_test_pool(backend_pool_t* pool)
{
    if (!pool) return;
    pthread_mutex_destroy(&pool->lock);
    keel_free(pool->connections);
    keel_free(pool);
}

/**
 * Match the clock source used by backend_pool.c's get_time_ms().
 */
static uint64_t test_time_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* ============================================================================
 * §1 — Idle timeout pruning
 * ============================================================================ */

static void test_prune_idle_basic(void) {
    TEST_BEGIN("prune_idle_basic");

    backend_pool_t* pool = make_test_pool(4);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.idle_timeout_ms = 100;  /* 100ms timeout */

    /* Set last_used to "long ago" for 2 connections */
    uint64_t now = test_time_ms();
    pool->connections[0].last_used = now - 500;  /* 500ms ago — expired */
    pool->connections[1].last_used = now - 200;  /* 200ms ago — expired */
    pool->connections[2].last_used = now;         /* Just now — not expired */
    pool->connections[3].last_used = now;         /* Just now — not expired */

    size_t pruned = backend_pool_prune_idle(pool);
    TEST_ASSERT_EQ(pruned, (size_t)2);

    /* Two connections should now be CLOSED */
    int closed_count = 0;
    for (size_t i = 0; i < pool->total_count; i++) {
        if (atomic_load(&pool->connections[i].state) == BACKEND_CONN_CLOSED)
            closed_count++;
    }
    TEST_ASSERT_EQ(closed_count, 2);

    free_test_pool(pool);
    TEST_END();
}

static void test_prune_idle_disabled(void) {
    TEST_BEGIN("prune_idle_disabled");

    backend_pool_t* pool = make_test_pool(2);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.idle_timeout_ms = 0;  /* Disabled */
    pool->connections[0].last_used = 1;  /* Ancient */

    size_t pruned = backend_pool_prune_idle(pool);
    TEST_ASSERT_EQ(pruned, (size_t)0);

    /* NULL pool */
    pruned = backend_pool_prune_idle(NULL);
    TEST_ASSERT_EQ(pruned, (size_t)0);

    free_test_pool(pool);
    TEST_END();
}

/* ============================================================================
 * §2 — Max connection age pruning
 * ============================================================================ */

static void test_prune_aged_basic(void) {
    TEST_BEGIN("prune_aged_basic");

    backend_pool_t* pool = make_test_pool(4);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.max_connection_age_ms = 1000;  /* 1 second max age */

    uint64_t now = test_time_ms();
    pool->connections[0].created_at = now - 2000;  /* 2s old — aged */
    pool->connections[1].created_at = now - 1500;  /* 1.5s old — aged */
    pool->connections[2].created_at = now - 500;   /* 0.5s old — fresh */
    pool->connections[3].created_at = now;          /* Just created — fresh */

    size_t pruned = backend_pool_prune_aged(pool);
    TEST_ASSERT_EQ(pruned, (size_t)2);

    /* Verify the right connections were closed */
    TEST_ASSERT(atomic_load(&pool->connections[0].state) == BACKEND_CONN_CLOSED);
    TEST_ASSERT(atomic_load(&pool->connections[1].state) == BACKEND_CONN_CLOSED);
    TEST_ASSERT(atomic_load(&pool->connections[2].state) == BACKEND_CONN_IDLE);
    TEST_ASSERT(atomic_load(&pool->connections[3].state) == BACKEND_CONN_IDLE);

    free_test_pool(pool);
    TEST_END();
}

static void test_prune_aged_disabled(void) {
    TEST_BEGIN("prune_aged_disabled");

    backend_pool_t* pool = make_test_pool(2);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.max_connection_age_ms = 0;  /* Disabled */
    pool->connections[0].created_at = 1;  /* Ancient */

    size_t pruned = backend_pool_prune_aged(pool);
    TEST_ASSERT_EQ(pruned, (size_t)0);

    /* NULL pool */
    pruned = backend_pool_prune_aged(NULL);
    TEST_ASSERT_EQ(pruned, (size_t)0);

    free_test_pool(pool);
    TEST_END();
}

static void test_prune_aged_no_timestamp(void) {
    TEST_BEGIN("prune_aged_no_timestamp");

    backend_pool_t* pool = make_test_pool(2);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.max_connection_age_ms = 100;
    /* created_at is 0 (not set) — should not be pruned */
    pool->connections[0].created_at = 0;
    pool->connections[1].created_at = 0;

    size_t pruned = backend_pool_prune_aged(pool);
    TEST_ASSERT_EQ(pruned, (size_t)0);

    free_test_pool(pool);
    TEST_END();
}

/* ============================================================================
 * §3 — Min connections respected
 * ============================================================================ */

static void test_prune_respects_min_connections(void) {
    TEST_BEGIN("prune_respects_min_connections");

    backend_pool_t* pool = make_test_pool(4);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.min_connections = 3;  /* Keep at least 3 */
    pool->config.idle_timeout_ms = 100;
    pool->config.max_connection_age_ms = 100;

    uint64_t now = test_time_ms();
    /* All connections are expired by both idle and age */
    for (size_t i = 0; i < 4; i++) {
        pool->connections[i].last_used = now - 500;
        pool->connections[i].created_at = now - 500;
    }

    /* idle prune should close at most 1 (4 idle - 3 min = 1) */
    size_t pruned = backend_pool_prune_idle(pool);
    TEST_ASSERT_EQ(pruned, (size_t)1);

    /* age prune should not close any more (3 idle == 3 min) */
    pruned = backend_pool_prune_aged(pool);
    TEST_ASSERT_EQ(pruned, (size_t)0);

    /* Verify exactly 1 closed connection */
    int closed = 0;
    for (size_t i = 0; i < pool->total_count; i++) {
        if (atomic_load(&pool->connections[i].state) == BACKEND_CONN_CLOSED)
            closed++;
    }
    TEST_ASSERT_EQ(closed, 1);

    free_test_pool(pool);
    TEST_END();
}

/* ============================================================================
 * §4 — Per-user connection tracking
 * ============================================================================ */

static void test_user_conn_tracking(void) {
    TEST_BEGIN("user_conn_tracking");

    backend_pool_t* pool = make_test_pool(4);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.max_user_connections = 3;

    /* Initially can acquire */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "alice") == true);

    /* Acquire 3 connections for alice */
    backend_pool_user_conn_acquire(pool, "alice");
    backend_pool_user_conn_acquire(pool, "alice");
    backend_pool_user_conn_acquire(pool, "alice");

    /* At limit — can't acquire more */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "alice") == false);

    /* Bob is a different user — can still acquire */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "bob") == true);
    backend_pool_user_conn_acquire(pool, "bob");
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "bob") == true);

    /* Release one for alice → can acquire again */
    backend_pool_user_conn_release(pool, "alice");
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "alice") == true);

    free_test_pool(pool);
    TEST_END();
}

/* ============================================================================
 * §5 — Per-user limit enforcement edge cases
 * ============================================================================ */

static void test_user_conn_unlimited(void) {
    TEST_BEGIN("user_conn_unlimited");

    backend_pool_t* pool = make_test_pool(2);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.max_user_connections = 0;  /* Unlimited */

    /* Always allowed */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "alice") == true);
    backend_pool_user_conn_acquire(pool, "alice");  /* No-op when unlimited */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "alice") == true);

    free_test_pool(pool);
    TEST_END();
}

static void test_user_conn_release_underflow(void) {
    TEST_BEGIN("user_conn_release_underflow");

    backend_pool_t* pool = make_test_pool(2);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.max_user_connections = 5;

    /* Release without acquire — should not underflow */
    backend_pool_user_conn_release(pool, "charlie");
    /* charlie not in map, release is a no-op */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "charlie") == true);

    /* Acquire then release more than acquired */
    backend_pool_user_conn_acquire(pool, "charlie");
    backend_pool_user_conn_release(pool, "charlie");
    backend_pool_user_conn_release(pool, "charlie");  /* Extra release — should clamp at 0 */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "charlie") == true);

    free_test_pool(pool);
    TEST_END();
}

/* ============================================================================
 * §6 — Edge cases
 * ============================================================================ */

static void test_null_pool_safety(void) {
    TEST_BEGIN("null_pool_safety");

    /* All functions should handle NULL gracefully */
    TEST_ASSERT_EQ(backend_pool_prune_idle(NULL), (size_t)0);
    TEST_ASSERT_EQ(backend_pool_prune_aged(NULL), (size_t)0);
    TEST_ASSERT(backend_pool_user_can_acquire(NULL, "user") == true);

    /* NULL and empty user */
    backend_pool_t* pool = make_test_pool(1);
    pool->config.max_user_connections = 5;
    TEST_ASSERT(backend_pool_user_can_acquire(pool, NULL) == true);
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "") == true);

    /* Acquire/release with NULL user should not crash */
    backend_pool_user_conn_acquire(pool, NULL);
    backend_pool_user_conn_release(pool, NULL);

    free_test_pool(pool);
    TEST_END();
}

static void test_prune_active_connections_skipped(void) {
    TEST_BEGIN("prune_active_connections_skipped");

    backend_pool_t* pool = make_test_pool(2);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.idle_timeout_ms = 100;
    pool->config.max_connection_age_ms = 100;

    uint64_t now = test_time_ms();
    /* Both are old, but connection[0] is ACTIVE */
    pool->connections[0].last_used = now - 500;
    pool->connections[0].created_at = now - 500;
    atomic_store(&pool->connections[0].state, BACKEND_CONN_ACTIVE);

    /* Properly remove conn[0] from clean_list.
     * make_test_pool wires: clean_list → [1] → [0] → NULL */
    pool->connections[1].next = NULL;  /* unlink [0] from [1] */
    pool->connections[0].next = NULL;
    pool->clean_count--;

    pool->connections[1].last_used = now - 500;
    pool->connections[1].created_at = now - 500;

    /* idle prune should only close [1], not [0] */
    size_t pruned = backend_pool_prune_idle(pool);
    TEST_ASSERT_EQ(pruned, (size_t)1);

    TEST_ASSERT(atomic_load(&pool->connections[0].state) == BACKEND_CONN_ACTIVE);
    TEST_ASSERT(atomic_load(&pool->connections[1].state) == BACKEND_CONN_CLOSED);

    free_test_pool(pool);
    TEST_END();
}

/* ============================================================================
 * §7 — User map capacity
 * ============================================================================ */

static void test_user_map_capacity(void) {
    TEST_BEGIN("user_map_capacity");

    backend_pool_t* pool = make_test_pool(1);
    TEST_ASSERT_NOT_NULL(pool);

    pool->config.max_user_connections = 100;

    /* Fill the user map with distinct users */
    char name[64];
    int acquired = 0;
    for (int i = 0; i < 64; i++) {
        snprintf(name, sizeof(name), "user_%03d", i);
        backend_pool_user_conn_acquire(pool, name);
        acquired++;
    }

    /* All 64 slots should be used */
    TEST_ASSERT_EQ(pool->user_conn_map_used, (size_t)64);

    /* Existing user should still work */
    TEST_ASSERT(backend_pool_user_can_acquire(pool, "user_000") == true);

    /* Check that at least some entries are tracked */
    int found = 0;
    for (size_t i = 0; i < 64; i++) {
        if (pool->user_conn_map[i].username[0] != '\0' &&
            pool->user_conn_map[i].active_count > 0)
            found++;
    }
    TEST_ASSERT(found > 0);

    free_test_pool(pool);
    TEST_END();
}

/* ============================================================================
 * §8 — created_at field
 * ============================================================================ */

static void test_created_at_field(void) {
    TEST_BEGIN("created_at_field");

    /* Verify the field exists and can be set */
    backend_conn_t conn = {0};
    TEST_ASSERT_EQ(conn.created_at, (uint64_t)0);

    conn.created_at = 1234567890ULL;
    TEST_ASSERT_EQ(conn.created_at, (uint64_t)1234567890ULL);

    /* Verify it's distinct from last_used */
    conn.last_used = 9999ULL;
    TEST_ASSERT(conn.created_at != conn.last_used);
    TEST_ASSERT_EQ(conn.created_at, (uint64_t)1234567890ULL);

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("=== Connection Lifecycle Management Tests ===\n\n");

    /* §1 — Idle timeout */
    test_prune_idle_basic();
    test_prune_idle_disabled();

    /* §2 — Max connection age */
    test_prune_aged_basic();
    test_prune_aged_disabled();
    test_prune_aged_no_timestamp();

    /* §3 — Min connections */
    test_prune_respects_min_connections();

    /* §4 — Per-user tracking */
    test_user_conn_tracking();

    /* §5 — Per-user edge cases */
    test_user_conn_unlimited();
    test_user_conn_release_underflow();

    /* §6 — Null/edge safety */
    test_null_pool_safety();
    test_prune_active_connections_skipped();

    /* §7 — User map capacity */
    test_user_map_capacity();

    /* §8 — created_at field */
    test_created_at_field();

    printf("\n");
    return test_summary();
}
