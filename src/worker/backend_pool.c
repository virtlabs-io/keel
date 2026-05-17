/**
 * @file backend_pool.c
 * @brief Backend pool management for transaction-aware multiplexing.
 *
 * The backend pool is one of KEEL's core scaling mechanisms. It lets many
 * frontend sessions share a smaller number of server connections while still
 * respecting the cases where reuse is unsafe: active transactions, sticky state,
 * prepared statement residue, or cleanup failures.
 *
 * The guiding principle is conservative reuse. A backend is only returned to
 * the clean or idle lists once the pool has high confidence that the next
 * frontend session will observe the state it expects.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/engine/backend_pool.h"
#include "keel/engine/backend_connect.h"
#include "keel/engine/invariant.h"
#include "keel/protocol/backend_auth.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/reactor/reactor.h"
#include "keel/log/log.h"
#include "keel/util/util.h"
#include "keel/session/state_profile.h"
#include "keel/mem/mem.h"
#include "keel/core/stats.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "keel/util/platform_compat.h"

/* Debug logging - set KEEL_DEBUG to enable */
#ifdef KEEL_DEBUG
#define KEEL_DEBUG_LOG(...) KEEL_LOG_TRACE(KEEL_LOG_CAT_IO, __VA_ARGS__)
#else
#define KEEL_DEBUG_LOG(...) ((void)0)
#endif

/* Stats helpers for CLEANING gauge — pool->stats_ctx may be NULL */
#define POOL_CLEANING_INC(pool) \
    do { \
        (pool)->cleaning_count++; \
        if ((pool)->stats_ctx) \
            KEEL_STAT_GAUGE_INC((pool)->stats_ctx, backends_cleaning); \
    } while (0)

#define POOL_CLEANING_DEC(pool) \
    do { \
        bool _keel_dec_cleaning = false; \
        if ((pool)->cleaning_count > 0) { \
            (pool)->cleaning_count--; \
            _keel_dec_cleaning = true; \
        } \
        if (_keel_dec_cleaning && (pool)->stats_ctx) \
            KEEL_STAT_GAUGE_DEC((pool)->stats_ctx, backends_cleaning); \
    } while (0)

#define POOL_REUSE_FAIL_INC(pool) \
    do { \
        if ((pool)->stats_ctx) \
            KEEL_STAT_INC((pool)->stats_ctx, proxy_backend_reuse_failure_total); \
    } while (0)

#define POOL_STAT_INC(pool, field) \
    do { \
        if ((pool)->stats_ctx) \
            KEEL_STAT_INC((pool)->stats_ctx, field); \
    } while (0)

typedef enum borrow_result_code {
    BORROW_RESULT_SUCCESS = 0,
    BORROW_RESULT_FAILED_INCOMPATIBLE,
    BORROW_RESULT_FAILED_QUARANTINED,
} borrow_result_code_t;

static inline void pool_record_borrow_result(backend_pool_t* pool,
                                             borrow_result_code_t result)
{
    if (!pool || !pool->stats_ctx)
        return;
    switch (result) {
    case BORROW_RESULT_SUCCESS:
        KEEL_STAT_INC(pool->stats_ctx, backend_borrow_success);
        break;
    case BORROW_RESULT_FAILED_INCOMPATIBLE:
        KEEL_STAT_INC(pool->stats_ctx, backend_borrow_failed_incompatible);
        break;
    case BORROW_RESULT_FAILED_QUARANTINED:
        KEEL_STAT_INC(pool->stats_ctx, backend_borrow_failed_quarantined);
        break;
    }
}

static uint64_t get_time_ms(void);

static void pool_record_backend_close(backend_pool_t* pool,
                                      backend_close_reason_t reason)
{
    if (!pool || !pool->stats_ctx)
        return;

    switch (reason) {
    case BACKEND_CLOSE_REASON_DEAD_IDLE:
        KEEL_STAT_INC(pool->stats_ctx, backend_close_dead_idle);
        break;
    case BACKEND_CLOSE_REASON_CLEANUP_TIMEOUT:
        KEEL_STAT_INC(pool->stats_ctx, backend_close_cleanup_timeout);
        KEEL_STAT_INC(pool->stats_ctx, cleaning_timeout_total);
        KEEL_STAT_INC(pool->stats_ctx, discard_all_failure);
        break;
    case BACKEND_CLOSE_REASON_CLEANUP_ERROR:
        KEEL_STAT_INC(pool->stats_ctx, backend_close_cleanup_error);
        KEEL_STAT_INC(pool->stats_ctx, discard_all_failure);
        break;
    case BACKEND_CLOSE_REASON_CLIENT_DISCONNECT:
        KEEL_STAT_INC(pool->stats_ctx, backend_close_client_disconnect);
        break;
    case BACKEND_CLOSE_REASON_NONE:
    case BACKEND_CLOSE_REASON_IO_ERROR:
    case BACKEND_CLOSE_REASON_PRUNE_IDLE:
    case BACKEND_CLOSE_REASON_PRUNE_AGED:
    case BACKEND_CLOSE_REASON_DRAIN_IDLE:
        break;
    }
}

static uint64_t pool_cleanup_elapsed_ns(const backend_conn_t* conn)
{
    if (!conn || conn->cleanup_started_ms == 0)
        return 0;
    uint64_t now_ms = get_time_ms();
    if (now_ms <= conn->cleanup_started_ms)
        return 0;
    return (now_ms - conn->cleanup_started_ms) * 1000000ULL;
}

static void pool_record_cleanup_result(backend_pool_t* pool,
                                       backend_conn_t* conn,
                                       backend_cleanup_result_t result)
{
    if (!conn)
        return;

    conn->cleanup_last_result = result;
    conn->cleanup_last_duration_ns = pool_cleanup_elapsed_ns(conn);

    if (!pool || !pool->stats_ctx)
        return;

    switch (result) {
    case BACKEND_CLEANUP_RESULT_SUCCESS:
        KEEL_STAT_INC(pool->stats_ctx, cleanup_result_success);
        break;
    case BACKEND_CLEANUP_RESULT_PROTOCOL_ERROR:
        KEEL_STAT_INC(pool->stats_ctx, cleanup_result_protocol_error);
        break;
    case BACKEND_CLEANUP_RESULT_TIMEOUT:
        KEEL_STAT_INC(pool->stats_ctx, cleanup_result_timeout);
        break;
    case BACKEND_CLEANUP_RESULT_BACKEND_EOF:
        KEEL_STAT_INC(pool->stats_ctx, cleanup_result_backend_eof);
        break;
    case BACKEND_CLEANUP_RESULT_SEND_FAILURE:
        KEEL_STAT_INC(pool->stats_ctx, cleanup_result_send_failure);
        break;
    case BACKEND_CLEANUP_RESULT_NONE:
        break;
    }

    if (conn->cleanup_last_duration_ns > 0)
        KEEL_STAT_LATENCY(pool->stats_ctx, cleanup_duration_ns,
                          conn->cleanup_last_duration_ns);
}

/* ============================================================================
 * Internal Helpers
 * ============================================================================ */

/* All backend connect/startup is now async via backend_connect_async.c */

/* Cleanup timeout in milliseconds. Cleanup I/O is reactor-owned; this bounds
 * sockets that stop making progress after entering CLEANING. */
#define BACKEND_CLEANUP_TIMEOUT_MS 5000

/**
 * @brief Get current time in milliseconds (monotonic)
 */
static uint64_t get_time_ms(void) { return keel_time_now_ms(); }
static void backend_pool_cleanup_reset(backend_conn_t* conn);

static inline void backend_pool_assert_owner_invariant(const backend_conn_t* conn)
{
#ifdef KEEL_DEBUG
    if (!conn) return;
    backend_conn_state_t st = atomic_load_explicit(&((backend_conn_t*)conn)->state,
                                                   memory_order_relaxed);
    if (st == BACKEND_CONN_IDLE) {
        KEEL_ASSERT(conn->active_owner == NULL, "backend invariant: IDLE must not have active owner");
    }
    if (st == BACKEND_CONN_ACTIVE) {
        KEEL_ASSERT(conn->active_owner != NULL, "backend invariant: ACTIVE must have active owner");
    }
#else
    (void)conn;
#endif
}

static inline void backend_pool_mark_borrowed(backend_conn_t* conn)
{
    if (!conn) return;
    conn->generation++;
    conn->close_reason = BACKEND_CLOSE_REASON_NONE;
    conn->cleanup_last_result = BACKEND_CLEANUP_RESULT_NONE;
    conn->cleanup_last_duration_ns = 0;
    conn->quarantine = BACKEND_QUARANTINE_NONE;
    conn->syncing = false;
    conn->replay_active = false;
    conn->active_owner = conn;
}

bool backend_pool_can_borrow(const backend_conn_t* conn)
{
    if (!conn) return false;
    if (atomic_load_explicit(&((backend_conn_t*)conn)->state, memory_order_relaxed) != BACKEND_CONN_IDLE)
        return false;
    if (conn->pinned_session != NULL)
        return false;
    if (conn->active_owner != NULL)
        return false;
    if (conn->quarantine != BACKEND_QUARANTINE_NONE)
        return false;
    if (conn->in_transaction)
        return false;
    if (conn->syncing || conn->replay_active || conn->protocol_desync)
        return false;
    if (conn->needs_full_cleanup)
        return false;
    return true;
}

bool backend_pool_validate_generation(const backend_conn_t* conn,
                                      uint64_t expected_generation)
{
    if (!conn) return false;
    backend_conn_state_t st = atomic_load_explicit(&((backend_conn_t*)conn)->state,
                                                   memory_order_relaxed);
    if (st == BACKEND_CONN_CLOSED)
        return false;
    return conn->generation == expected_generation;
}

void backend_pool_set_quarantine(backend_conn_t* conn,
                                 backend_quarantine_reason_t reason)
{
    if (!conn) return;
    conn->quarantine = reason;
}

static void backend_pool_close_slot_locked(backend_pool_t* pool,
                                           backend_conn_t* conn,
                                           backend_close_reason_t reason,
                                           bool count_active)
{
    if (!pool || !conn) return;
    if (atomic_load(&conn->state) == BACKEND_CONN_CLOSED &&
        conn->close_reason != BACKEND_CLOSE_REASON_NONE)
        return;

    if (count_active && pool->active_count > 0)
        pool->active_count--;

    if (conn->fd >= 0) {
        close(conn->fd);
        conn->fd = -1;
    }

    conn->pinned_session = NULL;
    conn->active_owner = NULL;
    conn->in_transaction = false;
    conn->needs_sync = false;
    conn->syncing = false;
    conn->replay_active = false;
    conn->needs_full_cleanup = false;
    conn->hard_pinned = false;
    conn->protocol_desync = false;
    conn->current_state_hash = 0;
    conn->stmt_set_hash = 0;
    conn->quarantine = BACKEND_QUARANTINE_NONE;
    conn->close_reason = reason;
    conn->generation++;
    if (conn->profile)
        state_profile_clear(conn->profile);
    backend_pool_cleanup_reset(conn);
    atomic_store(&conn->state, BACKEND_CONN_CLOSED);
    pool_record_backend_close(pool, reason);
}

void backend_pool_close_connection(backend_pool_t* pool,
                                   backend_conn_t* conn,
                                   backend_close_reason_t reason)
{
    if (!pool || !conn) return;
    pthread_mutex_lock(&pool->lock);
    bool count_active = (atomic_load(&conn->state) == BACKEND_CONN_ACTIVE);
    backend_pool_close_slot_locked(pool, conn, reason, count_active);
    KEEL_CHECK_POOL_INVARIANTS(pool);
    pthread_mutex_unlock(&pool->lock);
}

static void backend_pool_cleanup_send_cb(void* userdata, int result);
static void backend_pool_cleanup_recv_cb(void* userdata, int result);

static void backend_pool_cleanup_reset(backend_conn_t* conn)
{
    if (!conn) return;
    conn->cleanup_state = BACKEND_CLEANUP_NONE;
    conn->cleanup_io_armed = false;
    conn->cleanup_send_len = 0;
    conn->cleanup_send_off = 0;
    conn->cleanup_started_ms = 0;
    memset(&conn->cleanup_drain_state, 0, sizeof(conn->cleanup_drain_state));
}

static void backend_pool_wake_one_locked(backend_pool_t* pool)
{
    if (!pool->wait_queue_head || !pool->wait_callback) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
            "pool wake_waiter: no waiters (active=%zu clean=%zu wait=%zu)",
            pool->active_count, pool->clean_count, pool->wait_queue_size);
        return;
    }

    pool_waiter_t* waiter = pool->wait_queue_head;
    pool->wait_queue_head = waiter->next;
    if (!pool->wait_queue_head)
        pool->wait_queue_tail = NULL;
    pool->wait_queue_size--;

    pool->wait_callback(waiter->session, waiter->userdata);
    if (pool->waiter_pool) {
        keel_pool_free(pool->waiter_pool, waiter);
    } else {
        keel_free(waiter);
    }
}

static void backend_pool_close_cleaning_locked(backend_pool_t* pool,
                                               backend_conn_t* conn,
                                               backend_close_reason_t reason,
                                               backend_cleanup_result_t result)
{
    pool_record_cleanup_result(pool, conn, result);
    POOL_CLEANING_DEC(pool);
    POOL_REUSE_FAIL_INC(pool);
    backend_pool_close_slot_locked(pool, conn, reason, false);
    backend_pool_assert_owner_invariant(conn);
    KEEL_CHECK_POOL_INVARIANTS(pool);
}

static void backend_pool_reclaim_clean_locked(backend_pool_t* pool,
                                              backend_conn_t* conn)
{
    pool_record_cleanup_result(pool, conn, BACKEND_CLEANUP_RESULT_SUCCESS);
    POOL_CLEANING_DEC(pool);
    backend_pool_cleanup_reset(conn);
    conn->current_state_hash = 0;
    conn->stmt_set_hash = 0;
    conn->needs_sync = false;
    conn->syncing = false;
    conn->replay_active = false;
    conn->needs_full_cleanup = false;
    conn->protocol_desync = false;
    conn->quarantine = BACKEND_QUARANTINE_NONE;
    conn->close_reason = BACKEND_CLOSE_REASON_NONE;
    conn->pinned_session = NULL;
    conn->active_owner = NULL;
    conn->in_transaction = false;
    conn->hard_pinned = false;
    conn->last_used = get_time_ms();
    if (conn->profile)
        state_profile_clear(conn->profile);

    atomic_store(&conn->state, BACKEND_CONN_IDLE);
    backend_pool_assert_owner_invariant(conn);
    conn->next = pool->clean_list;
    pool->clean_list = conn;
    pool->clean_count++;

    KEEL_CHECK_POOL_INVARIANTS(pool);
    backend_pool_wake_one_locked(pool);
    KEEL_CHECK_POOL_INVARIANTS(pool);
}

static bool backend_pool_arm_cleanup_recv_locked(backend_pool_t* pool,
                                                 backend_conn_t* conn);

static bool backend_pool_prepare_cleanup_locked(backend_pool_t* pool,
                                                backend_conn_t* conn)
{
    if (!pool || !conn || !pool->flow_vt)
        return false;

    keel_cleanup_opts_t opts = {
        .mode = KEEL_CLEANUP_FULL,
        .timeout_ms = BACKEND_CLEANUP_TIMEOUT_MS,
    };

    ssize_t n = -1;
    if (pool->flow_vt->cleanup_slot) {
        n = pool->flow_vt->cleanup_slot(NULL, conn->fd, NULL, opts,
                                        conn->cleanup_send_buf,
                                        sizeof(conn->cleanup_send_buf));
    } else if (pool->flow_vt->build_cleanup) {
        n = pool->flow_vt->build_cleanup(NULL, KEEL_CLEANUP_UNKNOWN_STATE,
                                         conn->cleanup_send_buf,
                                         sizeof(conn->cleanup_send_buf));
    }

    if (n <= 0 || !pool->flow_vt->drain_cleanup_response)
        return false;

    conn->cleanup_send_len = (size_t)n;
    conn->cleanup_send_off = 0;
    memset(&conn->cleanup_drain_state, 0, sizeof(conn->cleanup_drain_state));
    return true;
}

static bool backend_pool_arm_cleanup_send_locked(backend_pool_t* pool,
                                                 backend_conn_t* conn)
{
    if (!pool->reactor || conn->fd < 0) {
        backend_pool_close_cleaning_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                           BACKEND_CLEANUP_RESULT_SEND_FAILURE);
        return false;
    }

    size_t remaining = conn->cleanup_send_len - conn->cleanup_send_off;
    if (remaining == 0)
        return backend_pool_arm_cleanup_recv_locked(pool, conn);

    conn->cleanup_state = BACKEND_CLEANUP_SEND;
    conn->cleanup_io_armed = true;
    int rc = keel_reactor_send(pool->reactor, conn->fd,
                               conn->cleanup_send_buf + conn->cleanup_send_off,
                               remaining, MSG_NOSIGNAL,
                               conn, backend_pool_cleanup_send_cb);
    if (rc < 0) {
        conn->cleanup_io_armed = false;
        backend_pool_close_cleaning_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                           BACKEND_CLEANUP_RESULT_SEND_FAILURE);
        return false;
    }
    return true;
}

static bool backend_pool_arm_cleanup_recv_locked(backend_pool_t* pool,
                                                 backend_conn_t* conn)
{
    if (!pool->reactor || conn->fd < 0) {
        backend_pool_close_cleaning_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                           BACKEND_CLEANUP_RESULT_SEND_FAILURE);
        return false;
    }

    conn->cleanup_state = BACKEND_CLEANUP_DRAIN;
    conn->cleanup_io_armed = true;
    int rc = keel_reactor_recv(pool->reactor, conn->fd,
                               conn->cleanup_recv_buf,
                               sizeof(conn->cleanup_recv_buf),
                               0, conn, backend_pool_cleanup_recv_cb);
    if (rc < 0) {
        conn->cleanup_io_armed = false;
        backend_pool_close_cleaning_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                           BACKEND_CLEANUP_RESULT_SEND_FAILURE);
        return false;
    }
    return true;
}

static void backend_pool_enter_cleanup_locked(backend_pool_t* pool,
                                              backend_conn_t* conn)
{
    conn->next = NULL;
    backend_pool_cleanup_reset(conn);
    conn->cleanup_state = BACKEND_CLEANUP_SEND;
    conn->cleanup_started_ms = get_time_ms();
    conn->last_used = conn->cleanup_started_ms;
    if (!backend_pool_prepare_cleanup_locked(pool, conn)) {
        backend_pool_close_cleaning_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                           BACKEND_CLEANUP_RESULT_SEND_FAILURE);
        KEEL_CHECK_POOL_INVARIANTS(pool);
        return;
    }
    POOL_CLEANING_INC(pool);
    backend_pool_arm_cleanup_send_locked(pool, conn);
    KEEL_CHECK_POOL_INVARIANTS(pool);
}

static bool backend_pool_kick_dirty_cleanups_locked(backend_pool_t* pool)
{
    bool kicked = false;
    backend_conn_t** prev = &pool->dirty_list;
    backend_conn_t* conn = pool->dirty_list;

    while (conn) {
        backend_conn_t* next = conn->next;
        backend_conn_state_t expected = BACKEND_CONN_IDLE;
        if (atomic_compare_exchange_strong(&conn->state,
                                           &expected,
                                           BACKEND_CONN_CLEANING)) {
            *prev = next;
            if (pool->dirty_count > 0)
                pool->dirty_count--;
            backend_pool_enter_cleanup_locked(pool, conn);
            kicked = true;
            conn = *prev;
            continue;
        }

        prev = &conn->next;
        conn = next;
    }

    KEEL_CHECK_POOL_INVARIANTS(pool);
    return kicked;
}

static void backend_pool_cleanup_send_cb(void* userdata, int result)
{
    backend_conn_t* conn = (backend_conn_t*)userdata;
    if (!conn || !conn->pool) return;
    backend_pool_t* pool = conn->pool;

    pthread_mutex_lock(&pool->lock);
    if (atomic_load(&conn->state) != BACKEND_CONN_CLEANING ||
        conn->cleanup_state != BACKEND_CLEANUP_SEND ||
        !conn->cleanup_io_armed) {
        pthread_mutex_unlock(&pool->lock);
        return;
    }

    conn->cleanup_io_armed = false;
    if (result <= 0) {
        backend_pool_close_cleaning_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                           BACKEND_CLEANUP_RESULT_SEND_FAILURE);
        pthread_mutex_unlock(&pool->lock);
        return;
    }

    size_t remaining = conn->cleanup_send_len - conn->cleanup_send_off;
    if ((size_t)result > remaining) {
        backend_pool_close_cleaning_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                           BACKEND_CLEANUP_RESULT_PROTOCOL_ERROR);
        pthread_mutex_unlock(&pool->lock);
        return;
    }

    conn->cleanup_send_off += (size_t)result;
    if (conn->cleanup_send_off < conn->cleanup_send_len) {
        backend_pool_arm_cleanup_send_locked(pool, conn);
    } else {
        backend_pool_arm_cleanup_recv_locked(pool, conn);
    }
    pthread_mutex_unlock(&pool->lock);
}

static void backend_pool_cleanup_recv_cb(void* userdata, int result)
{
    backend_conn_t* conn = (backend_conn_t*)userdata;
    if (!conn || !conn->pool) return;
    backend_pool_t* pool = conn->pool;

    pthread_mutex_lock(&pool->lock);
    if (atomic_load(&conn->state) != BACKEND_CONN_CLEANING ||
        conn->cleanup_state != BACKEND_CLEANUP_DRAIN ||
        !conn->cleanup_io_armed) {
        pthread_mutex_unlock(&pool->lock);
        return;
    }

    conn->cleanup_io_armed = false;
    if (result <= 0) {
        backend_pool_close_cleaning_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                           BACKEND_CLEANUP_RESULT_BACKEND_EOF);
        pthread_mutex_unlock(&pool->lock);
        return;
    }

    if (!pool->flow_vt || !pool->flow_vt->drain_cleanup_response) {
        backend_pool_close_cleaning_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                           BACKEND_CLEANUP_RESULT_PROTOCOL_ERROR);
        pthread_mutex_unlock(&pool->lock);
        return;
    }

    size_t consumed = 0;
    keel_proto_drain_result_t gate = pool->flow_vt->drain_cleanup_response(
        NULL, &conn->cleanup_drain_state, conn->cleanup_recv_buf,
        (size_t)result, &consumed);
    if (gate == KEEL_PROTO_DRAIN_COMPLETE && consumed == (size_t)result) {
        backend_pool_reclaim_clean_locked(pool, conn);
    } else if (gate == KEEL_PROTO_DRAIN_ERROR ||
               (gate == KEEL_PROTO_DRAIN_COMPLETE && consumed != (size_t)result)) {
        backend_pool_close_cleaning_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                           BACKEND_CLEANUP_RESULT_PROTOCOL_ERROR);
    } else {
        backend_pool_arm_cleanup_recv_locked(pool, conn);
    }
    pthread_mutex_unlock(&pool->lock);
}

/* ============================================================================
 * Pool Creation and Destruction
 * ============================================================================ */

/**
 * @brief Allocate and initialize a backend pool.
 *
 * Besides basic allocation, creation also resolves the backend address eagerly
 * so the later async connect path never performs blocking DNS work on a worker
 * thread.
 *
 * @param config Pool configuration.
 * @return Newly allocated pool, or `NULL` on failure.
 */
backend_pool_t* backend_pool_create(const backend_pool_config_t* config)
{
    backend_pool_t* pool = keel_calloc(1, sizeof(backend_pool_t));
    if (!pool) return NULL;
    
    pool->config = *config;
    {
        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
        pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
        pthread_mutex_init(&pool->lock, &attr);
        pthread_mutexattr_destroy(&attr);
    }
    
    /* Resolve protocol flow vtable — eliminates strcmp branching */
    if (config->protocol) {
        pool->flow_vt = keel_proto_flow_get(config->protocol);
    }
    
    pool->clean_list = NULL;
    pool->idle_list = NULL;
    pool->dirty_list = NULL;
    pool->active_count = 0;
    pool->total_count = 0;
    pool->clean_count = 0;
    pool->dirty_count = 0;
    
    /* Initialize waiting queue */
    pool->wait_queue_head = NULL;
    pool->wait_queue_tail = NULL;
    pool->wait_queue_size = 0;

    /* Create waiter pool — eliminates per-wait calloc/free on the hot path.
     * Each pool_waiter_t is only 24 bytes; pre-allocate 256, grow to max_waiting. */
    {
        keel_pool_config_t wcfg = {
            .object_size   = sizeof(pool_waiter_t),
            .object_align  = 0,
            .initial_count = 256,
            .max_count     = config->max_waiting,
            .zero_on_alloc = true,
        };
        pool->waiter_pool = keel_pool_create(&wcfg);
        /* Non-fatal: fall back to malloc if pool creation fails */
    }
    
    /* Pre-allocate connection slots up to max_connections.
     * All slots start as CLOSED; pre-connect populates min_connections. */
    pool->connections = keel_calloc(config->max_connections, sizeof(backend_conn_t));
    if (!pool->connections) {
        keel_free(pool);
        return NULL;
    }
    
    /* Initialize ALL slots as CLOSED with back-pointer and fd=-1 */
    for (size_t i = 0; i < config->max_connections; i++) {
        pool->connections[i].fd = -1;
        atomic_store(&pool->connections[i].state, BACKEND_CONN_CLOSED);
        pool->connections[i].pool = pool;
    }
    pool->total_count = config->max_connections;
    
    /* All slots start CLOSED.  Warming min_connections is deferred to
     * backend_pool_async_warmup() after the reactor is wired in, so all
     * pool connects go through the fully-async io_uring path — no poll(). */

    /* Resolve the backend hostname once at creation time so that the reactor
     * path (backend_async_start) never needs to call blocking getaddrinfo(). */
    memset(&pool->resolved_addr, 0, sizeof(pool->resolved_addr));
    pool->addr_resolved = false;
    {
        struct addrinfo hints;
        struct addrinfo* res = NULL;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        char portstr[8];
        snprintf(portstr, sizeof(portstr), "%u", (unsigned)config->port);
        int gai_rc = getaddrinfo(config->host, portstr, &hints, &res);
        if (gai_rc == 0 && res != NULL) {
            memcpy(&pool->resolved_addr, res->ai_addr, sizeof(pool->resolved_addr));
            pool->addr_resolved = true;
            freeaddrinfo(res);
        } else {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "Pool: failed to resolve backend host '%s:%u': %s",
                config->host, config->port, gai_strerror(gai_rc));
        }
    }

    return pool;
}

/**
 * @brief Destroy a backend pool and release all resources.
 *
 * Closes every open backend socket, drains the wait queue, frees the
 * waiter pool, and frees the connection array and pool struct itself.
 *
 * @param pool Pool to destroy. No-op if `NULL`.
 */
void backend_pool_destroy(backend_pool_t* pool)
{
    if (!pool) return;
    
    pthread_mutex_destroy(&pool->lock);
    
    /* Drain and free any remaining waiters */
    while (pool->wait_queue_head) {
        pool_waiter_t* w = pool->wait_queue_head;
        pool->wait_queue_head = w->next;
        if (pool->waiter_pool) {
            keel_pool_free(pool->waiter_pool, w);
        } else {
            keel_free(w);
        }
    }

    /* Destroy waiter pool */
    if (pool->waiter_pool) {
        keel_pool_destroy(pool->waiter_pool);
        pool->waiter_pool = NULL;
    }

    /* Close all connections */
    for (size_t i = 0; i < pool->total_count; i++) {
        if (pool->connections[i].fd >= 0) {
            close(pool->connections[i].fd);
        }
    }
    
    keel_free(pool->connections);
    keel_free(pool);
}

/* ============================================================================
 * Connection Borrowing
 * ============================================================================ */

/**
 * @brief Non-blocking TCP liveness check for an idle backend connection.
 *
 * Uses MSG_PEEK | MSG_DONTWAIT to test the socket without consuming data:
 *   > 0  — unexpected data waiting (backend sent something uninvited) → stale
 *   = 0  — EOF, backend closed the connection → stale
 *   < 0, errno EAGAIN/EWOULDBLOCK — no data, connection is alive → OK
 *   < 0, other errno — I/O error → stale
 *
 * Returns true if the connection appears alive, false if it should be discarded.
 */
static inline bool conn_is_alive(int fd)
{
    if (fd < 0) return false;
    uint8_t peek;
    ssize_t r = recv(fd, &peek, 1, MSG_PEEK | MSG_DONTWAIT);
    if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return true;   /* no data ready — connection is alive */
    return false;      /* EOF (r==0), error, or unexpected data */
}

/**
 * @brief Borrow the best available backend connection for one frontend session.
 *
 * The selection order is deliberate:
 * 1. exact clean connection when no state is required,
 * 2. exact state-hash match from idle connections,
 * 3. any idle connection, marked for synchronization if needed.
 *
 * That ordering maximizes reuse while minimizing unnecessary cleanup and state
 * replay.
 *
 * @param pool Pool to borrow from.
 * @param required_state_hash Desired backend state fingerprint.
 * @return Borrowed connection, or `NULL` if none are immediately available.
 */
backend_conn_t* backend_pool_borrow(backend_pool_t* pool, uint64_t required_state_hash)
{
    POOL_STAT_INC(pool, pool_borrow_attempts);
    pthread_mutex_lock(&pool->lock);
    /* First, try clean_list — connections with no state (hash == 0) */
    if (required_state_hash == 0) {
        backend_conn_t** prev_c = &pool->clean_list;
        backend_conn_t* conn = pool->clean_list;
        while (conn) {
            if (!backend_pool_can_borrow(conn)) {
                pool_record_borrow_result(pool,
                    conn->quarantine != BACKEND_QUARANTINE_NONE
                        ? BORROW_RESULT_FAILED_QUARANTINED
                        : BORROW_RESULT_FAILED_INCOMPATIBLE);
                prev_c = &conn->next;
                conn = conn->next;
                continue;
            }
            backend_conn_state_t expected = BACKEND_CONN_IDLE;
            if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
                *prev_c = conn->next;
                conn->next = NULL;
                pool->clean_count--;
                /* Discard connections that died while sitting in the pool
                 * (e.g. backend restarted within the probe interval). */
                if (!conn_is_alive(conn->fd)) {
                    POOL_REUSE_FAIL_INC(pool);
                    backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DEAD_IDLE, false);
                    prev_c = &pool->clean_list;
                    conn   = pool->clean_list;
                    continue;
                }
                backend_pool_mark_borrowed(conn);
                conn->needs_sync = false;
                POOL_STAT_INC(pool, pool_borrow_exact_state_match);
                pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                pool->active_count++;
                pthread_mutex_unlock(&pool->lock);
                return conn;
            }
            prev_c = &conn->next;
            conn = conn->next;
        }
    }

    /* Next, try to find a connection with matching state hash on idle_list */
    backend_conn_t** prev = &pool->idle_list;
    backend_conn_t* conn = pool->idle_list;
    
    while (conn) {
        if (!backend_pool_can_borrow(conn)) {
            pool_record_borrow_result(pool,
                conn->quarantine != BACKEND_QUARANTINE_NONE
                    ? BORROW_RESULT_FAILED_QUARANTINED
                    : BORROW_RESULT_FAILED_INCOMPATIBLE);
            prev = &conn->next;
            conn = conn->next;
            continue;
        }
        if (conn->current_state_hash == required_state_hash) {
            /* CAS: IDLE → ACTIVE prevents double-borrow race */
            backend_conn_state_t expected = BACKEND_CONN_IDLE;
            if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
                /* Won the CAS — safe to remove from list and use */
                *prev = conn->next;
                conn->next = NULL;
                if (!conn_is_alive(conn->fd)) {
                    POOL_REUSE_FAIL_INC(pool);
                    backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DEAD_IDLE, false);
                    prev = &pool->idle_list;
                    conn  = pool->idle_list;
                    continue;
                }
                backend_pool_mark_borrowed(conn);
                conn->needs_sync = false;
                POOL_STAT_INC(pool, pool_borrow_exact_state_match);
                pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                pool->active_count++;
                pthread_mutex_unlock(&pool->lock);
                return conn;
            }
            /* Lost race — someone else borrowed it, skip */
        }
        prev = &conn->next;
        conn = conn->next;
    }
    
    /* No exact match - take any idle connection */
    conn = pool->idle_list;
    prev = &pool->idle_list;
    while (conn) {
        if (!backend_pool_can_borrow(conn)) {
            pool_record_borrow_result(pool,
                conn->quarantine != BACKEND_QUARANTINE_NONE
                    ? BORROW_RESULT_FAILED_QUARANTINED
                    : BORROW_RESULT_FAILED_INCOMPATIBLE);
            prev = &conn->next;
            conn = conn->next;
            continue;
        }
        backend_conn_state_t expected = BACKEND_CONN_IDLE;
        if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
            /* Won the CAS */
            *prev = conn->next;
            conn->next = NULL;
            if (!conn_is_alive(conn->fd)) {
                POOL_REUSE_FAIL_INC(pool);
                backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DEAD_IDLE, false);
                prev = &pool->idle_list;
                conn  = pool->idle_list;
                continue;
            }
            backend_pool_mark_borrowed(conn);
            conn->needs_sync = (conn->current_state_hash != required_state_hash);
            if (conn->needs_sync)
                POOL_STAT_INC(pool, pool_borrow_state_replay);
            /* If this backend has named prepared statements from a different
             * session, the engine must run full protocol cleanup before the
             * new session can use it. */
            if (conn->stmt_set_hash != 0) {
                conn->needs_full_cleanup = true;
                conn->stmt_set_hash = 0;
                POOL_STAT_INC(pool, pool_borrow_cleanup_required);
            }
            pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
            pool->active_count++;
            pthread_mutex_unlock(&pool->lock);
            return conn;
        }
        prev = &conn->next;
        conn = conn->next;
    }
    
    /* Fallback: try clean_list even for non-zero state hash (needs sync but avoids new connection) */
    if (required_state_hash != 0) {
        backend_conn_t** prev_c = &pool->clean_list;
        conn = pool->clean_list;
        while (conn) {
            if (!backend_pool_can_borrow(conn)) {
                pool_record_borrow_result(pool,
                    conn->quarantine != BACKEND_QUARANTINE_NONE
                        ? BORROW_RESULT_FAILED_QUARANTINED
                        : BORROW_RESULT_FAILED_INCOMPATIBLE);
                prev_c = &conn->next;
                conn = conn->next;
                continue;
            }
            backend_conn_state_t expected = BACKEND_CONN_IDLE;
            if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
                *prev_c = conn->next;
                conn->next = NULL;
                pool->clean_count--;
                if (!conn_is_alive(conn->fd)) {
                    POOL_REUSE_FAIL_INC(pool);
                    backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DEAD_IDLE, false);
                    prev_c = &pool->clean_list;
                    conn   = pool->clean_list;
                    continue;
                }
                backend_pool_mark_borrowed(conn);
                conn->needs_sync = true;
                POOL_STAT_INC(pool, pool_borrow_state_replay);
                pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                pool->active_count++;
                pthread_mutex_unlock(&pool->lock);
                return conn;
            }
            prev_c = &conn->next;
            conn = conn->next;
        }
    }

    /* Dirty connections are not borrowable. Kick reactor-owned cleanup and
     * make the caller wait for a plugin-confirmed clean return. */
    backend_pool_kick_dirty_cleanups_locked(pool);

    /* No idle connections - rely on async refill timer to reconnect closed slots.
     * We no longer do synchronous connect here because it blocks the reactor thread.
     * The caller will queue the session in the wait queue, and the refill timer
     * (100ms interval) will reconnect closed slots asynchronously and wake waiters. */
    
    /* All connections busy - caller must queue */
    pthread_mutex_unlock(&pool->lock);
    return NULL;
}

/* ============================================================================
 * Prepared-Statement-Aware Borrowing (Spec §17)
 * ============================================================================ */

/**
 * @brief Borrow a backend connection with prepared-statement awareness.
 *
 * Extends `backend_pool_borrow` with a four-step selection strategy that
     * minimises unnecessary full-cleanup round trips when named prepared
 * statements are in use:
 *   1. Exact `stmt_set_hash` match — no replay required.
 *   2. Statement-clean connection (`stmt_set_hash == 0`) — replay safe.
 *   3. Dirty connection — start reactor-owned cleanup and wait.
 *   4. Any idle connection with a differing hash — marked for full cleanup
 *      by the engine before replay.
 *
 * @param pool Pool to borrow from.
 * @param required_state_hash Desired session-state fingerprint.
 * @param required_stmt_hash Fingerprint of the prepared-statement set.
 * @param out_needs_replay Set to `true` if the caller must replay `Parse`
 *        messages before forwarding application traffic.
 * @return Borrowed connection, or `NULL` if none are immediately available.
 */
backend_conn_t* backend_pool_borrow_with_stmts(backend_pool_t* pool,
                                                uint64_t required_state_hash,
                                                uint64_t required_stmt_hash,
                                                bool* out_needs_replay)
{
    bool replay_dummy = false;
    if (!out_needs_replay)
        out_needs_replay = &replay_dummy;
    *out_needs_replay = false;
    POOL_STAT_INC(pool, pool_borrow_attempts);
    pthread_mutex_lock(&pool->lock);

    /* Step 1: Prefer a connection with an exact stmt_set_hash match.
     * That means the backend already has our prepared statements —
     * no replay needed. */
    if (required_stmt_hash != 0) {
        backend_conn_t** prev = &pool->idle_list;
        backend_conn_t*  conn = pool->idle_list;
        while (conn) {
            if (!backend_pool_can_borrow(conn)) {
                pool_record_borrow_result(pool,
                    conn->quarantine != BACKEND_QUARANTINE_NONE
                        ? BORROW_RESULT_FAILED_QUARANTINED
                        : BORROW_RESULT_FAILED_INCOMPATIBLE);
                prev = &conn->next;
                conn = conn->next;
                continue;
            }
            if (conn->stmt_set_hash == required_stmt_hash) {
                backend_conn_state_t expected = BACKEND_CONN_IDLE;
                if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
                    *prev = conn->next;
                    conn->next = NULL;
                    if (!conn_is_alive(conn->fd)) {
                        POOL_REUSE_FAIL_INC(pool);
                        backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DEAD_IDLE, false);
                        prev = &pool->idle_list;
                        conn  = pool->idle_list;
                        continue;
                    }
                    backend_pool_mark_borrowed(conn);
                    conn->needs_sync = (conn->current_state_hash != required_state_hash);
                    if (conn->needs_sync)
                        POOL_STAT_INC(pool, pool_borrow_state_replay);
                    else
                        POOL_STAT_INC(pool, pool_borrow_exact_state_match);
                    POOL_STAT_INC(pool, pool_borrow_exact_stmt_match);
                    pool->active_count++;
                    pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                    *out_needs_replay = false;   /* stmts already present */
                    pthread_mutex_unlock(&pool->lock);
                    return conn;
                }
            }
            prev = &conn->next;
            conn = conn->next;
        }
    }

    /* Step 2: Fall back to a stmt-CLEAN connection (stmt_set_hash == 0).
     *
     * Falling back to ANY idle connection risks grabbing a backend that
     * has prepared statements from a DIFFERENT session.  Because all
     * sysbench (and many real-world) clients use the same statement names
     * ("sbstmt1", "sbstmt2", …), the replay would send Parse("sbstmtN") to
     * a backend that already has "sbstmtN", causing a protocol error and
     * backend recovery behavior that belongs to the database plugin.
     *
     * Only replay onto backends with no existing prepared statements
     * (stmt_set_hash == 0).  Backends from idle_list that were built up by
     * a different session lineage are left alone; they will be recycled when
     * that session lineage naturally finishes.
     *
     * If no stmt-clean backend is available the caller queues the session;
     * the pool refill timer will create new connections or a returning
     * backend after its session runs full cleanup will wake a waiter. */

    /* First try clean_list (no state, no stmts). */
    {
        backend_conn_t** prev_c = &pool->clean_list;
        backend_conn_t*  conn   = pool->clean_list;
        while (conn) {
            if (!backend_pool_can_borrow(conn)) {
                pool_record_borrow_result(pool,
                    conn->quarantine != BACKEND_QUARANTINE_NONE
                        ? BORROW_RESULT_FAILED_QUARANTINED
                        : BORROW_RESULT_FAILED_INCOMPATIBLE);
                prev_c = &conn->next;
                conn = conn->next;
                continue;
            }
            backend_conn_state_t expected = BACKEND_CONN_IDLE;
            if (atomic_compare_exchange_strong(&conn->state,
                                               &expected,
                                               BACKEND_CONN_ACTIVE)) {
                *prev_c = conn->next;
                conn->next = NULL;
                pool->clean_count--;
                if (!conn_is_alive(conn->fd)) {
                    POOL_REUSE_FAIL_INC(pool);
                    backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DEAD_IDLE, false);
                    prev_c = &pool->clean_list;
                    conn   = pool->clean_list;
                    continue;
                }
                backend_pool_mark_borrowed(conn);
                conn->needs_sync = (required_state_hash != 0);
                if (conn->needs_sync)
                    POOL_STAT_INC(pool, pool_borrow_state_replay);
                else
                    POOL_STAT_INC(pool, pool_borrow_exact_state_match);
                pool->active_count++;
                *out_needs_replay = (required_stmt_hash != 0);
                if (*out_needs_replay)
                    POOL_STAT_INC(pool, pool_borrow_stmt_replay);
                pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                pthread_mutex_unlock(&pool->lock);
                return conn;
            }
            prev_c = &conn->next;
            conn   = conn->next;
        }
    }

    /* Then try idle_list entries that are stmt-clean (stmt_set_hash == 0).
     * These backends may have session-state (SET vars) but no named stmts,
     * so replay is safe on them. */
    {
        backend_conn_t** prev = &pool->idle_list;
        backend_conn_t*  conn = pool->idle_list;
        while (conn) {
            if (!backend_pool_can_borrow(conn)) {
                pool_record_borrow_result(pool,
                    conn->quarantine != BACKEND_QUARANTINE_NONE
                        ? BORROW_RESULT_FAILED_QUARANTINED
                        : BORROW_RESULT_FAILED_INCOMPATIBLE);
                prev = &conn->next;
                conn = conn->next;
                continue;
            }
            if (conn->stmt_set_hash == 0) {
                backend_conn_state_t expected = BACKEND_CONN_IDLE;
                if (atomic_compare_exchange_strong(&conn->state,
                                                   &expected,
                                                   BACKEND_CONN_ACTIVE)) {
                    *prev = conn->next;
                    conn->next = NULL;
                    if (!conn_is_alive(conn->fd)) {
                        POOL_REUSE_FAIL_INC(pool);
                        backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DEAD_IDLE, false);
                        prev = &pool->idle_list;
                        conn  = pool->idle_list;
                        continue;
                    }
                    pool->active_count++;
                    backend_pool_mark_borrowed(conn);
                    conn->needs_sync = (conn->current_state_hash != required_state_hash);
                    if (conn->needs_sync)
                        POOL_STAT_INC(pool, pool_borrow_state_replay);
                    else
                        POOL_STAT_INC(pool, pool_borrow_exact_state_match);
                    *out_needs_replay = (required_stmt_hash != 0);
                    if (*out_needs_replay)
                        POOL_STAT_INC(pool, pool_borrow_stmt_replay);
                    pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                    pthread_mutex_unlock(&pool->lock);
                    return conn;
                }
            }
            prev = &conn->next;
            conn = conn->next;
        }
    }

    /* Step 3: dirty_list entries require reactor-owned cleanup before any
     * replay can be considered safe.  Start cleanup and let the waiter retry
     * after the plugin validates the reusable boundary. */
    backend_pool_kick_dirty_cleanups_locked(pool);

    /* Step 4: Last resort — grab any idle backend with a non-zero stmt hash.
     *
     * Two cases:
     *
     * (a) required_stmt_hash != 0: session has stmts but NO idle backend
     *     matches exactly and there are no clean backends.  Grab a backend
     *     with a DIFFERENT stmt hash, mark it needs_full_cleanup so the
     *     engine runs full cleanup asynchronously before replaying statements.
     *     We skip backends whose hash equals required_stmt_hash — Step 1
     *     already handles those, and replaying onto a backend that already
     *     has the right set would produce "already exists" errors.
     *
     * (b) required_stmt_hash == 0: session has NO stmts (first borrow) but
     *     there are no clean backends (Step 2 found nothing) and the pool
     *     is at capacity.  ALL backends in idle_list have stmts from other
     *     sessions.  We must reclaim one: grab any stmted backend, mark it
     *     needs_full_cleanup; the engine will run full cleanup asynchronously
     *     before forwarding the session's pending message.  This avoids
     *     a 10-second timeout when the pool is fully occupied by stmted
     *     connections and new sessions can't get a clean backend. */

    {
        backend_conn_t** prev = &pool->idle_list;
        backend_conn_t*  conn = pool->idle_list;
        while (conn) {
            if (!backend_pool_can_borrow(conn)) {
                pool_record_borrow_result(pool,
                    conn->quarantine != BACKEND_QUARANTINE_NONE
                        ? BORROW_RESULT_FAILED_QUARANTINED
                        : BORROW_RESULT_FAILED_INCOMPATIBLE);
                prev = &conn->next;
                conn = conn->next;
                continue;
            }
            if (conn->stmt_set_hash != 0 &&
                conn->stmt_set_hash != required_stmt_hash) {
                backend_conn_state_t expected = BACKEND_CONN_IDLE;
                if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
                    *prev = conn->next;
                    conn->next = NULL;
                    if (!conn_is_alive(conn->fd)) {
                        POOL_REUSE_FAIL_INC(pool);
                        backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DEAD_IDLE, false);
                        prev = &pool->idle_list;
                        conn  = pool->idle_list;
                        continue;
                    }
                    pool->active_count++;
                    backend_pool_mark_borrowed(conn);
                    /* Clear stale stmt hash; engine will clean before replay */
                    conn->stmt_set_hash      = 0;
                    conn->needs_full_cleanup  = true;
                    POOL_STAT_INC(pool, pool_borrow_cleanup_required);
                    *out_needs_replay        = (required_stmt_hash != 0);
                    if (*out_needs_replay)
                        POOL_STAT_INC(pool, pool_borrow_stmt_replay);
                    pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                    pthread_mutex_unlock(&pool->lock);
                    return conn;
                }
            }
            prev = &conn->next;
            conn = conn->next;
        }
    }

    /* No backend available — caller must queue the session. */
    pthread_mutex_unlock(&pool->lock);
    return NULL;
}

/**
 * @brief Borrow a backend connection and pin it to a specific session.
 *
 * If `session` already owns a pinned connection that connection is returned
 * directly, preserving the transaction boundary across multiple calls.
 * Otherwise a new connection is borrowed via `backend_pool_borrow` and
 * pinned to `session`.
 *
 * Admission control: if `max_pinned` is configured and the limit is already
 * reached, `NULL` is returned to prevent pool starvation.
 *
 * @param pool Pool to borrow from.
 * @param session Opaque session pointer used as the pin key.
 * @return Pinned connection, or `NULL` if at capacity or none available.
 */
backend_conn_t* backend_pool_borrow_pinned(backend_pool_t* pool, void* session)
{
    /* Check if this session already has a pinned connection */
    for (size_t i = 0; i < pool->total_count; i++) {
        backend_conn_t* conn = &pool->connections[i];
        if (conn->pinned_session == session &&
            atomic_load(&conn->state) == BACKEND_CONN_ACTIVE) {
            /* Session is already pinned to this connection */
            return conn;
        }
    }
    
    /* No existing pin - borrow a new connection.
     * Admission control: if max_pinned is configured and we've hit the
     * limit, reject the pin request to prevent pool starvation.  This
     * leaves idle connections available for unpinned sessions. */
    if (pool->max_pinned > 0 && pool->pinned_count >= pool->max_pinned) {
        return NULL;
    }

    backend_conn_t* conn = backend_pool_borrow(pool, 0);
    if (conn) {
        conn->pinned_session = session;
        pool->pinned_count++;
    }
    return conn;
}

/* ============================================================================
 * Profile-Aware Borrowing (Spec §6)
 * ============================================================================ */

/**
 * @brief Borrow a backend connection matched to a session-state profile.
 *
 * Uses a five-step strategy to find the best connection for the given
 * state profile: (1) clean connection for empty profiles, (2) exact profile
 * match on the idle list, (3) any clean connection (needs sync), (4) any idle
 * connection (needs sync), (5) dirty connection cleanup kick.
 *
 * @param pool Pool to borrow from.
 * @param profile Desired session-state profile, or `NULL` / empty for a
 *        clean connection.
 * @return Borrowed connection, or `NULL` if none are immediately available.
 */
backend_conn_t* backend_pool_borrow_profiled(backend_pool_t* pool,
                                              const struct state_profile* profile)
{
    backend_conn_t* conn;
    POOL_STAT_INC(pool, pool_borrow_attempts);
    pthread_mutex_lock(&pool->lock);

    /* Step 1: If profile is NULL or empty (clean request), prefer clean_list */
    if (!profile || (profile && profile->count == 0)) {
        backend_conn_t** prev_c = &pool->clean_list;
        conn = pool->clean_list;
        while (conn) {
            if (!backend_pool_can_borrow(conn)) {
                pool_record_borrow_result(pool,
                    conn->quarantine != BACKEND_QUARANTINE_NONE
                        ? BORROW_RESULT_FAILED_QUARANTINED
                        : BORROW_RESULT_FAILED_INCOMPATIBLE);
                prev_c = &conn->next;
                conn = conn->next;
                continue;
            }
            backend_conn_state_t expected = BACKEND_CONN_IDLE;
            if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
                *prev_c = conn->next;
                pool->clean_count--;
                conn->next = NULL;
                backend_pool_mark_borrowed(conn);
                conn->needs_sync = false;
                POOL_STAT_INC(pool, pool_borrow_exact_state_match);
                pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                pool->active_count++;
                pthread_mutex_unlock(&pool->lock);
                return conn;
            }
            prev_c = &conn->next;
            conn = conn->next;
        }
    }

    /* Step 2: Search idle_list for exact profile match */
    if (profile && profile->count > 0) {
        backend_conn_t** prev = &pool->idle_list;
        conn = pool->idle_list;

        while (conn) {
            if (!backend_pool_can_borrow(conn)) {
                pool_record_borrow_result(pool,
                    conn->quarantine != BACKEND_QUARANTINE_NONE
                        ? BORROW_RESULT_FAILED_QUARANTINED
                        : BORROW_RESULT_FAILED_INCOMPATIBLE);
                prev = &conn->next;
                conn = conn->next;
                continue;
            }
            if (conn->profile &&
                state_profile_equal_fast(conn->profile, profile)) {
                /* Exact hash match — CAS to claim */
                backend_conn_state_t expected = BACKEND_CONN_IDLE;
                if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
                    *prev = conn->next;
                    conn->next = NULL;
                    backend_pool_mark_borrowed(conn);
                    conn->needs_sync = false;
                    POOL_STAT_INC(pool, pool_borrow_exact_state_match);
                    pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                    pool->active_count++;
                    pthread_mutex_unlock(&pool->lock);
                    return conn;
                }
            }
            prev = &conn->next;
            conn = conn->next;
        }
    }

    /* Step 3: Take any clean connection (will need sync if profile != NULL) */
    {
        backend_conn_t** prev_c3 = &pool->clean_list;
        conn = pool->clean_list;
        while (conn) {
            if (!backend_pool_can_borrow(conn)) {
                pool_record_borrow_result(pool,
                    conn->quarantine != BACKEND_QUARANTINE_NONE
                        ? BORROW_RESULT_FAILED_QUARANTINED
                        : BORROW_RESULT_FAILED_INCOMPATIBLE);
                prev_c3 = &conn->next;
                conn = conn->next;
                continue;
            }
            backend_conn_state_t expected = BACKEND_CONN_IDLE;
            if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
                *prev_c3 = conn->next;
                pool->clean_count--;
                conn->next = NULL;
                backend_pool_mark_borrowed(conn);
                conn->needs_sync = (profile != NULL && profile->count > 0);
                if (conn->needs_sync)
                    POOL_STAT_INC(pool, pool_borrow_state_replay);
                else
                    POOL_STAT_INC(pool, pool_borrow_exact_state_match);
                pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                pool->active_count++;
                pthread_mutex_unlock(&pool->lock);
                return conn;
            }
            prev_c3 = &conn->next;
            conn = conn->next;
        }
    }

    /* Step 4: Take any idle connection (will need sync) */
    {
        backend_conn_t** prev_s4 = &pool->idle_list;
        conn = pool->idle_list;
        while (conn) {
            if (!backend_pool_can_borrow(conn)) {
                pool_record_borrow_result(pool,
                    conn->quarantine != BACKEND_QUARANTINE_NONE
                        ? BORROW_RESULT_FAILED_QUARANTINED
                        : BORROW_RESULT_FAILED_INCOMPATIBLE);
                prev_s4 = &conn->next;
                conn = conn->next;
                continue;
            }
            backend_conn_state_t expected = BACKEND_CONN_IDLE;
            if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
                *prev_s4 = conn->next;
                conn->next = NULL;
                backend_pool_mark_borrowed(conn);
                conn->needs_sync = true;
                POOL_STAT_INC(pool, pool_borrow_state_replay);
                pool_record_borrow_result(pool, BORROW_RESULT_SUCCESS);
                pool->active_count++;
                pthread_mutex_unlock(&pool->lock);
                return conn;
            }
            prev_s4 = &conn->next;
            conn = conn->next;
        }
    }

    /* Step 5: Dirty connections are quarantined into CLEANING and become
     * borrowable only after reactor-owned plugin cleanup completes. */
    backend_pool_kick_dirty_cleanups_locked(pool);

    /* No idle connections available.  The caller will queue in the wait
     * queue, and the async refill timer will grow the pool from
     * min_connections toward max_connections without blocking the reactor.
     *
     * We intentionally do NOT do synchronous connect here because the
     * SCRAM handshake (4 round trips) blocks the reactor thread for
     * 5–400 ms under load, causing cascading stalls. */
    pthread_mutex_unlock(&pool->lock);
    return NULL;
}

/**
 * @brief Return a borrowed backend connection to the pool.
 *
 * The return path makes a conservative reuse decision:
 * - Connections that are still inside a transaction are moved to
 *   `BACKEND_CONN_TXN_PINNED` and cannot be reused until the transaction
 *   completes.
 * - Connections with no session state (hash `0`, empty profile, no prepared
 *   statements) go directly to `clean_list`.
 * - Connections with only named prepared statements go to `idle_list` keyed
 *   by `stmt_set_hash` so a future session with the same statements avoids
 *   replay.
 * - Connections with general session state (`SET` vars, temp tables) enter
 *   a reactor-owned `CLEANING` state machine that sends plugin-built cleanup
 *   and drains protocol responses through the plugin before reuse.
 *
 * One waiting session is woken after a successful return.
 *
 * @param pool Pool that owns `conn`.
 * @param conn Connection to return.
 * @param in_transaction `true` if the connection has an open transaction.
 */
void backend_pool_discard(backend_pool_t* pool, backend_conn_t* conn)
{
    if (!conn) return;
    pthread_mutex_lock(&pool->lock);
    if (atomic_load(&conn->state) != BACKEND_CONN_ACTIVE) {
        pthread_mutex_unlock(&pool->lock);
        return;
    }
    backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_IO_ERROR, true);
    KEEL_CHECK_POOL_INVARIANTS(pool);
    pthread_mutex_unlock(&pool->lock);
}

void backend_pool_return(backend_pool_t* pool, backend_conn_t* conn, bool in_transaction)
{
    if (!conn || atomic_load(&conn->state) != BACKEND_CONN_ACTIVE) return;
    
    pthread_mutex_lock(&pool->lock);
    conn->in_transaction = in_transaction;
    
    if (in_transaction) {
        /* Transaction pinned - cannot return to pool */
        atomic_store(&conn->state, BACKEND_CONN_TXN_PINNED);
        KEEL_CHECK_POOL_INVARIANTS(pool);
        pthread_mutex_unlock(&pool->lock);
        return;
    }
    
    /* Bump generation counter on every return so stale references are detected */
    conn->clean_gen++;
    conn->generation++;
    
    /* Transaction complete — clear the session pin so the connection
     * goes back to the idle pool instead of staying stuck in STATE_PINNED.
     * Decrement pinned_count if this connection was pinned. */
    if (conn->pinned_session != NULL && pool->pinned_count > 0)
        pool->pinned_count--;
    conn->pinned_session = NULL;
    conn->active_owner = NULL;
    conn->last_used = get_time_ms();
    if (pool->active_count > 0)
        pool->active_count--;

    if (conn->protocol_desync) {
        conn->quarantine = BACKEND_QUARANTINE_PROTOCOL_DESYNC;
        backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_CLEANUP_ERROR, false);
        KEEL_CHECK_POOL_INVARIANTS(pool);
        pthread_mutex_unlock(&pool->lock);
        return;
    }
    if (conn->syncing || conn->replay_active || conn->quarantine != BACKEND_QUARANTINE_NONE) {
        if (conn->quarantine == BACKEND_QUARANTINE_NONE) {
            if (conn->syncing)
                conn->quarantine = BACKEND_QUARANTINE_SYNCING;
            else if (conn->replay_active)
                conn->quarantine = BACKEND_QUARANTINE_REPLAYING;
            else
                conn->quarantine = BACKEND_QUARANTINE_DIRTY;
        }
        atomic_store(&conn->state, BACKEND_CONN_CLEANING);
        backend_pool_enter_cleanup_locked(pool, conn);
        KEEL_CHECK_POOL_INVARIANTS(pool);
        pthread_mutex_unlock(&pool->lock);
        return;
    }

    /* Fast path: connection is already clean — no cleanup needed */
    if (conn->current_state_hash == 0 &&
        (!conn->profile || conn->profile->count == 0)) {
        if (!conn_is_alive(conn->fd)) {
            POOL_REUSE_FAIL_INC(pool);
            backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DEAD_IDLE, false);
            KEEL_CHECK_POOL_INVARIANTS(pool);
            pthread_mutex_unlock(&pool->lock);
            return;
        }

        conn->needs_sync = false;
        conn->syncing = false;
        conn->replay_active = false;
        conn->needs_full_cleanup = false;
        conn->in_transaction = false;
        conn->hard_pinned = false;
        conn->protocol_desync = false;
        conn->quarantine = BACKEND_QUARANTINE_NONE;
        conn->close_reason = BACKEND_CLOSE_REASON_NONE;

        if (conn->stmt_set_hash == 0) {
            /* Truly clean — return to clean list */
            atomic_store(&conn->state, BACKEND_CONN_IDLE);
            backend_pool_assert_owner_invariant(conn);
            conn->next = pool->clean_list;
            pool->clean_list = conn;
            pool->clean_count++;
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                "pool_return: fd=%d → clean_list (clean_count=%zu wait=%zu)",
                conn->fd, pool->clean_count, pool->wait_queue_size);
        } else {
            /* Has named prepared statements — keep them alive on the backend.
             * Put on idle_list keyed by stmt_set_hash so a session with the
             * same set of prepared statements can borrow it without replay.
             * Do not run full cleanup — that would destroy the statements. */
            atomic_store(&conn->state, BACKEND_CONN_IDLE);
            backend_pool_assert_owner_invariant(conn);
            conn->next = pool->idle_list;
            pool->idle_list = conn;
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                "pool_return: fd=%d → idle_list (stmt_hash=0x%016llx wait=%zu)",
                conn->fd, (unsigned long long)conn->stmt_set_hash, pool->wait_queue_size);
        }
        goto wake_waiter;
    }

    /* Connection has session state — it is not borrowable again until the
     * reactor-owned cleanup state machine sends plugin-built cleanup and
     * drains responses until the plugin reports a reusable boundary. */
    atomic_store(&conn->state, BACKEND_CONN_CLEANING);
    conn->quarantine = BACKEND_QUARANTINE_DIRTY;
    backend_pool_enter_cleanup_locked(pool, conn);
    KEEL_DEBUG_LOG("Connection %d entering CLEANING state (gen=%lu)\n",
                  conn->fd, (unsigned long)conn->clean_gen);
    KEEL_CHECK_POOL_INVARIANTS(pool);
    pthread_mutex_unlock(&pool->lock);
    return;

wake_waiter:
    backend_pool_wake_one_locked(pool);
    KEEL_CHECK_POOL_INVARIANTS(pool);
    pthread_mutex_unlock(&pool->lock);
}

/* ============================================================================
 * CLEANING State Drain
 * ============================================================================ */

/**
 * @brief Supervise reactor-owned cleanup slots.
 *
 * Called periodically from the refill timer. Cleanup I/O is performed only by
 * reactor callbacks; this function enforces timeout ownership and re-arms any
 * CLEANING slot that is waiting for the reactor but has no outstanding op.
 *
 * @return Number of timed-out/invalid cleanup slots closed
 */
size_t backend_pool_drain_cleaning(backend_pool_t* pool)
{
    if (!pool || pool->cleaning_count == 0) return 0;

    pthread_mutex_lock(&pool->lock);
    uint64_t now = get_time_ms();
    size_t closed = 0;

    for (size_t i = 0; i < pool->total_count; i++) {
        backend_conn_t* conn = &pool->connections[i];
        if (atomic_load(&conn->state) != BACKEND_CONN_CLEANING) continue;

        /* Timeout check: if stuck in CLEANING too long, close it */
        if (conn->cleanup_started_ms > 0 &&
            (now - conn->cleanup_started_ms) > BACKEND_CLEANUP_TIMEOUT_MS) {
            backend_pool_close_cleaning_locked(pool, conn,
                                               BACKEND_CLOSE_REASON_CLEANUP_TIMEOUT,
                                               BACKEND_CLEANUP_RESULT_TIMEOUT);
            KEEL_DEBUG_LOG("Connection CLEANING timeout — closed (gen=%lu)\n",
                          (unsigned long)conn->clean_gen);
            closed++;
            continue;
        }

        if (!conn->cleanup_io_armed) {
            if (conn->cleanup_state == BACKEND_CLEANUP_SEND) {
                backend_pool_arm_cleanup_send_locked(pool, conn);
            } else if (conn->cleanup_state == BACKEND_CLEANUP_DRAIN) {
                backend_pool_arm_cleanup_recv_locked(pool, conn);
            } else {
                backend_pool_close_cleaning_locked(pool, conn,
                                                   BACKEND_CLOSE_REASON_CLEANUP_ERROR,
                                                   BACKEND_CLEANUP_RESULT_PROTOCOL_ERROR);
                closed++;
            }
        }
    }

    KEEL_CHECK_POOL_INVARIANTS(pool);
    pthread_mutex_unlock(&pool->lock);
    return closed;
}

/**
 * @brief Unconditionally release all connections pinned to a session.
 *
 * Called from error paths (frontend disconnect, `EPIPE`, etc.) where
 * synchronous cleanup would block the event loop. The backend socket is
 * closed immediately rather than attempting cleanup; the pool refill
 * timer will replace the slot asynchronously.
 *
 * @param pool Pool that owns the connections.
 * @param session Opaque session pointer that was used as the pin key.
 */
void backend_pool_release_session(backend_pool_t* pool, void* session)
{
    if (!pool || !session) return;
    pthread_mutex_lock(&pool->lock);
    backend_pool_cancel_wait(pool, session);

    /* Release any pinned connection for this session.
     * 
     * IMPORTANT: This is called from error paths (FE disconnect, EPIPE, etc.)
     * where we cannot afford to block the event loop with synchronous cleanup.
     * Therefore, we just close the backend connection instead of trying to
     * clean it up. The pool will create new connections as needed.
     */
    for (size_t i = 0; i < pool->total_count; i++) {
        backend_conn_t* conn = &pool->connections[i];
        if (conn->pinned_session == session) {
            if (pool->pinned_count > 0) pool->pinned_count--;
            
            /* On error/disconnect paths, close the backend connection immediately.
             * Trying to do synchronous cleanup would block
             * the event loop and cause hangs under load. */
            backend_pool_close_slot_locked(pool, conn,
                                           BACKEND_CLOSE_REASON_CLIENT_DISCONNECT,
                                           true);
            
            /* Don't add to any list - the connection is closed.
             * New connections will be created on demand when pool is accessed. */
        }
    }
    KEEL_CHECK_POOL_INVARIANTS(pool);
    pthread_mutex_unlock(&pool->lock);
}

/* ============================================================================
 * Waiting Queue
 * ============================================================================ */

/**
 * @brief Enqueue a session to wait for the next available connection.
 *
 * The waiter is appended to the FIFO wait queue and the pool immediately
 * kicks up to 32 async connect attempts so new backends arrive without
 * waiting for the 100 ms refill timer.
 *
 * @param pool Pool to wait on.
 * @param session Opaque session pointer passed to the wait callback.
 * @param userdata Caller-defined context forwarded to the wait callback.
 * @return `0` on success, `-1` if the queue is full.
 */
int backend_pool_queue_wait(backend_pool_t* pool, void* session, void* userdata)
{
    if (!pool) return -1;
    pthread_mutex_lock(&pool->lock);
    if (pool->wait_queue_size >= pool->config.max_waiting) {
        if (pool->stats_ctx)
            KEEL_STAT_INC(pool->stats_ctx, pool_wait_queue_full_rejects);
        pthread_mutex_unlock(&pool->lock);
        return -1;  /* Queue full */
    }
    
    pool_waiter_t* waiter = pool->waiter_pool
        ? (pool_waiter_t*)keel_pool_alloc(pool->waiter_pool)
        : (pool_waiter_t*)keel_calloc(1, sizeof(pool_waiter_t));
    if (!waiter) {
        pthread_mutex_unlock(&pool->lock);
        return -1;
    }
    
    waiter->session = session;
    waiter->userdata = userdata;
    waiter->enqueue_time_ms = get_time_ms();
    waiter->next = NULL;
    
    if (pool->wait_queue_tail) {
        pool->wait_queue_tail->next = waiter;
    } else {
        pool->wait_queue_head = waiter;
    }
    pool->wait_queue_tail = waiter;
    pool->wait_queue_size++;
    if (pool->stats_ctx)
        KEEL_STAT_INC(pool->stats_ctx, pool_wait_queue_enqueued);
    pthread_mutex_unlock(&pool->lock);

    /* Kick immediate refill — don't wait for the 100ms timer.
     * Start up to 32 async connects to satisfy the burst. */
    for (int i = 0; i < 32; i++) {
        if (backend_pool_refill_one(pool) == 0)
            break;
    }
    
    return 0;
}

size_t backend_pool_cancel_wait(backend_pool_t* pool, void* session)
{
    if (!pool || !session) return 0;

    pthread_mutex_lock(&pool->lock);
    size_t removed = 0;
    pool_waiter_t** prev = &pool->wait_queue_head;
    pool_waiter_t* prev_node = NULL;
    pool_waiter_t* cur = pool->wait_queue_head;

    while (cur) {
        pool_waiter_t* next = cur->next;
        if (cur->session == session) {
            *prev = next;
            if (pool->wait_queue_tail == cur)
                pool->wait_queue_tail = prev_node;
            if (pool->wait_queue_size > 0)
                pool->wait_queue_size--;
            if (pool->waiter_pool)
                keel_pool_free(pool->waiter_pool, cur);
            else
                keel_free(cur);
            removed++;
            cur = next;
            continue;
        }
        prev_node = cur;
        prev = &cur->next;
        cur = next;
    }

    if (!pool->wait_queue_head)
        pool->wait_queue_tail = NULL;
    if (removed > 0 && pool->stats_ctx)
        KEEL_STAT_ADD(pool->stats_ctx, pool_wait_cancelled, removed);
    pthread_mutex_unlock(&pool->lock);
    return removed;
}

/**
 * @brief Register the callback invoked when a waiter should be woken.
 *
 * The callback is called with the session pointer and userdata that were
 * supplied to `backend_pool_queue_wait`. A `NULL` userdata argument
 * signals a wait-timeout expiry.
 *
 * @param pool Pool to configure.
 * @param callback Function to call when a connection becomes available.
 */
void backend_pool_set_wait_callback(backend_pool_t* pool, backend_pool_wait_cb callback)
{
    pool->wait_callback = callback;
}

/* ============================================================================
 * State Management
 * ============================================================================ */

/**
 * @brief Update the transaction state of a borrowed connection.
 *
 * When `begin` is `true` the connection transitions to
 * `BACKEND_CONN_TXN_PINNED` so the pool will not attempt to reclaim it
 * mid-transaction. When `begin` is `false` the transaction has ended and
 * the connection reverts to `BACKEND_CONN_ACTIVE` (ready to be returned).
 *
 * @param pool Pool that owns `conn`.
 * @param conn Connection whose transaction state is changing.
 * @param begin `true` to mark transaction start, `false` for commit/rollback.
 */
void backend_pool_mark_transaction(backend_pool_t* pool, backend_conn_t* conn, bool begin)
{
    if (begin) {
        conn->in_transaction = true;
        atomic_store(&conn->state, BACKEND_CONN_TXN_PINNED);
    } else {
        conn->in_transaction = false;
        if (!conn->pinned_session) {
            /* Transaction ended, no state pin - can return */
            atomic_store(&conn->state, BACKEND_CONN_ACTIVE);
        }
    }
}

/**
 * @brief Record a new session-state fingerprint on a backend connection.
 *
 * When the session issues a state-changing command (`SET`, `BEGIN`, etc.)
 * the engine calls this to record the new hash. A non-zero hash transitions
 * the connection to `BACKEND_CONN_STATE_PINNED`.
 *
 * @param pool Pool that owns `conn` (reserved for future use).
 * @param conn Connection to update.
 * @param hash New session-state fingerprint; `0` means clean.
 */
void backend_pool_update_state_hash(backend_pool_t* pool, backend_conn_t* conn, uint64_t hash)
{
    conn->current_state_hash = hash;
    if (hash != 0) {
        atomic_store(&conn->state, BACKEND_CONN_STATE_PINNED);
    }
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Populate a stats snapshot from the current pool state.
 *
 * Counts connections across all three idle sub-lists (clean, idle, dirty)
 * and writes totals into `stats`. The caller does not need to hold the pool
 * lock; values are a best-effort snapshot.
 *
 * @param pool Pool to sample.
 * @param stats Output struct to populate.
 */
void backend_pool_get_stats(backend_pool_t* pool, backend_pool_stats_t* stats)
{
    size_t clean = 0;
    size_t stateful = 0;
    size_t dirty = 0;
    size_t closed = 0;

    /* Count all idle sublists */
    backend_conn_t* c = pool->clean_list;
    while (c) { clean++; c = c->next; }

    c = pool->idle_list;
    while (c) { stateful++; c = c->next; }

    c = pool->dirty_list;
    while (c) { dirty++; c = c->next; }

    for (size_t i = 0; i < pool->total_count; i++) {
        if (atomic_load(&pool->connections[i].state) == BACKEND_CONN_CLOSED)
            closed++;
    }
    
    stats->total_connections = pool->total_count;
    stats->active_connections = pool->active_count;
    stats->idle_connections = clean + stateful + dirty;
    stats->clean_connections = clean;
    stats->stateful_connections = stateful;
    stats->dirty_connections = dirty;
    stats->closed_connections = closed;
    stats->waiting_sessions = pool->wait_queue_size;
    stats->cleaning_count = pool->cleaning_count;
    stats->pinned_count = pool->pinned_count;
}

/* ============================================================================
 * Idle Connection Pruning
 * ============================================================================ */

/**
 * @brief Helper to remove a connection from a linked list
 */
static bool remove_from_list(backend_conn_t** list, backend_conn_t* target)
{
    backend_conn_t** prev = list;
    backend_conn_t* curr = *list;
    
    while (curr) {
        if (curr == target) {
            *prev = curr->next;
            curr->next = NULL;
            return true;
        }
        prev = &curr->next;
        curr = curr->next;
    }
    return false;
}

/* ============================================================================
 * Pool Refill — async reconnect ONE closed slot and wake a waiter
 * ============================================================================
 * Called from a fast timer when there are closed slots and pending waiters.
 * Uses the async connect state machine so the reactor thread is never blocked.
 * Returns 1 if an async connect was started, 0 if nothing to do.
 */

/**
 * @brief Completion callback for async refill connect
 */
static void refill_async_complete(struct backend_conn* conn, bool success, void* userdata)
{
    backend_pool_t* pool = (backend_pool_t*)userdata;
    
    if (!success) {
        /* Async connect failed (for example: backend rejected too many clients).
         * Back off for 1 second to avoid hammering the backend. */
        pthread_mutex_lock(&pool->lock);
        backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_IO_ERROR, false);
        pthread_mutex_unlock(&pool->lock);
        pool->refill_backoff_until = get_time_ms() + 1000;
        return;
    }
    
    /* Connection is ready! Put on clean_list and wake a waiter */
    conn->current_state_hash = 0;
    conn->needs_sync = false;
    conn->syncing = false;
    conn->replay_active = false;
    conn->protocol_desync = false;
    conn->quarantine = BACKEND_QUARANTINE_NONE;
    conn->close_reason = BACKEND_CLOSE_REASON_NONE;
    conn->pinned_session = NULL;
    conn->active_owner = NULL;
    conn->in_transaction = false;
    conn->hard_pinned = false;
    if (conn->profile) {
        state_profile_clear(conn->profile);
    }
    
    atomic_store(&conn->state, BACKEND_CONN_IDLE);
    backend_pool_assert_owner_invariant(conn);
    conn->last_used = get_time_ms();
    conn->next = pool->clean_list;
    pool->clean_list = conn;
    pool->clean_count++;
    
    /* Wake a waiter if any — they'll borrow this connection */
    if (pool->wait_queue_head && pool->wait_callback) {
        pool_waiter_t* waiter = pool->wait_queue_head;
        pool->wait_queue_head = waiter->next;
        if (!pool->wait_queue_head) pool->wait_queue_tail = NULL;
        pool->wait_queue_size--;
        pool->wait_callback(waiter->session, waiter->userdata);
        if (pool->waiter_pool) {
            keel_pool_free(pool->waiter_pool, waiter);
        } else {
            keel_free(waiter);
        }
    }
}

/**
 * @brief Attempt to reconnect one closed pool slot asynchronously.
 *
 * Skips reconnection while an exponential backoff window is active.
 * Only triggers a new connect when there are pending waiters or the pool
 * is below `min_connections`. Requires `pool->reactor` to be set; without
 * a reactor there is no async connect path and this function returns `0`.
 *
 * @param pool Pool to refill.
 * @return `1` if an async connect was started, `0` if nothing was done.
 */
int backend_pool_refill_one(backend_pool_t* pool)
{
    if (!pool) return 0;
    
    /* Exponential backoff on consecutive refill failures.
     * Prevents log flooding and CPU waste when the backend is unreachable.
     * Starts at 1s, doubles up to 30s.  Resets on first successful connect. */
    if (pool->refill_backoff_until > 0) {
        uint64_t now = get_time_ms();
        if (now < pool->refill_backoff_until) return 0;
        pool->refill_backoff_until = 0;  /* Backoff expired, retry */
    }
    
    /* Only refill if there are waiters or we're below min_connections */
    size_t idle_count = 0;
    for (backend_conn_t* c = pool->clean_list; c; c = c->next) idle_count++;
    for (backend_conn_t* c = pool->idle_list; c; c = c->next) idle_count++;
    
    bool need_refill = (pool->wait_queue_head != NULL) ||
                       (idle_count + pool->active_count < pool->config.min_connections);
    if (!need_refill) return 0;
    
    /* Reactor must be set for async connect — no synchronous fallback */
    if (!pool->reactor) return 0;
    
    /* Async path: find ONE closed slot and start async connect */
    for (size_t i = 0; i < pool->total_count; i++) {
        backend_conn_t* conn = &pool->connections[i];
        backend_conn_state_t expected = BACKEND_CONN_CLOSED;
        if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
            int rc = backend_async_start(pool, conn, pool->reactor,
                                         refill_async_complete, pool);
            if (rc < 0) {
                /* Failed to start async connect */
                backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_IO_ERROR, false);
                return 0;
            }
            return 1;  /* Async connect in progress */
        }
    }
    return 0;
}

/**
 * @brief Eagerly warm up all pool connection slots using async connects.
 *
 * Kicks one async connect per slot up to `max_connections` so that
 * backends are ready before the first client arrives. The reactor must be
 * wired in before calling this function. Intended to be called once from
 * the worker thread after the reactor is initialised and before the accept
 * socket is armed.
 *
 * @param pool Pool to warm up.
 */
void backend_pool_async_warmup(backend_pool_t* pool)
{
    if (!pool || !pool->reactor) return;
    
    /* Eager warmup: kick async connects for ALL max_connections slots.
     * This eliminates the thundering herd of pool waits during the initial
     * burst of client connections.  backend_async_start queues a non-blocking
     * connect + SCRAM auth on the reactor; the completions populate
     * clean_list as they finish (typically 2-10 ms each).
     *
     * The caller (worker_thread_func) spins the reactor until at least
     * min_connections are established before arming the accept — so clients
     * don't arrive before backends are ready. */
    size_t target = pool->config.max_connections;
    int kicked = 0;
    for (size_t i = 0; i < pool->total_count && (size_t)kicked < target; i++) {
        backend_conn_t* conn = &pool->connections[i];
        backend_conn_state_t expected = BACKEND_CONN_CLOSED;
        if (atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_ACTIVE)) {
            int rc = backend_async_start(pool, conn, pool->reactor,
                                         refill_async_complete, pool);
            if (rc < 0) {
                backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_IO_ERROR, false);
                break;
            }
            kicked++;
        }
    }
    if (kicked > 0) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                "Pool warmup: kicked %d async connects to %s:%u (target=%zu)",
                kicked, pool->config.host, pool->config.port, target);
    }
}

/**
 * @brief Close idle connections that have exceeded the idle timeout.
 *
 * Iterates the connection array and closes any `BACKEND_CONN_IDLE`
 * connection whose `last_used` timestamp is older than
 * `config.idle_timeout_ms`. At least `min_connections` slots are kept alive
 * regardless of their age. Idle timeout is disabled when the configured
 * value is `0`.
 *
 * @param pool Pool to prune.
 * @return Number of connections closed.
 */
size_t backend_pool_prune_idle(backend_pool_t* pool)
{
    if (!pool || pool->config.idle_timeout_ms == 0) {
        return 0;  /* Idle timeout disabled */
    }
    
    uint64_t now = get_time_ms();
    size_t closed = 0;
    
    /* Count current idle connections across all lists */
    size_t idle_count = 0;
    for (backend_conn_t* c = pool->clean_list; c; c = c->next) idle_count++;
    for (backend_conn_t* c = pool->idle_list; c; c = c->next) idle_count++;
    for (backend_conn_t* c = pool->dirty_list; c; c = c->next) idle_count++;
    
    /* We need to keep at least min_connections alive */
    size_t min_to_keep = pool->config.min_connections;
    
    /* Check each idle connection for timeout.
     * We iterate through the connection array and check if it's idle and expired. */
    for (size_t i = 0; i < pool->total_count && idle_count > min_to_keep; i++) {
        backend_conn_t* conn = &pool->connections[i];
        backend_conn_state_t state = atomic_load(&conn->state);
        
        /* Only prune IDLE connections */
        if (state != BACKEND_CONN_IDLE) {
            continue;
        }
        
        /* Check if idle time exceeded */
        if (conn->last_used == 0) {
            continue;  /* Never used, keep it */
        }
        
        uint64_t idle_ms = now - conn->last_used;
        if (idle_ms < pool->config.idle_timeout_ms) {
            continue;  /* Not expired yet */
        }
        
        /* Would we go below min_connections? */
        if (idle_count <= min_to_keep) {
            break;
        }
        
        /* Try to claim this connection for closing */
        backend_conn_state_t expected = BACKEND_CONN_IDLE;
        if (!atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_CLOSED)) {
            continue;  /* Someone else grabbed it */
        }
        
        /* Remove from the appropriate idle list */
        bool removed = false;
        if (remove_from_list(&pool->clean_list, conn)) {
            pool->clean_count--;
            removed = true;
        } else if (remove_from_list(&pool->idle_list, conn)) {
            removed = true;
        } else if (remove_from_list(&pool->dirty_list, conn)) {
            pool->dirty_count--;
            removed = true;
        }
        
        if (removed) {
            backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_PRUNE_IDLE, false);
            idle_count--;
            closed++;
            
            KEEL_DEBUG_LOG("Pool pruned idle connection (idle %lums > timeout %lums)\n",
                         (unsigned long)idle_ms, 
                         (unsigned long)pool->config.idle_timeout_ms);
        } else {
            /* Shouldn't happen, but restore state if not found in any list */
            conn->close_reason = BACKEND_CLOSE_REASON_NONE;
            atomic_store(&conn->state, BACKEND_CONN_IDLE);
        }
    }
    
    return closed;
}

/* ============================================================================
 * Pool Target Update (for failover)
 * ============================================================================ */

/**
 * @brief Expire waiters that have been queued longer than `wait_timeout_ms`.
 *
 * The wait queue is FIFO so the function stops as soon as it encounters a
 * waiter within the timeout window. Expired waiters are invoked with a
 * `NULL` userdata argument to signal timeout, then freed.
 *
 * @param pool Pool whose wait queue should be checked.
 * @return Number of waiters expired.
 */
size_t backend_pool_expire_waiters(backend_pool_t* pool)
{
    if (!pool || pool->config.wait_timeout_ms == 0) return 0;
    if (!pool->wait_queue_head) return 0;

    uint64_t now = get_time_ms();
    size_t expired = 0;

    while (pool->wait_queue_head) {
        pool_waiter_t* w = pool->wait_queue_head;
        uint64_t waited = now - w->enqueue_time_ms;
        if (waited < pool->config.wait_timeout_ms) break;  /* Queue is FIFO — rest are newer */

        /* Expire this waiter */
        pool->wait_queue_head = w->next;
        if (!pool->wait_queue_head) pool->wait_queue_tail = NULL;
        pool->wait_queue_size--;

        /* Invoke callback with NULL userdata to signal timeout */
        if (pool->wait_callback) {
            pool->wait_callback(w->session, NULL);
        }
        if (pool->stats_ctx)
            KEEL_STAT_INC(pool->stats_ctx, pool_wait_timeout_events);

        if (pool->waiter_pool) {
            keel_pool_free(pool->waiter_pool, w);
        } else {
            keel_free(w);
        }
        expired++;
    }

    if (expired > 0)
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                "Pool expired %zu waiters (timeout=%lums) for %s:%u",
                expired, (unsigned long)pool->config.wait_timeout_ms,
                pool->config.host, pool->config.port);

    return expired;
}

/**
 * @brief Update the backend host and port for this pool (failover support).
 *
 * Changes the target coordinates recorded in `pool->config`. In-flight
 * connections are not affected; they will reconnect to the new target
 * after they are closed and the refill timer fires.
 *
 * @param pool Pool to reconfigure.
 * @param host New backend hostname or IP string.
 * @param port New backend TCP port.
 */
void backend_pool_update_target(backend_pool_t* pool, const char* host, uint16_t port)
{
    if (!pool) return;
    pool->config.host = host;
    pool->config.port = port;
    KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
            "Pool target updated → %s:%u", host, port);
}

/**
 * @brief Close and discard all idle connections in the pool.
 *
 * Drains the clean, idle, and dirty lists and closes every socket. Used
 * during failover to flush stale connections so the pool reconnects to the
 * new target immediately when the next session arrives.
 *
 * @param pool Pool to drain.
 * @return Number of connections closed.
 */
size_t backend_pool_drain_idle(backend_pool_t* pool)
{
    if (!pool) return 0;
    size_t closed = 0;

    /* Drain clean_list */
    backend_conn_t* conn = pool->clean_list;
    pool->clean_list = NULL;
    while (conn) {
        backend_conn_t* next = conn->next;
        backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DRAIN_IDLE, false);
        conn->next = NULL;
        closed++;
        conn = next;
    }
    pool->clean_count = 0;

    /* Drain idle_list */
    conn = pool->idle_list;
    pool->idle_list = NULL;
    while (conn) {
        backend_conn_t* next = conn->next;
        backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DRAIN_IDLE, false);
        conn->next = NULL;
        closed++;
        conn = next;
    }

    /* Drain dirty_list */
    conn = pool->dirty_list;
    pool->dirty_list = NULL;
    while (conn) {
        backend_conn_t* next = conn->next;
        backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_DRAIN_IDLE, false);
        conn->next = NULL;
        closed++;
        conn = next;
    }
    pool->dirty_count = 0;

    if (closed > 0)
        KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                "Pool drained %zu idle connections (failover)", closed);
    return closed;
}

/**
 * @brief Return whether the pool has at least one immediately available connection.
 *
 * Checks `clean_list` and `idle_list` without locking; the result is a
 * best-effort hint used by callers to decide whether to enqueue a wait or
 * attempt a synchronous borrow.
 *
 * @param pool Pool to query.
 * @return `true` if a connection is likely available, `false` otherwise.
 */
bool backend_pool_has_available(backend_pool_t* pool)
{
    if (!pool) return false;
    for (backend_conn_t* c = pool->clean_list; c; c = c->next)
        if (backend_pool_can_borrow(c))
            return true;
    for (backend_conn_t* c = pool->idle_list; c; c = c->next)
        if (backend_pool_can_borrow(c))
            return true;
    return false;
}

/* ============================================================================
 * Connection Age Pruning
 * ============================================================================ */

/**
 * @brief Close idle connections that have exceeded the maximum connection age.
 *
 * Long-lived backend connections can accumulate server-side overhead (e.g.
 * bloated memory caches, stale statistics). This function closes any idle
 * connection whose `created_at` timestamp is older than
 * `config.max_connection_age_ms`, while always preserving at least
 * `min_connections` live slots. Age pruning is disabled when the configured
 * value is `0`.
 *
 * @param pool Pool to prune.
 * @return Number of connections closed.
 */
size_t backend_pool_prune_aged(backend_pool_t* pool)
{
    if (!pool || pool->config.max_connection_age_ms == 0) {
        return 0;  /* Max age disabled */
    }

    uint64_t now = get_time_ms();
    size_t closed = 0;

    /* Count current idle connections to respect min_connections */
    size_t idle_count = 0;
    for (backend_conn_t* c = pool->clean_list; c; c = c->next) idle_count++;
    for (backend_conn_t* c = pool->idle_list; c; c = c->next) idle_count++;
    for (backend_conn_t* c = pool->dirty_list; c; c = c->next) idle_count++;

    size_t min_to_keep = pool->config.min_connections;

    for (size_t i = 0; i < pool->total_count && idle_count > min_to_keep; i++) {
        backend_conn_t* conn = &pool->connections[i];
        backend_conn_state_t state = atomic_load(&conn->state);

        /* Only prune IDLE connections */
        if (state != BACKEND_CONN_IDLE) {
            continue;
        }

        /* Check if connection is too old */
        if (conn->created_at == 0) {
            continue;  /* No timestamp — skip */
        }

        uint64_t age_ms = now - conn->created_at;
        if (age_ms < pool->config.max_connection_age_ms) {
            continue;  /* Not aged yet */
        }

        /* Would we go below min_connections? */
        if (idle_count <= min_to_keep) {
            break;
        }

        /* Try to claim this connection for closing */
        backend_conn_state_t expected = BACKEND_CONN_IDLE;
        if (!atomic_compare_exchange_strong(&conn->state, &expected, BACKEND_CONN_CLOSED)) {
            continue;  /* Someone else grabbed it */
        }

        /* Remove from the appropriate idle list */
        bool removed = false;
        if (remove_from_list(&pool->clean_list, conn)) {
            pool->clean_count--;
            removed = true;
        } else if (remove_from_list(&pool->idle_list, conn)) {
            removed = true;
        } else if (remove_from_list(&pool->dirty_list, conn)) {
            pool->dirty_count--;
            removed = true;
        }

        if (removed) {
            backend_pool_close_slot_locked(pool, conn, BACKEND_CLOSE_REASON_PRUNE_AGED, false);
            idle_count--;
            closed++;

            KEEL_DEBUG_LOG("Pool pruned aged connection (age %lums > max %lums)\n",
                         (unsigned long)age_ms,
                         (unsigned long)pool->config.max_connection_age_ms);
        } else {
            /* Shouldn't happen, but restore state if not found in any list */
            conn->close_reason = BACKEND_CLOSE_REASON_NONE;
            atomic_store(&conn->state, BACKEND_CONN_IDLE);
        }
    }

    return closed;
}

/* ============================================================================
 * Per-User Connection Tracking
 * ============================================================================ */

/**
 * Find or create a slot in the fixed user_conn_map for a given username.
 * Uses a simple hash-probe approach.  Returns NULL if the table is full
 * and the username is not already present.
 */
static struct user_conn_entry* user_conn_find_or_create(backend_pool_t* pool,
                                                         const char* user,
                                                         bool create)
{
    if (!user || !user[0]) return NULL;

    /* Simple djb2 hash */
    uint32_t h = 5381;
    for (const char* p = user; *p; p++)
        h = ((h << 5) + h) + (uint8_t)*p;

    size_t cap = sizeof(pool->user_conn_map) / sizeof(pool->user_conn_map[0]);
    size_t idx = h % cap;

    for (size_t probe = 0; probe < cap; probe++) {
        size_t slot = (idx + probe) % cap;
        struct user_conn_entry* e = &pool->user_conn_map[slot];

        if (e->username[0] == '\0') {
            /* Empty slot */
            if (!create) return NULL;
            if (pool->user_conn_map_used >= cap) return NULL;  /* Full */
            snprintf(e->username, sizeof(e->username), "%s", user);
            e->active_count = 0;
            pool->user_conn_map_used++;
            return e;
        }

        if (strncmp(e->username, user, sizeof(e->username) - 1) == 0) {
            return e;
        }
    }
    return NULL;  /* Table full */
}

/**
 * @brief Check whether a user is allowed to acquire another connection.
 *
 * Uses the per-user connection map to compare the user's current active
 * count against `config.max_user_connections`. Returns `true` when the
 * limit is disabled (`0`) or the user has not yet been seen.
 *
 * @param pool Pool to check.
 * @param user Username string.
 * @return `true` if the user may acquire a connection, `false` if at limit.
 */
bool backend_pool_user_can_acquire(backend_pool_t* pool, const char* user)
{
    if (!pool || pool->config.max_user_connections == 0) {
        return true;  /* Unlimited */
    }

    struct user_conn_entry* e = user_conn_find_or_create(pool, user, false);
    if (!e) return true;  /* User not tracked yet — allowed */

    return e->active_count < pool->config.max_user_connections;
}

/**
 * @brief Increment the active connection count for a user.
 *
 * Creates a tracking entry for `user` if one does not yet exist.
 * Must be paired with a corresponding `backend_pool_user_conn_release` call.
 *
 * @param pool Pool that owns the per-user map.
 * @param user Username string.
 */
void backend_pool_user_conn_acquire(backend_pool_t* pool, const char* user)
{
    if (!pool || pool->config.max_user_connections == 0) return;

    struct user_conn_entry* e = user_conn_find_or_create(pool, user, true);
    if (e) e->active_count++;
}

/**
 * @brief Decrement the active connection count for a user.
 *
 * Called when a session that was tracked via `backend_pool_user_conn_acquire`
 * finishes. The count is floored at `0` to guard against unbalanced calls.
 *
 * @param pool Pool that owns the per-user map.
 * @param user Username string.
 */
void backend_pool_user_conn_release(backend_pool_t* pool, const char* user)
{
    if (!pool || pool->config.max_user_connections == 0) return;

    struct user_conn_entry* e = user_conn_find_or_create(pool, user, false);
    if (e && e->active_count > 0) e->active_count--;
}
