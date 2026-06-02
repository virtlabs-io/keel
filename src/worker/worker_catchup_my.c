/**
 * @file worker_catchup_my.c
 * @brief Reactor-async MySQL replica catch-up probe state machine.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Mirror of worker_catchup_pg.c for MySQL. See that file's header for
 * the full design rationale; the differences here are protocol-specific:
 *
 *   - CONNECTING delegates to `backend_async_start` which already
 *     implements the MySQL handshake (caching_sha2_password and
 *     mysql_native_password) and optional TLS.
 *
 *   - The probe query is:
 *         SELECT WAIT_FOR_EXECUTED_GTID_SET('<gtid>', 0)
 *     where the timeout `0` makes it an immediate non-blocking check
 *     ('0' = reached, '1' = timeout, NULL = error).
 *
 *   - The reply is a 1-column / 1-row result set: col-count packet,
 *     column definition, EOF, row, EOF. Parsing is delegated to the
 *     pure helper `my_probe_parse_response`.
 *
 *   - MySQL GTID sets are partially ordered. The probe picker uses
 *     `my_token_compare` (longest string wins, lex tie-break) just to
 *     pick *some* parked token to probe; `my_token_satisfied_by` then
 *     gates which queued waiter the probe round answers for (exact
 *     string equality, which is conservative but correct).
 */

#include "worker_catchup_internal.h"
#include "worker_catchup_my_helpers.h"

#include "keel/engine/backend_connect.h"
#include "keel/engine/backend_pool.h"
#include "keel/engine/worker.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel/reactor/reactor.h"
#include "keel/util/util.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ============================================================================
 * Probe state
 * ============================================================================ */

typedef enum my_probe_state {
    MY_PROBE_INIT = 0,
    MY_PROBE_CONNECTING,
    MY_PROBE_READY,
    MY_PROBE_QUERY_SEND,
    MY_PROBE_QUERY_RECV,
    MY_PROBE_FAILED,
} my_probe_state_t;

typedef struct my_probe_ctx {
    keel_catchup_manager_t*  mgr;
    size_t                   server_index;
    my_probe_state_t         state;

    backend_pool_t*          pool;
    backend_conn_t*          conn;

    keel_consistency_token_t probing_token;
    uint64_t                 probe_started_ns;

    /* Send buffer for the COM_QUERY packet. GTIDs can reach ~500B so
     * the encoded SQL fits comfortably in 1 KiB. */
    uint8_t                  send_buf[1024];
    size_t                   send_len;
    size_t                   send_off;

    /* Receive buffer. For a single-row reply with a 1-byte value the
     * total wire size is ~50 B; 4 KiB leaves room for outsized server
     * version strings in column definitions. */
    uint8_t                  recv_buf[4096];
    size_t                   recv_have;

    /* Per-round packet counter, threaded into `my_probe_parse_response`
     * so it can recognise which packet in the result-set sequence it
     * is currently looking at. Reset to 0 in `my_probe_issue_query`. */
    int                      pkt_index;

    bool                     last_result;
    bool                     last_result_valid;
} my_probe_ctx_t;

/* ============================================================================
 * Forward declarations
 * ============================================================================ */

static void my_probe_start_connect(my_probe_ctx_t* ctx);
static void my_probe_on_connect(struct backend_conn* conn, bool success, void* ud);
static void my_probe_issue_query(my_probe_ctx_t* ctx, uint64_t now_ns);
static void my_probe_on_send(void* userdata, int result);
static void my_probe_post_recv(my_probe_ctx_t* ctx);
static void my_probe_on_recv(void* userdata, int result);
static void my_probe_handle_failure(my_probe_ctx_t* ctx, const char* why);
static void my_probe_handle_success(my_probe_ctx_t* ctx);
static void my_probe_close_socket(my_probe_ctx_t* ctx);

/* ============================================================================
 * Allocation / destruction
 * ============================================================================ */

static void my_probe_ctx_free(void* p)
{
    my_probe_ctx_t* ctx = (my_probe_ctx_t*)p;
    if (!ctx) return;
    if (ctx->conn) {
        if (ctx->conn->fd >= 0) {
            close(ctx->conn->fd);  /* NOLINT(keel-syscall) */
            ctx->conn->fd = -1;
        }
        keel_free(ctx->conn);
        ctx->conn = NULL;
    }
    keel_free(ctx);
}

static my_probe_ctx_t* my_probe_ctx_get_or_create(
    keel_catchup_manager_t* m, size_t server_index)
{
    keel_catchup_probe_socket_t* slot = &m->sockets[server_index];
    if (slot->probe_state) return (my_probe_ctx_t*)slot->probe_state;

    if (!m->worker || !m->worker->server_pools ||
        server_index >= m->worker->server_pool_count ||
        !m->worker->server_pools[server_index] ||
        !m->worker->reactor)
    {
        return NULL;
    }

    my_probe_ctx_t* ctx = keel_calloc(1, sizeof *ctx);
    if (!ctx) return NULL;
    ctx->mgr          = m;
    ctx->server_index = server_index;
    ctx->state        = MY_PROBE_INIT;
    ctx->pool         = m->worker->server_pools[server_index];

    slot->probe_state      = ctx;
    slot->probe_state_free = my_probe_ctx_free;
    slot->fd               = -1;
    return ctx;
}

void keel_catchup_my_close(struct keel_catchup_manager* m, size_t server_index)
{
    if (!m || server_index >= KEEL_MAX_SERVERS) return;
    keel_catchup_probe_socket_t* slot = &m->sockets[server_index];
    if (!slot->probe_state) return;
    my_probe_ctx_t* ctx = (my_probe_ctx_t*)slot->probe_state;
    my_probe_close_socket(ctx);
    ctx->state = MY_PROBE_INIT;
}

/* Test-only: inject a pre-authenticated probe socket.
 * MUST NOT be called from production code — it skips authentication. */
int keel_catchup_my_test_inject_ready(struct keel_catchup_manager* m,
                                      size_t server_index,
                                      int fd)
{
    if (!m || server_index >= KEEL_MAX_SERVERS || fd < 0) return -1;
    if (!m->worker || !m->worker->reactor) return -1;

    keel_catchup_probe_socket_t* slot = &m->sockets[server_index];
    if (slot->probe_state) return -1;

    my_probe_ctx_t* ctx = keel_calloc(1, sizeof *ctx);
    if (!ctx) return -1;
    ctx->conn = keel_calloc(1, sizeof *ctx->conn);
    if (!ctx->conn) { keel_free(ctx); return -1; }

    ctx->mgr          = m;
    ctx->server_index = server_index;
    ctx->state        = MY_PROBE_READY;
    ctx->pool         = NULL;
    ctx->conn->fd     = fd;

    slot->probe_state      = ctx;
    slot->probe_state_free = my_probe_ctx_free;
    slot->fd               = fd;
    slot->opened_ns        = (uint64_t)keel_time_now();

    if (keel_reactor_register_fd(m->worker->reactor, fd) < 0) {
        slot->probe_state = NULL;
        slot->fd          = -1;
        my_probe_ctx_free(ctx);
        return -1;
    }
    return 0;
}

/* ============================================================================
 * State transitions
 * ============================================================================ */

static void my_probe_close_socket(my_probe_ctx_t* ctx)
{
    if (!ctx) return;
    if (ctx->conn && ctx->conn->fd >= 0) {
        if (ctx->mgr->worker && ctx->mgr->worker->reactor) {
            keel_reactor_unregister_fd(ctx->mgr->worker->reactor, ctx->conn->fd);
        }
        close(ctx->conn->fd);  /* NOLINT(keel-syscall) */
        ctx->conn->fd = -1;
    }
    ctx->send_len = ctx->send_off = 0;
    ctx->recv_have = 0;
    ctx->pkt_index = 0;
    ctx->last_result_valid = false;
}

static void my_probe_handle_failure(my_probe_ctx_t* ctx, const char* why)
{
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_CONN,
        "catchup my probe[server=%zu] failed: %s", ctx->server_index, why);
    my_probe_close_socket(ctx);
    ctx->state = MY_PROBE_FAILED;
    keel_catchup_apply_backoff(ctx->mgr, ctx->server_index, (uint64_t)keel_time_now());
}

static void my_probe_handle_success(my_probe_ctx_t* ctx)
{
    uint64_t now = (uint64_t)keel_time_now();
    keel_catchup_cache_put(ctx->mgr, ctx->server_index, &ctx->probing_token,
                           ctx->last_result, now);
    ctx->mgr->probes_succeeded_total++;

    if (ctx->last_result) {
        keel_catchup_release_satisfied(ctx->mgr, ctx->server_index,
                                       &ctx->probing_token, now,
                                       my_token_satisfied_by);
    }
    ctx->state = MY_PROBE_READY;
}

/* ============================================================================
 * CONNECTING — delegated to backend_async_start
 * ============================================================================ */

static void my_probe_start_connect(my_probe_ctx_t* ctx)
{
    if (!ctx->conn) {
        ctx->conn = keel_calloc(1, sizeof *ctx->conn);
        if (!ctx->conn) {
            my_probe_handle_failure(ctx, "alloc backend_conn failed");
            return;
        }
        ctx->conn->fd = -1;
    }
    ctx->state = MY_PROBE_CONNECTING;
    ctx->mgr->probes_issued_total++;

    int rc = backend_async_start(ctx->pool, ctx->conn,
                                 ctx->mgr->worker->reactor,
                                 my_probe_on_connect, ctx);
    if (rc < 0) {
        my_probe_handle_failure(ctx, "backend_async_start launch failed");
    }
}

static void my_probe_on_connect(struct backend_conn* conn, bool success, void* ud)
{
    my_probe_ctx_t* ctx = (my_probe_ctx_t*)ud;
    if (!ctx) return;
    (void)conn;

    if (!success || ctx->conn->fd < 0) {
        my_probe_handle_failure(ctx, "async connect/auth failed");
        return;
    }
    ctx->state = MY_PROBE_READY;
    ctx->mgr->sockets[ctx->server_index].fd = ctx->conn->fd;
    ctx->mgr->sockets[ctx->server_index].opened_ns = (uint64_t)keel_time_now();

    my_probe_issue_query(ctx, (uint64_t)keel_time_now());
}

/* ============================================================================
 * QUERY_SEND
 * ============================================================================ */

static int my_probe_build_query(my_probe_ctx_t* ctx,
                                const keel_consistency_token_t* token)
{
    if (my_probe_encode_query(ctx->send_buf, sizeof ctx->send_buf,
                              token->value, &ctx->send_len) < 0) {
        return -1;
    }
    ctx->send_off = 0;
    return 0;
}

static void my_probe_issue_query(my_probe_ctx_t* ctx, uint64_t now_ns)
{
    keel_consistency_token_t target;
    if (!keel_catchup_pick_probe_token(ctx->mgr, ctx->server_index,
                                       my_token_compare, &target))
    {
        return;
    }
    ctx->probing_token     = target;
    ctx->probe_started_ns  = now_ns;
    ctx->last_result_valid = false;
    ctx->last_result       = false;
    ctx->recv_have         = 0;
    ctx->pkt_index         = 0;

    if (my_probe_build_query(ctx, &target) < 0) {
        my_probe_handle_failure(ctx, "build_query failed (unsafe GTID?)");
        return;
    }

    ctx->state = MY_PROBE_QUERY_SEND;
    int rc = keel_reactor_send(ctx->mgr->worker->reactor, ctx->conn->fd,
                               ctx->send_buf, ctx->send_len, 0,
                               ctx, my_probe_on_send);
    if (rc < 0) {
        my_probe_handle_failure(ctx, "reactor_send launch failed");
    }
}

static void my_probe_on_send(void* userdata, int result)
{
    my_probe_ctx_t* ctx = (my_probe_ctx_t*)userdata;
    if (!ctx) return;
    if (result < 0) {
        my_probe_handle_failure(ctx, "send error");
        return;
    }
    ctx->send_off += (size_t)result;
    if (ctx->send_off < ctx->send_len) {
        int rc = keel_reactor_send(ctx->mgr->worker->reactor, ctx->conn->fd,
                                   ctx->send_buf + ctx->send_off,
                                   ctx->send_len - ctx->send_off, 0,
                                   ctx, my_probe_on_send);
        if (rc < 0) my_probe_handle_failure(ctx, "reactor_send resume failed");
        return;
    }
    ctx->state = MY_PROBE_QUERY_RECV;
    my_probe_post_recv(ctx);
}

/* ============================================================================
 * QUERY_RECV
 * ============================================================================ */

static void my_probe_post_recv(my_probe_ctx_t* ctx)
{
    if (ctx->recv_have >= sizeof ctx->recv_buf) {
        my_probe_handle_failure(ctx, "recv buffer overflow");
        return;
    }
    int rc = keel_reactor_recv(ctx->mgr->worker->reactor, ctx->conn->fd,
                               ctx->recv_buf + ctx->recv_have,
                               sizeof ctx->recv_buf - ctx->recv_have, 0,
                               ctx, my_probe_on_recv);
    if (rc < 0) my_probe_handle_failure(ctx, "reactor_recv launch failed");
}

/** Try to consume one framed MySQL packet from the head of recv_buf.
 *  Returns: >0 bytes consumed; 0 not enough data; -1 protocol error.
 *  Sets *out_done=true when the response is complete (final EOF/OK). */
static ssize_t my_probe_consume_one(my_probe_ctx_t* ctx, bool* out_done)
{
    *out_done = false;
    my_probe_parse_status_t st;
    size_t consumed = my_probe_parse_response(ctx->recv_buf, ctx->recv_have,
                                              &ctx->pkt_index, &st,
                                              &ctx->last_result,
                                              &ctx->last_result_valid);
    switch (st) {
    case MY_PARSE_NEED_MORE: return 0;
    case MY_PARSE_ERROR:     return -1;
    case MY_PARSE_DONE:      *out_done = true; return (ssize_t)consumed;
    case MY_PARSE_CONSUMED:  return (ssize_t)consumed;
    }
    return -1;
}

static void my_probe_on_recv(void* userdata, int result)
{
    my_probe_ctx_t* ctx = (my_probe_ctx_t*)userdata;
    if (!ctx) return;
    if (result <= 0) {
        my_probe_handle_failure(ctx, result == 0 ? "EOF" : "recv error");
        return;
    }
    ctx->recv_have += (size_t)result;

    for (;;) {
        bool done = false;
        ssize_t consumed = my_probe_consume_one(ctx, &done);
        if (consumed < 0) {
            my_probe_handle_failure(ctx, "protocol error");
            return;
        }
        if (consumed == 0) break;
        memmove(ctx->recv_buf, ctx->recv_buf + consumed,
                ctx->recv_have - (size_t)consumed);
        ctx->recv_have -= (size_t)consumed;

        if (done) {
            if (!ctx->last_result_valid) {
                /* Empty result set or unexpected sequence — treat as
                 * "not reached" rather than failure; we still got a
                 * clean end-of-response from the server. */
                ctx->last_result = false;
                ctx->last_result_valid = true;
            }
            my_probe_handle_success(ctx);
            return;
        }
    }
    my_probe_post_recv(ctx);
}

/* ============================================================================
 * Public driver (called from keel_catchup_manager_tick)
 * ============================================================================ */

void keel_catchup_my_drive(struct keel_catchup_manager* m,
                           size_t server_index,
                           uint64_t now_ns)
{
    if (!m || server_index >= KEEL_MAX_SERVERS) return;

    my_probe_ctx_t* ctx = my_probe_ctx_get_or_create(m, server_index);
    if (!ctx) return;

    switch (ctx->state) {
    case MY_PROBE_INIT:
    case MY_PROBE_FAILED:
        my_probe_start_connect(ctx);
        return;

    case MY_PROBE_READY:
        my_probe_issue_query(ctx, now_ns);
        return;

    case MY_PROBE_CONNECTING:
    case MY_PROBE_QUERY_SEND:
    case MY_PROBE_QUERY_RECV:
        if (ctx->probe_started_ns &&
            now_ns > ctx->probe_started_ns &&
            (now_ns - ctx->probe_started_ns) >
                (uint64_t)m->cfg.probe_timeout_ms * 1000000ULL)
        {
            my_probe_handle_failure(ctx, "probe timeout");
        }
        return;
    }
}
