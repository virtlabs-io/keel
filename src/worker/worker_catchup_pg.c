/**
 * @file worker_catchup_pg.c
 * @brief Reactor-async PostgreSQL replica catch-up probe state machine.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * # Phase 2b — what this file does
 *
 * For every replica that has at least one parked catch-up waiter (see
 * worker_catchup.c), this module runs a tiny reactor-driven state
 * machine over one dedicated probe socket:
 *
 *   IDLE → CONNECTING → READY ⇄ QUERY_SEND → QUERY_RECV → READY
 *                         │                                 │
 *                         └──────── FAILED (backoff) ←──────┘
 *
 * The CONNECTING phase delegates the entire TCP + SCRAM-SHA-256 + TLS
 * negotiation to the existing async backend-connect library
 * (`backend_async_start` in backend_connect_async.c), so we get the full
 * authentication machinery without duplicating it.
 *
 * Once authenticated, the probe socket is kept warm and reused for every
 * subsequent catch-up query against that replica. A single query
 *
 *     SELECT
 *         (pg_last_wal_replay_lsn() IS NULL)        -- primary: always reached
 *         OR
 *         (pg_last_wal_replay_lsn() >= '$lsn'::pg_lsn);
 *
 * is sent, the boolean result parsed from the single DataRow ('D')
 * message, and on ReadyForQuery ('Z') all parked waiters whose token
 * is `<=` the probed token (by LSN, same timeline) are released with
 * `KEEL_CATCHUP_REACHED`.
 *
 * On any failure (socket EOF, ErrorResponse, malformed reply) the
 * socket is closed and the per-server backoff window is escalated via
 * `keel_catchup_apply_backoff`; waiters stay parked until either the
 * next successful probe or their own deadline fires
 * `KEEL_CATCHUP_TIMEOUT`.
 *
 * # Memory model
 *
 * One `pg_probe_ctx_t` per server index, lazily allocated on first
 * drive, freed via `probe_state_free` on manager destroy. The probe
 * `backend_conn_t` is also heap-owned by the ctx — it is NEVER inserted
 * into the real backend pool's lists, so it cannot be borrowed by
 * traffic.
 */

#include "worker_catchup_internal.h"

#include "keel/engine/backend_connect.h"
#include "keel/engine/backend_pool.h"
#include "keel/engine/worker.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel/reactor/reactor.h"
#include "keel/util/endian.h"
#include "keel/util/util.h"

#include <ctype.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ============================================================================
 * Probe state
 * ============================================================================ */

typedef enum pg_probe_state {
    PG_PROBE_INIT = 0,    /**< No backend_conn yet allocated. */
    PG_PROBE_CONNECTING,  /**< `backend_async_start` is running. */
    PG_PROBE_READY,       /**< Authenticated, no query in flight. */
    PG_PROBE_QUERY_SEND,  /**< Sending 'Q' message. */
    PG_PROBE_QUERY_RECV,  /**< Reading reply. */
    PG_PROBE_FAILED,      /**< Socket closed; will reconnect on next drive. */
} pg_probe_state_t;

typedef struct pg_probe_ctx {
    keel_catchup_manager_t*  mgr;
    size_t                   server_index;
    pg_probe_state_t         state;

    backend_pool_t*          pool;        /**< Borrowed: worker->server_pools[idx]. */
    backend_conn_t*          conn;        /**< Heap-owned, NEVER on a pool list. */

    /* The token currently being probed. We always probe the highest LSN
     * across all parked waiters for this server (LSN ordering is total). */
    keel_consistency_token_t probing_token;
    uint64_t                 probe_started_ns;

    /* Send buffer for the 'Q' message. */
    uint8_t                  send_buf[512];
    size_t                   send_len;
    size_t                   send_off;

    /* Receive buffer. PG replies for our boolean SELECT are tiny
     * (~50 bytes: T, D, C, Z), so 4 KiB is generous. */
    uint8_t                  recv_buf[4096];
    size_t                   recv_have;

    /* Latest parsed DataRow boolean. */
    bool                     last_result;
    bool                     last_result_valid;
} pg_probe_ctx_t;

/* ============================================================================
 * LSN parsing and token comparison
 * ============================================================================ */

/** Parse a PostgreSQL LSN string ("HHHH/LLLLLLLL") into a uint64_t.
 *  Returns false on malformed input. */
static bool pg_lsn_parse(const char* s, uint64_t* out)
{
    if (!s || !*s || !out) return false;
    uint32_t hi = 0, lo = 0;
    int n = sscanf(s, "%x/%x", &hi, &lo);
    if (n != 2) return false;
    *out = ((uint64_t)hi << 32) | lo;
    return true;
}

/** Compare two LSN tokens. Returns -1/0/1. Treats unparseable tokens as 0
 *  so callers don't silently misorder. Different timeline_ids are also
 *  ordered by their numeric value so the picker is deterministic. */
static int pg_token_compare(const keel_consistency_token_t* a,
                            const keel_consistency_token_t* b)
{
    if (a->timeline_id != b->timeline_id) {
        return (a->timeline_id < b->timeline_id) ? -1 : 1;
    }
    uint64_t la = 0, lb = 0;
    pg_lsn_parse(a->value, &la);
    pg_lsn_parse(b->value, &lb);
    if (la == lb) return 0;
    return (la < lb) ? -1 : 1;
}

/** A waiter token is satisfied when (same timeline) and (waiter LSN <= reached LSN). */
static bool pg_token_satisfied_by(const keel_consistency_token_t* waiter,
                                  const keel_consistency_token_t* reached)
{
    if (waiter->timeline_id != reached->timeline_id) return false;
    uint64_t lw = 0, lr = 0;
    if (!pg_lsn_parse(waiter->value, &lw))  return false;
    if (!pg_lsn_parse(reached->value, &lr)) return false;
    return lw <= lr;
}

/** Validate that an LSN string contains only the characters we accept,
 *  defending against injection into the parameterless Q message. */
static bool pg_lsn_token_is_safe(const char* value)
{
    if (!value || value[0] == '\0') return false;
    bool saw_slash = false, saw_left = false, saw_right = false;
    for (const char* p = value; *p; p++) {
        unsigned char ch = (unsigned char)*p;
        if (*p == '/') {
            if (saw_slash || !saw_left) return false;
            saw_slash = true;
            continue;
        }
        if (!isxdigit(ch)) return false;
        if (saw_slash) saw_right = true;
        else           saw_left  = true;
    }
    return saw_slash && saw_left && saw_right;
}

/* ============================================================================
 * Forward declarations
 * ============================================================================ */

static void pg_probe_start_connect(pg_probe_ctx_t* ctx);
static void pg_probe_on_connect(struct backend_conn* conn, bool success, void* ud);
static void pg_probe_issue_query(pg_probe_ctx_t* ctx, uint64_t now_ns);
static void pg_probe_on_send(void* userdata, int result);
static void pg_probe_post_recv(pg_probe_ctx_t* ctx);
static void pg_probe_on_recv(void* userdata, int result);
static void pg_probe_handle_failure(pg_probe_ctx_t* ctx, const char* why);
static void pg_probe_handle_success(pg_probe_ctx_t* ctx);
static void pg_probe_close_socket(pg_probe_ctx_t* ctx);

/* ============================================================================
 * Allocation / destruction
 * ============================================================================ */

static void pg_probe_ctx_free(void* p)
{
    pg_probe_ctx_t* ctx = (pg_probe_ctx_t*)p;
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

static pg_probe_ctx_t* pg_probe_ctx_get_or_create(
    keel_catchup_manager_t* m, size_t server_index)
{
    keel_catchup_probe_socket_t* slot = &m->sockets[server_index];
    if (slot->probe_state) return (pg_probe_ctx_t*)slot->probe_state;

    if (!m->worker || !m->worker->server_pools ||
        server_index >= m->worker->server_pool_count ||
        !m->worker->server_pools[server_index] ||
        !m->worker->reactor)
    {
        return NULL;
    }

    pg_probe_ctx_t* ctx = keel_calloc(1, sizeof *ctx);
    if (!ctx) return NULL;
    ctx->mgr          = m;
    ctx->server_index = server_index;
    ctx->state        = PG_PROBE_INIT;
    ctx->pool         = m->worker->server_pools[server_index];

    slot->probe_state      = ctx;
    slot->probe_state_free = pg_probe_ctx_free;
    slot->fd               = -1;
    return ctx;
}

void keel_catchup_pg_close(struct keel_catchup_manager* m, size_t server_index)
{
    if (!m || server_index >= KEEL_MAX_SERVERS) return;
    keel_catchup_probe_socket_t* slot = &m->sockets[server_index];
    if (!slot->probe_state) return;
    pg_probe_ctx_t* ctx = (pg_probe_ctx_t*)slot->probe_state;
    pg_probe_close_socket(ctx);
    /* The ctx itself is freed by the manager's destroy loop via
     * probe_state_free; closing the socket here just lets a future
     * drive() trigger a fresh CONNECT. */
    ctx->state = PG_PROBE_INIT;
}

/* ============================================================================
 * State transitions
 * ============================================================================ */

static void pg_probe_close_socket(pg_probe_ctx_t* ctx)
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
    ctx->last_result_valid = false;
}

static void pg_probe_handle_failure(pg_probe_ctx_t* ctx, const char* why)
{
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_CONN,
        "catchup pg probe[server=%zu] failed: %s", ctx->server_index, why);
    pg_probe_close_socket(ctx);
    ctx->state = PG_PROBE_FAILED;
    keel_catchup_apply_backoff(ctx->mgr, ctx->server_index, (uint64_t)keel_time_now());
}

static void pg_probe_handle_success(pg_probe_ctx_t* ctx)
{
    uint64_t now = (uint64_t)keel_time_now();
    /* Cache the result so concurrent enqueue() calls for the same token
     * hit the fast path without ever parking. */
    keel_catchup_cache_put(ctx->mgr, ctx->server_index, &ctx->probing_token,
                           ctx->last_result, now);
    ctx->mgr->probes_succeeded_total++;

    if (ctx->last_result) {
        /* All parked waiters with same timeline and LSN <= probed LSN
         * are now satisfied. */
        keel_catchup_release_satisfied(ctx->mgr, ctx->server_index,
                                       &ctx->probing_token, now,
                                       pg_token_satisfied_by);
    }
    /* If not reached, leave waiters parked; the next tick will reprobe
     * (no backoff after a clean negative answer — backoff is only for
     * transport / protocol failures). */
    ctx->state = PG_PROBE_READY;
}

/* ============================================================================
 * CONNECTING — delegated to backend_async_start
 * ============================================================================ */

static void pg_probe_start_connect(pg_probe_ctx_t* ctx)
{
    if (!ctx->conn) {
        ctx->conn = keel_calloc(1, sizeof *ctx->conn);
        if (!ctx->conn) {
            pg_probe_handle_failure(ctx, "alloc backend_conn failed");
            return;
        }
        ctx->conn->fd = -1;
    }
    ctx->state = PG_PROBE_CONNECTING;
    ctx->mgr->probes_issued_total++;

    int rc = backend_async_start(ctx->pool, ctx->conn,
                                 ctx->mgr->worker->reactor,
                                 pg_probe_on_connect, ctx);
    if (rc < 0) {
        pg_probe_handle_failure(ctx, "backend_async_start launch failed");
    }
}

static void pg_probe_on_connect(struct backend_conn* conn, bool success, void* ud)
{
    pg_probe_ctx_t* ctx = (pg_probe_ctx_t*)ud;
    if (!ctx) return;
    /* The async SM already populated ctx->conn->fd on success. */
    (void)conn;

    if (!success || ctx->conn->fd < 0) {
        pg_probe_handle_failure(ctx, "async connect/auth failed");
        return;
    }
    ctx->state = PG_PROBE_READY;
    ctx->mgr->sockets[ctx->server_index].fd = ctx->conn->fd;
    ctx->mgr->sockets[ctx->server_index].opened_ns = (uint64_t)keel_time_now();

    /* Immediately try to drive a query — there is at least one parked
     * waiter (otherwise the tick loop wouldn't have called drive()). */
    pg_probe_issue_query(ctx, (uint64_t)keel_time_now());
}

/* ============================================================================
 * QUERY_SEND
 * ============================================================================ */

/** Build the 'Q' Query message into ctx->send_buf. Returns 0 on success. */
static int pg_probe_build_query(pg_probe_ctx_t* ctx,
                                const keel_consistency_token_t* token)
{
    char sql[256];
    int n;
    if (!pg_lsn_token_is_safe(token->value)) {
        return -1;
    }
    /* Single SELECT that returns 't' on both:
     *   - the primary (pg_last_wal_replay_lsn() IS NULL there); or
     *   - a replica that has replayed at least the requested LSN.
     * Otherwise returns 'f'. We don't filter by timeline_id here —
     * pg_token_satisfied_by() enforces the same-timeline rule when
     * releasing waiters. */
    n = snprintf(sql, sizeof sql,
                 "SELECT (pg_last_wal_replay_lsn() IS NULL)"
                 " OR (pg_last_wal_replay_lsn() >= '%s'::pg_lsn);",
                 token->value);
    if (n < 0 || (size_t)n >= sizeof sql) return -1;

    size_t sql_len = (size_t)n;
    size_t total   = 1 + 4 + sql_len + 1;
    if (total > sizeof ctx->send_buf) return -1;

    ctx->send_buf[0] = 'Q';
    keel_be32_put(ctx->send_buf + 1, (uint32_t)(4 + sql_len + 1));
    memcpy(ctx->send_buf + 5, sql, sql_len);
    ctx->send_buf[5 + sql_len] = '\0';
    ctx->send_len = total;
    ctx->send_off = 0;
    return 0;
}

static void pg_probe_issue_query(pg_probe_ctx_t* ctx, uint64_t now_ns)
{
    /* Pick the highest-LSN parked token for this server. */
    keel_consistency_token_t target;
    if (!keel_catchup_pick_probe_token(ctx->mgr, ctx->server_index,
                                       pg_token_compare, &target))
    {
        /* No waiters left (they may have timed out between ticks). */
        return;
    }
    ctx->probing_token     = target;
    ctx->probe_started_ns  = now_ns;
    ctx->last_result_valid = false;
    ctx->last_result       = false;
    ctx->recv_have         = 0;

    if (pg_probe_build_query(ctx, &target) < 0) {
        pg_probe_handle_failure(ctx, "build_query failed (unsafe LSN?)");
        return;
    }

    ctx->state = PG_PROBE_QUERY_SEND;
    int rc = keel_reactor_send(ctx->mgr->worker->reactor, ctx->conn->fd,
                               ctx->send_buf, ctx->send_len, 0,
                               ctx, pg_probe_on_send);
    if (rc < 0) {
        pg_probe_handle_failure(ctx, "reactor_send launch failed");
    }
}

static void pg_probe_on_send(void* userdata, int result)
{
    pg_probe_ctx_t* ctx = (pg_probe_ctx_t*)userdata;
    if (!ctx) return;
    if (result < 0) {
        pg_probe_handle_failure(ctx, "send error");
        return;
    }
    ctx->send_off += (size_t)result;
    if (ctx->send_off < ctx->send_len) {
        /* Partial write — continue sending the rest. */
        int rc = keel_reactor_send(ctx->mgr->worker->reactor, ctx->conn->fd,
                                   ctx->send_buf + ctx->send_off,
                                   ctx->send_len - ctx->send_off, 0,
                                   ctx, pg_probe_on_send);
        if (rc < 0) pg_probe_handle_failure(ctx, "reactor_send resume failed");
        return;
    }
    ctx->state = PG_PROBE_QUERY_RECV;
    pg_probe_post_recv(ctx);
}

/* ============================================================================
 * QUERY_RECV
 * ============================================================================ */

static void pg_probe_post_recv(pg_probe_ctx_t* ctx)
{
    if (ctx->recv_have >= sizeof ctx->recv_buf) {
        pg_probe_handle_failure(ctx, "recv buffer overflow");
        return;
    }
    int rc = keel_reactor_recv(ctx->mgr->worker->reactor, ctx->conn->fd,
                               ctx->recv_buf + ctx->recv_have,
                               sizeof ctx->recv_buf - ctx->recv_have, 0,
                               ctx, pg_probe_on_recv);
    if (rc < 0) pg_probe_handle_failure(ctx, "reactor_recv launch failed");
}

/** Try to consume one framed PG message from the head of recv_buf.
 *  Returns:
 *    >0  bytes consumed (recv_buf updated by caller)
 *     0  not enough data yet
 *    -1  protocol error (caller should fail the probe)
 *
 *  Side-effects: sets *out_done=true when ReadyForQuery ('Z') is seen
 *  (probe round complete); records DataRow boolean into ctx->last_result.
 */
static ssize_t pg_probe_consume_one(pg_probe_ctx_t* ctx, bool* out_done)
{
    *out_done = false;
    if (ctx->recv_have < 5) return 0;

    uint8_t type = ctx->recv_buf[0];
    uint32_t body_len = keel_be32_get(ctx->recv_buf + 1);
    if (body_len < 4 || body_len > sizeof ctx->recv_buf) return -1;
    size_t total = 1 + body_len;
    if (ctx->recv_have < total) return 0;

    const uint8_t* body = ctx->recv_buf + 5;
    uint32_t body_payload_len = body_len - 4;

    switch (type) {
    case 'T':  /* RowDescription — ignore. */
    case 'C':  /* CommandComplete — ignore. */
    case 'N':  /* NoticeResponse — ignore. */
    case 'S':  /* ParameterStatus — ignore. */
        break;

    case 'D': {  /* DataRow — extract bool. */
        if (body_payload_len < 2) return -1;
        uint16_t ncols = (uint16_t)((body[0] << 8) | body[1]);
        if (ncols != 1) return -1;
        if (body_payload_len < 6) return -1;
        int32_t col_len = (int32_t)keel_be32_get(body + 2);
        if (col_len < 0 || (uint32_t)(6 + col_len) > body_payload_len) {
            /* NULL is acceptable: treat as not-reached. */
            ctx->last_result = false;
            ctx->last_result_valid = true;
            break;
        }
        if (col_len >= 1) {
            ctx->last_result = (body[6] == 't');
            ctx->last_result_valid = true;
        }
        break;
    }

    case 'Z':  /* ReadyForQuery — round complete. */
        *out_done = true;
        break;

    case 'E':  /* ErrorResponse. */
        return -1;

    default:
        /* Unknown message; tolerate (e.g., NotificationResponse 'A'). */
        break;
    }
    return (ssize_t)total;
}

static void pg_probe_on_recv(void* userdata, int result)
{
    pg_probe_ctx_t* ctx = (pg_probe_ctx_t*)userdata;
    if (!ctx) return;
    if (result <= 0) {
        pg_probe_handle_failure(ctx, result == 0 ? "EOF" : "recv error");
        return;
    }
    ctx->recv_have += (size_t)result;

    /* Drain all complete messages we have buffered. */
    for (;;) {
        bool done = false;
        ssize_t consumed = pg_probe_consume_one(ctx, &done);
        if (consumed < 0) {
            pg_probe_handle_failure(ctx, "protocol error");
            return;
        }
        if (consumed == 0) break;
        /* Slide the buffer down. */
        memmove(ctx->recv_buf, ctx->recv_buf + consumed,
                ctx->recv_have - (size_t)consumed);
        ctx->recv_have -= (size_t)consumed;

        if (done) {
            if (!ctx->last_result_valid) {
                pg_probe_handle_failure(ctx, "ReadyForQuery without DataRow");
                return;
            }
            pg_probe_handle_success(ctx);
            return;
        }
    }
    /* Need more bytes — re-arm recv. */
    pg_probe_post_recv(ctx);
}

/* ============================================================================
 * Public driver (called from keel_catchup_manager_tick)
 * ============================================================================ */

void keel_catchup_pg_drive(struct keel_catchup_manager* m,
                           size_t server_index,
                           uint64_t now_ns)
{
    if (!m || server_index >= KEEL_MAX_SERVERS) return;

    pg_probe_ctx_t* ctx = pg_probe_ctx_get_or_create(m, server_index);
    if (!ctx) return;

    switch (ctx->state) {
    case PG_PROBE_INIT:
    case PG_PROBE_FAILED:
        pg_probe_start_connect(ctx);
        return;

    case PG_PROBE_READY:
        pg_probe_issue_query(ctx, now_ns);
        return;

    case PG_PROBE_CONNECTING:
    case PG_PROBE_QUERY_SEND:
    case PG_PROBE_QUERY_RECV:
        /* In-flight — wait for the reactor callback. Apply a stuck-probe
         * guard: if the round has been outstanding longer than the
         * configured probe_timeout, treat as failure so we don't pin a
         * waiter forever on a stalled backend. */
        if (ctx->probe_started_ns &&
            now_ns > ctx->probe_started_ns &&
            (now_ns - ctx->probe_started_ns) >
                (uint64_t)m->cfg.probe_timeout_ms * 1000000ULL)
        {
            pg_probe_handle_failure(ctx, "probe timeout");
        }
        return;
    }
}
