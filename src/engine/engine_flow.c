/**
 * @file engine_flow.c
 * @brief Protocol-Agnostic Engine Flow Implementation
 *
 * Drives sessions through flow phases using protocol plugin vtables.
 * This file is the bridge between the worker's I/O and the protocol plugins.
 *
 * CRITICAL: This code never touches protocol framing or auth directly.
 *           It asks the protocol plugin via vtable methods.
 *
 * FLOW:
 *   FE recv → frame_len loop → on_fe_msg → dispatch action:
 *     SSL_REQUEST   → send(FE, 'N'), re-arm FE recv
 *     AUTH_COMPLETE  → send(FE, handshake), phase=READY, re-arm FE recv
 *     QUERY         → borrow backend → send(BE, payload) → arm BE recv
 *     FORWARD       → send(BE, payload) → arm BE recv
 *     TERMINATE     → close session
 *
 *   BE recv → frame_len loop → on_be_msg → dispatch action:
 *     FORWARD_FE    → send(FE, payload)
 *     query_complete + reusable → return BE to pool, re-arm FE recv
 *     query_complete + !reusable → keep BE, re-arm FE recv
 *     more data     → re-arm BE recv
 */

#include "keel/engine/engine_flow.h"
#include "engine_private.h"           /* cross-worker session lookup for cancel */
#include "keel/trace/trace.h"
#include "keel/protocol/tls_context.h"
#include "keel/log/log.h"
#include "keel/util/diag_ps.h"
#include "keel/log/audit_log.h"
#include "keel/core/stats.h"
#include "keel/log/query_log.h"
#include "keel/sql/sql.h"
#include "keel/sql/query_tree.h"
#include "keel/session/ssv.h"
#include "keel/session/state_profile.h"
#include "keel/reactor/io_splice.h"     /* for zero-copy splice operations */
#include "keel/plugin/plugin.h"        /* for Phase 5 plugin helpers */
#include "keel/engine/backend_pool.h"
#include "keel_hook.h"                 /* Hook/trigger system */
#include "keel/core/sharding.h"        /* keel_shard_key_t — for hook shard info */
#include "keel/mem/mem.h"
#include "keel/protocol/tls_context.h"
#include "keel/core/router.h"          /* keel_router_dispatch_sql() */
#include "keel/engine/catchup_consult.h" /* Patch 2d-4 wait-catchup verdict */
#include "keel/core/query_cache.h"     /* keel_query_cache_get/put/digest */
#include "engine_scatter.h"            /* keel_engine_scatter_execute() */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdatomic.h>
#include <time.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include "keel/util/platform_compat.h"

/* Helper to get the instrumentation context from a worker pointer */
#define WORKER_INSTR(w) ((w)->stats_ctx ? &(w)->stats_ctx->instr : NULL)

/* Small and medium frontend messages must be reassembled before the protocol
 * plugin sees them.  Very large messages may still use the existing streaming
 * path once a backend is available, preserving the jumbo-message behavior. */
#define KEEL_FE_PARTIAL_BUFFER_LIMIT (KEEL_RECV_BUF_SIZE / 2u)

/* ============================================================================
 * Fire-and-forget async CancelRequest
 *
 * Replaces the old synchronous connect+send+close with a fully async
 * state machine: io_uring connect → io_uring send → close.
 * ============================================================================ */

typedef struct {
    int              fd;
    keel_reactor_t*   reactor;
    uint8_t          payload[64];
    size_t           payload_len;
} cancel_async_ctx_t;

/**
 * @brief io_uring send-completion callback that tears down the cancel connection.
 *
 * Called by the reactor after the cancel payload has been delivered (or
 * failed).  Closes the ephemeral file descriptor and frees the
 * `cancel_async_ctx_t` context regardless of whether the send succeeded.
 *
 * @param userdata Pointer to the owning `cancel_async_ctx_t`.
 * @param result   Bytes sent (>= 0) or negative errno on failure.
 */
static void cancel_on_sent(void* userdata, int result)
{
    cancel_async_ctx_t* ctx = (cancel_async_ctx_t*)userdata;
    close(ctx->fd);
    keel_free(ctx);
}

/**
 * @brief Synchronise the session's pin-reason summary from flow-level pin state.
 *
 * Implementation lives in session_flow_pins.c so callers that only need
 * pin bookkeeping do not transitively pull in backend_pool dependencies.
 */

/**
 * @brief io_uring connect-completion callback that advances the cancel handshake.
 *
 * On a successful connect, kicks off the non-blocking send of the cancel
 * payload via `keel_reactor_send()` with `cancel_on_sent` as the next
 * completion.  On connect failure, closes the file descriptor and frees the
 * context immediately.
 *
 * @param userdata Pointer to the owning `cancel_async_ctx_t`.
 * @param result   0 on successful connect, or negative errno on failure.
 */
static void cancel_on_connected(void* userdata, int result)
{
    cancel_async_ctx_t* ctx = (cancel_async_ctx_t*)userdata;
    if (result < 0) {
        close(ctx->fd);
        keel_free(ctx);
        return;
    }
    /* Send cancel payload, then close on completion */
    if (keel_reactor_send(ctx->reactor, ctx->fd, ctx->payload, ctx->payload_len,
                         MSG_NOSIGNAL, ctx, cancel_on_sent) < 0) {
        close(ctx->fd);
        keel_free(ctx);
    }
}

/**
 * @brief Queue a fire-and-forget CancelRequest to the backend via io_uring.
 *
 * Opens a new TCP connection, sends the cancel key, and closes — all async.
 * Never blocks the reactor thread.  Silently drops the cancel on any failure.
 */
static void cancel_request_async(keel_reactor_t* reactor,
                                 const struct sockaddr_in* addr,
                                 const uint8_t* payload, size_t payload_len)
{
    if (!reactor || !addr || !payload || payload_len == 0 || payload_len > 64) return;

    int fd = keel_socket_nonblock(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return;

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    cancel_async_ctx_t* ctx = keel_calloc(1, sizeof(*ctx));
    if (!ctx) { close(fd); return; }

    ctx->fd = fd;
    ctx->reactor = reactor;
    memcpy(ctx->payload, payload, payload_len);
    ctx->payload_len = payload_len;

    if (keel_reactor_connect(reactor, fd, (const struct sockaddr*)addr, sizeof(*addr),
                            ctx, cancel_on_connected) < 0) {
        close(fd); keel_free(ctx);
    }
}

/* Debug logging - set to 0 for production to eliminate hot path overhead */
#ifndef KEEL_ENGINE_FLOW_DEBUG
#define KEEL_ENGINE_FLOW_DEBUG 0
#endif

#if KEEL_ENGINE_FLOW_DEBUG
#define KEEL_DEBUG_LOG(...) KEEL_LOG_TRACE(KEEL_LOG_CAT_IO, __VA_ARGS__)
#else
#define KEEL_DEBUG_LOG(...) ((void)0)
#endif

/* ============================================================================
 * Hook Integration Helpers
 * ============================================================================ */

/**
 * @brief Populate a hook context from session + frontend action.
 *
 * Fills read-only session info and mutable query/routing fields so that
 * hook callbacks can inspect and modify the query pipeline state.
 */
static inline void engine_fill_hook_ctx(
    keel_hook_ctx_t*       ctx,
    const keel_session_t*  session,
    const keel_fe_action_t* act,
    const keel_qt_query_t* qt)   /* NULL when parse has not run yet */
{
    memset(ctx, 0, sizeof(*ctx));

    /* Read-only session info */
    ctx->session_id     = session->id;
    ctx->username       = session->username;
    ctx->database       = session->database;
    ctx->client_fd      = session->client_fd;
    ctx->server_fd      = session->server_fd;
    ctx->in_transaction = session->in_transaction;
    ctx->query_count    = session->query_count;

    /* Query data from frontend action */
    ctx->raw_query      = act->be_payload;
    ctx->raw_query_len  = act->be_payload_len;
    ctx->sql_text       = act->sql_view;
    ctx->sql_text_len   = act->sql_view_len;

    /* Full parsed query tree (NULL before AFTER_QUERY_PARSE or for infra hooks) */
    ctx->query_tree     = qt;

    /* Classification */
    ctx->query_type     = act->query_type;
    ctx->effect_flags   = (uint32_t)act->effect;
    ctx->needs_primary  = (act->route_hint == KEEL_FROUTE_PRIMARY);

    /* Derive query_flags from effect flags */
    ctx->query_flags    = 0;
    if (act->effect & KEEL_QE_READONLY)   ctx->query_flags |= (1 << 0); /* READ_ONLY */
    if (act->effect & KEEL_QE_WRITE)      ctx->query_flags |= (1 << 1); /* WRITE */
    if (act->effect & KEEL_QE_DDL)        ctx->query_flags |= (1 << 2); /* DDL */
    if (act->effect & (KEEL_QE_BEGINS_TX|KEEL_QE_ENDS_TX))
                                          ctx->query_flags |= (1 << 3); /* TRANSACTION */
    if (act->effect & KEEL_QE_SETS_STATE) ctx->query_flags |= (1 << 4); /* SESSION */
    if (act->effect & KEEL_QE_MULTI_STMT) ctx->query_flags |= (1 << 5); /* MULTI */;

    /* Routing */
    ctx->route_hint     = (act->route_hint == KEEL_FROUTE_REPLICA)
                          ? KEEL_HOOK_ROUTE_REPLICA
                          : (act->route_hint == KEEL_FROUTE_PRIMARY)
                            ? KEEL_HOOK_ROUTE_PRIMARY
                            : KEEL_HOOK_ROUTE_ANY;
    ctx->pin_update     = (uint32_t)act->pin_update;
    ctx->pin_clear      = (uint32_t)act->pin_clear;
    ctx->splice_eligible= act->splice_eligible;

    /* Payload for BEFORE_SEND */
    ctx->be_payload     = act->be_payload;
    ctx->be_payload_len = act->be_payload_len;
}

/**
 * @brief Apply hook modifications back to the frontend action.
 *
 * After a hook modifies ctx->route_hint, ctx->effect_flags etc,
 * propagate changes back into the engine action structure.
 */
static inline void engine_apply_hook_ctx(
    const keel_hook_ctx_t* ctx,
    keel_fe_action_t*      act)
{
    /* Routing hint */
    switch (ctx->route_hint) {
    case KEEL_HOOK_ROUTE_PRIMARY: act->route_hint = KEEL_FROUTE_PRIMARY; break;
    case KEEL_HOOK_ROUTE_REPLICA: act->route_hint = KEEL_FROUTE_REPLICA; break;
    case KEEL_HOOK_ROUTE_ANY:     act->route_hint = KEEL_FROUTE_ANY;     break;
    }

    /* Effect flags */
    act->effect = (keel_query_effect_flags_t)ctx->effect_flags;

    /* Pin updates */
    act->pin_update = (keel_flow_pin_reason_t)ctx->pin_update;
    act->pin_clear  = (keel_flow_pin_reason_t)ctx->pin_clear;

    /* Splice eligibility */
    act->splice_eligible = ctx->splice_eligible;
}

/**
 * @brief Fill a keel_hook_shard_ctx_t from a dispatch result.
 *
 * Snapshot only — no pointers to engine-internal routing state are stored.
 * The caller must zero-initialise @p sctx before calling.
 * After filling, set hctx->ext = sctx.
 *
 * All merge plan fields (order_keys, agg_specs, group_key_cols, having_preds,
 * avg_finalize_specs, window_col_specs, limit_count, limit_offset) are copied
 * verbatim from the dispatch result so hooks have full access to the plan.
 * 2PC state (twopc_required, participating_shards_mask) is also captured.
 */
static inline void engine_fill_shard_ctx(
    keel_hook_shard_ctx_t*       sctx,
    const keel_dispatch_result_t* dr)
{
    memset(sctx, 0, sizeof(*sctx));

    /* ---- Merge plan (mirrors keel_dispatch_result_t exactly) ---- */
    sctx->requires_merge          = dr->requires_merge;
    sctx->requires_avg_rewrite    = dr->requires_avg_rewrite;
    sctx->requires_count_distinct = dr->requires_count_distinct;
    sctx->has_window_funcs        = dr->has_window_funcs;
    sctx->window_forced_single    = dr->window_forced_single;
    memcpy(sctx->count_distinct_col, dr->count_distinct_col,
           sizeof(sctx->count_distinct_col));

    /* ORDER BY / LIMIT */
    sctx->norder_keys  = dr->norder_keys;
    sctx->limit_count  = dr->limit_count;
    sctx->limit_offset = dr->limit_offset;
    if (dr->norder_keys > 0)
        memcpy(sctx->order_keys, dr->order_keys,
               dr->norder_keys * sizeof(sctx->order_keys[0]));

    /* Aggregate specs */
    sctx->nagg_specs = dr->nagg_specs;
    if (dr->nagg_specs > 0)
        memcpy(sctx->agg_specs, dr->agg_specs,
               dr->nagg_specs * sizeof(sctx->agg_specs[0]));

    /* GROUP BY specs */
    sctx->ngroup_key_cols = dr->ngroup_key_cols;
    if (dr->ngroup_key_cols > 0)
        memcpy(sctx->group_key_cols, dr->group_key_cols,
               dr->ngroup_key_cols * sizeof(sctx->group_key_cols[0]));

    /* HAVING predicates */
    sctx->nhaving_preds = dr->nhaving_preds;
    if (dr->nhaving_preds > 0)
        memcpy(sctx->having_preds, dr->having_preds,
               dr->nhaving_preds * sizeof(sctx->having_preds[0]));

    /* AVG finalize specs */
    sctx->navg_finalize_specs = dr->navg_finalize_specs;
    if (dr->navg_finalize_specs > 0)
        memcpy(sctx->avg_finalize_specs, dr->avg_finalize_specs,
               dr->navg_finalize_specs * sizeof(sctx->avg_finalize_specs[0]));

    /* Window function specs */
    sctx->nwindow_col_specs = dr->nwindow_col_specs;
    if (dr->nwindow_col_specs > 0)
        memcpy(sctx->window_col_specs, dr->window_col_specs,
               dr->nwindow_col_specs * sizeof(sctx->window_col_specs[0]));

    /* 2PC state */
    sctx->twopc_required            = dr->twopc_required;
    sctx->participating_shards_mask  =
        (dr->kind == KEEL_DISPATCH_SCATTER)
        ? dr->scatter.participating_shards_mask : 0u;

    if (dr->kind == KEEL_DISPATCH_SINGLE) {
        sctx->dispatch_kind      = KEEL_HOOK_DISPATCH_SINGLE;
        sctx->single_shard_index = dr->single.shard_index;
        sctx->shard_count        = 1;

        keel_hook_shard_info_t* si = &sctx->shards[0];
        si->shard_index     = dr->single.shard_index;
        si->is_write        = !dr->single.is_read;
        si->server_available= (dr->single.server != NULL);
        if (dr->single.server) {
            si->is_healthy  = true;  /* if server was selected it passed health */
        }
        /* shard_key left zeroed (kind == KEEL_SHARD_KEY_NONE) */
    } else {
        /* SCATTER */
        sctx->dispatch_kind = KEEL_HOOK_DISPATCH_SCATTER;
        const keel_scatter_plan_t* plan = &dr->scatter;
        size_t count = (plan->count < KEEL_HOOK_MAX_SHARDS)
                        ? plan->count : KEEL_HOOK_MAX_SHARDS;
        sctx->shard_count          = count;
        sctx->scatter_shards_fail  = plan->failed;
        sctx->scatter_shards_ok    = (count > plan->failed) ? count - plan->failed : 0;

        for (size_t i = 0; i < count; i++) {
            const keel_route_decision_t* dec = &plan->decisions[i];
            keel_hook_shard_info_t* si = &sctx->shards[i];
            si->shard_index      = dec->shard_index;
            si->is_write         = !dec->is_read;
            si->server_available = (dec->server != NULL);
            si->is_healthy       = (dec->server != NULL);
            /* shard_key left zeroed for scatter (fan-out, no single key) */
        }
    }
}

/**
 * @brief Annotate scatter timing + result stats onto an already-filled
 *        shard context.  Call after keel_engine_scatter_execute() returns.
 */
static inline void engine_annotate_scatter_result(
    keel_hook_shard_ctx_t* sctx,
    uint64_t               elapsed_us,
    size_t                 rows_merged,
    bool                   spilled)
{
    sctx->scatter_elapsed_us  = elapsed_us;
    sctx->scatter_rows_merged = rows_merged;
    sctx->scatter_spilled     = spilled;
}

/* Default sticky-primary TTL: 100 ms.  Read-after-write within this window
 * is routed to the primary even if the query is read-only, avoiding stale
 * reads from asynchronous replicas. */
#ifndef KEEL_STICKY_PRIMARY_TTL_MS
#define KEEL_STICKY_PRIMARY_TTL_MS 100
#endif

#ifndef KEEL_CONSISTENCY_CAPTURE_TIMEOUT_MS
#define KEEL_CONSISTENCY_CAPTURE_TIMEOUT_MS 50
#endif

/**
 * @brief Cheap monotonic timestamp in nanoseconds (CLOCK_MONOTONIC_COARSE).
 *
 * On Linux, CLOCK_MONOTONIC_COARSE is a vDSO call (~10 ns) with ~1 ms
 * resolution — perfectly adequate for sticky-primary gating.
 */
static inline uint64_t engine_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/**
 * @brief Non-blocking send — tries to drain the buffer without blocking.
 *
 * Loops send(MSG_NOSIGNAL) until the full buffer is sent or the kernel
 * returns EAGAIN.  Never calls poll/epoll — if the socket buffer is full,
 * the caller must defer the remainder to io_uring.
 *
 * @return bytes actually sent (may be < len), or -1 on hard error.
 */
ssize_t keel_try_send_nb(int fd, const void* buf, size_t len)
{
    const uint8_t* p = (const uint8_t*)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t s = send(fd, p + sent, len - sent, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (s < 0 && errno == EPERM) {
            /* Some sandbox/seccomp environments deny send* while allowing
             * write() on connected sockets. Preserve non-blocking semantics:
             * the fd is always O_NONBLOCK so write() will return EAGAIN rather
             * than blocking if the send buffer is full. */
            s = write(fd, p + sent, len - sent); /* NOLINT(keel-blocking) */
        }
        if (s > 0) {
            sent += (size_t)s;
            continue;
        }
        if (s < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            break;   /* socket buffer full — return partial */
        return -1;   /* hard error (EPIPE, ECONNRESET, etc.) */
    }
    return (ssize_t)sent;
}

/**
 * @brief Stash unsent data for the worker to complete via io_uring.
 *
 * Heap-copies the remaining bytes and records the target fd + the flow
 * result the worker should act on once the send finishes.
 */
/* Captured FE payloads bypass the regular frame-by-frame on_fe_msg path.
 * Ask the protocol plugin to report any pin transitions hidden in those
 * deferred bytes, keeping the engine wire-format agnostic. */
static void apply_captured_fe_pin_effects(keel_session_flow_t* sf,
                                          keel_session_t* session,
                                          keel_worker_t* worker,
                                          const uint8_t* buf,
                                          size_t len)
{
    if (!sf || !sf->flow || !sf->flow->captured_fe_pin_effects || !buf || len == 0)
        return;

    keel_flow_pin_reason_t pin_update = KEEL_FPIN_NONE;
    keel_flow_pin_reason_t pin_clear = KEEL_FPIN_NONE;
    sf->flow->captured_fe_pin_effects(sf->ctx, buf, len,
                                      &pin_update, &pin_clear);
    keel_session_flow_apply_pin_change(sf, session, worker, pin_update, pin_clear);
}

/**
 * @brief Run protocol classification (on_fe_msg) on each message in a
 *        FE buffer that is about to be forwarded RAW to the backend
 *        (e.g. via pre_query stash for state_sync / stmt_replay).
 *
 * Background: when state_sync or stmt_replay triggers on a pipelined
 * FE message (Parse/Bind/.../Sync batched into one recv), the dispatch
 * loop processes only the TRIGGER msg through on_fe_msg and then stashes
 * the entire tail (data + pos … data + len) into pending_pre_query_buf
 * for verbatim forwarding after the pre-query op completes.  Without
 * this prefetch, the tail's Parse messages bypass the protocol plugin's
 * stmt_cache update, so prepared-statements declared in the tail are
 * never tracked for replay → "prepared statement S_N does not exist"
 * after the backend is later reclaimed/cleaned.
 *
 * @param sf          Session flow context.
 * @param buf         Follow buffer (typically `data + pos`).
 * @param buf_len     Total bytes in @p buf.
 * @param skip_bytes  Bytes at the head that were already classified
 *                    (typically the trigger msg's frame length).
 */
/* Exposed (non-static) so tests can exercise it directly; not part of the
 * public engine_flow API.  Declared in tests via a local extern. */
void keel_engine_flow_prefetch_classify_follow_buf(keel_session_flow_t* sf,
                                                   const uint8_t* buf,
                                                   size_t buf_len,
                                                   size_t skip_bytes)
{
    if (!sf || !sf->ctx || !sf->flow || !sf->flow->on_fe_msg ||
        !sf->flow->frame_len ||
        !buf || buf_len <= skip_bytes)
        return;

    size_t p = skip_bytes;
    while (p < buf_len) {
        ssize_t fl = sf->flow->frame_len(sf->ctx, buf + p, buf_len - p, 0);
        if (fl <= 0 || (size_t)fl > buf_len - p)
            return;
        keel_fe_action_t throwaway = keel_fe_action_default();
        (void)sf->flow->on_fe_msg(sf->ctx, buf + p, (size_t)fl, &throwaway);
        p += (size_t)fl;
    }
}

/**
 * @brief Stash unsent data for completion by the io_uring send path.
 *
 * Called when a non-blocking `send()` loop returns a partial write (the
 * kernel socket buffer is full).  Heap-copies the remaining bytes into
 * `sf->pending_send_buf`, records the target @p fd and the @p resume
 * flow-result to apply once the deferred send completes, and returns
 * `KEEL_FLOW_SEND_PENDING` to signal the worker that I/O is in flight.
 *
 * @param sf     Session flow context that owns the pending-send fields.
 * @param fd     File descriptor the bytes should ultimately be written to.
 * @param buf    Start of the unsent data.
 * @param len    Number of bytes remaining to send.
 * @param resume Flow result to apply after the send completes.
 * @return `KEEL_FLOW_SEND_PENDING` on success, `KEEL_FLOW_ERROR` on OOM.
 */
static keel_flow_result_t defer_send(keel_session_flow_t* sf,
                                    int fd,
                                    const void* buf, size_t len,
                                    keel_flow_result_t resume)
{
    if (sf->pending_send_cap < len) {
        uint8_t* new_buf = keel_malloc(len);
        if (!new_buf) return KEEL_FLOW_ERROR;
        keel_free(sf->pending_send_buf);
        sf->pending_send_buf = new_buf;
        sf->pending_send_cap = len;
    }

    if (!sf->pending_send_buf) return KEEL_FLOW_ERROR;
    memcpy(sf->pending_send_buf, buf, len);
    sf->pending_send_len    = len;
    sf->pending_send_off    = 0;
    sf->pending_send_fd     = fd;
    sf->pending_send_resume = resume;
    return KEEL_FLOW_SEND_PENDING;
}

#define ENGINE_SETUP_CLEANUP_BUFSZ 2048
/* Keep setup payload queue intentionally small: only the ordered
 * pre-query chain (BEGIN/state-sync/replay/cleanup) is supported. */
#define ENGINE_PRE_QUERY_OP_CAP 4

static void pre_query_reset_active(keel_session_flow_t* sf)
{
    sf->pending_pre_query = KEEL_PRE_QUERY_NONE;
    sf->pending_pre_query_absorbed = 0;
    sf->pending_state_sync_hash = 0;
    sf->pending_pre_query_resume = KEEL_FLOW_OK;
}

static void pre_query_free_op(keel_pre_query_op_t* op)
{
    if (!op)
        return;
    if (op->payload) {
        keel_free(op->payload);
        op->payload = NULL;
    }
    memset(op, 0, sizeof(*op));
}

static void pre_query_clear_queue(keel_session_flow_t* sf)
{
    if (!sf)
        return;
    for (size_t i = 0; i < ENGINE_PRE_QUERY_OP_CAP; i++)
        pre_query_free_op(&sf->pre_query_ops[i]);
    sf->pre_query_head = 0;
    sf->pre_query_tail = 0;
    sf->pre_query_count = 0;
}

static void pre_query_fail_clear(keel_session_flow_t* sf)
{
    if (!sf)
        return;
    pre_query_reset_active(sf);
    sf->pending_pre_query_len = 0;
    sf->stmt_replay_needs_cleanup = false;
    sf->stmt_replay_count = 0;
    sf->stmt_replay_rfq_pending = false;
    sf->stmt_replay_hash = 0;
    pre_query_clear_queue(sf);
}

static void pre_query_record_result(keel_session_flow_t* sf,
                                    keel_worker_t* worker,
                                    keel_replay_result_t result)
{
    if (!sf)
        return;

    uint64_t dur_ns = 0;
    if (sf->pending_pre_query_started_ns > 0) {
        int64_t now_ns = keel_stats_now_ns();
        if (now_ns > 0 &&
            (uint64_t)now_ns > sf->pending_pre_query_started_ns) {
            dur_ns = (uint64_t)now_ns - sf->pending_pre_query_started_ns;
        }
    }

    sf->replay_last_result = result;
    sf->replay_last_duration_ns = dur_ns;
    sf->pending_pre_query_started_ns = 0;

    if (!worker || !worker->stats_ctx)
        return;

    switch (result) {
    case KEEL_REPLAY_RESULT_SUCCESS:
        KEEL_STAT_INC(worker->stats_ctx, replay_result_success);
        break;
    case KEEL_REPLAY_RESULT_PARSE_ERROR:
        KEEL_STAT_INC(worker->stats_ctx, replay_result_parse_error);
        break;
    case KEEL_REPLAY_RESULT_DRAIN_ERROR:
        KEEL_STAT_INC(worker->stats_ctx, replay_result_drain_error);
        break;
    case KEEL_REPLAY_RESULT_TIMEOUT:
        KEEL_STAT_INC(worker->stats_ctx, replay_result_timeout);
        break;
    case KEEL_REPLAY_RESULT_OOM:
        KEEL_STAT_INC(worker->stats_ctx, replay_result_oom);
        break;
    case KEEL_REPLAY_RESULT_PARTIAL_SEND_FAILURE:
        KEEL_STAT_INC(worker->stats_ctx, replay_result_partial_send_failure);
        break;
    case KEEL_REPLAY_RESULT_NONE:
        break;
    }

    if (dur_ns > 0)
        KEEL_STAT_LATENCY(worker->stats_ctx, replay_duration_ns, dur_ns);
}

static bool pre_query_has_active_op(const keel_session_flow_t* sf)
{
    if (!sf)
        return false;
    return sf->pending_pre_query != KEEL_PRE_QUERY_NONE ||
           sf->stmt_replay_needs_cleanup ||
           sf->stmt_replay_count > 0 ||
           sf->stmt_replay_rfq_pending;
}

static bool pre_query_queue_push(keel_session_flow_t* sf,
                                 keel_pre_query_op_type_t type,
                                 const uint8_t* payload,
                                 size_t payload_len,
                                 uint64_t state_sync_hash,
                                 keel_flow_result_t resume,
                                 uint32_t expected_msgs)
{
    if (!sf || !payload || payload_len == 0)
        return false;
    if (sf->pre_query_count >= ENGINE_PRE_QUERY_OP_CAP)
        return false;

    uint8_t idx = sf->pre_query_tail;
    keel_pre_query_op_t* op = &sf->pre_query_ops[idx];
    memset(op, 0, sizeof(*op));
    op->payload = (uint8_t*)keel_malloc(payload_len);
    if (!op->payload)
        return false;
    memcpy(op->payload, payload, payload_len);
    op->payload_len = payload_len;
    op->type = type;
    op->state = KEEL_PQSTATE_IDLE;
    op->state_sync_hash = state_sync_hash;
    op->resume = resume;
    op->expected_msgs = expected_msgs;

    sf->pre_query_tail = (uint8_t)((sf->pre_query_tail + 1u) %
                                   ENGINE_PRE_QUERY_OP_CAP);
    sf->pre_query_count++;
    return true;
}

static keel_flow_result_t pre_query_start_op(keel_session_flow_t* sf,
                                             keel_session_t* session,
                                             keel_pre_query_op_t* op)
{
    keel_worker_t* worker = session ? session->worker : NULL;
    if (!sf || !session || !op || !op->payload || op->payload_len == 0)
        return KEEL_FLOW_ERROR;

    if (sf->pending_pre_query_started_ns == 0)
        sf->pending_pre_query_started_ns = (uint64_t)keel_stats_now_ns();

    sf->pending_pre_query_absorbed = 0;
    sf->pending_pre_query_resume = op->resume;
    sf->pending_state_sync_hash = op->state_sync_hash;
    if (op->type == KEEL_PQOP_STATE_SYNC ||
        op->type == KEEL_PQOP_DEFERRED_BEGIN) {
        sf->pending_pre_query = (op->type == KEEL_PQOP_STATE_SYNC)
            ? KEEL_PRE_QUERY_STATE_SYNC
            : KEEL_PRE_QUERY_BEGIN_REPLAY;
    } else {
        sf->pending_pre_query = KEEL_PRE_QUERY_NONE;
    }
    if (op->type == KEEL_PQOP_CLEAN_CHECK) {
        sf->stmt_replay_needs_cleanup = true;
        memset(&sf->stmt_cleanup_drain_state, 0, sizeof(sf->stmt_cleanup_drain_state));
    } else if (op->type == KEEL_PQOP_PS_REPLAY) {
        sf->stmt_replay_count = op->expected_msgs;
        sf->stmt_replay_rfq_pending = false;
        sf->stmt_replay_hash = op->state_sync_hash;
    }

    ssize_t s = keel_try_send_nb(session->server_fd, op->payload, op->payload_len);
    if (s < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "W%u: pre-query op send failed type=%d: %s",
            worker ? worker->id : 0u, (int)op->type, strerror(errno));
        if (worker && worker->stats_ctx) {
            KEEL_STAT_INC(worker->stats_ctx, pre_query_send_fail);
            KEEL_STAT_INC(worker->stats_ctx, errors_backend);
        }
        pre_query_record_result(sf, worker,
                                KEEL_REPLAY_RESULT_PARTIAL_SEND_FAILURE);
        pre_query_reset_active(sf);
        return KEEL_FLOW_ERROR;
    }

    if (op->type == KEEL_PQOP_STATE_SYNC && worker && worker->stats_ctx)
        KEEL_STAT_INC(worker->stats_ctx, state_sync_count);
    if (op->type == KEEL_PQOP_CLEAN_CHECK && worker && worker->stats_ctx)
        KEEL_STAT_INC(worker->stats_ctx, discard_all_count);

    if ((size_t)s < op->payload_len) {
        keel_flow_result_t dr = defer_send(sf, session->server_fd,
                                           op->payload + s,
                                           op->payload_len - (size_t)s,
                                           (op->type == KEEL_PQOP_PS_REPLAY ||
                                            op->type == KEEL_PQOP_CLEAN_CHECK)
                                               ? KEEL_FLOW_WAIT_STMT_REPLAY
                                               : KEEL_FLOW_WAIT_BACKEND);
        if (dr == KEEL_FLOW_ERROR) {
            pre_query_record_result(sf, worker,
                                    KEEL_REPLAY_RESULT_PARTIAL_SEND_FAILURE);
        }
        return dr;
    }

    if (op->type == KEEL_PQOP_PS_REPLAY || op->type == KEEL_PQOP_CLEAN_CHECK)
        return KEEL_FLOW_WAIT_STMT_REPLAY;
    return KEEL_FLOW_WAIT_BACKEND;
}

static keel_flow_result_t pre_query_start_next_queued(keel_session_flow_t* sf,
                                                       keel_session_t* session)
{
    if (!sf || !session)
        return KEEL_FLOW_ERROR;
    if (sf->pre_query_count == 0)
        return KEEL_FLOW_OK;

    uint8_t idx = sf->pre_query_head;
    keel_pre_query_op_t op = sf->pre_query_ops[idx];
    sf->pre_query_ops[idx].payload = NULL;
    memset(&sf->pre_query_ops[idx], 0, sizeof(sf->pre_query_ops[idx]));
    sf->pre_query_head = (uint8_t)((sf->pre_query_head + 1u) %
                                   ENGINE_PRE_QUERY_OP_CAP);
    sf->pre_query_count--;

    keel_flow_result_t r = pre_query_start_op(sf, session, &op);
    pre_query_free_op(&op);
    return r;
}

static keel_flow_result_t pre_query_enqueue(keel_session_flow_t* sf,
                                            keel_session_t* session,
                                            keel_pre_query_op_type_t type,
                                            const uint8_t* payload,
                                            size_t payload_len,
                                            const uint8_t* follow_buf,
                                            size_t follow_len,
                                            uint64_t state_sync_hash,
                                            keel_flow_result_t resume,
                                            uint32_t expected_msgs)
{
    keel_worker_t* worker = session ? session->worker : NULL;

    if (!sf || !session || !payload || payload_len == 0)
        return KEEL_FLOW_ERROR;
    if (follow_len > sizeof(sf->pending_pre_query_buf)) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "W%u: pre-query follow-up payload %zu B exceeds stash %zu B",
            worker ? worker->id : 0u, follow_len,
            sizeof(sf->pending_pre_query_buf));
        if (worker && worker->stats_ctx)
            KEEL_STAT_INC(worker->stats_ctx, pre_query_overflow);
        pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_OOM);
        return KEEL_FLOW_ERROR;
    }

    if (follow_len > 0 && follow_buf) {
        if (sf->pending_pre_query_len == 0) {
            /* review_20260620_01.md RC-7: classify every frame in the
             * follow buffer BEFORE stashing.  This is the single
             * centralized chokepoint — every pre-query op (state_sync,
             * stmt_replay, deferred BEGIN, full cleanup, etc.) funnels
             * through here, so every named Parse in the tail is now
             * guaranteed to be inserted into the per-session stmt cache
             * before the tail is stashed.  Without this, the original
             * commit 72d50d0 regression (Bind to S_N → 26000) can be
             * re-introduced by any new code path that calls
             * pre_query_enqueue with a tail containing a named Parse. */
            keel_engine_flow_prefetch_classify_follow_buf(sf, follow_buf,
                                                          follow_len, 0);
            memcpy(sf->pending_pre_query_buf, follow_buf, follow_len);
            sf->pending_pre_query_len = follow_len;
            sf->pending_pre_query_resume = resume;
        } else if (sf->pending_pre_query_len != follow_len ||
                   memcmp(sf->pending_pre_query_buf, follow_buf, follow_len) != 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "W%u: mismatched held payload while enqueuing pre-query op",
                worker ? worker->id : 0u);
            if (worker && worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, pre_query_proto_violation);
            pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_PARSE_ERROR);
            return KEEL_FLOW_ERROR;
        }
    }

    if (!pre_query_has_active_op(sf) && sf->pre_query_count == 0) {
        keel_pre_query_op_t tmp = {
            .type = type,
            .state = KEEL_PQSTATE_IDLE,
            .payload = (uint8_t*)payload,
            .payload_len = payload_len,
            .state_sync_hash = state_sync_hash,
            .resume = resume,
            .expected_msgs = expected_msgs,
        };
        return pre_query_start_op(sf, session, &tmp);
    }

    if (!pre_query_queue_push(sf, type, payload, payload_len,
                              state_sync_hash, resume, expected_msgs)) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "W%u: pre-query op queue full (count=%u)",
            worker ? worker->id : 0u, (unsigned)sf->pre_query_count);
        if (worker && worker->stats_ctx)
            KEEL_STAT_INC(worker->stats_ctx, pre_query_overflow);
        pre_query_record_result(sf, worker,
            sf->pre_query_count >= ENGINE_PRE_QUERY_OP_CAP
                ? KEEL_REPLAY_RESULT_DRAIN_ERROR
                : KEEL_REPLAY_RESULT_OOM);
        return KEEL_FLOW_ERROR;
    }

    return KEEL_FLOW_WAIT_BACKEND;
}

static keel_flow_result_t pre_query_enqueue_cleanup(keel_session_flow_t* sf,
                                                    keel_session_t* session,
                                                    const char* where,
                                                    const uint8_t* follow_buf,
                                                    size_t follow_len)
{
    if (!sf || !session || !sf->flow || !sf->flow->drain_cleanup_response)
        return KEEL_FLOW_ERROR;

    uint8_t buf[ENGINE_SETUP_CLEANUP_BUFSZ];
    keel_cleanup_opts_t opts = {
        .mode = KEEL_CLEANUP_FULL,
        .timeout_ms = 0,
    };
    ssize_t n = -1;
    if (sf->flow->cleanup_slot) {
        n = sf->flow->cleanup_slot(sf->ctx, session->server_fd, NULL,
                                   opts, buf, sizeof(buf));
    } else if (sf->flow->build_cleanup) {
        n = sf->flow->build_cleanup(sf->ctx, KEEL_CLEANUP_UNKNOWN_STATE,
                                    buf, sizeof(buf));
    }
    if (n <= 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "W%u: setup cleanup build failed (%s)",
            session && session->worker ? session->worker->id : 0u,
            where ? where : "unknown");
        pre_query_record_result(sf, session ? session->worker : NULL,
                                KEEL_REPLAY_RESULT_DRAIN_ERROR);
        return KEEL_FLOW_ERROR;
    }
    return pre_query_enqueue(sf, session, KEEL_PQOP_CLEAN_CHECK,
                             buf, (size_t)n,
                             follow_buf, follow_len,
                             0, KEEL_FLOW_WAIT_BACKEND, 0);
}

static keel_flow_result_t pre_query_enqueue_stmt_replay(keel_session_flow_t* sf,
                                                         keel_session_t* session,
                                                         const uint8_t* replay_buf,
                                                         size_t replay_len,
                                                         uint32_t replay_count,
                                                         uint64_t replay_hash,
                                                         const uint8_t* follow_buf,
                                                         size_t follow_len)
{
    if (!replay_buf || replay_len == 0 || replay_count == 0)
    {
        pre_query_record_result(sf, session ? session->worker : NULL,
                                KEEL_REPLAY_RESULT_PARSE_ERROR);
        return KEEL_FLOW_ERROR;
    }
    return pre_query_enqueue(sf, session, KEEL_PQOP_PS_REPLAY,
                             replay_buf, replay_len,
                             follow_buf, follow_len,
                             replay_hash, KEEL_FLOW_WAIT_BACKEND,
                             replay_count);
}

/**
 * @brief Send the deferred BEGIN asynchronously and stash the follow-up
 *        FE payload for replay after ReadyForQuery arrives.
 *
 * Replaces the old blocking BEGIN+drain inline loops (engine_flow.c spec
 * §A in docs/REACTOR_BLOCKING_INVENTORY.md, PR #4).  The caller MUST have
 * already verified that no prepared-statement replay or DISCARD-ALL is
 * required on this backend (those combos are not yet supported and are
 * defended against at the call sites).
 *
 * On success the caller should return KEEL_FLOW_WAIT_BACKEND; the BE-side
 * intercept in keel_engine_flow_on_be_data() absorbs the BEGIN response
 * until 'Z' (ReadyForQuery) arrives, then forwards `follow_buf` to the
 * backend.
 *
 * @param sf         Session-flow state (begin_deferred_payload* must be set).
 * @param session    Session (server_fd must point at the assigned backend).
 * @param follow_buf FE payload to forward AFTER the BEGIN's 'Z'.  May be
 *                   NULL/0 for a degenerate "BEGIN with nothing to follow".
 * @param follow_len Byte count for follow_buf.  Must be ≤
 *                   KEEL_PRE_QUERY_REPLAY_BUFSZ or the call hard-errors.
 *
 * @return KEEL_FLOW_WAIT_BACKEND on success (BEGIN sent, awaiting Z),
 *         KEEL_FLOW_SEND_PENDING if the BEGIN partially drained,
 *         KEEL_FLOW_ERROR on overflow / send failure (counters bumped).
 */
static keel_flow_result_t defer_begin_replay(keel_session_flow_t* sf,
                                             keel_session_t* session,
                                             const uint8_t* follow_buf,
                                             size_t follow_len)
{
    const uint8_t* bp = sf->begin_deferred_payload;
    size_t         bl = sf->begin_deferred_payload_len;
    sf->begin_deferred_payload_len = 0;
    if (!bp || bl == 0)
        return KEEL_FLOW_ERROR;
    return pre_query_enqueue(sf, session, KEEL_PQOP_DEFERRED_BEGIN,
                             bp, bl, follow_buf, follow_len,
                             0, KEEL_FLOW_WAIT_BACKEND, 0);
}

/**
 * @brief Send session-state sync SQL asynchronously before forwarding a query.
 *
 * Transaction pooling correctness requires the sync response stream to be
 * fully drained before client traffic is forwarded. This arms the existing
 * pre-query absorber so backend bytes are consumed until ReadyForQuery, then
 * the backend is stamped with the session profile and the stashed FE payload
 * is sent.
 */
static keel_flow_result_t defer_state_sync_replay(keel_session_flow_t* sf,
                                                  keel_session_t* session,
                                                  backend_conn_t* be_conn,
                                                  const uint8_t* sync_buf,
                                                  size_t sync_len,
                                                  const uint8_t* follow_buf,
                                                  size_t follow_len,
                                                  keel_flow_result_t resume)
{
    if (!be_conn || !sync_buf || sync_len == 0)
        return KEEL_FLOW_ERROR;
    return pre_query_enqueue(sf, session, KEEL_PQOP_STATE_SYNC,
                             sync_buf, sync_len,
                             follow_buf, follow_len,
                             session->state_hash, resume, 0);
}

static bool backend_needs_state_sync(const keel_session_flow_t* sf,
                                     const keel_session_t* session,
                                     const backend_conn_t* be_conn)
{
    /* be_conn->profile is not required: generate_sync_sql() treats a NULL
     * backend profile as the clean baseline (empty), which is correct for
     * any backend whose profile was never explicitly recorded.  The hash
     * comparison alone is sufficient to detect whether sync is needed. */
    return KEEL_TIER_HAS_STATE_SYNC(sf->mode) &&
           be_conn && session->state_profile &&
           be_conn->current_state_hash != session->state_hash &&
           session->state_hash != 0;
}

static int capture_consistency_token_after_write(
    keel_session_flow_t* sf,
    keel_session_t* session,
    const keel_proto_flow_vtable_t* flow)
{
    if (!sf || !session || !flow || !flow->capture_consistency_token ||
        session->server_fd < 0) {
        return -1;
    }

    int fd = session->server_fd;
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return -1;
    }

    struct timeval saved_rcv = {0, 0};
    struct timeval saved_snd = {0, 0};
    socklen_t optlen = sizeof(saved_rcv);
    bool have_rcv = (getsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,
                                &saved_rcv, &optlen) == 0);
    optlen = sizeof(saved_snd);
    bool have_snd = (getsockopt(fd, SOL_SOCKET, SO_SNDTIMEO,
                                &saved_snd, &optlen) == 0);

    struct timeval tv = {
        .tv_sec = KEEL_CONSISTENCY_CAPTURE_TIMEOUT_MS / 1000,
        .tv_usec = (KEEL_CONSISTENCY_CAPTURE_TIMEOUT_MS % 1000) * 1000,
    };
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    if ((flags & O_NONBLOCK) != 0 &&
        fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) < 0) { /* NOLINT(keel-blocking) */
        if (have_rcv)
            (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,
                             &saved_rcv, sizeof(saved_rcv));
        if (have_snd)
            (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO,
                             &saved_snd, sizeof(saved_snd));
        return -1;
    }

    keel_consistency_token_t token;
    memset(&token, 0, sizeof(token));
    int rc = flow->capture_consistency_token(sf->ctx, fd, &token);

    if ((flags & O_NONBLOCK) != 0)
        (void)fcntl(fd, F_SETFL, flags);
    if (have_rcv)
        (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,
                         &saved_rcv, sizeof(saved_rcv));
    if (have_snd)
        (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO,
                         &saved_snd, sizeof(saved_snd));

    if (rc != 0 || token.value[0] == '\0') {
        return -1;
    }

    if (token.captured_at_ns == 0)
        token.captured_at_ns = engine_now_ns();
    sf->last_write_token = token;
    sf->last_write_ns = token.captured_at_ns;
    keel_ssv_consistency_set_token(sf->consistency_atoms, &token);
    if (flow->notify_write_lsn)
        flow->notify_write_lsn(sf->ctx, token.value);
    return 0;
}

/* ============================================================================
 * Session Flow Init/Destroy
 * ============================================================================ */

/**
 * @brief Initialise a keel_session_flow_t for a newly accepted client session.
 *
 * Called once from on_accept_complete() after a session slab entry is
 * allocated.  Zeroes the structure, sets the initial phase to
 * KEEL_PHASE_HANDSHAKE_AUTH, and invokes the protocol vtable's
 * create_context() to allocate per-protocol state (pg_flow_ctx_t,
 * my_flow_ctx_t, etc.).
 *
 * Inherits configuration from the owning worker:
 *  - ps_mode        — prepared-statement pooling mode (virtualize/pinning/…)
 *  - txn_tracking   — enable XID probe on COMMIT; consistency-token capture
 *                     must be reactor-owned before it is used on the hot path
 *
 * @param sf      Uninitialised flow state struct (will be zero-filled).
 * @param flow    Protocol vtable (postgres, mysql, …).  Must not be NULL.
 * @param session Owning session (for worker config access and plugin_state).
 * @return 0 on success, -1 if protocol context allocation failed.
 */
int keel_session_flow_init(keel_session_flow_t* sf,
                          const keel_proto_flow_vtable_t* flow,
                          keel_session_t* session) {
    memset(sf, 0, sizeof(*sf));
    sf->flow = flow;
    sf->phase = KEEL_PHASE_HANDSHAKE_AUTH;
    sf->tx = KEEL_TX_IDLE;
    sf->pins = KEEL_FPIN_NONE;
    sf->pending_msg = NULL;
    sf->pending_msg_len = 0;
    sf->pending_msg_resume = KEEL_FLOW_WAIT_BACKEND;
    sf->queued_for_pool = false;

    /* Inherit PS mode from the worker config */
    if (session && session->worker)
        sf->ps_mode = session->worker->ps_mode;
    else
        sf->ps_mode = KEEL_PS_MODE_VIRTUALIZE;

    /* Inherit runtime mode tier.  Fall back to POOL (not FULL) when there is
     * no worker, so unit tests and standalone tools stay conservative. */
    if (session && session->worker)
        sf->mode = session->worker->runtime_mode;
    else
        sf->mode = KEEL_TIER_POOL;

    /* Inherit replication uncertainty tracking */
    sf->txn_tracking = (session && session->worker)
                       ? session->worker->txn_tracking : false;

    /* Initialise SSV atoms (per-session, no locking) */
    keel_ssv_consistency_init(sf->consistency_atoms);
    keel_ssv_opaque_init(sf->opaque_atoms);
    keel_ssv_config_init(sf->config_atoms);

    /* Inherit sticky-primary TTL (0 = disabled) */
    sf->sticky_primary_ttl_ms = (session && session->worker)
                                ? session->worker->sticky_primary_ttl_ms
                                : (uint32_t)KEEL_STICKY_PRIMARY_TTL_MS;

    /* Inherit buffer caps (0 = unlimited) */
    sf->session_max_buffered_bytes = (session && session->worker)
                                     ? session->worker->session_max_buffered_bytes : 0;
    sf->backend_max_replay_bytes   = (session && session->worker)
                                     ? session->worker->backend_max_replay_bytes : 0;

    /* PROXY mode: force PS off and disable txn tracking — the session
     * is hard-pinned to one backend, no pooling sophistication needed. */
    if (sf->mode == KEEL_TIER_PROXY) {
        sf->ps_mode = KEEL_PS_MODE_OFF;
        sf->txn_tracking = false;
    }

    if (flow && flow->create_context) {
        sf->ctx = flow->create_context(session);
        if (!sf->ctx) return -1;
    }
    /* auth_notify_fd: -1 until an async auth op is in flight */
    sf->auth_notify_fd = -1;
    /* Expose the plugin context through the session so any code that only
     * has a session* pointer can access plugin state without needing to
     * reach back into the recv_context. */
    if (session) session->plugin_state = sf->ctx;
    return 0;
}

/**
 * @brief Tear down a session flow and release all associated resources.
 *
 * Cleans up in order:
 *  1. Invokes vtable destroy_context() to free per-protocol state.
 *  2. Frees any pending deferred-send buffer (partial send that was
 *     queued for io_uring completion).
 *  3. Frees the prepared-statement replay buffer.
 *  4. Returns the commit-in-doubt check connection to the pool if it
 *     was borrowed — prevents silent pool leaks when a session is
 *     destroyed while an outcome-check query is still in flight.
 *
 * THREAD SAFETY: Must only be called from the session's owning worker
 * thread.  The pool return is lock-free (CAS on the connection state).
 */
void keel_session_flow_destroy(keel_session_flow_t* sf) {
    if (sf->flow && sf->flow->destroy_context && sf->ctx) {
        sf->flow->destroy_context(sf->ctx);
        sf->ctx = NULL;
    }
    keel_free(sf->pending_send_buf);
    sf->pending_send_buf = NULL;
    sf->pending_send_len = 0;
    sf->pending_send_off = 0;
    sf->pending_send_cap = 0;
    if (sf->stmt_replay_buf) {
        keel_free(sf->stmt_replay_buf);
        sf->stmt_replay_buf = NULL;
    }
    if (sf->cache_capture_buf) {
        keel_free(sf->cache_capture_buf);
        sf->cache_capture_buf = NULL;
        sf->cache_capture_len = 0;
        sf->cache_capture_cap = 0;
    }
    sf->cache_pending = false;
    if (sf->cache_inval_sql) {
        keel_free(sf->cache_inval_sql);
        sf->cache_inval_sql = NULL;
    }
    sf->cache_inval_pending = false;
    sf->pending_msg = NULL;
    sf->pending_msg_len = 0;
    sf->pending_msg_resume = KEEL_FLOW_WAIT_BACKEND;
    sf->queued_for_pool = false;
    pre_query_clear_queue(sf);

    /* Replication tracking: if a commit-in-doubt check connection is still
     * borrowed, return it to the pool so it isn't leaked. */
    if (sf->xid_check_conn) {
        if (sf->xid_check_conn->pool)
            backend_pool_return(sf->xid_check_conn->pool, sf->xid_check_conn, false);
        sf->xid_check_conn = NULL;
    }
}

/* ============================================================================
 * Resume Session After Async Auth
 * ============================================================================ */

/**
 * @brief Resume a session whose off-thread auth operation completed.
 *
 * Called by the worker when the eventfd stored in sf->auth_notify_fd becomes
 * readable (io_uring or epoll).  Drains the eventfd, closes it, resets
 * sf->auth_notify_fd to -1, then re-invokes the protocol's on_fe_msg with
 * an empty 'p'-like trigger so the protocol can pick up the auth result.
 */
keel_flow_result_t keel_engine_flow_resume_auth(
    keel_session_flow_t* sf,
    keel_session_t* session)
{
    /* Drain the eventfd (must read 8 bytes). The fd is set non-blocking at
     * creation in the auth-pool thread, so this read either returns 8 bytes
     * (signalled) or fails with EAGAIN — it cannot block the reactor. */
    if (sf->auth_notify_fd >= 0) {
        uint64_t val;
        ssize_t n = read(sf->auth_notify_fd, &val, sizeof(val)); /* NOLINT(keel-blocking) */
        (void)n; /* eventfd drain; EAGAIN is harmless if pool thread hasn't written yet */
        close(sf->auth_notify_fd);
        sf->auth_notify_fd = -1;
    }

    /* Re-invoke on_fe_msg with a zero-length payload.  The protocol
     * detects auth_pending==true + VERIFY state completed and transitions
     * to success or failure without reading new wire bytes. */
    static const uint8_t kEmpty[1] = {0};
    return keel_engine_flow_on_fe_data(sf, session, kEmpty, 0);
}

/* ============================================================================
 * Resume Session After Pool Wait
 * ============================================================================ */

/**
 * @brief Resume a session that was queued while waiting for a backend
 *        connection to become available.
 *
 * Called by the pool's wait-queue callback when a backend is returned to
 * the pool and a queued session is selected to receive it.  The session's
 * pending message was saved at queue time; this function forwards it to
 * the newly assigned backend.
 *
 * Before forwarding, the function handles two cases:
 *
 *  A. **Prepared-statement replay** (spec §17): If the session has named
 *     prepared statements (`sf->pins & KEEL_FPIN_PREPARED_STMT`) and the
 *     borrowed backend has a different stmt_set_hash, the Parse wire
 *     messages are sent first.  The original message is held in
 *     `sf->stmt_replay_orig_msg` and forwarded after all ParseCompletes
 *     are acknowledged.  If the backend also has `needs_full_cleanup` set,
 *     plugin cleanup is sent first to clear stale session state before replay.
 *
 *  B. **Needs cleanup but no stmts**: Backend was borrowed from a dirty pool
 *     slot; plugin cleanup is sent and the pending message is deferred until
 *     the response is validated.
 *
 * After handling the above, the pending message is forwarded non-blocking.
 * A partial send is deferred via `defer_send()`.
 *
 * @param sf       Session flow state.
 * @param session  Owning session (has server_fd set to the new backend).
 * @param be_conn  The borrowed backend connection.
 * @return KEEL_FLOW_WAIT_BACKEND on success (engine arms backend recv),
 *         KEEL_FLOW_WAIT_STMT_REPLAY if replay is needed,
 *         KEEL_FLOW_SEND_PENDING if the send was partial,
 *         KEEL_FLOW_ERROR on fatal error.
 */
keel_flow_result_t keel_engine_flow_resume_from_pool(
    keel_session_flow_t* sf,
    keel_session_t* session,
    backend_conn_t* be_conn) {

    const keel_proto_flow_vtable_t* flow = sf->flow;
    if (!flow) return KEEL_FLOW_ERROR;

    keel_worker_t* worker = session->worker;
    KEEL_INSTR_SCOPE(WORKER_INSTR(worker), KEEL_INSTR_FLOW_RESUME_FROM_POOL);

    /* Clear queue state */
    sf->queued_for_pool = false;
    const uint8_t* pending_data = sf->pending_msg;
    size_t pending_len = sf->pending_msg_len;
    keel_flow_result_t pending_resume = sf->pending_msg_resume;
    sf->pending_msg = NULL;
    sf->pending_msg_len = 0;
    sf->pending_msg_resume = KEEL_FLOW_WAIT_BACKEND;

    if (!pending_data || pending_len == 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "W%u: resume_from_pool with no pending msg", worker->id);
        return KEEL_FLOW_ERROR;
    }

    /* Early cancel: a CancelRequest arrived while the session was queued
     * waiting for a pool slot.  Return the just-borrowed connection and
     * synthesise E(57014) — identical to the on_fe_data fast-path above.
     * KEEL_FLOW_OK re-arms the FE recv for the client's recovery query. */
    if (atomic_load_explicit(&session->cancel_pending, memory_order_acquire)) {
        atomic_store_explicit(&session->cancel_pending, false, memory_order_relaxed);
        if (be_conn && be_conn->pool)
            backend_pool_return(be_conn->pool, be_conn, false);
        if (flow->generate_error) {
            uint8_t ebuf[256];
            ssize_t elen = flow->generate_error(sf->ctx, "57014",
                "canceling statement due to user request",
                ebuf, sizeof(ebuf));
            if (elen > 0)
                keel_try_send_nb(session->client_fd, ebuf, (size_t)elen);
        }
        sf->phase = KEEL_PHASE_READY;
        return KEEL_FLOW_OK;
    }

    /* Assign the backend connection */
    session->backend_conn = be_conn;
    session->backend_generation = be_conn ? be_conn->generation : 0;
    session->server_fd = be_conn->fd;

    keel_flow_result_t setup_result = KEEL_FLOW_OK;
    if (backend_needs_state_sync(sf, session, be_conn)) {
        uint8_t sync_buf[4096];
        uint64_t _sync_t0 = keel_instr_begin(WORKER_INSTR(worker),
                                             KEEL_INSTR_STATE_SYNC);
        ssize_t sync_len = flow->build_state_sync
            ? flow->build_state_sync(sf->ctx, be_conn->profile,
                                     session->state_profile,
                                     sync_buf, sizeof(sync_buf))
            : -1;
        keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_STATE_SYNC, _sync_t0);
        if (sync_len < 0)
            return KEEL_FLOW_ERROR;
        if (sync_len == 0) {
            be_conn->current_state_hash = session->state_hash;
            be_conn->needs_sync = false;
            if (be_conn->profile && session->state_profile)
                state_profile_copy(be_conn->profile, session->state_profile);
        } else {
            setup_result = defer_state_sync_replay(sf, session, be_conn,
                                                   sync_buf, (size_t)sync_len,
                                                   pending_data, pending_len,
                                                   KEEL_FLOW_WAIT_BACKEND);
            if (setup_result == KEEL_FLOW_ERROR)
                return KEEL_FLOW_ERROR;
        }
    }

    /* Deferred-BEGIN replay (PR #4 — async): if a BEGIN was buffered during
     * shard deferral, send it to the backend non-blocking and stash the
     * pending FE message for forwarding once the backend's ReadyForQuery
     * arrives.  The BE-side intercept in keel_engine_flow_on_be_data()
     * absorbs the BEGIN response and replays the stashed payload.
     *
     * Combination with prepared-statement replay or DISCARD-ALL on the same
     * borrow is not yet supported: deferred BEGIN happens only on the first
     * routable query of a sharded session, before the session has pinned
     * named prepared statements; and the pool borrow paths that set
     * needs_full_cleanup are mutually exclusive with shard-deferred BEGIN.
     * Both invariants are asserted below — violation hard-errors the
     * session rather than silently falling back to the old blocking path. */
    if (sf->begin_deferred && sf->begin_deferred_payload_len > 0) {
        sf->begin_deferred = false;

        /* Query logging happens in the BE-side intercept after setup drain. */
        keel_flow_result_t br = defer_begin_replay(sf, session,
                                                   pending_data, pending_len);
        if (br == KEEL_FLOW_ERROR)
            return KEEL_FLOW_ERROR;
        if (setup_result == KEEL_FLOW_OK)
            setup_result = br;
    }

    if (setup_result != KEEL_FLOW_OK)
        return setup_result;

    KEEL_DEBUG_LOG("W%u: resumed session %lu with BE fd=%d\n",
                worker->id, (unsigned long)session->id, be_conn->fd);

    /* Prepared-statement replay (spec §17): if this backend doesn't have
     * the session's named prepared statements, send Parse messages first
     * and wait for ParseComplete before forwarding the client's message. */
    if ((sf->pins & KEEL_FPIN_PREPARED_STMT) && flow->get_stmt_replay) {
        uint64_t stmt_hash = 0;
        flow->get_stmt_replay(sf->ctx, NULL, NULL, NULL, &stmt_hash);
        KEEL_DEBUG_LOG("W%u: resume_from_pool: fd=%d pins=0x%x"
            " session_stmt_hash=0x%016llx be_stmt_hash=0x%016llx"
            " needs_discard=%d pending[0]=0x%02x\n",
            worker->id, be_conn->fd, (unsigned)sf->pins,
            (unsigned long long)stmt_hash,
            (unsigned long long)be_conn->stmt_set_hash,
            (int)be_conn->needs_full_cleanup,
            pending_data && pending_len > 0 ? (unsigned)pending_data[0] : 0);
        if (stmt_hash != 0 && be_conn->stmt_set_hash != stmt_hash) {
            /* Backend needs stmts replayed before we send pending_data */
            uint8_t* rbuf   = NULL;
            size_t   rlen   = 0;
            uint32_t rcount = 0;
            uint64_t rhash  = 0;
            if (flow->get_stmt_replay(sf->ctx, &rbuf, &rlen, &rcount, &rhash) == 0 &&
                rbuf && rlen > 0 && rcount > 0) {
                /* Apply pin effects for messages in pending_data that bypass
                 * the FE loop (Sync clears KEEL_FPIN_EXTENDED_PROTO). */
                apply_captured_fe_pin_effects(sf, session, worker, pending_data, pending_len);
                keel_flow_result_t rr = KEEL_FLOW_WAIT_BACKEND;
                if (be_conn->needs_full_cleanup) {
                    be_conn->needs_full_cleanup = false;
                    rr = pre_query_enqueue_cleanup(sf, session, "resume replay",
                                                   pending_data, pending_len);
                    if (rr == KEEL_FLOW_ERROR) {
                        keel_free(rbuf);
                        return rr;
                    }
                }
                keel_flow_result_t rqr = pre_query_enqueue_stmt_replay(
                    sf, session, rbuf, rlen, rcount, rhash,
                    pending_data, pending_len);
                keel_free(rbuf);
                if (rqr == KEEL_FLOW_ERROR)
                    return rqr;
                if (rr == KEEL_FLOW_WAIT_BACKEND || rr == KEEL_FLOW_WAIT_STMT_REPLAY)
                    return rr;
                return rqr;
            }
            if (rbuf) keel_free(rbuf);
        }
    }

    /* If the backend was borrowed with incompatible state for a session with
     * no prepared state yet, run plugin cleanup before forwarding data. */
    if (be_conn->needs_full_cleanup) {
        be_conn->needs_full_cleanup = false;
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
            "W%u: resume no-stmt cleanup: fd=%d pending_len=%zu pins=0x%x",
            worker->id, session->server_fd, pending_len, (unsigned)sf->pins);
        return pre_query_enqueue_cleanup(sf, session, "resume no-stmt",
                                         pending_data, pending_len);
    }

    /* ----- Query logging (must happen AFTER backend is assigned so that
     *       the log record picks up dst=<backend_ip>:<port>).
     *
     *       FIX: Previously, queries that went through the pool wait queue
     *       were never logged because the initial flow path returned
     *       KEEL_FLOW_WAIT_POOL before reaching the logging code, and this
     *       resume path had no logging.  This caused >99% of queries to be
     *       invisible to the query log under high-concurrency workloads. */
    if (KEEL_TIER_HAS_QUERY_LOG(sf->mode) &&
        pending_len > 5 && pending_data[0] == 'Q') {
        /* PostgreSQL SimpleQuery: type('Q') + int32(len) + NUL-terminated SQL */
        const char* sql_data = (const char*)(pending_data + 5);
        size_t sql_len = pending_len - 5;
        /* Strip NUL terminator if present */
        if (sql_len > 0 && sql_data[sql_len - 1] == '\0') sql_len--;

        if (sql_len > 0) {
            keel_query_log_t* qlog = keel_query_log_get_global();
            if (qlog && qlog->enabled) {
                keel_str_t sql_str = { .data = sql_data, .len = sql_len };
                keel_proto_query_t qr;
                memset(&qr, 0, sizeof(qr));
                keel_sql_analyze(sql_str, &qr);
                keel_query_log_emit(qlog, session, &qr);
            }
        }
    }

    if (pending_len > 0 && pending_data[0] == 'H' &&
        sf->flush_pending_count > 0) {
        sf->flush_in_flight = true;
    }

    /* Invariant (Bug B catch): if the session has named prepared statements
     * pinned and we are about to forward a bare Bind ('B') to a backend whose
     * stmt_set_hash is zero (definitively no named statements present), then
     * the replay block above failed to fire.  PG will reject with "prepared
     * statement does not exist" and KEEL will silently deadlock.  Convert
     * this into a loud, recoverable session abort instead. */
    if ((sf->pins & KEEL_FPIN_PREPARED_STMT) &&
        be_conn->stmt_set_hash == 0 &&
        pending_len > 0 && pending_data[0] == 'B') {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL,
            "W%u: INVARIANT (Bug B): session %lu has PREPARED_STMT pin but "
            "backend fd=%d has stmt_set_hash=0; refusing to forward bare Bind "
            "(pending_len=%zu) — would cause silent deadlock. Aborting session.",
            worker->id, (unsigned long)session->id,
            session->server_fd, pending_len);
        static const uint8_t ps_missing_err[] = {
            'E',
            0,0,0,51,
            'S','F','A','T','A','L',0,
            'V','F','A','T','A','L',0,
            'C','0','8','0','0','6',0,
            'M','K','E','E','L',':',' ','P','S',' ','r','e','p','l','a','y',
            ' ','m','i','s','s','i','n','g',0,
            0
        };
        keel_try_send_nb(session->client_fd, ps_missing_err, sizeof(ps_missing_err));
        return KEEL_FLOW_CLOSED;
    }

    /* Forward the pending message to backend (non-blocking) */
    ssize_t s = keel_try_send_nb(session->server_fd, pending_data, pending_len);
    if (s < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "Worker %u: backend send failed on resume: %s",
                    worker->id, strerror(errno));
        if (worker->stats_ctx)
            KEEL_STAT_INC(worker->stats_ctx, errors_backend);
        return KEEL_FLOW_ERROR;
    }
    size_t actual = (size_t)s;
    if (worker->stats_ctx)
        KEEL_STAT_ADD(worker->stats_ctx, bytes_backend_sent, actual);

    if (actual < pending_len) {
        return defer_send(sf, session->server_fd,
                          pending_data + actual, pending_len - actual,
                          pending_resume);
    }

    return pending_resume;
}

/* ============================================================================
 * Backend Connection Helpers
 *
 * All backend connection is now async via backend_connect_async.c.
 * CancelRequest uses cancel_request_async() with io_uring.
 * ============================================================================ */

/* ============================================================================
 * Frontend Data Handler
 * ============================================================================ */

/**
 * @brief Process a chunk of bytes received from the client socket.
 *
 * This is the core FE (frontend) data path, called by the worker after
 * every io_uring recv CQE on the client fd.  It may be called with one
 * or more complete protocol messages, partial messages, or a mix.
 *
 * The function handles several fast-path cases before the main message
 * loop:
 *
 *  1. **COPY IN fast-path**: When `KEEL_FPIN_COPY` is set, all incoming
 *     bytes are forwarded directly to the backend without per-message
 *     framing.  The buffer is scanned for `CopyDone('c')` / `CopyFail('f')`
 *     to detect the end of the COPY stream.  Partial headers across buffer
 *     boundaries are tracked in `sf->copy_hdr[]` / `sf->copy_skip`.
 *
 *  2. **Jumbo message continuation**: When `sf->fe_fwd_remaining > 0`,
 *     the previous buffer contained only a prefix of a large protocol
 *     message.  Forward the current buffer (or as much as fits) to the
 *     backend without reparsing.
 *
 * The main `while (pos < len)` loop calls `flow->frame_len()` to extract
 * one complete protocol message at a time, then `flow->on_fe_msg()` to
 * classify it.  The returned `keel_fe_action_t` is dispatched:
 *
 *  - `SSL_REQUEST`         → send 'N' decline, loop
 *  - `AUTH_COMPLETE`       → send handshake buffer to client, phase=READY
 *  - `SEND_FE`             → send response to client (synthetic auth or PS intercept)
 *  - `QUERY` / `FORWARD`  → acquire backend, fire hooks, forward payload
 *  - `TERMINATE`          → close session
 *
 * For `QUERY` actions:
 *  - Hook chain fires: AFTER_QUERY_READ → AFTER_QUERY_PARSE → BEFORE_ROUTE
 *  - Route decision: primary / replica / sticky-primary override
 *  - Shard dispatch: keel_router_dispatch_sql() resolves SINGLE vs SCATTER
 *  - AFTER_ROUTE hook: sees dispatch kind and shard decisions; can veto
 *  - BEFORE_SCATTER hook: fires before fan-out; can veto scatter execution
 *  - keel_engine_scatter_execute(): parallel fan-out + merge (scatter path)
 *  - AFTER_SCATTER hook: observational; sees timing, shard counts (scatter path)
 *  - Backend borrow: hard-pin (transactions/COPY) or soft-borrow (stmt replay)
 *  - State sync: if the borrowed backend profile differs from the session
 *  - BEFORE_SEND hook: final mutation of payload before forwarding (single-shard)
 *  - `commit_in_flight` set for COMMIT queries (enables commit-in-doubt)
 *  - WRITE/DDL queries stamp sticky-primary state; token capture is not run
 *    inline on the worker reactor
 *
 * Non-blocking send: `keel_try_send_nb()` is attempted first; partial
 * sends are handed off to `defer_send()` → `KEEL_FLOW_SEND_PENDING`.
 *
 * @param sf      Session flow state.
 * @param session Owning session (must have worker set).
 * @param data    Buffer received from client socket.
 * @param len     Number of bytes in buffer.
 * @return KEEL_FLOW_OK            — more FE data may arrive
 *         KEEL_FLOW_WAIT_BACKEND  — waiting for backend response
 *         KEEL_FLOW_WAIT_POOL     — queued waiting for a pool connection
 *         KEEL_FLOW_SEND_PENDING  — deferred partial send to io_uring
 *         KEEL_FLOW_CLOSED        — session should be closed
 *         KEEL_FLOW_ERROR         — unrecoverable error
 */
keel_flow_result_t keel_engine_flow_on_fe_data(
    keel_session_flow_t* sf,
    keel_session_t* session,
    const uint8_t* data,
    size_t len) {

    const keel_proto_flow_vtable_t* flow = sf->flow;
    if (!flow) return KEEL_FLOW_ERROR;

    keel_worker_t* worker = session->worker;
    KEEL_INSTR_SCOPE(WORKER_INSTR(worker), KEEL_INSTR_FLOW_FE_DATA);
    size_t pos = 0;
    size_t ext_batch_start = (size_t)-1;  /* extended protocol send batching */
    uint16_t ext_flush_response_count = 0; /* pending response count for Flush terminal */
    /* Accumulator for query-effect flags across an extended-protocol batch:
     * the actual send happens on the Sync/Flush terminal, whose own act
     * carries effect=0, so per-message effects (WRITE, DDL, BEGINS_TX, etc.)
     * would otherwise be lost — making downstream post-send bookkeeping
     * (sticky-primary stamp, query-cache invalidation, txn tracking) miss
     * every parameterized statement.  Cleared each time a batch terminates. */
    keel_query_effect_flags_t ext_batch_effect = 0;
    sf->linked_send_len = 0;              /* clear linked send state */

    KEEL_DEBUG_LOG("W%u: fe_data len=%zu phase=%d\n", worker->id, len, sf->phase);

    /* ---- Async auth resume (len == 0) -----------------------------------
     * Called from keel_engine_flow_resume_auth() after the LDAP/PAM thread
     * pool writes to the eventfd.  The protocol has no new wire bytes to
     * parse; instead on_fe_msg() is invoked with len=0 so it can pick up
     * the completed async result and build the final AUTH_COMPLETE or
     * AUTH_REJECT response.  We then dispatch that single action and return.
     * ---------------------------------------------------------------------- */
    if (len == 0) {
        keel_fe_action_t act = keel_fe_action_default();
        if (!flow->on_fe_msg || flow->on_fe_msg(sf->ctx, NULL, 0, &act) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "W%u: auth resume on_fe_msg failed", worker->id);
            return KEEL_FLOW_ERROR;
        }
        /* Handle the single action returned by the async-resume protocol call.
         * Only AUTH_COMPLETE, AUTH_REJECT and a few error/close types are
         * expected here.  Anything else is an internal error. */
        switch (act.type) {
        case KEEL_FE_ACT_AUTH_COMPLETE:
            /* Send handshake (AuthOK + ParameterStatus + ReadyForQuery) */
            if (act.client_username && act.client_username[0])
                strncpy(session->username, act.client_username,
                        sizeof(session->username) - 1);
            if (act.client_database && act.client_database[0])
                strncpy(session->database, act.client_database,
                        sizeof(session->database) - 1);
            if (act.fe_response && act.fe_response_len > 0) {
                ssize_t s = keel_try_send_nb(session->client_fd,
                                             act.fe_response, act.fe_response_len);
                if (s < 0) return KEEL_FLOW_ERROR;
                if ((size_t)s < act.fe_response_len) {
                    sf->phase = KEEL_PHASE_READY;
                    session->state = KEEL_SESSION_READY;
                    session->flags |= KEEL_SESSION_FLAG_AUTHENTICATED;
                    return defer_send(sf, session->client_fd,
                                      (const uint8_t*)act.fe_response + s,
                                      act.fe_response_len - (size_t)s,
                                      KEEL_FLOW_OK);
                }
            }
            sf->phase = KEEL_PHASE_READY;
            session->state = KEEL_SESSION_READY;
            session->flags |= KEEL_SESSION_FLAG_AUTHENTICATED;
            return KEEL_FLOW_OK;

        case KEEL_FE_ACT_AUTH_REJECT:
        case KEEL_FE_ACT_ERROR:
            if (act.fe_response && act.fe_response_len > 0)
                keel_try_send_nb(session->client_fd,
                                 act.fe_response, act.fe_response_len);
            return KEEL_FLOW_CLOSED;

        case KEEL_FE_ACT_WAIT_AUTH:
            /* Auth is still in VERIFY state — should not normally happen
             * on the resume path (means the pool thread wrote before it
             * finished).  Return KEEL_FLOW_CLOSED to avoid a hung session. */
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "W%u: auth resume returned WAIT_AUTH — state still VERIFY",
                worker->id);
            return KEEL_FLOW_CLOSED;

        default:
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "W%u: unexpected action %d on auth resume", worker->id, (int)act.type);
            return KEEL_FLOW_CLOSED;
        }
    }

    /* ------------------------------------------------------------------ *
     * COPY IN fast-path: when the session is pinned for COPY, all FE     *
     * data is CopyData('d')/CopyDone('c')/CopyFail('f').  Forward the   *
     * entire buffer to the backend in one shot — no per-message framing  *
     * needed.  This avoids partial-frame residual issues and is much     *
     * faster for bulk loads (pgbench -i, COPY FROM STDIN, etc.).         *
     * ------------------------------------------------------------------ */
    if ((sf->pins & KEEL_FPIN_COPY) && session->server_fd >= 0) {
        /* Non-blocking send of COPY data to backend.
         * If the socket buffer fills up, defer the remainder to io_uring.
         * We scan the ENTIRE buffer (including unsent parts) for CopyDone/
         * CopyFail so the resume action is correct once the deferred send
         * completes. */
        ssize_t nb = keel_try_send_nb(session->server_fd, data, len);
        if (nb < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: COPY send to backend failed: %s",
                        worker->id, strerror(errno));
            return KEEL_FLOW_ERROR;
        }
        size_t sent = (size_t)nb;

        /* Track COPY bytes sent to backend */
        if (sent > 0 && worker->stats_ctx) {
            KEEL_STAT_ADD(worker->stats_ctx, bytes_backend_sent, sent);
            KEEL_STAT_ADD(worker->stats_ctx, copy_bytes_total, sent);
        }

        /* Scan the FULL buffer for CopyDone/CopyFail to determine the
         * correct resume action.  This is safe because the scan doesn't
         * depend on the data having been sent — it just looks for message
         * boundaries to decide whether to wait for a backend response. */
        keel_flow_result_t copy_result = KEEL_FLOW_OK;
        /* Scan for CopyDone ('c') or CopyFail ('f') in the buffer.
         * These end the COPY stream, so we must wait for the backend's
         * CommandComplete + ReadyForQuery response.
         *
         * IMPORTANT: A CopyData message can span recv buffer boundaries.
         * We track three pieces of state across calls:
         *   copy_skip:    payload bytes to skip (from a partial message body)
         *   copy_hdr_len: saved partial header bytes (type + length, 1-4)
         *   copy_hdr[]:   the saved header bytes
         */
        size_t i = 0;

        /* Case 1: skip remaining body bytes from a prior partial message */
        if (sf->copy_skip > 0) {
            if (sf->copy_skip >= len) {
                sf->copy_skip -= len;
                goto copy_scan_done;
            }
            i = sf->copy_skip;
            sf->copy_skip = 0;
        }

        /* Case 2: reassemble a split header from previous buffer */
        if (sf->copy_hdr_len > 0) {
            /* We need 5 total header bytes (type + 4-byte length).
             * Append from current buffer until we have 5. */
            size_t need = 5 - sf->copy_hdr_len;
            size_t avail = len - i;
            if (avail < need) {
                /* Still not enough — save what we have and wait */
                memcpy(sf->copy_hdr + sf->copy_hdr_len, data + i, avail);
                sf->copy_hdr_len += (uint8_t)avail;
                goto copy_scan_done;
            }
            memcpy(sf->copy_hdr + sf->copy_hdr_len, data + i, need);
            i += need;
            sf->copy_hdr_len = 0;

            /* Parse the reassembled header */
            uint8_t t = sf->copy_hdr[0];
            if (t == 'c' || t == 'f') {
                sf->copy_skip = 0;
                copy_result = KEEL_FLOW_WAIT_BACKEND;
                goto copy_scan_done;
            }
            uint32_t ml = ((uint32_t)sf->copy_hdr[1]<<24)|
                          ((uint32_t)sf->copy_hdr[2]<<16)|
                          ((uint32_t)sf->copy_hdr[3]<<8)|
                          sf->copy_hdr[4];
            if (ml >= 4) {
                /* msg_total = 1 + ml, we already consumed 5 bytes (1 type + 4 len).
                 * Remaining payload bytes = ml - 4. */
                size_t body_left = ml - 4;
                size_t buf_left = len - i;
                if (body_left > buf_left) {
                    sf->copy_skip = body_left - buf_left;
                    goto copy_scan_done;
                }
                i += body_left;
            }
            /* Fall through to scan the rest of this buffer */
        }

        /* Case 3: normal scanning at message boundaries */
        while (i < len) {
            size_t remaining = len - i;
            if (remaining < 5) {
                /* Partial header at end — save it for next buffer */
                memcpy(sf->copy_hdr, data + i, remaining);
                sf->copy_hdr_len = (uint8_t)remaining;
                goto copy_scan_done;
            }
            uint8_t t = data[i];
            uint32_t ml = ((uint32_t)data[i+1]<<24)|((uint32_t)data[i+2]<<16)|
                          ((uint32_t)data[i+3]<<8)|data[i+4];
            if (ml < 4) break;       /* Invalid length — stop scanning */
            size_t msg_total = 1 + ml;

            if (t == 'c' || t == 'f') {
                /* COPY is ending — switch to waiting for backend response */
                sf->copy_skip = 0;
                sf->copy_hdr_len = 0;
                copy_result = KEEL_FLOW_WAIT_BACKEND;
                goto copy_scan_done;
            }
            if (i + msg_total > len) {
                /* Partial message body — record how many bytes remain
                 * so the next buffer scan starts at the right boundary. */
                sf->copy_skip = (i + msg_total) - len;
                goto copy_scan_done;
            }
            i += msg_total;
        }

copy_scan_done:
        /* If send was partial, defer the remainder to io_uring (backpressure) */
        if (sent < len) {
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, copy_pause_count);
            return defer_send(sf, session->server_fd,
                              data + sent, len - sent, copy_result);
        }
        return copy_result;
    }

    /* ------------------------------------------------------------------ *
     * Large-message continuation: when a previous recv delivered only a   *
     * partial protocol message, fe_fwd_remaining holds the byte count    *
     * still needed.  Forward as much of this buffer as possible without  *
     * trying to parse it (it's the middle of a message).                 *
     * ------------------------------------------------------------------ */
    if (sf->fe_fwd_remaining > 0 && session->server_fd >= 0) {
        size_t chunk = (len < sf->fe_fwd_remaining) ? len : sf->fe_fwd_remaining;
        ssize_t s = keel_try_send_nb(session->server_fd, data, chunk);
        if (s < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "Worker %u: backend send (FE continuation) failed: %s",
                worker->id, strerror(errno));
            return KEEL_FLOW_ERROR;
        }
        size_t actual = (size_t)s;
        if (worker->stats_ctx)
            KEEL_STAT_ADD(worker->stats_ctx, bytes_backend_sent, actual);

        /* Subtract the bytes we've committed to sending (actual now + deferred later) */
        sf->fe_fwd_remaining -= chunk;

        if (actual < chunk) {
            /* Partial send — defer remainder. Determine correct resume:
             * If there's still more continuation data coming from future FE
             * recv operations, resume OK (re-arm FE recv). Otherwise
             * check if this message needed a backend response. */
            keel_flow_result_t resume = (sf->fe_fwd_remaining > 0)
                ? KEEL_FLOW_OK
                : (sf->fe_fwd_wait_be ? KEEL_FLOW_WAIT_BACKEND : KEEL_FLOW_OK);
            if (resume == KEEL_FLOW_WAIT_BACKEND)
                sf->fe_fwd_wait_be = false;
            return defer_send(sf, session->server_fd,
                              data + actual, chunk - actual, resume);
        }

        pos = chunk;

        if (sf->fe_fwd_remaining > 0) {
            /* Still more continuation to come — keep reading from FE */
            return sf->fe_fwd_wait_be ? KEEL_FLOW_OK : KEEL_FLOW_OK;
        }

        /* Message is now fully forwarded */
        if (sf->fe_fwd_wait_be) {
            sf->fe_fwd_wait_be = false;
            /* If there's trailing data after the completed message,
             * we can't process it yet — need backend response first.
             * Save the trailing bytes in client_residual for later.
             * For now, drop through to regular loop which will process
             * any remaining messages (they shouldn't be there for a
             * simple query, but extended protocol might batch). */
            if (pos >= len)
                return KEEL_FLOW_WAIT_BACKEND;
            /* There's more data after the completed message —
             * still need backend response for the query we just sent */
            return KEEL_FLOW_WAIT_BACKEND;
        }
        /* Non-query forward (handshake etc) — continue processing */
        if (pos >= len)
            return KEEL_FLOW_OK;
    }

    while (pos < len) {
        /* Frame extraction */
        ssize_t flen = flow->frame_len(sf->ctx, data+pos, len-pos, 0);
        if (flen < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "Worker %u: session %lu: protocol framing error",
                        worker->id, (unsigned long)session->id);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, errors_proto);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, proxy_state_desync_total);
            return KEEL_FLOW_ERROR;
        }
        if (flen == 0) {
            if (keel_residual_append(&session->client_residual,
                                     data + pos, len - pos) < 0) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                    "Worker %u: session %lu: failed to buffer partial FE header",
                    worker->id, (unsigned long)session->id);
                return KEEL_FLOW_ERROR;
            }
            session->client_residual.expected_len = 0;
            session->client_residual.header_complete = false;
            return KEEL_FLOW_OK;
        }

        /* Jumbo message detection: the declared frame length exceeds
         * the bytes available in this recv buffer.  Reassemble ordinary
         * partial frames before protocol classification; only stream very
         * large frames when a backend is already available. */
        bool jumbo_msg = false;
        size_t jumbo_remaining = 0;
        size_t available = len - pos;
        if ((size_t)flen > available) {
            size_t total_frame = (size_t)flen;

            if (sf->session_max_buffered_bytes > 0 &&
                total_frame > sf->session_max_buffered_bytes) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                    "Worker %u: session %lu: message too large "
                    "(%zu bytes > limit %zu) — closing",
                    worker ? worker->id : 0u,
                    session ? (unsigned long)session->id : 0ul,
                    total_frame, sf->session_max_buffered_bytes);
                if (worker && worker->stats_ctx)
                    KEEL_STAT_INC(worker->stats_ctx, errors_proto);
                return KEEL_FLOW_ERROR;
            }

            if (session->server_fd < 0 ||
                total_frame <= KEEL_FE_PARTIAL_BUFFER_LIMIT) {
                if (keel_residual_append(&session->client_residual,
                                         data + pos, available) < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                        "Worker %u: session %lu: failed to buffer partial FE frame",
                        worker->id, (unsigned long)session->id);
                    return KEEL_FLOW_ERROR;
                }
                session->client_residual.expected_len = total_frame;
                session->client_residual.header_complete = true;
                return KEEL_FLOW_OK;
            }

            jumbo_msg = true;
            jumbo_remaining = total_frame - available;
            flen = (ssize_t)available;
        } else if (sf->session_max_buffered_bytes > 0 &&
                   (size_t)flen > sf->session_max_buffered_bytes) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "Worker %u: session %lu: message too large "
                "(%zu bytes > limit %zu) — closing",
                worker ? worker->id : 0u,
                session ? (unsigned long)session->id : 0ul,
                (size_t)flen, sf->session_max_buffered_bytes);
            if (worker && worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, errors_proto);
            return KEEL_FLOW_ERROR;
        }

        /* Ask protocol what to do */
        keel_fe_action_t act;
        uint64_t _fe_t0 = keel_instr_begin(WORKER_INSTR(worker), KEEL_INSTR_PROTO_FE_MSG);
        int rc = flow->on_fe_msg(sf->ctx, data+pos, (size_t)flen, &act);
        keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_PROTO_FE_MSG, _fe_t0);
        KEEL_DEBUG_LOG("W%u: on_fe_msg rc=%d act.type=%d flen=%zd\n",
                    worker->id, rc, act.type, flen);
        if (rc < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "Worker %u: session %lu: on_fe_msg error",
                        worker->id, (unsigned long)session->id);
            /* Auth errors are signalled during handshake phase */
            if (worker->stats_ctx) {
                if (sf->phase == KEEL_PHASE_HANDSHAKE_AUTH)
                    KEEL_STAT_INC(worker->stats_ctx, errors_auth);
                else
                    KEEL_STAT_INC(worker->stats_ctx, errors_proto);
                if (sf->phase != KEEL_PHASE_HANDSHAKE_AUTH)
                    KEEL_STAT_INC(worker->stats_ctx, proxy_state_desync_total);
            }
            return KEEL_FLOW_ERROR;
        }

        /* Update pins */
        keel_session_flow_apply_pin_change(sf, session, worker,
                                           act.pin_update, act.pin_clear);

        /* Dispatch on action type */
        switch (act.type) {

        case KEEL_FE_ACT_SSL_REQUEST: {
            /* If TLS is configured: accept SSL, signal worker to run handshake */
            if (session->worker &&
                session->worker->tls_config.mode != KEEL_TLS_DISABLE) {
                /* Send 'S' to accept TLS */
                static const uint8_t ssl_accept = 'S';
                ssize_t s = keel_try_send_nb(session->client_fd, &ssl_accept, 1);
                KEEL_DEBUG_LOG("W%u: sent SSL accept 'S' result=%zd fd=%d\n",
                            worker->id, s, session->client_fd);
                if (s < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: SSL accept send failed: %s",
                                worker->id, strerror(errno));
                    return KEEL_FLOW_ERROR;
                }
                /* Signal worker to start TLS handshake */
                return KEEL_FLOW_TLS_HANDSHAKE;
            }
            /* TLS disabled: send 'N' to decline SSL */
            if (act.fe_response && act.fe_response_len > 0) {
                ssize_t s = keel_try_send_nb(session->client_fd, act.fe_response,
                                            act.fe_response_len);
                KEEL_DEBUG_LOG("W%u: sent SSL decline '%c' result=%zd fd=%d\n",
                            worker->id, act.fe_response[0], s, session->client_fd);
                if (s < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: SSL decline send failed: %s",
                                worker->id, strerror(errno));
                    return KEEL_FLOW_ERROR;
                }
                if ((size_t)s < act.fe_response_len)
                    return defer_send(sf, session->client_fd,
                                      (const uint8_t*)act.fe_response + s,
                                      act.fe_response_len - (size_t)s,
                                      KEEL_FLOW_OK);
            }
            /* Re-arm FE recv — client will retry with real StartupMessage */
            pos += (size_t)flen;
            continue;
        }

        case KEEL_FE_ACT_AUTH_COMPLETE:
            /* Downgrade protection: reject plaintext when TLS is required */
            if (session->worker &&
                session->worker->tls_config.mode == KEEL_TLS_REQUIRE &&
                !(session->flags & KEEL_SESSION_FLAG_SSL)) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_TLS,
                    "Worker %u: rejecting plaintext connection (tls_mode=require) session %lu",
                    worker->id, (unsigned long)session->id);
                keel_tls_stat_downgrade_rejected();
                /* Send PostgreSQL ErrorResponse: FATAL 08004 */
                static const uint8_t tls_required_err[] = {
                    'E',  /* Error */
                    0,0,0,62,  /* length = 62 */
                    'S','F','A','T','A','L',0,
                    'V','F','A','T','A','L',0,
                    'C','0','8','0','0','4',0,
                    'M','n','o',' ','e','n','c','r','y','p','t','i','o','n',0,
                    'D','T','L','S',' ','r','e','q','u','i','r','e','d',0,
                    0  /* terminator */
                };
                keel_try_send_nb(session->client_fd, tls_required_err, sizeof(tls_required_err));
                return KEEL_FLOW_CLOSED;
            }
            /* Copy client identity from protocol context to session */
            if (act.client_username && act.client_username[0]) {
                strncpy(session->username, act.client_username,
                        sizeof(session->username) - 1);
                session->username[sizeof(session->username) - 1] = '\0';
            }
            if (act.client_database && act.client_database[0]) {
                strncpy(session->database, act.client_database,
                        sizeof(session->database) - 1);
                session->database[sizeof(session->database) - 1] = '\0';
            }
            /* Send handshake response to frontend — non-blocking, defer if partial */
            KEEL_DEBUG_LOG("W%u: AUTH_COMPLETE fe_response=%p len=%zu\n",
                        worker->id, (void*)act.fe_response, act.fe_response_len);
            if (act.fe_response && act.fe_response_len > 0) {
                ssize_t s = keel_try_send_nb(session->client_fd, act.fe_response,
                                            act.fe_response_len);
                KEEL_DEBUG_LOG("W%u: handshake sent %zd bytes to fd=%d\n",
                            worker->id, s, session->client_fd);
                if (s < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: handshake send failed: %s",
                                worker->id, strerror(errno));
                    return KEEL_FLOW_ERROR;
                }
                worker->stats.bytes_sent += (size_t)s < act.fe_response_len
                    ? (uint64_t)act.fe_response_len   /* will finish via deferred */
                    : (uint64_t)act.fe_response_len;
                if ((size_t)s < act.fe_response_len) {
                    sf->phase = KEEL_PHASE_READY;
                    session->state = KEEL_SESSION_READY;
                    return defer_send(sf, session->client_fd,
                                      (const uint8_t*)act.fe_response + s,
                                      act.fe_response_len - (size_t)s,
                                      KEEL_FLOW_OK);
                }
            }
            sf->phase = KEEL_PHASE_READY;
            session->state = KEEL_SESSION_READY;
            session->flags |= KEEL_SESSION_FLAG_AUTHENTICATED;

            /* Audit: AUTH_OK — client successfully authenticated. */
            if (worker->audit_log) {
                keel_audit_emit_auth(
                    (keel_audit_log_t*)worker->audit_log,
                    KEEL_AUDIT_AUTH_OK,
                    session->username[0] ? session->username : NULL,
                    session->database[0] ? session->database : NULL,
                    NULL, 0, NULL);
            }

            pos += (size_t)flen;
            continue;

        case KEEL_FE_ACT_QUERY:
        case KEEL_FE_ACT_FORWARD_TO_BACKEND: {
            /* Need a backend connection */
            KEEL_DEBUG_LOG("W%u: QUERY/FWD case entered, phase=%d\n", worker->id, sf->phase);
            sf->phase = KEEL_PHASE_QUERY;

            /* --- Stats: query counters --- */
            if (act.type == KEEL_FE_ACT_QUERY) {
                session->query_count++;
                session->query_start_ns = (uint64_t)keel_stats_now_ns();
                if (worker->stats_ctx) {
                    KEEL_STAT_INC(worker->stats_ctx, queries_total);
                    if (KEEL_TIER_HAS_FULL_STATS(sf->mode)) {
                        if (act.effect & KEEL_QE_READONLY)
                            KEEL_STAT_INC(worker->stats_ctx, queries_read);
                        if (act.effect & (KEEL_QE_WRITE | KEEL_QE_DDL))
                            KEEL_STAT_INC(worker->stats_ctx, queries_write);
                        if (act.effect & KEEL_QE_BEGINS_TX)
                            KEEL_STAT_INC(worker->stats_ctx, queries_tx);
                    }
                }
            }

            /* === EARLY CANCEL: CancelRequest arrived before backend was borrowed ===
             * A cancel handler running cross-worker may have set cancel_pending
             * atomically when it found backend_conn == NULL.  Synthesise
             * E(57014) to the client and skip backend borrow entirely.  The
             * next ReadyForQuery will come from the recovery query's response,
             * which is the correct protocol sync point. */
            if (act.type == KEEL_FE_ACT_QUERY &&
                atomic_load_explicit(&session->cancel_pending, memory_order_acquire)) {
                atomic_store_explicit(&session->cancel_pending, false, memory_order_relaxed);
                if (flow->generate_error) {
                    uint8_t ebuf[256];
                    ssize_t elen = flow->generate_error(sf->ctx, "57014",
                        "canceling statement due to user request",
                        ebuf, sizeof(ebuf));
                    if (elen > 0)
                        keel_try_send_nb(session->client_fd, ebuf, (size_t)elen);
                }
                sf->phase = KEEL_PHASE_READY;
                pos += (size_t)flen;
                continue;
            }

            /* === HOOK: AFTER_QUERY_READ ===
             * Raw query bytes are available.  Hooks can inspect sql_text,
             * modify route_hint, or abort the query.
             * query_tree is NULL here: parse has not run yet. */
            if (KEEL_TIER_HAS_HOOKS(sf->mode) && act.type == KEEL_FE_ACT_QUERY) {
                uint64_t _hook_t0 = keel_instr_begin(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN);
                keel_hook_ctx_t hctx;
                engine_fill_hook_ctx(&hctx, session, &act, NULL);
                hctx.hook_point = KEEL_HOOK_AFTER_QUERY_READ;
                keel_hook_registry_t* hooks = worker->hooks;
                if (!keel_hook_fire(hooks, KEEL_HOOK_AFTER_QUERY_READ, &hctx)) {
                    keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN, _hook_t0);
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "W%u: session %lu: query aborted by AFTER_QUERY_READ hook: %s",
                        worker->id, (unsigned long)session->id,
                        hctx.error_msg[0] ? hctx.error_msg : "hook returned false");
                    /* Send error to client and drop this message */
                    pos += (size_t)flen;
                    continue;
                }
                engine_apply_hook_ctx(&hctx, &act);
                keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN, _hook_t0);
            }

            /* === HOOK: AFTER_QUERY_PARSE ===
             * SQL has been classified (effect flags, route hint set by protocol).
             * Hooks can override classification or abort.
             * query_tree is the full parsed AST; hooks can inspect tables,
             * columns, subqueries, and the raw keel_sql_node_t*.
             * Fires for every query, not just first-in-session. */
            if (KEEL_TIER_HAS_HOOKS(sf->mode) && act.type == KEEL_FE_ACT_QUERY) {
                uint64_t _hook_t1 = keel_instr_begin(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN);
                keel_arena_t* _aqp_qt_arena = keel_arena_create(8192);
                keel_str_t _aqp_sql = { .data = act.sql_view, .len = act.sql_view_len };
                const keel_qt_query_t* _aqp_qt = _aqp_qt_arena
                    ? keel_sql_analyze_full(_aqp_sql, _aqp_qt_arena) : NULL;
                keel_hook_ctx_t hctx;
                engine_fill_hook_ctx(&hctx, session, &act, _aqp_qt);
                hctx.hook_point = KEEL_HOOK_AFTER_QUERY_PARSE;
                keel_hook_registry_t* hooks = worker->hooks;
                if (!keel_hook_fire(hooks, KEEL_HOOK_AFTER_QUERY_PARSE, &hctx)) {
                    keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN, _hook_t1);
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "W%u: session %lu: query aborted by AFTER_QUERY_PARSE hook: %s",
                        worker->id, (unsigned long)session->id,
                        hctx.error_msg[0] ? hctx.error_msg : "hook returned false");
                    pos += (size_t)flen;
                    continue;
                }
                engine_apply_hook_ctx(&hctx, &act);
                keel_arena_destroy(_aqp_qt_arena);
                keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN, _hook_t1);
            }

            /* Eager transaction state update.
             * The authoritative in_transaction flag is set from backend
             * responses (ReadyForQuery / OK status flags), but hooks fire
             * BEFORE the backend responds.  With Extended Query pipelining,
             * the backend may not have responded to BEGIN yet when the next
             * Parse/Execute arrives.  Update eagerly so subsequent hooks
             * see the correct in_transaction state.
             * The backend response handler (tx_state_changed) will
             * reconcile with the authoritative server-side state later. */
            if (act.type == KEEL_FE_ACT_QUERY) {
                if (act.effect & KEEL_QE_BEGINS_TX) {
                    session->in_transaction = true;
                    sf->txn_had_writes = false;   /* reset for new explicit transaction */
                } else if (act.effect & KEEL_QE_ENDS_TX) {
                    session->in_transaction = false;
                }

                /* Trace event: query classified */
                KEEL_TRACE_EVENT(session, "query.classify");
                KEEL_TRACE_ATTR_INT(session, "query.route",
                    (int64_t)act.route_hint);

                /* OpenTelemetry semantic conventions for DB spans:
                 * db.statement  — the first 512 characters of the SQL text
                 * db.operation  — SELECT / INSERT / UPDATE / DELETE / DDL / OTHER */
                if (session->trace_sampled && act.sql_view && act.sql_view_len > 0) {
                    /* db.statement: truncated copy so we don't store a pointer
                     * into a buffer that may be recycled before span export. */
                    char db_stmt[513];
                    size_t stmt_len = act.sql_view_len < 512 ? act.sql_view_len : 512;
                    memcpy(db_stmt, act.sql_view, stmt_len);
                    db_stmt[stmt_len] = '\0';
                    keel_span_set_attr_str(&session->trace_span, "db.statement", db_stmt);

                    const char* db_op =
                        (act.effect & KEEL_QE_DDL)      ? "DDL"    :
                        (act.effect & KEEL_QE_WRITE)    ? "WRITE"  :
                        (act.effect & KEEL_QE_READONLY) ? "SELECT" :
                                                          "OTHER";
                    keel_span_set_attr_str(&session->trace_span, "db.operation", db_op);
                }
            }

            /* === QUERY RESULT CACHE GET ===
             * For autocommit read-only queries when the worker has a live
             * cache: compute the digest, check the cache, and serve the
             * response from cache if available (skipping backend entirely).
             * On a miss: record the digest so on_be_data can store the result. */
            if (worker->query_cache &&
                act.type == KEEL_FE_ACT_QUERY &&
                (act.effect & KEEL_QE_READONLY) &&
                !session->in_transaction &&
                act.sql_view && act.sql_view_len > 0) {
                /* NUL-terminate the SQL view for keel_query_cache_digest() */
                char sql_nt[4096];
                size_t copy_len = act.sql_view_len < sizeof(sql_nt) - 1
                                  ? act.sql_view_len : sizeof(sql_nt) - 1;
                memcpy(sql_nt, act.sql_view, copy_len);
                sql_nt[copy_len] = '\0';

                uint8_t digest[32];
                keel_error_t drc = keel_query_cache_digest(sql_nt, digest);
                if (drc == KEEL_OK) {
                    const uint8_t* cached_result;
                    size_t cached_len;
                    keel_error_t grc = keel_query_cache_get(
                        worker->query_cache, digest, &cached_result, &cached_len);
                    if (grc == KEEL_OK) {
                        /* Cache HIT — send stored response directly to client */
                        keel_try_send_nb(session->client_fd,
                                         cached_result, cached_len);
                        sf->phase = KEEL_PHASE_READY;
                        pos += (size_t)flen;
                        continue; /* next message in this buffer, if any */
                    }
                    /* Cache MISS — arm capture so on_be_data stores the result */
                    memcpy(sf->cache_digest, digest, 32);
                    sf->cache_pending = true;
                    sf->cache_capture_len = 0;
                }
            }

            /* Get or borrow a backend connection */
            backend_conn_t* be_conn = session->backend_conn;

            if (!be_conn && session->server_fd < 0) {
                /* Deferred-BEGIN optimisation: when a shard router is present
                 * and an explicit write-mode BEGIN arrives before the target
                 * shard is known, synthesise CommandComplete/ReadyForQuery
                 * locally and buffer the wire payload.  On the next routable
                 * DML/SELECT the buffered BEGIN is forwarded to the correct
                 * shard backend first, preventing random shard assignment. */
                if (worker->router &&
                    (act.effect & KEEL_QE_BEGINS_TX) &&
                    !(act.effect & KEEL_QE_READONLY) &&
                    (sf->pins & ~(keel_flow_pin_reason_t)(
                        KEEL_FPIN_TRANSACTION | KEEL_FPIN_EXTENDED_PROTO)) ==
                        KEEL_FPIN_NONE &&
                    act.be_payload && act.be_payload_len > 0 &&
                    act.be_payload_len <= sizeof(sf->begin_deferred_payload)) {
                    memcpy(sf->begin_deferred_payload, act.be_payload,
                           act.be_payload_len);
                    sf->begin_deferred_payload_len = act.be_payload_len;
                    sf->begin_deferred = true;
                    /* CommandComplete('BEGIN') = 'C' + uint32(10) + "BEGIN\0" (11 bytes)
                     * ReadyForQuery('T')       = 'Z' + uint32(5)  + 'T'      (6 bytes) */
                    static const uint8_t begin_synth[] = {
                        'C', 0, 0, 0, 10, 'B','E','G','I','N', 0,
                        'Z', 0, 0, 0,  5, 'T'
                    };
                    keel_try_send_nb(session->client_fd, begin_synth,
                                     sizeof(begin_synth));
                    pos += (size_t)flen;
                    sf->phase = KEEL_PHASE_READY;
                    continue;
                }

                session->pool_wait_ns = (uint64_t)keel_stats_now_ns();
                uint64_t _route_t0 = keel_instr_begin(WORKER_INSTR(worker), KEEL_INSTR_ROUTE_DECISION);
                /* No existing backend connection — need to acquire one.
                 * Select pool based on route_hint (Spec §7 routing). */
                backend_pool_t* pool = NULL;

                /* Sticky-primary override: if this session wrote recently,
                 * force read-only queries to the primary to prevent stale
                 * reads from async replicas (read-after-write consistency).
                 *
                 * Exception: an explicit BEGIN READ ONLY transaction is exempt
                 * — the client has opted into a potentially-stale snapshot and
                 * MUST go to a replica pool.  Detect it as a BEGINS_TX action
                 * that classify_sql already routed to FROUTE_REPLICA. */
                uint8_t route = act.route_hint;
                if (KEEL_TIER_HAS_ROUTING(sf->mode)) {
                    bool explicit_read_only_tx =
                        (act.effect & KEEL_QE_BEGINS_TX) &&
                        (act.route_hint == KEEL_FROUTE_REPLICA);
                    if (!explicit_read_only_tx &&
                        route == KEEL_FROUTE_REPLICA &&
                        sf->last_write_ns != 0) {
                        uint64_t now = engine_now_ns();
                        uint64_t ttl_ms = sf->sticky_primary_ttl_ms
                                            ? sf->sticky_primary_ttl_ms
                                            : KEEL_STICKY_PRIMARY_TTL_MS;
                        if (keel_ssv_requires_primary(sf->consistency_atoms,
                                                      now, (uint32_t)ttl_ms)) {
                            /* Reactor-safe fallback: a real LSN/GTID token must
                             * not be expired by time alone. Route to primary
                             * until a future async verifier proves a replica is
                             * caught up. */
                            route = KEEL_FROUTE_PRIMARY;
                            if (worker->stats_ctx)
                                KEEL_STAT_INC(worker->stats_ctx, sticky_primary_hits);
                        } else if (!keel_ssv_consistency_ttl_ok(sf->consistency_atoms,
                                                                now, (uint32_t)ttl_ms)) {
                            /* Timestamp-only legacy fallback for local writes
                             * where no exact token has been captured yet. */
                            route = KEEL_FROUTE_PRIMARY;
                            if (worker->stats_ctx)
                                KEEL_STAT_INC(worker->stats_ctx, sticky_primary_hits);
                        } else {
                            /* TTL expired for a timestamp-only sticky marker. */
                            sf->last_write_ns = 0;
                        }
                    }
                } /* KEEL_TIER_HAS_ROUTING */

                /* === HOOK: BEFORE_ROUTE ===
                 * Final chance to override the routing decision before
                 * a backend pool is selected.
                 * query_tree is the full parsed AST for inspection or mutation. */
                if (KEEL_TIER_HAS_HOOKS(sf->mode) && act.type == KEEL_FE_ACT_QUERY) {
                    keel_arena_t* _br_qt_arena = keel_arena_create(8192);
                    keel_str_t _br_sql = { .data = act.sql_view, .len = act.sql_view_len };
                    const keel_qt_query_t* _br_qt = _br_qt_arena
                        ? keel_sql_analyze_full(_br_sql, _br_qt_arena) : NULL;
                    keel_hook_ctx_t hctx;
                    engine_fill_hook_ctx(&hctx, session, &act, _br_qt);
                    hctx.hook_point = KEEL_HOOK_BEFORE_ROUTE;
                    /* Override route_hint from sticky-primary logic */
                    hctx.route_hint = (route == KEEL_FROUTE_REPLICA)
                                      ? KEEL_HOOK_ROUTE_REPLICA
                                      : (route == KEEL_FROUTE_PRIMARY)
                                        ? KEEL_HOOK_ROUTE_PRIMARY
                                        : KEEL_HOOK_ROUTE_ANY;
                    keel_hook_registry_t* hooks = worker->hooks;
                    if (!keel_hook_fire(hooks, KEEL_HOOK_BEFORE_ROUTE, &hctx)) {
                        keel_arena_destroy(_br_qt_arena);
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "W%u: session %lu: query aborted by BEFORE_ROUTE hook: %s",
                            worker->id, (unsigned long)session->id,
                            hctx.error_msg[0] ? hctx.error_msg : "hook returned false");
                        pos += (size_t)flen;
                        continue;
                    }
                    keel_arena_destroy(_br_qt_arena);
                    /* Apply hook's routing override */
                    switch (hctx.route_hint) {
                    case KEEL_HOOK_ROUTE_PRIMARY: route = KEEL_FROUTE_PRIMARY; break;
                    case KEEL_HOOK_ROUTE_REPLICA: route = KEEL_FROUTE_REPLICA; break;
                    case KEEL_HOOK_ROUTE_ANY:     route = KEEL_FROUTE_ANY;     break;
                    }
                    /* Apply SQL rewrite if a BEFORE_ROUTE hook (e.g. query_rules)
                     * signalled one.  Replace the sql_view used for routing and
                     * analysis, and replace be_payload so the rewritten SQL is
                     * forwarded to the backend instead of the original client bytes. */
                    if (hctx.rewrite_sql && hctx.rewrite_sql_len > 0) {
                        act.sql_view     = hctx.rewrite_sql;
                        act.sql_view_len = hctx.rewrite_sql_len;
                        if (hctx.rewrite_be_payload && hctx.rewrite_be_payload_len > 0) {
                            act.be_payload     = hctx.rewrite_be_payload;
                            act.be_payload_len = hctx.rewrite_be_payload_len;
                        }
                    }

                    /* Apply per-rule SQL rewrite policy knobs (statement_timeout,
                     * force_read_only, inject_search_path) via keel_sql_rewrite().
                     * Only called when at least one knob is set; the resulting SQL
                     * is allocated from a short-lived per-query arena. */
                    if (hctx.sql_rewrite_add_read_only ||
                        hctx.sql_rewrite_add_timeout   ||
                        hctx.sql_rewrite_search_path) {
                        keel_sql_rewrite_opts_t rw_opts = {
                            .add_read_only         = hctx.sql_rewrite_add_read_only,
                            .add_statement_timeout = hctx.sql_rewrite_add_timeout,
                            .statement_timeout     = hctx.sql_rewrite_timeout_ms,
                            .search_path           = hctx.sql_rewrite_search_path,
                        };
                        keel_arena_t* rw_arena = keel_arena_create(4096);
                        if (rw_arena) {
                            keel_str_t rw_sql_in  = { .data = act.sql_view,
                                                       .len  = act.sql_view_len };
                            keel_str_t rw_sql_out = {0};
                            if (keel_sql_rewrite(rw_sql_in, &rw_opts,
                                                 rw_arena, &rw_sql_out) == KEEL_OK
                                && rw_sql_out.data && rw_sql_out.len > 0) {
                                act.sql_view     = rw_sql_out.data;
                                act.sql_view_len = rw_sql_out.len;
                                /* Clear pre-built payload so engine re-builds from new SQL */
                                act.be_payload     = NULL;
                                act.be_payload_len = 0;
                            }
                            /* Arena freed at end of request (engine_flow scope ends) */
                            keel_arena_destroy(rw_arena);
                        }
                    }
                }  /* end BEFORE_ROUTE hook block */

                /* === SHARD ROUTING ===
                 * When a router is configured, dispatch the SQL to determine
                 * whether this query targets a single shard (SINGLE) or all
                 * shards (SCATTER).  For SCATTER+requires_merge we execute the
                 * query synchronously across all shards, merge results, send
                 * back to the client, and skip the normal backend pool path. */

                /* Audit: DDL event — emitted before routing to record every
                 * schema-changing statement regardless of target backend. */
                if (worker->audit_log && (act.effect & KEEL_QE_DDL) &&
                    act.sql_view && act.sql_view_len > 0) {
                    char ddl_sql[513];
                    size_t ddl_len = act.sql_view_len < 512 ? act.sql_view_len : 512;
                    memcpy(ddl_sql, act.sql_view, ddl_len);
                    ddl_sql[ddl_len] = '\0';
                    keel_audit_emit_ddl(
                        (keel_audit_log_t*)worker->audit_log,
                        session->username[0] ? session->username : NULL,
                        session->database[0] ? session->database : NULL,
                        NULL, 0,
                        ddl_sql);
                }

                struct backend_pool* shard_dispatched_pool = NULL;
                if (worker->router && act.sql_view && act.sql_view_len > 0) {
                    /* Build a NUL-terminated copy of the SQL for the router */
                    char sql_nt[4096];
                    size_t copy_len = act.sql_view_len < sizeof(sql_nt) - 1
                                      ? act.sql_view_len : sizeof(sql_nt) - 1;
                    memcpy(sql_nt, act.sql_view, copy_len);
                    sql_nt[copy_len] = '\0';

                    keel_str_t sql_str = { .data = sql_nt, .len = copy_len };
                    bool is_write = (act.effect & (KEEL_QE_WRITE | KEEL_QE_DDL)) != 0;
                    keel_dispatch_result_t dr;
                    keel_error_t derr = keel_router_dispatch_sql(
                        worker->router, sql_str, NULL, NULL, is_write, &dr);

                    if (derr == KEEL_OK) {
                        /* Cache the dispatch reason on the session so the
                         * query logger picks it up on the next emit. */
                        if (session) {
                            const char* reason =
                                (dr.kind == KEEL_DISPATCH_SCATTER) ? "SHARD_SCATTER"
                              : (dr.kind == KEEL_DISPATCH_SINGLE)  ? "SHARD_SINGLE"
                              :                                       "ROUTED";
                            snprintf(session->last_route_reason,
                                     sizeof session->last_route_reason,
                                     "%s", reason);
                        }
                        /* === HOOK: AFTER_ROUTE ===
                         * Routing decision is final; dispatch kind and shard
                         * index(es) are available.  Hooks can veto execution
                         * (set sctx.veto_execution + sctx.veto_reason) or
                         * observe the decision for audit / telemetry.
                         * Zero-cost when no hooks are registered for this point. */
                        if (KEEL_TIER_HAS_HOOKS(sf->mode) && act.type == KEEL_FE_ACT_QUERY &&
                            KEEL_HOOK_FIRED_FOR(worker->hooks, KEEL_HOOK_AFTER_ROUTE)) {
                            uint64_t _ar_t0 = keel_instr_begin(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN);
                            keel_arena_t* _ar_qt_arena = keel_arena_create(8192);
                            const keel_qt_query_t* _ar_qt = _ar_qt_arena
                                ? keel_sql_analyze_full(sql_str, _ar_qt_arena) : NULL;
                            keel_hook_ctx_t hctx;
                            keel_hook_shard_ctx_t sctx;
                            engine_fill_hook_ctx(&hctx, session, &act, _ar_qt);
                            engine_fill_shard_ctx(&sctx, &dr);
                            hctx.hook_point = KEEL_HOOK_AFTER_ROUTE;
                            hctx.ext = &sctx;
                            bool ok = keel_hook_fire(worker->hooks, KEEL_HOOK_AFTER_ROUTE, &hctx);
                            keel_arena_destroy(_ar_qt_arena);
                            keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN, _ar_t0);
                            if (!ok || sctx.veto_execution) {
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                    "W%u: session %lu: query vetoed by AFTER_ROUTE hook: %s",
                                    worker->id, (unsigned long)session->id,
                                    sctx.veto_execution ? sctx.veto_reason
                                    : (hctx.error_msg[0] ? hctx.error_msg : "hook returned false"));
                                uint8_t sendbuf[512];
                                size_t sendlen = 0;
                                {
                                    uint8_t errbuf[256];
                                    const char* reason = sctx.veto_execution ? sctx.veto_reason
                                                        : (hctx.error_msg[0] ? hctx.error_msg : "query vetoed by hook");
                                    ssize_t el = flow->generate_error(sf->ctx, "42501",
                                        reason, errbuf, sizeof(errbuf));
                                    if (el > 0) { memcpy(sendbuf, errbuf, (size_t)el); sendlen += (size_t)el; }
                                }
                                if (flow->generate_ready_for_query) {
                                    uint8_t z[16];
                                    ssize_t zlen = flow->generate_ready_for_query(sf->ctx, z, sizeof(z));
                                    if (zlen > 0) { memcpy(sendbuf + sendlen, z, (size_t)zlen); sendlen += (size_t)zlen; }
                                }
                                if (sendlen > 0) {
                                    keel_try_send_nb(session->client_fd, sendbuf, sendlen);
                                }
                                pos += (size_t)flen;
                                sf->phase = KEEL_PHASE_READY;
                                continue;
                            }
                        }

                        if (dr.kind == KEEL_DISPATCH_SINGLE) {
                            /* Route to the specific shard's backend pool.
                             * If the shard is marked unhealthy by the probe,
                             * still attempt routing — the connection attempt
                             * itself will fail if the shard is truly down.
                             * We return an error only if the shard pool cannot
                             * even be identified (not just probe-flagged). */
                            size_t shard_idx = dr.single.shard_index;
                            bool shard_found = false;
                            if (worker->server_pool &&
                                shard_idx < KEEL_MAX_SERVERS &&
                                worker->server_pool->shard_pool_index[shard_idx] != (size_t)-1) {
                                size_t srv_idx = worker->server_pool->shard_pool_index[shard_idx];
                                if (srv_idx < worker->server_pool_count &&
                                    worker->server_pools[srv_idx]) {
                                    shard_found = true;
                                    if (worker->server_pool->servers[srv_idx].healthy) {
                                        shard_dispatched_pool = worker->server_pools[srv_idx];
                                    } else {
                                        /* Shard is probe-flagged unhealthy.
                                         * Return error immediately — the test
                                         * test_write_to_down_shard_returns_error
                                         * depends on this behavior.  The probe
                                         * marks unhealthy only after a confirmed
                                         * failure so this is reliable. */
                                        KEEL_LOG_WARN(KEEL_LOG_CAT_CONN,
                                            "W%u: shard %zu is unhealthy (probe-flagged)",
                                            worker->id, shard_idx);
                                        uint8_t sendbuf[512];
                                        size_t sendlen = 0;
                                        {
                                            uint8_t errbuf[256];
                                            ssize_t el = flow->generate_error(sf->ctx, "08006",
                                                "shard unavailable", errbuf, sizeof(errbuf));
                                            if (el > 0) { memcpy(sendbuf, errbuf, (size_t)el); sendlen += (size_t)el; }
                                        }
                                        if (flow->generate_ready_for_query) {
                                            uint8_t z[16];
                                            ssize_t zlen = flow->generate_ready_for_query(sf->ctx, z, sizeof(z));
                                            if (zlen > 0) { memcpy(sendbuf + sendlen, z, (size_t)zlen); sendlen += (size_t)zlen; }
                                        }
                                        if (sendlen > 0) {
                                            ssize_t s = keel_try_send_nb(
                                                session->client_fd, sendbuf, sendlen);
                                            if (s > 0 && (size_t)s < sendlen) {
                                                pos += (size_t)flen;
                                                sf->phase = KEEL_PHASE_READY;
                                                return defer_send(sf,
                                                    session->client_fd,
                                                    sendbuf + s, sendlen - (size_t)s,
                                                    KEEL_FLOW_OK);
                                            }
                                        }
                                        pos += (size_t)flen;
                                        sf->phase = KEEL_PHASE_READY;
                                        continue;
                                    }
                                }
                            }
                            (void)shard_found;
                        } else if (dr.kind == KEEL_DISPATCH_SCATTER && !(act.effect & (KEEL_QE_WRITE | KEEL_QE_DDL))) {
                            /* Scatter read: execute on all shards, merge/concatenate, send to client */

                            /* === HOOK: BEFORE_SCATTER ===
                             * All shard routing decisions are in dr.scatter.
                             * Hooks can veto scatter execution entirely.
                             * Zero-cost when no hooks are registered. */
                            if (KEEL_TIER_HAS_HOOKS(sf->mode) && act.type == KEEL_FE_ACT_QUERY &&
                                KEEL_HOOK_FIRED_FOR(worker->hooks, KEEL_HOOK_BEFORE_SCATTER)) {
                                uint64_t _bs_t0 = keel_instr_begin(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN);
                                keel_arena_t* _bs_qt_arena = keel_arena_create(8192);
                                const keel_qt_query_t* _bs_qt = _bs_qt_arena
                                    ? keel_sql_analyze_full(sql_str, _bs_qt_arena) : NULL;
                                keel_hook_ctx_t hctx;
                                keel_hook_shard_ctx_t sctx;
                                engine_fill_hook_ctx(&hctx, session, &act, _bs_qt);
                                engine_fill_shard_ctx(&sctx, &dr);
                                hctx.hook_point = KEEL_HOOK_BEFORE_SCATTER;
                                hctx.ext = &sctx;
                                bool ok = keel_hook_fire(worker->hooks, KEEL_HOOK_BEFORE_SCATTER, &hctx);
                                keel_arena_destroy(_bs_qt_arena);
                                keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN, _bs_t0);
                                if (!ok || sctx.veto_execution) {
                                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                        "W%u: session %lu: scatter vetoed by BEFORE_SCATTER hook: %s",
                                        worker->id, (unsigned long)session->id,
                                        sctx.veto_execution ? sctx.veto_reason
                                        : (hctx.error_msg[0] ? hctx.error_msg : "hook returned false"));
                                    uint8_t sendbuf[512];
                                    size_t sendlen = 0;
                                    {
                                        uint8_t errbuf[256];
                                        const char* reason = sctx.veto_execution ? sctx.veto_reason
                                                            : (hctx.error_msg[0] ? hctx.error_msg : "scatter vetoed by hook");
                                        ssize_t el = flow->generate_error(sf->ctx, "42501",
                                            reason, errbuf, sizeof(errbuf));
                                        if (el > 0) { memcpy(sendbuf, errbuf, (size_t)el); sendlen += (size_t)el; }
                                    }
                                    if (flow->generate_ready_for_query) {
                                        uint8_t z[16];
                                        ssize_t zlen = flow->generate_ready_for_query(sf->ctx, z, sizeof(z));
                                        if (zlen > 0) { memcpy(sendbuf + sendlen, z, (size_t)zlen); sendlen += (size_t)zlen; }
                                    }
                                    if (sendlen > 0) {
                                        keel_try_send_nb(session->client_fd, sendbuf, sendlen);
                                    }
                                    pos += (size_t)flen;
                                    sf->phase = KEEL_PHASE_READY;
                                    continue;
                                }
                            }

                            uint64_t _scatter_t0 = engine_now_ns();
                            keel_scatter_obs_ctx_t scatter_obs = {
                                .trace_ctx = &session->trace_ctx,
                                .audit_log = (keel_audit_log_t*)worker->audit_log,
                                .username  = session->username,
                                .database  = session->database,
                            };
                            keel_engine_scatter_execute(
                                worker->server_pool,
                                worker->server_pools,
                                worker->server_pool_count,
                                sql_nt, &dr,
                                session->client_fd,
                                worker->scatter_merge_max_mem_bytes,
                                worker->scatter_merge_spill_dir,
                                &scatter_obs);
                            uint64_t _scatter_elapsed_us =
                                (engine_now_ns() - _scatter_t0) / 1000u;

                            /* === HOOK: AFTER_SCATTER ===
                             * Result already sent to client.  This hook is
                             * observational only — returning false has no
                             * effect on the response already delivered.
                             * Zero-cost when no hooks are registered. */
                            if (KEEL_TIER_HAS_HOOKS(sf->mode) && act.type == KEEL_FE_ACT_QUERY &&
                                KEEL_HOOK_FIRED_FOR(worker->hooks, KEEL_HOOK_AFTER_SCATTER)) {
                                uint64_t _as_t0 = keel_instr_begin(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN);
                                keel_arena_t* _as_qt_arena = keel_arena_create(8192);
                                const keel_qt_query_t* _as_qt = _as_qt_arena
                                    ? keel_sql_analyze_full(sql_str, _as_qt_arena) : NULL;
                                keel_hook_ctx_t hctx;
                                keel_hook_shard_ctx_t sctx;
                                engine_fill_hook_ctx(&hctx, session, &act, _as_qt);
                                engine_fill_shard_ctx(&sctx, &dr);
                                engine_annotate_scatter_result(&sctx, _scatter_elapsed_us,
                                    scatter_obs.rows_merged_out, scatter_obs.spilled_out);
                                hctx.hook_point = KEEL_HOOK_AFTER_SCATTER;
                                hctx.ext = &sctx;
                                keel_hook_fire(worker->hooks, KEEL_HOOK_AFTER_SCATTER, &hctx);
                                keel_arena_destroy(_as_qt_arena);
                                keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN, _as_t0);
                            }

                            /* Scatter result already sent; update phase and continue */
                            pos += (size_t)flen;
                            sf->phase = KEEL_PHASE_READY;
                            continue;
                        }
                        /* For SCATTER without merge (e.g. INSERT/UPDATE fan-out):
                         * execute a 2PC scatter write across all participating shards.
                         * Must only run when the router decided SCATTER — not SINGLE.
                         * Bug fix: without the dr.kind guard, single-shard INSERTs were
                         * incorrectly fanned out to all shards via 2PC. */
                        if (dr.kind == KEEL_DISPATCH_SCATTER &&
                            (act.effect & (KEEL_QE_WRITE | KEEL_QE_DDL))) {
                            keel_engine_scatter_write(
                                worker->server_pool,
                                worker->server_pools,
                                worker->server_pool_count,
                                sql_nt, &dr,
                                session->client_fd);
                            pos += (size_t)flen;
                            sf->phase = KEEL_PHASE_READY;
                            keel_dispatch_result_cleanup(&dr);
                            continue;
                        }
                    }
                    /* derr != KEEL_OK falls through here. Two cases:
                     *   1) reject_reason == NONE: no shard rule matched the
                     *      table — fall back to the non-sharded routing path
                     *      below (existing behaviour).
                     *   2) reject_reason != NONE: the dispatcher fail-closed
                     *      a known silent-wrong-result class (scatter gate
                     *      off, recursive CTE, …). Emit a PostgreSQL
                     *      ErrorResponse (SQLSTATE 0A000 = feature_not_supported)
                     *      plus ReadyForQuery, then advance the phase so the
                     *      session stays usable. Do NOT fall back. */
                    if (derr != KEEL_OK &&
                        dr.reject_reason != KEEL_DISPATCH_REJECT_NONE) {
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "W%u: session %lu: dispatch rejected (reason=%d): %s",
                            worker->id, (unsigned long)session->id,
                            (int)dr.reject_reason, dr.reject_message);
                        uint8_t sendbuf[512];
                        size_t  sendlen = 0;
                        {
                            uint8_t errbuf[256];
                            const char* reason = dr.reject_message[0]
                                ? dr.reject_message
                                : "dispatch rejected";
                            ssize_t el = flow->generate_error(sf->ctx, "0A000",
                                reason, errbuf, sizeof(errbuf));
                            if (el > 0) {
                                memcpy(sendbuf, errbuf, (size_t)el);
                                sendlen += (size_t)el;
                            }
                        }
                        if (flow->generate_ready_for_query) {
                            uint8_t z[16];
                            ssize_t zlen = flow->generate_ready_for_query(
                                sf->ctx, z, sizeof(z));
                            if (zlen > 0) {
                                memcpy(sendbuf + sendlen, z, (size_t)zlen);
                                sendlen += (size_t)zlen;
                            }
                        }
                        if (sendlen > 0) {
                            keel_try_send_nb(session->client_fd, sendbuf, sendlen);
                        }
                        pos += (size_t)flen;
                        sf->phase = KEEL_PHASE_READY;
                        keel_dispatch_result_cleanup(&dr);
                        continue;
                    }
                    /* KEEL_ERR_NOT_SUPPORTED with reject_reason==NONE:
                     * no shard rule matched — use existing routing. */
                }

                /* === Patch 2d-4: stale_read_policy=wait consultation ===
                 *
                 * If the deployment carries a router with
                 *   consistency_mode  = read_your_writes
                 *   stale_read_policy = wait
                 * AND this is a replica-eligible read AND the session has
                 * captured a consistency token (LSN/GTID) from a prior write,
                 * consult the router to see whether it would emit
                 * KEEL_ROUTE_REASON_WAIT_CATCHUP for this query.
                 *
                 * v0.5-alpha SCOPE: when the router says "WAIT", we cannot
                 * yet async-park + re-dispatch the session (the resume
                 * continuation lands in v0.5-beta — see
                 * `keel_engine_consult_catchup` in catchup_bridge.h, which
                 * has the enqueue path wired). So for now we degrade to the
                 * primary, log it, and bump a counter so operators can see
                 * how often the policy degrades. This is strictly safer than
                 * the alternative (serving a stale read from a behind
                 * replica) and gives operators a measurable signal to size
                 * the eventual park-and-re-dispatch budget. */
                if (route == KEEL_FROUTE_READ &&
                    worker->router != NULL &&
                    sf->last_write_token.value[0] != '\0' &&
                    act.sql_view != NULL &&
                    act.sql_view_len > 0) {

                    /* Parse the SQL just enough for the router to confirm
                     * "replica-safe read"; 64KiB arena matches the bridge
                     * unit test budget (smaller arenas overflow on real
                     * production SQL). */
                    keel_arena_t* _wc_arena = keel_arena_create(65536);
                    if (_wc_arena) {
                        keel_str_t _wc_sql = {
                            .data = act.sql_view,
                            .len  = act.sql_view_len,
                        };
                        const keel_qt_query_t* _wc_qt =
                            keel_sql_analyze_full(_wc_sql, _wc_arena);

                        keel_route_decision_t rd;
                        memset(&rd, 0, sizeof(rd));
                        worker->stats.wait_catchup_consulted_total++;
                        if (keel_engine_should_degrade_to_primary_on_wait(
                                worker->router,
                                _wc_qt,
                                &sf->last_write_token,
                                session->in_transaction,
                                &rd)) {
                            worker->stats.wait_catchup_degraded_to_primary++;
                            KEEL_LOG_INFO(KEEL_LOG_CAT_CONN,
                                "W%u: session %lu: stale_read_policy=wait "
                                "(WAIT_CATCHUP) degraded to primary in v0.5-alpha "
                                "(token=%.*s tl=%u replica_idx=%zu max_wait_ms=%u)",
                                worker->id, (unsigned long)session->id,
                                (int)sizeof(rd.wait_token.value), rd.wait_token.value,
                                rd.wait_token.timeline_id,
                                rd.wait_server_index, rd.wait_max_ms);
                            route = KEEL_FROUTE_WRITE;
                        }
                        keel_arena_destroy(_wc_arena);
                    }
                }

                switch (route) {
                case KEEL_FROUTE_READ:
                    /* Try RO pools first (round-robin, health-aware),
                     * then fall back to RW pools.
                     * Skip servers marked unhealthy by the probe thread. */
                    if (worker->server_pool && worker->server_pools) {
                        uint64_t base = worker->stats.rr_read_counter++;
                        /* Try RO servers */
                        for (size_t attempt = 0; attempt < worker->server_pool->ro_count; attempt++) {
                            size_t ci = (base + attempt) % worker->server_pool->ro_count;
                            size_t srv_idx = worker->server_pool->ro_indices[ci];
                            if (worker->server_pool->servers[srv_idx].healthy &&
                                srv_idx < worker->server_pool_count &&
                                worker->server_pools[srv_idx]) {
                                pool = worker->server_pools[srv_idx];
                                break;
                            }
                        }
                        /* Fallback: try RW servers for reads */
                        if (!pool) {
                            for (size_t i = 0; i < worker->server_pool->rw_count; i++) {
                                size_t srv_idx = worker->server_pool->rw_indices[i];
                                if (worker->server_pool->servers[srv_idx].healthy &&
                                    srv_idx < worker->server_pool_count &&
                                    worker->server_pools[srv_idx]) {
                                    pool = worker->server_pools[srv_idx];
                                    break;
                                }
                            }
                        }
                    }
                    /* Last resort: use first available pool */
                    if (!pool) {
                        for (size_t i = 0; i < worker->server_pool_count; i++) {
                            if (worker->server_pools[i]) { pool = worker->server_pools[i]; break; }
                        }
                    }
                    break;

                case KEEL_FROUTE_WRITE:
                default:
                    /* Write queries: try RW pools (preferred), then WO pools */
                    if (worker->server_pool && worker->server_pools) {
                        uint64_t base = worker->stats.rr_write_counter++;
                        /* Try RW servers first */
                        for (size_t attempt = 0; attempt < worker->server_pool->rw_count; attempt++) {
                            size_t ci = (base + attempt) % worker->server_pool->rw_count;
                            size_t srv_idx = worker->server_pool->rw_indices[ci];
                            if (worker->server_pool->servers[srv_idx].healthy &&
                                srv_idx < worker->server_pool_count &&
                                worker->server_pools[srv_idx]) {
                                pool = worker->server_pools[srv_idx];
                                break;
                            }
                        }
                        /* Fallback: try WO servers */
                        if (!pool) {
                            for (size_t i = 0; i < worker->server_pool->wo_count; i++) {
                                size_t srv_idx = worker->server_pool->wo_indices[i];
                                if (worker->server_pool->servers[srv_idx].healthy &&
                                    srv_idx < worker->server_pool_count &&
                                    worker->server_pools[srv_idx]) {
                                    pool = worker->server_pools[srv_idx];
                                    break;
                                }
                            }
                        }
                    }
                    /* Last resort: use first available pool */
                    if (!pool) {
                        for (size_t i = 0; i < worker->server_pool_count; i++) {
                            if (worker->server_pools[i]) { pool = worker->server_pools[i]; break; }
                        }
                    }
                    break;

                case KEEL_FROUTE_ANY:
                    /* Any server: try all pools round-robin */
                    if (worker->server_pools) {
                        uint64_t base = worker->stats.rr_any_counter++;
                        for (size_t attempt = 0; attempt < worker->server_pool_count; attempt++) {
                            size_t idx = (base + attempt) % worker->server_pool_count;
                            if (worker->server_pools[idx] &&
                                (!worker->server_pool ||
                                 worker->server_pool->servers[idx].healthy)) {
                                pool = worker->server_pools[idx];
                                break;
                            }
                        }
                    }
                    break;
                }

                /* Override pool with shard-dispatch result (if any). The switch
                 * above sets pool via round-robin; the shard router overrides it
                 * when a specific shard was identified from the SQL's WHERE clause. */
                if (shard_dispatched_pool) {
                    pool = shard_dispatched_pool;
                    /* Each shard is an independent PostgreSQL instance with
                     * its own WAL sequence. Token-based cross-shard replica
                     * checks remain disabled until they are reactor-owned. */
                }

                /* Borrow from pool — the ONLY path to a backend connection.
                 * No direct-connect fallback; the pool IS the safeguard. */
                if (!pool) {
                    /* No pool configured — fatal misconfiguration.
                     * Build combined error + RFQ into one buffer, send non-blocking. */
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "W%u: no backend pool configured", worker->id);
                    uint8_t sendbuf[512];
                    size_t sendlen = 0;
                    {
                        uint8_t errbuf[256];
                        ssize_t el = flow->generate_error(sf->ctx, "08001",
                            "no backend pool configured", errbuf, sizeof(errbuf));
                        if (el > 0) { memcpy(sendbuf, errbuf, (size_t)el); sendlen += (size_t)el; }
                    }
                    if (flow->generate_ready_for_query) {
                        uint8_t z[16];
                        ssize_t zlen = flow->generate_ready_for_query(sf->ctx, z, sizeof(z));
                        if (zlen > 0) { memcpy(sendbuf + sendlen, z, (size_t)zlen); sendlen += (size_t)zlen; }
                    }
                    if (sendlen > 0) {
                        ssize_t s = keel_try_send_nb(session->client_fd, sendbuf, sendlen);
                        if (s > 0 && (size_t)s < sendlen) {
                            sf->phase = KEEL_PHASE_READY;
                            return defer_send(sf, session->client_fd,
                                              sendbuf + s, sendlen - (size_t)s,
                                              KEEL_FLOW_OK);
                        }
                    }
                    pos += (size_t)flen;
                    sf->phase = KEEL_PHASE_READY;
                    continue;
                }

                /* When server_fd < 0 (no backend yet), KEEL_FPIN_EXTENDED_PROTO can
                 * only have been set by the CURRENT message — there is no prior
                 * extended-query sequence in flight (a prior Sync would have cleared
                 * it before releasing the backend).  Strip EXTENDED_PROTO when
                 * evaluating the borrow strategy so that a session with only
                 * prepared-stmt state correctly uses borrow_with_stmts (enabling
                 * stmt-set replay) rather than the hard-pin path.
                 *
                 * Without this, Parse/Bind setting EXTENDED_PROTO makes
                 * only_stmt_pin always false, borrow_with_stmts never fires,
                 * replay never happens, and Bind/Execute fail with
                 * "prepared statement does not exist". */
                keel_flow_pin_reason_t pins_stable =
                    sf->pins & ~(keel_flow_pin_reason_t)KEEL_FPIN_EXTENDED_PROTO;

                keel_flow_pin_reason_t pins_for_borrow = pins_stable;
                if ((act.effect & KEEL_QE_BEGINS_TX) &&
                    keel_ssv_is_stmt_only_pin(
                        pins_stable & ~(keel_flow_pin_reason_t)KEEL_FPIN_TRANSACTION,
                        sf->ps_mode)) {
                    pins_for_borrow =
                        pins_stable & ~(keel_flow_pin_reason_t)KEEL_FPIN_TRANSACTION;
                }

                if (pins_stable != KEEL_FPIN_NONE) {
                    /* KEEL_FPIN_PREPARED_STMT alone does NOT require hard-pin (spec §17).
                     * The backend is released at each transaction boundary and re-borrowed
                     * on demand via stmt_set_hash matching; mismatches trigger Parse replay
                     * to restore the statements on the new backend.
                     *
                     * Exception: KEEL_PS_MODE_PINNING — the user has opted for the
                     * hard-pin strategy where the first PREPARE locks the backend for
                     * the lifetime of the named-statement set.  In this mode,
                     * KEEL_FPIN_PREPARED_STMT is treated like any other hard-pin reason.
                     *
                     * All other pins (transaction, copy, cursor, etc.) always require
                     * the exact same backend connection. */
                    bool only_stmt_pin = keel_ssv_is_stmt_only_pin(pins_for_borrow,
                                                                  sf->ps_mode);
                    /* OFF mode: hard-pin like PINNING but with zero tracking overhead.
                     * No stmt_cache on the protocol side means no hash to match and
                     * no DISCARD ALL / replay to perform — just grab the same backend. */
                    bool pinning_mode  = (sf->ps_mode == KEEL_PS_MODE_PINNING
                                      ||  sf->ps_mode == KEEL_PS_MODE_OFF);
                    if (!only_stmt_pin || !flow->get_stmt_replay || pinning_mode) {
                        /* Hard-pinned — must use same backend connection */
                        be_conn = backend_pool_borrow_pinned(pool, session);
                        if (worker->stats_ctx && (pins_stable & KEEL_FPIN_PREPARED_STMT))
                            KEEL_STAT_INC(worker->stats_ctx, prepared_hardpin_count);
                        if (worker->stats_ctx && (pins_stable & KEEL_FPIN_OSC))
                            KEEL_STAT_INC(worker->stats_ctx, osc_sessions_detected);
                    } else {
                        /* Stmt-only pin: prefer a backend with matching prepared stmts */
                        keel_stmt_compat_profile_t stmt_profile;
                        memset(&stmt_profile, 0, sizeof(stmt_profile));
                        flow->get_stmt_replay(sf->ctx, NULL, NULL, NULL,
                                              &stmt_profile.stmt_set_hash);
                        if (flow->get_stmt_compat_profile) {
                            if (flow->get_stmt_compat_profile(sf->ctx, &stmt_profile) < 0)
                                memset(&stmt_profile, 0, sizeof(stmt_profile));
                        } else if (stmt_profile.stmt_set_hash != 0) {
                            /* No semantic callback: force conservative replay path. */
                            stmt_profile.semantic_unknown = true;
                        }
                        bool needs_replay = false;
                        be_conn = backend_pool_borrow_with_stmts(pool, session->state_hash,
                                                                  &stmt_profile, &needs_replay);
                        /* Signal the replay path below — non-zero means replay needed */
                        sf->stmt_replay_hash = (needs_replay && stmt_profile.stmt_set_hash)
                            ? stmt_profile.stmt_set_hash : 0;
                        sf->stmt_replay_profile = stmt_profile;
                        KEEL_DEBUG_LOG("W%u: FE borrow_with_stmts: stmt_hash=0x%016llx"
                            " needs_replay=%d be_fd=%d be_stmt_hash=0x%016llx"
                            " replay_hash=0x%016llx msg[0]=0x%02x\n",
                            worker->id, (unsigned long long)stmt_profile.stmt_set_hash,
                            (int)needs_replay,
                            be_conn ? be_conn->fd : -1,
                            be_conn ? (unsigned long long)be_conn->stmt_set_hash : 0ULL,
                            (unsigned long long)sf->stmt_replay_hash,
                            (data + pos) ? (unsigned)(data + pos)[0] : 0);
                        KEEL_DIAG_PS("BORROW sess=%lu sess_hash=0x%016llx be_fd=%d be_hash=0x%016llx needs_replay=%d msg='%c'",
                            (unsigned long)session->id,
                            (unsigned long long)stmt_profile.stmt_set_hash,
                            be_conn ? be_conn->fd : -1,
                            be_conn ? (unsigned long long)be_conn->stmt_set_hash : 0ULL,
                            (int)needs_replay,
                            (data + pos) && pos < len ? (char)(data + pos)[0] : '?');
                    }
                } else {
                    /* pins_stable == NONE: no sticky state — free borrow */
                    be_conn = backend_pool_borrow(pool, session->state_hash);
                    sf->stmt_replay_hash = 0;
                    memset(&sf->stmt_replay_profile, 0,
                           sizeof(sf->stmt_replay_profile));
                }

                if (!be_conn) {
                    /*
                     * Pool exhausted — try to queue in bounded wait queue.
                     * This implements fairness and prevents thundering herd.
                     * Pass the pool as userdata so the resume callback borrows
                     * from the correct pool (primary or replica).
                     */
	                    int queue_rc = backend_pool_queue_wait(pool, session, pool);
	                    if (queue_rc == 0) {
	                        keel_flow_result_t pending_resume = KEEL_FLOW_WAIT_BACKEND;
	                        if (act.no_response ||
	                            (act.msg_kind == KEEL_MSG_KIND_COPY &&
	                             act.be_payload && act.be_payload_len > 0 &&
	                             act.be_payload[0] == 'd') ||
	                            (act.msg_kind == KEEL_MSG_KIND_EXTENDED &&
	                             !(act.pin_clear & KEEL_FPIN_EXTENDED_PROTO))) {
	                            pending_resume = KEEL_FLOW_OK;
	                        }
	                        if (pos < len && data[pos] == 'H' &&
	                            sf->flush_pending_count > 0) {
	                            pending_resume = KEEL_FLOW_WAIT_BACKEND;
	                        }
	                        /* Store the pending FE message so resume_from_pool can forward it.
	                         *
	                         * If the protocol plugin rewrote the payload (e.g. COMMIT is
                         * rewritten to "SELECT txid_current() AS _keel_txid; COMMIT;" by
                         * the XID probe), use the rewritten payload rather than the raw
                         * client bytes.  Sending raw bytes would skip the rewrite and
                         * leave xid_probe_active stuck, causing every subsequent
                         * RowDescription to be silently absorbed ("D without T").
                         *
	                         * Save only the current message as pending.  Any pipelined tail
	                         * bytes from the same FE recv go into client_residual so the
	                         * worker immediately replays them if the pending message does
	                         * not itself require a backend response. */
	                        if (act.be_payload != NULL &&
	                            act.be_payload != (const uint8_t*)(data + pos)) {
	                            sf->pending_msg     = act.be_payload;
	                            sf->pending_msg_len = act.be_payload_len;
	                            sf->pending_msg_resume = pending_resume;
	                            apply_captured_fe_pin_effects(sf, session, worker, act.be_payload, act.be_payload_len);
	                            /* Save any pipelined bytes that follow in this recv. */
	                            if ((size_t)flen < len - pos) {
	                                keel_residual_append(&session->client_residual,
                                                    data + pos + (size_t)flen,
	                                                    len - pos - (size_t)flen);
	                            }
	                        } else {
	                            sf->pending_msg     = data + pos;
	                            sf->pending_msg_len = (size_t)flen;
	                            sf->pending_msg_resume = pending_resume;
	                            apply_captured_fe_pin_effects(sf, session, worker, data + pos, (size_t)flen);
	                            if ((size_t)flen < len - pos) {
	                                keel_residual_append(&session->client_residual,
	                                                    data + pos + (size_t)flen,
	                                                    len - pos - (size_t)flen);
	                            }
	                        }
                        sf->queued_for_pool = true;
                        KEEL_DEBUG_LOG("W%u: session %lu queued for pool (queue_size=%zu)\n",
                                    worker->id, (unsigned long)session->id, pool->wait_queue_size);
                        return KEEL_FLOW_WAIT_POOL;
                    }

                    /* Queue full — build combined error + RFQ, send non-blocking */
                    KEEL_LOG_WARN(KEEL_LOG_CAT_POOL, "W%u: pool exhausted and queue full, rejecting", worker->id);
                    uint8_t sendbuf2[512];
                    size_t sendlen2 = 0;
                    {
                        uint8_t errbuf[256];
                        ssize_t el = flow->generate_error(sf->ctx, "53300",
                            "connection pool exhausted, retry later", errbuf, sizeof(errbuf));
                        if (el > 0) { memcpy(sendbuf2, errbuf, (size_t)el); sendlen2 += (size_t)el; }
                    }
                    if (flow->generate_ready_for_query) {
                        uint8_t z[16];
                        ssize_t zlen = flow->generate_ready_for_query(sf->ctx, z, sizeof(z));
                        if (zlen > 0) { memcpy(sendbuf2 + sendlen2, z, (size_t)zlen); sendlen2 += (size_t)zlen; }
                    }
                    if (sendlen2 > 0) {
                        ssize_t s = keel_try_send_nb(session->client_fd, sendbuf2, sendlen2);
                        if (s > 0 && (size_t)s < sendlen2) {
                            sf->phase = KEEL_PHASE_READY;
                            return defer_send(sf, session->client_fd,
                                              sendbuf2 + s, sendlen2 - (size_t)s,
                                              KEEL_FLOW_OK);
                        }
                    }
                    pos += (size_t)flen;
                    sf->phase = KEEL_PHASE_READY;
                    continue;
                }

                session->backend_conn = be_conn;
                session->backend_generation = be_conn ? be_conn->generation : 0;
                session->server_fd = be_conn->fd;
                keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_ROUTE_DECISION, _route_t0);

                /* --- Stats: pool borrow --- */
                if (worker->stats_ctx) {
                    KEEL_STAT_INC(worker->stats_ctx, pool_borrows);
                    if (be_conn->current_state_hash == session->state_hash ||
                        session->state_hash == 0)
                        KEEL_STAT_INC(worker->stats_ctx, pool_hits);
                    else
                        KEEL_STAT_INC(worker->stats_ctx, pool_misses);
                    if (session->pool_wait_ns) {
                        uint64_t wait_ns = (uint64_t)keel_stats_now_ns() - session->pool_wait_ns;
                        KEEL_STAT_LATENCY(worker->stats_ctx, wait_latency_ns, wait_ns);
                        session->pool_wait_ns = 0;
                    }
                }

                /* Trace event: pool borrow + route decision */
                KEEL_TRACE_EVENT(session, "pool.borrow");
                KEEL_TRACE_ATTR_INT(session, "pool.server_fd", be_conn->fd);
                KEEL_TRACE_ATTR_INT(session, "pool.route", route);
                KEEL_TRACE_ATTR_INT(session, "pool.pin_reason", session->pin_reason);
                KEEL_TRACE_ATTR_INT(session, "pool.session_state_hash",
                                    (int64_t)session->state_hash);
                KEEL_TRACE_ATTR_INT(session, "pool.backend_state_hash",
                                    (int64_t)be_conn->current_state_hash);
                KEEL_TRACE_ATTR_INT(session, "pool.backend_stmt_hash",
                                    (int64_t)be_conn->stmt_set_hash);
                KEEL_TRACE_ATTR_INT(session, "pool.needs_state_sync",
                                    be_conn->needs_sync ? 1 : 0);
                KEEL_TRACE_ATTR_INT(session, "pool.needs_cleanup",
                                    be_conn->needs_full_cleanup ? 1 : 0);
                KEEL_TRACE_ATTR_INT(session, "pool.stmt_replay_hash",
                                    (int64_t)sf->stmt_replay_hash);

                /* State sync is performed later, immediately before the
                 * final client payload is forwarded, so hooks and batching
                 * still see the original query. */
            }

            keel_flow_result_t setup_result = KEEL_FLOW_OK;
            bool queue_begin_after_state_sync = false;
            bool needs_state_sync =
                backend_needs_state_sync(sf, session, session->backend_conn);

            /* Snapshot the prepared-statement replay buffer BEFORE any
             * pre-classification of the follow-buffer tail.  Prefetch may
             * include DDL (CREATE/ALTER/DROP) whose classification clears
             * the per-session stmt cache, which would otherwise leave the
             * later get_stmt_replay() call at the stmt-replay branch with
             * an empty buffer — even though the borrowed backend still
             * needs the OLD cached statements to satisfy the tail's
             * Bind(S_N).  Captured here so prefetch can safely mutate the
             * cache; freed below at the stmt-replay branch (or in the
             * dispose at the bottom of this block if not consumed). */
            uint8_t* prefetched_replay_buf   = NULL;
            size_t   prefetched_replay_len   = 0;
            uint32_t prefetched_replay_count = 0;
            uint64_t prefetched_replay_hash  = 0;

            const uint8_t* setup_follow_buf = NULL;
            size_t setup_follow_len = 0;
            if (act.be_payload != NULL &&
                act.be_payload != (const uint8_t*)(data + pos)) {
                setup_follow_buf = act.be_payload;
                setup_follow_len = act.be_payload_len;
            } else {
                setup_follow_buf = data + pos;
                setup_follow_len = len - pos;
                /* Pre-classify any pipelined messages in the tail so
                 * Parses inside the stash update stmt_cache.  Without
                 * this, named PS declared in a pipelined batch that
                 * follows a state_sync/stmt_replay trigger never reach
                 * the replay buffer → "S_N does not exist" after the
                 * backend is reclaimed.  Only run when a deferred path
                 * will actually stash these bytes (state_sync,
                 * stmt_replay, deferred BEGIN, or full cleanup) — the
                 * normal dispatch loop classifies them otherwise. */
                bool will_stash_follow =
                    needs_state_sync ||
                    (sf->stmt_replay_hash != 0 && flow->get_stmt_replay) ||
                    (sf->begin_deferred && sf->begin_deferred_payload_len > 0) ||
                    (session->backend_conn &&
                     session->backend_conn->needs_full_cleanup);
                if (will_stash_follow) {
                    /* Snapshot replay first (see comment above) so prefetch
                     * mutations don't strand the stmt-replay branch with an
                     * empty buffer.  Mirror the exact precondition the
                     * stmt-replay branch uses so we don't snapshot when it
                     * won't consume (avoid heap leak on the rare path where
                     * act.be_payload is NULL). */
                    if (sf->stmt_replay_hash != 0 && flow->get_stmt_replay &&
                        act.be_payload && act.be_payload_len > 0) {
                        (void)flow->get_stmt_replay(sf->ctx,
                                                    &prefetched_replay_buf,
                                                    &prefetched_replay_len,
                                                    &prefetched_replay_count,
                                                    &prefetched_replay_hash);
                    }
                    keel_engine_flow_prefetch_classify_follow_buf(sf, setup_follow_buf,
                                                                  setup_follow_len,
                                                                  (size_t)flen);
                }
            }

            /* Deferred-BEGIN replay (PR #4 — async): a BEGIN was buffered
             * earlier to avoid binding the session to a random shard before
             * the shard key was known.  Now that we have the correct shard
             * backend, send the BEGIN non-blocking and stash the upcoming
             * client query (or full pipelined remainder) for forwarding
             * after the BEGIN's ReadyForQuery arrives.  The BE-side
             * intercept in keel_engine_flow_on_be_data() handles the rest.
             *
             * This is now part of the generic pre-query queue and can compose
             * with state sync, cleanup, and stmt replay on the same borrow. */
            if (sf->begin_deferred && sf->begin_deferred_payload_len > 0) {
                if (needs_state_sync) {
                    queue_begin_after_state_sync = true;
                } else {
                    sf->begin_deferred = false;
                    keel_flow_result_t br =
                        defer_begin_replay(sf, session, setup_follow_buf, setup_follow_len);
                    if (br == KEEL_FLOW_ERROR) {
                        if (prefetched_replay_buf) keel_free(prefetched_replay_buf);
                        return KEEL_FLOW_ERROR;
                    }
                    if (setup_result == KEEL_FLOW_OK)
                        setup_result = br;
                }
            }

            if (act.be_payload && act.be_payload_len > 0 &&
                needs_state_sync) {
                uint8_t sync_buf[4096];
                uint64_t _sync_t0 = keel_instr_begin(WORKER_INSTR(worker),
                                                     KEEL_INSTR_STATE_SYNC);
                ssize_t sync_len = flow->build_state_sync
                    ? flow->build_state_sync(sf->ctx,
                                             session->backend_conn->profile,
                                             session->state_profile,
                                             sync_buf, sizeof(sync_buf))
                    : -1;
                keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_STATE_SYNC,
                               _sync_t0);
                if (sync_len < 0) {
                    if (prefetched_replay_buf) keel_free(prefetched_replay_buf);
                    return KEEL_FLOW_ERROR;
                }
                if (sync_len == 0) {
                    session->backend_conn->current_state_hash = session->state_hash;
                    session->backend_conn->needs_sync = false;
                    if (session->backend_conn->profile && session->state_profile)
                        state_profile_copy(session->backend_conn->profile,
                                           session->state_profile);
                } else {
                    keel_flow_result_t resume = KEEL_FLOW_WAIT_BACKEND;
                    if (act.no_response ||
                        (act.msg_kind == KEEL_MSG_KIND_COPY &&
                         act.be_payload_len > 0 && act.be_payload[0] == 'd') ||
                        (act.msg_kind == KEEL_MSG_KIND_EXTENDED &&
                         !(act.pin_clear & KEEL_FPIN_EXTENDED_PROTO))) {
                        resume = KEEL_FLOW_OK;
                    }
                    /* When the stashed follow-up buffer contains a Sync ('S')
                     * message, the entire Extended Protocol pipeline is being
                     * forwarded after state_sync.  The backend will send a
                     * final Z('I'), so we must wait for it (WAIT_BACKEND),
                     * not re-arm FE recv (FLOW_OK). */
                    if (resume == KEEL_FLOW_OK &&
                        flow->captured_fe_pin_effects &&
                        setup_follow_len > 0) {
                        keel_flow_pin_reason_t _fpu = KEEL_FPIN_NONE;
                        keel_flow_pin_reason_t _fpc = KEEL_FPIN_NONE;
                        flow->captured_fe_pin_effects(sf->ctx,
                                                      setup_follow_buf,
                                                      setup_follow_len,
                                                      &_fpu, &_fpc);
                        if (_fpc & KEEL_FPIN_EXTENDED_PROTO)
                            resume = KEEL_FLOW_WAIT_BACKEND;
                    }
                    keel_flow_result_t sr = defer_state_sync_replay(
                        sf, session, session->backend_conn,
                        sync_buf, (size_t)sync_len,
                        setup_follow_buf, setup_follow_len,
                        resume);
                    if (sr == KEEL_FLOW_ERROR) {
                        if (prefetched_replay_buf) keel_free(prefetched_replay_buf);
                        return KEEL_FLOW_ERROR;
                    }
                    if (setup_result == KEEL_FLOW_OK)
                        setup_result = sr;
                }
            }
            if (queue_begin_after_state_sync &&
                sf->begin_deferred && sf->begin_deferred_payload_len > 0) {
                sf->begin_deferred = false;
                keel_flow_result_t br =
                    defer_begin_replay(sf, session, setup_follow_buf, setup_follow_len);
                if (br == KEEL_FLOW_ERROR) {
                    if (prefetched_replay_buf) keel_free(prefetched_replay_buf);
                    return KEEL_FLOW_ERROR;
                }
                if (setup_result == KEEL_FLOW_OK)
                    setup_result = br;
            }

            /* ----- Query logging (after backend is connected) ----- */
            if (KEEL_TIER_HAS_QUERY_LOG(sf->mode) &&
                act.type == KEEL_FE_ACT_QUERY &&
                act.sql_view && act.sql_view_len > 0) {
                keel_query_log_t* qlog = keel_query_log_get_global();
                if (qlog && qlog->enabled) {
                    keel_str_t sql_str = { .data = act.sql_view,
                                          .len  = act.sql_view_len };
                    keel_proto_query_t qr;
                    memset(&qr, 0, sizeof(qr));
                    keel_sql_analyze(sql_str, &qr);
                    keel_query_log_emit(qlog, session, &qr);
                }
            }

            /* ---- Prepared-statement replay (spec §17) ----
             *
             * If the borrowed backend lacks the session's named prepared
             * statements, send all Parse wire messages to it before forwarding
             * the client's message (which may be a Bind that references them).
             * We wait for ParseComplete responses in on_be_data. */
            if (sf->stmt_replay_hash != 0 && flow->get_stmt_replay &&
                act.be_payload && act.be_payload_len > 0) {
                uint8_t* rbuf   = NULL;
                size_t   rlen   = 0;
                uint32_t rcount = 0;
                uint64_t rhash  = 0;

                int gr;
                if (prefetched_replay_buf && prefetched_replay_len > 0 &&
                    prefetched_replay_count > 0) {
                    /* Consume the snapshot captured before prefetch
                     * mutated the cache (e.g. DDL clear_all).  Ownership
                     * transfers to the local rbuf — clear the snapshot
                     * pointers so the late dispose below is a no-op. */
                    rbuf   = prefetched_replay_buf;
                    rlen   = prefetched_replay_len;
                    rcount = prefetched_replay_count;
                    rhash  = prefetched_replay_hash;
                    prefetched_replay_buf   = NULL;
                    prefetched_replay_len   = 0;
                    prefetched_replay_count = 0;
                    prefetched_replay_hash  = 0;
                    gr = 0;
                } else {
                    gr = flow->get_stmt_replay(sf->ctx, &rbuf, &rlen, &rcount, &rhash);
                }
                if (gr == 0 && rbuf && rlen > 0 && rcount > 0) {
                    /* Replay buffer size guard */
                    if (sf->backend_max_replay_bytes > 0 &&
                        rlen > sf->backend_max_replay_bytes) {
                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                            "Worker %u: session %lu: PS replay buffer too large "
                            "(%zu bytes > limit %zu) — closing",
                            worker ? worker->id : 0u,
                            session ? (unsigned long)session->id : 0ul,
                            rlen, sf->backend_max_replay_bytes);
                        keel_free(rbuf);
                        if (worker && worker->stats_ctx)
                            KEEL_STAT_INC(worker->stats_ctx, errors_proto);
                        return KEEL_FLOW_ERROR;
                    }
                    /* Determine what to send to the backend after replay.
                     *
                     * Normal case (no payload rewrite): capture the whole
                     * remaining FE buffer so pipelined messages (Parse+Sync,
                     * Bind+Execute+Sync …) all reach the backend.
                     *
                     * Rewrite case: the protocol plugin replaced act.be_payload
                     * with a synthetic message (e.g. the txid_current probe
                     * rewrites a bare COMMIT to "SELECT txid_current() AS
                     * _keel_txid; COMMIT;").  Using data+pos would send the
                     * original client bytes — skipping the rewrite — and leave
                     * xid_probe_active stuck at true, causing every subsequent
                     * RowDescription to be silently absorbed ("D without T").
                     * Use act.be_payload in that case so the rewritten message
                     * reaches the backend. */
                    if (act.be_payload != NULL &&
                        act.be_payload != (const uint8_t*)(data + pos)) {
                        /* Rewritten payload — use it verbatim */
                        apply_captured_fe_pin_effects(sf, session, worker,
                                                act.be_payload,
                                                act.be_payload_len);
                    } else {
                        /* Un-rewritten — capture full pipeline */
                        apply_captured_fe_pin_effects(sf, session, worker, data + pos, len - pos);
                    }

                    const uint8_t* follow_buf = NULL;
                    size_t follow_len = 0;
                    if (act.be_payload != NULL &&
                        act.be_payload != (const uint8_t*)(data + pos)) {
                        follow_buf = act.be_payload;
                        follow_len = act.be_payload_len;
                    } else {
                        follow_buf = data + pos;
                        follow_len = len - pos;
                    }

                    /* needs_full_cleanup: backend was borrowed with a different
                     * statement hash. Plugin cleanup clears old state before
                     * this session's replay can proceed. */
                    keel_flow_result_t qr = KEEL_FLOW_WAIT_BACKEND;
                    if (session->backend_conn &&
                        session->backend_conn->needs_full_cleanup) {
                        session->backend_conn->needs_full_cleanup = false;
                        qr = pre_query_enqueue_cleanup(sf, session, "FE replay",
                                                       follow_buf, follow_len);
                        if (qr == KEEL_FLOW_ERROR) {
                            keel_free(rbuf);
                            return qr;
                        }
                    }

                    keel_flow_result_t rr = pre_query_enqueue_stmt_replay(
                        sf, session, rbuf, rlen, rcount, rhash,
                        follow_buf, follow_len);
                    keel_free(rbuf);
                    if (rr == KEEL_FLOW_ERROR)
                        return rr;
                    if (qr == KEEL_FLOW_WAIT_BACKEND || qr == KEEL_FLOW_WAIT_STMT_REPLAY)
                        return qr;
                    return rr;
                }
                if (rbuf) keel_free(rbuf);
                sf->stmt_replay_hash = 0;  /* fallback: send original msg as-is */
            }
            /* Snapshot must be consumed by the branch above; free if any
             * path skipped it (defensive — should not happen in practice). */
            if (prefetched_replay_buf) {
                keel_free(prefetched_replay_buf);
                prefetched_replay_buf = NULL;
            }

            /* needs_full_cleanup for stmt_hash=0 (FE path): backend was
             * reclaimed with incompatible state. Run plugin cleanup, then
             * forward the payload after the cleanup response is drained. */
            if (session->backend_conn &&
                session->backend_conn->needs_full_cleanup &&
                act.be_payload && act.be_payload_len > 0) {
                session->backend_conn->needs_full_cleanup = false;
                KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                    "W%u: FE no-stmt cleanup: fd=%d pos=%zu len=%zu pins=0x%x",
                    worker->id, session->server_fd, pos, len, (unsigned)sf->pins);
                /* Use the rewritten payload (e.g. kPgXidCommitMsg) when the
                 * protocol plugin rewrote the FE message; otherwise fall back
                 * to the raw client bytes as-is. */
                const uint8_t* follow_buf = NULL;
                size_t follow_len = 0;
                if (act.be_payload != NULL &&
                    act.be_payload != (const uint8_t*)(data + pos)) {
                    follow_buf = act.be_payload;
                    follow_len = act.be_payload_len;
                } else {
                    follow_buf = data + pos;
                    follow_len = len - pos;
                }
                return pre_query_enqueue_cleanup(sf, session, "FE no-stmt",
                                                 follow_buf, follow_len);
            }

            if (setup_result != KEEL_FLOW_OK)
                return setup_result;

            /* Forward payload to backend */
            KEEL_DEBUG_LOG("W%u: fwd to BE fd=%d payload=%p len=%zu splice=%d\n",
                        worker->id, session->server_fd, (void*)act.be_payload,
                        act.be_payload_len, act.splice_eligible);

            /* === HOOK: BEFORE_SEND ===
             * Last chance to inspect/abort before payload goes to backend. */
            if (KEEL_TIER_HAS_HOOKS(sf->mode) &&
                act.type == KEEL_FE_ACT_QUERY &&
                act.be_payload && act.be_payload_len > 0) {
                uint64_t _hook_t2 = keel_instr_begin(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN);
                keel_hook_ctx_t hctx;
                engine_fill_hook_ctx(&hctx, session, &act, NULL);
                hctx.hook_point = KEEL_HOOK_BEFORE_SEND;
                keel_hook_registry_t* hooks = worker->hooks;
                if (!keel_hook_fire(hooks, KEEL_HOOK_BEFORE_SEND, &hctx)) {
                    keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN, _hook_t2);
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "W%u: session %lu: query aborted by BEFORE_SEND hook: %s",
                        worker->id, (unsigned long)session->id,
                        hctx.error_msg[0] ? hctx.error_msg : "hook returned false");
                    pos += (size_t)flen;
                    continue;
                }
                engine_apply_hook_ctx(&hctx, &act);
                keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_HOOK_CHAIN, _hook_t2);
            }

            /* Extended protocol send batching: for non-Sync pipelined
             * messages (Parse/Bind/Execute/Describe/Close), defer the
             * individual send() and accumulate contiguous buffer ranges.
             * When Sync arrives, all messages are sent in a single send()
             * call, reducing 4 syscalls/query to 1 (~25μs savings). */
            if (act.msg_kind == KEEL_MSG_KIND_EXTENDED &&
                !(act.pin_clear & KEEL_FPIN_EXTENDED_PROTO) &&
                !jumbo_msg &&
                act.be_payload != NULL &&
                act.be_payload == (const uint8_t*)(data + pos)) {
                if (ext_batch_start == (size_t)-1)
                    ext_batch_start = pos;
                ext_flush_response_count += (uint16_t)act.ext_response_count;
                ext_batch_effect |= act.effect;
                pos += (size_t)flen;
                continue;
            }

            /* Merge batched extended protocol messages into current payload.
             * The batch covers Parse..Execute; current message is usually Sync
             * or Flush.  Result: one contiguous send() for the entire sequence.
             *
             * Flush terminal: unlike Sync, Flush does NOT cause the backend to
             * emit ReadyForQuery.  Set flush_in_flight so the BE handler knows
             * to re-arm FE recv once all expected response groups arrive. */
            if (ext_batch_start != (size_t)-1 &&
                act.be_payload == (const uint8_t*)(data + pos)) {
                act.be_payload = data + ext_batch_start;
                act.be_payload_len = pos + (size_t)flen - ext_batch_start;
                ext_batch_start = (size_t)-1;
                /* Hoist accumulated per-message effects onto the terminal's
                 * act so post-send bookkeeping (sticky-primary stamp,
                 * write-cache invalidation, txn tracking) sees the batch
                 * as a whole.  OR-merge so any flag the terminal itself
                 * may carry (rare) is preserved. */
                act.effect |= ext_batch_effect;
                ext_batch_effect = 0;
                if (pos < len && data[pos] == 'H') { /* Flush terminal */
                    sf->flush_in_flight = true;
                    if (UINT16_MAX - sf->flush_pending_count < ext_flush_response_count)
                        sf->flush_pending_count = UINT16_MAX;
                    else
                        sf->flush_pending_count += ext_flush_response_count;
                    /* Flush terminates a batch: we need BE recv to collect
                     * responses (ParseComplete, RowDescription, …) before
                     * switching back to FE recv.  Clear no_response so the
                     * send path arms backend recv via flush_pending_count. */
                    act.no_response = false;
                } else if (pos < len && data[pos] == 'S') { /* Sync terminal */
                    sf->flush_pending_count = 0;
                }
                ext_flush_response_count = 0;
            }

            /* Split Flush terminal: the preceding extended messages may have
             * arrived in an earlier recv and were already forwarded by the
             * uncommitted-batch flush below.  In that case ext_batch_start is
             * empty in this call, but Flush still causes the backend to emit the
             * pending Parse/Bind/Describe/Execute responses. */
            if (pos < len && data[pos] == 'H' &&
                sf->flush_pending_count > 0 &&
                act.be_payload == (const uint8_t*)(data + pos)) {
                sf->flush_in_flight = true;
                act.no_response = false;
            } else if (pos < len && data[pos] == 'S') {
                sf->flush_pending_count = 0;
            }

            if (act.be_payload && act.be_payload_len > 0) {
#if KEEL_HAVE_SPLICE
                /* Try zero-copy splice if eligible and session has pipe */
                if (act.splice_eligible && session->c2s_pipe != NULL) {
                    /* Cast keel_pipe_t* to keel_splice_pipe_t* - compatible layout */
                    keel_splice_pipe_t splice_pipe = {
                        .pipe_fds = { session->c2s_pipe->read_fd,
                                      session->c2s_pipe->write_fd },
                        .capacity = session->c2s_pipe->capacity,
                        .pending = session->c2s_pipe->pending,
                        .valid = (session->c2s_pipe->read_fd >= 0 &&
                                  session->c2s_pipe->write_fd >= 0)
                    };

                    if (splice_pipe.valid) {
                        keel_transfer_result_t res = keel_splice_from_buffer(
                            act.be_payload, act.be_payload_len,
                            session->server_fd, &splice_pipe,
                            KEEL_TRANSFER_NONBLOCK);

                        if (res.error == KEEL_OK && res.bytes > 0) {
                            worker->stats.bytes_spliced += res.bytes;
                            if (worker->stats_ctx) {
                                KEEL_STAT_ADD(worker->stats_ctx, bytes_spliced, res.bytes);
                                KEEL_STAT_ADD(worker->stats_ctx, bytes_backend_sent, res.bytes);
                            }
                            KEEL_DEBUG_LOG("W%u: BE splice success %zu bytes\n",
                                         worker->id, res.bytes);
                            /* Update session pipe pending count */
                            session->c2s_pipe->pending = splice_pipe.pending;
                            goto be_forward_done;
                        }
                        /* Fall through to regular send on splice failure */
                        KEEL_DEBUG_LOG("W%u: splice failed, falling back to send\n",
                                     worker->id);
                    }
                }
#endif
                /* io_uring linked send optimization: for small payloads where
                 * we know the flow will return KEEL_FLOW_WAIT_BACKEND, skip
                 * the inline send() syscall and let the worker chain
                 * send(BE) → recv(BE) as linked io_uring SQEs.  This
                 * eliminates one kernel entry per query on the hot path.
                 *
                 * Conditions for linked send eligibility:
                 *   1. Payload fits in one TCP segment (no short-send risk)
                 *   2. Not a no-response command (needs recv to chain with)
                 *   3. Not COPY data (needs FE recv, not BE recv)
                 *   4. Not extended protocol (Parse/Bind/Execute batches are
                 *      already pipelined and need exact ordering semantics)
                 *   5. Not a jumbo message (needs FE continuation)
                 */
                {
                    bool want_wait_be = !act.no_response
                        && !(act.msg_kind == KEEL_MSG_KIND_COPY
                             && act.be_payload_len > 0 && act.be_payload[0] == 'd')
                        && act.msg_kind != KEEL_MSG_KIND_EXTENDED
                        && !jumbo_msg;

                    if (want_wait_be && act.be_payload_len <= 65536) {
                        sf->linked_send_buf = act.be_payload;
                        sf->linked_send_len = act.be_payload_len;
                        sf->linked_send_fd  = session->server_fd;
                        sf->linked_send_resume = KEEL_FLOW_WAIT_BACKEND;
                        if (worker->stats_ctx)
                            KEEL_STAT_ADD(worker->stats_ctx, bytes_backend_sent,
                                          act.be_payload_len);
                        goto be_forward_done;
                    }
                }

                /* Regular send fallback — non-blocking with io_uring deferred path */
                uint64_t _be_send_t0 = keel_instr_begin(WORKER_INSTR(worker), KEEL_INSTR_SEND_BACKEND);
                ssize_t s = keel_try_send_nb(session->server_fd, act.be_payload,
                                        act.be_payload_len);
                keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_SEND_BACKEND, _be_send_t0);
                KEEL_DEBUG_LOG("W%u: BE send result=%zd\n", worker->id, s);
                if (s < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: backend send failed: %s",
                                worker->id, strerror(errno));
                    if (worker->stats_ctx)
                        KEEL_STAT_INC(worker->stats_ctx, errors_backend);
                    return KEEL_FLOW_ERROR;
                }
                size_t be_sent = (size_t)s;
                if (worker->stats_ctx)
                    KEEL_STAT_ADD(worker->stats_ctx, bytes_backend_sent, be_sent);

                if (be_sent < act.be_payload_len) {
                    /* Send blocked — defer remainder to io_uring.
                     * Stamp write time eagerly (the send WILL complete). */
                    if (KEEL_TIER_HAS_ROUTING(sf->mode) &&
                        (act.effect & (KEEL_QE_WRITE | KEEL_QE_DDL))) {
                        uint64_t write_ns = engine_now_ns();
                        sf->last_write_ns = write_ns;
                        keel_ssv_consistency_set_write_ts(sf->consistency_atoms,
                                                          write_ns);
                    }
                    if (act.effect & KEEL_QE_UNKNOWN_STATE)
                        keel_ssv_opaque_set_unknown(sf->opaque_atoms);

                    /* Determine resume action */
                    keel_flow_result_t resume;
                    if (act.no_response ||
                        (act.msg_kind == KEEL_MSG_KIND_COPY &&
                         act.be_payload_len > 0 && act.be_payload[0] == 'd') ||
                        (act.msg_kind == KEEL_MSG_KIND_EXTENDED &&
                         !(act.pin_clear & KEEL_FPIN_EXTENDED_PROTO))) {
                        resume = KEEL_FLOW_OK;
                    } else if (jumbo_msg) {
                        sf->fe_fwd_remaining = jumbo_remaining;
                        sf->fe_fwd_wait_be = true;
                        resume = KEEL_FLOW_OK;
                    } else {
                        resume = KEEL_FLOW_WAIT_BACKEND;
                    }
                    /* Save any remaining unprocessed FE data */
                    size_t next_pos = pos + (size_t)flen;
                    if (next_pos < len) {
                        keel_residual_append(&session->client_residual,
                                            data + next_pos, len - next_pos);
                    }
                    return defer_send(sf, session->server_fd,
                                      (const uint8_t*)act.be_payload + be_sent,
                                      act.be_payload_len - be_sent, resume);
                }
            }
be_forward_done:
            /* Record backend send timestamp for latency tracking */
            session->be_send_ns = (uint64_t)keel_stats_now_ns();

            /* Replication tracking: when txn_tracking is on, mark COMMIT
             * in-flight so that if the backend dies before we receive
             * CommandComplete(COMMIT) we know the outcome is uncertain. */
            if (KEEL_TIER_HAS_TXN_TRACK(sf->mode) &&
                sf->txn_tracking && (act.effect & KEEL_QE_ENDS_TX)) {
                keel_session_flow_commit_mark_in_flight(sf);
                KEEL_LOG_TRACE(KEEL_LOG_CAT_CORE, "[COMMIT-IN-FLIGHT] SET msg_type=0x%02x msg_kind=%d ctx=%p",
                        (act.be_payload && act.be_payload_len > 0) ? (unsigned)act.be_payload[0] : 0,
                        (int)act.msg_kind, sf->ctx);
            }

            /* Deferred consistency-token capture marker: if writes occurred
             * inside an explicit BEGIN…COMMIT, remember that COMMIT reached
             * the point where an async token-capture state machine could run.
             * The worker does not perform inline token capture because that
             * legacy path blocked the reactor. */
            if (KEEL_TIER_HAS_LSN_CAPTURE(sf->mode) &&
                sf->txn_had_writes &&
                (act.effect & KEEL_QE_ENDS_TX) &&
                keel_plugin_has_cap(flow, KEEL_PCAP_CONSISTENCY_TOKEN) &&
                session->server_fd >= 0) {
                sf->capture_lsn_pending = true;
                sf->txn_had_writes = false;
            }

            /* Stamp last-write time for sticky-primary routing.
             * This must happen after the payload is successfully sent so
             * that read-after-write queries within the TTL window are
             * routed to the primary. */
            if (act.effect & (KEEL_QE_WRITE | KEEL_QE_DDL)) {
                if (KEEL_TIER_HAS_ROUTING(sf->mode)) {
                    uint64_t write_ns = engine_now_ns();
                    sf->last_write_ns = write_ns;
                    keel_ssv_consistency_set_write_ts(sf->consistency_atoms,
                                                      write_ns);
                }

                /* Cache write invalidation: save a copy of the write query so
                 * that query_complete can parse it to evict affected table entries
                 * from the query cache.  A fresh copy is stored for every write
                 * statement even inside a multi-statement transaction so that
                 * the last write SQL observed is what gets invalidated. */
                if (worker->query_cache && act.sql_view && act.sql_view_len > 0) {
                    sf->cache_inval_pending = true;
                    keel_free(sf->cache_inval_sql);
                    sf->cache_inval_sql = keel_malloc(act.sql_view_len + 1);
                    if (sf->cache_inval_sql) {
                        memcpy(sf->cache_inval_sql, act.sql_view, act.sql_view_len);
                        sf->cache_inval_sql[act.sql_view_len] = '\0';
                    }
                }

                /* Mark that WAL LSN / GTID capture would be useful once the
                 * backend reaches ReadyForQuery.  Actual capture is disabled
                 * until it is implemented as a reactor-owned state machine;
                 * sticky-primary routing above provides the stable safety
                 * boundary in the meantime. */
                if (KEEL_TIER_HAS_LSN_CAPTURE(sf->mode) &&
                    keel_plugin_has_cap(flow, KEEL_PCAP_CONSISTENCY_TOKEN) &&
                    session->server_fd >= 0) {
                    if (!session->in_transaction) {
                        /* Autocommit write: eligible after this statement's
                         * ReadyForQuery('I') once async capture exists. */
                        sf->capture_lsn_pending = true;
                    } else {
                        /* Inside explicit BEGIN…COMMIT: remember that writes
                         * occurred.  capture_lsn_pending is set at COMMIT
                         * (ENDS_TX block above) so a future async capture does
                         * not observe an uncommitted mid-transaction position. */
                        sf->txn_had_writes = true;
                    }
                }
            }

            /* Unknown-state flag: if the protocol classified the command as
             * unmodellable (DO, CALL, etc.), mark the opaque atom so that
             * pool return forces DISCARD ALL on the backend. */
            if (act.effect & KEEL_QE_UNKNOWN_STATE)
                keel_ssv_opaque_set_unknown(sf->opaque_atoms);

            /* No-response commands: COM_STMT_CLOSE, COM_STMT_SEND_LONG_DATA
             * in MySQL don't produce a backend response.  Forward but don't
             * wait — continue processing the buffer or return OK. */
            if (act.no_response) {
                if (jumbo_msg) {
                    sf->fe_fwd_remaining = jumbo_remaining;
                    sf->fe_fwd_wait_be = false;
                    return KEEL_FLOW_OK;
                }
                pos += (size_t)flen;
                continue;
            }

            /* COPY IN mode: CopyData ('d') must NOT wait for backend response.
             * The backend won't respond until it receives CopyDone ('c') or
             * CopyFail ('f'), so waiting here would deadlock.  Instead, keep
             * reading from the client (re-arm FE recv). */
            if (act.msg_kind == KEEL_MSG_KIND_COPY &&
                act.be_payload && act.be_payload_len > 0 &&
                act.be_payload[0] == 'd') {
                if (jumbo_msg) {
                    sf->fe_fwd_remaining = jumbo_remaining;
                    sf->fe_fwd_wait_be = false;
                    return KEEL_FLOW_OK;
                }
                pos += (size_t)flen;
                continue;  /* Process more FE data in this buffer, or return OK */
            }

            /* Extended query protocol pipelining: Parse(P), Bind(B),
             * Describe(D), Execute(E), Close(C) are batched — the backend
             * only responds after Sync(S).  Forward each message but keep
             * processing the buffer; only wait when we see Sync.
             *
             * Without this, each P/B/D/E causes a return to the reactor
             * event loop, and the remaining messages in the TCP segment
             * are lost.  This creates a near-deadlock: the proxy waits
             * for a backend response while the backend waits for Sync
             * that was never forwarded.
             *
             * Sync is identified by pin_clear having FPIN_EXTENDED_PROTO
             * set (Sync clears KEEL_FPIN_EXTENDED_PROTO).  Close may set
             * pin_clear for PREPARED_STMT but should still be pipelined. */
            if (act.msg_kind == KEEL_MSG_KIND_EXTENDED &&
                !(act.pin_clear & KEEL_FPIN_EXTENDED_PROTO)) {
                if (jumbo_msg) {
                    sf->fe_fwd_remaining = jumbo_remaining;
                    sf->fe_fwd_wait_be = false;
                    return KEEL_FLOW_OK;
                }
                pos += (size_t)flen;
                continue;  /* Not Sync — keep forwarding */
            }

            /* Jumbo message: more data still incoming from FE.
             * Re-arm FE recv so continuation handler will forward
             * the rest, then trigger WAIT_BACKEND when complete. */
            if (jumbo_msg) {
                sf->fe_fwd_remaining = jumbo_remaining;
                sf->fe_fwd_wait_be = true;
                return KEEL_FLOW_OK;
            }

            /* Regular query, COPY done/fail, or Sync — wait for backend response.
             *
             * FIX: Save any remaining FE data from coalesced TCP segments.
             * MySQL clients (libmysqlclient, sysbench, etc.) may send
             * multiple commands in one TCP segment (e.g., SET NAMES + the
             * actual query).  We process only the first query here and save
             * the rest.  After the backend responds, the worker will replay
             * the saved data via on_fe_data.  Without this, the remaining
             * commands are lost, causing a deadlock where the proxy waits
             * for FE data that will never arrive and the client waits for
             * a response to the command that was dropped. */
            KEEL_LOG_TRACE(KEEL_LOG_CAT_IO, "[WAIT-BE] msg_type=0x%02x pos=%zu len=%zu effect=0x%x ctx=%p",
                    (unsigned)(pos < len ? data[pos] : 0), pos, len,
                    (unsigned)act.effect, sf->ctx);
            {
                size_t next_pos = pos + (size_t)flen;
                if (next_pos < len) {
                    keel_residual_append(&session->client_residual,
                                        data + next_pos, len - next_pos);
                }
            }
            /* If linked send was prepared (payload stored, no inline send()
             * called), return LINKED_SEND so worker chains send+recv SQEs. */
            if (sf->linked_send_len > 0)
                return KEEL_FLOW_LINKED_SEND;
            return KEEL_FLOW_WAIT_BACKEND;
        }

        case KEEL_FE_ACT_TERMINATE:
            return KEEL_FLOW_CLOSED;

        case KEEL_FE_ACT_CANCEL_REQUEST: {
            /* Cancel-key forwarding — supports both PostgreSQL and MySQL.
             *
             * PG:    [length(4) | code(4) | pid(4) | secret(4)] = 16 bytes
             *        Synthetic pid encodes (worker_id << 16 | slab_index).
             *
             * MySQL: COM_PROCESS_KILL packet (4-byte hdr + 1 cmd + 4 conn_id)
             *        Synthetic conn_id encodes (worker_id << 16 | slab_index).
             */
            if (!act.be_payload || act.be_payload_len < 9)
                return KEEL_FLOW_CLOSED;
            const uint8_t* cp = act.be_payload;

            uint32_t tgt_wid = 0, tgt_idx = 0;
            bool is_pg = (act.be_payload_len >= 16 &&
                          cp[4]==0x04 && cp[5]==0xD2 && cp[6]==0x16 && cp[7]==0x2E);

            if (is_pg) {
                /* PG CancelRequest: extract synthetic pid/secret (big-endian) */
                uint32_t syn_pid = ((uint32_t)cp[8]<<24) | ((uint32_t)cp[9]<<16)
                                 | ((uint32_t)cp[10]<<8) | cp[11];
                uint32_t syn_sec = ((uint32_t)cp[12]<<24) | ((uint32_t)cp[13]<<16)
                                 | ((uint32_t)cp[14]<<8) | cp[15];
                tgt_wid = syn_pid >> 16;
                tgt_idx = syn_pid & 0xFFFF;

                keel_engine_t* eng = worker->engine;
                uint32_t nw = eng ? eng->num_workers : 0;
                if (tgt_wid < nw) {
                    keel_worker_t* tw = &eng->worker_pool.workers[tgt_wid];
                    if (tw && tgt_idx < tw->sessions.capacity) {
                        keel_session_t* ts = &tw->sessions.sessions[tgt_idx];
                        if (ts->cancel_pid == syn_pid &&
                            ts->cancel_secret == syn_sec) {
                            if (ts->backend_conn &&
                                ts->backend_conn->backend_pid != 0) {
                                uint8_t real_cancel[16];
                                uint32_t rpid = ts->backend_conn->backend_pid;
                                uint32_t rsec = ts->backend_conn->cancel_secret;
                                real_cancel[0]=0; real_cancel[1]=0;
                                real_cancel[2]=0; real_cancel[3]=16;
                                real_cancel[4]=0x04; real_cancel[5]=0xD2;
                                real_cancel[6]=0x16; real_cancel[7]=0x2E;
                                real_cancel[8]=(uint8_t)(rpid>>24);
                                real_cancel[9]=(uint8_t)(rpid>>16);
                                real_cancel[10]=(uint8_t)(rpid>>8);
                                real_cancel[11]=(uint8_t)rpid;
                                real_cancel[12]=(uint8_t)(rsec>>24);
                                real_cancel[13]=(uint8_t)(rsec>>16);
                                real_cancel[14]=(uint8_t)(rsec>>8);
                                real_cancel[15]=(uint8_t)rsec;

                                backend_pool_t* bpool = ts->backend_conn->pool;
                                if (bpool && bpool->addr_resolved) {
                                    cancel_request_async(worker->reactor,
                                                         &bpool->resolved_addr,
                                                         real_cancel, 16);
                                }
                            } else {
                                /* Backend not yet borrowed — store a pending
                                 * cancel flag.  The FE query handler will
                                 * synthesise E(57014) before borrowing. */
                                atomic_store_explicit(&ts->cancel_pending,
                                                      true,
                                                      memory_order_release);
                            }
                        }
                    } else {
                        /* tgt_idx out of bounds or tw == NULL — silently skip */
                    }
                } else {
                    /* tgt_wid >= nw — silently skip */
                }
            } else if (act.be_payload_len >= 9 && cp[4] == 0x0c) {
                /* MySQL COM_PROCESS_KILL: look up the target session's real
                 * MySQL connection ID and delegate to the protocol vtable. */
                uint32_t syn_conn = ((uint32_t)cp[5])
                                  | ((uint32_t)cp[6] << 8)
                                  | ((uint32_t)cp[7] << 16)
                                  | ((uint32_t)cp[8] << 24);
                tgt_wid = syn_conn >> 16;
                tgt_idx = syn_conn & 0xFFFF;

                keel_engine_t* eng = worker->engine;
                uint32_t nw = eng ? eng->num_workers : 0;
                if (tgt_wid < nw) {
                    keel_worker_t* tw = &eng->worker_pool.workers[tgt_wid];
                    if (tw && tgt_idx < tw->sessions.capacity) {
                        keel_session_t* ts = &tw->sessions.sessions[tgt_idx];
                        if (ts->backend_conn &&
                            ts->backend_conn->my_connection_id != 0) {
                            const char* proto = tw->backend_protocol
                                              ? tw->backend_protocol : "mysql";
                            const keel_proto_flow_vtable_t* vt =
                                keel_proto_flow_get(proto);
                            if (vt && vt->cancel_async) {
                                vt->cancel_async(
                                    tw->backend_host,
                                    tw->backend_port,
                                    tw->backend_user,
                                    tw->backend_password,
                                    tw->backend_database,
                                    ts->backend_conn->my_connection_id);
                            }
                        }
                    }
                }
            }
            return KEEL_FLOW_CLOSED;
        }

        case KEEL_FE_ACT_AUTH_REJECT:
            /* Authentication failed: send ErrorResponse to client then close */
            if (act.fe_response && act.fe_response_len > 0) {
                keel_try_send_nb(session->client_fd,
                                 act.fe_response, act.fe_response_len);
            }
            if (worker->audit_log) {
                keel_audit_emit_auth(
                    (keel_audit_log_t*)worker->audit_log,
                    KEEL_AUDIT_AUTH_FAIL,
                    act.client_username && act.client_username[0]
                        ? act.client_username : NULL,
                    act.client_database && act.client_database[0]
                        ? act.client_database : NULL,
                    NULL, 0, "authentication failed");
            }
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, errors_auth);
            return KEEL_FLOW_CLOSED;

        case KEEL_FE_ACT_ERROR:
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, errors_proto);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, proxy_state_desync_total);
            /* Audit: AUTH_FAIL — error during handshake = authentication failure. */
            if (worker->audit_log && sf->phase == KEEL_PHASE_HANDSHAKE_AUTH) {
                keel_audit_emit_auth(
                    (keel_audit_log_t*)worker->audit_log,
                    KEEL_AUDIT_AUTH_FAIL,
                    act.client_username && act.client_username[0] ? act.client_username : NULL,
                    act.client_database && act.client_database[0] ? act.client_database : NULL,
                    NULL, 0, "protocol error during handshake");
            }
            return KEEL_FLOW_ERROR;

        case KEEL_FE_ACT_WAIT_AUTH:
            /* Async auth op dispatched — store the notify fd so the worker
             * can arm a reactor read and resume via keel_engine_flow_resume_auth(). */
            sf->auth_notify_fd = act.auth_notify_fd;
            return KEEL_FLOW_WAIT_AUTH;

        case KEEL_FE_ACT_NONE:
        default:
            break;

        case KEEL_FE_ACT_SEND_FE:
            /* Send a protocol-synthesized response directly to the frontend.
             * The original FE message is NOT forwarded to the backend.
             * Used by Anonymous PS mode to return a synthetic ParseComplete
             * without sending Parse to the backend.
             *
             * If fe_response is set, send it to client; otherwise the
             * message is silently absorbed (e.g. Flush in anonymous mode). */
            if (act.fe_response && act.fe_response_len > 0) {
                ssize_t s = keel_try_send_nb(session->client_fd,
                                             act.fe_response,
                                             act.fe_response_len);
                if (s < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                        "Worker %u: SEND_FE send failed: %s",
                        worker->id, strerror(errno));
                    return KEEL_FLOW_ERROR;
                }
                if ((size_t)s < act.fe_response_len)
                    return defer_send(sf, session->client_fd,
                                      (const uint8_t*)act.fe_response + s,
                                      act.fe_response_len - (size_t)s,
                                      KEEL_FLOW_OK);
            }
            /* Cross-service RYW: if the protocol injected a consistency LSN
             * (from SET keel.read_after_lsn), update the session's consistency
             * atom so subsequent replica reads are gated on the replica reaching
             * at least this position.  The sticky-primary TTL is also set so
             * the enforcement is active immediately. */
            if (act.inject_consistency_lsn[0] != '\0') {
                keel_consistency_token_t tok;
                memset(&tok, 0, sizeof(tok));
                strncpy(tok.value, act.inject_consistency_lsn,
                        sizeof(tok.value) - 1);
                tok.captured_at_ns = engine_now_ns();
                keel_ssv_consistency_set_token(sf->consistency_atoms, &tok);
                sf->last_write_ns = tok.captured_at_ns;
            }
            pos += (size_t)flen;
            continue;
        }

        pos += (size_t)flen;
    }

    /* Flush uncommitted extended-protocol batch.
     *
     * libpq pipeline mode (psycopg3, libpq-based drivers) may send P/B/D/E
     * messages in one TCP segment and the terminating Sync in the next.  When
     * this buffer contains only EXTENDED messages (no Sync/Flush), the
     * while-loop exits with ext_batch_start pointing at unsent data.
     *
     * Without this flush, the batch is silently dropped when on_fe_data
     * returns: the backend never receives the pipelined statements, responds
     * to the later Sync with just ReadyForQuery, and the client deadlocks
     * waiting for ParseComplete / BindComplete responses that never arrive.
     *
     * Fix: forward the accumulated batch now.  The backend queues the
     * messages and does NOT respond until it receives Sync, so we return
     * KEEL_FLOW_OK to re-arm FE recv and collect the Sync in the next call. */
    if (ext_batch_start != (size_t)-1 && session->server_fd >= 0) {
        size_t batch_len = pos - ext_batch_start;
        if (batch_len > 0) {
            ssize_t s = keel_try_send_nb(session->server_fd,
                                         data + ext_batch_start, batch_len);
            if (s < 0) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                    "Worker %u: ext-proto pre-Sync batch flush failed: %s",
                    worker->id, strerror(errno));
                return KEEL_FLOW_ERROR;
            }
            if (worker->stats_ctx)
                KEEL_STAT_ADD(worker->stats_ctx, bytes_backend_sent, (size_t)s);
            if (UINT16_MAX - sf->flush_pending_count < ext_flush_response_count)
                sf->flush_pending_count = UINT16_MAX;
            else
                sf->flush_pending_count += ext_flush_response_count;
            if ((size_t)s < batch_len) {
                return defer_send(sf, session->server_fd,
                                  data + ext_batch_start + (size_t)s,
                                  batch_len - (size_t)s, KEEL_FLOW_OK);
            }
        }
    }

    KEEL_DEBUG_LOG("W%u: fe_data returning OK (pos=%zu len=%zu)\n", worker->id, pos, len);
    return KEEL_FLOW_OK;
}

/* ============================================================================
 * Replication Tracking: Commit-in-Doubt Resolution (spec §TXN-TRACK)
 * ============================================================================ */

static void send_commit_doubt_response(const keel_proto_flow_vtable_t* flow,
                                       void* flow_ctx,
                                       int client_fd,
                                       keel_commit_doubt_reason_t reason,
                                       uint64_t xid)
{
    if (!flow || client_fd < 0)
        return;

    uint8_t out[512];
    ssize_t n = -1;
    if (flow->generate_commit_doubt_response) {
        n = flow->generate_commit_doubt_response(flow_ctx, reason, xid,
                                                 out, sizeof(out));
    } else if (flow->generate_error) {
        const char* code = (reason == KEEL_CIDR_RESOLVED_ABORTED) ? "40000" : "08006";
        const char* msg = (reason == KEEL_CIDR_RESOLVED_ABORTED)
            ? "connection lost before COMMIT confirmation: transaction failed"
            : "connection lost before COMMIT confirmation";
        ssize_t el = flow->generate_error(flow_ctx, code, msg, out, sizeof(out));
        if (el > 0) {
            size_t total = (size_t)el;
            if (flow->generate_ready_for_query) {
                ssize_t zl = flow->generate_ready_for_query(flow_ctx,
                                                            out + total,
                                                            sizeof(out) - total);
                if (zl > 0)
                    total += (size_t)zl;
            }
            n = (ssize_t)total;
        }
    }

    if (n > 0)
        (void)keel_try_send_nb(client_fd, out, (size_t)n);
}

/**
 * @brief Handle a commit-in-doubt event — backend died while COMMIT was in flight.
 *
 * Called from keel_engine_flow_on_be_data() when the backend connection is
 * lost and `sf->commit_in_flight == true`.  The function attempts to
 * determine the transaction outcome and synthesise an appropriate response
 * for the client:
 *
 *  Case 1 — XID not yet captured (`sf->pending_commit_xid == 0`):
 *    The backend died before even the SELECT txid_current() result arrived.
 *    Outcome is completely unknown.  Synthesise ErrorResponse(08006),
 *    send it to the client, return KEEL_FLOW_CLOSED.
 *
 *  Case 2 — token captured but no RW pool:
 *    Ask the protocol plugin to synthesize an operator-facing uncertain result.
 *
 *  Case 3 — token captured, RW pool available:
 *    Borrow a new connection from the first RW server pool.
 *    Build and send a protocol-owned outcome-check query to that connection.
 *    Store the connection in `sf->xid_check_conn`.
 *    Return KEEL_FLOW_WAIT_COMMIT_CHECK — the worker arms recv on the
 *    check connection and routes responses back through on_be_data where
 *    plugin callbacks determine committed/aborted/uncertain outcomes.
 *
 * IMPORTANT: This function sets `session->server_fd = check_conn->fd`
 * and clears `session->backend_conn` so the worker's recv arming targets
 * the check connection, not the dead original backend.
 *
 * @param sf       Session flow state (commit_in_flight == true on entry).
 * @param session  Owning session.
 * @param worker   Owning worker (provides server_pools).
 * @return KEEL_FLOW_WAIT_COMMIT_CHECK or KEEL_FLOW_CLOSED or KEEL_FLOW_ERROR.
 */
void keel_session_flow_commit_mark_in_flight(keel_session_flow_t* sf)
{
    if (!sf) return;
    sf->commit_in_flight = true;
    sf->pending_commit_xid = 0;
}

void keel_session_flow_commit_clear_in_flight(keel_session_flow_t* sf)
{
    if (!sf) return;
    sf->commit_in_flight = false;
}

void keel_session_flow_commit_mark_doubt(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_worker_t* worker)
{
    if (!sf || !session) return;
    sf->commit_in_doubt      = true;
    sf->indoubt_xid          = sf->pending_commit_xid;
    sf->commit_in_flight     = false;
    sf->indoubt_check_result = 0;

    /* Mirror to session so engine drain can avoid force-closing this session */
    session->commit_in_doubt = true;
    session->indoubt_xid     = sf->indoubt_xid;

    if (worker && worker->stats_ctx) {
        KEEL_STAT_INC(worker->stats_ctx, commit_in_doubt_started);
        KEEL_STAT_GAUGE_INC(worker->stats_ctx, sessions_commit_in_doubt);
    }
}

void keel_session_flow_commit_clear_doubt(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_worker_t* worker,
    keel_commit_doubt_resolution_t resolution)
{
    if (!sf || !session) return;
    sf->commit_in_doubt      = false;
    session->commit_in_doubt = false;
    session->indoubt_xid     = 0;

    if (worker && worker->stats_ctx) {
        if (resolution == KEEL_COMMIT_DOUBT_RESOLVED)
            KEEL_STAT_INC(worker->stats_ctx, commit_in_doubt_resolved);
        else
            KEEL_STAT_INC(worker->stats_ctx, commit_in_doubt_failed);
        KEEL_STAT_GAUGE_DEC(worker->stats_ctx, sessions_commit_in_doubt);
    }
}

keel_flow_result_t keel_engine_flow_handle_commit_doubt(
    keel_session_flow_t* sf,
    keel_session_t* session,
    keel_worker_t* worker)
{
    const keel_proto_flow_vtable_t* flow = sf ? sf->flow : NULL;
    keel_session_flow_commit_mark_doubt(sf, session, worker);

    if (sf->indoubt_xid == 0) {
        send_commit_doubt_response(flow, sf->ctx, session->client_fd,
                                   KEEL_CIDR_NO_XID, 0);
        keel_session_flow_commit_clear_doubt(sf, session, worker,
                                             KEEL_COMMIT_DOUBT_FAILED);
        return KEEL_FLOW_CLOSED;
    }

    /* Try to borrow an RW pool connection for the check */
    backend_pool_t* pool = NULL;
    if (worker->server_pool && worker->server_pool->rw_count > 0) {
        size_t rw_idx = worker->server_pool->rw_indices[0];
        if (rw_idx < worker->server_pool_count && worker->server_pools[rw_idx]) {
            pool = worker->server_pools[rw_idx];
        }
    }
    if (!pool) {
        send_commit_doubt_response(flow, sf->ctx, session->client_fd,
                                   KEEL_CIDR_NO_RW_POOL, sf->indoubt_xid);
        keel_session_flow_commit_clear_doubt(sf, session, worker,
                                             KEEL_COMMIT_DOUBT_FAILED);
        return KEEL_FLOW_CLOSED;
    }

    backend_conn_t* check_conn = backend_pool_borrow(pool, 0);
    if (!check_conn) {
        send_commit_doubt_response(flow, sf->ctx, session->client_fd,
                                   KEEL_CIDR_NO_CHECK_CONN, sf->indoubt_xid);
        keel_session_flow_commit_clear_doubt(sf, session, worker,
                                             KEEL_COMMIT_DOUBT_FAILED);
        return KEEL_FLOW_CLOSED;
    }

    uint8_t qbuf[256];
    ssize_t qmsglen = 0;
    if (flow && flow->build_commit_doubt_check) {
        qmsglen = flow->build_commit_doubt_check(sf->ctx, sf->indoubt_xid,
                                                 qbuf, sizeof(qbuf));
    }
    if (qmsglen <= 0) {
        backend_pool_return(pool, check_conn, false);
        send_commit_doubt_response(flow, sf->ctx, session->client_fd,
                                   KEEL_CIDR_CHECK_BUILD_FAIL, sf->indoubt_xid);
        keel_session_flow_commit_clear_doubt(sf, session, worker,
                                             KEEL_COMMIT_DOUBT_FAILED);
        return KEEL_FLOW_CLOSED;
    }

    ssize_t s = keel_try_send_nb(check_conn->fd, qbuf, (size_t)qmsglen);
    if (s < 0 || (size_t)s < (size_t)qmsglen) {
        backend_pool_return(pool, check_conn, false);
        send_commit_doubt_response(flow, sf->ctx, session->client_fd,
                                   KEEL_CIDR_CHECK_SEND_FAIL, sf->indoubt_xid);
        keel_session_flow_commit_clear_doubt(sf, session, worker,
                                             KEEL_COMMIT_DOUBT_FAILED);
        return KEEL_FLOW_CLOSED;
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "W%u: session %lu: commit-in-doubt — checking outcome for token=%llu on fd=%d",
        worker->id, (unsigned long)session->id,
        (unsigned long long)sf->indoubt_xid, check_conn->fd);

    sf->xid_check_conn  = check_conn;
    session->server_fd  = check_conn->fd;
    session->backend_conn = NULL;  /* already cleared by worker before calling us */
    session->backend_generation = 0;
    return KEEL_FLOW_WAIT_COMMIT_CHECK;
}

/* ============================================================================
 * Backend Data Handler
 * ============================================================================ */

/**
 * @brief Process a chunk of bytes received from the backend database socket.
 *
 * Called by the worker after every io_uring recv CQE on the backend fd.
 * Handles several distinct states:
 *
 *  **commit_in_doubt** — If `sf->commit_in_doubt` is set, the session is
 *    waiting for protocol-owned outcome-check messages on a borrowed
 *    check connection. On completion the check connection is returned
 *    to the pool.
 *
 *  **WAIT_STMT_REPLAY** — When performing prepared-statement replay after
 *    pool-wait resume, responses to Parse messages are counted.  Each
 *    `ParseComplete ('1')` decrements the outstanding count.  Error ('E')
 *    aborts the replay.  Once count reaches zero, the originally deferred
 *    client message is forwarded to the backend.  If `needs_discard` is
 *    set, a DISCARD ALL response is drained first.
 *
 *  **Normal path** — For all other states, the function loops over
 *    complete protocol messages, calling `flow->on_be_msg()` for each,
 *    and dispatches the resulting `keel_be_action_t`:
 *
 *    - `FORWARD_FE`    → forward bytes to client socket
 *    - `ABSORB`        → silently drop (XID probe messages)
 *    - `QUERY_COMPLETE`→ update transaction state, return backend to pool
 *      or keep for next query in the same transaction
 *
 *  **Fast-forward path**: When `session->fast_forward_mode` is enabled and
 *    `flow->is_data_frame()` returns true (e.g. pure DataRow packets),
 *    bytes are forwarded to the client without calling `on_be_msg()`.
 *    This is the L4-speed hot path for large result sets.
 *
 *  **Jumbo backend messages**: If a message's declared length exceeds the
 *    64KB recv buffer, the available bytes are forwarded immediately and
 *    `sf->be_fwd_remaining` is set for continuation in the next call.
 *
 *  **query_complete handling**: When the protocol signals
 *    `act.query_complete`, the engine:
 *    - Updates `session->in_transaction` from tx status.
 *    - Applies `commit_in_flight` → `pending_commit_xid` from `act.commit_xid`.
 *    - Clears `capture_lsn_pending`; inline token capture is intentionally
 *      disabled on the reactor thread.
 *    - Returns backend to pool if no active pins, otherwise keeps it.
 *    - Re-arms frontend recv.
 *
 * @param sf      Session flow state.
 * @param session Owning session.
 * @param data    Buffer received from backend socket.
 * @param len     Number of bytes in buffer (0 = connection closed by backend).
 * @return KEEL_FLOW_OK            — re-arm backend recv
 *         KEEL_FLOW_WAIT_BACKEND  — forward complete; waiting for next be recv
 *         KEEL_FLOW_SEND_PENDING  — deferred partial send to client
 *         KEEL_FLOW_CLOSED        — session should be closed
 *         KEEL_FLOW_ERROR         — unrecoverable error
 */
keel_flow_result_t keel_engine_flow_on_be_data(
    keel_session_flow_t* sf,
    keel_session_t* session,
    const uint8_t* data,
    size_t len) {

    const keel_proto_flow_vtable_t* flow = sf->flow;
    if (!flow) return KEEL_FLOW_ERROR;

    keel_worker_t* worker = session->worker;
    KEEL_INSTR_SCOPE(WORKER_INSTR(worker), KEEL_INSTR_FLOW_BE_DATA);
    bool query_complete = false;
    bool backend_reusable = false;

    /* Clear linked FE response state from any previous call */
    sf->linked_send_len = 0;

    /* ------------------------------------------------------------------ *
     * Query result cache capture: accumulate raw backend bytes while a   *
     * cacheable SELECT is in flight.  Accumulation happens at the very   *
     * top so we collect data regardless of which code path handles it.   *
     * The capture is undone for any trailing partial frame that gets      *
     * stashed in server_residual (see bottom of function).               *
     * ------------------------------------------------------------------ */
#define KEEL_CACHE_MAX_CAPTURE_BYTES (16u * 1024u * 1024u)  /* 16 MiB per result */
    if (sf->cache_pending && len > 0) {
        if (sf->cache_capture_len + len > KEEL_CACHE_MAX_CAPTURE_BYTES) {
            /* Result too large to cache — abort capture */
            sf->cache_pending = false;
            keel_free(sf->cache_capture_buf);
            sf->cache_capture_buf = NULL;
            sf->cache_capture_cap = 0;
            sf->cache_capture_len = 0;
        } else {
            size_t needed = sf->cache_capture_len + len;
            if (needed > sf->cache_capture_cap) {
                size_t new_cap = sf->cache_capture_cap ? sf->cache_capture_cap * 2 : 8192u;
                while (new_cap < needed) new_cap *= 2;
                uint8_t* nb = keel_realloc(sf->cache_capture_buf, new_cap);
                if (nb) {
                    sf->cache_capture_buf = nb;
                    sf->cache_capture_cap = new_cap;
                } else {
                    /* OOM — abort capture silently */
                    sf->cache_pending = false;
                    keel_free(sf->cache_capture_buf);
                    sf->cache_capture_buf = NULL;
                    sf->cache_capture_cap = 0;
                    sf->cache_capture_len = 0;
                }
            }
            if (sf->cache_pending) {
                memcpy(sf->cache_capture_buf + sf->cache_capture_len, data, len);
                sf->cache_capture_len += len;
            }
        }
    }

    /* ------------------------------------------------------------------ *
     * Replication tracking: commit_in_doubt response handler.            *
     * After keel_engine_flow_handle_commit_doubt() borrows a check conn  *
     * and sends a protocol-owned outcome-check query, responses flow      *
     * through here and are interpreted by plugin callbacks.              *
     * ------------------------------------------------------------------ */
    if (sf->commit_in_doubt) {
        if (!flow->frame_len || !flow->on_be_msg)
            return KEEL_FLOW_ERROR;
        size_t cid_pos = 0;
        while (cid_pos < len) {
            ssize_t fl = flow->frame_len(sf->ctx, data + cid_pos, len - cid_pos, 1);
            if (fl < 0)
                return KEEL_FLOW_ERROR;
            if (fl == 0 || (size_t)fl > (len - cid_pos))
                break;

            keel_be_action_t act;
            if (flow->on_be_msg(sf->ctx, data + cid_pos, (size_t)fl, &act) < 0)
                return KEEL_FLOW_ERROR;

            if (act.commit_doubt_outcome_changed) {
                sf->indoubt_check_result = act.commit_doubt_outcome;
            }

            if (act.query_complete) {
                if (sf->xid_check_conn) {
                    backend_pool_return(sf->xid_check_conn->pool,
                                        sf->xid_check_conn, false);
                    sf->xid_check_conn = NULL;
                    session->server_fd = -1;
                }
                keel_session_flow_commit_clear_doubt(
                    sf, session, worker,
                    (sf->indoubt_check_result == 1 || sf->indoubt_check_result == 2)
                        ? KEEL_COMMIT_DOUBT_RESOLVED
                        : KEEL_COMMIT_DOUBT_FAILED);

                if (sf->indoubt_check_result == 1) {
                    send_commit_doubt_response(flow, sf->ctx, session->client_fd,
                                               KEEL_CIDR_RESOLVED_COMMITTED,
                                               sf->indoubt_xid);
                } else if (sf->indoubt_check_result == 2) {
                    send_commit_doubt_response(flow, sf->ctx, session->client_fd,
                                               KEEL_CIDR_RESOLVED_ABORTED,
                                               sf->indoubt_xid);
                } else {
                    send_commit_doubt_response(flow, sf->ctx, session->client_fd,
                                               KEEL_CIDR_RESOLVED_UNKNOWN,
                                               sf->indoubt_xid);
                }
                return KEEL_FLOW_CLOSED;
            }
            cid_pos += (size_t)fl;
        }
        return KEEL_FLOW_WAIT_BACKEND;  /* wait for the rest of the response */
    }

    /* ------------------------------------------------------------------ *
     * Async pre-query replay.  Deferred BEGIN and state sync both have to *
     * finish, including their ReadyForQuery, before the client's payload  *
     * can be forwarded.  The backend response bytes are absorbed here so  *
     * CommandComplete/ReadyForQuery frames from setup work never leak into *
     * the client query response stream.                                  *
     * ------------------------------------------------------------------ */
    if (sf->pending_pre_query == KEEL_PRE_QUERY_BEGIN_REPLAY ||
        sf->pending_pre_query == KEEL_PRE_QUERY_STATE_SYNC) {
        bool is_state_sync = (sf->pending_pre_query == KEEL_PRE_QUERY_STATE_SYNC);
        keel_tx_status_t expected_tx =
            is_state_sync ? KEEL_TX_IDLE : KEEL_TX_ACTIVE;
        if (len == 0) {
            /* recv()==0 on backend → connection lost mid-replay. */
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONN,
                "W%u: backend disconnect during %s",
                worker->id, is_state_sync ? "state sync" : "deferred BEGIN replay");
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, pre_query_be_disconnect);
            pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_DRAIN_ERROR);
            pre_query_fail_clear(sf);
            return KEEL_FLOW_ERROR;
        }

        /* Runaway guard: a misbehaving / wedged backend that never sends 'Z'
         * could pin this session indefinitely.  Cap absorption at 64 KiB. */
        enum { KEEL_PRE_QUERY_RUNAWAY_MAX = 64u * 1024u };
        sf->pending_pre_query_absorbed += len;
        if (sf->pending_pre_query_absorbed > KEEL_PRE_QUERY_RUNAWAY_MAX) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "W%u: %s absorbed %zu B without "
                "ReadyForQuery; aborting session",
                worker->id,
                is_state_sync ? "state sync" : "deferred BEGIN replay",
                sf->pending_pre_query_absorbed);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, pre_query_runaway);
            pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_TIMEOUT);
            pre_query_fail_clear(sf);
            return KEEL_FLOW_ERROR;
        }

        if (!flow->frame_len || !flow->on_be_msg) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "W%u: protocol handler missing frame_len/on_be_msg during %s",
                worker->id, is_state_sync ? "state sync" : "deferred begin");
            pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_PARSE_ERROR);
            pre_query_fail_clear(sf);
            return KEEL_FLOW_ERROR;
        }

        size_t pp = 0;
        bool saw_complete = false;
        bool saw_tx_state = false;
        keel_tx_status_t observed_tx = KEEL_TX_IDLE;
        while (pp < len) {
            ssize_t fl = flow->frame_len(sf->ctx, data + pp, len - pp, 1);
            if (fl < 0) {
                if (worker->stats_ctx)
                    KEEL_STAT_INC(worker->stats_ctx, pre_query_proto_violation);
                pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_PARSE_ERROR);
                pre_query_fail_clear(sf);
                return KEEL_FLOW_ERROR;
            }
            if (fl == 0 || (size_t)fl > (len - pp))
                break;

            keel_be_action_t act;
            if (flow->on_be_msg(sf->ctx, data + pp, (size_t)fl, &act) < 0) {
                if (worker->stats_ctx)
                    KEEL_STAT_INC(worker->stats_ctx, pre_query_proto_violation);
                pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_PARSE_ERROR);
                pre_query_fail_clear(sf);
                return KEEL_FLOW_ERROR;
            }

            /* A backend ErrorResponse during state sync or deferred BEGIN means
             * the operation failed (e.g. a SET command was rejected).  The sync
             * hash must NOT be stamped; close the backend immediately. */
            if (act.is_error_response) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                    "W%u: ErrorResponse from backend during %s; aborting session",
                    worker->id, is_state_sync ? "state sync" : "deferred BEGIN");
                if (worker->stats_ctx)
                    KEEL_STAT_INC(worker->stats_ctx, pre_query_proto_violation);
                pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_DRAIN_ERROR);
                pre_query_fail_clear(sf);
                return KEEL_FLOW_ERROR;
            }

            if (act.tx_state_changed) {
                saw_tx_state = true;
                observed_tx = act.tx_status;
                sf->tx = act.tx_status;
                session->in_transaction = (act.tx_status != KEEL_TX_IDLE);
                if (session->backend_conn)
                    session->backend_conn->in_transaction = session->in_transaction;
            }

            if (act.query_complete) {
                saw_complete = true;
                pp += (size_t)fl;
                break;
            }

            pp += (size_t)fl;
        }

        if (!saw_complete) {
            if (pp < len)
                keel_residual_append(&session->server_residual,
                                     data + pp, len - pp);
            /* Still draining BEGIN response — wait for more BE data. */
            return KEEL_FLOW_WAIT_BACKEND;
        }

        if (!saw_tx_state || observed_tx != expected_tx) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "W%u: %s completed with unexpected tx state (seen=%d expected=%d)",
                worker->id,
                is_state_sync ? "state sync" : "deferred begin",
                (int)observed_tx, (int)expected_tx);
            pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_DRAIN_ERROR);
            pre_query_fail_clear(sf);
            return KEEL_FLOW_ERROR;
        }

        if (is_state_sync) {
            if (session->backend_conn) {
                session->backend_conn->current_state_hash =
                    sf->pending_state_sync_hash;
                session->backend_conn->needs_sync = false;
                if (session->backend_conn->profile && session->state_profile)
                    state_profile_copy(session->backend_conn->profile,
                                       session->state_profile);
            }
        }

        /* 'Z' received — complete this setup op. If additional pre-query ops
         * are queued, start the next one first; held FE bytes are forwarded
         * only after the entire queue has drained successfully. */
        size_t forward_len = sf->pending_pre_query_len;
        keel_flow_result_t forward_resume = sf->pending_pre_query_resume;
        pre_query_reset_active(sf);

        if (worker->stats_ctx)
            KEEL_STAT_INC(worker->stats_ctx, pre_query_replay_count);

        if (sf->pre_query_count > 0) {
            if (pp < len)
                keel_residual_append(&session->server_residual,
                                     data + pp, len - pp);
            return pre_query_start_next_queued(sf, session);
        }

        if (forward_len > 0) {
            ssize_t fs = keel_try_send_nb(session->server_fd,
                                          sf->pending_pre_query_buf,
                                          forward_len);
            if (fs < 0) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                    "W%u: post-%s payload send failed fd=%d len=%zu: %s",
                    worker->id,
                    is_state_sync ? "state-sync" : "BEGIN",
                    session->server_fd, forward_len,
                    strerror(errno));
                if (worker->stats_ctx) {
                    KEEL_STAT_INC(worker->stats_ctx, pre_query_send_fail);
                    KEEL_STAT_INC(worker->stats_ctx, errors_backend);
                }
                pre_query_record_result(sf, worker,
                                        KEEL_REPLAY_RESULT_PARTIAL_SEND_FAILURE);
                sf->pending_pre_query_len = 0;
                return KEEL_FLOW_ERROR;
            }
            if (worker->stats_ctx)
                KEEL_STAT_ADD(worker->stats_ctx, bytes_backend_sent,
                              (uint64_t)fs);
            if ((size_t)fs < forward_len) {
                /* Partial send — io_uring path will complete the rest, then
                 * resume into KEEL_FLOW_WAIT_BACKEND so we can pick up the
                 * real query response. */
                keel_flow_result_t dr = defer_send(sf, session->server_fd,
                                                   sf->pending_pre_query_buf + fs,
                                                   forward_len - (size_t)fs,
                                                   forward_resume);
                if (dr == KEEL_FLOW_ERROR) {
                    pre_query_record_result(sf, worker,
                                            KEEL_REPLAY_RESULT_PARTIAL_SEND_FAILURE);
                }
                return dr;
            }
        }
        sf->pending_pre_query_len = 0;
        pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_SUCCESS);

        /* Any trailing bytes after 'Z' are part of the real query stream;
         * re-enter this function on that slice rather than duplicating the
         * downstream handling here. */
        if (pp < len)
            return keel_engine_flow_on_be_data(sf, session,
                                               data + pp, len - pp);
        return forward_resume;
    }

    /* ------------------------------------------------------------------ *
     * Setup cleanup drain (prepared-state replay borrow path):           *
     * Core owns sequencing only. The protocol plugin parses/validates the *
     * cleanup response stream and tells us when the backend is reusable.  *
     * ------------------------------------------------------------------ */
    if (sf->stmt_replay_needs_cleanup) {
        if (!flow->drain_cleanup_response) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "W%u: setup cleanup drain requested without protocol handler fd=%d",
                worker->id, session->server_fd);
            return KEEL_FLOW_ERROR;
        }

        size_t consumed = 0;
        keel_proto_drain_result_t gate = flow->drain_cleanup_response(
            sf->ctx, &sf->stmt_cleanup_drain_state, data, len, &consumed);
        if (gate == KEEL_PROTO_DRAIN_ERROR || consumed > len) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "W%u: setup cleanup drain failed fd=%d consumed=%zu len=%zu",
                worker->id, session->server_fd, consumed, len);
            pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_DRAIN_ERROR);
            return KEEL_FLOW_ERROR;
        }
        if (gate == KEEL_PROTO_DRAIN_MORE)
            return KEEL_FLOW_WAIT_STMT_REPLAY;

        sf->stmt_replay_needs_cleanup = false;
        memset(&sf->stmt_cleanup_drain_state, 0,
               sizeof(sf->stmt_cleanup_drain_state));
        data += consumed;
        len  -= consumed;

        if (sf->pre_query_count > 0) {
            if (len > 0)
                keel_residual_append(&session->server_residual, data, len);
            return pre_query_start_next_queued(sf, session);
        }

        if (sf->pending_pre_query_len > 0 && sf->stmt_replay_count == 0 &&
            !sf->stmt_replay_rfq_pending) {
            size_t forward_len = sf->pending_pre_query_len;
            keel_flow_result_t forward_resume = sf->pending_pre_query_resume;
            ssize_t rs = keel_try_send_nb(session->server_fd,
                                          sf->pending_pre_query_buf,
                                          forward_len);
            sf->pending_pre_query_len = 0;
            if (rs < 0)
            {
                pre_query_record_result(sf, worker,
                                        KEEL_REPLAY_RESULT_PARTIAL_SEND_FAILURE);
                return KEEL_FLOW_ERROR;
            }
            if ((size_t)rs < forward_len) {
                keel_flow_result_t dr = defer_send(sf, session->server_fd,
                                                   sf->pending_pre_query_buf + rs,
                                                   forward_len - (size_t)rs,
                                                   forward_resume);
                if (dr == KEEL_FLOW_ERROR) {
                    pre_query_record_result(sf, worker,
                                            KEEL_REPLAY_RESULT_PARTIAL_SEND_FAILURE);
                }
                return dr;
            }
            pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_SUCCESS);
            if (len > 0)
                return keel_engine_flow_on_be_data(sf, session, data, len);
            return forward_resume;
        }

        if (len == 0)
            return KEEL_FLOW_WAIT_STMT_REPLAY;
    }

    /* ------------------------------------------------------------------ *
     * Prepared-statement replay (spec §17):
     * Core drains replay responses using protocol frame parsing and
     * semantic backend actions only (no protocol tag checks in core).
     * ------------------------------------------------------------------ */
    if (sf->stmt_replay_count > 0 || sf->stmt_replay_rfq_pending) {
        if (len == 0) {
            /* Backend closed socket during stmt replay — clear all pre-query state
             * before returning, consistent with all other error paths. */
            pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_DRAIN_ERROR);
            pre_query_fail_clear(sf);
            return KEEL_FLOW_ERROR;
        }
        if (!flow->frame_len || !flow->on_be_msg) {
            pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_PARSE_ERROR);
            return KEEL_FLOW_ERROR;
        }

        size_t scan_pos = 0;
        while (scan_pos < len) {
            ssize_t fl = flow->frame_len(sf->ctx, data + scan_pos, len - scan_pos, 1);
            if (fl < 0) {
                pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_PARSE_ERROR);
                return KEEL_FLOW_ERROR;
            }
            if (fl == 0 || (size_t)fl > (len - scan_pos))
                break;

            keel_be_action_t act;
            if (flow->on_be_msg(sf->ctx, data + scan_pos, (size_t)fl, &act) < 0) {
                pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_PARSE_ERROR);
                return KEEL_FLOW_ERROR;
            }

                if (act.is_error_response) {
                if (act.fe_payload && act.fe_payload_len > 0 && session->client_fd >= 0)
                    keel_try_send_nb(session->client_fd, act.fe_payload, act.fe_payload_len);
                if (session->backend_conn)
                {
                    /* Replay failed mid-stream.  The backend's prepared
                     * statement state is now ambiguous: a "duplicate stmt"
                     * error means the offending Parse left a stmt resident,
                     * while a syntax error means no stmt was created.  We
                     * cannot reliably tell from here, so quarantine the
                     * backend and force it to be destroyed by close_session
                     * rather than risk pooling a connection that still
                     * carries leftover "S_*" definitions. */
                    session->backend_conn->stmt_set_hash = 0;
                    memset(&session->backend_conn->stmt_profile, 0,
                           sizeof(session->backend_conn->stmt_profile));
                    session->backend_conn->protocol_desync = true;
                }
                sf->stmt_replay_count       = 0;
                sf->stmt_replay_rfq_pending = false;
                sf->stmt_replay_orig_msg    = NULL;
                sf->stmt_replay_orig_len    = 0;
                sf->stmt_replay_hash        = 0;
                sf->pending_pre_query_len   = 0;
                pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_PARSE_ERROR);
                return KEEL_FLOW_ERROR;
            }

            if (act.tx_state_changed) {
                sf->tx = act.tx_status;
                session->in_transaction = (act.tx_status != KEEL_TX_IDLE);
                if (session->backend_conn)
                    session->backend_conn->in_transaction = session->in_transaction;
            }

            if (sf->stmt_replay_count > 0 && act.stmt_replay_accepted) {
                sf->stmt_replay_count--;
                if (sf->stmt_replay_count == 0)
                    sf->stmt_replay_rfq_pending = true;
            }

            if (sf->stmt_replay_rfq_pending && act.query_complete)
                sf->stmt_replay_rfq_pending = false;

            scan_pos += (size_t)fl;
        }

        if (sf->stmt_replay_count == 0 && !sf->stmt_replay_rfq_pending) {
            /* All ParseComplete responses received AND replay RFQ drained.
             * Stamp the backend with the session's stmt_set_hash. */
            if (session->backend_conn && sf->stmt_replay_hash) {
                KEEL_DIAG_PS("REPLAY_DONE sess=%lu be_fd=%d new_be_hash=0x%016llx",
                    (unsigned long)session->id,
                    session->backend_conn->fd,
                    (unsigned long long)sf->stmt_replay_hash);
                session->backend_conn->stmt_set_hash = sf->stmt_replay_hash;
                session->backend_conn->stmt_profile = sf->stmt_replay_profile;
                session->backend_conn->stmt_profile.stmt_set_hash = sf->stmt_replay_hash;
            }
            sf->stmt_replay_hash = 0;

            if (sf->pre_query_count > 0) {
                if (scan_pos < len)
                    keel_residual_append(&session->server_residual,
                                         data + scan_pos, len - scan_pos);
                return pre_query_start_next_queued(sf, session);
            }

            /* Forward held client payload to backend */
            if (sf->pending_pre_query_len > 0 ||
                (sf->stmt_replay_orig_msg && sf->stmt_replay_orig_len > 0)) {
                const uint8_t* orig = sf->pending_pre_query_len > 0
                    ? sf->pending_pre_query_buf
                    : sf->stmt_replay_orig_msg;
                size_t orig_len = sf->pending_pre_query_len > 0
                    ? sf->pending_pre_query_len
                    : sf->stmt_replay_orig_len;
                keel_flow_result_t forward_resume = sf->pending_pre_query_resume;
                sf->pending_pre_query_len = 0;
                sf->stmt_replay_orig_msg = NULL;
                sf->stmt_replay_orig_len = 0;

#if KEEL_ENGINE_FLOW_DEBUG
                /* Log what we're about to send: extract Parse stmt name if it's a Parse msg */
                {
                    char sname[64] = {0};
                    if (orig_len >= 6 && orig[0] == 'P') {
                        /* Parse('P') + len(4) + stmt_name(NUL-terminated) */
                        size_t ns = 0;
                        while (ns < orig_len - 5 - 1 && ns < sizeof(sname)-1 && orig[5+ns]) {
                            sname[ns] = (char)orig[5+ns];
                            ns++;
                        }
                    }
                    KEEL_DEBUG_LOG("W%u: sending orig_msg after replay: fd=%d orig_len=%zu"
                        " orig[0]=0x%02x stmt_name='%s' be_stmt_hash=0x%016llx\n",
                        worker->id, session->server_fd, orig_len,
                        orig_len>0?orig[0]:0, sname,
                        session->backend_conn
                            ? (unsigned long long)session->backend_conn->stmt_set_hash : 0ULL);
                }
#endif

                ssize_t s = keel_try_send_nb(session->server_fd, orig, orig_len);
                if (s < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                        "W%u: orig msg send after replay failed: %s",
                        worker->id, strerror(errno));
                    pre_query_record_result(sf, worker,
                                            KEEL_REPLAY_RESULT_PARTIAL_SEND_FAILURE);
                    return KEEL_FLOW_ERROR;
                }
                if ((size_t)s < orig_len) {
                    keel_flow_result_t dr = defer_send(sf, session->server_fd,
                                                       orig + s, orig_len - (size_t)s,
                                                       forward_resume);
                    if (dr == KEEL_FLOW_ERROR) {
                        pre_query_record_result(sf, worker,
                                                KEEL_REPLAY_RESULT_PARTIAL_SEND_FAILURE);
                    }
                    return dr;
                }
            }
            pre_query_record_result(sf, worker, KEEL_REPLAY_RESULT_SUCCESS);
            if (scan_pos < len)
                return keel_engine_flow_on_be_data(sf, session,
                                                   data + scan_pos, len - scan_pos);
            return KEEL_FLOW_WAIT_BACKEND;
        }
        /* Still waiting for more ParseComplete responses or the replay RFQ */
        if (scan_pos < len)
            keel_residual_append(&session->server_residual,
                                 data + scan_pos, len - scan_pos);
        return KEEL_FLOW_WAIT_STMT_REPLAY;
    }

    /* ------------------------------------------------------------------ *
     * Large-message continuation: when a previous recv delivered only a   *
     * partial backend message, be_fwd_remaining holds the byte count     *
     * still needed.  Forward as much of this buffer as possible to the   *
     * client without trying to parse it (it's the middle of a message).  *
     * ------------------------------------------------------------------ */
    size_t pos = 0;
    if (sf->be_fwd_remaining > 0) {
        KEEL_LOG_TRACE(KEEL_LOG_CAT_IO, "[CONT] be_fwd_remaining=%zu chunk-of-len=%zu ctx=%p",
                sf->be_fwd_remaining, len, sf->ctx);
        size_t chunk = (len < sf->be_fwd_remaining) ? len : sf->be_fwd_remaining;
        ssize_t s = keel_try_send_nb(session->client_fd, data, chunk);
        if (s < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "Worker %u: FE send (BE continuation) failed: %s",
                worker->id, strerror(errno));
            return KEEL_FLOW_ERROR;
        }
        size_t actual = (size_t)s;
        worker->stats.bytes_sent += (uint64_t)actual;
        if (worker->stats_ctx)
            KEEL_STAT_ADD(worker->stats_ctx, bytes_sent, actual);

        /* Commit the full chunk (actual sent now + deferred later) */
        sf->be_fwd_remaining -= chunk;

        if (actual < chunk) {
            /* Partial — save any remaining unprocessed BE data to residual */
            if (chunk < len) {
                keel_residual_append(&session->server_residual,
                                    data + chunk, len - chunk);
            }
            return defer_send(sf, session->client_fd,
                              data + actual, chunk - actual,
                              KEEL_FLOW_WAIT_BACKEND);
        }
        pos = chunk;
        if (sf->be_fwd_remaining > 0)
            return KEEL_FLOW_WAIT_BACKEND; /* Still more continuation coming */
        if (pos >= len)
            return KEEL_FLOW_WAIT_BACKEND; /* Continuation done, need more BE data */
    }

    /* Process all complete messages in the buffer.
     *
     * BATCH SEND OPTIMIZATION: For the common case where all backend messages
     * are forwarded verbatim to the client (fe_payload points into the original
     * data buffer), we defer sends and flush the contiguous range at the end.
     * For a typical point_select response (T+D+C+Z ≈ 100 bytes in one recv),
     * this reduces 4 send() syscalls to 1. */
    size_t batch_unsent_start = pos;  /* start of unsent contiguous range */
    bool   batch_active = false;      /* whether we have deferred sends */

    while (pos < len) {
        ssize_t flen = flow->frame_len(sf->ctx, data+pos, len-pos, 1);
        if (flen < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "Worker %u: backend framing error: pos=%zu len=%zu first_bytes=%02x%02x%02x%02x%02x "
                "session=%lu pins=0x%x be_fd=%d be_fwd_rem=%zu",
                worker->id, pos, len,
                (unsigned)(pos < len ? data[pos] : 0),
                (unsigned)(pos+1 < len ? data[pos+1] : 0),
                (unsigned)(pos+2 < len ? data[pos+2] : 0),
                (unsigned)(pos+3 < len ? data[pos+3] : 0),
                (unsigned)(pos+4 < len ? data[pos+4] : 0),
                (unsigned long)session->id, sf->pins,
                session->server_fd, sf->be_fwd_remaining);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, errors_proto);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, proxy_state_desync_total);
            return KEEL_FLOW_ERROR;
        }
        if (flen == 0) break; /* need more header data */

        /* Jumbo backend message: declared length exceeds available data.
         * Forward the available portion directly to FE without parsing,
         * record remaining byte count for next recv continuation.
         *
         * IMPORTANT: flush any batched messages first!  The batch may
         * contain preceding messages (e.g. RowDescription + earlier
         * DataRows) that the client must see before this jumbo frame.
         * Without this flush, the client would see a partial DataRow
         * without a preceding RowDescription — a protocol violation
         * that crashes libpq. */
        if ((size_t)flen > len - pos) {
            /* Flush batch: the batch and this jumbo fragment are contiguous
             * in the same buffer, so send from batch_unsent_start to end. */
            size_t send_start = batch_active ? batch_unsent_start : pos;
            size_t avail = len - send_start;
            batch_active = false;
            KEEL_LOG_TRACE(KEEL_LOG_CAT_IO, "[JUMBO-BYPASS] type=0x%02x flen=%zd avail=%zu pos=%zu ctx=%p",
                    (unsigned)(pos < len ? data[pos] : 0), flen, avail, pos, sf->ctx);
            ssize_t s = keel_try_send_nb(session->client_fd, data+send_start, avail);
            if (s < 0) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                    "Worker %u: FE send (BE jumbo) failed: %s",
                    worker->id, strerror(errno));
                return KEEL_FLOW_ERROR;
            }
            size_t actual = (size_t)s;
            worker->stats.bytes_sent += (uint64_t)actual;
            if (worker->stats_ctx)
                KEEL_STAT_ADD(worker->stats_ctx, bytes_sent, actual);
            /* be_fwd_remaining tracks the jumbo message tail, NOT the batch */
            sf->be_fwd_remaining = (size_t)flen - (len - pos);
            if (actual < avail) {
                return defer_send(sf, session->client_fd,
                                  data + send_start + actual, avail - actual,
                                  KEEL_FLOW_WAIT_BACKEND);
            }
            return KEEL_FLOW_WAIT_BACKEND;
        }

        /* ---- Fast-forward path: L4 speed for result-set data frames --------
         * When fast_forward_mode is active (plugin set splice_eligible on a
         * previous frame) and the vtable provides a frame classifier, check
         * whether this frame is a pure data/result-row packet.  If it is,
         * forward the bytes directly to the client — skipping the full
         * on_be_msg call and all state-tracking overhead.  This is the
         * "Core stays dumb" hot path for the 99% DATA frames in a result set.
         *
         * Safety: is_data_frame() must return true ONLY for frames that carry
         * no state-change information (no tx update, no query_complete).
         * Terminal frames (ReadyForQuery, EOF, ERR, CommandComplete) always
         * return false and fall through to the normal on_be_msg path, which
         * detects query_complete and clears fast_forward_mode. */
        size_t ff_peek = (size_t)flen < 9 ? (size_t)flen : 9;
        if (session->fast_forward_mode && flow->is_data_frame &&
                flow->is_data_frame(sf->ctx, data + pos, ff_peek)) {
            KEEL_LOG_TRACE(KEEL_LOG_CAT_IO, "[FF-BYPASS] type=0x%02x flen=%zd ctx=%p",
                    (unsigned)(pos < len ? data[pos] : 0), flen, sf->ctx);
            /* Fast-forward data frames participate in batch send too */
            if (!batch_active) {
                batch_unsent_start = pos;
                batch_active = true;
            }
            worker->stats.bytes_sent += (uint64_t)flen;
            if (worker->stats_ctx)
                KEEL_STAT_ADD(worker->stats_ctx, bytes_sent, flen);
            pos += (size_t)flen;
            continue;
        }

        keel_be_action_t act;
        uint64_t _be_t0 = keel_instr_begin(WORKER_INSTR(worker), KEEL_INSTR_PROTO_BE_MSG);
        int rc = flow->on_be_msg(sf->ctx, data+pos, (size_t)flen, &act);
        keel_instr_end(WORKER_INSTR(worker), KEEL_INSTR_PROTO_BE_MSG, _be_t0);
        if (rc < 0) {
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, errors_proto);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, proxy_state_desync_total);
            return KEEL_FLOW_ERROR;
        }

        /* ---- Plugin error classification (Phase 5) ----
         * When the protocol signals a backend error, ask the plugin to
         * classify it.  This lets the engine decide: close the slot,
         * retry, or just forward the error to the client.
         */
        if (act.type == KEEL_BE_ACT_ERROR) {
            keel_error_info_t einfo;
            if (keel_plugin_classify_error(flow, sf->ctx, data+pos,
                                          (size_t)flen, &einfo) == 0) {
                if (!einfo.connection_ok) {
                    /* Backend connection is dead — mark unusable */
                    if (session->backend_conn) {
                        session->backend_conn->protocol_desync = true;
                        backend_pool_set_quarantine(session->backend_conn,
                                                    BACKEND_QUARANTINE_PROTOCOL_DESYNC);
                    }
                    if (worker->stats_ctx)
                        KEEL_STAT_INC(worker->stats_ctx, errors_backend);
                    KEEL_DEBUG_LOG("W%u: classify_error → FATAL (%s)\n",
                                  worker->id,
                                  einfo.sqlstate ? einfo.sqlstate : "?");
                }
                /* Connection-fatal errors are operator-actionable → WARN.
                 * Pure client-side query errors (syntax/permission/constraint
                 * — SQLSTATE class 22/23/42) are user mistakes routinely
                 * forwarded to the client; logging them at WARN floods the
                 * log on every typo. Demote those to INFO. */
                const char* ss = einfo.sqlstate;
                bool client_side_error = ss && (
                    strncmp(ss, "22", 2) == 0 ||  /* data exception */
                    strncmp(ss, "23", 2) == 0 ||  /* integrity constraint */
                    strncmp(ss, "42", 2) == 0);   /* syntax / access rule */
                if (einfo.connection_ok && client_side_error) {
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CONN,
                        "W%u: backend error class=%d sqlstate=%s msg=%s",
                        worker->id,
                        (int)einfo.error_class,
                        ss ? ss : "?",
                        einfo.message ? einfo.message : "?");
                } else {
                    KEEL_LOG_WARN(KEEL_LOG_CAT_CONN,
                        "W%u: backend error class=%d sqlstate=%s msg=%s",
                        worker->id,
                        (int)einfo.error_class,
                        ss ? ss : "?",
                        einfo.message ? einfo.message : "?");
                }
            }
            /* Error response — do not cache this result */
            if (sf->cache_pending) {
                sf->cache_pending = false;
                keel_free(sf->cache_capture_buf);
                sf->cache_capture_buf = NULL;
                sf->cache_capture_cap = 0;
                sf->cache_capture_len = 0;
            }
        }

        /* Update pins */
        keel_session_flow_apply_pin_change(sf, session, worker,
                                           act.pin_update, act.pin_clear);
        if (act.pin_clear & KEEL_FPIN_COPY) {
            sf->copy_skip = 0;      /* Reset COPY scanner state */
            sf->copy_hdr_len = 0;
        }

        /* Update tx state */
        if (act.tx_state_changed) {
            sf->tx = act.tx_status;
            session->in_transaction = (act.tx_status != KEEL_TX_IDLE);
            if (session->backend_conn) {
                session->backend_conn->in_transaction = session->in_transaction;
            }
        }

        /* Update state profile from ParameterStatus (SSV CONFIG domain).
         * The protocol adapter extracts key-value pairs from backend
         * ParameterStatus messages.  We accumulate them into the session's
         * state profile so the borrow algorithm can prefer backends with
         * matching GUC configuration, avoiding DISCARD ALL + SET replay. */
        if (act.has_profile_update && act.profile_key && act.profile_key_len > 0) {
            if (!session->state_profile) {
                session->state_profile = keel_calloc(1, sizeof(state_profile_t));
                if (session->state_profile)
                    state_profile_init(session->state_profile);
            }
            if (session->state_profile) {
                char key_buf[STATE_PROFILE_KEY_MAX];
                char val_buf[STATE_PROFILE_VALUE_MAX];
                size_t klen = act.profile_key_len < sizeof(key_buf) - 1
                    ? act.profile_key_len : sizeof(key_buf) - 1;
                size_t vlen = act.profile_value_len < sizeof(val_buf) - 1
                    ? act.profile_value_len : sizeof(val_buf) - 1;
                memcpy(key_buf, act.profile_key, klen);
                key_buf[klen] = '\0';
                memcpy(val_buf, act.profile_value, vlen);
                val_buf[vlen] = '\0';

                state_profile_set(session->state_profile, key_buf, val_buf);
                session->state_hash = session->state_profile->hash;
                keel_ssv_config_set_profile_hash(sf->config_atoms,
                                                 session->state_profile->hash);
            }
        }

        /* Forward to frontend — both normal responses AND errors.
         * Errors (ERR packets) must be forwarded so the client can handle
         * them properly (e.g., retry, fall back, or report).
         *
         * BATCH SEND: If the payload points into the original data buffer
         * (raw passthrough), defer the send and track the contiguous range.
         * Flush happens at the end of the loop or on non-contiguous payload. */
        if ((act.type == KEEL_BE_ACT_FORWARD_FE || act.type == KEEL_BE_ACT_ERROR) &&
            act.fe_payload && act.fe_payload_len > 0) {
            /* Check if this is a raw passthrough (payload is contiguous
             * in the original data buffer at the current position) */
            if (act.fe_payload == (const uint8_t*)(data + pos) &&
                act.fe_payload_len == (size_t)flen) {
                /* Contiguous raw passthrough — defer send for batching */
                if (!batch_active) {
                    batch_unsent_start = pos;
                    batch_active = true;
                }
                /* Stats: count bytes but don't send yet */
                worker->stats.bytes_sent += (uint64_t)act.fe_payload_len;
                if (worker->stats_ctx)
                    KEEL_STAT_ADD(worker->stats_ctx, bytes_sent, act.fe_payload_len);
                goto fe_forward_done;
            }

            /* Non-contiguous or modified payload — flush batch first */
            if (batch_active) {
                size_t batch_len = pos - batch_unsent_start;
                ssize_t bs = keel_try_send_nb(session->client_fd,
                                              data + batch_unsent_start,
                                              batch_len);
                if (bs < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                        "Worker %u: FE batch send failed: %s",
                        worker->id, strerror(errno));
                    return KEEL_FLOW_ERROR;
                }
                if ((size_t)bs < batch_len) {
                    /* Partial batch send — save remaining data */
                    size_t next_pos = pos + (size_t)flen;
                    if (next_pos < len) {
                        keel_residual_append(&session->server_residual,
                                            data + next_pos, len - next_pos);
                    }
                    /* Also need to send the current non-contiguous payload
                     * after the batch remainder, so save it too */
                    keel_residual_append(&session->server_residual,
                                        act.fe_payload, act.fe_payload_len);
                    return defer_send(sf, session->client_fd,
                                      data + batch_unsent_start + (size_t)bs,
                                      batch_len - (size_t)bs,
                                      KEEL_FLOW_WAIT_BACKEND);
                }
                batch_active = false;
            }

            /* Now send the non-contiguous payload individually */
#if KEEL_HAVE_SPLICE
            /* Use the plugin's splice eligibility decision (set in on_be_msg)
             * rather than a hardcoded size threshold.  The plugin has full
             * protocol context: it knows whether this frame is a "boring"
             * DataRow chain that is safe to splice vs. a control message
             * like ReadyForQuery that must stay in userspace. */
            bool splice_eligible = act.splice_eligible;
            if (splice_eligible) {
                /* Mark session in fast-forward mode.  Future frames within
                 * this result set will also be splice-eligible.  Cleared
                 * when the plugin signals query_complete below.
                 * The per-frame bypass in worker_splice_bypass_loop uses
                 * flow->is_data_frame() from the protocol vtable to decide
                 * per-frame eligibility without calling on_be_msg. */
                session->fast_forward_mode = 1;
            }
            if (splice_eligible && session->s2c_pipe != NULL) {
                /* Cast keel_pipe_t* to keel_splice_pipe_t* - compatible layout */
                keel_splice_pipe_t splice_pipe = {
                    .pipe_fds = { session->s2c_pipe->read_fd,
                                  session->s2c_pipe->write_fd },
                    .capacity = session->s2c_pipe->capacity,
                    .pending = session->s2c_pipe->pending,
                    .valid = (session->s2c_pipe->read_fd >= 0 &&
                              session->s2c_pipe->write_fd >= 0)
                };

                if (splice_pipe.valid) {
                    keel_transfer_result_t res = keel_splice_from_buffer(
                        act.fe_payload, act.fe_payload_len,
                        session->client_fd, &splice_pipe,
                        KEEL_TRANSFER_NONBLOCK);

                    if (res.error == KEEL_OK && res.bytes > 0) {
                        worker->stats.bytes_spliced += res.bytes;
                        worker->stats.bytes_sent += res.bytes;
                        if (worker->stats_ctx) {
                            KEEL_STAT_ADD(worker->stats_ctx, bytes_spliced, res.bytes);
                            KEEL_STAT_ADD(worker->stats_ctx, bytes_sent, res.bytes);
                        }
                        /* Update session pipe pending count */
                        session->s2c_pipe->pending = splice_pipe.pending;
                        goto fe_forward_done;
                    }
                    /* Fall through to regular send on splice failure */
                }
            }
#endif
            if (act.fe_payload && act.fe_payload_len >= 1 &&
                (act.fe_payload[0] == 'T' || act.fe_payload[0] == 'D')) {
                KEEL_LOG_TRACE(KEEL_LOG_CAT_IO, "[FWD-TO-CLIENT] type='%c' len=%zu ctx=%p",
                        (char)act.fe_payload[0], act.fe_payload_len, sf->ctx);
            }
            ssize_t s = keel_try_send_nb(session->client_fd, act.fe_payload,
                                    act.fe_payload_len);
            if (s < 0) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: FE send failed: %s",
                            worker->id, strerror(errno));
                return KEEL_FLOW_ERROR;
            }
            size_t fe_sent = (size_t)s;
            worker->stats.bytes_sent += (uint64_t)fe_sent;
            if (worker->stats_ctx)
                KEEL_STAT_ADD(worker->stats_ctx, bytes_sent, fe_sent);

            if (fe_sent < act.fe_payload_len) {
                /* Partial send — save remaining unprocessed BE data and defer */
                size_t next_pos = pos + (size_t)flen;
                if (next_pos < len) {
                    keel_residual_append(&session->server_residual,
                                        data + next_pos, len - next_pos);
                }
                return defer_send(sf, session->client_fd,
                                  (const uint8_t*)act.fe_payload + fe_sent,
                                  act.fe_payload_len - fe_sent,
                                  KEEL_FLOW_WAIT_BACKEND);
            }
fe_forward_done: ;
        } else if (act.type == KEEL_BE_ACT_ABSORB && batch_active) {
            /* Flush the pending batch NOW, before pos advances past the
             * absorbed message.  If we skip this flush, the end-of-loop
             * batch send will compute batch_len = pos_after_absorb -
             * batch_unsent_start, which spans the absorbed bytes too —
             * silently forwarding them to the client and defeating the
             * absorb intent (e.g. the cancel ReadyForQuery reaches the
             * client socket buffer even though cancel_rfq_suppress was set). */
            size_t batch_len = pos - batch_unsent_start;
            if (batch_len > 0) {
                ssize_t bs = keel_try_send_nb(session->client_fd,
                                              data + batch_unsent_start,
                                              batch_len);
                if (bs < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                        "Worker %u: FE batch flush (pre-absorb) failed: %s",
                        worker->id, strerror(errno));
                    return KEEL_FLOW_ERROR;
                }
                if ((size_t)bs < batch_len) {
                    /* Partial send (extremely unlikely on localhost).
                     * Defer batch remainder; skip absorbed msg; save tail. */
                    size_t after_abs = pos + (size_t)flen;
                    if (after_abs < len)
                        keel_residual_append(&session->server_residual,
                                            data + after_abs, len - after_abs);
                    return defer_send(sf, session->client_fd,
                                      data + batch_unsent_start + (size_t)bs,
                                      batch_len - (size_t)bs,
                                      KEEL_FLOW_WAIT_BACKEND);
                }
            }
            batch_active = false;
        }

        if (act.query_complete) query_complete = true;
        if (act.backend_reusable) backend_reusable = true;

        /* Flush in-flight: update expected response counter.
         * When a Flush-terminated batch was sent (not Sync), the backend does
         * not emit ReadyForQuery.  Track each control-message terminal so we
         * know when all expected responses have been forwarded to the client.
         *
         * Terminals that decrement the count:
         *   '1' ParseComplete, '2' BindComplete, 'C' CommandComplete,
         *   '3' CloseComplete, 'T' RowDescription, 'n' NoData.
         * ErrorResponse ('E') clears the count entirely: the backend enters
         *   discard-until-Sync mode and will send no further responses.
         * ParameterDescription ('t') and DataRow ('D') are NOT terminals. */
        if (sf->flush_in_flight) {
            uint8_t fmsg = (pos < len) ? data[pos] : 0;
            if (fmsg == 'E') {
                sf->flush_pending_count = 0; /* backend discards until Sync */
            } else if (sf->flush_pending_count > 0 &&
                       (fmsg == '1' || fmsg == '2' || fmsg == 'C' ||
                        fmsg == '3' || fmsg == 'T' || fmsg == 'n')) {
                sf->flush_pending_count--;
            }
        }

        /* Replication tracking: harvest XID captured by the protocol plugin.
         * commit_xid_captured is set by pgf_on_be_msg when it absorbs the
         * DataRow from the "SELECT txid_current()" query prepended to COMMIT. */
        if (act.commit_xid_captured)
            sf->pending_commit_xid = act.commit_xid;

        /* Clear the in-flight flag once the COMMIT response has fully arrived
         * (ReadyForQuery 'I' signals the backend is idle again). */
        if (act.backend_reusable && sf->commit_in_flight)
            keel_session_flow_commit_clear_in_flight(sf);

        /* Copy mode transition */
        if (act.enters_copy_mode) {
            /* Flush any batched sends before switching to copy mode */
            pos += (size_t)flen;
            if (batch_active) {
                size_t batch_len = pos - batch_unsent_start;
                ssize_t bs = keel_try_send_nb(session->client_fd,
                                              data + batch_unsent_start,
                                              batch_len);
                if (bs < 0) return KEEL_FLOW_ERROR;
                if ((size_t)bs < batch_len) {
                    if (pos < len)
                        keel_residual_append(&session->server_residual,
                                            data + pos, len - pos);
                    return defer_send(sf, session->client_fd,
                                      data + batch_unsent_start + (size_t)bs,
                                      batch_len - (size_t)bs,
                                      KEEL_FLOW_OK);
                }
                batch_active = false;
            }
            /* After sending CopyIn response to FE, switch to FE recv
             * so we can read COPY data from client */
            sf->copy_skip = 0;      /* Reset scanner state for new COPY stream */
            sf->copy_hdr_len = 0;
            /* Forward remaining backend data first */
            if (pos < len) {
                ssize_t s = keel_try_send_nb(session->client_fd, data+pos, len-pos);
                if (s > 0) {
                    worker->stats.bytes_sent += (uint64_t)s;
                    if (worker->stats_ctx)
                        KEEL_STAT_ADD(worker->stats_ctx, bytes_sent, s);
                }
                if (s >= 0 && (size_t)s < len - pos) {
                    /* Partial — defer remainder, then re-arm FE recv */
                    return defer_send(sf, session->client_fd,
                                      data + pos + (size_t)s,
                                      (len - pos) - (size_t)s,
                                      KEEL_FLOW_OK);
                }
            }
            return KEEL_FLOW_OK; /* Re-arm FE recv for COPY data */
        }

        pos += (size_t)flen;
    }

    /* Flush any remaining batched sends.  For the common point_select case,
     * this is the ONLY send() for the entire response (T+D+C+Z). */
    if (batch_active) {
        size_t batch_len = pos - batch_unsent_start;
        ssize_t bs = keel_try_send_nb(session->client_fd,
                                      data + batch_unsent_start,
                                      batch_len);
        if (bs < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "Worker %u: FE batch flush failed: %s",
                worker->id, strerror(errno));
            return KEEL_FLOW_ERROR;
        }
        if ((size_t)bs < batch_len) {
            /* Partial batch flush — save trailing data and defer */
            if (pos < len) {
                keel_residual_append(&session->server_residual,
                                    data + pos, len - pos);
            }
            return defer_send(sf, session->client_fd,
                              data + batch_unsent_start + (size_t)bs,
                              batch_len - (size_t)bs,
                              KEEL_FLOW_WAIT_BACKEND);
        }
        batch_active = false;
    }

    /* Save any un-framed trailing data (partial message header) for the next
     * backend recv.  We must NOT forward partial messages to the client —
     * the proxy would lose track of the BE stream position and desync on
     * the next recv when the continuation arrives without a valid tag byte. */
    if (pos < len) {
        keel_residual_append(&session->server_residual, data + pos, len - pos);
        /* Undo the over-accumulation of partial frame bytes that were saved
         * to server_residual.  They will be re-accumulated on the next call
         * as part of the combined buffer (residual + new recv data). */
        if (sf->cache_pending) {
            sf->cache_capture_len -= (len - pos);
        }
    }

    if (query_complete) {
        sf->flush_in_flight = false; /* safety: clear on normal query completion */
        sf->flush_pending_count = 0;
        /* Emit trace span event marking query completion */
        KEEL_TRACE_EVENT(session, "query.complete");

        /* Commit captured response to the result cache if capture succeeded */
        if (sf->cache_pending && worker->query_cache &&
            sf->cache_capture_buf && sf->cache_capture_len > 0) {
            keel_query_cache_put(worker->query_cache,
                                 sf->cache_digest,
                                 sf->cache_capture_buf,
                                 sf->cache_capture_len,
                                 0 /* use cache default TTL */);
        }
        if (sf->cache_capture_buf) {
            keel_free(sf->cache_capture_buf);
            sf->cache_capture_buf = NULL;
            sf->cache_capture_cap = 0;
            sf->cache_capture_len = 0;
        }
        sf->cache_pending = false;

        /* Cache write invalidation: parse the saved write SQL to identify
         * affected tables and evict them from the query cache.  This ensures
         * that stale SELECT results are not served after a write/DDL commits. */
        if (sf->cache_inval_pending && worker->query_cache && sf->cache_inval_sql) {
            keel_arena_t* _inv_arena = keel_arena_create(4096);
            if (_inv_arena) {
                keel_str_t _inv_sql = { .data = sf->cache_inval_sql,
                                        .len  = strlen(sf->cache_inval_sql) };
                const keel_qt_query_t* _inv_qt =
                    keel_sql_analyze_full(_inv_sql, _inv_arena);
                if (_inv_qt) {
                    keel_qt_table_ref_t* tbls[16];
                    size_t n = keel_qt_get_invalidated_tables(_inv_qt, tbls, 16);
                    for (size_t ti = 0; ti < n; ti++) {
                        char tname[256];
                        size_t tlen = tbls[ti]->table.len < 255
                                    ? tbls[ti]->table.len : 255;
                        memcpy(tname, tbls[ti]->table.data, tlen);
                        tname[tlen] = '\0';
                        keel_query_cache_invalidate_table(worker->query_cache, tname);
                    }
                }
                keel_arena_destroy(_inv_arena);
            }
            keel_free(sf->cache_inval_sql);
            sf->cache_inval_sql    = NULL;
            sf->cache_inval_pending = false;
        }

        /* Record query and backend latency */
        if (worker->stats_ctx) {
            uint64_t now_ns = (uint64_t)keel_stats_now_ns();
            if (session->query_start_ns) {
                KEEL_STAT_LATENCY(worker->stats_ctx, query_latency_ns,
                                  now_ns - session->query_start_ns);
                session->query_start_ns = 0;
            }
            if (session->be_send_ns) {
                KEEL_STAT_LATENCY(worker->stats_ctx, backend_latency_ns,
                                  now_ns - session->be_send_ns);
                session->be_send_ns = 0;
            }
        }

        /* Clear fast-forward mode: plugin has signalled end of result set.
         * Core returns to L7 mode for the next query. */
        session->fast_forward_mode = 0;

        if (sf->capture_lsn_pending) {
            if (capture_consistency_token_after_write(sf, session, flow) != 0) {
                if (session->backend_conn) {
                    session->backend_conn->protocol_desync = true;
                    if (session->backend_conn->pool) {
                        backend_pool_discard(session->backend_conn->pool,
                                             session->backend_conn,
                                             BACKEND_CLOSE_REASON_PROTOCOL_ERROR);
                    } else if (session->server_fd >= 0) {
                        close(session->server_fd);
                    }
                    session->backend_conn = NULL;
                    session->backend_generation = 0;
                    session->server_fd = -1;
                } else if (session->server_fd >= 0) {
                    close(session->server_fd);
                    session->server_fd = -1;
                }
                sf->capture_lsn_pending = false;
                return KEEL_FLOW_ERROR;
            }
            sf->capture_lsn_pending = false;
        }

        /*
         * FIX: Use protocol's authoritative backend_reuse_gate() instead of
         * trusting the per-message backend_reusable flag. The gate function
         * has full context about transaction state, copy mode, etc.
         */
        bool gate_ok = false;
        if (flow->backend_reuse_gate) {
            gate_ok = flow->backend_reuse_gate(sf->ctx);
        } else {
            /* Fallback to per-message flag if vtable method not implemented */
            gate_ok = backend_reusable;
        }

        /* KEEL_FPIN_PREPARED_STMT alone does not block backend release (spec §17):
         * the backend is kept on the idle list with its stmt_set_hash so a
         * matching session can reuse it without replay, or a new session will
         * replay the missing Parse messages.  All other pins must be clear.
         *
         * Exception: KEEL_PS_MODE_OFF — no stmt tracking means no replay is
         * possible.  Treat PREPARED_STMT as a hard pin so the backend is never
         * returned between transactions (session-mode semantics for PS users).
         * This avoids DISCARD ALL + re-prepare round-trips entirely; the
         * backend connection is held for the client session lifetime. */
        bool can_release = gate_ok &&
                           keel_ssv_allows_backend_release(sf->pins, sf->ps_mode) &&
                           !session->in_transaction;

        /* Quarantine resolution: if the only remaining pins after removing
         * QUARANTINE are SSV-virtualizable (or none), decide whether to
         * promote quarantine to a hard pin or clear it.
         *
         * The quarantine flag was set optimistically by classify_sql() for
         * statements that *might* create server-side state (PREPARE, CREATE
         * TEMP TABLE, LISTEN, etc.).  Now that the backend has responded, we
         * know whether it succeeded.  If the backend reported an error (the
         * protocol plugin clears query_complete on error), the state was
         * never created so we can safely release.  If it succeeded, we
         * must keep the session pinned to this backend.
         *
         * Note: PREPARE sets both QUARANTINE and PREPARED_STMT simultaneously.
         * Since PREPARED_STMT is SSV-virtualizable, we must allow quarantine
         * resolution when PREPARED_STMT is the only other pin — otherwise
         * QUARANTINE is never cleared and the backend stays hard-pinned,
         * preventing stmt replay (and thus semantic GUC re-application). */
        if (!can_release && gate_ok && !session->in_transaction &&
            (sf->pins & KEEL_FPIN_QUARANTINE) &&
            keel_ssv_allows_backend_release(
                sf->pins & ~(keel_flow_pin_reason_t)KEEL_FPIN_QUARANTINE,
                sf->ps_mode)) {

            if (sf->quarantine_pending) {
                /* Query succeeded — promote quarantine to hard pin.
                 * The session must stay on this backend because server-side
                 * state (temp table, prepared stmt, cursor, advisory lock)
                 * now exists. */
                keel_session_flow_apply_pin_change(sf, session, worker,
                                                   KEEL_FPIN_NONE,
                                                   KEEL_FPIN_QUARANTINE);
                if (session->backend_conn)
                    session->backend_conn->hard_pinned = true;
                session->hard_pinned = true;
                sf->quarantine_pending = 0;
                if (worker->stats_ctx)
                    KEEL_STAT_INC(worker->stats_ctx, quarantine_count);
                KEEL_DEBUG_LOG("W%u: quarantine → hard-pin (session %lu)\n",
                              worker->id, (unsigned long)session->id);
            } else {
                /* No pending quarantine evidence — clear it */
                keel_session_flow_apply_pin_change(sf, session, worker,
                                                   KEEL_FPIN_NONE,
                                                   KEEL_FPIN_QUARANTINE);
                can_release = true;
            }
        }

        if (can_release && session->backend_conn) {
            /* Return backend to pool.
             * The behaviour when returning a backend that carried named prepared
             * statements depends on the session's PS mode:
             *
             *  VIRTUALIZE / TRACKING  — stamp the backend with the session's
             *      stmt_set_hash so the pool keeps it on idle_list and future
             *      sessions with the same statement set can reuse it without
             *      replay (spec §17).
             *
             *  PINNING  — the backend was hard-pinned for the lifetime of the
             *      PS set.  On release we must clean the backend so it doesn't
             *      carry stale named statements.  Set needs_full_cleanup so the
             *      pool sends DEALLOCATE ALL before returning the connection to
             *      the clean list.  Clear stmt_set_hash to 0 so it goes to the
             *      dirty list until the cleanup is confirmed.
             *
             *  ANONYMOUS  — the backend never saw any named statements (Parse
             *      was intercepted and rewritten), so stmt_set_hash is always 0
             *      and the backend goes straight to clean_list; nothing to do.
             */
            backend_conn_t* be = session->backend_conn;
            if (be->pool) {
                keel_stmt_compat_profile_t stmt_profile;
                memset(&stmt_profile, 0, sizeof(stmt_profile));
                /* Unknown-state rule: if the protocol adapter flagged a
                 * command it could not semantically model, any cached
                 * backend state (SET variables, prepared stmts) may be
                 * stale.  Force DISCARD ALL on the next borrow so the
                 * backend is returned to a clean baseline. */
                if (keel_ssv_opaque_has_unknown(sf->opaque_atoms)) {
                    be->current_state_hash = 0xFFFFFFFFFFFFFFFFULL;
                    be->stmt_set_hash      = 0;
                    memset(&be->stmt_profile, 0, sizeof(be->stmt_profile));
                }

                if ((sf->pins & KEEL_FPIN_PREPARED_STMT) && flow->get_stmt_replay
                    && sf->ps_mode != KEEL_PS_MODE_PINNING
                    && sf->ps_mode != KEEL_PS_MODE_ANONYMOUS) {
                    bool have_stmt_profile = false;
                    if (flow->get_stmt_compat_profile &&
                        flow->get_stmt_compat_profile(sf->ctx, &stmt_profile) == 0) {
                        have_stmt_profile = true;
                    }
                    flow->get_stmt_replay(sf->ctx, NULL, NULL, NULL,
                                          &stmt_profile.stmt_set_hash);
                    if (!have_stmt_profile && stmt_profile.stmt_set_hash != 0)
                        stmt_profile.semantic_unknown = true;

                    if (stmt_profile.semantic_unknown) {
                        be->stmt_set_hash = 0;
                        memset(&be->stmt_profile, 0, sizeof(be->stmt_profile));
                        be->current_state_hash = 0xFFFFFFFFFFFFFFFFULL;
                    } else if (be->stmt_set_hash != 0 &&
                               !backend_pool_stmt_compatible(&stmt_profile, be)) {
                        be->stmt_set_hash = 0;
                        memset(&be->stmt_profile, 0, sizeof(be->stmt_profile));
                        be->current_state_hash = 0xFFFFFFFFFFFFFFFFULL;
                    } else {
                        be->stmt_set_hash = stmt_profile.stmt_set_hash;
                        be->stmt_profile = stmt_profile;
                        KEEL_DIAG_PS("RELEASE sess=%lu be_fd=%d stamp_hash=0x%016llx",
                            (unsigned long)session->id, be->fd,
                            (unsigned long long)stmt_profile.stmt_set_hash);
                    }
                } else if (sf->ps_mode == KEEL_PS_MODE_PINNING
                           && (sf->pins & KEEL_FPIN_PREPARED_STMT)) {
                    /* PINNING: force cleanup — backend carried real PS state.
                     * Zero out stmt_set_hash and set current_state_hash to a
                     * non-zero sentinel so backend_pool_return routes to the
                     * CLEANING path (DISCARD ALL), clearing all named
                     * prepared statements from the backend. */
                    be->stmt_set_hash       = 0;
                    memset(&be->stmt_profile, 0, sizeof(be->stmt_profile));
                    be->current_state_hash  = 0xFFFFFFFFFFFFFFFFULL;  /* sentinel → DISCARD ALL */
                } else {
                    /* No prepared stmts pin from this session (or ANONYMOUS).
                     *
                     * If the backend still carries a non-zero stmt_set_hash,
                     * those statements were prepared by a *previous* session
                     * whose ownership we did not inherit (e.g. this session
                     * borrowed the backend for a non-PS command before any
                     * Parse).  Clearing the hash without cleanup would lie
                     * to the next borrower: a fresh session would receive
                     * a backend that still has "S_1" defined, and its first
                     * Parse("S_1", ...) would fail with "prepared statement
                     * already exists".  Force a DISCARD ALL via the cleanup
                     * sentinel so the backend returns to the clean list with
                     * no leftover named statements. */
                    if (be->stmt_set_hash != 0) {
                        be->current_state_hash = 0xFFFFFFFFFFFFFFFFULL;
                    }
                    /* The session may have just executed a DDL or DISCARD
                     * PLANS via named Parse that left orphan PS on the
                     * backend; the protocol adapter signals this via the
                     * stmt_compat_profile's semantic_unknown flag (one-shot,
                     * consumed by this call). */
                    if (flow->get_stmt_compat_profile) {
                        keel_stmt_compat_profile_t orphan_check;
                        memset(&orphan_check, 0, sizeof(orphan_check));
                        if (flow->get_stmt_compat_profile(sf->ctx, &orphan_check) == 0 &&
                            orphan_check.semantic_unknown) {
                            be->current_state_hash = 0xFFFFFFFFFFFFFFFFULL;
                        }
                    }
                    be->stmt_set_hash = 0;
                    memset(&be->stmt_profile, 0, sizeof(be->stmt_profile));
                }
                backend_pool_return(be->pool, be, false);
                if (worker->stats_ctx)
                    KEEL_STAT_INC(worker->stats_ctx, pool_returns);
            }
            session->backend_conn = NULL;
            session->backend_generation = 0;
            session->server_fd = -1;
        } else if (can_release && session->server_fd >= 0 && !session->backend_conn) {
            /* Non-pooled connection that's reusable — close it since we can't pool */
            close(session->server_fd);
            session->server_fd = -1;
        }
        /* else: backend stays pinned to session (tx, extended proto, etc.) */

        sf->phase = KEEL_PHASE_READY;
        return KEEL_FLOW_OK; /* Re-arm FE recv */
    }

    /* More backend data expected.
     *
     * When fast_forward_mode is active (we're mid-result-set streaming
     * DataRow frames) and the worker has fast_network_path enabled, signal
     * the worker to enter zero-copy splice bypass: it will peek at future
     * backend message headers and splice DataRow frames directly from
     * the backend socket to the client socket through a kernel pipe,
     * without any userspace copy.  Terminal frames (ReadyForQuery, Error,
     * CommandComplete) will cause the worker to exit splice mode and
     * revert to normal recv + engine_flow processing.
     *
     * If result_cache is enabled we must NOT splice because the data
     * needs to be captured in userspace for caching. */
#if KEEL_HAVE_SPLICE
    if (session->fast_forward_mode &&
        session->s2c_pipe != NULL &&
        session->worker->fast_network_path &&
        !session->worker->result_cache) {
        return KEEL_FLOW_SPLICE_BYPASS;
    }
#endif
    /* Flush in-flight complete: all expected non-RFQ response groups received.
     * The backend has gone silent (waiting for client's Bind+Execute+Sync).
     * Re-arm FE recv so KEEL can read the next client message batch. */
    if (sf->flush_in_flight && sf->flush_pending_count == 0) {
        sf->flush_in_flight = false;
        return KEEL_FLOW_OK;
    }
    return KEEL_FLOW_WAIT_BACKEND;
}
