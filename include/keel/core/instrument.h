/**
 * @file instrument.h
 * @brief Public API for function-level timing instrumentation and scoped probes.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Lightweight scoped-timer probes for identifying where CPU time is spent
 * across KEEL hot-path functions. Each probe accumulates call count,
 * total nanoseconds, min and max per invocation.
 *
 * Design:
 * =======
 *   - Per-worker probe arrays (no cross-thread contention on hot path).
 *   - Category bitmask for granular enable/disable via INI `[instrument]`.
 *   - Zero overhead when a category is disabled (single branch, predict-not-taken).
 *   - GCC cleanup attribute for automatic scope-exit recording.
 *   - Reports integrated into stats_dump output.
 *
 * Usage:
 * ======
 *   // Automatic scope timer (fires on all exit paths including early returns):
 *   void my_hot_function(keel_worker_t *worker, ...) {
 *       KEEL_INSTR_SCOPE(worker->instr, KEEL_INSTR_FLOW_FE_DATA);
 *       ...
 *   }
 *
 *   // Manual begin/end for partial-function timing:
 *   uint64_t t0 = keel_instr_begin(worker->instr, KEEL_INSTR_POOL_BORROW);
 *   backend_conn_t *conn = backend_pool_borrow(...);
 *   keel_instr_end(worker->instr, KEEL_INSTR_POOL_BORROW, t0);
 *
 * Configuration:
 * ==============
 *   [instrument]
 *   enabled     = on          ; master switch (default: off)
 *   cat_engine  = on          ; flow processing probes
 *   cat_pool    = on          ; connection pool probes
 *   cat_proto   = on          ; protocol framing/classification
 *   cat_io      = on          ; network I/O send/recv
 *   cat_hook    = on          ; hook chain execution
 *   cat_route   = on          ; routing decisions
 *   cat_ps      = on          ; prepared statement ops
 *   cat_state   = on          ; state sync / DISCARD ALL
 */

#ifndef KEEL_INSTRUMENT_H
#define KEEL_INSTRUMENT_H

#include "keel_types.h"
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Instrumentation Categories (bitmask)
 * ============================================================================ */

#define KEEL_INSTR_CAT_ENGINE  (1u << 0)   /**< Flow processing (fe_data, be_data, resume) */
#define KEEL_INSTR_CAT_POOL    (1u << 1)   /**< Connection pool (borrow, return, drain) */
#define KEEL_INSTR_CAT_PROTO   (1u << 2)   /**< Protocol framing & classification */
#define KEEL_INSTR_CAT_IO      (1u << 3)   /**< Network I/O (send_nb) */
#define KEEL_INSTR_CAT_HOOK    (1u << 4)   /**< Hook chain execution */
#define KEEL_INSTR_CAT_ROUTE   (1u << 5)   /**< Routing decision */
#define KEEL_INSTR_CAT_PS      (1u << 6)   /**< Prepared statement ops */
#define KEEL_INSTR_CAT_STATE   (1u << 7)   /**< State sync / DISCARD ALL */

#define KEEL_INSTR_CAT_ALL     0xFFu
#define KEEL_INSTR_CAT_NONE    0u

/* ============================================================================
 * Probe Point Enumeration
 * ============================================================================
 * Each enum value identifies a distinct instrumented code region.
 * Order determines display order in stats_dump.
 */

typedef enum keel_instr_id {
    /* Engine flow (CAT_ENGINE) */
    KEEL_INSTR_FLOW_FE_DATA,             /**< keel_engine_flow_on_fe_data */
    KEEL_INSTR_FLOW_BE_DATA,             /**< keel_engine_flow_on_be_data */
    KEEL_INSTR_FLOW_RESUME_FROM_POOL,    /**< keel_engine_flow_resume_from_pool */

    /* Connection pool (CAT_POOL) */
    KEEL_INSTR_POOL_BORROW,              /**< backend_pool_borrow */
    KEEL_INSTR_POOL_BORROW_STMTS,        /**< backend_pool_borrow_with_stmts */
    KEEL_INSTR_POOL_RETURN,              /**< backend_pool_return */
    KEEL_INSTR_POOL_DRAIN_CLEANING,      /**< backend_pool_drain_cleaning */

    /* Protocol (CAT_PROTO) */
    KEEL_INSTR_PROTO_FE_MSG,             /**< on_fe_msg classification */
    KEEL_INSTR_PROTO_BE_MSG,             /**< on_be_msg processing */

    /* I/O (CAT_IO) */
    KEEL_INSTR_SEND_CLIENT,              /**< send to frontend */
    KEEL_INSTR_SEND_BACKEND,             /**< send to backend */

    /* Hook chain (CAT_HOOK) */
    KEEL_INSTR_HOOK_CHAIN,               /**< hook chain invocation */

    /* Routing (CAT_ROUTE) */
    KEEL_INSTR_ROUTE_DECISION,           /**< read/write routing */

    /* Prepared statements (CAT_PS) */
    KEEL_INSTR_PS_LOOKUP,                /**< PS cache lookup */
    KEEL_INSTR_PS_REPLAY,                /**< PS replay to backend */

    /* State sync (CAT_STATE) */
    KEEL_INSTR_STATE_SYNC,               /**< SET/RESET state replay */

    KEEL_INSTR__COUNT                    /**< Sentinel — total probe count */
} keel_instr_id_t;

/* ============================================================================
 * Probe Point Data (per-worker, no atomics needed — single writer)
 * ============================================================================ */

typedef struct keel_instr_probe {
    uint64_t call_count;    /**< Number of invocations */
    uint64_t total_ns;      /**< Cumulative nanoseconds */
    uint64_t min_ns;        /**< Minimum single-call duration */
    uint64_t max_ns;        /**< Maximum single-call duration */
} keel_instr_probe_t;

/* ============================================================================
 * Per-Worker Instrumentation Context
 * ============================================================================ */

typedef struct keel_instr_ctx {
    uint32_t            enabled_mask;   /**< Category bitmask (0 = all off) */
    keel_instr_probe_t  probes[KEEL_INSTR__COUNT];
} keel_instr_ctx_t;

/* ============================================================================
 * Aggregate Snapshot (for cross-worker reporting)
 * ============================================================================ */

typedef struct keel_instr_snapshot {
    keel_instr_probe_t probes[KEEL_INSTR__COUNT];
} keel_instr_snapshot_t;

/* ============================================================================
 * Probe → Category Mapping
 * ============================================================================ */

/** Return the category mask associated with a probe identifier. */
KEEL_INLINE uint32_t keel_instr_probe_category(keel_instr_id_t id)
{
    static const uint32_t cat_map[KEEL_INSTR__COUNT] = {
        [KEEL_INSTR_FLOW_FE_DATA]          = KEEL_INSTR_CAT_ENGINE,
        [KEEL_INSTR_FLOW_BE_DATA]          = KEEL_INSTR_CAT_ENGINE,
        [KEEL_INSTR_FLOW_RESUME_FROM_POOL] = KEEL_INSTR_CAT_ENGINE,
        [KEEL_INSTR_POOL_BORROW]           = KEEL_INSTR_CAT_POOL,
        [KEEL_INSTR_POOL_BORROW_STMTS]    = KEEL_INSTR_CAT_POOL,
        [KEEL_INSTR_POOL_RETURN]           = KEEL_INSTR_CAT_POOL,
        [KEEL_INSTR_POOL_DRAIN_CLEANING]   = KEEL_INSTR_CAT_POOL,
        [KEEL_INSTR_PROTO_FE_MSG]          = KEEL_INSTR_CAT_PROTO,
        [KEEL_INSTR_PROTO_BE_MSG]          = KEEL_INSTR_CAT_PROTO,
        [KEEL_INSTR_SEND_CLIENT]           = KEEL_INSTR_CAT_IO,
        [KEEL_INSTR_SEND_BACKEND]          = KEEL_INSTR_CAT_IO,
        [KEEL_INSTR_HOOK_CHAIN]            = KEEL_INSTR_CAT_HOOK,
        [KEEL_INSTR_ROUTE_DECISION]        = KEEL_INSTR_CAT_ROUTE,
        [KEEL_INSTR_PS_LOOKUP]             = KEEL_INSTR_CAT_PS,
        [KEEL_INSTR_PS_REPLAY]             = KEEL_INSTR_CAT_PS,
        [KEEL_INSTR_STATE_SYNC]            = KEEL_INSTR_CAT_STATE,
    };
    return (id < KEEL_INSTR__COUNT) ? cat_map[id] : 0;
}

/* ============================================================================
 * Probe Name Strings
 * ============================================================================ */

/** Return a stable human-readable name for a probe identifier. */
KEEL_INLINE const char *keel_instr_probe_name(keel_instr_id_t id)
{
    static const char *names[KEEL_INSTR__COUNT] = {
        [KEEL_INSTR_FLOW_FE_DATA]          = "flow_fe_data",
        [KEEL_INSTR_FLOW_BE_DATA]          = "flow_be_data",
        [KEEL_INSTR_FLOW_RESUME_FROM_POOL] = "flow_resume_pool",
        [KEEL_INSTR_POOL_BORROW]           = "pool_borrow",
        [KEEL_INSTR_POOL_BORROW_STMTS]    = "pool_borrow_stmts",
        [KEEL_INSTR_POOL_RETURN]           = "pool_return",
        [KEEL_INSTR_POOL_DRAIN_CLEANING]   = "pool_drain_clean",
        [KEEL_INSTR_PROTO_FE_MSG]          = "proto_fe_msg",
        [KEEL_INSTR_PROTO_BE_MSG]          = "proto_be_msg",
        [KEEL_INSTR_SEND_CLIENT]           = "send_client",
        [KEEL_INSTR_SEND_BACKEND]          = "send_backend",
        [KEEL_INSTR_HOOK_CHAIN]            = "hook_chain",
        [KEEL_INSTR_ROUTE_DECISION]        = "route_decision",
        [KEEL_INSTR_PS_LOOKUP]             = "ps_lookup",
        [KEEL_INSTR_PS_REPLAY]             = "ps_replay",
        [KEEL_INSTR_STATE_SYNC]            = "state_sync",
    };
    return (id < KEEL_INSTR__COUNT) ? names[id] : "unknown";
}

/* ============================================================================
 * Timing Primitives
 * ============================================================================ */

/** High-resolution monotonic clock used for timing instrumentation scopes. */
KEEL_INLINE uint64_t keel_instr_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ============================================================================
 * Manual Begin/End API
 * ============================================================================ */

/**
 * @brief Begin timing a probe. Returns start timestamp, or 0 if disabled.
 * @param ctx  Instrumentation context (may be NULL)
 * @param id   Probe identifier
 */
KEEL_INLINE uint64_t keel_instr_begin(keel_instr_ctx_t *ctx, keel_instr_id_t id)
{
    if (KEEL_UNLIKELY(ctx != NULL &&
                      (ctx->enabled_mask & keel_instr_probe_category(id)))) {
        return keel_instr_now_ns();
    }
    return 0;
}

/**
 * @brief End timing a probe. Records measurement if t0 > 0.
 * @param ctx  Instrumentation context (may be NULL)
 * @param id   Probe identifier
 * @param t0   Start timestamp from keel_instr_begin (0 = skip)
 */
KEEL_INLINE void keel_instr_end(keel_instr_ctx_t *ctx, keel_instr_id_t id,
                                uint64_t t0)
{
    if (KEEL_LIKELY(t0 == 0)) return;

    uint64_t elapsed = keel_instr_now_ns() - t0;
    keel_instr_probe_t *p = &ctx->probes[id];
    p->call_count++;
    p->total_ns += elapsed;
    if (elapsed < p->min_ns) p->min_ns = elapsed;
    if (elapsed > p->max_ns) p->max_ns = elapsed;
}

/* ============================================================================
 * Scoped Timer (GCC/Clang cleanup attribute)
 * ============================================================================
 * Automatically records timing on scope exit, including early returns.
 */

typedef struct keel_instr_scope {
    keel_instr_ctx_t *ctx;
    keel_instr_id_t   id;
    uint64_t          start_ns;
} keel_instr_scope_t;

/** Cleanup handler automatically invoked when a scoped probe leaves scope. */
KEEL_INLINE void keel_instr_scope_cleanup_(keel_instr_scope_t *s)
{
    if (KEEL_LIKELY(s->start_ns == 0)) return;
    keel_instr_end(s->ctx, s->id, s->start_ns);
}

/* Internal helper for unique variable names */
#define KEEL_INSTR_CONCAT_(a, b) a##b
#define KEEL_INSTR_CONCAT(a, b) KEEL_INSTR_CONCAT_(a, b)

/**
 * @brief Declare a scoped instrumentation timer.
 *
 * Usage (must be placed at the start of a block):
 *   KEEL_INSTR_SCOPE(ctx_ptr, KEEL_INSTR_FLOW_FE_DATA);
 *
 * The timer starts immediately and records on any exit path.
 */
#define KEEL_INSTR_SCOPE(ictx, probe_id) \
    keel_instr_ctx_t *KEEL_INSTR_CONCAT(_keel_ictx_, __LINE__) = (ictx); \
    __attribute__((cleanup(keel_instr_scope_cleanup_))) \
    keel_instr_scope_t KEEL_INSTR_CONCAT(_keel_is_, __LINE__); \
    KEEL_INSTR_CONCAT(_keel_is_, __LINE__).ctx = KEEL_INSTR_CONCAT(_keel_ictx_, __LINE__); \
    KEEL_INSTR_CONCAT(_keel_is_, __LINE__).id = (probe_id); \
    KEEL_INSTR_CONCAT(_keel_is_, __LINE__).start_ns = \
        keel_instr_begin(KEEL_INSTR_CONCAT(_keel_ictx_, __LINE__), (probe_id))

/* ============================================================================
 * Context Lifecycle
 * ============================================================================ */

/** Initialize a probe context, zeroing counters and priming min values. */
KEEL_INLINE void keel_instr_ctx_init(keel_instr_ctx_t *ctx, uint32_t mask)
{
    ctx->enabled_mask = mask;
    for (int i = 0; i < KEEL_INSTR__COUNT; i++) {
        ctx->probes[i].call_count = 0;
        ctx->probes[i].total_ns   = 0;
        ctx->probes[i].min_ns     = UINT64_MAX;
        ctx->probes[i].max_ns     = 0;
    }
}

/** Reset all probe counters while preserving the enabled category mask. */
KEEL_INLINE void keel_instr_ctx_reset(keel_instr_ctx_t *ctx)
{
    for (int i = 0; i < KEEL_INSTR__COUNT; i++) {
        ctx->probes[i].call_count = 0;
        ctx->probes[i].total_ns   = 0;
        ctx->probes[i].min_ns     = UINT64_MAX;
        ctx->probes[i].max_ns     = 0;
    }
}

/** Merge one probe snapshot into another for cross-worker aggregation. */
KEEL_INLINE void keel_instr_probe_merge(keel_instr_probe_t *dst,
                                        const keel_instr_probe_t *src)
{
    dst->call_count += src->call_count;
    dst->total_ns   += src->total_ns;
    if (src->min_ns < dst->min_ns) dst->min_ns = src->min_ns;
    if (src->max_ns > dst->max_ns) dst->max_ns = src->max_ns;
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_INSTRUMENT_H */
