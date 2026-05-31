/**
 * @file stats.h
 * @brief Public instrumentation API for worker-local counters, histograms, and process snapshots.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * KEEL's statistics subsystem is designed around a strict separation between hot
 * path and cold path. Workers record events into their own per-thread contexts
 * using relaxed atomics and fixed-layout structures; aggregation, formatting, and
 * `/proc` sampling happen elsewhere and only on timers or administrative reads.
 *
 * The core design goals are:
 *
 * - near-zero branch and synchronization overhead for the common event path;
 * - progressively richer observability as the configured level increases;
 * - bounded data structures whose memory layout is stable enough for fast worker
 *   access and cheap whole-process aggregation.
 *
 * Precision is intentionally approximate in places. Relaxed atomic loads and
 * cross-worker snapshots are acceptable because operational visibility matters
 * more here than perfectly serialized accounting.
 */

#ifndef KEEL_STATS_H
#define KEEL_STATS_H

#include "keel_types.h"
#include "instrument.h"
#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Stats Levels
 * ============================================================================ */

typedef enum keel_stats_level {
    KEEL_STATS_OFF      = 0,   /**< No collection */
    KEEL_STATS_BASIC    = 1,   /**< Atomic counters only */
    KEEL_STATS_EXTENDED = 2,   /**< + latency histograms */
    KEEL_STATS_SYSTEM   = 3,   /**< + OS-level sampling */
    KEEL_STATS_TRACE    = 4,   /**< + USDT probes */
    KEEL_STATS_FULL     = 5,   /**< + SQL fingerprinting */
} keel_stats_level_t;

/* ==========================================================================
 * System Instrumentation Category Mask (L3)
 * ==========================================================================
 * Runtime-selectable categories for KEEL_STATS_SYSTEM sampling.
 * This allows operators to disable expensive samplers when needed.
 */

#define KEEL_STAT_SYS_CPU      (1u << 0)
#define KEEL_STAT_SYS_MEMORY   (1u << 1)
#define KEEL_STAT_SYS_FD       (1u << 2)
#define KEEL_STAT_SYS_DISK     (1u << 3)
#define KEEL_STAT_SYS_NETWORK  (1u << 4)

#define KEEL_STAT_SYS_ALL ( \
    KEEL_STAT_SYS_CPU    | \
    KEEL_STAT_SYS_MEMORY | \
    KEEL_STAT_SYS_FD     | \
    KEEL_STAT_SYS_DISK   | \
    KEEL_STAT_SYS_NETWORK)

/* ==========================================================================
 * Hot-Path Instrumentation Category Mask (L1/L2)
 * ==========================================================================
 * Runtime-selectable categories for worker hot-path timing/counters.
 * These gates are independent of stats level and are intended to control
 * attribution overhead while keeping critical visibility on demand.
 */

#define KEEL_HOT_INSTR_WAIT_POOL                (1u << 0)
#define KEEL_HOT_INSTR_WAIT_BACKEND             (1u << 1)
#define KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT (1u << 2)
#define KEEL_HOT_INSTR_DEFERRED_SEND            (1u << 3)

#define KEEL_HOT_INSTR_ALL ( \
    KEEL_HOT_INSTR_WAIT_POOL                | \
    KEEL_HOT_INSTR_WAIT_BACKEND             | \
    KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT | \
    KEEL_HOT_INSTR_DEFERRED_SEND)

/* ============================================================================
 * Atomic Counter Types
 * ============================================================================
 * Per-worker, cache-line padded to prevent false sharing.
 */

/**
 * @brief Monotonically increasing counter used for totals and event counts.
 */
typedef struct keel_counter {
    KEEL_CACHE_ALIGNED atomic_uint_fast64_t value;
} keel_counter_t;

/**
 * @brief Signed gauge used for live occupancy-style metrics.
 */
typedef struct keel_gauge {
    KEEL_CACHE_ALIGNED atomic_int_fast64_t value;
} keel_gauge_t;

/* Counter operations use relaxed ordering because the values are observational
 * only; no correctness decision in the engine depends on a strongly ordered stats
 * update becoming visible immediately. */
KEEL_INLINE void keel_counter_inc(keel_counter_t *c)
{
    atomic_fetch_add_explicit(&c->value, 1, memory_order_relaxed);
}

KEEL_INLINE void keel_counter_add(keel_counter_t *c, uint64_t n)
{
    atomic_fetch_add_explicit(&c->value, n, memory_order_relaxed);
}

KEEL_INLINE uint64_t keel_counter_get(const keel_counter_t *c)
{
    const atomic_uint_fast64_t *v =
        (const atomic_uint_fast64_t *)(uintptr_t)&c->value;
    return atomic_load_explicit(v, memory_order_relaxed);
}

KEEL_INLINE void keel_counter_reset(keel_counter_t *c)
{
    atomic_store_explicit(&c->value, 0, memory_order_relaxed);
}

/* Gauge operations */
KEEL_INLINE void keel_gauge_set(keel_gauge_t *g, int64_t v)
{
    atomic_store_explicit(&g->value, v, memory_order_relaxed);
}

KEEL_INLINE void keel_gauge_inc(keel_gauge_t *g)
{
    atomic_fetch_add_explicit(&g->value, 1, memory_order_relaxed);
}

KEEL_INLINE void keel_gauge_dec(keel_gauge_t *g)
{
    atomic_fetch_sub_explicit(&g->value, 1, memory_order_relaxed);
}

KEEL_INLINE int64_t keel_gauge_get(const keel_gauge_t *g)
{
    const atomic_int_fast64_t *v =
        (const atomic_int_fast64_t *)(uintptr_t)&g->value;
    return atomic_load_explicit(v, memory_order_relaxed);
}

/* ============================================================================
 * Latency Histogram (Level 2+)
 * ============================================================================
 * Log2-based histogram with 64 buckets.
 * Bucket k covers [2^k, 2^(k+1)) nanoseconds.
 * Bucket 0 = [0, 2) ns, Bucket 10 = [1024, 2048) ns ≈ 1μs,
 * Bucket 20 ≈ 1ms, Bucket 30 ≈ 1s, etc.
 *
 * This gives sub-microsecond resolution at low latencies and
 * logarithmic compression at high latencies — perfect for
 * database query latency distributions.
 */

#define KEEL_HISTOGRAM_BUCKETS 64

typedef struct keel_histogram {
    atomic_uint_fast64_t buckets[KEEL_HISTOGRAM_BUCKETS];
    atomic_uint_fast64_t count;      /**< Total observations */
    atomic_uint_fast64_t sum;        /**< Sum of all observed values */
    atomic_uint_fast64_t min_val;    /**< Minimum observed value */
    atomic_uint_fast64_t max_val;    /**< Maximum observed value */
} keel_histogram_t;

/**
 * @brief Record one latency sample into the logarithmic histogram.
 *
 * @param h Histogram to update.
 * @param val Sample value, typically measured in nanoseconds.
 * @return
 */
void keel_histogram_record(keel_histogram_t *h, uint64_t val);

/**
 * @brief Reset a histogram to the empty baseline.
 *
 * @param h Histogram to reset.
 * @return
 */
void keel_histogram_reset(keel_histogram_t *h);

/**
 * @brief Estimate a percentile from the histogram's bucket distribution.
 *
 * @param h Histogram to inspect.
 * @param pct Percentile as a fraction in the range `[0.0, 1.0]`.
 * @return Approximate percentile value.
 */
uint64_t keel_histogram_percentile(const keel_histogram_t *h, double pct);

/* Snapshot structure for reading histogram without races */
typedef struct keel_histogram_snapshot {
    uint64_t buckets[KEEL_HISTOGRAM_BUCKETS];
    uint64_t count;
    uint64_t sum;
    uint64_t min_val;
    uint64_t max_val;
} keel_histogram_snapshot_t;

/**
 * @brief Copy histogram counters into a stable snapshot structure.
 *
 * @param h Histogram to sample.
 * @param snap [out] Destination snapshot.
 * @return
 */
void keel_histogram_snapshot(const keel_histogram_t *h,
                            keel_histogram_snapshot_t *snap);

/* ============================================================================
 * Per-Worker Stats Context (L1 — BASIC)
 * ============================================================================
 * One of these per worker thread. No locking needed for writes because
 * each worker is the sole producer. Reads are safe due to atomic loads.
 */

typedef struct keel_stats_basic {
    /* -- Pool metrics -- */
    keel_counter_t   pool_borrows;       /**< Backend borrowed from pool */
    keel_counter_t   pool_returns;       /**< Backend returned to pool */
    keel_counter_t   pool_creates;       /**< Backend connections opened */
    keel_counter_t   pool_destroys;      /**< Backend connections closed */
    keel_counter_t   pool_hits;          /**< Pool had a clean conn ready */
    keel_counter_t   pool_misses;        /**< Pool empty, had to create */
    keel_counter_t   pool_borrow_attempts;            /**< Borrow decisions attempted */
    keel_counter_t   pool_borrow_exact_state_match;   /**< Borrow selected exact session-state match */
    keel_counter_t   pool_borrow_exact_stmt_match;    /**< Borrow selected exact prepared-statement match */
    keel_counter_t   pool_borrow_state_replay;        /**< Borrow selected backend requiring state replay */
    keel_counter_t   pool_borrow_stmt_replay;         /**< Borrow selected backend requiring statement replay */
    keel_counter_t   pool_borrow_cleanup_required;    /**< Borrow selected backend requiring cleanup before use */
    keel_counter_t   backend_borrow_success;          /**< Central borrow predicate accepted and borrow succeeded */
    keel_counter_t   backend_borrow_failed_incompatible; /**< Borrow rejected by lifecycle incompatibility */
    keel_counter_t   backend_borrow_failed_quarantined;  /**< Borrow rejected because backend is quarantined */

    /* -- Session metrics -- */
    keel_counter_t   sessions_created;   /**< Frontend connections accepted */
    keel_counter_t   sessions_closed;    /**< Frontend connections closed */
    keel_gauge_t     sessions_active;    /**< Currently active sessions */
    uint64_t        sessions_peak;      /**< High-water mark (non-atomic, same thread) */

    /* -- Query metrics -- */
    keel_counter_t   queries_total;      /**< Total queries routed */
    keel_counter_t   queries_read;       /**< SELECT / read-only queries */
    keel_counter_t   queries_write;      /**< INSERT/UPDATE/DELETE */
    keel_counter_t   queries_tx;         /**< Explicit transactions (BEGIN..END) */

    /* -- Error metrics -- */
    keel_counter_t   errors_auth;        /**< Authentication failures */
    keel_counter_t   errors_proto;       /**< Protocol errors */
    keel_counter_t   errors_backend;     /**< Backend errors (connect/recv/send) */
    keel_counter_t   errors_timeout;     /**< Timeout errors */
    keel_counter_t   errors_total;       /**< Sum of all errors */

    /* -- I/O metrics -- */
    keel_counter_t   bytes_recv;         /**< Bytes received from frontends */
    keel_counter_t   bytes_sent;         /**< Bytes sent to frontends */
    keel_counter_t   bytes_backend_recv; /**< Bytes received from backends */
    keel_counter_t   bytes_backend_sent; /**< Bytes sent to backends */
    keel_counter_t   bytes_spliced;      /**< Bytes zero-copy spliced */

    /* -- Reactor loop metrics -- */
    keel_counter_t   loop_iterations;    /**< Event loop iterations */
    keel_counter_t   ops_submitted;      /**< io_uring SQEs submitted */
    keel_counter_t   ops_completed;      /**< CQEs reaped */

    /* -- Multiplexing safety metrics -- */
    keel_counter_t   discard_all_count;       /**< DISCARD ALL commands issued */
    keel_counter_t   discard_all_failure;     /**< Cleanup/discard failed before reusable boundary */
    keel_counter_t   state_sync_count;        /**< SET/RESET state sync replays issued */
    keel_counter_t   quarantine_count;        /**< Statements held in quarantine */
    keel_counter_t   sticky_primary_hits;     /**< Reads routed to primary due to sticky affinity */
    keel_counter_t   copy_pause_count;        /**< COPY backpressure pause events */
    keel_counter_t   prepared_hardpin_count;  /**< Sessions hard-pinned due to prepared stmts */
    keel_counter_t   backend_error_transient; /**< Transient backend errors (EAGAIN, temp) */
    keel_counter_t   backend_error_fatal;     /**< Fatal backend errors (EOF, reset) */
    keel_counter_t   copy_bytes_total;        /**< Total bytes sent during COPY operations */
    keel_counter_t   notify_relayed;          /**< NotificationResponse ('A') messages relayed to clients */
    keel_counter_t   osc_sessions_detected;   /**< Sessions identified as OSC tool connections (gh-ost/pt-osc) */
    keel_counter_t   backend_close_dead_idle;       /**< Idle backend closed after failed liveness check */
    keel_counter_t   backend_close_cleanup_error;   /**< Cleaning backend closed after cleanup/protocol error */
    keel_counter_t   backend_close_cleanup_timeout; /**< Cleaning backend closed after cleanup timeout */
    keel_counter_t   backend_close_client_disconnect; /**< Backend closed because owning client disconnected */
    keel_counter_t   backend_close_io_error;        /**< Backend closed after socket-level I/O error */
    keel_counter_t   backend_close_prune_idle;      /**< Idle backend pruned by size policy */
    keel_counter_t   backend_close_prune_aged;      /**< Backend pruned for exceeding max-age */
    keel_counter_t   backend_close_drain_idle;      /**< Idle backend closed during pool drain */
    keel_counter_t   backend_close_backend_eof;     /**< Backend closed unexpectedly (EOF/RST) outside cleanup */
    keel_counter_t   backend_close_connect_failed;  /**< Backend close after connect/handshake socket failure */
    keel_counter_t   backend_close_auth_failed;     /**< Backend close after authentication denial */
    keel_counter_t   backend_close_protocol_error;  /**< Backend close after steady-state protocol violation */
    keel_counter_t   backend_close_sync_error;      /**< Backend close after extended-protocol Sync mismatch */
    keel_counter_t   backend_close_stmt_replay_error;/**< Backend close after prepared-statement replay failure */
    keel_counter_t   backend_close_shutdown;        /**< Backend close during process shutdown */
    keel_counter_t   backend_close_pool_eviction;   /**< Backend close due to pool resize/policy eviction */
    keel_counter_t   cleaning_timeout_total;        /**< Cleanup state-machine timeout events */
    keel_counter_t   pin_reason_transaction;        /**< Times transaction pin became active */
    keel_counter_t   pin_reason_extended_protocol;  /**< Times extended-protocol pin became active */
    keel_counter_t   pin_reason_prepared_stmt;      /**< Times prepared-statement pin became active */
    keel_counter_t   pin_reason_other;              /**< Times another hard pin reason became active */
    keel_counter_t   commit_in_doubt_started;       /**< Commit-in-doubt recovery sessions started */
    keel_counter_t   commit_in_doubt_resolved;      /**< Commit-in-doubt recovery sessions resolved */
    keel_counter_t   commit_in_doubt_failed;        /**< Commit-in-doubt recovery sessions could not resolve */
    keel_gauge_t     sessions_pinned;         /**< Currently pinned sessions */
    keel_gauge_t     sessions_pinned_transaction;       /**< Sessions currently pinned by transaction */
    keel_gauge_t     sessions_pinned_extended_protocol; /**< Sessions currently pinned by extended protocol */
    keel_gauge_t     sessions_pinned_prepared_stmt;      /**< Sessions currently pinned by prepared statements */
    keel_gauge_t     sessions_commit_in_doubt;           /**< Sessions currently resolving commit outcome */
    keel_gauge_t     backends_cleaning;       /**< Backend slots in CLEANING state */

    /* -- Async pre-query replay (deferred BEGIN, PR #4) -- */
    keel_counter_t   pre_query_replay_count;    /**< Successful BEGIN-then-payload replays */
    keel_counter_t   pre_query_send_fail;       /**< BEGIN send failed with hard errno */
    keel_counter_t   pre_query_be_disconnect;   /**< Backend disconnected mid-replay */
    keel_counter_t   pre_query_proto_violation; /**< Malformed wire frame during absorption */
    keel_counter_t   pre_query_overflow;        /**< FE payload exceeded KEEL_PRE_QUERY_REPLAY_BUFSZ */
    keel_counter_t   pre_query_runaway;         /**< Absorbed too many bytes without ReadyForQuery */
    keel_counter_t   cleanup_result_success;        /**< Cleanup finished at reusable boundary */
    keel_counter_t   cleanup_result_protocol_error; /**< Cleanup failed due to unsafe response stream */
    keel_counter_t   cleanup_result_timeout;        /**< Cleanup timed out */
    keel_counter_t   cleanup_result_backend_eof;    /**< Backend disconnected during cleanup */
    keel_counter_t   cleanup_result_send_failure;   /**< Cleanup command send failure */
    keel_counter_t   replay_result_success;         /**< Replay/setup pipeline completed */
    keel_counter_t   replay_result_parse_error;     /**< Replay failed with protocol parse error */
    keel_counter_t   replay_result_drain_error;     /**< Replay failed while draining setup responses */
    keel_counter_t   replay_result_timeout;         /**< Replay/setup timed out */
    keel_counter_t   replay_result_oom;             /**< Replay/setup failed due to allocation failure */
    keel_counter_t   replay_result_partial_send_failure; /**< Replay/setup failed after partial send path */

    /* -- Connection migration metrics -- */
    keel_counter_t   migrations_sent;         /**< Sessions migrated away to another worker */
    keel_counter_t   migrations_received;     /**< Sessions received from another worker */
    keel_counter_t   rebalance_checks;        /**< Number of rebalance timer ticks */
    keel_counter_t   rebalance_migrations;    /**< Sessions migrated due to automatic rebalancing */
    keel_counter_t   rebalance_skipped;       /**< Rebalance check skipped (e.g., threshold not met) */

    /* -- Protocol-path wait instrumentation -- */
    keel_counter_t   flow_wait_pool_events;      /**< FE messages queued waiting for pool backend */
    keel_counter_t   flow_wait_pool_ns_total;    /**< Total nanoseconds spent in WAIT_POOL */
    keel_counter_t   flow_wait_backend_events;   /**< FE/flow transitions entering WAIT_BACKEND */
    keel_counter_t   flow_wait_backend_ns_total; /**< Total nanoseconds until first backend byte */
    keel_counter_t   flow_wait_backend_query_events;    /**< WAIT_BACKEND entered for normal query/forward */
    keel_counter_t   flow_wait_backend_query_ns_total;  /**< WAIT_BACKEND query nanoseconds until first backend byte */
    keel_counter_t   flow_wait_backend_query_exec_ns_total; /**< WAIT_BACKEND query ns before backend socket becomes readable */
    keel_counter_t   flow_wait_backend_query_io_ns_total;   /**< WAIT_BACKEND query ns from recv-arm to first backend byte */
    keel_counter_t   flow_wait_backend_query_io_reactor_ns_total; /**< Query IO ns from recv-arm to backend recv callback entry */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_ns_total; /**< Query IO ns from recv-arm to CQE-ready observation */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_wakeup_ns_total; /**< Query IO ready ns from recv-arm to reactor wakeup */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_sched_ns_total; /**< Query IO ready ns from reactor wakeup to CQE dispatch */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_sched_head_ns_total; /**< Query IO sched ns when CQE is first in completion batch */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_sched_tail_ns_total; /**< Query IO sched ns when CQE is not first in completion batch */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_sched_batch_size_sum; /**< Sum of completion batch sizes for query ready-sched samples */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_sched_batch_index_sum; /**< Sum of 1-based completion batch indices for query ready-sched samples */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_sched_batch_size_1_events; /**< Query ready-sched samples observed in completion batch size 1 */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_sched_batch_size_2_events; /**< Query ready-sched samples observed in completion batch size 2 */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_sched_batch_size_3_events; /**< Query ready-sched samples observed in completion batch size 3 */
    keel_counter_t   flow_wait_backend_query_io_reactor_ready_sched_batch_size_4p_events; /**< Query ready-sched samples observed in completion batch size >=4 */
    keel_counter_t   flow_wait_backend_query_io_reactor_dispatch_ns_total; /**< Query IO ns from CQE-ready observation to callback entry */
    keel_counter_t   flow_wait_backend_query_io_service_ns_total; /**< Query IO ns spent in callback before first-byte accounting */
    keel_counter_t   flow_wait_backend_query_framing_ns_total; /**< Query backend-response framing/dispatch ns after first byte */
    keel_counter_t   flow_wait_backend_query_deferred_send_events; /**< Query waits preceded by deferred FE→BE send */
    keel_counter_t   flow_wait_backend_query_deferred_send_ns_total; /**< Deferred FE→BE send ns before WAIT_BACKEND(query) */
    keel_counter_t   flow_wait_backend_replay_events;   /**< WAIT_STMT_REPLAY entered for Parse replay */
    keel_counter_t   flow_wait_backend_replay_ns_total; /**< WAIT_STMT_REPLAY replay nanoseconds until first backend byte */
    keel_counter_t   flow_wait_backend_discard_events;  /**< WAIT_STMT_REPLAY entered for DISCARD ALL drain */
    keel_counter_t   flow_wait_backend_discard_ns_total;/**< WAIT_STMT_REPLAY discard nanoseconds until first backend byte */

    /* -- Catch-up wait (Phase 2 reactor-owned WAIT loop) --
     * One waiter == one session parked in keel_catchup_manager_t pending a
     * replica reaching its required LSN/GTID token. */
    keel_counter_t   catchup_waiters_enqueued;          /**< Sessions enqueued in the catch-up wait list */
    keel_counter_t   catchup_waiters_fulfilled;         /**< Waiters released because target replica reached token */
    keel_counter_t   catchup_waiters_timeout;           /**< Waiters released because max_replica_catchup_ms elapsed */
    keel_counter_t   catchup_waiters_cancelled;         /**< Waiters released because session/connection closed */
    keel_counter_t   catchup_wait_ns_total;             /**< Total nanoseconds sessions spent parked in WAIT_CATCHUP */
    keel_counter_t   catchup_probes_issued;             /**< Replica catch-up probes sent (cache misses only) */
    keel_counter_t   catchup_probes_succeeded;          /**< Probes that returned "reached" */
    keel_counter_t   catchup_probes_negative;           /**< Probes that returned "not yet reached" */
    keel_counter_t   catchup_probes_failed;             /**< Probes that errored (I/O, timeout, parse) */
    keel_counter_t   catchup_cache_hits;                /**< Decisions resolved by the probe-result cache */
    keel_counter_t   catchup_probe_reconnects;          /**< Probe sockets reopened after error/EOF */
    keel_counter_t   catchup_probe_backoff_skips;       /**< Probe attempts skipped because socket is in backoff */

    /* -- Pool queue diagnostics -- */
    keel_counter_t   pool_wait_queue_enqueued;      /**< Sessions enqueued waiting for backend */
    keel_counter_t   pool_wait_queue_full_rejects;  /**< Enqueue attempts rejected due to full queue */
    keel_counter_t   pool_wait_resume_success;      /**< Wait callbacks that successfully borrowed backend */
    keel_counter_t   pool_wait_resume_requeues;     /**< Wait callbacks that had to requeue (no backend yet) */
    keel_counter_t   pool_wait_timeout_events;      /**< Waiters expired by wait_timeout_ms */
    keel_counter_t   pool_wait_cancelled;           /**< Waiters cancelled because client/session closed */

    /* -- Protocol health observability -- */
    keel_counter_t   proxy_state_desync_total;          /**< Protocol/state mismatch events */
    keel_counter_t   proxy_orphaned_transactions_total; /**< Sessions closed while tx still open */
    keel_counter_t   proxy_backend_reuse_failure_total; /**< Backend could not be safely reused */
    keel_counter_t   proxy_io_uring_sq_overflow_total;  /**< io_uring SQ ring full events */
    keel_gauge_t     proxy_buffer_pool_utilization_bytes; /**< Active recv-context pool bytes */
    keel_gauge_t     proxy_connection_age_seconds;      /**< Oldest active frontend connection age */
    keel_gauge_t     proxy_heartbeat_last_ns;           /**< Last worker heartbeat (monotonic ns) */
} keel_stats_basic_t;

/* ============================================================================
 * Extended Stats (L2)
 * ============================================================================ */

typedef struct keel_stats_extended {
    keel_histogram_t query_latency_ns;       /**< End-to-end query latency */
    keel_histogram_t backend_latency_ns;     /**< Backend response latency */
    keel_histogram_t connect_latency_ns;     /**< Backend connect latency */
    keel_histogram_t session_duration_ns;    /**< Frontend session duration */
    keel_histogram_t wait_latency_ns;        /**< Time waiting for pool conn */
    keel_histogram_t discard_latency_ns;     /**< DISCARD ALL round-trip latency */
    keel_histogram_t state_sync_latency_ns;  /**< State sync SET/RESET latency */
    keel_histogram_t cleanup_duration_ns;    /**< Cleanup state-machine duration */
    keel_histogram_t replay_duration_ns;     /**< Setup replay/state-sync duration */
} keel_stats_extended_t;

/* ============================================================================
 * System Stats (L3) — Sampled periodically, not per-event
 * ============================================================================ */

typedef struct keel_stats_system {
    /* CPU */
    double      cpu_user_pct;       /**< User CPU % */
    double      cpu_sys_pct;        /**< System CPU % */

    /* Memory */
    uint64_t    rss_bytes;          /**< Resident set size */
    uint64_t    vm_bytes;           /**< Virtual memory size */

    /* File descriptors */
    uint32_t    fd_open;            /**< Open file descriptors */
    uint32_t    fd_limit;           /**< FD ulimit */

    /* io_uring (when applicable) */
    uint32_t    uring_sq_pending;   /**< SQ entries pending */
    uint32_t    uring_cq_pending;   /**< CQ entries ready */

    /* Disk I/O (Linux /proc/self/io) */
    uint64_t    disk_read_bytes;     /**< Process read bytes */
    uint64_t    disk_write_bytes;    /**< Process write bytes */

    /* Network I/O (Linux /proc/net/dev aggregate) */
    uint64_t    net_rx_bytes;        /**< Aggregate RX bytes */
    uint64_t    net_tx_bytes;        /**< Aggregate TX bytes */

    /* OS context */
    uint64_t    ctx_switches_vol;   /**< Voluntary context switches */
    uint64_t    ctx_switches_inv;   /**< Involuntary context switches */

    /* Timestamp of last sample */
    int64_t     sampled_at_ns;      /**< When this was last updated */

    /* Active category mask used for this sample */
    uint32_t    probe_mask;
} keel_stats_system_t;

/* ============================================================================
 * Per-Worker Stats Context (composite)
 * ============================================================================ */

typedef struct keel_stats_ctx {
    uint32_t                worker_id;   /**< Owning worker index */
    keel_stats_level_t       level;       /**< Configured level */

    /* L1 – always present when level >= BASIC */
    keel_stats_basic_t       basic;

    /* L2 – populated when level >= EXTENDED */
    keel_stats_extended_t    extended;

    /* Function-level instrumentation probes */
    keel_instr_ctx_t         instr;

    /* Padding to occupy full cache lines */
    char                    _pad[64];
} keel_stats_ctx_t;

/* ============================================================================
 * Global Stats Collector
 * ============================================================================
 * One per process. Aggregates across all worker contexts and collects
 * system-level metrics on a timer.
 */

typedef struct keel_stats_collector {
    keel_stats_level_t       level;          /**< Active collection level */
    keel_stats_ctx_t        *contexts;       /**< Array [num_workers] */
    size_t                  num_workers;    /**< Worker count */

    /* L3 system stats (sampled on timer) */
    keel_stats_system_t      system;

    /* Process start time for uptime calculation */
    int64_t                 start_time_ns;

    /* System-stats sampling state */
    uint64_t                prev_utime;     /**< Previous /proc utime ticks */
    uint64_t                prev_stime;     /**< Previous /proc stime ticks */
    int64_t                 prev_sample_ns; /**< Previous sample timestamp */

    /* Runtime system instrumentation category mask (KEEL_STAT_SYS_*) */
    uint32_t                system_probe_mask;
} keel_stats_collector_t;

/* ============================================================================
 * Zero-Cost Counter Macros
 * ============================================================================
 * These compile to no-ops when the stats level is below the required level.
 * Runtime check on the context's level field — the branch predictor will
 * learn the pattern very quickly since the level is set once at startup.
 */

/**
 * @brief Increment a BASIC-level counter if the context is collecting at that level.
 */
#define KEEL_STAT_INC(ctx, fld) \
    do { \
        if (KEEL_LIKELY((ctx)->level >= KEEL_STATS_BASIC)) { \
            keel_counter_inc(&(ctx)->basic.fld); \
        } \
    } while (0)

/**
 * @brief Add an arbitrary delta to a BASIC-level counter.
 */
#define KEEL_STAT_ADD(ctx, fld, n) \
    do { \
        if (KEEL_LIKELY((ctx)->level >= KEEL_STATS_BASIC)) { \
            keel_counter_add(&(ctx)->basic.fld, (uint64_t)(n)); \
        } \
    } while (0)

/**
 * @brief Store a BASIC-level gauge value.
 */
#define KEEL_STAT_GAUGE_SET(ctx, fld, v) \
    do { \
        if (KEEL_LIKELY((ctx)->level >= KEEL_STATS_BASIC)) { \
            keel_gauge_set(&(ctx)->basic.fld, (int64_t)(v)); \
        } \
    } while (0)

/**
 * @brief Increment a BASIC-level gauge.
 */
#define KEEL_STAT_GAUGE_INC(ctx, fld) \
    do { \
        if (KEEL_LIKELY((ctx)->level >= KEEL_STATS_BASIC)) { \
            keel_gauge_inc(&(ctx)->basic.fld); \
        } \
    } while (0)

/**
 * @brief Decrement a BASIC-level gauge.
 */
#define KEEL_STAT_GAUGE_DEC(ctx, fld) \
    do { \
        if (KEEL_LIKELY((ctx)->level >= KEEL_STATS_BASIC)) { \
            keel_gauge_dec(&(ctx)->basic.fld); \
        } \
    } while (0)

/**
 * @brief Record an EXTENDED-level latency sample if enabled.
 */
#define KEEL_STAT_LATENCY(ctx, fld, val_ns) \
    do { \
        if ((ctx)->level >= KEEL_STATS_EXTENDED) { \
            keel_histogram_record(&(ctx)->extended.fld, (uint64_t)(val_ns)); \
        } \
    } while (0)

/**
 * @brief Track a single-writer high-water mark without atomics.
 */
#define KEEL_STAT_PEAK(ctx, peak_fld, current_val) \
    do { \
        if (KEEL_LIKELY((ctx)->level >= KEEL_STATS_BASIC)) { \
            if ((uint64_t)(current_val) > (ctx)->basic.peak_fld) { \
                (ctx)->basic.peak_fld = (uint64_t)(current_val); \
            } \
        } \
    } while (0)

/* ============================================================================
 * Collector Lifecycle
 * ============================================================================ */

/**
 * @brief Allocate the process-wide stats collector and per-worker contexts.
 *
 * @param level Active collection level.
 * @param num_workers Number of worker threads/contexts to allocate.
 * @return Collector handle, or `NULL` on allocation failure.
 */
keel_stats_collector_t *keel_stats_collector_create(keel_stats_level_t level,
                                                   size_t num_workers);

/**
 * @brief Destroy the process-wide stats collector and all owned storage.
 *
 * @param collector Collector to destroy.
 * @return
 */
void keel_stats_collector_destroy(keel_stats_collector_t *collector);

/**
 * @brief Return the worker-local stats context owned by a collector.
 *
 * @param collector Global collector.
 * @param worker_id Zero-based worker index.
 * @return Context pointer, or `NULL` if unavailable or out of range.
 */
keel_stats_ctx_t *keel_stats_collector_get_ctx(keel_stats_collector_t *collector,
                                              uint32_t worker_id);

/* ============================================================================
 * Aggregate Snapshot
 * ============================================================================
 * Read-side API: aggregate all per-worker counters into a single snapshot.
 * This is called from the admin thread or periodic timer, never from the
 * hot path.
 */

typedef struct keel_stats_snapshot {
    /* Aggregated basic counters (sum across all workers) */
    keel_stats_basic_t       basic;

    /* Aggregated histograms (merged) */
    keel_stats_extended_t    extended;

    /* System stats (copy of last sample) */
    keel_stats_system_t      system;

    /* Function-level instrumentation (aggregated) */
    keel_instr_snapshot_t    instr;

    /* Meta */
    size_t                  num_workers;
    keel_stats_level_t       level;
    int64_t                 snapshot_time_ns;
    int64_t                 uptime_ns;
} keel_stats_snapshot_t;

/**
 * @brief Aggregate per-worker state into a process-level snapshot.
 *
 * @param collector Global collector.
 * @param snap [out] Destination snapshot.
 * @return
 */
void keel_stats_snapshot_take(keel_stats_collector_t *collector,
                             keel_stats_snapshot_t *snap);

/**
 * @brief Reset counters, gauges, histograms, and probes across all workers.
 *
 * @param collector Global collector to reset.
 * @return
 */
void keel_stats_collector_reset(keel_stats_collector_t *collector);

/* ============================================================================
 * System Stats Sampling (L3)
 * ============================================================================ */

/**
 * @brief Sample system-level process metrics into the collector's L3 snapshot.
 *
 * @param collector Global collector.
 * @return
 */
void keel_stats_sample_system(keel_stats_collector_t *collector);

/**
 * @brief Set the runtime category mask that controls L3 `/proc` samplers.
 *
 * @param collector Global collector.
 * @param mask Bitmask of `KEEL_STAT_SYS_*` categories.
 * @return
 */
void keel_stats_set_system_probe_mask(keel_stats_collector_t *collector,
                                      uint32_t mask);

/**
 * @brief Return the currently enabled L3 system-sampling category mask.
 */
uint32_t keel_stats_get_system_probe_mask(const keel_stats_collector_t *collector);

/* ============================================================================
 * Stats-Level Parsing
 * ============================================================================ */

/**
 * @brief Parse a configuration string into a stats level enum.
 *
 * @param str Level string, case-insensitive.
 * @return Parsed level, or `KEEL_STATS_OFF` when unrecognized.
 */
keel_stats_level_t keel_stats_level_from_str(const char *str);

/**
 * @brief Return the canonical string representation of a stats level.
 */
const char *keel_stats_level_to_str(keel_stats_level_t level);

/* ============================================================================
 * Monotonic Time Helper
 * ============================================================================ */

/**
 * @brief Return a monotonic nanosecond timestamp for instrumentation timing.
 */
int64_t keel_stats_now_ns(void);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_STATS_H */
