/**
 * @file stats.c
 * @brief Cold-path implementation for KEEL's multi-level instrumentation framework.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This module handles the parts of statistics collection that do not belong on
 * the hot I/O path: histogram aggregation, process-wide snapshot construction,
 * collector lifecycle, and optional Linux `/proc` sampling. The hot-path counter
 * and latency-recording macros remain inline in `stats.h` so workers can update
 * their own contexts with minimal indirection.
 */

#include "keel/core/stats.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"

#include <string.h>
#include <strings.h>
#include <time.h>
#include <stdio.h>
#include <unistd.h>
#include <dirent.h>

/* ============================================================================
 * Monotonic Clock
 * ============================================================================ */

/**
 * @brief Return a monotonic nanosecond timestamp for instrumentation timing.
 */
int64_t keel_stats_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * KEEL_NS_PER_SEC + ts.tv_nsec;
}

/* ============================================================================
 * Histogram
 * ============================================================================ */

/**
 * @brief Map a value onto the histogram's logarithmic bucket index.
 *
 * The log2 layout compresses wide latency ranges into a small fixed array while
 * preserving fine resolution for the low-latency region where most operational
 * regressions first become visible.
 *
 * @param val Sample value.
 * @return Bucket index in the range `[0, KEEL_HISTOGRAM_BUCKETS)`. 
 */
static inline unsigned hist_bucket(uint64_t val)
{
    if (val == 0) return 0;
    unsigned bits = (unsigned)(63 - __builtin_clzll(val));
    return bits < KEEL_HISTOGRAM_BUCKETS ? bits : KEEL_HISTOGRAM_BUCKETS - 1;
}

/**
 * @brief Record one sample into a histogram using relaxed atomic updates.
 *
 * The min/max updates use short CAS loops because they are rare compared with the
 * common bucket/count/sum increments and do not need sequential consistency.
 *
 * @param h Histogram to update.
 * @param val Sample value.
 * @return
 */
void keel_histogram_record(keel_histogram_t *h, uint64_t val)
{
    unsigned idx = hist_bucket(val);
    atomic_fetch_add_explicit(&h->buckets[idx], 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&h->count, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&h->sum, val, memory_order_relaxed);

    /* Update min — CAS loop (rare contention, relaxed is fine) */
    uint64_t cur_min = atomic_load_explicit(&h->min_val, memory_order_relaxed);
    while (val < cur_min) {
        if (atomic_compare_exchange_weak_explicit(
                &h->min_val, &cur_min, val,
                memory_order_relaxed, memory_order_relaxed))
            break;
    }

    /* Update max — CAS loop */
    uint64_t cur_max = atomic_load_explicit(&h->max_val, memory_order_relaxed);
    while (val > cur_max) {
        if (atomic_compare_exchange_weak_explicit(
                &h->max_val, &cur_max, val,
                memory_order_relaxed, memory_order_relaxed))
            break;
    }
}

/**
 * @brief Reset histogram buckets and summary fields to the empty baseline.
 */
void keel_histogram_reset(keel_histogram_t *h)
{
    for (unsigned i = 0; i < KEEL_HISTOGRAM_BUCKETS; i++)
        atomic_store_explicit(&h->buckets[i], 0, memory_order_relaxed);
    atomic_store_explicit(&h->count, 0, memory_order_relaxed);
    atomic_store_explicit(&h->sum, 0, memory_order_relaxed);
    atomic_store_explicit(&h->min_val, UINT64_MAX, memory_order_relaxed);
    atomic_store_explicit(&h->max_val, 0, memory_order_relaxed);
}

/**
 * @brief Take an approximate point-in-time snapshot of a histogram.
 *
 * Reads are relaxed and therefore not perfectly synchronized with concurrent
 * writers, but the result is sufficient for observability and percentile
 * estimation.
 */
void keel_histogram_snapshot(const keel_histogram_t *h,
                            keel_histogram_snapshot_t *snap)
{
    /* Relaxed loads — snapshot is approximate but good enough for metrics */
    const atomic_uint_fast64_t *buckets =
        (const atomic_uint_fast64_t *)(uintptr_t)h->buckets;
    const atomic_uint_fast64_t *cnt =
        (const atomic_uint_fast64_t *)(uintptr_t)&h->count;
    const atomic_uint_fast64_t *s =
        (const atomic_uint_fast64_t *)(uintptr_t)&h->sum;
    const atomic_uint_fast64_t *mn =
        (const atomic_uint_fast64_t *)(uintptr_t)&h->min_val;
    const atomic_uint_fast64_t *mx =
        (const atomic_uint_fast64_t *)(uintptr_t)&h->max_val;

    for (unsigned i = 0; i < KEEL_HISTOGRAM_BUCKETS; i++)
        snap->buckets[i] = atomic_load_explicit(&buckets[i], memory_order_relaxed);
    snap->count   = atomic_load_explicit(cnt, memory_order_relaxed);
    snap->sum     = atomic_load_explicit(s, memory_order_relaxed);
    snap->min_val = atomic_load_explicit(mn, memory_order_relaxed);
    snap->max_val = atomic_load_explicit(mx, memory_order_relaxed);
}

/**
 * @brief Estimate a percentile from the histogram snapshot.
 *
 * The return value is the midpoint of the bucket where cumulative count reaches
 * the threshold, which keeps the computation cheap while remaining directionally
 * useful for operational dashboards.
 */
uint64_t keel_histogram_percentile(const keel_histogram_t *h, double pct)
{
    keel_histogram_snapshot_t snap;
    keel_histogram_snapshot(h, &snap);

    if (snap.count == 0) return 0;

    uint64_t threshold = (uint64_t)((double)snap.count * pct);
    uint64_t cumulative = 0;

    for (unsigned i = 0; i < KEEL_HISTOGRAM_BUCKETS; i++) {
        cumulative += snap.buckets[i];
        if (cumulative >= threshold) {
            /* Return midpoint of bucket range [2^i, 2^(i+1)) */
            if (i == 0) return 1;
            return (1ULL << i) + (1ULL << (i - 1));
        }
    }
    return snap.max_val;
}

/* ============================================================================
 * Histogram Merge Helper (for aggregation)
 * ============================================================================ */

/**
 * @brief Merge one worker histogram into an aggregate destination histogram.
 */
static void histogram_merge(keel_histogram_t *dst, const keel_histogram_t *src)
{
    for (unsigned i = 0; i < KEEL_HISTOGRAM_BUCKETS; i++) {
        uint64_t v = atomic_load_explicit(
            (atomic_uint_fast64_t *)(uintptr_t)&src->buckets[i],
            memory_order_relaxed);
        atomic_fetch_add_explicit(&dst->buckets[i], v, memory_order_relaxed);
    }

    uint64_t cnt = atomic_load_explicit(
        (atomic_uint_fast64_t *)(uintptr_t)&src->count, memory_order_relaxed);
    atomic_fetch_add_explicit(&dst->count, cnt, memory_order_relaxed);

    uint64_t s = atomic_load_explicit(
        (atomic_uint_fast64_t *)(uintptr_t)&src->sum, memory_order_relaxed);
    atomic_fetch_add_explicit(&dst->sum, s, memory_order_relaxed);

    /* Merge min */
    uint64_t src_min = atomic_load_explicit(
        (atomic_uint_fast64_t *)(uintptr_t)&src->min_val, memory_order_relaxed);
    uint64_t dst_min = atomic_load_explicit(&dst->min_val, memory_order_relaxed);
    if (src_min < dst_min)
        atomic_store_explicit(&dst->min_val, src_min, memory_order_relaxed);

    /* Merge max */
    uint64_t src_max = atomic_load_explicit(
        (atomic_uint_fast64_t *)(uintptr_t)&src->max_val, memory_order_relaxed);
    uint64_t dst_max = atomic_load_explicit(&dst->max_val, memory_order_relaxed);
    if (src_max > dst_max)
        atomic_store_explicit(&dst->max_val, src_max, memory_order_relaxed);
}

/* ============================================================================
 * Counter/Gauge Aggregate Helpers
 * ============================================================================ */

/**
 * @brief Add one counter's current value into an aggregate counter.
 */
static void counter_add_to(keel_counter_t *dst, const keel_counter_t *src)
{
    uint64_t v = keel_counter_get(src);
    keel_counter_add(dst, v);
}

/**
 * @brief Add one gauge's current value into an aggregate gauge.
 *
 * Aggregated gauges are useful for process-wide occupancy views, even though they
 * do not represent an atomic instant across all workers.
 */
static void gauge_add_to(keel_gauge_t *dst, const keel_gauge_t *src)
{
    int64_t v = keel_gauge_get(src);
    atomic_fetch_add_explicit(&dst->value, v, memory_order_relaxed);
}

/* ============================================================================
 * Collector Lifecycle
 * ============================================================================ */

/**
 * @brief Allocate the process-wide collector and initialize per-worker contexts.
 */
keel_stats_collector_t *keel_stats_collector_create(keel_stats_level_t level,
                                                   size_t num_workers)
{
    keel_stats_collector_t *c = keel_calloc(1, sizeof(*c));
    if (!c) return NULL;

    c->level       = level;
    c->num_workers = num_workers;
    c->start_time_ns = keel_stats_now_ns();
    c->system_probe_mask = KEEL_STAT_SYS_ALL;

    if (level >= KEEL_STATS_BASIC) {
        size_t contexts_size = num_workers * sizeof(keel_stats_ctx_t);
        c->contexts = keel_aligned_alloc(64, contexts_size);
        if (!c->contexts) {
            keel_free(c);
            return NULL;
        }
        memset(c->contexts, 0, contexts_size);

        for (size_t i = 0; i < num_workers; i++) {
            c->contexts[i].worker_id = (uint32_t)i;
            c->contexts[i].level     = level;

            /* Initialize histogram min to UINT64_MAX so first record wins */
            if (level >= KEEL_STATS_EXTENDED) {
                keel_histogram_reset(&c->contexts[i].extended.query_latency_ns);
                keel_histogram_reset(&c->contexts[i].extended.backend_latency_ns);
                keel_histogram_reset(&c->contexts[i].extended.connect_latency_ns);
                keel_histogram_reset(&c->contexts[i].extended.session_duration_ns);
                keel_histogram_reset(&c->contexts[i].extended.wait_latency_ns);
            }

            /* Initialize instrumentation probes (mask set later by config) */
            keel_instr_ctx_init(&c->contexts[i].instr, KEEL_INSTR_CAT_NONE);
        }
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_STATS, "Stats collector created: level=%s workers=%zu",
                 keel_stats_level_to_str(level), num_workers);

    return c;
}

/**
 * @brief Destroy the collector and release owned memory.
 */
void keel_stats_collector_destroy(keel_stats_collector_t *collector)
{
    if (!collector) return;

    KEEL_LOG_INFO(KEEL_LOG_CAT_STATS, "Stats collector destroyed");

    keel_aligned_free(collector->contexts);
    keel_free(collector);
}

/**
 * @brief Return the worker-local context indexed by `worker_id`.
 */
keel_stats_ctx_t *keel_stats_collector_get_ctx(keel_stats_collector_t *collector,
                                              uint32_t worker_id)
{
    if (!collector || !collector->contexts) return NULL;
    if (worker_id >= collector->num_workers) return NULL;
    return &collector->contexts[worker_id];
}

/* ============================================================================
 * Aggregate Snapshot
 * ============================================================================ */

/**
 * @brief Aggregate per-worker counters, histograms, probes, and system state into one snapshot.
 *
 * This is intentionally a read-mostly, approximate operation. The snapshot is not
 * globally atomic across workers, but it gives a coherent operational summary at
 * a fraction of the cost of pausing or synchronizing live threads.
 */
void keel_stats_snapshot_take(keel_stats_collector_t *collector,
                             keel_stats_snapshot_t *snap)
{
    memset(snap, 0, sizeof(*snap));

    if (!collector) return;

    snap->level       = collector->level;
    snap->num_workers = collector->num_workers;
    snap->snapshot_time_ns = keel_stats_now_ns();
    snap->uptime_ns   = snap->snapshot_time_ns - collector->start_time_ns;

    if (collector->level < KEEL_STATS_BASIC || !collector->contexts)
        return;

    /* Initialize instrumentation snapshot min values */
    for (int p = 0; p < KEEL_INSTR__COUNT; p++) {
        snap->instr.probes[p].min_ns = UINT64_MAX;
        snap->instr.probes[p].max_ns = 0;
        snap->instr.probes[p].call_count = 0;
        snap->instr.probes[p].total_ns = 0;
    }

    /* Initialize histogram min values for merge */
    if (collector->level >= KEEL_STATS_EXTENDED) {
        keel_histogram_reset(&snap->extended.query_latency_ns);
        keel_histogram_reset(&snap->extended.backend_latency_ns);
        keel_histogram_reset(&snap->extended.connect_latency_ns);
        keel_histogram_reset(&snap->extended.session_duration_ns);
        keel_histogram_reset(&snap->extended.wait_latency_ns);
    }

    for (size_t i = 0; i < collector->num_workers; i++) {
        const keel_stats_basic_t *src = &collector->contexts[i].basic;
        keel_stats_basic_t *dst = &snap->basic;

        /* Pool */
        counter_add_to(&dst->pool_borrows,  &src->pool_borrows);
        counter_add_to(&dst->pool_returns,  &src->pool_returns);
        counter_add_to(&dst->pool_creates,  &src->pool_creates);
        counter_add_to(&dst->pool_destroys, &src->pool_destroys);
        counter_add_to(&dst->pool_hits,     &src->pool_hits);
        counter_add_to(&dst->pool_misses,   &src->pool_misses);

        /* Sessions */
        counter_add_to(&dst->sessions_created, &src->sessions_created);
        counter_add_to(&dst->sessions_closed,  &src->sessions_closed);
        gauge_add_to(&dst->sessions_active, &src->sessions_active);
        if (src->sessions_peak > dst->sessions_peak)
            dst->sessions_peak = src->sessions_peak;

        /* Queries */
        counter_add_to(&dst->queries_total,  &src->queries_total);
        counter_add_to(&dst->queries_read,   &src->queries_read);
        counter_add_to(&dst->queries_write,  &src->queries_write);
        counter_add_to(&dst->queries_tx,     &src->queries_tx);

        /* Errors */
        counter_add_to(&dst->errors_auth,    &src->errors_auth);
        counter_add_to(&dst->errors_proto,   &src->errors_proto);
        counter_add_to(&dst->errors_backend, &src->errors_backend);
        counter_add_to(&dst->errors_timeout, &src->errors_timeout);
        counter_add_to(&dst->errors_total,   &src->errors_total);

        /* I/O */
        counter_add_to(&dst->bytes_recv,         &src->bytes_recv);
        counter_add_to(&dst->bytes_sent,         &src->bytes_sent);
        counter_add_to(&dst->bytes_backend_recv, &src->bytes_backend_recv);
        counter_add_to(&dst->bytes_backend_sent, &src->bytes_backend_sent);
        counter_add_to(&dst->bytes_spliced,      &src->bytes_spliced);

        /* Reactor */
        counter_add_to(&dst->loop_iterations, &src->loop_iterations);
        counter_add_to(&dst->ops_submitted,   &src->ops_submitted);
        counter_add_to(&dst->ops_completed,   &src->ops_completed);

        /* Multiplexing safety */
        counter_add_to(&dst->discard_all_count,       &src->discard_all_count);
        counter_add_to(&dst->state_sync_count,        &src->state_sync_count);
        counter_add_to(&dst->quarantine_count,        &src->quarantine_count);
        counter_add_to(&dst->sticky_primary_hits,     &src->sticky_primary_hits);
        counter_add_to(&dst->copy_pause_count,        &src->copy_pause_count);
        counter_add_to(&dst->prepared_hardpin_count,  &src->prepared_hardpin_count);
        counter_add_to(&dst->backend_error_transient, &src->backend_error_transient);
        counter_add_to(&dst->backend_error_fatal,     &src->backend_error_fatal);
        counter_add_to(&dst->copy_bytes_total,        &src->copy_bytes_total);
        counter_add_to(&dst->notify_relayed,          &src->notify_relayed);
        counter_add_to(&dst->osc_sessions_detected,   &src->osc_sessions_detected);
        gauge_add_to(&dst->sessions_pinned,   &src->sessions_pinned);
        gauge_add_to(&dst->backends_cleaning, &src->backends_cleaning);

        /* Connection migration */
        counter_add_to(&dst->migrations_sent,     &src->migrations_sent);
        counter_add_to(&dst->migrations_received, &src->migrations_received);

        /* Protocol-path wait instrumentation */
        counter_add_to(&dst->flow_wait_pool_events,      &src->flow_wait_pool_events);
        counter_add_to(&dst->flow_wait_pool_ns_total,    &src->flow_wait_pool_ns_total);
        counter_add_to(&dst->flow_wait_backend_events,   &src->flow_wait_backend_events);
        counter_add_to(&dst->flow_wait_backend_ns_total, &src->flow_wait_backend_ns_total);
        counter_add_to(&dst->flow_wait_backend_query_events,   &src->flow_wait_backend_query_events);
        counter_add_to(&dst->flow_wait_backend_query_ns_total, &src->flow_wait_backend_query_ns_total);
        counter_add_to(&dst->flow_wait_backend_query_exec_ns_total, &src->flow_wait_backend_query_exec_ns_total);
        counter_add_to(&dst->flow_wait_backend_query_io_ns_total,   &src->flow_wait_backend_query_io_ns_total);
        counter_add_to(&dst->flow_wait_backend_query_io_reactor_ns_total,
                   &src->flow_wait_backend_query_io_reactor_ns_total);
        counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_ns_total,
               &src->flow_wait_backend_query_io_reactor_ready_ns_total);
         counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_wakeup_ns_total,
             &src->flow_wait_backend_query_io_reactor_ready_wakeup_ns_total);
         counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_sched_ns_total,
             &src->flow_wait_backend_query_io_reactor_ready_sched_ns_total);
         counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_sched_head_ns_total,
             &src->flow_wait_backend_query_io_reactor_ready_sched_head_ns_total);
         counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_sched_tail_ns_total,
             &src->flow_wait_backend_query_io_reactor_ready_sched_tail_ns_total);
         counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_sched_batch_size_sum,
             &src->flow_wait_backend_query_io_reactor_ready_sched_batch_size_sum);
         counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_sched_batch_index_sum,
             &src->flow_wait_backend_query_io_reactor_ready_sched_batch_index_sum);
         counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_sched_batch_size_1_events,
             &src->flow_wait_backend_query_io_reactor_ready_sched_batch_size_1_events);
         counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_sched_batch_size_2_events,
             &src->flow_wait_backend_query_io_reactor_ready_sched_batch_size_2_events);
         counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_sched_batch_size_3_events,
             &src->flow_wait_backend_query_io_reactor_ready_sched_batch_size_3_events);
         counter_add_to(&dst->flow_wait_backend_query_io_reactor_ready_sched_batch_size_4p_events,
             &src->flow_wait_backend_query_io_reactor_ready_sched_batch_size_4p_events);
        counter_add_to(&dst->flow_wait_backend_query_io_reactor_dispatch_ns_total,
               &src->flow_wait_backend_query_io_reactor_dispatch_ns_total);
        counter_add_to(&dst->flow_wait_backend_query_io_service_ns_total,
                   &src->flow_wait_backend_query_io_service_ns_total);
        counter_add_to(&dst->flow_wait_backend_query_framing_ns_total,
                   &src->flow_wait_backend_query_framing_ns_total);
        counter_add_to(&dst->flow_wait_backend_query_deferred_send_events,
                   &src->flow_wait_backend_query_deferred_send_events);
        counter_add_to(&dst->flow_wait_backend_query_deferred_send_ns_total,
                   &src->flow_wait_backend_query_deferred_send_ns_total);
        counter_add_to(&dst->flow_wait_backend_replay_events,  &src->flow_wait_backend_replay_events);
        counter_add_to(&dst->flow_wait_backend_replay_ns_total,&src->flow_wait_backend_replay_ns_total);
        counter_add_to(&dst->flow_wait_backend_discard_events, &src->flow_wait_backend_discard_events);
        counter_add_to(&dst->flow_wait_backend_discard_ns_total,&src->flow_wait_backend_discard_ns_total);

        /* Pool queue diagnostics */
        counter_add_to(&dst->pool_wait_queue_enqueued,     &src->pool_wait_queue_enqueued);
        counter_add_to(&dst->pool_wait_queue_full_rejects, &src->pool_wait_queue_full_rejects);
        counter_add_to(&dst->pool_wait_resume_success,     &src->pool_wait_resume_success);
        counter_add_to(&dst->pool_wait_resume_requeues,    &src->pool_wait_resume_requeues);
        counter_add_to(&dst->pool_wait_timeout_events,     &src->pool_wait_timeout_events);

        /* Protocol health observability */
        counter_add_to(&dst->proxy_state_desync_total,          &src->proxy_state_desync_total);
        counter_add_to(&dst->proxy_orphaned_transactions_total, &src->proxy_orphaned_transactions_total);
        counter_add_to(&dst->proxy_backend_reuse_failure_total, &src->proxy_backend_reuse_failure_total);
        counter_add_to(&dst->proxy_io_uring_sq_overflow_total,  &src->proxy_io_uring_sq_overflow_total);
        gauge_add_to(&dst->proxy_buffer_pool_utilization_bytes, &src->proxy_buffer_pool_utilization_bytes);
        if (keel_gauge_get(&src->proxy_connection_age_seconds) > keel_gauge_get(&dst->proxy_connection_age_seconds))
            keel_gauge_set(&dst->proxy_connection_age_seconds, keel_gauge_get(&src->proxy_connection_age_seconds));
        if (keel_gauge_get(&src->proxy_heartbeat_last_ns) > keel_gauge_get(&dst->proxy_heartbeat_last_ns))
            keel_gauge_set(&dst->proxy_heartbeat_last_ns, keel_gauge_get(&src->proxy_heartbeat_last_ns));

        /* L2: Merge histograms */
        if (collector->level >= KEEL_STATS_EXTENDED) {
            const keel_stats_extended_t *esrc = &collector->contexts[i].extended;
            keel_stats_extended_t *edst = &snap->extended;

            histogram_merge(&edst->query_latency_ns,    &esrc->query_latency_ns);
            histogram_merge(&edst->backend_latency_ns,  &esrc->backend_latency_ns);
            histogram_merge(&edst->connect_latency_ns,  &esrc->connect_latency_ns);
            histogram_merge(&edst->session_duration_ns, &esrc->session_duration_ns);
            histogram_merge(&edst->wait_latency_ns,     &esrc->wait_latency_ns);
        }

        /* Merge instrumentation probes */
        {
            const keel_instr_ctx_t *isrc = &collector->contexts[i].instr;
            for (int p = 0; p < KEEL_INSTR__COUNT; p++) {
                keel_instr_probe_merge(&snap->instr.probes[p], &isrc->probes[p]);
            }
        }
    }

    /* L3: Copy system stats */
    if (collector->level >= KEEL_STATS_SYSTEM)
        snap->system = collector->system;
}

/**
 * @brief Reset worker-local counters, histograms, and instrumentation probes.
 */
void keel_stats_collector_reset(keel_stats_collector_t *collector)
{
    if (!collector || !collector->contexts) return;

    for (size_t i = 0; i < collector->num_workers; i++) {
        keel_stats_ctx_t *ctx = &collector->contexts[i];

        /* Zero out basic counters */
        memset(&ctx->basic, 0, sizeof(ctx->basic));

        /* Reset histograms */
        if (collector->level >= KEEL_STATS_EXTENDED) {
            keel_histogram_reset(&ctx->extended.query_latency_ns);
            keel_histogram_reset(&ctx->extended.backend_latency_ns);
            keel_histogram_reset(&ctx->extended.connect_latency_ns);
            keel_histogram_reset(&ctx->extended.session_duration_ns);
            keel_histogram_reset(&ctx->extended.wait_latency_ns);
        }

        /* Reset instrumentation probes */
        keel_instr_ctx_reset(&ctx->instr);
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_STATS, "Stats counters reset");
}

/* ============================================================================
 * System Stats Sampling (L3)
 * ============================================================================ */

/**
 * @brief Refresh the collector's system-level metrics from Linux `/proc` sources.
 *
 * Sampling is category-masked at runtime so operators can disable more expensive
 * samplers without turning off the whole subsystem. Non-Linux platforms degrade
 * gracefully to stubs.
 */
void keel_stats_sample_system(keel_stats_collector_t *collector)
{
    if (!collector || collector->level < KEEL_STATS_SYSTEM) return;

    keel_stats_system_t *sys = &collector->system;
    int64_t now = keel_stats_now_ns();
    uint32_t mask = collector->system_probe_mask;
    sys->probe_mask = mask;

#if defined(KEEL_PLATFORM_LINUX) || defined(__linux__)
    /* CPU percentages are derived from deltas between successive `/proc/self/stat`
     * snapshots, not from a single instantaneous reading. */
    if (mask & KEEL_STAT_SYS_CPU) {
        FILE *f = fopen("/proc/self/stat", "r");
        if (f) {
            /* Skip first 13 fields (pid, comm, state, ..., cminflt) */
            unsigned long utime = 0, stime = 0;
            int matched = fscanf(f,
                "%*d %*s %*c %*d %*d %*d %*d %*d %*u "   /* 1-9 */
                "%*u %*u %*u %*u "                        /* 10-13 */
                "%lu %lu",                                 /* 14=utime, 15=stime */
                &utime, &stime);
            fclose(f);

            if (matched == 2 && collector->prev_sample_ns > 0) {
                long ticks = sysconf(_SC_CLK_TCK);
                if (ticks <= 0) ticks = 100;
                double elapsed_sec =
                    (double)(now - collector->prev_sample_ns) / 1.0e9;
                if (elapsed_sec > 0.01) {
                    double delta_u = (double)(utime - collector->prev_utime)
                                     / (double)ticks;
                    double delta_s = (double)(stime - collector->prev_stime)
                                     / (double)ticks;
                    sys->cpu_user_pct = delta_u / elapsed_sec * 100.0;
                    sys->cpu_sys_pct  = delta_s / elapsed_sec * 100.0;
                }
            }
            collector->prev_utime = utime;
            collector->prev_stime = stime;
        }
    }

    /* Memory and context-switch figures are read from `/proc/self/status`. */
    if (mask & KEEL_STAT_SYS_MEMORY) {
        FILE *f = fopen("/proc/self/status", "r");
        if (f) {
            char line[256];
            while (fgets(line, sizeof(line), f)) {
                unsigned long val;
                if (sscanf(line, "VmRSS: %lu kB", &val) == 1)
                    sys->rss_bytes = val * 1024UL;
                else if (sscanf(line, "VmSize: %lu kB", &val) == 1)
                    sys->vm_bytes = val * 1024UL;
                else if (sscanf(line, "voluntary_ctxt_switches: %lu", &val) == 1)
                    sys->ctx_switches_vol = val;
                else if (sscanf(line, "nonvoluntary_ctxt_switches: %lu", &val) == 1)
                    sys->ctx_switches_inv = val;
            }
            fclose(f);
        }
    }

    /* --- Context-switch counters from /proc/self/status --- */
    if (mask & KEEL_STAT_SYS_CPU) {
        FILE *f = fopen("/proc/self/status", "r");
        if (f) {
            char line[256];
            while (fgets(line, sizeof(line), f)) {
                unsigned long val;
                if (sscanf(line, "voluntary_ctxt_switches: %lu", &val) == 1)
                    sys->ctx_switches_vol = val;
                else if (sscanf(line, "nonvoluntary_ctxt_switches: %lu", &val) == 1)
                    sys->ctx_switches_inv = val;
            }
            fclose(f);
        }
    }

    /* File-descriptor counts require directory enumeration; the configured soft
     * limit is pulled separately from `/proc/self/limits`. */
    if (mask & KEEL_STAT_SYS_FD) {
        DIR *d = opendir("/proc/self/fd");
        if (d) {
            uint32_t count = 0;
            struct dirent *ent;
            while ((ent = readdir(d)) != NULL) {
                if (ent->d_name[0] == '.') continue;
                count++;
            }
            closedir(d);
            sys->fd_open = count;
        }

        /* Simpler: read /proc/self/limits for fd limit */
        FILE *f = fopen("/proc/self/limits", "r");
        if (f) {
            char line[256];
            while (fgets(line, sizeof(line), f)) {
                unsigned long soft, hard;
                if (strstr(line, "Max open files") &&
                    sscanf(line, "Max open files %lu %lu", &soft, &hard) >= 1) {
                    sys->fd_limit = (uint32_t)soft;
                }
            }
            fclose(f);
        }
    }

    /* --- Disk I/O from /proc/self/io --- */
    if (mask & KEEL_STAT_SYS_DISK) {
        FILE *f = fopen("/proc/self/io", "r");
        if (f) {
            char line[256];
            while (fgets(line, sizeof(line), f)) {
                unsigned long long val;
                if (sscanf(line, "read_bytes: %llu", &val) == 1)
                    sys->disk_read_bytes = (uint64_t)val;
                else if (sscanf(line, "write_bytes: %llu", &val) == 1)
                    sys->disk_write_bytes = (uint64_t)val;
            }
            fclose(f);
        }
    }

    /* --- Network I/O from /proc/net/dev (aggregate interfaces) --- */
    if (mask & KEEL_STAT_SYS_NETWORK) {
        FILE *f = fopen("/proc/net/dev", "r");
        if (f) {
            char line[512];
            uint64_t rx_sum = 0;
            uint64_t tx_sum = 0;
            int line_no = 0;
            while (fgets(line, sizeof(line), f)) {
                line_no++;
                if (line_no <= 2) continue; /* headers */

                char ifname[32];
                unsigned long long rx = 0, tx = 0;
                int n = sscanf(line,
                               " %31[^:]: %llu %*u %*u %*u %*u %*u %*u %*u %llu",
                               ifname, &rx, &tx);
                if (n == 3) {
                    rx_sum += (uint64_t)rx;
                    tx_sum += (uint64_t)tx;
                }
            }
            fclose(f);
            sys->net_rx_bytes = rx_sum;
            sys->net_tx_bytes = tx_sum;
        }
    }
#else
    /* Non-Linux: basic stubs */
    (void)sys;
#endif

    sys->sampled_at_ns = now;
    collector->prev_sample_ns = now;
}

/**
 * @brief Store the runtime category mask for L3 system sampling.
 */
void keel_stats_set_system_probe_mask(keel_stats_collector_t *collector,
                                      uint32_t mask)
{
    if (!collector) return;
    collector->system_probe_mask = mask & KEEL_STAT_SYS_ALL;
}

/**
 * @brief Return the currently configured L3 system-sampling category mask.
 */
uint32_t keel_stats_get_system_probe_mask(const keel_stats_collector_t *collector)
{
    if (!collector) return 0;
    return collector->system_probe_mask;
}

/* ============================================================================
 * Level String Conversion
 * ============================================================================ */

static const char *level_names[] = {
    [KEEL_STATS_OFF]      = "off",
    [KEEL_STATS_BASIC]    = "basic",
    [KEEL_STATS_EXTENDED] = "extended",
    [KEEL_STATS_SYSTEM]   = "system",
    [KEEL_STATS_TRACE]    = "trace",
    [KEEL_STATS_FULL]     = "full",
};

/**
 * @brief Parse a case-insensitive stats level string into its enum value.
 */
keel_stats_level_t keel_stats_level_from_str(const char *str)
{
    if (!str) return KEEL_STATS_OFF;
    for (int i = 0; i <= KEEL_STATS_FULL; i++) {
        if (strcasecmp(str, level_names[i]) == 0)
            return (keel_stats_level_t)i;
    }
    return KEEL_STATS_OFF;
}

/**
 * @brief Return the canonical lowercase name for a stats level.
 */
const char *keel_stats_level_to_str(keel_stats_level_t level)
{
    if (level > KEEL_STATS_FULL) return "unknown";
    return level_names[level];
}
