/**
 * @file keel_otlp_aggregator.c
 * @brief Stats → OTLP snapshot conversion + periodic submit-loop.
 */

#include "keel_otlp_aggregator.h"

#include "keel/core/stats.h"
#include "keel/mem/mem.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

/* ============================================================================
 * Conversion: keel_stats_snapshot_t -> keel_otlp_snapshot_t
 * ============================================================================ */

static void put_metric(keel_otlp_snapshot_t* out,
                       const char* name,
                       uint64_t value)
{
    if (out->metric_count >= KEEL_OTLP_MAX_METRICS) return;
    keel_otlp_metric_sample_t* m = &out->metrics[out->metric_count++];
    size_t n = strlen(name);
    if (n >= KEEL_OTLP_MAX_NAME_LEN) n = KEEL_OTLP_MAX_NAME_LEN - 1;
    memcpy(m->name, name, n);
    m->name[n] = '\0';
    m->value = value;
}

static uint64_t clamp_gauge(int64_t v) {
    return v < 0 ? 0u : (uint64_t)v;
}

int keel_otlp_snapshot_from_stats(const keel_stats_snapshot_t* in,
                                  uint64_t start_time_unix_nano,
                                  uint64_t time_unix_nano,
                                  keel_otlp_snapshot_t* out)
{
    if (!in || !out) return -1;

    memset(out, 0, sizeof(*out));
    out->start_time_unix_nano = start_time_unix_nano;
    out->time_unix_nano       = time_unix_nano;

    const keel_stats_basic_t* b = &in->basic;

    /* Sessions */
    put_metric(out, "keel_sessions_created_total",
               keel_counter_get(&b->sessions_created));
    put_metric(out, "keel_sessions_closed_total",
               keel_counter_get(&b->sessions_closed));
    put_metric(out, "keel_sessions_active",
               clamp_gauge(keel_gauge_get(&b->sessions_active)));
    put_metric(out, "keel_sessions_pinned",
               clamp_gauge(keel_gauge_get(&b->sessions_pinned)));

    /* Queries */
    put_metric(out, "keel_queries_total",
               keel_counter_get(&b->queries_total));
    put_metric(out, "keel_queries_read_total",
               keel_counter_get(&b->queries_read));
    put_metric(out, "keel_queries_write_total",
               keel_counter_get(&b->queries_write));
    put_metric(out, "keel_queries_tx_total",
               keel_counter_get(&b->queries_tx));

    /* Errors */
    put_metric(out, "keel_errors_total",
               keel_counter_get(&b->errors_total));
    put_metric(out, "keel_errors_auth_total",
               keel_counter_get(&b->errors_auth));
    put_metric(out, "keel_errors_proto_total",
               keel_counter_get(&b->errors_proto));
    put_metric(out, "keel_errors_backend_total",
               keel_counter_get(&b->errors_backend));
    put_metric(out, "keel_errors_timeout_total",
               keel_counter_get(&b->errors_timeout));

    /* Bytes */
    put_metric(out, "keel_bytes_recv_total",
               keel_counter_get(&b->bytes_recv));
    put_metric(out, "keel_bytes_sent_total",
               keel_counter_get(&b->bytes_sent));
    put_metric(out, "keel_bytes_backend_recv_total",
               keel_counter_get(&b->bytes_backend_recv));
    put_metric(out, "keel_bytes_backend_sent_total",
               keel_counter_get(&b->bytes_backend_sent));
    put_metric(out, "keel_bytes_spliced_total",
               keel_counter_get(&b->bytes_spliced));

    /* Pool */
    put_metric(out, "keel_pool_borrows_total",
               keel_counter_get(&b->pool_borrows));
    put_metric(out, "keel_pool_returns_total",
               keel_counter_get(&b->pool_returns));
    put_metric(out, "keel_pool_creates_total",
               keel_counter_get(&b->pool_creates));
    put_metric(out, "keel_pool_destroys_total",
               keel_counter_get(&b->pool_destroys));
    put_metric(out, "keel_pool_hits_total",
               keel_counter_get(&b->pool_hits));
    put_metric(out, "keel_pool_misses_total",
               keel_counter_get(&b->pool_misses));

    /* Reactor */
    put_metric(out, "keel_loop_iterations_total",
               keel_counter_get(&b->loop_iterations));
    put_metric(out, "keel_ops_submitted_total",
               keel_counter_get(&b->ops_submitted));
    put_metric(out, "keel_ops_completed_total",
               keel_counter_get(&b->ops_completed));

    /* Multiplexing safety */
    put_metric(out, "keel_discard_all_total",
               keel_counter_get(&b->discard_all_count));
    put_metric(out, "keel_state_sync_total",
               keel_counter_get(&b->state_sync_count));
    put_metric(out, "keel_backends_cleaning",
               clamp_gauge(keel_gauge_get(&b->backends_cleaning)));

    /* Reason-coded backend close (proposal §28 R1). */
    put_metric(out, "keel_backend_close_dead_idle_total",
               keel_counter_get(&b->backend_close_dead_idle));
    put_metric(out, "keel_backend_close_cleanup_error_total",
               keel_counter_get(&b->backend_close_cleanup_error));
    put_metric(out, "keel_backend_close_cleanup_timeout_total",
               keel_counter_get(&b->backend_close_cleanup_timeout));
    put_metric(out, "keel_backend_close_client_disconnect_total",
               keel_counter_get(&b->backend_close_client_disconnect));
    put_metric(out, "keel_backend_close_io_error_total",
               keel_counter_get(&b->backend_close_io_error));
    put_metric(out, "keel_backend_close_prune_idle_total",
               keel_counter_get(&b->backend_close_prune_idle));
    put_metric(out, "keel_backend_close_prune_aged_total",
               keel_counter_get(&b->backend_close_prune_aged));
    put_metric(out, "keel_backend_close_drain_idle_total",
               keel_counter_get(&b->backend_close_drain_idle));
    put_metric(out, "keel_backend_close_backend_eof_total",
               keel_counter_get(&b->backend_close_backend_eof));
    put_metric(out, "keel_backend_close_connect_failed_total",
               keel_counter_get(&b->backend_close_connect_failed));
    put_metric(out, "keel_backend_close_auth_failed_total",
               keel_counter_get(&b->backend_close_auth_failed));
    put_metric(out, "keel_backend_close_protocol_error_total",
               keel_counter_get(&b->backend_close_protocol_error));
    put_metric(out, "keel_backend_close_sync_error_total",
               keel_counter_get(&b->backend_close_sync_error));
    put_metric(out, "keel_backend_close_stmt_replay_error_total",
               keel_counter_get(&b->backend_close_stmt_replay_error));
    put_metric(out, "keel_backend_close_shutdown_total",
               keel_counter_get(&b->backend_close_shutdown));
    put_metric(out, "keel_backend_close_pool_eviction_total",
               keel_counter_get(&b->backend_close_pool_eviction));

    /* Commit-in-doubt resolution (proposal §28 R2). */
    put_metric(out, "keel_commit_in_doubt_started_total",
               keel_counter_get(&b->commit_in_doubt_started));
    put_metric(out, "keel_commit_in_doubt_resolved_total",
               keel_counter_get(&b->commit_in_doubt_resolved));
    put_metric(out, "keel_commit_in_doubt_failed_total",
               keel_counter_get(&b->commit_in_doubt_failed));
    put_metric(out, "keel_sessions_commit_in_doubt",
               clamp_gauge(keel_gauge_get(&b->sessions_commit_in_doubt)));

    /* Process meta */
    uint64_t uptime_seconds = (in->uptime_ns > 0)
        ? (uint64_t)(in->uptime_ns / 1000000000LL) : 0u;
    put_metric(out, "keel_uptime_seconds", uptime_seconds);
    put_metric(out, "keel_workers", (uint64_t)in->num_workers);

    return 0;
}

/* ============================================================================
 * Aggregator: background thread driving periodic submits
 * ============================================================================ */

struct keel_otlp_aggregator {
    keel_stats_collector_t* collector;
    keel_otlp_exporter_t*   exporter;
    uint32_t                interval_ms;

    pthread_t               thread;
    pthread_mutex_t         mu;
    pthread_cond_t          cv;
    bool                    thread_started;
    _Atomic bool            stop_requested;

    uint64_t                start_time_unix_nano;
    _Atomic uint64_t        ticks;
};

static uint64_t now_unix_nano(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void do_one_tick(keel_otlp_aggregator_t* agg) {
    keel_stats_snapshot_t snap;
    keel_stats_snapshot_take(agg->collector, &snap);

    keel_otlp_snapshot_t osnap;
    keel_otlp_snapshot_from_stats(&snap,
                                  agg->start_time_unix_nano,
                                  now_unix_nano(),
                                  &osnap);
    keel_otlp_exporter_submit(agg->exporter, &osnap);
    atomic_fetch_add_explicit(&agg->ticks, 1, memory_order_relaxed);
}

static void* aggregator_thread_main(void* arg) {
    keel_otlp_aggregator_t* agg = (keel_otlp_aggregator_t*)arg;
    while (!atomic_load_explicit(&agg->stop_requested, memory_order_acquire)) {
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        uint64_t ns = (uint64_t)deadline.tv_nsec
                    + (uint64_t)agg->interval_ms * 1000000ULL;
        deadline.tv_sec  += (time_t)(ns / 1000000000ULL);
        deadline.tv_nsec  = (long)(ns % 1000000000ULL);

        pthread_mutex_lock(&agg->mu);
        while (!atomic_load_explicit(&agg->stop_requested,
                                     memory_order_acquire)) {
            int rc = pthread_cond_timedwait(&agg->cv, &agg->mu, &deadline);
            if (rc == ETIMEDOUT) break;
        }
        pthread_mutex_unlock(&agg->mu);

        if (atomic_load_explicit(&agg->stop_requested, memory_order_acquire))
            break;

        do_one_tick(agg);
    }
    return NULL;
}

keel_otlp_aggregator_t* keel_otlp_aggregator_create(
    keel_stats_collector_t* collector,
    keel_otlp_exporter_t* exporter,
    uint32_t interval_ms)
{
    if (!collector || !exporter || interval_ms == 0) return NULL;

    keel_otlp_aggregator_t* agg = keel_calloc(1, sizeof(*agg));
    if (!agg) return NULL;

    agg->collector   = collector;
    agg->exporter    = exporter;
    agg->interval_ms = interval_ms;

    if (pthread_mutex_init(&agg->mu, NULL) != 0) {
        keel_free(agg);
        return NULL;
    }
    if (pthread_cond_init(&agg->cv, NULL) != 0) {
        pthread_mutex_destroy(&agg->mu);
        keel_free(agg);
        return NULL;
    }

    atomic_store_explicit(&agg->stop_requested, false, memory_order_release);
    atomic_store_explicit(&agg->ticks, 0, memory_order_release);
    return agg;
}

int keel_otlp_aggregator_start(keel_otlp_aggregator_t* agg) {
    if (!agg || agg->thread_started) return -1;
    agg->start_time_unix_nano = now_unix_nano();
    if (pthread_create(&agg->thread, NULL, aggregator_thread_main, agg) != 0)
        return -1;
    agg->thread_started = true;
    return 0;
}

void keel_otlp_aggregator_stop(keel_otlp_aggregator_t* agg) {
    if (!agg || !agg->thread_started) return;
    atomic_store_explicit(&agg->stop_requested, true, memory_order_release);
    pthread_mutex_lock(&agg->mu);
    pthread_cond_broadcast(&agg->cv);
    pthread_mutex_unlock(&agg->mu);
    pthread_join(agg->thread, NULL);
    agg->thread_started = false;
}

void keel_otlp_aggregator_destroy(keel_otlp_aggregator_t* agg) {
    if (!agg) return;
    if (agg->thread_started) keel_otlp_aggregator_stop(agg);
    pthread_cond_destroy(&agg->cv);
    pthread_mutex_destroy(&agg->mu);
    keel_free(agg);
}

uint64_t keel_otlp_aggregator_ticks(const keel_otlp_aggregator_t* agg) {
    if (!agg) return 0;
    /* atomic load on the const handle (single-writer increment elsewhere). */
    _Atomic uint64_t* p = (_Atomic uint64_t*)(uintptr_t)&agg->ticks;
    return atomic_load_explicit(p, memory_order_relaxed);
}
