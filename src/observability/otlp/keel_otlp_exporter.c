/**
 * @file keel_otlp_exporter.c
 * @brief Exporter run loop: drains the snapshot queue, encodes, POSTs,
 *        updates self-stats.
 */
#include "keel_otlp_exporter.h"
#include "keel_otlp_encode.h"
#include "keel_snapshot_queue.h"

#include "keel/mem/mem.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#define KEEL_OTLP_DEFAULT_QUEUE_CAPACITY   4u
#define KEEL_OTLP_DEFAULT_ENCODE_BUF_BYTES (64u * 1024u)
#define KEEL_OTLP_DEFAULT_INTERVAL_MS      5000u

struct keel_otlp_exporter {
    keel_otlp_exporter_config_t cfg;
    keel_otlp_http_t*           http;
    keel_snapshot_queue_t*      queue;
    uint8_t*                    encode_buf;
    size_t                      encode_buf_cap;
    keel_exporter_stats_t       stats;
    pthread_t                   thread;
    atomic_bool                 running;
    atomic_bool                 stop_requested;
};

static uint64_t monotonic_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static uint64_t wall_unix_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

static void dispatch_one(keel_otlp_exporter_t* exp, const keel_otlp_snapshot_t* snap)
{
    atomic_fetch_add(&exp->stats.attempts, 1);

    size_t encoded_len = 0;
    keel_otlp_encode_result_t er = keel_otlp_encode_metrics(
        snap, exp->encode_buf, exp->encode_buf_cap, &encoded_len);

    uint64_t                t0 = monotonic_ns();
    keel_otlp_http_result_t hr;
    if (er != KEEL_OTLP_ENCODE_OK) {
        hr = KEEL_OTLP_HTTP_PROTOCOL_ERROR;
    } else {
        hr = keel_otlp_http_post(exp->http, exp->encode_buf, encoded_len);
        uint32_t attempts_left = exp->cfg.max_retries;
        while ((hr == KEEL_OTLP_HTTP_SERVER_RETRY ||
                hr == KEEL_OTLP_HTTP_TIMEOUT ||
                hr == KEEL_OTLP_HTTP_CONNECT_FAILED) &&
               attempts_left > 0 &&
               !atomic_load(&exp->stop_requested)) {
            hr = keel_otlp_http_post(exp->http, exp->encode_buf, encoded_len);
            attempts_left--;
        }
    }
    uint64_t dur = monotonic_ns() - t0;

    atomic_store(&exp->stats.last_duration_ns, dur);
    atomic_store(&exp->stats.last_status,      (int64_t)hr);

    if (hr == KEEL_OTLP_HTTP_OK) {
        atomic_fetch_add(&exp->stats.successes, 1);
        atomic_store(&exp->stats.last_success_unix_ms, wall_unix_ms());
    } else {
        atomic_fetch_add(&exp->stats.failures, 1);
        if (hr == KEEL_OTLP_HTTP_TIMEOUT)
            atomic_fetch_add(&exp->stats.timeouts, 1);
        atomic_store(&exp->stats.last_failure_unix_ms, wall_unix_ms());
    }
}

static void* exporter_thread_main(void* arg)
{
    keel_otlp_exporter_t* exp = arg;
    while (!atomic_load(&exp->stop_requested)) {
        keel_otlp_snapshot_t snap;
        int r = keel_snapshot_queue_pop(exp->queue, &snap, exp->cfg.interval_ms);
        if (r == 1) {
            dispatch_one(exp, &snap);
        } else if (r < 0) {
            break;
        }
        atomic_store(&exp->stats.queue_depth,
                     (uint64_t)keel_snapshot_queue_depth(exp->queue));
        atomic_store(&exp->stats.dropped,
                     keel_snapshot_queue_dropped_total(exp->queue));
    }
    /* Drain whatever is left without blocking — best-effort flush. */
    for (;;) {
        keel_otlp_snapshot_t snap;
        if (!keel_snapshot_queue_try_pop(exp->queue, &snap))
            break;
        dispatch_one(exp, &snap);
    }
    return NULL;
}

keel_otlp_exporter_t* keel_otlp_exporter_create(const keel_otlp_exporter_config_t* cfg)
{
    if (!cfg)
        return NULL;
    keel_otlp_exporter_t* exp = keel_calloc(1, sizeof(*exp));
    if (!exp)
        return NULL;
    exp->cfg = *cfg;
    if (exp->cfg.interval_ms == 0)
        exp->cfg.interval_ms = KEEL_OTLP_DEFAULT_INTERVAL_MS;
    if (exp->cfg.queue_capacity == 0)
        exp->cfg.queue_capacity = KEEL_OTLP_DEFAULT_QUEUE_CAPACITY;
    if (exp->cfg.encode_buf_bytes == 0)
        exp->cfg.encode_buf_bytes = KEEL_OTLP_DEFAULT_ENCODE_BUF_BYTES;

    exp->http = keel_otlp_http_create(&cfg->http);
    if (!exp->http)
        goto fail;
    exp->queue = keel_snapshot_queue_create(exp->cfg.queue_capacity);
    if (!exp->queue)
        goto fail;
    exp->encode_buf = keel_calloc(1, exp->cfg.encode_buf_bytes);
    if (!exp->encode_buf)
        goto fail;
    exp->encode_buf_cap = exp->cfg.encode_buf_bytes;

    keel_exporter_stats_init(&exp->stats, (uint64_t)exp->cfg.queue_capacity);
    atomic_store(&exp->running, false);
    atomic_store(&exp->stop_requested, false);
    return exp;

fail:
    if (exp->queue) keel_snapshot_queue_destroy(exp->queue);
    if (exp->http)  keel_otlp_http_destroy(exp->http);
    keel_free(exp->encode_buf);
    keel_free(exp);
    return NULL;
}

int keel_otlp_exporter_start(keel_otlp_exporter_t* exp)
{
    if (!exp || atomic_load(&exp->running))
        return -1;
    atomic_store(&exp->stop_requested, false);
    if (pthread_create(&exp->thread, NULL, exporter_thread_main, exp) != 0)
        return -1;
    atomic_store(&exp->running, true);
    return 0;
}

void keel_otlp_exporter_stop(keel_otlp_exporter_t* exp)
{
    if (!exp || !atomic_load(&exp->running))
        return;
    atomic_store(&exp->stop_requested, true);
    keel_snapshot_queue_shutdown(exp->queue);
    pthread_join(exp->thread, NULL);
    atomic_store(&exp->running, false);
}

void keel_otlp_exporter_destroy(keel_otlp_exporter_t* exp)
{
    if (!exp)
        return;
    keel_otlp_exporter_stop(exp);
    if (exp->queue) keel_snapshot_queue_destroy(exp->queue);
    if (exp->http)  keel_otlp_http_destroy(exp->http);
    keel_free(exp->encode_buf);
    keel_free(exp);
}

int keel_otlp_exporter_submit(keel_otlp_exporter_t* exp, const keel_otlp_snapshot_t* snap)
{
    if (!exp || !snap)
        return 0;
    return keel_snapshot_queue_push(exp->queue, snap);
}

void keel_otlp_exporter_self_stats(const keel_otlp_exporter_t* exp, keel_exporter_stats_t* out)
{
    if (!exp || !out)
        return;
    out->attempts             = atomic_load(&exp->stats.attempts);
    out->successes            = atomic_load(&exp->stats.successes);
    out->failures             = atomic_load(&exp->stats.failures);
    out->dropped              = atomic_load(&exp->stats.dropped);
    out->queue_depth          = atomic_load(&exp->stats.queue_depth);
    out->queue_capacity       = atomic_load(&exp->stats.queue_capacity);
    out->timeouts             = atomic_load(&exp->stats.timeouts);
    out->last_duration_ns     = atomic_load(&exp->stats.last_duration_ns);
    out->last_status          = atomic_load(&exp->stats.last_status);
    out->last_success_unix_ms = atomic_load(&exp->stats.last_success_unix_ms);
    out->last_failure_unix_ms = atomic_load(&exp->stats.last_failure_unix_ms);
}

