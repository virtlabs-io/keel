/**
 * @file keel_exporter_stats.h
 * @brief Exporter self-observation counters (proposal §21).
 *
 * These are process-global (not worker-additive). Updated only by the
 * exporter thread / queue layer, read by the JSON admin endpoint.
 */
#ifndef KEEL_EXPORTER_STATS_H
#define KEEL_EXPORTER_STATS_H

#include <stdatomic.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct keel_exporter_stats {
    _Atomic uint64_t attempts;
    _Atomic uint64_t successes;
    _Atomic uint64_t failures;
    _Atomic uint64_t dropped;            /**< Snapshots dropped because queue full */
    _Atomic uint64_t queue_depth;        /**< Last observed queue depth */
    _Atomic uint64_t queue_capacity;     /**< Configured queue capacity */
    _Atomic uint64_t timeouts;
    _Atomic uint64_t last_duration_ns;
    _Atomic int64_t  last_status;        /**< keel_otlp_http_result_t of last attempt */
    _Atomic uint64_t last_success_unix_ms;
    _Atomic uint64_t last_failure_unix_ms;
} keel_exporter_stats_t;

static inline void keel_exporter_stats_init(keel_exporter_stats_t* s, uint64_t queue_capacity)
{
    atomic_store(&s->attempts,             0);
    atomic_store(&s->successes,            0);
    atomic_store(&s->failures,             0);
    atomic_store(&s->dropped,              0);
    atomic_store(&s->queue_depth,          0);
    atomic_store(&s->queue_capacity,       queue_capacity);
    atomic_store(&s->timeouts,             0);
    atomic_store(&s->last_duration_ns,     0);
    atomic_store(&s->last_status,          0);
    atomic_store(&s->last_success_unix_ms, 0);
    atomic_store(&s->last_failure_unix_ms, 0);
}

#ifdef __cplusplus
}
#endif

#endif /* KEEL_EXPORTER_STATS_H */
