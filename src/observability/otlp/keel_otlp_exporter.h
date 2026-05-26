/**
 * @file keel_otlp_exporter.h
 * @brief OTLP metrics exporter top-level lifecycle.
 *
 * Owns a single dedicated worker thread that:
 *   - Aggregates per-worker `keel_stats_basic_t` snapshots on a timer.
 *   - Encodes them via @ref keel_otlp_encode_metrics.
 *   - Posts them via @ref keel_otlp_http_post with retry/backoff.
 *
 * Per proposal §1330-1410: exporters never execute in the query
 * forwarding hot path and must never block the reactor.
 *
 * Chunk 7a: lifecycle scaffolding only; the worker thread is created
 * but its run loop is a no-op. Aggregation + encode + post wiring lands
 * in chunk 7d.
 */
#ifndef KEEL_OTLP_EXPORTER_H
#define KEEL_OTLP_EXPORTER_H

#include <stdbool.h>
#include <stdint.h>

#include "keel_exporter_stats.h"
#include "keel_otlp_encode.h"
#include "keel_otlp_http.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct keel_otlp_exporter_config {
    keel_otlp_http_config_t http;
    uint32_t                interval_ms;     /**< Aggregation tick period (also pop timeout) */
    uint32_t                max_retries;     /**< Retries per export attempt (0 = single shot) */
    uint32_t                queue_capacity;  /**< Snapshot queue depth (default 4) */
    uint32_t                encode_buf_bytes;/**< Encode scratch buffer size (default 64 KiB) */
} keel_otlp_exporter_config_t;

typedef struct keel_otlp_exporter keel_otlp_exporter_t;

keel_otlp_exporter_t* keel_otlp_exporter_create(const keel_otlp_exporter_config_t* cfg);
int                   keel_otlp_exporter_start(keel_otlp_exporter_t* exp);
void                  keel_otlp_exporter_stop(keel_otlp_exporter_t* exp);
void                  keel_otlp_exporter_destroy(keel_otlp_exporter_t* exp);

/** Producer-side: enqueue a snapshot for export. Thread-safe.
 *  Returns 1 if a stale snapshot was dropped, 0 otherwise. */
int                   keel_otlp_exporter_submit(keel_otlp_exporter_t* exp,
                                                const keel_otlp_snapshot_t* snap);

/** Copy a consistent view of exporter self-stats into @p out. */
void                  keel_otlp_exporter_self_stats(const keel_otlp_exporter_t* exp,
                                                    keel_exporter_stats_t* out);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_OTLP_EXPORTER_H */
