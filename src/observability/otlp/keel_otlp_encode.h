/**
 * @file keel_otlp_encode.h
 * @brief Snapshot → OTLP/protobuf encoder (cold path).
 *
 * Converts an aggregated `keel_stats_basic_t`-derived snapshot into a
 * serialized OTLP `ExportMetricsServiceRequest` (one ResourceMetrics,
 * one ScopeMetrics, N cumulative Sum metrics with a single int64
 * NumberDataPoint each) suitable for HTTP POST to an OTLP/HTTP
 * collector endpoint.
 *
 * Per proposals/v0.2-alpha_observability.md §22.3 the encoder runs only
 * on the dedicated exporter thread; never on the reactor.
 */
#ifndef KEEL_OTLP_ENCODE_H
#define KEEL_OTLP_ENCODE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define KEEL_OTLP_MAX_METRICS   64
#define KEEL_OTLP_MAX_NAME_LEN  96

typedef enum keel_otlp_encode_result {
    KEEL_OTLP_ENCODE_OK = 0,
    KEEL_OTLP_ENCODE_BUFFER_TOO_SMALL,
    KEEL_OTLP_ENCODE_PROTO_ERROR,
    KEEL_OTLP_ENCODE_INVALID_ARG,
} keel_otlp_encode_result_t;

typedef struct keel_otlp_metric_sample {
    char     name[KEEL_OTLP_MAX_NAME_LEN];
    uint64_t value;
} keel_otlp_metric_sample_t;

typedef struct keel_otlp_snapshot {
    uint64_t start_time_unix_nano;
    uint64_t time_unix_nano;
    size_t   metric_count;
    keel_otlp_metric_sample_t metrics[KEEL_OTLP_MAX_METRICS];
} keel_otlp_snapshot_t;

/**
 * @brief Encode @p snap into a serialized OTLP request body.
 *
 * @param snap          Snapshot to encode (non-NULL).
 * @param out_buf       Output buffer (non-NULL).
 * @param out_cap       Capacity of @p out_buf in bytes.
 * @param out_len[out]  On success, written with the encoded byte count.
 * @return KEEL_OTLP_ENCODE_OK on success.
 */
keel_otlp_encode_result_t keel_otlp_encode_metrics(
    const keel_otlp_snapshot_t* snap,
    uint8_t* out_buf,
    size_t out_cap,
    size_t* out_len);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_OTLP_ENCODE_H */
