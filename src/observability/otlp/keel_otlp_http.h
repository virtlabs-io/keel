/**
 * @file keel_otlp_http.h
 * @brief Bounded-timeout HTTP/1.1 client for OTLP/HTTP collector POSTs.
 *
 * Owns its own dedicated worker thread (one per exporter) so that
 * collector-side stalls cannot back-pressure the reactor workers.
 * Per proposal §1199-1320: the timeout is configurable and bounded,
 * connection reuse is best-effort, and every failure increments an
 * exporter self-metric.
 *
 * Chunk 7a: function signatures only; bodies stubbed.
 */
#ifndef KEEL_OTLP_HTTP_H
#define KEEL_OTLP_HTTP_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum keel_otlp_http_result {
    KEEL_OTLP_HTTP_OK = 0,
    KEEL_OTLP_HTTP_CONNECT_FAILED,
    KEEL_OTLP_HTTP_TIMEOUT,
    KEEL_OTLP_HTTP_PROTOCOL_ERROR,
    KEEL_OTLP_HTTP_SERVER_REJECT,   /**< 4xx; do not retry */
    KEEL_OTLP_HTTP_SERVER_RETRY,    /**< 5xx / 429; retry with backoff */
    KEEL_OTLP_HTTP_NOT_IMPLEMENTED,
} keel_otlp_http_result_t;

typedef struct keel_otlp_http_config {
    const char* endpoint_url;   /**< e.g. "http://otel-collector:4318/v1/metrics" */
    uint32_t    timeout_ms;     /**< Per-request hard cap */
    const char* bearer_token;   /**< Optional; NULL disables Authorization header */
} keel_otlp_http_config_t;

typedef struct keel_otlp_http keel_otlp_http_t;

keel_otlp_http_t* keel_otlp_http_create(const keel_otlp_http_config_t* cfg);
void              keel_otlp_http_destroy(keel_otlp_http_t* http);

/**
 * @brief Synchronously POST @p body to the configured endpoint.
 *
 * Caller is the exporter's own worker thread; never invoked from the
 * reactor. Blocks up to @ref keel_otlp_http_config_t::timeout_ms.
 */
keel_otlp_http_result_t keel_otlp_http_post(
    keel_otlp_http_t* http,
    const uint8_t* body,
    size_t body_len);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_OTLP_HTTP_H */
