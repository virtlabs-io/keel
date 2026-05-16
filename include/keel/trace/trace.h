/**
 * @file trace.h
 * @brief Distributed tracing — W3C Trace Context, span lifecycle, OTLP export.
 * @author Keel Authors
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Lightweight distributed tracing for the Keel database proxy. Implements:
 *
 *   - W3C Trace Context (traceparent / tracestate) parsing and propagation
 *   - Per-session span lifecycle with nanosecond timestamps
 *   - Per-worker lock-free span ring buffer with async export thread
 *   - OTLP/HTTP JSON exporter (no protobuf dependency)
 *   - Head-based sampling (configurable rate in parts-per-million)
 *
 * Trace context flows through the session object:
 *   client connect → session.trace_ctx initialised (new or propagated)
 *   query classify → span annotated with query type + route
 *   pool borrow    → span event "pool.borrow"
 *   backend I/O    → span event "backend.query"
 *   session close  → root span finished and queued for export
 *
 * Design constraints:
 *   - Zero allocation on the hot path (span storage is inline in session)
 *   - No mutex on the per-worker ring — single-producer (worker), single-consumer (exporter)
 *   - Graceful degradation: if the ring is full, spans are dropped (counted)
 *   - If tracing is disabled, all macros compile to no-ops
 */

#ifndef KEEL_TRACE_H
#define KEEL_TRACE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Trace ID / Span ID Types
 * ============================================================================ */

/** W3C trace-id: 16 bytes (128-bit), stored as two uint64_t for cheap copy. */
typedef struct keel_trace_id {
    uint64_t hi;
    uint64_t lo;
} keel_trace_id_t;

/** W3C span-id: 8 bytes (64-bit). */
typedef uint64_t keel_span_id_t;

/** Check if a trace ID is zero (invalid / not set). */
static inline bool keel_trace_id_is_zero(keel_trace_id_t id) {
    return id.hi == 0 && id.lo == 0;
}

/* ============================================================================
 * W3C Trace Context
 * ============================================================================ */

#define KEEL_TRACE_FLAG_SAMPLED  0x01

/** Parsed W3C traceparent header. */
typedef struct keel_trace_ctx {
    uint8_t          version;        /**< Always 0x00 for current spec */
    keel_trace_id_t  trace_id;       /**< 128-bit trace identifier */
    keel_span_id_t   parent_span_id; /**< 64-bit parent span identifier */
    uint8_t          trace_flags;    /**< W3C trace flags (bit 0 = sampled) */
} keel_trace_ctx_t;

/**
 * @brief Parse a W3C traceparent header value.
 *
 * Expected format: "00-<32 hex>-<16 hex>-<2 hex>"
 *
 * @param header  Null-terminated traceparent string.
 * @param out     Parsed trace context on success.
 * @return true on successful parse, false on malformed input.
 */
bool keel_trace_parse_traceparent(const char* header, keel_trace_ctx_t* out);

/**
 * @brief Format a trace context as a W3C traceparent header.
 *
 * @param ctx     Trace context to format.
 * @param buf     Output buffer (must be >= 56 bytes).
 * @param buflen  Buffer size.
 * @return Number of bytes written (excluding NUL), or 0 on error.
 */
size_t keel_trace_format_traceparent(const keel_trace_ctx_t* ctx,
                                     char* buf, size_t buflen);

/* ============================================================================
 * Span Types
 * ============================================================================ */

/** Span kind (subset of OpenTelemetry SpanKind). */
typedef enum keel_span_kind {
    KEEL_SPAN_INTERNAL = 0, /**< Default — internal operation */
    KEEL_SPAN_SERVER   = 1, /**< Inbound client request */
    KEEL_SPAN_CLIENT   = 2, /**< Outbound backend request */
} keel_span_kind_t;

/** Span status code. */
typedef enum keel_span_status {
    KEEL_SPAN_STATUS_UNSET = 0,
    KEEL_SPAN_STATUS_OK    = 1,
    KEEL_SPAN_STATUS_ERROR = 2,
} keel_span_status_t;

/** Maximum number of inline events per span. */
#define KEEL_SPAN_MAX_EVENTS     8
/** Maximum number of inline attributes per span. */
#define KEEL_SPAN_MAX_ATTRIBUTES 12

/** A span event (timestamped annotation). */
typedef struct keel_span_event {
    const char* name;           /**< Static string (no ownership) */
    uint64_t    timestamp_ns;   /**< CLOCK_REALTIME nanoseconds */
} keel_span_event_t;

/** A span attribute (key-value pair). */
typedef struct keel_span_attr {
    const char* key;            /**< Static string key */
    enum { KEEL_ATTR_STR, KEEL_ATTR_INT, KEEL_ATTR_BOOL } type;
    union {
        const char* str_val;    /**< Static or session-lifetime string */
        int64_t     int_val;
        bool        bool_val;
    };
} keel_span_attr_t;

/**
 * @brief A single trace span.
 *
 * Stored inline in the session for hot-path spans (session root, query).
 * Finished spans are memcpy'd into the per-worker ring buffer.
 */
typedef struct keel_span {
    keel_trace_id_t  trace_id;
    keel_span_id_t   span_id;
    keel_span_id_t   parent_span_id;
    keel_span_kind_t kind;

    const char*      name;            /**< Static string — span operation name */

    uint64_t         start_time_ns;   /**< CLOCK_REALTIME ns */
    uint64_t         end_time_ns;     /**< CLOCK_REALTIME ns (0 if not finished) */

    keel_span_status_t status;
    const char*        status_msg;    /**< Optional error message (static) */

    /* Inline events */
    keel_span_event_t events[KEEL_SPAN_MAX_EVENTS];
    uint8_t           event_count;

    /* Inline attributes */
    keel_span_attr_t  attrs[KEEL_SPAN_MAX_ATTRIBUTES];
    uint8_t           attr_count;

    /* Resource attributes (set once at tracer init) */
    const char*       service_name;   /**< "keel" */
    const char*       node_id;        /**< Cluster node ID or hostname */
    uint32_t          worker_id;      /**< Worker index */
} keel_span_t;

/* ============================================================================
 * Span Lifecycle
 * ============================================================================ */

/** Generate a random 128-bit trace ID using a fast PRNG. */
keel_trace_id_t keel_trace_generate_trace_id(void);

/** Generate a random 64-bit span ID. */
keel_span_id_t keel_trace_generate_span_id(void);

/** Get current wall-clock time in nanoseconds. */
uint64_t keel_trace_now_ns(void);

/**
 * @brief Start a new span.
 *
 * Initializes the span in-place. Caller owns the memory (typically inline
 * in the session or on the stack).
 */
void keel_span_start(keel_span_t* span, const char* name,
                     keel_span_kind_t kind,
                     keel_trace_id_t trace_id,
                     keel_span_id_t parent_span_id);

/**
 * @brief Add a timestamped event to a span.
 */
void keel_span_add_event(keel_span_t* span, const char* name);

/**
 * @brief Add a string attribute to a span.
 */
void keel_span_set_attr_str(keel_span_t* span, const char* key, const char* val);

/**
 * @brief Add an integer attribute to a span.
 */
void keel_span_set_attr_int(keel_span_t* span, const char* key, int64_t val);

/**
 * @brief Add a boolean attribute to a span.
 */
void keel_span_set_attr_bool(keel_span_t* span, const char* key, bool val);

/**
 * @brief Set span status.
 */
void keel_span_set_status(keel_span_t* span, keel_span_status_t status,
                          const char* msg);

/**
 * @brief Finish a span (set end_time_ns).
 */
void keel_span_finish(keel_span_t* span);

/**
 * @brief Format a W3C traceparent SQL comment for backend propagation.
 *
 * Formats a SQL block comment containing the W3C traceparent header value,
 * e.g. traceparent=00-<32hexchars>-<16hexchars>-01, into @p buf.
 * The comment can be prepended to a SQL query string to
 * propagate trace context to the backend database server.
 *
 * @param ctx     Trace context carrying trace_id / parent_id.
 * @param buf     Output buffer.
 * @param buflen  Buffer size (must be >= 80 bytes for the full comment + NUL).
 * @return Number of bytes written (excluding NUL), or 0 when tracing is
 *         disabled or @p ctx is NULL.
 */
size_t keel_trace_format_sql_comment(const keel_trace_ctx_t* ctx,
                                     char* buf, size_t buflen);

/* ============================================================================
 * Tracer Configuration
 * ============================================================================ */

/** OTLP export protocol. */
typedef enum keel_otlp_protocol {
    KEEL_OTLP_HTTP_JSON     = 0,  /**< application/json (default) */
    KEEL_OTLP_HTTP_PROTOBUF = 1,  /**< application/x-protobuf */
} keel_otlp_protocol_t;

typedef struct keel_trace_config {
    bool     enabled;             /**< Master switch */
    char     endpoint[512];       /**< OTLP/HTTP endpoint URL */
    uint32_t sample_rate_ppm;     /**< Sampling rate in parts-per-million (1000000 = 100%) */
    uint32_t batch_size;          /**< Max spans per export batch */
    uint32_t flush_interval_ms;   /**< Flush interval in milliseconds */
    uint32_t ring_capacity;       /**< Per-worker span ring buffer capacity */
    char     service_name[128];   /**< OTLP resource service.name */
    char     node_id[128];        /**< Cluster node ID */
    uint32_t export_timeout_ms;   /**< HTTP timeout for OTLP export */
    keel_otlp_protocol_t protocol; /**< Export protocol (json or protobuf) */
} keel_trace_config_t;

/** Default configuration values. */
#define KEEL_TRACE_CONFIG_DEFAULT {                          \
    .enabled           = false,                             \
    .endpoint          = "http://localhost:4318/v1/traces", \
    .sample_rate_ppm   = 10000,   /* 1% */                  \
    .batch_size        = 256,                               \
    .flush_interval_ms = 5000,                              \
    .ring_capacity     = 4096,                              \
    .service_name      = "keel",                            \
    .node_id           = "",                                \
    .export_timeout_ms = 10000,                             \
    .protocol          = KEEL_OTLP_HTTP_JSON,               \
}

/* ============================================================================
 * Per-Worker Span Ring Buffer
 * ============================================================================ */

/**
 * @brief Lock-free SPSC ring buffer for finished spans.
 *
 * One per worker thread. The worker is the sole producer; the exporter
 * thread is the sole consumer.
 */
typedef struct keel_span_ring {
    keel_span_t*  slots;             /**< Heap-allocated span array */
    uint32_t      capacity;          /**< Must be power of 2 */
    _Atomic(uint32_t) head;          /**< Next write position (producer) */
    _Atomic(uint32_t) tail;          /**< Next read position (consumer) */
} keel_span_ring_t;

/** Initialise a span ring buffer. Returns 0 on success. */
int  keel_span_ring_init(keel_span_ring_t* ring, uint32_t capacity);

/** Destroy a span ring buffer. */
void keel_span_ring_destroy(keel_span_ring_t* ring);

/** Enqueue a finished span. Returns false if the ring is full (span dropped). */
bool keel_span_ring_push(keel_span_ring_t* ring, const keel_span_t* span);

/** Dequeue a batch of spans. Returns count of spans written to `out`. */
size_t keel_span_ring_pop_batch(keel_span_ring_t* ring,
                                keel_span_t* out, size_t max_count);

/* ============================================================================
 * Global Tracer
 * ============================================================================ */

/** Opaque tracer handle (one per process). */
typedef struct keel_tracer keel_tracer_t;

/**
 * @brief Create and start the global tracer.
 *
 * Allocates per-worker ring buffers and starts the async exporter thread.
 *
 * @param config  Tracing configuration.
 * @param num_workers Number of worker threads.
 * @return Tracer handle, or NULL on failure.
 */
keel_tracer_t* keel_tracer_create(const keel_trace_config_t* config,
                                  uint32_t num_workers);

/**
 * @brief Stop the exporter thread and destroy the tracer.
 *
 * Flushes remaining spans before shutdown.
 */
void keel_tracer_destroy(keel_tracer_t* tracer);

/**
 * @brief Submit a finished span to the tracer for export.
 *
 * @param tracer    Global tracer.
 * @param worker_id Worker index (selects ring buffer).
 * @param span      Finished span to enqueue.
 * @return true if enqueued, false if dropped (ring full).
 */
bool keel_tracer_submit(keel_tracer_t* tracer, uint32_t worker_id,
                        const keel_span_t* span);

/**
 * @brief Make a sampling decision for a new trace.
 *
 * Uses a fast thread-local PRNG seeded per-worker.
 * Returns false when tracing is runtime-disabled.
 *
 * @param tracer  Global tracer.
 * @return true if the trace should be sampled (recorded + exported).
 */
bool keel_tracer_should_sample(const keel_tracer_t* tracer);

/**
 * @brief Enable or disable tracing at runtime.
 *
 * When disabled, keel_tracer_should_sample() always returns false.
 * The exporter thread keeps running (drains any in-flight spans).
 */
void keel_tracer_set_enabled(keel_tracer_t* tracer, bool enabled);

/**
 * @brief Check if tracing is runtime-enabled.
 */
bool keel_tracer_is_enabled(const keel_tracer_t* tracer);

/**
 * @brief Change the sampling rate at runtime.
 * @param ppm  New rate in parts-per-million (0 = off, 1000000 = 100%).
 */
void keel_tracer_set_sample_rate(keel_tracer_t* tracer, uint32_t ppm);

/**
 * @brief Get tracer stats (spans created, exported, dropped).
 */
typedef struct keel_tracer_stats {
    _Atomic(uint64_t) spans_created;
    _Atomic(uint64_t) spans_exported;
    _Atomic(uint64_t) spans_dropped;
    _Atomic(uint64_t) export_errors;
    _Atomic(uint64_t) export_batches;
} keel_tracer_stats_t;

const keel_tracer_stats_t* keel_tracer_get_stats(const keel_tracer_t* tracer);

/* ============================================================================
 * OTLP Payload Builders (exposed for testing)
 * ============================================================================ */

/**
 * @brief Build an OTLP JSON payload for a batch of spans.
 *
 * Returns heap-allocated JSON string (caller must free with keel_free()).
 */
char* keel_otlp_build_json(const keel_span_t* spans, size_t count,
                            const char* service_name, size_t* out_len);

/**
 * @brief Build an OTLP protobuf payload for a batch of spans.
 *
 * Returns heap-allocated protobuf bytes (caller must free with keel_free()).
 */
uint8_t* keel_otlp_build_protobuf(const keel_span_t* spans, size_t count,
                                    const char* service_name, size_t* out_len);

/* ============================================================================
 * Convenience Macros (compile to no-ops when tracing is disabled at runtime)
 * ============================================================================ */

/**
 * @brief Start a root span for a session.
 *
 * Usage: KEEL_TRACE_SESSION_START(tracer, session)
 * Initialises session->trace_span if sampled.
 */
#define KEEL_TRACE_SESSION_START(tracer, sess, name)                          \
    do {                                                                      \
        if ((tracer) && keel_tracer_should_sample(tracer)) {                  \
            (sess)->trace_sampled = true;                                     \
            (sess)->trace_ctx.trace_id = keel_trace_generate_trace_id();      \
            (sess)->trace_ctx.trace_flags = KEEL_TRACE_FLAG_SAMPLED;          \
            keel_span_start(&(sess)->trace_span, (name), KEEL_SPAN_SERVER,    \
                            (sess)->trace_ctx.trace_id, 0);                   \
            (sess)->trace_span.worker_id =                                    \
                (sess)->worker ? (sess)->worker->id : 0;                      \
        }                                                                     \
    } while (0)

/**
 * @brief Add an event to the session's active span (if sampled).
 */
#define KEEL_TRACE_EVENT(sess, event_name)                                    \
    do {                                                                      \
        if ((sess)->trace_sampled)                                            \
            keel_span_add_event(&(sess)->trace_span, (event_name));           \
    } while (0)

/**
 * @brief Set a string attribute on the session's span (if sampled).
 */
#define KEEL_TRACE_ATTR_STR(sess, key, val)                                   \
    do {                                                                      \
        if ((sess)->trace_sampled)                                            \
            keel_span_set_attr_str(&(sess)->trace_span, (key), (val));        \
    } while (0)

/**
 * @brief Set an integer attribute on the session's span (if sampled).
 */
#define KEEL_TRACE_ATTR_INT(sess, key, val)                                   \
    do {                                                                      \
        if ((sess)->trace_sampled)                                            \
            keel_span_set_attr_int(&(sess)->trace_span, (key), (val));        \
    } while (0)

/**
 * @brief Finish the session root span and submit for export.
 */
#define KEEL_TRACE_SESSION_END(tracer, sess)                                  \
    do {                                                                      \
        if ((sess)->trace_sampled) {                                          \
            keel_span_finish(&(sess)->trace_span);                            \
            keel_tracer_submit((tracer),                                      \
                (sess)->worker ? (sess)->worker->id : 0,                      \
                &(sess)->trace_span);                                         \
            (sess)->trace_sampled = false;                                    \
        }                                                                     \
    } while (0)

#ifdef __cplusplus
}
#endif

#endif /* KEEL_TRACE_H */
