/**
 * @file trace.c
 * @brief Distributed tracing — W3C Trace Context, span lifecycle, OTLP export.
 * @author Keel Authors
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Implementation of the lightweight tracing module. Key design decisions:
 *
 *   1. No heap allocation on the hot path — spans live inline in sessions.
 *   2. Per-worker SPSC ring buffers — no lock contention between workers.
 *   3. Single exporter thread — drains all rings, batches, HTTP POSTs.
 *   4. OTLP/HTTP JSON — avoids protobuf dependency; the collector does the
 *      conversion to its native format.
 *   5. Fast PRNG for sampling decisions — splitmix64 seeded from /dev/urandom.
 */

#include "keel/trace/trace.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"
#include "keel/util/encoding.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/time.h>     /* struct timeval — needed explicitly on musl libc */
#include <netdb.h>
#include <arpa/inet.h>

/* ============================================================================
 * Internal PRNG — splitmix64
 * ============================================================================ */

static _Thread_local uint64_t tl_prng_state;

static void prng_seed(void) {
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd >= 0) {
        ssize_t n = read(fd, &tl_prng_state, sizeof(tl_prng_state));
        close(fd);
        if (n < (ssize_t)sizeof(tl_prng_state))
            tl_prng_state = 0; /* Force fallback */
    }
    if (tl_prng_state == 0) {
        /* Fallback: mix thread ID + clock */
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        tl_prng_state = (uint64_t)ts.tv_nsec ^ ((uint64_t)pthread_self() * 0x9E3779B97F4A7C15ULL);
    }
}

static uint64_t prng_next(void) {
    if (__builtin_expect(tl_prng_state == 0, 0))
        prng_seed();
    uint64_t z = (tl_prng_state += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

/* ============================================================================
 * Hex Encoding / Decoding
 * ============================================================================ */

/* hex_chars removed: use keel_hex_encode() from keel/util/encoding.h */

/** Decode `len` hex characters into bytes. Returns false on bad input. */
static bool hex_decode(const char* src, uint8_t* dst, size_t byte_count) {
    for (size_t i = 0; i < byte_count; i++) {
        char hi = src[i * 2];
        char lo = src[i * 2 + 1];

        int h = (hi >= '0' && hi <= '9') ? (hi - '0') :
                (hi >= 'a' && hi <= 'f') ? (hi - 'a' + 10) :
                (hi >= 'A' && hi <= 'F') ? (hi - 'A' + 10) : -1;
        int l = (lo >= '0' && lo <= '9') ? (lo - '0') :
                (lo >= 'a' && lo <= 'f') ? (lo - 'a' + 10) :
                (lo >= 'A' && lo <= 'F') ? (lo - 'A' + 10) : -1;

        if (h < 0 || l < 0)
            return false;

        dst[i] = (uint8_t)((h << 4) | l);
    }
    return true;
}

/** Encode `byte_count` bytes into lowercase hex. */
static void hex_encode(const uint8_t* src, char* dst, size_t byte_count) {
    keel_hex_encode(src, dst, byte_count);
}

/* ============================================================================
 * W3C Trace Context
 * ============================================================================ */

bool keel_trace_parse_traceparent(const char* header, keel_trace_ctx_t* out) {
    if (!header || !out)
        return false;

    /* Expected: "VV-TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT-SSSSSSSSSSSSSSSS-FF" */
    /* Lengths:   2  1  32                            1  16              1  2 = 55 */
    size_t len = strlen(header);
    if (len < 55)
        return false;

    /* Check delimiters */
    if (header[2] != '-' || header[35] != '-' || header[52] != '-')
        return false;

    /* Version */
    uint8_t ver;
    if (!hex_decode(header, &ver, 1))
        return false;

    /* For version 00, we don't accept future version extensions with extra fields */
    if (ver == 0x00 && len != 55)
        return false;

    /* Trace ID (16 bytes = 32 hex chars) */
    uint8_t trace_bytes[16];
    if (!hex_decode(header + 3, trace_bytes, 16))
        return false;

    /* Span ID (8 bytes = 16 hex chars) */
    uint8_t span_bytes[8];
    if (!hex_decode(header + 36, span_bytes, 8))
        return false;

    /* Flags (1 byte = 2 hex chars) */
    uint8_t flags;
    if (!hex_decode(header + 53, &flags, 1))
        return false;

    /* Assemble trace_id from big-endian bytes */
    keel_trace_id_t tid = {0};
    for (int i = 0; i < 8; i++) {
        tid.hi = (tid.hi << 8) | trace_bytes[i];
        tid.lo = (tid.lo << 8) | trace_bytes[i + 8];
    }

    /* Assemble span_id from big-endian bytes */
    keel_span_id_t sid = 0;
    for (int i = 0; i < 8; i++)
        sid = (sid << 8) | span_bytes[i];

    /* Zero trace-id or span-id is invalid per spec */
    if (keel_trace_id_is_zero(tid) || sid == 0)
        return false;

    out->version = ver;
    out->trace_id = tid;
    out->parent_span_id = sid;
    out->trace_flags = flags;
    return true;
}

/**
 * @brief Format a W3C traceparent header value from a trace context.
 *
 * Produces the 55-character string "00-<32 hex>-<16 hex>-<2 hex>" (plus NUL).
 * Requires @p buflen ≥ 56.
 *
 * @param ctx     Trace context supplying trace ID, span ID, and flags.
 * @param[out] buf Output buffer of at least 56 bytes.
 * @param buflen  Capacity of @p buf.
 * @return Number of characters written (55), or 0 on invalid arguments.
 */
size_t keel_trace_format_traceparent(const keel_trace_ctx_t* ctx,
                                     char* buf, size_t buflen) {
    if (!ctx || !buf || buflen < 56)
        return 0;

    /* Version */
    buf[0] = '0';
    buf[1] = '0';
    buf[2] = '-';

    /* Trace ID — big-endian */
    uint8_t trace_bytes[16];
    for (int i = 7; i >= 0; i--) {
        trace_bytes[i]     = (uint8_t)(ctx->trace_id.hi & 0xFF);
        trace_bytes[i + 8] = (uint8_t)(ctx->trace_id.lo & 0xFF);
        if (i > 0) {
            /* Avoid shifting on last iteration to silence UBSAN for signed types */
        }
    }
    /* More explicit big-endian encoding */
    for (int i = 0; i < 8; i++) {
        trace_bytes[i]     = (uint8_t)(ctx->trace_id.hi >> (56 - i * 8));
        trace_bytes[i + 8] = (uint8_t)(ctx->trace_id.lo >> (56 - i * 8));
    }
    hex_encode(trace_bytes, buf + 3, 16);
    buf[35] = '-';

    /* Span ID — big-endian */
    uint8_t span_bytes[8];
    for (int i = 0; i < 8; i++)
        span_bytes[i] = (uint8_t)(ctx->parent_span_id >> (56 - i * 8));
    hex_encode(span_bytes, buf + 36, 8);
    buf[52] = '-';

    /* Flags */
    keel_hex_encode(&ctx->trace_flags, buf + 53, 1);
    buf[55] = '\0';

    return 55;
}

size_t keel_trace_format_sql_comment(const keel_trace_ctx_t* ctx,
                                     char* buf, size_t buflen) {
    if (!ctx || !buf || buflen < 80)
        return 0;
    /* Only emit when sampled (bit 0 of trace_flags) */
    if (!(ctx->trace_flags & 0x01))
        return 0;

    char tp[56];
    size_t tlen = keel_trace_format_traceparent(ctx, tp, sizeof(tp));
    if (tlen == 0)
        return 0;

    int n = snprintf(buf, buflen, "/* traceparent=%s */ ", tp);
    if (n < 0 || (size_t)n >= buflen)
        return 0;
    return (size_t)n;
}

/* ============================================================================
 * ID Generation
 * ============================================================================ */

/** @brief Generate a random 128-bit trace ID using the thread-local PRNG. */
keel_trace_id_t keel_trace_generate_trace_id(void) {
    return (keel_trace_id_t){ .hi = prng_next(), .lo = prng_next() };
}

/** @brief Generate a non-zero random 64-bit span ID. */
keel_span_id_t keel_trace_generate_span_id(void) {
    keel_span_id_t id;
    do {
        id = prng_next();
    } while (id == 0); /* span-id must not be zero */
    return id;
}

/** @brief Return the current wall-clock time in nanoseconds (CLOCK_REALTIME). */
uint64_t keel_trace_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ============================================================================
 * Span Lifecycle
 * ============================================================================ */

/**
 * @brief Initialise and start a span.
 *
 * Zeros the span, assigns name, kind, trace/parent IDs, generates a new span
 * ID, and records the start timestamp.
 *
 * @param span           Span to initialise.
 * @param name           Static string identifying the operation.
 * @param kind           Span kind (server, client, producer, consumer, etc.).
 * @param trace_id       Trace ID to associate with this span.
 * @param parent_span_id Parent span ID, or 0 for a root span.
 */
void keel_span_start(keel_span_t* span, const char* name,
                     keel_span_kind_t kind,
                     keel_trace_id_t trace_id,
                     keel_span_id_t parent_span_id) {
    memset(span, 0, sizeof(*span));
    span->name            = name;
    span->kind            = kind;
    span->trace_id        = trace_id;
    span->span_id         = keel_trace_generate_span_id();
    span->parent_span_id  = parent_span_id;
    span->start_time_ns   = keel_trace_now_ns();
    span->status          = KEEL_SPAN_STATUS_UNSET;
}

/**
 * @brief Append a named timestamped event to a span.
 *
 * Silently drops the event if the span's event array is full.
 *
 * @param span  Span to annotate.
 * @param name  Static event name string.
 */
void keel_span_add_event(keel_span_t* span, const char* name) {
    if (!span || span->event_count >= KEEL_SPAN_MAX_EVENTS)
        return;
    keel_span_event_t* ev = &span->events[span->event_count++];
    ev->name = name;
    ev->timestamp_ns = keel_trace_now_ns();
}

/**
 * @brief Set a string attribute on a span.
 *
 * @param span  Target span.
 * @param key   Attribute key (static string).
 * @param val   Attribute value (static string).
 */
void keel_span_set_attr_str(keel_span_t* span, const char* key, const char* val) {
    if (!span || span->attr_count >= KEEL_SPAN_MAX_ATTRIBUTES)
        return;
    keel_span_attr_t* a = &span->attrs[span->attr_count++];
    a->key = key;
    a->type = KEEL_ATTR_STR;
    a->str_val = val;
}

/**
 * @brief Set an integer attribute on a span.
 *
 * @param span  Target span.
 * @param key   Attribute key (static string).
 * @param val   64-bit integer value.
 */
void keel_span_set_attr_int(keel_span_t* span, const char* key, int64_t val) {
    if (!span || span->attr_count >= KEEL_SPAN_MAX_ATTRIBUTES)
        return;
    keel_span_attr_t* a = &span->attrs[span->attr_count++];
    a->key = key;
    a->type = KEEL_ATTR_INT;
    a->int_val = val;
}

/**
 * @brief Set a boolean attribute on a span.
 *
 * @param span  Target span.
 * @param key   Attribute key (static string).
 * @param val   Boolean value.
 */
void keel_span_set_attr_bool(keel_span_t* span, const char* key, bool val) {
    if (!span || span->attr_count >= KEEL_SPAN_MAX_ATTRIBUTES)
        return;
    keel_span_attr_t* a = &span->attrs[span->attr_count++];
    a->key = key;
    a->type = KEEL_ATTR_BOOL;
    a->bool_val = val;
}

/**
 * @brief Set the status and optional message on a span.
 *
 * @param span    Target span.
 * @param status  Status code (UNSET, OK, or ERROR).
 * @param msg     Optional human-readable message; may be NULL.
 */
void keel_span_set_status(keel_span_t* span, keel_span_status_t status,
                          const char* msg) {
    if (!span)
        return;
    span->status = status;
    span->status_msg = msg;
}

/** @brief Record the end timestamp on a span. No-op if already finished. */
void keel_span_finish(keel_span_t* span) {
    if (!span || span->end_time_ns != 0)
        return;
    span->end_time_ns = keel_trace_now_ns();
}

/* ============================================================================
 * Span Ring Buffer (SPSC)
 * ============================================================================ */

/** @brief Round @p v up to the next power of two. */
static uint32_t next_pow2(uint32_t v) {
    v--;
    v |= v >> 1; v |= v >> 2; v |= v >> 4;
    v |= v >> 8; v |= v >> 16;
    return v + 1;
}

/**
 * @brief Initialise a lock-free SPSC span ring buffer.
 *
 * The actual capacity is rounded up to the next power of two.
 *
 * @param ring      Ring to initialise.
 * @param capacity  Minimum number of span slots.
 * @return 0 on success, -1 on invalid arguments or allocation failure.
 */
int keel_span_ring_init(keel_span_ring_t* ring, uint32_t capacity) {
    if (!ring || capacity == 0)
        return -1;

    capacity = next_pow2(capacity);
    ring->slots = keel_calloc(capacity, sizeof(keel_span_t));
    if (!ring->slots)
        return -1;

    ring->capacity = capacity;
    atomic_store_explicit(&ring->head, 0, memory_order_relaxed);
    atomic_store_explicit(&ring->tail, 0, memory_order_relaxed);
    return 0;
}

/** @brief Free the storage of a span ring buffer and reset its fields. */
void keel_span_ring_destroy(keel_span_ring_t* ring) {
    if (!ring)
        return;
    keel_free(ring->slots);
    ring->slots = NULL;
    ring->capacity = 0;
}

/**
 * @brief Push a span into the ring buffer (producer side).
 *
 * Lock-free SPSC; safe to call from any single producer thread without
 * external synchronisation.
 *
 * @param ring  Ring buffer.
 * @param span  Span to copy into the ring.
 * @return true on success, false if the ring is full.
 */
bool keel_span_ring_push(keel_span_ring_t* ring, const keel_span_t* span) {
    uint32_t head = atomic_load_explicit(&ring->head, memory_order_relaxed);
    uint32_t tail = atomic_load_explicit(&ring->tail, memory_order_acquire);
    uint32_t mask = ring->capacity - 1;

    if (((head + 1) & mask) == (tail & mask))
        return false; /* Full */

    ring->slots[head & mask] = *span;
    atomic_store_explicit(&ring->head, (head + 1) & mask, memory_order_release);
    return true;
}

/**
 * @brief Pop up to @p max_count spans from the ring buffer (consumer side).
 *
 * Lock-free SPSC; safe to call from the single consumer (exporter) thread.
 *
 * @param ring       Ring buffer.
 * @param[out] out   Destination array for popped spans.
 * @param max_count  Maximum number of spans to dequeue.
 * @return Number of spans written to @p out (0 if empty).
 */
size_t keel_span_ring_pop_batch(keel_span_ring_t* ring,
                                keel_span_t* out, size_t max_count) {
    uint32_t tail = atomic_load_explicit(&ring->tail, memory_order_relaxed);
    uint32_t head = atomic_load_explicit(&ring->head, memory_order_acquire);
    uint32_t mask = ring->capacity - 1;
    size_t count = 0;

    while (count < max_count && tail != head) {
        out[count++] = ring->slots[tail & mask];
        tail = (tail + 1) & mask;
    }

    if (count > 0)
        atomic_store_explicit(&ring->tail, tail, memory_order_release);

    return count;
}

/* ============================================================================
 * OTLP/HTTP JSON Exporter
 * ============================================================================ */

/**
 * @brief Format a batch of spans as OTLP JSON and send via HTTP POST.
 *
 * This is a simplified OTLP/HTTP JSON encoder. It produces a valid
 * ExportTraceServiceRequest JSON payload that any OTLP collector understands.
 */

/** JSON-escape a string into dst. Returns bytes written (excluding NUL). */
static size_t json_escape(char* dst, size_t dst_size, const char* src) {
    return keel_json_escape(dst, dst_size, src);
}

/** Format trace_id as 32 hex chars into buf (must be >= 33 bytes). */
static void format_trace_id_hex(const keel_trace_id_t* id, char* buf) {
    uint8_t bytes[16];
    for (int i = 0; i < 8; i++) {
        bytes[i]     = (uint8_t)(id->hi >> (56 - i * 8));
        bytes[i + 8] = (uint8_t)(id->lo >> (56 - i * 8));
    }
    hex_encode(bytes, buf, 16);
    buf[32] = '\0';
}

/** Format span_id as 16 hex chars into buf (must be >= 17 bytes). */
static void format_span_id_hex(keel_span_id_t id, char* buf) {
    uint8_t bytes[8];
    for (int i = 0; i < 8; i++)
        bytes[i] = (uint8_t)(id >> (56 - i * 8));
    hex_encode(bytes, buf, 8);
    buf[16] = '\0';
}

/**
 * @brief Build OTLP JSON payload for a batch of spans.
 *
 * Returns heap-allocated JSON string. Caller must free with keel_free().
 */
char* keel_otlp_build_json(const keel_span_t* spans, size_t count,
                            const char* service_name, size_t* out_len) {
    /* Pre-allocate generously — 2KB per span is typically sufficient */
    size_t buf_size = 512 + count * 2048;
    char* buf = keel_malloc(buf_size);
    if (!buf) return NULL;

    size_t pos = 0;

#define APPEND(...) pos += (size_t)snprintf(buf + pos, buf_size - pos, __VA_ARGS__)

    APPEND("{\"resourceSpans\":[{");
    APPEND("\"resource\":{\"attributes\":[");
    APPEND("{\"key\":\"service.name\",\"value\":{\"stringValue\":\"");
    {
        char escaped[256];
        json_escape(escaped, sizeof(escaped), service_name);
        APPEND("%s", escaped);
    }
    APPEND("\"}}]},");
    APPEND("\"scopeSpans\":[{");
    APPEND("\"scope\":{\"name\":\"keel.trace\",\"version\":\"0.1.0\"},");
    APPEND("\"spans\":[");

    for (size_t i = 0; i < count; i++) {
        const keel_span_t* s = &spans[i];
        if (i > 0) APPEND(",");

        char trace_hex[33], span_hex[17], parent_hex[17];
        format_trace_id_hex(&s->trace_id, trace_hex);
        format_span_id_hex(s->span_id, span_hex);
        format_span_id_hex(s->parent_span_id, parent_hex);

        APPEND("{\"traceId\":\"%s\"", trace_hex);
        APPEND(",\"spanId\":\"%s\"", span_hex);
        if (s->parent_span_id != 0)
            APPEND(",\"parentSpanId\":\"%s\"", parent_hex);
        APPEND(",\"name\":\"");
        {
            char escaped[256];
            json_escape(escaped, sizeof(escaped), s->name ? s->name : "");
            APPEND("%s", escaped);
        }
        APPEND("\"");

        /* Kind: OTLP uses 1=INTERNAL, 2=SERVER, 3=CLIENT */
        int otlp_kind = (s->kind == KEEL_SPAN_SERVER) ? 2 :
                        (s->kind == KEEL_SPAN_CLIENT) ? 3 : 1;
        APPEND(",\"kind\":%d", otlp_kind);

        /* Timestamps in nanoseconds as strings (OTLP convention) */
        APPEND(",\"startTimeUnixNano\":\"%lu\"", (unsigned long)s->start_time_ns);
        APPEND(",\"endTimeUnixNano\":\"%lu\"", (unsigned long)s->end_time_ns);

        /* Status */
        if (s->status != KEEL_SPAN_STATUS_UNSET) {
            APPEND(",\"status\":{\"code\":%d", s->status);
            if (s->status_msg) {
                APPEND(",\"message\":\"");
                char escaped[256];
                json_escape(escaped, sizeof(escaped), s->status_msg);
                APPEND("%s", escaped);
                APPEND("\"");
            }
            APPEND("}");
        }

        /* Attributes */
        if (s->attr_count > 0) {
            APPEND(",\"attributes\":[");
            for (uint8_t a = 0; a < s->attr_count; a++) {
                if (a > 0) APPEND(",");
                const keel_span_attr_t* attr = &s->attrs[a];
                char key_esc[128];
                json_escape(key_esc, sizeof(key_esc), attr->key);
                APPEND("{\"key\":\"%s\",\"value\":{", key_esc);
                switch (attr->type) {
                case KEEL_ATTR_STR: {
                    char val_esc[512];
                    json_escape(val_esc, sizeof(val_esc), attr->str_val);
                    APPEND("\"stringValue\":\"%s\"", val_esc);
                    break;
                }
                case KEEL_ATTR_INT:
                    APPEND("\"intValue\":\"%ld\"", (long)attr->int_val);
                    break;
                case KEEL_ATTR_BOOL:
                    APPEND("\"boolValue\":%s", attr->bool_val ? "true" : "false");
                    break;
                }
                APPEND("}}");
            }
            APPEND("]");
        }

        /* Events */
        if (s->event_count > 0) {
            APPEND(",\"events\":[");
            for (uint8_t e = 0; e < s->event_count; e++) {
                if (e > 0) APPEND(",");
                const keel_span_event_t* ev = &s->events[e];
                char name_esc[128];
                json_escape(name_esc, sizeof(name_esc), ev->name);
                APPEND("{\"timeUnixNano\":\"%lu\"", (unsigned long)ev->timestamp_ns);
                APPEND(",\"name\":\"%s\"}", name_esc);
            }
            APPEND("]");
        }

        APPEND("}");
    }

    APPEND("]}]}]}");
#undef APPEND

    if (out_len) *out_len = pos;
    return buf;
}

/* ============================================================================
 * OTLP Protobuf Encoder (hand-rolled, no external dependency)
 * ============================================================================ */

/**
 * Growable byte buffer for protobuf serialization.
 * Two-pass encoding: first measure, then write — avoids backpatching.
 */
typedef struct pb_buf {
    uint8_t* data;
    size_t   pos;
    size_t   cap;
} pb_buf_t;

static void pb_init(pb_buf_t* b, size_t initial_cap) {
    b->data = keel_malloc(initial_cap);
    b->pos = 0;
    b->cap = initial_cap;
}

static void pb_ensure(pb_buf_t* b, size_t need) {
    if (b->pos + need <= b->cap) return;
    size_t new_cap = b->cap * 2;
    while (new_cap < b->pos + need) new_cap *= 2;
    uint8_t* p = keel_malloc(new_cap);
    if (p) { memcpy(p, b->data, b->pos); keel_free(b->data); b->data = p; b->cap = new_cap; }
}

static void pb_raw(pb_buf_t* b, const void* src, size_t len) {
    pb_ensure(b, len);
    memcpy(b->data + b->pos, src, len);
    b->pos += len;
}

static void pb_varint(pb_buf_t* b, uint64_t val) {
    pb_ensure(b, 10);
    while (val > 0x7F) {
        b->data[b->pos++] = (uint8_t)(val & 0x7F) | 0x80;
        val >>= 7;
    }
    b->data[b->pos++] = (uint8_t)val;
}

static void pb_fixed64(pb_buf_t* b, uint64_t val) {
    pb_ensure(b, 8);
    for (int i = 0; i < 8; i++)
        b->data[b->pos++] = (uint8_t)(val >> (i * 8));
}

static void pb_tag(pb_buf_t* b, uint32_t field, uint32_t wire_type) {
    pb_varint(b, ((uint64_t)field << 3) | wire_type);
}

static void pb_bytes(pb_buf_t* b, uint32_t field, const void* data, size_t len) {
    pb_tag(b, field, 2);
    pb_varint(b, len);
    pb_raw(b, data, len);
}

static void pb_string(pb_buf_t* b, uint32_t field, const char* s) {
    if (!s || !s[0]) return;
    pb_bytes(b, field, s, strlen(s));
}

static void pb_submsg(pb_buf_t* b, uint32_t field, const pb_buf_t* sub) {
    pb_tag(b, field, 2);
    pb_varint(b, sub->pos);
    pb_raw(b, sub->data, sub->pos);
}

static void pb_field_varint(pb_buf_t* b, uint32_t field, uint64_t val) {
    pb_tag(b, field, 0);
    pb_varint(b, val);
}

static void pb_field_fixed64(pb_buf_t* b, uint32_t field, uint64_t val) {
    pb_tag(b, field, 1);
    pb_fixed64(b, val);
}

/**
 * Encode a KeyValue for resource attributes (field 1 in Resource).
 */
static void pb_encode_resource_attr(pb_buf_t* b, const char* key, const char* val) {
    pb_buf_t kv = {0};
    pb_init(&kv, 128);
    pb_string(&kv, 1, key);

    pb_buf_t av = {0};
    pb_init(&av, 64);
    pb_string(&av, 1, val);
    pb_submsg(&kv, 2, &av);
    keel_free(av.data);

    pb_submsg(b, 1, &kv);  /* Resource.attributes field 1 */
    keel_free(kv.data);
}

/**
 * Encode a span Event.
 * Event { fixed64 time_unix_nano=1; string name=2; }
 */
static void pb_encode_event(pb_buf_t* b, const keel_span_event_t* ev) {
    pb_buf_t ev_buf = {0};
    pb_init(&ev_buf, 64);
    pb_field_fixed64(&ev_buf, 1, ev->timestamp_ns);
    pb_string(&ev_buf, 2, ev->name);
    pb_submsg(b, 11, &ev_buf);  /* Span.events field 11 */
    keel_free(ev_buf.data);
}

/**
 * Encode a single Span message.
 */
static void pb_encode_span(pb_buf_t* b, const keel_span_t* s) {
    pb_buf_t span = {0};
    pb_init(&span, 512);

    /* trace_id: bytes field 1 (16 bytes, big-endian) */
    uint8_t trace_bytes[16];
    for (int i = 0; i < 8; i++) {
        trace_bytes[i]     = (uint8_t)(s->trace_id.hi >> (56 - i * 8));
        trace_bytes[i + 8] = (uint8_t)(s->trace_id.lo >> (56 - i * 8));
    }
    pb_bytes(&span, 1, trace_bytes, 16);

    /* span_id: bytes field 2 (8 bytes, big-endian) */
    uint8_t span_bytes[8];
    for (int i = 0; i < 8; i++)
        span_bytes[i] = (uint8_t)(s->span_id >> (56 - i * 8));
    pb_bytes(&span, 2, span_bytes, 8);

    /* parent_span_id: bytes field 4 */
    if (s->parent_span_id != 0) {
        uint8_t parent_bytes[8];
        for (int i = 0; i < 8; i++)
            parent_bytes[i] = (uint8_t)(s->parent_span_id >> (56 - i * 8));
        pb_bytes(&span, 4, parent_bytes, 8);
    }

    /* name: string field 5 */
    pb_string(&span, 5, s->name ? s->name : "");

    /* kind: enum field 6 — OTLP: INTERNAL=1, SERVER=2, CLIENT=3 */
    int otlp_kind = (s->kind == KEEL_SPAN_SERVER) ? 2 :
                    (s->kind == KEEL_SPAN_CLIENT) ? 3 : 1;
    pb_field_varint(&span, 6, (uint64_t)otlp_kind);

    /* start_time_unix_nano: fixed64 field 7 */
    pb_field_fixed64(&span, 7, s->start_time_ns);

    /* end_time_unix_nano: fixed64 field 8 */
    pb_field_fixed64(&span, 8, s->end_time_ns);

    /* attributes: repeated KeyValue field 9 */
    for (uint8_t a = 0; a < s->attr_count; a++) {
        /* pb_encode_attr writes to field 9 of the parent */
        pb_buf_t kv = {0};
        pb_init(&kv, 128);
        pb_string(&kv, 1, s->attrs[a].key);

        pb_buf_t av = {0};
        pb_init(&av, 64);
        switch (s->attrs[a].type) {
        case KEEL_ATTR_STR: pb_string(&av, 1, s->attrs[a].str_val); break;
        case KEEL_ATTR_BOOL: pb_field_varint(&av, 2, s->attrs[a].bool_val ? 1 : 0); break;
        case KEEL_ATTR_INT: pb_field_varint(&av, 3, (uint64_t)s->attrs[a].int_val); break;
        }
        pb_submsg(&kv, 2, &av);
        keel_free(av.data);
        pb_submsg(&span, 9, &kv);
        keel_free(kv.data);
    }

    /* events: repeated Event field 11 */
    for (uint8_t e = 0; e < s->event_count; e++)
        pb_encode_event(&span, &s->events[e]);

    /* status: Status field 15 */
    if (s->status != KEEL_SPAN_STATUS_UNSET) {
        pb_buf_t status = {0};
        pb_init(&status, 32);
        if (s->status_msg)
            pb_string(&status, 2, s->status_msg);
        pb_field_varint(&status, 3, (uint64_t)s->status);
        pb_submsg(&span, 15, &status);
        keel_free(status.data);
    }

    pb_submsg(b, 2, &span);  /* ScopeSpans.spans field 2 */
    keel_free(span.data);
}

/**
 * @brief Build OTLP protobuf payload for a batch of spans.
 *
 * Produces a serialized ExportTraceServiceRequest.
 * Returns heap-allocated buffer. Caller must free with keel_free().
 */
uint8_t* keel_otlp_build_protobuf(const keel_span_t* spans, size_t count,
                                   const char* service_name, size_t* out_len) {
    /* Build bottom-up: Spans → ScopeSpans → ResourceSpans → Request */

    /* InstrumentationScope */
    pb_buf_t scope = {0};
    pb_init(&scope, 64);
    pb_string(&scope, 1, "keel.trace");
    pb_string(&scope, 2, "0.1.0");

    /* ScopeSpans = { scope=1, repeated span=2 } */
    pb_buf_t scope_spans = {0};
    pb_init(&scope_spans, count * 256);
    pb_submsg(&scope_spans, 1, &scope);
    keel_free(scope.data);

    for (size_t i = 0; i < count; i++)
        pb_encode_span(&scope_spans, &spans[i]);

    /* Resource = { repeated KeyValue attributes=1 } */
    pb_buf_t resource = {0};
    pb_init(&resource, 128);
    pb_encode_resource_attr(&resource, "service.name", service_name);

    /* ResourceSpans = { resource=1, scope_spans=2 } */
    pb_buf_t res_spans = {0};
    pb_init(&res_spans, scope_spans.pos + resource.pos + 64);
    pb_submsg(&res_spans, 1, &resource);
    keel_free(resource.data);
    pb_submsg(&res_spans, 2, &scope_spans);
    keel_free(scope_spans.data);

    /* ExportTraceServiceRequest = { repeated ResourceSpans=1 } */
    pb_buf_t request = {0};
    pb_init(&request, res_spans.pos + 16);
    pb_submsg(&request, 1, &res_spans);
    keel_free(res_spans.data);

    if (out_len) *out_len = request.pos;
    return request.data;  /* Caller frees with keel_free() */
}

/* ============================================================================
 * HTTP Client (minimal, blocking — used only by exporter thread)
 * ============================================================================ */

/**
 * @brief Parse a URL into host, port, path components.
 * Returns 0 on success.
 */
static int parse_url(const char* url, char* host, size_t host_len,
                     char* port, size_t port_len,
                     char* path, size_t path_len) {
    /* Skip scheme */
    const char* p = url;
    if (strncmp(p, "http://", 7) == 0) {
        p += 7;
    } else if (strncmp(p, "https://", 8) == 0) {
        p += 8;
    }

    /* Host[:port] */
    const char* slash = strchr(p, '/');
    const char* colon = strchr(p, ':');

    if (colon && (!slash || colon < slash)) {
        size_t hlen = (size_t)(colon - p);
        if (hlen >= host_len) return -1;
        memcpy(host, p, hlen);
        host[hlen] = '\0';

        const char* port_start = colon + 1;
        size_t plen = slash ? (size_t)(slash - port_start) : strlen(port_start);
        if (plen >= port_len) return -1;
        memcpy(port, port_start, plen);
        port[plen] = '\0';
    } else {
        size_t hlen = slash ? (size_t)(slash - p) : strlen(p);
        if (hlen >= host_len) return -1;
        memcpy(host, p, hlen);
        host[hlen] = '\0';
        snprintf(port, port_len, "4318"); /* Default OTLP port */
    }

    /* Path */
    if (slash) {
        snprintf(path, path_len, "%s", slash);
    } else {
        snprintf(path, path_len, "/v1/traces");
    }

    return 0;
}

/**
 * @brief Send a payload via HTTP POST.
 *
 * If *persistent_fd >= 0, attempts to reuse it (HTTP keep-alive).
 * On failure, reconnects. Stores the fd back for reuse.
 * Returns HTTP status code, or -1 on network error.
 */
static int http_post_keepalive(const char* url, const char* content_type,
                                const void* body, size_t body_len,
                                uint32_t timeout_ms, int *persistent_fd) {
    char host[256], port[16], path[256];
    if (parse_url(url, host, sizeof(host), port, sizeof(port),
                  path, sizeof(path)) != 0)
        return -1;

    int fd = *persistent_fd;

    /* Try reusing existing connection; if send fails, reconnect */
    if (fd >= 0) {
        char header[512];
        int hlen = snprintf(header, sizeof(header),
            "POST %s HTTP/1.1\r\n"
            "Host: %s:%s\r\n"
            "Content-Type: %s\r\n"
            "Content-Length: %zu\r\n"
            "Connection: keep-alive\r\n"
            "\r\n",
            path, host, port, content_type, body_len);

        ssize_t sent = send(fd, header, (size_t)hlen, MSG_NOSIGNAL);
        if (sent < 0) {
            close(fd);
            fd = -1;
            *persistent_fd = -1;
        } else {
            sent = send(fd, body, body_len, MSG_NOSIGNAL);
            if (sent < 0) {
                close(fd);
                fd = -1;
                *persistent_fd = -1;
            }
        }
    }

    /* (Re)connect if needed */
    if (fd < 0) {
        struct addrinfo hints = { .ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM };
        struct addrinfo* res = NULL;
        if (getaddrinfo(host, port, &hints, &res) != 0 || !res)
            return -1;

        fd = socket(res->ai_family, SOCK_STREAM, 0);
        if (fd < 0) {
            freeaddrinfo(res);
            return -1;
        }

        struct timeval tv = {
            .tv_sec = timeout_ms / 1000,
            .tv_usec = (timeout_ms % 1000) * 1000
        };
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        /* Enable TCP keep-alive */
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));

        if (connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
            freeaddrinfo(res);
            close(fd);
            return -1;
        }
        freeaddrinfo(res);

        /* Send on new connection */
        char header[512];
        int hlen = snprintf(header, sizeof(header),
            "POST %s HTTP/1.1\r\n"
            "Host: %s:%s\r\n"
            "Content-Type: %s\r\n"
            "Content-Length: %zu\r\n"
            "Connection: keep-alive\r\n"
            "\r\n",
            path, host, port, content_type, body_len);

        ssize_t sent = send(fd, header, (size_t)hlen, MSG_NOSIGNAL);
        if (sent < 0) { close(fd); return -1; }

        sent = send(fd, body, body_len, MSG_NOSIGNAL);
        if (sent < 0) { close(fd); return -1; }
    }

    /* Read status line */
    char resp[256];
    ssize_t nread = recv(fd, resp, sizeof(resp) - 1, 0);
    if (nread <= 0) {
        close(fd);
        *persistent_fd = -1;
        return -1;
    }
    resp[nread] = '\0';

    /* Drain remainder (Content-Length body) so connection is reusable */
    /* Parse Content-Length if present, skip remainder */
    char *cl = strstr(resp, "Content-Length:");
    if (!cl) cl = strstr(resp, "content-length:");
    if (cl) {
        /* Find end of headers (\r\n\r\n) in what we already read */
        char *hdr_end = strstr(resp, "\r\n\r\n");
        if (hdr_end) {
            size_t hdr_len = (size_t)(hdr_end + 4 - resp);
            int content_len = 0;
            sscanf(cl + 15, "%d", &content_len);
            size_t body_already = (size_t)nread - hdr_len;
            size_t remaining = (content_len > (int)body_already) ?
                               (size_t)content_len - body_already : 0;
            /* Drain remaining response body */
            char drain[1024];
            while (remaining > 0) {
                ssize_t r = recv(fd, drain, remaining < sizeof(drain) ?
                                 remaining : sizeof(drain), 0);
                if (r <= 0) {
                    close(fd);
                    *persistent_fd = -1;
                    break;
                }
                remaining -= (size_t)r;
            }
        }
    }

    *persistent_fd = fd;

    int status = 0;
    if (sscanf(resp, "HTTP/%*d.%*d %d", &status) != 1)
        return -1;

    return status;
}

/** Check if HTTP status is retryable (server error or throttled). */
static bool http_status_retryable(int status) {
    return status == 429 || status == 502 || status == 503 || status == 504
        || status == -1;
}

/* ============================================================================
 * Tracer (Global Singleton)
 * ============================================================================ */

struct keel_tracer {
    keel_trace_config_t config;
    uint32_t            num_workers;
    keel_span_ring_t*   rings;          /* Array[num_workers] */
    keel_tracer_stats_t stats;
    _Atomic(bool)       runtime_enabled; /* Toggle via admin cmd */

    /* Exporter thread */
    pthread_t           exporter_thread;
    _Atomic(bool)       exporter_running;
    pthread_mutex_t     flush_mutex;
    pthread_cond_t      flush_cond;
};

/**
 * @brief Background exporter thread — drains span rings and ships via OTLP/HTTP.
 *
 * Wakes at each flush_interval_ms, drains all per-worker ring buffers in
 * batches, builds OTLP JSON, and POSTs to config.endpoint.  Performs a final
 * drain on shutdown before exiting.
 *
 * @param arg  Pointer to the owning keel_tracer_t.
 * @return NULL.
 */
static void* exporter_thread_func(void* arg) {
    keel_tracer_t* tracer = (keel_tracer_t*)arg;
    const uint32_t batch_size = tracer->config.batch_size;
    const bool use_protobuf = (tracer->config.protocol == KEEL_OTLP_HTTP_PROTOBUF);
    const char* content_type = use_protobuf ? "application/x-protobuf"
                                            : "application/json";
    keel_span_t* batch = keel_malloc(batch_size * sizeof(keel_span_t));
    if (!batch) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_STATS, "Trace exporter: failed to allocate batch buffer");
        return NULL;
    }

    int persistent_fd = -1;  /* Keep-alive connection to OTLP collector */

    KEEL_LOG_INFO(KEEL_LOG_CAT_STATS,
                  "Trace exporter thread started (endpoint=%s, protocol=%s, flush_interval=%ums)",
                  tracer->config.endpoint,
                  use_protobuf ? "http/protobuf" : "http/json",
                  tracer->config.flush_interval_ms);

    while (atomic_load_explicit(&tracer->exporter_running, memory_order_acquire)) {
        /* Wait for flush interval or shutdown signal */
        {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            uint64_t ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
            ns += (uint64_t)tracer->config.flush_interval_ms * 1000000ULL;
            ts.tv_sec = (time_t)(ns / 1000000000ULL);
            ts.tv_nsec = (long)(ns % 1000000000ULL);

            pthread_mutex_lock(&tracer->flush_mutex);
            /* Guard against lost-signal race: if destroy() set exporter_running=false
             * and signalled the cond *before* we reached this wait (e.g. under
             * sanitiser thread-startup overhead), skip the timed wait entirely so
             * we don't block for the full flush_interval_ms. */
            if (atomic_load_explicit(&tracer->exporter_running, memory_order_acquire))
                pthread_cond_timedwait(&tracer->flush_cond, &tracer->flush_mutex, &ts);
            pthread_mutex_unlock(&tracer->flush_mutex);
        }

        /* Drain all worker rings */
        size_t total_drained = 0;
        for (uint32_t w = 0; w < tracer->num_workers; w++) {
            size_t n;
            while ((n = keel_span_ring_pop_batch(&tracer->rings[w], batch, batch_size)) > 0) {
                total_drained += n;

                /* Build payload (JSON or protobuf) */
                void*  payload = NULL;
                size_t payload_len = 0;
                if (use_protobuf) {
                    payload = keel_otlp_build_protobuf(batch, n,
                                                        tracer->config.service_name,
                                                        &payload_len);
                } else {
                    payload = keel_otlp_build_json(batch, n,
                                                   tracer->config.service_name,
                                                   &payload_len);
                }
                if (!payload) {
                    atomic_fetch_add_explicit(&tracer->stats.export_errors, 1,
                                             memory_order_relaxed);
                    continue;
                }

                int status = http_post_keepalive(
                                           tracer->config.endpoint,
                                           content_type,
                                           payload, payload_len,
                                           tracer->config.export_timeout_ms,
                                           &persistent_fd);

                /* Retry with exponential backoff on transient failures */
                for (int retry = 0; retry < 3 && http_status_retryable(status); retry++) {
                    usleep((useconds_t)(100000 << retry)); /* 100ms, 200ms, 400ms */
                    status = http_post_keepalive(
                                           tracer->config.endpoint,
                                           content_type,
                                           payload, payload_len,
                                           tracer->config.export_timeout_ms,
                                           &persistent_fd);
                }
                keel_free(payload);

                atomic_fetch_add_explicit(&tracer->stats.export_batches, 1,
                                         memory_order_relaxed);

                if (status >= 200 && status < 300) {
                    atomic_fetch_add_explicit(&tracer->stats.spans_exported, n,
                                             memory_order_relaxed);
                } else {
                    atomic_fetch_add_explicit(&tracer->stats.export_errors, 1,
                                             memory_order_relaxed);
                    KEEL_LOG_WARN(KEEL_LOG_CAT_STATS,
                                  "OTLP export failed: HTTP %d (%zu spans)",
                                  status, n);
                }
            }
        }

        if (total_drained > 0) {
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_STATS,
                          "Trace exporter flushed %zu spans", total_drained);
        }
    }

    /* Final drain on shutdown */
    for (uint32_t w = 0; w < tracer->num_workers; w++) {
        size_t n;
        while ((n = keel_span_ring_pop_batch(&tracer->rings[w], batch, batch_size)) > 0) {
            void*  payload = NULL;
            size_t payload_len = 0;
            if (use_protobuf) {
                payload = keel_otlp_build_protobuf(batch, n,
                                                    tracer->config.service_name,
                                                    &payload_len);
            } else {
                payload = keel_otlp_build_json(batch, n,
                                               tracer->config.service_name,
                                               &payload_len);
            }
            if (payload) {
                http_post_keepalive(tracer->config.endpoint, content_type,
                                    payload, payload_len,
                                    tracer->config.export_timeout_ms,
                                    &persistent_fd);
                keel_free(payload);
            }
        }
    }

    if (persistent_fd >= 0) close(persistent_fd);

    keel_free(batch);
    KEEL_LOG_INFO(KEEL_LOG_CAT_STATS, "Trace exporter thread stopped");
    return NULL;
}

/**
 * @brief Create a tracer with per-worker SPSC rings and a background exporter.
 *
 * Allocates @p num_workers ring buffers sized config->ring_capacity, starts
 * the exporter thread, and returns the tracer.  Returns NULL when
 * config->enabled is false or on allocation/thread-start failure.
 *
 * @param config       Tracing configuration (endpoint, sample rate, etc.).
 * @param num_workers  Number of per-worker ring buffers to allocate.
 * @return Heap-allocated tracer, or NULL on failure.
 */
keel_tracer_t* keel_tracer_create(const keel_trace_config_t* config,
                                  uint32_t num_workers) {
    if (!config || !config->enabled || num_workers == 0)
        return NULL;

    keel_tracer_t* tracer = keel_calloc(1, sizeof(keel_tracer_t));
    if (!tracer)
        return NULL;

    tracer->config = *config;
    tracer->num_workers = num_workers;

    /* Allocate per-worker rings */
    tracer->rings = keel_calloc(num_workers, sizeof(keel_span_ring_t));
    if (!tracer->rings) {
        keel_free(tracer);
        return NULL;
    }

    for (uint32_t i = 0; i < num_workers; i++) {
        if (keel_span_ring_init(&tracer->rings[i], config->ring_capacity) != 0) {
            for (uint32_t j = 0; j < i; j++)
                keel_span_ring_destroy(&tracer->rings[j]);
            keel_free(tracer->rings);
            keel_free(tracer);
            return NULL;
        }
    }

    /* Init stats */
    atomic_store(&tracer->stats.spans_created, 0);
    atomic_store(&tracer->stats.spans_exported, 0);
    atomic_store(&tracer->stats.spans_dropped, 0);
    atomic_store(&tracer->stats.export_errors, 0);
    atomic_store(&tracer->stats.export_batches, 0);
    atomic_store_explicit(&tracer->runtime_enabled, true, memory_order_release);

    /* Start exporter thread */
    pthread_mutex_init(&tracer->flush_mutex, NULL);
    pthread_cond_init(&tracer->flush_cond, NULL);
    atomic_store_explicit(&tracer->exporter_running, true, memory_order_release);

    if (pthread_create(&tracer->exporter_thread, NULL,
                       exporter_thread_func, tracer) != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_STATS, "Failed to create trace exporter thread");
        for (uint32_t i = 0; i < num_workers; i++)
            keel_span_ring_destroy(&tracer->rings[i]);
        keel_free(tracer->rings);
        keel_free(tracer);
        return NULL;
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_STATS,
                  "Tracer created: workers=%u, ring_capacity=%u, sample_rate=%u ppm",
                  num_workers, config->ring_capacity, config->sample_rate_ppm);

    return tracer;
}

/**
 * @brief Destroy a tracer, flushing all buffered spans first.
 *
 * Signals the exporter thread to stop, waits for it to complete its final
 * drain, then frees all ring buffers and the tracer itself.
 *
 * @param tracer  Tracer to destroy; no-op if NULL.
 */
void keel_tracer_destroy(keel_tracer_t* tracer) {
    if (!tracer)
        return;

    /* Signal exporter to stop */
    atomic_store_explicit(&tracer->exporter_running, false, memory_order_release);
    pthread_mutex_lock(&tracer->flush_mutex);
    pthread_cond_signal(&tracer->flush_cond);
    pthread_mutex_unlock(&tracer->flush_mutex);

    pthread_join(tracer->exporter_thread, NULL);
    pthread_mutex_destroy(&tracer->flush_mutex);
    pthread_cond_destroy(&tracer->flush_cond);

    /* Free rings */
    for (uint32_t i = 0; i < tracer->num_workers; i++)
        keel_span_ring_destroy(&tracer->rings[i]);
    keel_free(tracer->rings);

    KEEL_LOG_INFO(KEEL_LOG_CAT_STATS, "Tracer destroyed: exported=%lu, dropped=%lu, errors=%lu",
                  (unsigned long)atomic_load(&tracer->stats.spans_exported),
                  (unsigned long)atomic_load(&tracer->stats.spans_dropped),
                  (unsigned long)atomic_load(&tracer->stats.export_errors));

    keel_free(tracer);
}

/**
 * @brief Submit a finished span to the specified worker's ring buffer.
 *
 * Increments the spans_created counter; if the ring is full the span is
 * dropped and spans_dropped is incremented.
 *
 * @param tracer     Owning tracer.
 * @param worker_id  Index of the worker ring (must be < num_workers).
 * @param span       Completed span to enqueue.
 * @return true if the span was enqueued; false if dropped or args invalid.
 */
bool keel_tracer_submit(keel_tracer_t* tracer, uint32_t worker_id,
                        const keel_span_t* span) {
    if (!tracer || worker_id >= tracer->num_workers)
        return false;

    atomic_fetch_add_explicit(&tracer->stats.spans_created, 1,
                             memory_order_relaxed);

    if (!keel_span_ring_push(&tracer->rings[worker_id], span)) {
        atomic_fetch_add_explicit(&tracer->stats.spans_dropped, 1,
                                 memory_order_relaxed);
        return false;
    }

    return true;
}

/**
 * @brief Make a probabilistic sampling decision.
 *
 * Returns true with probability sample_rate_ppm / 1 000 000.  Always returns
 * false when the tracer is NULL or sample_rate_ppm is 0; always true when
 * sample_rate_ppm ≥ 1 000 000.
 *
 * @param tracer  Tracer whose config holds the sample rate.
 * @return true if this span should be recorded.
 */
bool keel_tracer_should_sample(const keel_tracer_t* tracer) {
    if (!tracer || tracer->config.sample_rate_ppm == 0)
        return false;
    if (!atomic_load_explicit(&tracer->runtime_enabled, memory_order_relaxed))
        return false;
    if (tracer->config.sample_rate_ppm >= 1000000)
        return true;

    /* Use lower 20 bits of PRNG for parts-per-million decision */
    uint64_t r = prng_next();
    return (r % 1000000) < tracer->config.sample_rate_ppm;
}

/**
 * @brief Return a pointer to the tracer's statistics.
 *
 * @param tracer  Tracer to query; may be NULL.
 * @return Pointer to keel_tracer_stats_t, or NULL if @p tracer is NULL.
 */
const keel_tracer_stats_t* keel_tracer_get_stats(const keel_tracer_t* tracer) {
    return tracer ? &tracer->stats : NULL;
}

void keel_tracer_set_enabled(keel_tracer_t* tracer, bool enabled) {
    if (tracer)
        atomic_store_explicit(&tracer->runtime_enabled, enabled, memory_order_release);
}

bool keel_tracer_is_enabled(const keel_tracer_t* tracer) {
    return tracer && atomic_load_explicit(&tracer->runtime_enabled, memory_order_acquire);
}

void keel_tracer_set_sample_rate(keel_tracer_t* tracer, uint32_t ppm) {
    if (tracer) {
        /* Not perfectly atomic with should_sample, but safe enough —
         * the PRNG check is already racy by nature (sampling). */
        tracer->config.sample_rate_ppm = ppm > 1000000 ? 1000000 : ppm;
    }
}
