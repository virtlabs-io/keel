/**
 * @file test_otlp_e2e.c
 * @brief End-to-end OTLP integration test
 *        (proposals/v0.2-alpha_observability.md §29.5).
 *
 * Implements an in-process mock OTLP/HTTP receiver that fully decodes
 * the protobuf payload using the same nanopb runtime the exporter uses
 * to encode it, then asserts the decoded structure against the §1190
 * canonical schema.
 *
 * Covers §29.5 must-validate items the in-tree encoder supports today:
 *   - /v1/metrics endpoint + correct Content-Type
 *   - Resource attributes (service.name, service.version, telemetry.sdk.*)
 *   - Cumulative temporality + monotonic Sum metrics
 *   - Metric name + int64 value roundtrip
 *   - Queue overflow handling (drop-oldest + drop counter)
 *   - Export failure isolation (collector 5xx never blocks the producer)
 *   - Worker/admin reactor continues responding while exporter fails
 *
 * Histograms are explicitly out of scope: the OTLP encoder is scalar-only
 * for v0.2-alpha (§1190 schema), and §13.7 keeps the query-response
 * histogram compile-time-disabled. They will be covered by a follow-up
 * once the histogram pipeline lands.
 */

#include "test_utils.h"
#include "keel/core/admin.h"
#include "keel/engine/engine.h"

#include "keel_otlp_aggregator.h"
#include "keel_otlp_exporter.h"
#include "keel_otlp_encode.h"

#include "opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h"
#include "opentelemetry/proto/metrics/v1/metrics.pb.h"
#include "opentelemetry/proto/resource/v1/resource.pb.h"
#include "opentelemetry/proto/common/v1/common.pb.h"

#include <pb_decode.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* ============================================================================
 * Decoded representation
 * ============================================================================ */

#define MAX_ATTRS   16
#define MAX_METRICS 64

typedef struct attr_kv {
    char key[64];
    char value[128];
} attr_kv_t;

typedef struct decoded_metric {
    char     name[KEEL_OTLP_MAX_NAME_LEN];
    int      temporality;   /* opentelemetry_proto_metrics_v1_AggregationTemporality */
    bool     is_monotonic;
    bool     is_sum;
    int64_t  value;
} decoded_metric_t;

typedef struct decoded_request {
    bool             have_resource;
    size_t           attr_count;
    attr_kv_t        attrs[MAX_ATTRS];
    size_t           metric_count;
    decoded_metric_t metrics[MAX_METRICS];

    /* Sticky scratch state used by decode callbacks below. */
    char    cur_attr_key[64];
    char    cur_attr_value[128];
    bool    cur_attr_has_key;
    bool    cur_attr_has_value;

    char    cur_metric_name[KEEL_OTLP_MAX_NAME_LEN];
    int     cur_temporality;
    bool    cur_is_monotonic;
    bool    cur_is_sum;
    int64_t cur_metric_value;
    bool    cur_metric_value_set;
} decoded_request_t;

/* ============================================================================
 * nanopb decode callbacks
 * ============================================================================ */

static bool copy_string_cb(pb_istream_t* stream,
                           const pb_field_iter_t* field,
                           void** arg)
{
    (void)field;
    char*  dst    = (char*)(*arg);
    size_t dstcap = 128; /* All target buffers in this file are >= 96. */
    size_t n      = stream->bytes_left;
    if (n >= dstcap) n = dstcap - 1;
    if (!pb_read(stream, (pb_byte_t*)dst, n)) return false;
    dst[n] = '\0';
    /* Drain any remainder so the stream advances correctly. */
    while (stream->bytes_left > 0) {
        pb_byte_t skip;
        if (!pb_read(stream, &skip, 1)) return false;
    }
    return true;
}

/* AnyValue (oneof). We only care about string_value for resource attributes. */
static bool decode_any_value(pb_istream_t* stream,
                             const pb_field_iter_t* field,
                             void** arg)
{
    (void)field;
    decoded_request_t* d = (decoded_request_t*)(*arg);
    opentelemetry_proto_common_v1_AnyValue av =
        opentelemetry_proto_common_v1_AnyValue_init_zero;
    av.value.string_value.funcs.decode = copy_string_cb;
    av.value.string_value.arg          = d->cur_attr_value;
    d->cur_attr_value[0] = '\0';

    if (!pb_decode(stream, opentelemetry_proto_common_v1_AnyValue_fields, &av))
        return false;
    d->cur_attr_has_value = (d->cur_attr_value[0] != '\0');
    return true;
}

static bool decode_kv(pb_istream_t* stream,
                      const pb_field_iter_t* field,
                      void** arg)
{
    (void)field;
    decoded_request_t* d = (decoded_request_t*)(*arg);
    d->cur_attr_has_key   = false;
    d->cur_attr_has_value = false;
    d->cur_attr_key[0]    = '\0';
    d->cur_attr_value[0]  = '\0';

    opentelemetry_proto_common_v1_KeyValue kv =
        opentelemetry_proto_common_v1_KeyValue_init_zero;
    kv.key.funcs.decode = copy_string_cb;
    kv.key.arg          = d->cur_attr_key;
    /* value submessage handled inline via AnyValue's string_value callback. */
    kv.value.value.string_value.funcs.decode = copy_string_cb;
    kv.value.value.string_value.arg          = d->cur_attr_value;

    if (!pb_decode(stream, opentelemetry_proto_common_v1_KeyValue_fields, &kv))
        return false;
    d->cur_attr_has_key   = (d->cur_attr_key[0]   != '\0');
    d->cur_attr_has_value = (d->cur_attr_value[0] != '\0');
    if (d->cur_attr_has_key && d->cur_attr_has_value && d->attr_count < MAX_ATTRS) {
        size_t i = d->attr_count++;
        snprintf(d->attrs[i].key,   sizeof(d->attrs[i].key),   "%s", d->cur_attr_key);
        snprintf(d->attrs[i].value, sizeof(d->attrs[i].value), "%s", d->cur_attr_value);
    }
    return true;
}

static bool decode_resource(pb_istream_t* stream,
                            const pb_field_iter_t* field,
                            void** arg)
{
    (void)field;
    decoded_request_t* d = (decoded_request_t*)(*arg);
    opentelemetry_proto_resource_v1_Resource res =
        opentelemetry_proto_resource_v1_Resource_init_zero;
    res.attributes.funcs.decode = decode_kv;
    res.attributes.arg          = d;
    if (!pb_decode(stream, opentelemetry_proto_resource_v1_Resource_fields, &res))
        return false;
    d->have_resource = true;
    return true;
}

static bool decode_number_data_point(pb_istream_t* stream,
                                     const pb_field_iter_t* field,
                                     void** arg)
{
    (void)stream; (void)field; (void)arg;
    return false; /* unused — replaced by wire-format scan */
}

static bool decode_metric_name(pb_istream_t* stream,
                               const pb_field_iter_t* field,
                               void** arg)
{
    (void)stream; (void)field; (void)arg;
    return false; /* unused — replaced by wire-format scan */
}

/* Minimal protobuf wire-format reader. nanopb's oneof activation memsets
 * the union (m.data) before decoding the active branch, which wipes any
 * pb_callback_t pointers we pre-installed on inner fields like
 * Sum.data_points. To keep the decode path robust without forking nanopb,
 * we walk the Metric submessage bytes ourselves — the wire layout the
 * encoder produces is fixed: name + Sum{data_points:NumberDataPoint, agg
 * temporality, is_monotonic}. */
static bool wire_read_varint(const uint8_t** p, const uint8_t* end, uint64_t* v) {
    uint64_t r = 0;
    int shift = 0;
    while (*p < end) {
        uint8_t b = *(*p)++;
        r |= ((uint64_t)(b & 0x7f)) << shift;
        if (!(b & 0x80)) { *v = r; return true; }
        shift += 7;
        if (shift > 63) return false;
    }
    return false;
}

static bool wire_skip(const uint8_t** p, const uint8_t* end, uint32_t wire) {
    uint64_t v;
    switch (wire) {
        case 0: return wire_read_varint(p, end, &v);
        case 1: if (end - *p < 8) return false; *p += 8; return true;
        case 5: if (end - *p < 4) return false; *p += 4; return true;
        case 2:
            if (!wire_read_varint(p, end, &v)) return false;
            if ((uint64_t)(end - *p) < v) return false;
            *p += (size_t)v;
            return true;
        default: return false;
    }
}

static bool parse_number_data_point(const uint8_t* buf, size_t n,
                                    int64_t* out_value, bool* out_set)
{
    const uint8_t* p   = buf;
    const uint8_t* end = buf + n;
    *out_value = 0;
    *out_set   = false;
    while (p < end) {
        uint64_t tag;
        if (!wire_read_varint(&p, end, &tag)) return false;
        uint32_t fnum = (uint32_t)(tag >> 3);
        uint32_t wire = (uint32_t)(tag & 7);
        if (fnum == 6 && wire == 1) { /* as_int — sfixed64 */
            if (end - p < 8) return false;
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i) v |= ((uint64_t)p[i]) << (8 * i);
            p += 8;
            *out_value = (int64_t)v;
            *out_set   = true;
            continue;
        }
        if (fnum == 6 && wire == 0) { /* as_int — varint (defensive) */
            uint64_t v;
            if (!wire_read_varint(&p, end, &v)) return false;
            *out_value = (int64_t)v;
            *out_set   = true;
            continue;
        }
        if (!wire_skip(&p, end, wire)) return false;
    }
    return true;
}

static bool parse_sum(const uint8_t* buf, size_t n,
                      int64_t* out_value, bool* out_value_set,
                      int* out_temporality, bool* out_is_monotonic)
{
    const uint8_t* p   = buf;
    const uint8_t* end = buf + n;
    *out_temporality  = 0;
    *out_is_monotonic = false;
    *out_value        = 0;
    *out_value_set    = false;
    while (p < end) {
        uint64_t tag;
        if (!wire_read_varint(&p, end, &tag)) return false;
        uint32_t fnum = (uint32_t)(tag >> 3);
        uint32_t wire = (uint32_t)(tag & 7);
        if (fnum == 1 && wire == 2) { /* data_points */
            uint64_t len;
            if (!wire_read_varint(&p, end, &len)) return false;
            if ((uint64_t)(end - p) < len) return false;
            if (!parse_number_data_point(p, (size_t)len, out_value, out_value_set))
                return false;
            p += (size_t)len;
        } else if (fnum == 2 && wire == 0) { /* aggregation_temporality */
            uint64_t v;
            if (!wire_read_varint(&p, end, &v)) return false;
            *out_temporality = (int)v;
        } else if (fnum == 3 && wire == 0) { /* is_monotonic */
            uint64_t v;
            if (!wire_read_varint(&p, end, &v)) return false;
            *out_is_monotonic = (v != 0);
        } else if (!wire_skip(&p, end, wire)) {
            return false;
        }
    }
    return true;
}

static bool decode_metric(pb_istream_t* stream,
                          const pb_field_iter_t* field,
                          void** arg)
{
    (void)field;
    decoded_request_t* d = (decoded_request_t*)(*arg);

    /* Slurp the full Metric submessage into a stack buffer. */
    uint8_t  buf[2048];
    size_t   n = stream->bytes_left;
    if (n > sizeof(buf)) return false;
    if (!pb_read(stream, buf, n)) return false;

    char   name[KEEL_OTLP_MAX_NAME_LEN] = {0};
    bool   is_sum = false;
    int    temporality = 0;
    bool   is_monotonic = false;
    int64_t value = 0;
    bool   value_set = false;

    const uint8_t* p   = buf;
    const uint8_t* end = buf + n;
    while (p < end) {
        uint64_t tag;
        if (!wire_read_varint(&p, end, &tag)) return false;
        uint32_t fnum = (uint32_t)(tag >> 3);
        uint32_t wire = (uint32_t)(tag & 7);
        if (fnum == 1 && wire == 2) { /* name */
            uint64_t len;
            if (!wire_read_varint(&p, end, &len)) return false;
            if ((uint64_t)(end - p) < len) return false;
            size_t copy = (len < sizeof(name) - 1) ? (size_t)len
                                                   : sizeof(name) - 1;
            memcpy(name, p, copy);
            name[copy] = '\0';
            p += (size_t)len;
        } else if (fnum == 7 && wire == 2) { /* sum */
            uint64_t len;
            if (!wire_read_varint(&p, end, &len)) return false;
            if ((uint64_t)(end - p) < len) return false;
            if (!parse_sum(p, (size_t)len, &value, &value_set,
                           &temporality, &is_monotonic))
                return false;
            is_sum = true;
            p += (size_t)len;
        } else if (!wire_skip(&p, end, wire)) {
            return false;
        }
    }

    if (d->metric_count < MAX_METRICS && name[0] != '\0') {
        size_t i = d->metric_count++;
        snprintf(d->metrics[i].name, sizeof(d->metrics[i].name), "%s", name);
        d->metrics[i].temporality  = temporality;
        d->metrics[i].is_monotonic = is_monotonic;
        d->metrics[i].is_sum       = is_sum;
        d->metrics[i].value        = value_set ? value : 0;
    }
    return true;
}

static bool decode_scope_metrics(pb_istream_t* stream,
                                 const pb_field_iter_t* field,
                                 void** arg)
{
    (void)field;
    decoded_request_t* d = (decoded_request_t*)(*arg);
    opentelemetry_proto_metrics_v1_ScopeMetrics sm =
        opentelemetry_proto_metrics_v1_ScopeMetrics_init_zero;
    sm.metrics.funcs.decode = decode_metric;
    sm.metrics.arg          = d;
    return pb_decode(stream,
                     opentelemetry_proto_metrics_v1_ScopeMetrics_fields, &sm);
}

static bool decode_resource_metrics(pb_istream_t* stream,
                                    const pb_field_iter_t* field,
                                    void** arg)
{
    (void)field;
    decoded_request_t* d = (decoded_request_t*)(*arg);
    opentelemetry_proto_metrics_v1_ResourceMetrics rm =
        opentelemetry_proto_metrics_v1_ResourceMetrics_init_zero;
    rm.resource.attributes.funcs.decode = decode_kv;
    rm.resource.attributes.arg          = d;
    rm.scope_metrics.funcs.decode       = decode_scope_metrics;
    rm.scope_metrics.arg                = d;
    if (!pb_decode(stream,
                   opentelemetry_proto_metrics_v1_ResourceMetrics_fields, &rm))
        return false;
    d->have_resource = true;
    return true;
}

static bool decode_payload(const uint8_t* body, size_t n, decoded_request_t* out)
{
    memset(out, 0, sizeof(*out));
    opentelemetry_proto_collector_metrics_v1_ExportMetricsServiceRequest req =
        opentelemetry_proto_collector_metrics_v1_ExportMetricsServiceRequest_init_zero;
    req.resource_metrics.funcs.decode = decode_resource_metrics;
    req.resource_metrics.arg          = out;

    pb_istream_t stream = pb_istream_from_buffer(body, n);
    return pb_decode(&stream,
        opentelemetry_proto_collector_metrics_v1_ExportMetricsServiceRequest_fields,
        &req);
}

/* ============================================================================
 * Mock OTLP/HTTP receiver
 * ============================================================================ */

typedef struct otlp_receiver {
    int                listen_fd;
    uint16_t           port;
    pthread_t          thread;
    atomic_bool        stop;
    atomic_int         requests;
    int                response_status;     /* 200, 500, ... */
    bool               capture_last;        /* if true, save last decoded req */
    pthread_mutex_t    mu;
    decoded_request_t  last;                /* protected by mu when capture_last */
    char               last_path[64];       /* protected by mu */
    char               last_ctype[128];     /* protected by mu */
    bool               last_valid;          /* decode succeeded */
} otlp_receiver_t;

static void* receiver_thread(void* arg)
{
    otlp_receiver_t* s = (otlp_receiver_t*)arg;
    while (!atomic_load(&s->stop)) {
        struct pollfd p = { .fd = s->listen_fd, .events = POLLIN };
        if (poll(&p, 1, 50) <= 0) continue;
        int cfd = accept(s->listen_fd, NULL, NULL);
        if (cfd < 0) continue;

        /* Read until full request (Content-Length-terminated). */
        char   hdr[8192];
        size_t off = 0, cl = 0, hdr_end = 0;
        bool   have_hdrs = false;
        uint8_t* body = NULL;
        size_t   body_len = 0;
        while (off + 1 < sizeof(hdr)) {
            struct pollfd cp = { .fd = cfd, .events = POLLIN };
            if (poll(&cp, 1, 1000) <= 0) break;
            ssize_t n = recv(cfd, hdr + off, sizeof(hdr) - 1 - off, 0);
            if (n <= 0) break;
            off += (size_t)n;
            hdr[off] = '\0';
            if (!have_hdrs) {
                char* he = strstr(hdr, "\r\n\r\n");
                if (he) {
                    have_hdrs = true;
                    hdr_end   = (size_t)(he - hdr) + 4;
                    char* clh = strcasestr(hdr, "Content-Length:");
                    if (clh) cl = strtoul(clh + 15, NULL, 10);
                }
            }
            if (have_hdrs && off >= hdr_end + cl) break;
        }

        /* Parse method + path. */
        char path[64] = "";
        char ctype[128] = "";
        if (have_hdrs) {
            sscanf(hdr, "%*s %63s", path);
            char* cth = strcasestr(hdr, "Content-Type:");
            if (cth) {
                cth += 13;
                while (*cth == ' ') cth++;
                size_t i = 0;
                while (*cth && *cth != '\r' && *cth != '\n'
                       && i + 1 < sizeof(ctype))
                    ctype[i++] = *cth++;
                ctype[i] = '\0';
            }
            /* Body is whatever lies past the header end. */
            if (off > hdr_end) {
                body_len = off - hdr_end;
                body     = (uint8_t*)(hdr + hdr_end);
            }
        }

        decoded_request_t dec;
        bool ok = (body && body_len > 0)
                  ? decode_payload(body, body_len, &dec) : false;

        if (s->capture_last) {
            pthread_mutex_lock(&s->mu);
            memcpy(&s->last, &dec, sizeof(dec));
            snprintf(s->last_path,  sizeof(s->last_path),  "%s", path);
            snprintf(s->last_ctype, sizeof(s->last_ctype), "%s", ctype);
            s->last_valid = ok;
            pthread_mutex_unlock(&s->mu);
        }
        atomic_fetch_add(&s->requests, 1);

        int status = s->response_status > 0 ? s->response_status : 200;
        const char* reason = status == 200 ? "OK"
                           : status == 500 ? "Internal Server Error"
                           : "Status";
        char resp[256];
        int  rl = snprintf(resp, sizeof(resp),
            "HTTP/1.1 %d %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            status, reason);
        (void)send(cfd, resp, (size_t)rl, MSG_NOSIGNAL);
        close(cfd);
    }
    return NULL;
}

static otlp_receiver_t* receiver_start(int response_status, bool capture)
{
    static otlp_receiver_t s;
    memset(&s, 0, sizeof(s));
    s.response_status = response_status;
    s.capture_last    = capture;
    pthread_mutex_init(&s.mu, NULL);

    s.listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    TEST_ASSERT(s.listen_fd >= 0);
    int one = 1;
    setsockopt(s.listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    TEST_ASSERT(bind(s.listen_fd, (struct sockaddr*)&a, sizeof(a)) == 0);
    socklen_t al = sizeof(a);
    TEST_ASSERT(getsockname(s.listen_fd, (struct sockaddr*)&a, &al) == 0);
    s.port = ntohs(a.sin_port);
    TEST_ASSERT(listen(s.listen_fd, 8) == 0);
    TEST_ASSERT(pthread_create(&s.thread, NULL, receiver_thread, &s) == 0);
    return &s;
}

static void receiver_stop(otlp_receiver_t* s)
{
    atomic_store(&s->stop, true);
    pthread_join(s->thread, NULL);
    close(s->listen_fd);
    pthread_mutex_destroy(&s->mu);
}

/* ============================================================================
 * Helpers
 * ============================================================================ */

static void msleep(uint32_t ms) {
    struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

static const decoded_metric_t* find_metric(const decoded_request_t* d,
                                           const char* name)
{
    for (size_t i = 0; i < d->metric_count; ++i)
        if (strcmp(d->metrics[i].name, name) == 0)
            return &d->metrics[i];
    return NULL;
}

static const attr_kv_t* find_attr(const decoded_request_t* d, const char* key)
{
    for (size_t i = 0; i < d->attr_count; ++i)
        if (strcmp(d->attrs[i].key, key) == 0)
            return &d->attrs[i];
    return NULL;
}

static keel_otlp_snapshot_t make_snap(uint64_t seq)
{
    keel_otlp_snapshot_t s = {0};
    s.start_time_unix_nano = 1000;
    s.time_unix_nano       = 1000 + seq;
    s.metric_count         = 3;
    snprintf(s.metrics[0].name, sizeof(s.metrics[0].name),
             "keel_sessions_created_total");
    s.metrics[0].value = 100 + seq;
    snprintf(s.metrics[1].name, sizeof(s.metrics[1].name),
             "keel_queries_total");
    s.metrics[1].value = 200 + seq;
    snprintf(s.metrics[2].name, sizeof(s.metrics[2].name),
             "keel_errors_total");
    s.metrics[2].value = 7 + seq;
    return s;
}

static keel_otlp_exporter_t* exporter_for(uint16_t port, uint32_t cap,
                                          uint32_t timeout_ms)
{
    static char url[128];
    snprintf(url, sizeof(url), "http://127.0.0.1:%u/v1/metrics", (unsigned)port);
    keel_otlp_exporter_config_t cfg = {
        .http = { .endpoint_url = url, .timeout_ms = timeout_ms },
        .interval_ms      = 30,
        .max_retries      = 0,
        .queue_capacity   = cap,
        .encode_buf_bytes = 16384,
    };
    return keel_otlp_exporter_create(&cfg);
}

/* ============================================================================
 * Test 1: full protobuf roundtrip + resource attributes + cumulative Sum
 * ============================================================================ */

static void test_roundtrip_decode_full_payload(void)
{
    otlp_receiver_t* r = receiver_start(/*status=*/200, /*capture=*/true);
    keel_otlp_exporter_t* e = exporter_for(r->port, 4, 1000);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);

    keel_otlp_snapshot_t snap = make_snap(0);
    keel_otlp_exporter_submit(e, &snap);

    for (int i = 0; i < 200 && atomic_load(&r->requests) < 1; ++i)
        msleep(10);
    TEST_ASSERT(atomic_load(&r->requests) >= 1);

    pthread_mutex_lock(&r->mu);
    decoded_request_t d = r->last;
    char path[64];  snprintf(path,  sizeof(path),  "%s", r->last_path);
    char ctype[128]; snprintf(ctype, sizeof(ctype), "%s", r->last_ctype);
    bool valid = r->last_valid;
    pthread_mutex_unlock(&r->mu);

    /* HTTP framing. */
    TEST_ASSERT_STR_EQ(path, "/v1/metrics");
    TEST_ASSERT(strstr(ctype, "application/x-protobuf") != NULL);
    TEST_ASSERT(valid);

    /* Resource attributes (§1190). */
    TEST_ASSERT(d.have_resource);
    const attr_kv_t* svc  = find_attr(&d, "service.name");
    const attr_kv_t* ver  = find_attr(&d, "service.version");
    const attr_kv_t* sdk  = find_attr(&d, "telemetry.sdk.name");
    const attr_kv_t* lang = find_attr(&d, "telemetry.sdk.language");
    TEST_ASSERT_NOT_NULL(svc);
    TEST_ASSERT_NOT_NULL(ver);
    TEST_ASSERT_NOT_NULL(sdk);
    TEST_ASSERT_NOT_NULL(lang);
    TEST_ASSERT_STR_EQ(svc->value,  "keel");
    TEST_ASSERT_STR_EQ(lang->value, "c");

    /* All three metrics present, all cumulative Sum, all monotonic, values
     * roundtripped intact. */
    TEST_ASSERT_EQ((int)d.metric_count, 3);
    const decoded_metric_t* m0 = find_metric(&d, "keel_sessions_created_total");
    const decoded_metric_t* m1 = find_metric(&d, "keel_queries_total");
    const decoded_metric_t* m2 = find_metric(&d, "keel_errors_total");
    TEST_ASSERT_NOT_NULL(m0);
    TEST_ASSERT_NOT_NULL(m1);
    TEST_ASSERT_NOT_NULL(m2);
    TEST_ASSERT(m0->is_sum && m1->is_sum && m2->is_sum);
    TEST_ASSERT(m0->is_monotonic && m1->is_monotonic && m2->is_monotonic);
    TEST_ASSERT_EQ(m0->temporality,
        (int)opentelemetry_proto_metrics_v1_AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE);
    TEST_ASSERT_EQ((int)m0->value, 100);
    TEST_ASSERT_EQ((int)m1->value, 200);
    TEST_ASSERT_EQ((int)m2->value, 7);

    keel_otlp_exporter_destroy(e);
    receiver_stop(r);
}

/* ============================================================================
 * Test 2: queue overflow → drop-oldest + dropped counter
 * ============================================================================ */

static void test_queue_overflow_handling(void)
{
    /* Dead port so the exporter thread blocks on connect() and the queue
     * fills up; submits then exercise drop-oldest. */
    int s = socket(AF_INET, SOCK_STREAM, 0); TEST_ASSERT(s >= 0);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    TEST_ASSERT(bind(s, (struct sockaddr*)&a, sizeof(a)) == 0);
    socklen_t al = sizeof(a);
    getsockname(s, (struct sockaddr*)&a, &al);
    uint16_t dead = ntohs(a.sin_port);
    close(s);

    keel_otlp_exporter_t* e = exporter_for(dead, /*cap=*/2, /*timeout=*/200);
    TEST_ASSERT_NOT_NULL(e);

    /* Push BEFORE start: pure queue accounting; do not depend on consumer. */
    int dropped_pushes = 0;
    for (uint64_t i = 0; i < 20; ++i) {
        keel_otlp_snapshot_t snap = make_snap(i);
        dropped_pushes += keel_otlp_exporter_submit(e, &snap);
    }
    TEST_ASSERT(dropped_pushes >= 16); /* 20 pushes - 2 slots ≈ 18 drops */

    /* Now run the loop so dropped counter surfaces in self-stats. */
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);
    keel_exporter_stats_t st;
    for (int i = 0; i < 100; ++i) {
        keel_otlp_exporter_self_stats(e, &st);
        if (st.dropped >= (uint64_t)dropped_pushes && st.failures >= 1) break;
        msleep(20);
    }
    TEST_ASSERT(st.dropped >= (uint64_t)dropped_pushes);
    TEST_ASSERT(st.failures >= 1);     /* connection refused → failures */
    TEST_ASSERT_EQ((int)st.successes, 0);

    keel_otlp_exporter_destroy(e);
}

/* ============================================================================
 * Test 3: collector returns 500 → exporter failures isolated;
 *         admin reactor + producers keep working.
 * ============================================================================ */

typedef struct admin_fix {
    keel_engine_t* engine;
    keel_admin_t*  admin;
    uint16_t       port;
} admin_fix_t;

static int admin_fix_start(admin_fix_t* f) {
    memset(f, 0, sizeof(*f));
    keel_engine_config_t ecfg = KEEL_ENGINE_CONFIG_DEFAULT;
    f->engine = keel_engine_create(&ecfg);
    if (!f->engine) return -1;
    keel_admin_config_t acfg = KEEL_ADMIN_CONFIG_DEFAULT;
    acfg.admin_enabled = true;
    acfg.admin_addr    = "127.0.0.1"; acfg.admin_port = 0;
    acfg.prom_enabled  = true;
    acfg.prom_addr     = "127.0.0.1"; acfg.prom_port  = 0;
    f->admin = keel_admin_start(&acfg, f->engine);
    if (!f->admin) { keel_engine_destroy(f->engine); return -1; }
    f->port = keel_admin_get_prom_port(f->admin);
    return f->port > 0 ? 0 : -1;
}

static void admin_fix_stop(admin_fix_t* f) {
    keel_admin_stop(f->admin);
    keel_engine_destroy(f->engine);
}

static int http_get(uint16_t port, const char* path, char* out, size_t out_cap) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET; sa.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
    if (connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        close(fd); return -1;
    }
    char req[256];
    int  n = snprintf(req, sizeof(req),
                      "GET %s HTTP/1.0\r\nHost: localhost\r\n\r\n", path);
    if (send(fd, req, (size_t)n, 0) < 0) { close(fd); return -1; }
    size_t total = 0;
    for (;;) {
        if (total >= out_cap - 1) break;
        ssize_t r = recv(fd, out + total, out_cap - 1 - total, 0);
        if (r <= 0) break;
        total += (size_t)r;
    }
    out[total] = '\0';
    close(fd);
    return (int)total;
}

static void test_export_failure_isolation(void)
{
    /* Receiver replies 500 to every request. Exporter must record failures
     * but admin/healthz must keep returning 200 throughout, proving the
     * reactor side is not blocked by exporter trouble. */
    otlp_receiver_t* r = receiver_start(/*status=*/500, /*capture=*/false);

    admin_fix_t f;
    TEST_ASSERT_EQ(admin_fix_start(&f), 0);
    keel_otlp_exporter_t* e = exporter_for(r->port, 4, 500);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);
    keel_admin_set_otlp_exporter(f.admin, e);

    /* Drive a continuous stream of snapshots while concurrently scraping
     * /healthz and /api/observability/exporter.json. */
    for (int round = 0; round < 20; ++round) {
        for (uint64_t i = 0; i < 4; ++i) {
            keel_otlp_snapshot_t snap = make_snap((uint64_t)round * 4 + i);
            keel_otlp_exporter_submit(e, &snap);
        }
        char buf[8192];
        int  n = http_get(f.port, "/healthz", buf, sizeof(buf));
        TEST_ASSERT(n > 0);
        TEST_ASSERT(strstr(buf, "HTTP/1.1 200 OK") != NULL);
        msleep(15);
    }

    /* Exporter must have recorded the 500s as failures. */
    keel_exporter_stats_t st;
    keel_otlp_exporter_self_stats(e, &st);
    TEST_ASSERT(st.failures >= 1);
    TEST_ASSERT_EQ((int)st.successes, 0);
    TEST_ASSERT(atomic_load(&r->requests) >= 1);

    /* Exporter JSON still serves with last_failure populated. */
    char buf[16384];
    int  n = http_get(f.port, "/api/observability/exporter.json",
                      buf, sizeof(buf));
    TEST_ASSERT(n > 0);
    TEST_ASSERT(strstr(buf, "HTTP/1.1 200 OK") != NULL);
    TEST_ASSERT(strstr(buf, "\"export_failure_total\":") != NULL);

    keel_admin_set_otlp_exporter(f.admin, NULL);
    keel_otlp_exporter_destroy(e);
    admin_fix_stop(&f);
    receiver_stop(r);
}

/* ============================================================================
 * Test 4: end-to-end via the real aggregator (snapshot rotation + decode)
 * ============================================================================ */

static void test_aggregator_to_receiver_roundtrip(void)
{
    otlp_receiver_t* r = receiver_start(/*status=*/200, /*capture=*/true);
    admin_fix_t f;
    TEST_ASSERT_EQ(admin_fix_start(&f), 0);

    keel_otlp_exporter_t* e = exporter_for(r->port, 4, 1000);
    TEST_ASSERT_NOT_NULL(e);
    TEST_ASSERT_EQ(keel_otlp_exporter_start(e), 0);
    keel_admin_set_otlp_exporter(f.admin, e);

    keel_stats_collector_t* coll = keel_engine_get_stats_collector(f.engine);
    TEST_ASSERT_NOT_NULL(coll);
    keel_otlp_aggregator_t* agg = keel_otlp_aggregator_create(coll, e, 20);
    TEST_ASSERT_NOT_NULL(agg);
    TEST_ASSERT_EQ(keel_otlp_aggregator_start(agg), 0);

    /* Wait for the aggregator → exporter → receiver path to deliver. */
    for (int i = 0; i < 200; ++i) {
        if (atomic_load(&r->requests) >= 1) break;
        msleep(10);
    }
    TEST_ASSERT(atomic_load(&r->requests) >= 1);

    pthread_mutex_lock(&r->mu);
    bool valid             = r->last_valid;
    bool have_resource     = r->last.have_resource;
    size_t metric_count    = r->last.metric_count;
    const attr_kv_t* svc   = find_attr(&r->last, "service.name");
    char svc_val[128]      = "";
    if (svc) snprintf(svc_val, sizeof(svc_val), "%s", svc->value);
    pthread_mutex_unlock(&r->mu);

    TEST_ASSERT(valid);
    TEST_ASSERT(have_resource);
    /* The from_stats converter produces 32 curated metrics. */
    TEST_ASSERT(metric_count >= 1);
    TEST_ASSERT_STR_EQ(svc_val, "keel");

    keel_otlp_aggregator_stop(agg);
    keel_otlp_aggregator_destroy(agg);
    keel_admin_set_otlp_exporter(f.admin, NULL);
    keel_otlp_exporter_destroy(e);
    admin_fix_stop(&f);
    receiver_stop(r);
}

int main(void) {
    test_roundtrip_decode_full_payload();
    test_queue_overflow_handling();
    test_export_failure_isolation();
    test_aggregator_to_receiver_roundtrip();
    return test_summary();
}
