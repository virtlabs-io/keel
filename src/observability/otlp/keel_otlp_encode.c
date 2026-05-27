/**
 * @file keel_otlp_encode.c
 * @brief nanopb-based OTLP metrics encoder.
 *
 * Builds an ExportMetricsServiceRequest containing exactly one
 * ResourceMetrics → one ScopeMetrics → @c snapshot->metric_count Sum
 * metrics, each with a single cumulative int64 NumberDataPoint. The
 * payload shape mirrors the canonical schema in
 * proposals/v0.2-alpha_observability.md §1190-1320.
 *
 * All repeated/string proto fields are emitted via nanopb encode
 * callbacks so we never materialise an intermediate object tree.
 */
#include "keel_otlp_encode.h"

#include "opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h"
#include "opentelemetry/proto/metrics/v1/metrics.pb.h"
#include "opentelemetry/proto/resource/v1/resource.pb.h"
#include "opentelemetry/proto/common/v1/common.pb.h"

#include <pb_encode.h>

#include <string.h>

/* ------------------------------------------------------------------------- */
/* Callback helpers                                                           */
/* ------------------------------------------------------------------------- */

static bool encode_cstring(pb_ostream_t* stream,
                           const pb_field_iter_t* field,
                           void* const* arg)
{
    /* nanopb passes arg = &pb_callback_t::arg; the user pointer lives
     * in *arg (a void*). We stored the string pointer directly. */
    const char* s = (const char*)(*arg);
    if (!s)
        return true;
    if (!pb_encode_tag_for_field(stream, field))
        return false;
    return pb_encode_string(stream, (const uint8_t*)s, strlen(s));
}

typedef struct metric_ctx {
    const keel_otlp_metric_sample_t* sample;
    uint64_t start_time_unix_nano;
    uint64_t time_unix_nano;
} metric_ctx_t;

static bool encode_one_number_data_point(pb_ostream_t* stream,
                                         const pb_field_iter_t* field,
                                         void* const* arg)
{
    const metric_ctx_t* mc = (const metric_ctx_t*)(*arg);
    opentelemetry_proto_metrics_v1_NumberDataPoint dp =
        opentelemetry_proto_metrics_v1_NumberDataPoint_init_zero;
    dp.start_time_unix_nano = mc->start_time_unix_nano;
    dp.time_unix_nano       = mc->time_unix_nano;
    dp.which_value          = opentelemetry_proto_metrics_v1_NumberDataPoint_as_int_tag;
    dp.value.as_int         = (int64_t)mc->sample->value;
    /* attributes/exemplars callbacks left NULL — nanopb treats that as empty. */

    if (!pb_encode_tag_for_field(stream, field))
        return false;
    return pb_encode_submessage(
        stream,
        opentelemetry_proto_metrics_v1_NumberDataPoint_fields,
        &dp);
}

typedef struct snapshot_ctx {
    const keel_otlp_snapshot_t* snap;
} snapshot_ctx_t;

static bool encode_metrics_list(pb_ostream_t* stream,
                                const pb_field_iter_t* field,
                                void* const* arg)
{
    const snapshot_ctx_t* sc = *(const snapshot_ctx_t* const*)arg;
    const keel_otlp_snapshot_t* snap = sc->snap;

    for (size_t i = 0; i < snap->metric_count; ++i) {
        metric_ctx_t mc = {
            .sample               = &snap->metrics[i],
            .start_time_unix_nano = snap->start_time_unix_nano,
            .time_unix_nano       = snap->time_unix_nano,
        };

        opentelemetry_proto_metrics_v1_Sum sum =
            opentelemetry_proto_metrics_v1_Sum_init_zero;
        sum.aggregation_temporality =
            opentelemetry_proto_metrics_v1_AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE;
        sum.is_monotonic = true;
        sum.data_points.funcs.encode = encode_one_number_data_point;
        sum.data_points.arg          = &mc;

        static const char empty[] = "";

        opentelemetry_proto_metrics_v1_Metric metric =
            opentelemetry_proto_metrics_v1_Metric_init_zero;
        metric.name.funcs.encode        = encode_cstring;
        metric.name.arg                 = (void*)snap->metrics[i].name;
        metric.description.funcs.encode = encode_cstring;
        metric.description.arg          = (void*)empty;
        metric.unit.funcs.encode        = encode_cstring;
        metric.unit.arg                 = (void*)empty;
        metric.which_data = opentelemetry_proto_metrics_v1_Metric_sum_tag;
        metric.data.sum   = sum;

        if (!pb_encode_tag_for_field(stream, field))
            return false;
        if (!pb_encode_submessage(
                stream,
                opentelemetry_proto_metrics_v1_Metric_fields,
                &metric))
            return false;
    }
    return true;
}

static bool encode_scope_metrics_list(pb_ostream_t* stream,
                                      const pb_field_iter_t* field,
                                      void* const* arg)
{
    const snapshot_ctx_t* sc = *(const snapshot_ctx_t* const*)arg;

    opentelemetry_proto_metrics_v1_ScopeMetrics sm =
        opentelemetry_proto_metrics_v1_ScopeMetrics_init_zero;
    sm.metrics.funcs.encode = encode_metrics_list;
    sm.metrics.arg          = (void*)sc;
    /* scope.name + schema_url left empty for v0.2-alpha. */

    if (!pb_encode_tag_for_field(stream, field))
        return false;
    return pb_encode_submessage(
        stream,
        opentelemetry_proto_metrics_v1_ScopeMetrics_fields,
        &sm);
}

static bool encode_resource_metrics_list(pb_ostream_t* stream,
                                         const pb_field_iter_t* field,
                                         void* const* arg);

/* Resource attributes per proposal §1190: identifies this process as a
 * KEEL instance to the collector. The set is fixed at compile time for
 * v0.2-alpha; user-supplied attributes are deferred. */
static const struct {
    const char* key;
    const char* value;
} k_resource_attrs[] = {
    { "service.name",        "keel" },
    { "service.version",     "0.2.0-alpha" },
    { "telemetry.sdk.name",  "keel-otlp" },
    { "telemetry.sdk.language", "c" },
};

static bool encode_kv_string_value(pb_ostream_t* stream,
                                   const pb_field_iter_t* field,
                                   void* const* arg)
{
    const char* s = (const char*)(*arg);
    if (!pb_encode_tag_for_field(stream, field))
        return false;
    return pb_encode_string(stream, (const uint8_t*)s, strlen(s));
}

static bool encode_resource_attributes(pb_ostream_t* stream,
                                       const pb_field_iter_t* field,
                                       void* const* arg)
{
    (void)arg;
    const size_t n = sizeof(k_resource_attrs) / sizeof(k_resource_attrs[0]);
    for (size_t i = 0; i < n; ++i) {
        opentelemetry_proto_common_v1_KeyValue kv =
            opentelemetry_proto_common_v1_KeyValue_init_zero;
        kv.key.funcs.encode = encode_cstring;
        kv.key.arg          = (void*)k_resource_attrs[i].key;
        kv.has_value        = true;
        kv.value.which_value =
            opentelemetry_proto_common_v1_AnyValue_string_value_tag;
        kv.value.value.string_value.funcs.encode = encode_kv_string_value;
        kv.value.value.string_value.arg          = (void*)k_resource_attrs[i].value;

        if (!pb_encode_tag_for_field(stream, field))
            return false;
        if (!pb_encode_submessage(
                stream,
                opentelemetry_proto_common_v1_KeyValue_fields,
                &kv))
            return false;
    }
    return true;
}

static bool encode_resource_metrics_list(pb_ostream_t* stream,
                                         const pb_field_iter_t* field,
                                         void* const* arg)
{
    const snapshot_ctx_t* sc = *(const snapshot_ctx_t* const*)arg;

    opentelemetry_proto_resource_v1_Resource res =
        opentelemetry_proto_resource_v1_Resource_init_zero;
    res.attributes.funcs.encode = encode_resource_attributes;
    res.attributes.arg          = NULL;

    opentelemetry_proto_metrics_v1_ResourceMetrics rm =
        opentelemetry_proto_metrics_v1_ResourceMetrics_init_zero;
    rm.has_resource = true;
    rm.resource     = res;
    rm.scope_metrics.funcs.encode = encode_scope_metrics_list;
    rm.scope_metrics.arg          = (void*)sc;

    if (!pb_encode_tag_for_field(stream, field))
        return false;
    return pb_encode_submessage(
        stream,
        opentelemetry_proto_metrics_v1_ResourceMetrics_fields,
        &rm);
}

/* ------------------------------------------------------------------------- */
/* Public API                                                                 */
/* ------------------------------------------------------------------------- */

keel_otlp_encode_result_t keel_otlp_encode_metrics(
    const keel_otlp_snapshot_t* snap,
    uint8_t* out_buf,
    size_t out_cap,
    size_t* out_len)
{
    if (!snap || !out_buf || !out_len)
        return KEEL_OTLP_ENCODE_INVALID_ARG;
    if (snap->metric_count > KEEL_OTLP_MAX_METRICS)
        return KEEL_OTLP_ENCODE_INVALID_ARG;

    snapshot_ctx_t sc = { .snap = snap };

    opentelemetry_proto_collector_metrics_v1_ExportMetricsServiceRequest req =
        opentelemetry_proto_collector_metrics_v1_ExportMetricsServiceRequest_init_zero;
    req.resource_metrics.funcs.encode = encode_resource_metrics_list;
    req.resource_metrics.arg          = (void*)&sc;

    pb_ostream_t stream = pb_ostream_from_buffer(out_buf, out_cap);
    if (!pb_encode(
            &stream,
            opentelemetry_proto_collector_metrics_v1_ExportMetricsServiceRequest_fields,
            &req))
    {
        if (stream.bytes_written >= out_cap)
            return KEEL_OTLP_ENCODE_BUFFER_TOO_SMALL;
        return KEEL_OTLP_ENCODE_PROTO_ERROR;
    }

    *out_len = stream.bytes_written;
    return KEEL_OTLP_ENCODE_OK;
}
