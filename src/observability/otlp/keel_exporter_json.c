/**
 * @file keel_exporter_json.c
 */
#include "keel_exporter_json.h"

#include <stdbool.h>
#include <stdio.h>

const char* keel_otlp_http_result_str(int status)
{
    switch ((keel_otlp_http_result_t)status) {
    case KEEL_OTLP_HTTP_OK:               return "ok";
    case KEEL_OTLP_HTTP_CONNECT_FAILED:   return "connect_failed";
    case KEEL_OTLP_HTTP_TIMEOUT:          return "timeout";
    case KEEL_OTLP_HTTP_PROTOCOL_ERROR:   return "protocol_error";
    case KEEL_OTLP_HTTP_SERVER_REJECT:    return "server_reject";
    case KEEL_OTLP_HTTP_SERVER_RETRY:     return "server_retry";
    case KEEL_OTLP_HTTP_NOT_IMPLEMENTED:  return "not_implemented";
    default:                              return "unknown";
    }
}

int keel_exporter_stats_to_json(const keel_exporter_stats_t* s,
                                char* out, size_t out_cap)
{
    if (!s || !out || out_cap == 0)
        return -1;

    const char* status_str = keel_otlp_http_result_str((int)s->last_status);
    /* last_status==0 (initial) reports "none" rather than the OK string,
     * so observers can distinguish "no export yet" from a successful one. */
    bool        no_attempt_yet = (s->attempts == 0);
    bool        last_ok        = ((keel_otlp_http_result_t)s->last_status ==
                                  KEEL_OTLP_HTTP_OK);
    if (no_attempt_yet)
        status_str = "none";

    /* last_export_error: JSON null when no attempt yet or last was OK;
     * otherwise a quoted string with the error code. */
    char err_field[64];
    if (no_attempt_yet || last_ok)
        snprintf(err_field, sizeof(err_field), "null");
    else
        snprintf(err_field, sizeof(err_field), "\"%s\"", status_str);

    return snprintf(out, out_cap,
        "{"
        "\"export_queue_depth\":%llu,"
        "\"export_queue_capacity\":%llu,"
        "\"export_snapshots_dropped_total\":%llu,"
        "\"export_attempts_total\":%llu,"
        "\"export_success_total\":%llu,"
        "\"export_failure_total\":%llu,"
        "\"export_timeout_total\":%llu,"
        "\"last_export_duration_ns\":%llu,"
        "\"last_export_status\":\"%s\","
        "\"last_export_error\":%s,"
        "\"last_success_timestamp_ms\":%llu,"
        "\"last_failure_timestamp_ms\":%llu"
        "}",
        (unsigned long long)s->queue_depth,
        (unsigned long long)s->queue_capacity,
        (unsigned long long)s->dropped,
        (unsigned long long)s->attempts,
        (unsigned long long)s->successes,
        (unsigned long long)s->failures,
        (unsigned long long)s->timeouts,
        (unsigned long long)s->last_duration_ns,
        status_str,
        err_field,
        (unsigned long long)s->last_success_unix_ms,
        (unsigned long long)s->last_failure_unix_ms);
}
