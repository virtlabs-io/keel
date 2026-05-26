/**
 * @file keel_exporter_json.h
 * @brief JSON serializer for exporter self-stats (proposal §21).
 *
 * Pure formatter — no I/O, no allocation. The admin HTTP layer will
 * write the produced buffer onto the wire.
 *
 * Required keys (per §21):
 *   export_queue_depth
 *   export_queue_capacity
 *   export_snapshots_dropped_total
 *   export_attempts_total
 *   export_success_total
 *   export_failure_total
 *   export_timeout_total
 *   last_export_duration_ns
 *   last_export_status
 *   last_export_error
 *   last_success_timestamp_ms
 *   last_failure_timestamp_ms
 */
#ifndef KEEL_EXPORTER_JSON_H
#define KEEL_EXPORTER_JSON_H

#include <stddef.h>

#include "keel_exporter_stats.h"
#include "keel_otlp_http.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Write a JSON object describing @p stats into @p out.
 *
 *  Returns the number of bytes that would be written (excluding the
 *  trailing NUL), like snprintf. If the return value is >= @p out_cap,
 *  the output was truncated.
 *  Returns -1 on bad arguments.
 */
int keel_exporter_stats_to_json(const keel_exporter_stats_t* stats,
                                char* out, size_t out_cap);

/** Map a keel_otlp_http_result_t value to a short stable string. */
const char* keel_otlp_http_result_str(int status);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_EXPORTER_JSON_H */
