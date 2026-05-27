/**
 * @file keel_prom_format.h
 * @brief OTLP snapshot → Prometheus text exposition formatter (§23.2).
 *
 * The Prometheus compatibility exporter shares its metric model with the
 * OTLP exporter: both consume the same `keel_otlp_snapshot_t` produced
 * by `keel_otlp_snapshot_from_stats()`. This guarantees the two surfaces
 * never drift in names, units, or types.
 *
 * Per proposals/v0.2-alpha_observability.md §23.2 the formatter handles:
 *   - OpenTelemetry-style name → Prometheus name conversion (`.` → `_`).
 *   - HELP metadata for each metric.
 *   - TYPE metadata (counter | gauge) inferred from the metric name suffix
 *     (`_total` → counter; otherwise gauge).
 *   - Cumulative counter `_total` suffix preserved from the source name.
 *
 * Classic histogram bucket conversion and `_sum`/`_count` emission are
 * handled by the legacy `prom_write_histogram()` path in `src/admin/admin.c`
 * because `keel_otlp_snapshot_t` is restricted to scalar cumulative Sums
 * (per the v0.2-alpha OTLP encoder). Histograms remain on the legacy
 * `/metrics` endpoint until v0.3.
 */
#ifndef KEEL_PROM_FORMAT_H
#define KEEL_PROM_FORMAT_H

#include "keel_otlp_encode.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Render @p snap as Prometheus text exposition format 0.0.4.
 *
 * @param snap     Source snapshot (must be non-NULL).
 * @param out      Output character buffer (must be non-NULL).
 * @param out_cap  Capacity of @p out in bytes.
 * @return Bytes written to @p out (excluding the NUL terminator) on
 *         success, or a negative value on error:
 *           -1: invalid arguments,
 *           -2: output buffer too small.
 */
int keel_prom_format_snapshot(const keel_otlp_snapshot_t* snap,
                              char* out,
                              size_t out_cap);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PROM_FORMAT_H */
