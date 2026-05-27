/**
 * @file keel_otlp_aggregator.h
 * @brief Periodic stats → OTLP-snapshot aggregator (cold-path background thread).
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Bridges the per-worker `keel_stats_collector_t` with the OTLP exporter
 * pipeline (`keel_otlp_exporter_t`). Every `interval_ms`:
 *   1. `keel_stats_snapshot_take()` aggregates worker-local counters.
 *   2. `keel_otlp_snapshot_from_stats()` projects the aggregated snapshot
 *      into a fixed list of OTLP cumulative metric samples.
 *   3. `keel_otlp_exporter_submit()` enqueues the result (drop-oldest).
 *
 * Lives on its own pthread — never on the reactor; never on a worker.
 */
#ifndef KEEL_OTLP_AGGREGATOR_H
#define KEEL_OTLP_AGGREGATOR_H

#include <stdint.h>

#include "keel_otlp_encode.h"
#include "keel_otlp_exporter.h"

struct keel_stats_snapshot;
struct keel_stats_collector;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Project an aggregated stats snapshot into a fixed OTLP snapshot.
 *
 * Emits a curated set of cumulative counters and gauges (well under
 * `KEEL_OTLP_MAX_METRICS`). All metric values are stored as `uint64_t`.
 * Negative gauge values are clamped to 0.
 *
 * @param in                    Aggregated stats snapshot (non-NULL).
 * @param start_time_unix_nano  Process/exporter start (cumulative anchor).
 * @param time_unix_nano        Snapshot wall-clock time.
 * @param out                   Destination OTLP snapshot (non-NULL).
 * @return 0 on success, -1 on invalid arguments.
 */
int keel_otlp_snapshot_from_stats(
    const struct keel_stats_snapshot* in,
    uint64_t start_time_unix_nano,
    uint64_t time_unix_nano,
    keel_otlp_snapshot_t* out);

/* ------------------------------------------------------------------------- */

typedef struct keel_otlp_aggregator keel_otlp_aggregator_t;

/**
 * @brief Construct an aggregator. Caller retains ownership of `collector`
 *        and `exporter`; both must outlive the aggregator.
 *
 * @return Handle on success, NULL on allocation failure / bad arguments.
 */
keel_otlp_aggregator_t* keel_otlp_aggregator_create(
    struct keel_stats_collector* collector,
    keel_otlp_exporter_t* exporter,
    uint32_t interval_ms);

/**
 * @brief Spawn the background thread and begin periodic aggregation+submit.
 * @return 0 on success, -1 on failure.
 */
int keel_otlp_aggregator_start(keel_otlp_aggregator_t* agg);

/**
 * @brief Signal the background thread to stop and join it. Idempotent.
 *        Safe to call from a signal handler context only via the wrapper
 *        in keel_main; the implementation itself uses pthread primitives.
 */
void keel_otlp_aggregator_stop(keel_otlp_aggregator_t* agg);

/**
 * @brief Free all aggregator resources. Calls `stop()` if still running.
 */
void keel_otlp_aggregator_destroy(keel_otlp_aggregator_t* agg);

/**
 * @brief Return the number of aggregation ticks completed since start.
 *        Useful for tests and live introspection. Returns 0 on NULL.
 */
uint64_t keel_otlp_aggregator_ticks(const keel_otlp_aggregator_t* agg);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_OTLP_AGGREGATOR_H */
