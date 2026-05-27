/**
 * @file keel_snapshot_queue.h
 * @brief Bounded queue between the aggregation tick and the OTLP exporter.
 *
 * Per proposal §20.6: when the queue is full, push drops the OLDEST
 * snapshot and replaces it with the newest, incrementing a drop counter.
 * Rationale: the newest snapshot is more useful than stale backlog and
 * exporter backpressure must never affect the worker hot path.
 *
 * Internally uses a mutex+condvar (cold-path; aggregation tick runs at
 * O(seconds) so lock overhead is irrelevant).
 */
#ifndef KEEL_SNAPSHOT_QUEUE_H
#define KEEL_SNAPSHOT_QUEUE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "keel_otlp_encode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct keel_snapshot_queue keel_snapshot_queue_t;

keel_snapshot_queue_t* keel_snapshot_queue_create(size_t capacity);
void                   keel_snapshot_queue_destroy(keel_snapshot_queue_t* q);

/** Push a snapshot. Returns 1 if the queue was full and the oldest snapshot
 *  was dropped to make room (drop counter incremented); 0 on a normal push. */
int                    keel_snapshot_queue_push(keel_snapshot_queue_t* q,
                                                const keel_otlp_snapshot_t* snap);

/** Try to pop the oldest snapshot without blocking. Returns 1 if one was
 *  copied to @p out; 0 if empty. */
int                    keel_snapshot_queue_try_pop(keel_snapshot_queue_t* q,
                                                   keel_otlp_snapshot_t* out);

/** Pop with a bounded wait. Returns 1 if popped, 0 on timeout, -1 if shut down. */
int                    keel_snapshot_queue_pop(keel_snapshot_queue_t* q,
                                               keel_otlp_snapshot_t* out,
                                               uint32_t timeout_ms);

/** Signal any waiter to exit. After this, pop returns -1 once drained. */
void                   keel_snapshot_queue_shutdown(keel_snapshot_queue_t* q);

size_t                 keel_snapshot_queue_depth(const keel_snapshot_queue_t* q);
size_t                 keel_snapshot_queue_capacity(const keel_snapshot_queue_t* q);
uint64_t               keel_snapshot_queue_dropped_total(const keel_snapshot_queue_t* q);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_SNAPSHOT_QUEUE_H */
