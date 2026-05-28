/**
 * @file replay_log.h
 * @brief Durable, redacted operational replay log for reproducing failures.
 *
 * The replay log is intentionally protocol-neutral. Protocol plugins may attach
 * payload hashes and semantic labels, but raw frontend/backend bytes are never
 * written by this API. Each record is newline-delimited JSON and can be used as
 * a deterministic fixture seed for postmortems and regression tests.
 */

#ifndef KEEL_UTIL_REPLAY_LOG_H
#define KEEL_UTIL_REPLAY_LOG_H

#include "keel_error.h"
#include "keel_types.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum keel_replay_event_type {
    KEEL_REPLAY_EVENT_FRONTEND_MESSAGE = 1,
    KEEL_REPLAY_EVENT_BACKEND_RESPONSE,
    KEEL_REPLAY_EVENT_ROUTE_DECISION,
    KEEL_REPLAY_EVENT_SEMANTIC_PLAN,
    KEEL_REPLAY_EVENT_STATE_TRANSITION,
    KEEL_REPLAY_EVENT_CID_TRANSITION,
    KEEL_REPLAY_EVENT_FAILOVER,
    KEEL_REPLAY_EVENT_CHAOS_EVENT,
    KEEL_REPLAY_EVENT_CHECKPOINT,
} keel_replay_event_type_t;

typedef struct keel_replay_log_config {
    const char* path;          /**< Destination NDJSON path. */
    bool        fsync_each;    /**< fsync after each append for crash fixtures. */
    uint64_t    max_bytes;     /**< Zero means unlimited. */
} keel_replay_log_config_t;

typedef struct keel_replay_log keel_replay_log_t;

typedef struct keel_replay_event {
    keel_replay_event_type_t type;

    uint64_t session_id;
    uint64_t transaction_id;
    uint64_t connection_id;
    uint64_t worker_id;

    uint64_t query_hash;
    uint64_t payload_hash;
    uint64_t payload_len;

    const char* route;
    const char* route_reason;
    const char* semantic_class;
    const char* safety_level;

    const char* state_domain;
    const char* old_state;
    const char* new_state;
    const char* outcome;
    const char* detail;
} keel_replay_event_t;

const char* keel_replay_event_type_name(keel_replay_event_type_t type);

uint64_t keel_replay_payload_hash(const void* payload, size_t len);

keel_error_t keel_replay_log_open(const keel_replay_log_config_t* config,
                                  keel_replay_log_t** out);

keel_error_t keel_replay_log_append(keel_replay_log_t* log,
                                    const keel_replay_event_t* event);

keel_error_t keel_replay_log_flush(keel_replay_log_t* log);

void keel_replay_log_close(keel_replay_log_t* log);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_UTIL_REPLAY_LOG_H */
