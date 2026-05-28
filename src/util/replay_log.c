/**
 * @file replay_log.c
 * @brief Durable NDJSON replay log writer.
 */

#include "keel/util/replay_log.h"

#include "keel/util/encoding.h"
#include "keel/util/util.h"
#include "keel/mem/mem.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

struct keel_replay_log {
    FILE*    fp;
    char     path[512];
    bool     fsync_each;
    uint64_t max_bytes;
    uint64_t bytes_written;
    uint64_t seq;
};

static uint64_t replay_now_ns(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return ((uint64_t)ts.tv_sec * 1000000000ULL) + (uint64_t)ts.tv_nsec;
}

static void escape_or_empty(char* dst, size_t dst_size, const char* src)
{
    keel_json_escape(dst, dst_size, src ? src : "");
}

const char* keel_replay_event_type_name(keel_replay_event_type_t type)
{
    switch (type) {
    case KEEL_REPLAY_EVENT_FRONTEND_MESSAGE:  return "frontend_message";
    case KEEL_REPLAY_EVENT_BACKEND_RESPONSE:  return "backend_response";
    case KEEL_REPLAY_EVENT_ROUTE_DECISION:    return "route_decision";
    case KEEL_REPLAY_EVENT_SEMANTIC_PLAN:     return "semantic_plan";
    case KEEL_REPLAY_EVENT_STATE_TRANSITION:  return "state_transition";
    case KEEL_REPLAY_EVENT_CID_TRANSITION:    return "cid_transition";
    case KEEL_REPLAY_EVENT_FAILOVER:          return "failover";
    case KEEL_REPLAY_EVENT_CHAOS_EVENT:       return "chaos_event";
    case KEEL_REPLAY_EVENT_CHECKPOINT:        return "checkpoint";
    default:                                  return "unknown";
    }
}

uint64_t keel_replay_payload_hash(const void* payload, size_t len)
{
    if (!payload || len == 0) {
        return 0;
    }
    return keel_hash_fnv1a_64(payload, len);
}

keel_error_t keel_replay_log_open(const keel_replay_log_config_t* config,
                                  keel_replay_log_t** out)
{
    if (!config || !config->path || !out) {
        return KEEL_ERR_INVALID_ARG;
    }
    *out = NULL;

    keel_replay_log_t* log = keel_calloc(1, sizeof *log);
    if (!log) {
        return KEEL_ERR_NOMEM;
    }

    snprintf(log->path, sizeof log->path, "%s", config->path);
    log->fsync_each = config->fsync_each;
    log->max_bytes = config->max_bytes;

    log->fp = fopen(config->path, "a");
    if (!log->fp) {
        keel_free(log);
        return KEEL_ERR_IO;
    }

    struct stat st;
    if (fstat(fileno(log->fp), &st) == 0 && st.st_size > 0) {
        log->bytes_written = (uint64_t)st.st_size;
    }

    *out = log;
    return KEEL_OK;
}

keel_error_t keel_replay_log_append(keel_replay_log_t* log,
                                    const keel_replay_event_t* event)
{
    if (!log || !log->fp || !event) {
        return KEEL_ERR_INVALID_ARG;
    }

    char route[128];
    char reason[128];
    char sem[64];
    char safety[64];
    char domain[64];
    char old_state[64];
    char new_state[64];
    char outcome[96];
    char detail[256];

    escape_or_empty(route, sizeof route, event->route);
    escape_or_empty(reason, sizeof reason, event->route_reason);
    escape_or_empty(sem, sizeof sem, event->semantic_class);
    escape_or_empty(safety, sizeof safety, event->safety_level);
    escape_or_empty(domain, sizeof domain, event->state_domain);
    escape_or_empty(old_state, sizeof old_state, event->old_state);
    escape_or_empty(new_state, sizeof new_state, event->new_state);
    escape_or_empty(outcome, sizeof outcome, event->outcome);
    escape_or_empty(detail, sizeof detail, event->detail);

    char line[2048];
    int n = snprintf(line, sizeof line,
        "{\"seq\":%llu,"
        "\"ts_ns\":%llu,"
        "\"type\":\"%s\","
        "\"session_id\":%llu,"
        "\"transaction_id\":%llu,"
        "\"connection_id\":%llu,"
        "\"worker_id\":%llu,"
        "\"query_hash\":\"0x%016llx\","
        "\"payload_hash\":\"0x%016llx\","
        "\"payload_len\":%llu,"
        "\"route\":\"%s\","
        "\"route_reason\":\"%s\","
        "\"semantic_class\":\"%s\","
        "\"safety_level\":\"%s\","
        "\"state_domain\":\"%s\","
        "\"old_state\":\"%s\","
        "\"new_state\":\"%s\","
        "\"outcome\":\"%s\","
        "\"detail\":\"%s\"}\n",
        (unsigned long long)++log->seq,
        (unsigned long long)replay_now_ns(),
        keel_replay_event_type_name(event->type),
        (unsigned long long)event->session_id,
        (unsigned long long)event->transaction_id,
        (unsigned long long)event->connection_id,
        (unsigned long long)event->worker_id,
        (unsigned long long)event->query_hash,
        (unsigned long long)event->payload_hash,
        (unsigned long long)event->payload_len,
        route,
        reason,
        sem,
        safety,
        domain,
        old_state,
        new_state,
        outcome,
        detail);

    if (n <= 0 || (size_t)n >= sizeof line) {
        return KEEL_ERR_BUFFER_TOO_SMALL;
    }
    if (log->max_bytes > 0 &&
        log->bytes_written + (uint64_t)n > log->max_bytes) {
        return KEEL_ERR_OVERFLOW;
    }

    if (fwrite(line, 1, (size_t)n, log->fp) != (size_t)n) {
        return KEEL_ERR_IO_WRITE;
    }
    log->bytes_written += (uint64_t)n;

    if (log->fsync_each) {
        return keel_replay_log_flush(log);
    }
    return KEEL_OK;
}

keel_error_t keel_replay_log_flush(keel_replay_log_t* log)
{
    if (!log || !log->fp) {
        return KEEL_ERR_INVALID_ARG;
    }
    if (fflush(log->fp) != 0) {
        return KEEL_ERR_IO_WRITE;
    }
    if (fsync(fileno(log->fp)) != 0) {
        return KEEL_ERR_IO_WRITE;
    }
    return KEEL_OK;
}

void keel_replay_log_close(keel_replay_log_t* log)
{
    if (!log) return;
    if (log->fp) {
        (void)keel_replay_log_flush(log);
        fclose(log->fp);
    }
    keel_free(log);
}
