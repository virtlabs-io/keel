/**
 * @file test_replay_log.c
 * @brief Durable operational replay log tests.
 */

#include "test_utils.h"
#include "keel/util/replay_log.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

static void read_file(const char* path, char* out, size_t out_size)
{
    FILE* f = fopen(path, "r");
    TEST_ASSERT_NOT_NULL(f);
    size_t n = fread(out, 1, out_size - 1, f);
    out[n] = '\0';
    fclose(f);
}

static void test_replay_log_writes_redacted_ndjson(void)
{
    TEST_BEGIN("replay log writes redacted NDJSON");

    char path[256];
    snprintf(path, sizeof path, "/tmp/keel-replay-log-%ld.ndjson", (long)getpid());
    unlink(path);

    keel_replay_log_t* log = NULL;
    keel_replay_log_config_t cfg = {
        .path = path,
        .fsync_each = true,
        .max_bytes = 0,
    };
    TEST_ASSERT_EQ(keel_replay_log_open(&cfg, &log), KEEL_OK);
    TEST_ASSERT_NOT_NULL(log);

    const char secret_payload[] = "password=super-secret SELECT 1";
    keel_replay_event_t ev = {
        .type = KEEL_REPLAY_EVENT_FRONTEND_MESSAGE,
        .session_id = 42,
        .transaction_id = 7,
        .connection_id = 3,
        .worker_id = 2,
        .query_hash = 0x81ab,
        .payload_hash = keel_replay_payload_hash(secret_payload, strlen(secret_payload)),
        .payload_len = strlen(secret_payload),
        .semantic_class = "READ_ONLY",
        .safety_level = "SAFE_REPLICA",
        .detail = "simple query payload redacted",
    };
    TEST_ASSERT_EQ(keel_replay_log_append(log, &ev), KEEL_OK);

    ev.type = KEEL_REPLAY_EVENT_ROUTE_DECISION;
    ev.route = "replica_1";
    ev.route_reason = "READ_SPLIT";
    ev.detail = "route chosen";
    TEST_ASSERT_EQ(keel_replay_log_append(log, &ev), KEEL_OK);

    keel_replay_log_close(log);

    char body[4096];
    read_file(path, body, sizeof body);
    TEST_ASSERT(strstr(body, "\"type\":\"frontend_message\"") != NULL);
    TEST_ASSERT(strstr(body, "\"type\":\"route_decision\"") != NULL);
    TEST_ASSERT(strstr(body, "\"payload_hash\":\"0x") != NULL);
    TEST_ASSERT(strstr(body, "\"payload_len\":30") != NULL);
    TEST_ASSERT(strstr(body, "super-secret") == NULL);
    TEST_ASSERT(strstr(body, "password=") == NULL);
    TEST_ASSERT(strstr(body, "\"route\":\"replica_1\"") != NULL);
    TEST_ASSERT(strstr(body, "\"route_reason\":\"READ_SPLIT\"") != NULL);

    unlink(path);
    TEST_END();
}

static void test_replay_log_captures_state_and_cid(void)
{
    TEST_BEGIN("replay log captures state and CID transitions");

    char path[256];
    snprintf(path, sizeof path, "/tmp/keel-replay-cid-%ld.ndjson", (long)getpid());
    unlink(path);

    keel_replay_log_t* log = NULL;
    keel_replay_log_config_t cfg = {
        .path = path,
        .fsync_each = false,
        .max_bytes = 4096,
    };
    TEST_ASSERT_EQ(keel_replay_log_open(&cfg, &log), KEEL_OK);

    keel_replay_event_t ev = {
        .type = KEEL_REPLAY_EVENT_STATE_TRANSITION,
        .session_id = 100,
        .state_domain = "transaction",
        .old_state = "in_tx",
        .new_state = "committing",
        .outcome = "commit_sent",
    };
    TEST_ASSERT_EQ(keel_replay_log_append(log, &ev), KEEL_OK);

    ev.type = KEEL_REPLAY_EVENT_CID_TRANSITION;
    ev.state_domain = "cid";
    ev.old_state = "commit_sent";
    ev.new_state = "resolved_unknown";
    ev.outcome = "client_must_retry";
    ev.detail = "commit outcome unknown after backend loss";
    TEST_ASSERT_EQ(keel_replay_log_append(log, &ev), KEEL_OK);
    TEST_ASSERT_EQ(keel_replay_log_flush(log), KEEL_OK);
    keel_replay_log_close(log);

    char body[4096];
    read_file(path, body, sizeof body);
    TEST_ASSERT(strstr(body, "\"type\":\"state_transition\"") != NULL);
    TEST_ASSERT(strstr(body, "\"type\":\"cid_transition\"") != NULL);
    TEST_ASSERT(strstr(body, "\"new_state\":\"resolved_unknown\"") != NULL);
    TEST_ASSERT(strstr(body, "\"outcome\":\"client_must_retry\"") != NULL);

    unlink(path);
    TEST_END();
}

static void test_replay_log_size_gate(void)
{
    TEST_BEGIN("replay log enforces max_bytes");

    char path[256];
    snprintf(path, sizeof path, "/tmp/keel-replay-small-%ld.ndjson", (long)getpid());
    unlink(path);

    keel_replay_log_t* log = NULL;
    keel_replay_log_config_t cfg = {
        .path = path,
        .fsync_each = false,
        .max_bytes = 64,
    };
    TEST_ASSERT_EQ(keel_replay_log_open(&cfg, &log), KEEL_OK);

    keel_replay_event_t ev = {
        .type = KEEL_REPLAY_EVENT_CHECKPOINT,
        .detail = "this record is intentionally larger than the configured cap",
    };
    TEST_ASSERT_EQ(keel_replay_log_append(log, &ev), KEEL_ERR_OVERFLOW);

    keel_replay_log_close(log);
    unlink(path);
    TEST_END();
}

int main(void)
{
    test_replay_log_writes_redacted_ndjson();
    test_replay_log_captures_state_and_cid();
    test_replay_log_size_gate();
    return test_summary();
}
