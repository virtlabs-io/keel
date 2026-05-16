/**
 * @file test_cluster_fuzz.c
 * @brief Adversarial / fuzz tests for the cluster wire protocol.
 *
 * Tests the cluster subsystem's resilience to:
 *  - Truncated messages (every possible truncation point)
 *  - Oversized payloads (larger than struct)
 *  - Byte-flipped fields (single-bit and multi-bit mutations)
 *  - All-zero and all-ones payloads
 *  - Unknown / out-of-range message type codes
 *  - Correct messages with term = UINT64_MAX (term overflow)
 *  - txn_id = UINT64_MAX (2PC counter overflow)
 *  - Mismatched header.length vs actual payload
 *  - Zero-length payload with every message type
 *  - Concurrent connect+send from multiple goroutines
 *
 * This file is both a standard ctest binary (deterministic corpus) and an
 * AFL++ / libFuzzer compatible entry point (LLVMFuzzerTestOneInput).
 *
 * The safety contract:
 *   - The receiving cluster node must not crash, hang, or trigger sanitiser
 *     findings when processing any of these inputs.
 *   - The sender's return values may indicate failure; that is acceptable.
 *   - The receiver's health counters may change; that is acceptable.
 */

#include "keel/core/cluster.h"
#include "keel/mem/mem.h"

#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ============================================================================
 * Test harness
 * ============================================================================ */

static int g_passed = 0;
static int g_failed = 0;
static int g_total  = 0;

#define TEST_BEGIN(name) \
    do { g_total++; printf("  [%d] %-65s ", g_total, name); fflush(stdout); } while(0)
#define TEST_PASS()  do { g_passed++; printf("PASS\n"); } while(0)
#define TEST_FAIL(msg) do { g_failed++; printf("FAIL: %s\n", msg); } while(0)

#define ASSERT_TRUE(expr) \
    do { if (!(expr)) { TEST_FAIL(#expr " is false"); return; } } while(0)
#define ASSERT_EQ(a,b) \
    do { if ((a)!=(b)) { TEST_FAIL(#a " != " #b); return; } } while(0)
#define ASSERT_NOT_NULL(p) \
    do { if (!(p)) { TEST_FAIL(#p " is NULL"); return; } } while(0)

/* ============================================================================
 * Wire helper — send raw bytes to a cluster node's listen port
 * ============================================================================ */

/**
 * Open a TCP connection to 127.0.0.1:port, send 'len' bytes, then close.
 * Returns 0 on success (connected + data sent), -1 on connect failure.
 * This is intentionally fire-and-forget: the receiver may reject the data.
 */
static int send_raw(uint16_t port, const uint8_t *data, size_t len) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }

    if (len > 0) {
        ssize_t written = 0;
        while (written < (ssize_t)len) {
            ssize_t n = write(fd, data + written, len - (size_t)written);
            if (n <= 0) break;
            written += n;
        }
    }

    close(fd);
    return 0;
}

/**
 * Build a minimal well-formed cluster message header (72 bytes on the wire).
 *
 * keel_cluster_msg_header_t layout (72 bytes, __attribute__((packed))):
 *   uint32_t magic       [0-3]   = KEEL_CLUSTER_MAGIC ("KELC")
 *   uint8_t  version     [4]
 *   uint8_t  msg_type    [5]
 *   uint16_t payload_len [6-7]   (network byte order)
 *   char     node_id[64] [8-71]
 *
 * Caller must ensure buf is at least sizeof(keel_cluster_msg_header_t).
 */
static void build_header(uint8_t *buf, uint8_t type, uint16_t plen) {
    keel_cluster_msg_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic       = htonl(KEEL_CLUSTER_MAGIC);
    hdr.version     = (uint8_t)KEEL_CLUSTER_PROTO_VERSION;
    hdr.msg_type    = type;
    hdr.payload_len = htons(plen);
    memcpy(hdr.node_id, "fuzz-sender", 11); /* plausible node_id */
    memcpy(buf, &hdr, sizeof(hdr));
}

/* ============================================================================
 * AFL++ / libFuzzer entry point
 * ============================================================================ */

/* Port of the fuzz target node — chosen to avoid collision with other tests. */
#define FUZZ_PORT 20700

/* Convenience: actual byte size of the cluster wire header. */
#define HLEN ((size_t)sizeof(keel_cluster_msg_header_t))

static keel_cluster_t *g_fuzz_node = NULL;

/**
 * Core fuzz function: feed arbitrary bytes to the cluster listener.
 *
 * This is the AFL++ libFuzzer entry point AND the internal helper used by
 * the deterministic corpus tests.
 */
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (!g_fuzz_node) return 0;
    send_raw(FUZZ_PORT, data, size);
    usleep(1000); /* 1 ms: let receiver process */
    return 0;
}

/* ============================================================================
 * Deterministic corpus
 * ============================================================================ */

/* Start a cluster node to receive fuzz traffic.
 * Returns the node (already started) or NULL on failure. */
static keel_cluster_t *start_fuzz_receiver(uint16_t port) {
    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    cfg.election_enabled = false; /* keep it simple — just receive */
    snprintf(cfg.node_id, sizeof(cfg.node_id), "fuzz-recv");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = port;
    cfg.heartbeat_interval_ms = 500;
    cfg.heartbeat_timeout_ms  = 2000;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    if (!c) return NULL;
    if (keel_cluster_start(c) != 0) {
        keel_cluster_destroy(c);
        return NULL;
    }
    usleep(100000); /* let accept loop start */
    return c;
}

/* ---- §1: Empty / zero-byte input ---- */
static void test_fuzz_empty_input(void) {
    TEST_BEGIN("fuzz_empty_input: zero bytes");
    send_raw(FUZZ_PORT, NULL, 0);
    usleep(5000);
    /* No crash == pass */
    TEST_PASS();
}

/* ---- §2: Single byte (every possible value) ---- */
static void test_fuzz_single_byte_all_values(void) {
    TEST_BEGIN("fuzz_single_byte_all_values: 0x00-0xFF");
    for (int b = 0; b <= 0xFF; b++) {
        uint8_t byte = (uint8_t)b;
        send_raw(FUZZ_PORT, &byte, 1);
    }
    usleep(10000);
    TEST_PASS();
}

/* ---- §3: Truncated well-formed header ---- */
static void test_fuzz_truncated_header(void) {
    TEST_BEGIN("fuzz_truncated_header: every prefix of full header");

    uint8_t hdr[HLEN];
    build_header(hdr, KEEL_CLUSTER_MSG_HEARTBEAT, 0);

    for (size_t trunc = 1; trunc < HLEN; trunc++) {
        send_raw(FUZZ_PORT, hdr, trunc);
    }
    usleep(10000);
    TEST_PASS();
}

/* ---- §4: Complete header, then truncated payload ---- */
static void test_fuzz_truncated_payload(void) {
    TEST_BEGIN("fuzz_truncated_payload: header+partial payload");

    uint8_t buf[HLEN + sizeof(keel_cluster_config_prepare_t)];
    size_t plen = sizeof(keel_cluster_config_prepare_t);
    build_header(buf, KEEL_CLUSTER_MSG_CONFIG_PREPARE, (uint16_t)plen);
    /* fill payload with a recognisable pattern */
    memset(buf + HLEN, 0xAB, plen);

    for (size_t trunc = 8; trunc < 8 + plen; trunc++) {
        send_raw(FUZZ_PORT, buf, trunc);
    }
    usleep(10000);
    TEST_PASS();
}

/* ---- §5: All known message types with zero-length payload ---- */
static void test_fuzz_zero_payload_all_types(void) {
    TEST_BEGIN("fuzz_zero_payload_all_types: every type, no payload");

    uint8_t hdr[HLEN];
    for (int t = 0; t <= 15; t++) {
        build_header(hdr, (uint8_t)t, 0);
        send_raw(FUZZ_PORT, hdr, HLEN);
    }
    usleep(10000);
    TEST_PASS();
}

/* ---- §6: Unknown / out-of-range message type codes ---- */
static void test_fuzz_unknown_type(void) {
    TEST_BEGIN("fuzz_unknown_type: types 13-255 are unknown");

    uint8_t hdr[HLEN];
    for (int t = 13; t <= 255; t++) {
        build_header(hdr, (uint8_t)t, 0);
        send_raw(FUZZ_PORT, hdr, HLEN);
    }
    usleep(20000);
    TEST_PASS();
}

/* ---- §7: All-zero payload (every type) ---- */
static void test_fuzz_all_zeros(void) {
    TEST_BEGIN("fuzz_all_zeros: 512-byte zero buffer");

    uint8_t buf[512];
    memset(buf, 0, sizeof(buf));
    send_raw(FUZZ_PORT, buf, sizeof(buf));
    usleep(5000);
    TEST_PASS();
}

/* ---- §8: All-ones payload ---- */
static void test_fuzz_all_ones(void) {
    TEST_BEGIN("fuzz_all_ones: 512-byte 0xFF buffer");

    uint8_t buf[512];
    memset(buf, 0xFF, sizeof(buf));
    send_raw(FUZZ_PORT, buf, sizeof(buf));
    usleep(5000);
    TEST_PASS();
}

/* ---- §9: Header length field > actual data sent ---- */
static void test_fuzz_header_length_overread(void) {
    TEST_BEGIN("fuzz_header_length_overread: length > bytes sent");

    uint8_t buf[HLEN];
    build_header(buf, KEEL_CLUSTER_MSG_HEARTBEAT, 0xFFFF);
    /* Send only 10 bytes of the header despite claiming 0xFFFF payload */
    send_raw(FUZZ_PORT, buf, 10);
    usleep(5000);
    TEST_PASS();
}

/* ---- §10: term = UINT64_MAX in VOTE_REQUEST ---- */
static void test_fuzz_term_overflow(void) {
    TEST_BEGIN("fuzz_term_overflow: VOTE_REQUEST with term=UINT64_MAX");

    uint8_t buf[HLEN + sizeof(keel_cluster_vote_request_t)];
    size_t plen = sizeof(keel_cluster_vote_request_t);
    build_header(buf, KEEL_CLUSTER_MSG_VOTE_REQUEST, (uint16_t)plen);

    keel_cluster_vote_request_t *req = (keel_cluster_vote_request_t*)(buf + HLEN);
    req->term = UINT64_MAX;
    memset(req->candidate_id, 'X', sizeof(req->candidate_id) - 1);
    req->candidate_id[sizeof(req->candidate_id) - 1] = '\0';

    send_raw(FUZZ_PORT, buf, sizeof(buf));
    usleep(5000);
    TEST_PASS();
}

/* ---- §11: txn_id = UINT64_MAX in CONFIG_PREPARE ---- */
static void test_fuzz_txn_overflow(void) {
    TEST_BEGIN("fuzz_txn_overflow: CONFIG_PREPARE with txn_id=UINT64_MAX");

    uint8_t buf[HLEN + sizeof(keel_cluster_config_prepare_t)];
    size_t plen = sizeof(keel_cluster_config_prepare_t);
    build_header(buf, KEEL_CLUSTER_MSG_CONFIG_PREPARE, (uint16_t)plen);

    keel_cluster_config_prepare_t *prep = (keel_cluster_config_prepare_t*)(buf + HLEN);
    prep->term         = UINT64_MAX;
    prep->txn_id       = UINT64_MAX;
    prep->new_checksum = UINT64_MAX;

    send_raw(FUZZ_PORT, buf, sizeof(buf));
    usleep(5000);
    TEST_PASS();
}

/* ---- §12: Repeated rapid connections (connection storm) ---- */
static void test_fuzz_connection_storm(void) {
    TEST_BEGIN("fuzz_connection_storm: 200 rapid connect+close");

    uint8_t hdr[HLEN];
    build_header(hdr, KEEL_CLUSTER_MSG_HEARTBEAT, 0);

    for (int i = 0; i < 200; i++) {
        /* Some send data, some just connect and close. */
        if (i % 3 == 0) {
            send_raw(FUZZ_PORT, NULL, 0); /* connect-only */
        } else {
            send_raw(FUZZ_PORT, hdr, sizeof(hdr));
        }
    }
    usleep(50000);
    TEST_PASS();
}

/* ---- §13: Bit-flip mutations of a valid HEARTBEAT ---- */
static void test_fuzz_bitflip_heartbeat(void) {
    TEST_BEGIN("fuzz_bitflip_heartbeat: flip every bit in 8-byte header");

    uint8_t base[HLEN];
    build_header(base, KEEL_CLUSTER_MSG_HEARTBEAT, 0);

    for (size_t byte_idx = 0; byte_idx < HLEN; byte_idx++) {
        for (int bit = 0; bit < 8; bit++) {
            uint8_t mutated[HLEN];
            memcpy(mutated, base, HLEN);
            mutated[byte_idx] ^= (uint8_t)(1u << bit);
            send_raw(FUZZ_PORT, mutated, HLEN);
        }
    }
    usleep(20000);
    TEST_PASS();
}

/* ---- §14: Bit-flip mutations of a valid CONFIG_PREPARE ---- */
static void test_fuzz_bitflip_config_prepare(void) {
    TEST_BEGIN("fuzz_bitflip_config_prepare: flip every bit in msg");

    uint8_t base[HLEN + sizeof(keel_cluster_config_prepare_t)];
    size_t plen = sizeof(keel_cluster_config_prepare_t);
    build_header(base, KEEL_CLUSTER_MSG_CONFIG_PREPARE, (uint16_t)plen);
    memset(base + HLEN, 0, plen);

    for (size_t byte = 0; byte < sizeof(base); byte++) {
        for (int bit = 0; bit < 8; bit++) {
            uint8_t mutated[sizeof(base)];
            memcpy(mutated, base, sizeof(base));
            mutated[byte] ^= (uint8_t)(1u << bit);
            send_raw(FUZZ_PORT, mutated, sizeof(base));
        }
    }
    usleep(50000);
    TEST_PASS();
}

/* ---- §15: CONFIG_COMMIT with commit=0 (abort) and commit=0xFF ---- */
static void test_fuzz_config_commit_values(void) {
    TEST_BEGIN("fuzz_config_commit_values: commit byte boundary values");

    uint8_t buf[HLEN + sizeof(keel_cluster_config_commit_t)];
    size_t plen = sizeof(keel_cluster_config_commit_t);

    uint8_t commit_values[] = { 0x00, 0x01, 0x02, 0x7F, 0x80, 0xFF };
    for (size_t i = 0; i < sizeof(commit_values); i++) {
        build_header(buf, KEEL_CLUSTER_MSG_CONFIG_COMMIT, (uint16_t)plen);
        keel_cluster_config_commit_t *comm = (keel_cluster_config_commit_t*)(buf + HLEN);
        comm->term   = 1;
        comm->txn_id = 1;
        comm->commit = commit_values[i];
        memset(comm->_pad, 0, sizeof(comm->_pad));
        send_raw(FUZZ_PORT, buf, sizeof(buf));
    }
    usleep(10000);
    TEST_PASS();
}

/* ---- §16: Very large payload (heap overflow guard) ---- */
static void test_fuzz_oversized_payload(void) {
    TEST_BEGIN("fuzz_oversized_payload: claim 60000 byte payload");

    uint8_t buf[HLEN + 256];
    build_header(buf, KEEL_CLUSTER_MSG_HEARTBEAT, 60000); /* lie about size */
    memset(buf + HLEN, 0xBB, 256);
    send_raw(FUZZ_PORT, buf, HLEN + 256); /* send only 264 bytes */
    usleep(5000);
    TEST_PASS();
}

/* ---- §17: Concurrent senders (thread storm) ---- */

struct sender_args {
    uint16_t port;
    int      iterations;
};

static void *sender_thread(void *arg) {
    struct sender_args *a = (struct sender_args*)arg;
    uint8_t buf[HLEN];
    build_header(buf, KEEL_CLUSTER_MSG_HEARTBEAT, 0);
    for (int i = 0; i < a->iterations; i++) {
        send_raw(a->port, buf, HLEN);
    }
    return NULL;
}

static void test_fuzz_concurrent_senders(void) {
    TEST_BEGIN("fuzz_concurrent_senders: 8 threads × 50 messages");

    enum { N_THREADS = 8, ITERS = 50 };
    pthread_t tids[N_THREADS];
    struct sender_args args = { FUZZ_PORT, ITERS };

    for (int i = 0; i < N_THREADS; i++)
        pthread_create(&tids[i], NULL, sender_thread, &args);
    for (int i = 0; i < N_THREADS; i++)
        pthread_join(tids[i], NULL);

    usleep(20000);
    TEST_PASS();
}

/* ---- §18: Vote response with vote_granted = 0xFF ---- */
static void test_fuzz_vote_response_invalid(void) {
    TEST_BEGIN("fuzz_vote_response_invalid: vote_granted=0xFF, bad term");

    uint8_t buf[HLEN + sizeof(keel_cluster_vote_response_t)];
    size_t plen = sizeof(keel_cluster_vote_response_t);
    build_header(buf, KEEL_CLUSTER_MSG_VOTE_RESPONSE, (uint16_t)plen);

    keel_cluster_vote_response_t *resp = (keel_cluster_vote_response_t*)(buf + HLEN);
    resp->term        = 0;
    resp->vote_granted = 0xFF;
    memset(resp->_pad, 0xFF, sizeof(resp->_pad));

    send_raw(FUZZ_PORT, buf, sizeof(buf));
    usleep(5000);
    TEST_PASS();
}

/* ---- §19: NOTIFY_SERVER with max-length host field ---- */
static void test_fuzz_notify_server_long_host(void) {
    TEST_BEGIN("fuzz_notify_server_long_host: 128-char host, no NUL");

    uint8_t buf[HLEN + sizeof(keel_cluster_server_notify_t)];
    size_t plen = sizeof(keel_cluster_server_notify_t);
    build_header(buf, KEEL_CLUSTER_MSG_NOTIFY_SERVER, (uint16_t)plen);

    keel_cluster_server_notify_t *n = (keel_cluster_server_notify_t*)(buf + HLEN);
    n->action = 2;
    n->role   = 0;
    n->port   = htons(5432);
    n->weight = htonl(100);
    memset(n->host, 'A', sizeof(n->host)); /* no NUL terminator */

    send_raw(FUZZ_PORT, buf, sizeof(buf));
    usleep(5000);
    TEST_PASS();
}

/* ---- §20: Random-corpus sweep — 1000 pseudo-random inputs ---- */
static void test_fuzz_pseudo_random_corpus(void) {
    TEST_BEGIN("fuzz_pseudo_random_corpus: 1000 pseudo-random inputs");

    /* Deterministic LCG so the test is reproducible. */
    uint32_t state = 0xDEADBEEF;
    uint8_t  buf[64];

    for (int i = 0; i < 1000; i++) {
        /* LCG step */
        state = state * 1664525u + 1013904223u;

        size_t len = (state >> 16) % 64;
        for (size_t j = 0; j < len; j++) {
            state = state * 1664525u + 1013904223u;
            buf[j] = (uint8_t)(state >> 24);
        }
        send_raw(FUZZ_PORT, buf, len);
    }
    usleep(100000);
    TEST_PASS();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    printf("\n=== KEEL Cluster Wire-Protocol Fuzz Tests ===\n\n");

    keel_mem_init(NULL);

    /* Spin up a single receiver node that all fuzz corpus tests share. */
    g_fuzz_node = start_fuzz_receiver(FUZZ_PORT);
    if (!g_fuzz_node) {
        fprintf(stderr, "FATAL: could not start fuzz receiver on port %d\n",
                FUZZ_PORT);
        return 1;
    }

    test_fuzz_empty_input();
    test_fuzz_single_byte_all_values();
    test_fuzz_truncated_header();
    test_fuzz_truncated_payload();
    test_fuzz_zero_payload_all_types();
    test_fuzz_unknown_type();
    test_fuzz_all_zeros();
    test_fuzz_all_ones();
    test_fuzz_header_length_overread();
    test_fuzz_term_overflow();
    test_fuzz_txn_overflow();
    test_fuzz_connection_storm();
    test_fuzz_bitflip_heartbeat();
    test_fuzz_bitflip_config_prepare();
    test_fuzz_config_commit_values();
    test_fuzz_oversized_payload();
    test_fuzz_concurrent_senders();
    test_fuzz_vote_response_invalid();
    test_fuzz_notify_server_long_host();
    test_fuzz_pseudo_random_corpus();

    keel_cluster_stop(g_fuzz_node);
    keel_cluster_destroy(g_fuzz_node);

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           g_passed, g_failed, g_total);

    return g_failed > 0 ? 1 : 0;
}
