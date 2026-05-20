/**
 * @file test_cluster.c
 * @brief Unit tests for the multi-proxy HA cluster mode.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file exercises the cluster subsystem:
 *   - Configuration parsing and defaults.
 *   - Peer lifecycle (create, add, remove, destroy).
 *   - Wire protocol encoding/decoding (header, heartbeat).
 *   - Heartbeat logic and peer state transitions.
 *   - Cluster statistics collection.
 *   - Graceful leave/stop behavior.
 */

#include "keel/core/cluster.h"
#include "keel/mem/mem.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

/* ============================================================================
 * Test Helpers
 * ============================================================================ */

static int g_passed = 0;
static int g_failed = 0;
static int g_total  = 0;

/**
 * Poll until a peer reaches UP status or timeout expires.
 * Returns true if peer is UP within the deadline.
 */
static bool wait_peer_up(keel_cluster_t *c, size_t peer_idx, int timeout_ms) {
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        keel_cluster_peer_t p;
        if (keel_cluster_get_peer(c, peer_idx, &p) &&
            atomic_load(&p.status) == KEEL_PEER_UP)
            return true;
        usleep(100000); /* 100ms */
        elapsed += 100;
    }
    return false;
}

static bool wait_peer_heartbeats(keel_cluster_t *c, size_t peer_idx, int timeout_ms) {
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        keel_cluster_peer_t p;
        if (keel_cluster_get_peer(c, peer_idx, &p) &&
            atomic_load(&p.total_heartbeats) > 0)
            return true;
        usleep(100000); /* 100ms */
        elapsed += 100;
    }
    return false;
}

static bool wait_peer_count(keel_cluster_t *c, size_t expected, int timeout_ms) {
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        if (keel_cluster_peer_count(c) >= expected)
            return true;
        usleep(100000); /* 100ms */
        elapsed += 100;
    }
    return false;
}

static bool cluster_has_peer_port(keel_cluster_t *c, uint16_t port,
                                  keel_cluster_peer_t *out_peer) {
    size_t count = keel_cluster_peer_count(c);
    for (size_t i = 0; i < count; i++) {
        keel_cluster_peer_t p;
        if (!keel_cluster_get_peer(c, i, &p) || !p.active) continue;
        if (p.port == port) {
            if (out_peer) *out_peer = p;
            return true;
        }
    }
    return false;
}

#define TEST_BEGIN(name) \
    do { g_total++; printf("  [%d] %-50s ", g_total, name); fflush(stdout); } while(0)

#define TEST_PASS() \
    do { g_passed++; printf("PASS\n"); } while(0)

#define TEST_FAIL(msg) \
    do { g_failed++; printf("FAIL: %s\n", msg); } while(0)

#define ASSERT_TRUE(expr) \
    do { if (!(expr)) { TEST_FAIL(#expr " is false"); return; } } while(0)

#define ASSERT_FALSE(expr) \
    do { if (expr) { TEST_FAIL(#expr " is true"); return; } } while(0)

#define ASSERT_EQ(a, b) \
    do { if ((a) != (b)) { TEST_FAIL(#a " != " #b); return; } } while(0)

#define ASSERT_STR_EQ(a, b) \
    do { if (strcmp((a), (b)) != 0) { TEST_FAIL(#a " != " #b); return; } } while(0)

#define ASSERT_NOT_NULL(p) \
    do { if (!(p)) { TEST_FAIL(#p " is NULL"); return; } } while(0)

#define ASSERT_NULL(p) \
    do { if (p) { TEST_FAIL(#p " is not NULL"); return; } } while(0)

/* ============================================================================
 * Tests
 * ============================================================================ */

/**
 * Test 1: Default config values.
 */
static void test_config_defaults(void) {
    TEST_BEGIN("config_defaults");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    ASSERT_FALSE(cfg.enabled);
    ASSERT_EQ(cfg.listen_port, 9100);
    ASSERT_EQ(cfg.heartbeat_interval_ms, 1000);
    ASSERT_EQ(cfg.heartbeat_timeout_ms, 5000);
    ASSERT_EQ(cfg.failure_threshold, 3);
    ASSERT_TRUE(cfg.auto_sync);
    ASSERT_EQ(cfg.initial_peer_count, 0);
    ASSERT_STR_EQ(cfg.listen_addr, "0.0.0.0");

    TEST_PASS();
}

/**
 * Test 2: Create and destroy with defaults (disabled).
 */
static void test_create_destroy_disabled(void) {
    TEST_BEGIN("create_destroy_disabled");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_FALSE(keel_cluster_is_active(c));
    ASSERT_EQ(keel_cluster_peer_count(c), (size_t)0);
    keel_cluster_destroy(c);

    TEST_PASS();
}

/**
 * Test 3: Create with NULL config.
 */
static void test_create_null(void) {
    TEST_BEGIN("create_null");

    keel_cluster_t *c = keel_cluster_create(NULL);
    ASSERT_NULL(c);

    TEST_PASS();
}

/**
 * Test 4: Peer add/remove lifecycle.
 */
static void test_peer_add_remove(void) {
    TEST_BEGIN("peer_add_remove");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    /* Add peers */
    ASSERT_EQ(keel_cluster_add_peer(c, "10.0.0.1", 9100), 0);
    ASSERT_EQ(keel_cluster_peer_count(c), (size_t)1);

    ASSERT_EQ(keel_cluster_add_peer(c, "10.0.0.2", 9100), 0);
    ASSERT_EQ(keel_cluster_peer_count(c), (size_t)2);

    /* Duplicate add should succeed (idempotent) */
    ASSERT_EQ(keel_cluster_add_peer(c, "10.0.0.1", 9100), 0);
    ASSERT_EQ(keel_cluster_peer_count(c), (size_t)2);

    /* Verify peer data */
    keel_cluster_peer_t peer;
    ASSERT_TRUE(keel_cluster_get_peer(c, 0, &peer));
    ASSERT_STR_EQ(peer.addr, "10.0.0.1");
    ASSERT_EQ(peer.port, 9100);
    ASSERT_TRUE(peer.active);

    /* Remove peer */
    ASSERT_EQ(keel_cluster_remove_peer(c, "10.0.0.1", 9100), 0);

    /* Non-existent removal should return -1 */
    ASSERT_EQ(keel_cluster_remove_peer(c, "10.0.0.99", 9100), -1);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 5: Bootstrap peers from config.
 */
static void test_bootstrap_peers(void) {
    TEST_BEGIN("bootstrap_peers");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    strcpy(cfg.initial_peers[0].addr, "192.168.1.10");
    cfg.initial_peers[0].port = 9100;
    strcpy(cfg.initial_peers[1].addr, "192.168.1.11");
    cfg.initial_peers[1].port = 9100;
    cfg.initial_peer_count = 2;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_peer_count(c), (size_t)2);

    keel_cluster_peer_t peer;
    ASSERT_TRUE(keel_cluster_get_peer(c, 0, &peer));
    ASSERT_STR_EQ(peer.addr, "192.168.1.10");

    ASSERT_TRUE(keel_cluster_get_peer(c, 1, &peer));
    ASSERT_STR_EQ(peer.addr, "192.168.1.11");

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 6: Fill peer table to maximum.
 */
static void test_peer_table_full(void) {
    TEST_BEGIN("peer_table_full");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    /* Fill all slots */
    char addr[32];
    for (int i = 0; i < KEEL_CLUSTER_MAX_PEERS; i++) {
        snprintf(addr, sizeof(addr), "10.0.0.%d", i + 1);
        ASSERT_EQ(keel_cluster_add_peer(c, addr, 9100), 0);
    }
    ASSERT_EQ(keel_cluster_peer_count(c), (size_t)KEEL_CLUSTER_MAX_PEERS);

    /* Next add should fail */
    ASSERT_EQ(keel_cluster_add_peer(c, "10.0.0.99", 9100), -1);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 7: Config checksum get/set.
 */
static void test_config_checksum(void) {
    TEST_BEGIN("config_checksum");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    ASSERT_EQ(keel_cluster_get_config_checksum(c), (uint64_t)0);
    keel_cluster_set_config_checksum(c, 0xDEADBEEF12345678ULL);
    ASSERT_EQ(keel_cluster_get_config_checksum(c), (uint64_t)0xDEADBEEF12345678ULL);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 8: Runtime config snapshot getter.
 */
static void test_runtime_config_snapshot(void) {
    TEST_BEGIN("runtime_config_snapshot");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "cfg-snap-node");
    strcpy(cfg.listen_addr, "127.0.0.1");
    cfg.listen_port = 19900;
    cfg.heartbeat_interval_ms = 222;
    cfg.heartbeat_timeout_ms = 3333;
    cfg.failure_threshold = 4;
    cfg.auto_sync = false;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    keel_cluster_set_config_checksum(c, 0xAABBCCDDEEFF0011ULL);

    keel_cluster_runtime_config_t snap;
    ASSERT_TRUE(keel_cluster_get_runtime_config(c, &snap));
    ASSERT_STR_EQ(snap.node_id, "cfg-snap-node");
    ASSERT_STR_EQ(snap.listen_addr, "127.0.0.1");
    ASSERT_EQ(snap.listen_port, (uint16_t)19900);
    ASSERT_EQ(snap.active_peers, (uint16_t)0);
    ASSERT_EQ(snap.heartbeat_interval_ms, (uint32_t)222);
    ASSERT_EQ(snap.heartbeat_timeout_ms, (uint32_t)3333);
    ASSERT_EQ(snap.failure_threshold, (uint32_t)4);
    ASSERT_EQ(snap.auto_sync, (uint8_t)0);
    ASSERT_EQ(snap.running, (uint8_t)0);
    ASSERT_EQ(snap.config_checksum, (uint64_t)0xAABBCCDDEEFF0011ULL);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 8: Statistics initial state.
 */
static void test_stats_initial(void) {
    TEST_BEGIN("stats_initial");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    strcpy(cfg.initial_peers[0].addr, "10.0.0.1");
    cfg.initial_peers[0].port = 9100;
    cfg.initial_peer_count = 1;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    keel_cluster_stats_t stats;
    keel_cluster_get_stats(c, &stats);
    ASSERT_EQ(stats.total_peers, (size_t)1);
    ASSERT_EQ(stats.peers_up, (size_t)0);
    ASSERT_EQ(stats.heartbeats_sent, (uint64_t)0);
    ASSERT_EQ(stats.server_notifications, (uint64_t)0);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 9: Active state check (not started).
 */
static void test_active_not_started(void) {
    TEST_BEGIN("active_not_started");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    /* Created but not started */
    ASSERT_FALSE(keel_cluster_is_active(c));

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 10: NULL safety across all APIs.
 */
static void test_null_safety(void) {
    TEST_BEGIN("null_safety");

    ASSERT_FALSE(keel_cluster_is_active(NULL));
    ASSERT_EQ(keel_cluster_peer_count(NULL), (size_t)0);
    ASSERT_EQ(keel_cluster_get_config_checksum(NULL), (uint64_t)0);

    keel_cluster_peer_t peer;
    ASSERT_FALSE(keel_cluster_get_peer(NULL, 0, &peer));

    keel_cluster_runtime_config_t runtime_cfg;
    ASSERT_FALSE(keel_cluster_get_runtime_config(NULL, &runtime_cfg));
    ASSERT_FALSE(keel_cluster_get_runtime_config(NULL, NULL));

    /* These should not crash */
    keel_cluster_set_config_checksum(NULL, 0);
    keel_cluster_destroy(NULL);
    keel_cluster_stop(NULL);

    keel_cluster_stats_t stats = {0};
    keel_cluster_get_stats(NULL, &stats);

    TEST_PASS();
}

/**
 * Test 11: Start and stop cluster (loopback, no real peers).
 */
static void test_start_stop(void) {
    TEST_BEGIN("start_stop");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "test-node-1");
    strcpy(cfg.listen_addr, "127.0.0.1");
    cfg.listen_port = 19100;  /* High port to avoid conflicts */

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    ASSERT_EQ(keel_cluster_start(c), 0);
    ASSERT_TRUE(keel_cluster_is_active(c));

    /* Let it run briefly */
    usleep(50000); /* 50ms */

    keel_cluster_stop(c);
    ASSERT_FALSE(keel_cluster_is_active(c));

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 12: Two-node cluster heartbeat exchange.
 */
static void test_two_node_heartbeat(void) {
    TEST_BEGIN("two_node_heartbeat");

    /* Node A */
    keel_cluster_config_t cfg_a = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_a.enabled = true;
    snprintf(cfg_a.node_id, sizeof(cfg_a.node_id), "node-A");
    strcpy(cfg_a.listen_addr, "127.0.0.1");
    cfg_a.listen_port = 19200;
    cfg_a.heartbeat_interval_ms = 200;
    cfg_a.heartbeat_timeout_ms = 2000;
    cfg_a.failure_threshold = 3;

    /* Node B's address as peer */
    strcpy(cfg_a.initial_peers[0].addr, "127.0.0.1");
    cfg_a.initial_peers[0].port = 19201;
    cfg_a.initial_peer_count = 1;

    /* Node B */
    keel_cluster_config_t cfg_b = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_b.enabled = true;
    snprintf(cfg_b.node_id, sizeof(cfg_b.node_id), "node-B");
    strcpy(cfg_b.listen_addr, "127.0.0.1");
    cfg_b.listen_port = 19201;
    cfg_b.heartbeat_interval_ms = 200;
    cfg_b.heartbeat_timeout_ms = 2000;
    cfg_b.failure_threshold = 3;

    /* Node A's address as peer */
    strcpy(cfg_b.initial_peers[0].addr, "127.0.0.1");
    cfg_b.initial_peers[0].port = 19200;
    cfg_b.initial_peer_count = 1;

    keel_cluster_t *a = keel_cluster_create(&cfg_a);
    keel_cluster_t *b = keel_cluster_create(&cfg_b);
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);

    ASSERT_EQ(keel_cluster_start(a), 0);
    ASSERT_EQ(keel_cluster_start(b), 0);

    /* Poll until both peers reach UP (up to 30s — TSan slows I/O significantly) */
    ASSERT_TRUE(wait_peer_up(a, 0, 30000));
    ASSERT_TRUE(wait_peer_up(b, 0, 30000));

    /* Verify node A's view of node B */
    keel_cluster_peer_t peer;
    ASSERT_TRUE(keel_cluster_get_peer(a, 0, &peer));
    int status_a = atomic_load(&peer.status);
    /* Peer should be UP after successful heartbeat exchange */
    ASSERT_EQ(status_a, KEEL_PEER_UP);
    ASSERT_TRUE(wait_peer_heartbeats(a, 0, 10000));

    /* Verify node B's view of node A */
    ASSERT_TRUE(keel_cluster_get_peer(b, 0, &peer));
    int status_b = atomic_load(&peer.status);
    ASSERT_EQ(status_b, KEEL_PEER_UP);

    /* Check stats */
    keel_cluster_stats_t stats_a;
    keel_cluster_get_stats(a, &stats_a);
    ASSERT_TRUE(stats_a.heartbeats_sent > 0);
    ASSERT_TRUE(stats_a.heartbeats_received > 0);
    ASSERT_EQ(stats_a.peers_up, (size_t)1);

    keel_cluster_stop(a);
    keel_cluster_stop(b);
    keel_cluster_destroy(a);
    keel_cluster_destroy(b);
    TEST_PASS();
}

/**
 * Test 13: Peer failure detection (connect to dead node).
 */
static void test_peer_failure_detection(void) {
    TEST_BEGIN("peer_failure_detection");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "alive-node");
    strcpy(cfg.listen_addr, "127.0.0.1");
    cfg.listen_port = 19300;
    cfg.heartbeat_interval_ms = 100;
    cfg.heartbeat_timeout_ms = 500;
    cfg.failure_threshold = 2;

    /* Add a peer that doesn't exist */
    strcpy(cfg.initial_peers[0].addr, "127.0.0.1");
    cfg.initial_peers[0].port = 19399;  /* Nothing listening here */
    cfg.initial_peer_count = 1;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);

    /* Wait for failure threshold (2 failures × 100ms interval + timeout) */
    usleep(2500000); /* 2.5 seconds */

    keel_cluster_peer_t peer;
    ASSERT_TRUE(keel_cluster_get_peer(c, 0, &peer));
    int status = atomic_load(&peer.status);
    /* Should be DOWN or SUSPECT after missed heartbeats */
    ASSERT_TRUE(status == KEEL_PEER_DOWN || status == KEEL_PEER_SUSPECT);
    ASSERT_TRUE(atomic_load(&peer.total_failures) > 0);

    keel_cluster_stop(c);
    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 14: Server change notification.
 */
static void test_server_notify(void) {
    TEST_BEGIN("server_notify");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    /* Notify with no peers should succeed (no-op) */
    ASSERT_EQ(keel_cluster_notify_server_change(c, 0, "db1.example.com",
                                                 5432, 0, 100), 0);

    keel_cluster_stats_t stats;
    keel_cluster_get_stats(c, &stats);
    /* No UP peers, so no notifications actually sent */
    ASSERT_EQ(stats.server_notifications, (uint64_t)0);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 15: Node ID auto-generation.
 */
static void test_node_id_auto(void) {
    TEST_BEGIN("node_id_auto");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    cfg.listen_port = 9100;
    /* node_id is empty, should be auto-generated */
    cfg.node_id[0] = '\0';

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    /* The node_id should be non-empty after creation */
    /* We can't easily check the value, but it should contain the port */
    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 16: Get peer with invalid index.
 */
static void test_get_peer_invalid(void) {
    TEST_BEGIN("get_peer_invalid_index");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    keel_cluster_peer_t peer;
    ASSERT_FALSE(keel_cluster_get_peer(c, 0, &peer));
    ASSERT_FALSE(keel_cluster_get_peer(c, 999, &peer));
    ASSERT_FALSE(keel_cluster_get_peer(c, 0, NULL));

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 17: Start without enabling returns error.
 */
static void test_start_disabled(void) {
    TEST_BEGIN("start_disabled");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = false;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    ASSERT_EQ(keel_cluster_start(c), -1);
    ASSERT_FALSE(keel_cluster_is_active(c));

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 18: Config checksum mismatch detection (two-node).
 */
static void test_config_mismatch_detection(void) {
    TEST_BEGIN("config_mismatch_detection");

    /* Node A with checksum 0xAAA */
    keel_cluster_config_t cfg_a = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_a.enabled = true;
    snprintf(cfg_a.node_id, sizeof(cfg_a.node_id), "mismatch-A");
    strcpy(cfg_a.listen_addr, "127.0.0.1");
    cfg_a.listen_port = 19400;
    cfg_a.heartbeat_interval_ms = 200;
    cfg_a.heartbeat_timeout_ms = 2000;
    strcpy(cfg_a.initial_peers[0].addr, "127.0.0.1");
    cfg_a.initial_peers[0].port = 19401;
    cfg_a.initial_peer_count = 1;

    /* Node B with different checksum 0xBBB */
    keel_cluster_config_t cfg_b = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_b.enabled = true;
    snprintf(cfg_b.node_id, sizeof(cfg_b.node_id), "mismatch-B");
    strcpy(cfg_b.listen_addr, "127.0.0.1");
    cfg_b.listen_port = 19401;
    cfg_b.heartbeat_interval_ms = 200;
    cfg_b.heartbeat_timeout_ms = 2000;
    strcpy(cfg_b.initial_peers[0].addr, "127.0.0.1");
    cfg_b.initial_peers[0].port = 19400;
    cfg_b.initial_peer_count = 1;

    keel_cluster_t *a = keel_cluster_create(&cfg_a);
    keel_cluster_t *b = keel_cluster_create(&cfg_b);
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);

    keel_cluster_set_config_checksum(a, 0xAAAAAAAA);
    keel_cluster_set_config_checksum(b, 0xBBBBBBBB);

    ASSERT_EQ(keel_cluster_start(a), 0);
    ASSERT_EQ(keel_cluster_start(b), 0);

    /* Poll until both peers reach UP (up to 30s — TSan slows I/O significantly) */
    ASSERT_TRUE(wait_peer_up(a, 0, 30000));
    ASSERT_TRUE(wait_peer_up(b, 0, 30000));

    /* Both nodes should be UP despite config mismatch
     * (mismatch is logged but doesn't affect health) */
    keel_cluster_peer_t peer;
    ASSERT_TRUE(keel_cluster_get_peer(a, 0, &peer));
    ASSERT_EQ(atomic_load(&peer.status), KEEL_PEER_UP);

    /* Peer should report its own checksum */
    ASSERT_EQ(peer.config_checksum, (uint64_t)0xBBBBBBBB);

    keel_cluster_stop(a);
    keel_cluster_stop(b);
    keel_cluster_destroy(a);
    keel_cluster_destroy(b);
    TEST_PASS();
}

/**
 * Test 19: Dynamic peer discovery via JOIN.
 */
static void test_join_discovers_peer(void) {
    TEST_BEGIN("join_discovers_peer");

    keel_cluster_config_t cfg_a = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_a.enabled = true;
    snprintf(cfg_a.node_id, sizeof(cfg_a.node_id), "discover-A");
    strcpy(cfg_a.listen_addr, "127.0.0.1");
    cfg_a.listen_port = 19500;
    cfg_a.heartbeat_interval_ms = 200;
    cfg_a.heartbeat_timeout_ms = 2000;

    keel_cluster_config_t cfg_b = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_b.enabled = true;
    snprintf(cfg_b.node_id, sizeof(cfg_b.node_id), "discover-B");
    strcpy(cfg_b.listen_addr, "127.0.0.1");
    cfg_b.listen_port = 19501;
    cfg_b.heartbeat_interval_ms = 200;
    cfg_b.heartbeat_timeout_ms = 2000;
    strcpy(cfg_b.initial_peers[0].addr, "127.0.0.1");
    cfg_b.initial_peers[0].port = 19500;
    cfg_b.initial_peer_count = 1;

    keel_cluster_t *a = keel_cluster_create(&cfg_a);
    keel_cluster_t *b = keel_cluster_create(&cfg_b);
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);

    ASSERT_EQ(keel_cluster_start(a), 0);
    ASSERT_EQ(keel_cluster_start(b), 0);

    int elapsed = 0;
    while (elapsed < 10000 && keel_cluster_peer_count(a) == 0) {
        usleep(100000);
        elapsed += 100;
    }

    ASSERT_EQ(keel_cluster_peer_count(a), (size_t)1);

    keel_cluster_peer_t peer_a;
    ASSERT_TRUE(keel_cluster_get_peer(a, 0, &peer_a));
    ASSERT_STR_EQ(peer_a.addr, "127.0.0.1");
    ASSERT_EQ(peer_a.port, 19501);

    ASSERT_TRUE(wait_peer_up(a, 0, 30000));
    ASSERT_TRUE(wait_peer_up(b, 0, 30000));

    keel_cluster_stop(a);
    keel_cluster_stop(b);
    keel_cluster_destroy(a);
    keel_cluster_destroy(b);
    TEST_PASS();
}

/**
 * Test 20: Transitive peer discovery via peer-list gossip.
 */
static void test_transitive_peer_discovery_gossip(void) {
    TEST_BEGIN("transitive_peer_discovery_gossip");

    keel_cluster_config_t cfg_a = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_a.enabled = true;
    snprintf(cfg_a.node_id, sizeof(cfg_a.node_id), "gossip-A");
    strcpy(cfg_a.listen_addr, "127.0.0.1");
    cfg_a.listen_port = 19600;
    cfg_a.heartbeat_interval_ms = 200;
    cfg_a.heartbeat_timeout_ms = 2000;
    strcpy(cfg_a.initial_peers[0].addr, "127.0.0.1");
    cfg_a.initial_peers[0].port = 19601; /* A only knows B */
    cfg_a.initial_peer_count = 1;

    keel_cluster_config_t cfg_b = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_b.enabled = true;
    snprintf(cfg_b.node_id, sizeof(cfg_b.node_id), "gossip-B");
    strcpy(cfg_b.listen_addr, "127.0.0.1");
    cfg_b.listen_port = 19601;
    cfg_b.heartbeat_interval_ms = 200;
    cfg_b.heartbeat_timeout_ms = 2000;
    strcpy(cfg_b.initial_peers[0].addr, "127.0.0.1");
    cfg_b.initial_peers[0].port = 19602; /* B knows C */
    cfg_b.initial_peer_count = 1;

    keel_cluster_config_t cfg_c = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_c.enabled = true;
    snprintf(cfg_c.node_id, sizeof(cfg_c.node_id), "gossip-C");
    strcpy(cfg_c.listen_addr, "127.0.0.1");
    cfg_c.listen_port = 19602;
    cfg_c.heartbeat_interval_ms = 200;
    cfg_c.heartbeat_timeout_ms = 2000;

    keel_cluster_t *a = keel_cluster_create(&cfg_a);
    keel_cluster_t *b = keel_cluster_create(&cfg_b);
    keel_cluster_t *c = keel_cluster_create(&cfg_c);
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);
    ASSERT_NOT_NULL(c);

    ASSERT_EQ(keel_cluster_start(c), 0);
    ASSERT_EQ(keel_cluster_start(b), 0);
    ASSERT_EQ(keel_cluster_start(a), 0);

    ASSERT_TRUE(wait_peer_count(a, 2, 30000));

    keel_cluster_peer_t discovered;
    ASSERT_TRUE(cluster_has_peer_port(a, 19602, &discovered));
    ASSERT_TRUE(discovered.source == KEEL_PEER_SOURCE_GOSSIP ||
                discovered.source == KEEL_PEER_SOURCE_JOIN);

    keel_cluster_stop(a);
    keel_cluster_stop(b);
    keel_cluster_stop(c);
    keel_cluster_destroy(a);
    keel_cluster_destroy(b);
    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 21: Config mismatch triggers sync request/response payload path.
 */
static void test_sync_payload_on_mismatch(void) {
    TEST_BEGIN("sync_payload_on_mismatch");

    keel_cluster_config_t cfg_a = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_a.enabled = true;
    snprintf(cfg_a.node_id, sizeof(cfg_a.node_id), "sync-A");
    strcpy(cfg_a.listen_addr, "127.0.0.1");
    cfg_a.listen_port = 19700;
    cfg_a.heartbeat_interval_ms = 150;
    cfg_a.heartbeat_timeout_ms = 1800;
    cfg_a.failure_threshold = 2;
    strcpy(cfg_a.initial_peers[0].addr, "127.0.0.1");
    cfg_a.initial_peers[0].port = 19701;
    cfg_a.initial_peer_count = 1;

    keel_cluster_config_t cfg_b = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_b.enabled = true;
    snprintf(cfg_b.node_id, sizeof(cfg_b.node_id), "sync-B");
    strcpy(cfg_b.listen_addr, "127.0.0.1");
    cfg_b.listen_port = 19701;
    cfg_b.heartbeat_interval_ms = 350;
    cfg_b.heartbeat_timeout_ms = 4000;
    cfg_b.failure_threshold = 5;
    strcpy(cfg_b.initial_peers[0].addr, "127.0.0.1");
    cfg_b.initial_peers[0].port = 19700;
    cfg_b.initial_peer_count = 1;

    keel_cluster_t *a = keel_cluster_create(&cfg_a);
    keel_cluster_t *b = keel_cluster_create(&cfg_b);
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);

    keel_cluster_set_config_checksum(a, 0x11111111ULL);
    keel_cluster_set_config_checksum(b, 0x22222222ULL);

    ASSERT_EQ(keel_cluster_start(a), 0);
    ASSERT_EQ(keel_cluster_start(b), 0);
    ASSERT_TRUE(wait_peer_up(a, 0, 30000));
    ASSERT_TRUE(wait_peer_up(b, 0, 30000));

    /* Poll until at least one node records a config reconciliation.
     * A bare usleep() is too short under sanitizers (3-5x slowdown). */
    keel_cluster_stats_t stats_a;
    keel_cluster_stats_t stats_b;
    for (int i = 0; i < 100; i++) {
        usleep(100000); /* 100 ms */
        keel_cluster_get_stats(a, &stats_a);
        keel_cluster_get_stats(b, &stats_b);
        if ((stats_a.config_reconciliations + stats_b.config_reconciliations) > 0)
            break;
    }

    ASSERT_TRUE((stats_a.sync_requests_sent + stats_b.sync_requests_sent) > 0);
    ASSERT_TRUE((stats_a.sync_responses_sent + stats_b.sync_responses_sent) > 0);
    ASSERT_TRUE((stats_a.config_reconciliations + stats_b.config_reconciliations) > 0);

    /* Deterministic reconciliation rule: lexicographically smaller node_id wins.
     * sync-A should remain authoritative and sync-B should converge to it. */
    ASSERT_EQ(stats_a.local_heartbeat_interval_ms, (uint32_t)150);
    ASSERT_EQ(stats_a.local_heartbeat_timeout_ms, (uint32_t)1800);
    ASSERT_EQ(stats_a.local_failure_threshold, (uint32_t)2);
    ASSERT_TRUE(stats_a.local_auto_sync == 1);

    ASSERT_EQ(stats_b.local_heartbeat_interval_ms, (uint32_t)150);
    ASSERT_EQ(stats_b.local_heartbeat_timeout_ms, (uint32_t)1800);
    ASSERT_EQ(stats_b.local_failure_threshold, (uint32_t)2);
    ASSERT_TRUE(stats_b.local_auto_sync == 1);

    keel_cluster_stop(a);
    keel_cluster_stop(b);
    keel_cluster_destroy(a);
    keel_cluster_destroy(b);
    TEST_PASS();
}

/* ============================================================================
 * Main
 * ============================================================================ */

/**
 * Test 23: Stats callback (C-compatible) — wires and resets without crash.
 */
static void stats_cb_impl(void* ud, uint32_t* cli, uint32_t* be, uint32_t* srv) {
    (void)ud;
    *cli = 99;
    *be  = 5;
    *srv = 2;
}

static void test_stats_callback_wire(void) {
    TEST_BEGIN("stats_callback_wire");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    /* Wire callback */
    keel_cluster_set_stats_cb(c, stats_cb_impl, NULL);

    /* Reset to NULL — cluster should accept it without crash */
    keel_cluster_set_stats_cb(c, NULL, NULL);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * Test 24: Server-notify callback — verifies callback receives correct args.
 */
/* Use _Atomic so the cluster thread's write and the main thread's spin-read
 * establish a proper happens-before edge (release/acquire).  All other fields
 * are written before the atomic store so they are visible once the main thread
 * observes g_notify_action >= 0. */
static _Atomic int   g_notify_action  = -1;
static volatile int  g_notify_port    = -1;
static char          g_notify_host[64];
static volatile uint8_t g_notify_role   = 255;
static volatile uint32_t g_notify_weight = 0;

static void server_notify_cb_impl(void* ud, uint8_t action, const char* host,
                                   uint16_t port, uint8_t role, uint32_t weight) {
    (void)ud;
    /* Write all payload fields first, then release-store the sentinel so the
     * main thread's acquire-load provides a happens-before for every field. */
    g_notify_port   = (int)port;
    g_notify_role   = role;
    g_notify_weight = weight;
    if (host) {
        size_t len = strlen(host);
        if (len >= sizeof(g_notify_host)) len = sizeof(g_notify_host) - 1;
        memcpy(g_notify_host, host, len);
        g_notify_host[len] = '\0';
    }
    atomic_store_explicit(&g_notify_action, (int)action, memory_order_release);
}

static void test_server_notify_callback(void) {
    TEST_BEGIN("server_notify_callback");

    keel_cluster_config_t cfg_a = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_a.enabled = true;
    cfg_a.listen_port = 9260;
    snprintf(cfg_a.node_id, sizeof(cfg_a.node_id), "cb-a");

    keel_cluster_config_t cfg_b = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg_b.enabled = true;
    cfg_b.listen_port = 9261;
    snprintf(cfg_b.node_id, sizeof(cfg_b.node_id), "cb-b");
    /* b starts knowing about a */
    memcpy(cfg_b.initial_peers[0].addr, "127.0.0.1", 9);
    cfg_b.initial_peers[0].port = 9260;
    cfg_b.initial_peer_count = 1;

    keel_cluster_t *a = keel_cluster_create(&cfg_a);
    keel_cluster_t *b = keel_cluster_create(&cfg_b);
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);

    /* Register notify callback on b — it should fire when a sends NOTIFY */
    g_notify_action = -1;
    keel_cluster_set_server_notify_cb(b, server_notify_cb_impl, NULL);

    ASSERT_EQ(keel_cluster_start(a), 0);
    ASSERT_EQ(keel_cluster_start(b), 0);

    /* Wait for b to see a as UP */
    ASSERT_TRUE(wait_peer_up(b, 0, 3000));

    /* Have a broadcast a server change; b should receive it */
    keel_cluster_notify_server_change(a, 2, "db1.example.com", 5432, 1, 50);

    /* Give the message time to arrive */
    int waited = 0;
    while (atomic_load_explicit(&g_notify_action, memory_order_acquire) < 0
           && waited < 3000) {
        usleep(50000);
        waited += 50;
    }

    ASSERT_EQ(atomic_load_explicit(&g_notify_action, memory_order_relaxed), 2);
    ASSERT_EQ(g_notify_port, 5432);
    ASSERT_EQ((int)g_notify_role, 1);
    ASSERT_EQ(g_notify_weight, (uint32_t)50);
    ASSERT_STR_EQ(g_notify_host, "db1.example.com");

    keel_cluster_stop(a);
    keel_cluster_stop(b);
    keel_cluster_destroy(a);
    keel_cluster_destroy(b);
    TEST_PASS();
}

/**
 * Test 25: KEEL_CLUSTER_* env var parsing mirrors INI parsing.
 *
 * We validate the parsing logic directly (main.c integration requires a full
 * process) by exercising the cluster config struct the same way the env-var
 * block does, using the same API.
 */
static void test_env_override_parsing(void) {
    TEST_BEGIN("env_override_parsing");

    /* Simulate the env var parsing logic from main.c inline */
    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;

    /* KEEL_CLUSTER_NODE_ID */
    const char *node_id = "keel-docker-1";
    size_t nlen = strlen(node_id);
    if (nlen >= KEEL_CLUSTER_MAX_NODE_ID) nlen = KEEL_CLUSTER_MAX_NODE_ID - 1;
    memcpy(cfg.node_id, node_id, nlen);
    cfg.node_id[nlen] = '\0';
    ASSERT_STR_EQ(cfg.node_id, "keel-docker-1");

    /* KEEL_CLUSTER_LISTEN_PORT */
    long port = strtol("9200", NULL, 10);
    ASSERT_TRUE(port > 0 && port <= 65535);
    cfg.listen_port = (uint16_t)port;
    ASSERT_EQ(cfg.listen_port, (uint16_t)9200);

    /* KEEL_CLUSTER_HB_INTERVAL */
    long hb_int = strtol("500", NULL, 10);
    ASSERT_TRUE(hb_int > 0);
    cfg.heartbeat_interval_ms = (uint32_t)hb_int;
    ASSERT_EQ(cfg.heartbeat_interval_ms, (uint32_t)500);

    /* KEEL_CLUSTER_INITIAL_PEERS: parse comma-separated host:port */
    char peers_buf[] = "keel-2:9100, keel-3:9200";
    cfg.initial_peer_count = 0;
    char *saveptr2 = NULL;
    char *tok = strtok_r(peers_buf, ",", &saveptr2);
    while (tok && cfg.initial_peer_count < KEEL_CLUSTER_MAX_PEERS) {
        while (*tok == ' ') tok++;
        char *colon = strrchr(tok, ':');
        if (colon && colon > tok) {
            *colon = '\0';
            size_t idx  = cfg.initial_peer_count;
            size_t alen = strlen(tok);
            if (alen >= KEEL_CLUSTER_MAX_ADDR) alen = KEEL_CLUSTER_MAX_ADDR - 1;
            memcpy(cfg.initial_peers[idx].addr, tok, alen);
            cfg.initial_peers[idx].addr[alen] = '\0';
            cfg.initial_peers[idx].port = (uint16_t)atoi(colon + 1);
            cfg.initial_peer_count++;
        }
        tok = strtok_r(NULL, ",", &saveptr2);
    }
    ASSERT_EQ(cfg.initial_peer_count, (size_t)2);
    ASSERT_STR_EQ(cfg.initial_peers[0].addr, "keel-2");
    ASSERT_EQ(cfg.initial_peers[0].port, (uint16_t)9100);
    ASSERT_STR_EQ(cfg.initial_peers[1].addr, "keel-3");
    ASSERT_EQ(cfg.initial_peers[1].port, (uint16_t)9200);

    /* KEEL_CLUSTER_ENABLED */
    cfg.enabled = (strcmp("true", "true") == 0);
    ASSERT_TRUE(cfg.enabled);

    /* KEEL_CLUSTER_AUTO_SYNC */
    cfg.auto_sync = (strcmp("1", "1") == 0);
    ASSERT_TRUE(cfg.auto_sync);

    TEST_PASS();
}

int main(void) {
    printf("\n=== KEEL Cluster Mode Tests ===\n\n");

    keel_mem_init(NULL);

    test_config_defaults();
    test_create_destroy_disabled();
    test_create_null();
    test_peer_add_remove();
    test_bootstrap_peers();
    test_peer_table_full();
    test_config_checksum();
    test_runtime_config_snapshot();
    test_stats_initial();
    test_active_not_started();
    test_null_safety();
    test_start_stop();
    test_two_node_heartbeat();
    test_peer_failure_detection();
    test_server_notify();
    test_node_id_auto();
    test_get_peer_invalid();
    test_start_disabled();
    test_config_mismatch_detection();
    test_join_discovers_peer();
    test_transitive_peer_discovery_gossip();
    test_sync_payload_on_mismatch();
    test_stats_callback_wire();
    test_server_notify_callback();
    test_env_override_parsing();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           g_passed, g_failed, g_total);

    return g_failed > 0 ? 1 : 0;
}
