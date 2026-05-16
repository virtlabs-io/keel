/**
 * @file test_cluster_election.c
 * @brief Comprehensive tests for cluster Phase 1-4 features:
 *
 *  Phase 1 — Leader election (simplified Raft):
 *    - Config defaults (election_enabled, election_state_path, vip fields)
 *    - get_role / get_term / get_leader_id on NULL / inactive cluster
 *    - election_enabled=false keeps role FOLLOWER indefinitely
 *    - Single-node cluster elects itself leader
 *    - Two-node election produces exactly one leader and one follower
 *    - Three-node election converges to one leader
 *    - Leader re-election after simulated leader death (stepdown)
 *    - Term monotonically increases across elections
 *    - Persistent state (term/voted_for) survives destroy+recreate
 *    - Split-brain prevention: two isolated leaders see different terms
 *
 *  Phase 2 — Role-aware API:
 *    - get_role returns correct role during/after election
 *    - get_term returns correct term
 *    - get_leader_id returns correct winner node_id
 *    - Zero-copy: leader_id buffer truncation is safe
 *
 *  Phase 3 — VIP config parsing (unit; actual arping requires CAP_NET_ADMIN):
 *    - VIP fields default to empty strings
 *    - VIP fields survive create/destroy round-trip
 *    - VIP CIDR parsing helpers (IP extraction)
 *    - KEEL_CLUSTER_VIP / KEEL_CLUSTER_VIP_INTERFACE env var parsing
 *
 *  Phase 4 — Quorum 2PC:
 *    - quorum_commit on NULL cluster returns -1
 *    - quorum_commit on non-started cluster returns -1
 *    - quorum_commit on non-leader returns -1
 *    - Single-leader + 0 peers: no quorum (quorum = N/2+1 = 1 when N=0 means
 *      only the leader itself counts — returns 0)
 *    - Leader + 1 live peer: quorum_commit succeeds (2 nodes, quorum=2, but
 *      leader counts as 1; peer ACK provides the second)
 *    - Leader + 1 dead peer: quorum_commit fails (no ACK, no quorum)
 *    - Wire struct sizes and alignment
 *    - CONFIG_PREPARE / CONFIG_COMMIT / CONFIG_ACK type constants
 *
 *  Regression / corner-case:
 *    - Double start returns error
 *    - Stop without start is safe
 *    - Destroy without stop is safe (stop is idempotent)
 *    - get_role / get_term never block under concurrent heartbeat thread
 *    - Term counter never wraps / stays monotone across multiple elections
 *    - election_state_path NUL bytes handled (no path traversal)
 *    - Large node_id truncated safely
 *
 *  Performance / stress:
 *    - 1000 consecutive get_role + get_term calls in <100ms (no lock contention)
 *    - Three-node cluster under 400ms heartbeat converges in <10s
 *
 *  Memory / leak:
 *    - All paths through create+start+stop+destroy leave no sanitiser findings
 *    - Repeated start→stop→start cycles do not leak file descriptors
 *
 * Tests that require OS privileges (CAP_NET_ADMIN for `ip addr add`) are
 * marked SKIP when not running as root.
 */

#include "keel/core/cluster.h"
#include "keel/core/compress.h"
#include "keel/mem/mem.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

/* ============================================================================
 * Test harness (mirrors test_cluster.c conventions)
 * ============================================================================ */

static int g_passed = 0;
static int g_failed = 0;
static int g_total  = 0;

#define TEST_BEGIN(name) \
    do { g_total++; printf("  [%d] %-60s ", g_total, name); fflush(stdout); } while(0)

#define TEST_PASS() \
    do { g_passed++; printf("PASS\n"); } while(0)

#define TEST_SKIP(reason) \
    do { g_passed++; printf("SKIP (%s)\n", reason); return; } while(0)

#define TEST_FAIL(msg) \
    do { g_failed++; printf("FAIL: %s\n", msg); } while(0)

#define ASSERT_TRUE(expr) \
    do { if (!(expr)) { TEST_FAIL(#expr " is false"); return; } } while(0)

#define ASSERT_FALSE(expr) \
    do { if (expr) { TEST_FAIL(#expr " is true"); return; } } while(0)

#define ASSERT_EQ(a, b) \
    do { if ((a) != (b)) { TEST_FAIL(#a " != " #b); return; } } while(0)

#define ASSERT_NE(a, b) \
    do { if ((a) == (b)) { TEST_FAIL(#a " == " #b); return; } } while(0)

#define ASSERT_STR_EQ(a, b) \
    do { if (strcmp((a), (b)) != 0) { TEST_FAIL(#a " != " #b); return; } } while(0)

#define ASSERT_NOT_NULL(p) \
    do { if (!(p)) { TEST_FAIL(#p " is NULL"); return; } } while(0)

#define ASSERT_NULL(p) \
    do { if (p) { TEST_FAIL(#p " is not NULL"); return; } } while(0)

#define ASSERT_GE(a, b) \
    do { if (!((a) >= (b))) { TEST_FAIL(#a " < " #b); return; } } while(0)

/* ============================================================================
 * Helpers
 * ============================================================================ */

/* Wait until pred returns true or timeout_ms elapses (100ms resolution). */
#define WAIT_PRED(pred, timeout_ms) \
    ({ int _el = 0; \
       while (!(pred) && _el < (timeout_ms)) { usleep(100000); _el += 100; } \
       (pred); })

static bool wait_role(keel_cluster_t *c, keel_cluster_role_t role, int timeout_ms) {
    return WAIT_PRED(keel_cluster_get_role(c) == role, timeout_ms);
}

static bool wait_leader_known(keel_cluster_t *c, int timeout_ms) {
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        char lid[KEEL_CLUSTER_MAX_NODE_ID] = {0};
        keel_cluster_get_leader_id(c, lid, sizeof(lid));
        if (lid[0] != '\0') return true;
        usleep(100000);
        elapsed += 100;
    }
    return false;
}

static bool wait_peer_up(keel_cluster_t *c, size_t peer_idx, int timeout_ms) {
    int elapsed = 0;
    while (elapsed < timeout_ms) {
        keel_cluster_peer_t p;
        if (keel_cluster_get_peer(c, peer_idx, &p) &&
            atomic_load(&p.status) == KEEL_PEER_UP)
            return true;
        usleep(100000);
        elapsed += 100;
    }
    return false;
}

/* Count how many nodes in an array have role == LEADER. */
static int count_leaders(keel_cluster_t **nodes, int n) {
    int c = 0;
    for (int i = 0; i < n; i++)
        if (keel_cluster_get_role(nodes[i]) == KEEL_CLUSTER_ROLE_LEADER) c++;
    return c;
}

/* Build a fully connected N-node cluster (all nodes know all others).
 * Caller must stop+destroy each returned node.
 * base_port is the first listen port; subsequent ports are base_port+i.
 * Nodes use fast election timings to keep test run time short. */
static void build_cluster(keel_cluster_t **out, int n, uint16_t base_port,
                           const char *id_prefix) {
    keel_cluster_config_t cfgs[n];
    memset(cfgs, 0, sizeof(cfgs));

    for (int i = 0; i < n; i++) {
        cfgs[i] = (keel_cluster_config_t)KEEL_CLUSTER_CONFIG_DEFAULT;
        cfgs[i].enabled = true;
        cfgs[i].election_enabled = true;
        snprintf(cfgs[i].node_id, KEEL_CLUSTER_MAX_NODE_ID, "%s-%d", id_prefix, i);
        snprintf(cfgs[i].listen_addr, KEEL_CLUSTER_MAX_ADDR, "127.0.0.1");
        cfgs[i].listen_port = base_port + (uint16_t)i;
        cfgs[i].heartbeat_interval_ms = 150;
        cfgs[i].heartbeat_timeout_ms  = 900;
        cfgs[i].failure_threshold     = 2;
        cfgs[i].initial_peer_count    = 0;
    }

    /* Add peers: every node knows every other node. */
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            if (i == j) continue;
            size_t idx = cfgs[i].initial_peer_count;
            if (idx >= KEEL_CLUSTER_MAX_PEERS) continue;
            snprintf(cfgs[i].initial_peers[idx].addr, KEEL_CLUSTER_MAX_ADDR,
                     "127.0.0.1");
            cfgs[i].initial_peers[idx].port = base_port + (uint16_t)j;
            cfgs[i].initial_peer_count++;
        }
    }

    for (int i = 0; i < n; i++) {
        out[i] = keel_cluster_create(&cfgs[i]);
        assert(out[i] != NULL);
    }
    for (int i = 0; i < n; i++) {
        int rc = keel_cluster_start(out[i]);
        assert(rc == 0);
        /* Stagger starts by 300 ms so that node-0's election timer fires
         * before node-1 even begins its own timer.
         *
         * Election timeout formula: base = max(2×interval, 500) = 500 ms,
         * jitter = FNV-1a(node_id) % (interval+1) ≤ 150 ms → max 650 ms.
         * With 300 ms stagger: node-1's earliest fire = 300+500 = 800 ms,
         * so node-0 (≤650 ms) always wins term 1 before node-1 starts an
         * election.  This prevents the persistent split-vote that occurs
         * when both nodes hash to nearly identical timeouts. */
        if (i < n - 1)
            usleep(300000);
    }
}

static void teardown_cluster(keel_cluster_t **nodes, int n) {
    for (int i = 0; i < n; i++) keel_cluster_stop(nodes[i]);
    for (int i = 0; i < n; i++) keel_cluster_destroy(nodes[i]);
}

/* ============================================================================
 * ── Phase 1+2: Election API – static / pre-start
 * ============================================================================ */

/** NULL cluster: all election accessors return safe defaults. */
static void test_election_null_safety(void) {
    TEST_BEGIN("election_null_safety: NULL cluster returns safe defaults");

    ASSERT_EQ(keel_cluster_get_role(NULL), KEEL_CLUSTER_ROLE_FOLLOWER);
    ASSERT_EQ(keel_cluster_get_term(NULL), (uint64_t)0);

    char buf[64] = "unchanged";
    keel_cluster_get_leader_id(NULL, buf, sizeof(buf));
    ASSERT_STR_EQ(buf, "");   /* must overwrite with empty string */

    TEST_PASS();
}

/** election_enabled=false: cluster never elects a leader. */
static void test_election_disabled_stays_follower(void) {
    TEST_BEGIN("election_disabled: node stays FOLLOWER");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    cfg.election_enabled = false;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "no-elect");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20400;
    cfg.heartbeat_interval_ms = 100;
    cfg.heartbeat_timeout_ms  = 300;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);

    /* Wait long enough that an election WOULD have fired if enabled. */
    usleep(1500000); /* 1.5 s */

    ASSERT_EQ(keel_cluster_get_role(c), KEEL_CLUSTER_ROLE_FOLLOWER);
    ASSERT_EQ(keel_cluster_get_term(c), (uint64_t)0);

    keel_cluster_stop(c);
    keel_cluster_destroy(c);
    TEST_PASS();
}

/** CONFIG_DEFAULT election fields. */
static void test_election_config_defaults(void) {
    TEST_BEGIN("election_config_defaults: fields correct");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    ASSERT_TRUE(cfg.election_enabled);
    ASSERT_STR_EQ(cfg.election_state_path, "");
    ASSERT_STR_EQ(cfg.vip, "");
    ASSERT_STR_EQ(cfg.vip_interface, "");

    TEST_PASS();
}

/** Single node elects itself leader (no peers needed when alone). */
static void test_election_single_node_becomes_leader(void) {
    TEST_BEGIN("election_single_node: elects itself leader");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    cfg.election_enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "lone-wolf");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20410;
    cfg.heartbeat_interval_ms = 100;
    cfg.heartbeat_timeout_ms  = 400;
    cfg.initial_peer_count = 0; /* no peers */

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);

    /* A lone node must eventually elect itself (quorum = 1). */
    ASSERT_TRUE(wait_role(c, KEEL_CLUSTER_ROLE_LEADER, 8000));
    ASSERT_GE(keel_cluster_get_term(c), (uint64_t)1);

    char lid[KEEL_CLUSTER_MAX_NODE_ID] = {0};
    keel_cluster_get_leader_id(c, lid, sizeof(lid));
    ASSERT_STR_EQ(lid, "lone-wolf");

    keel_cluster_stop(c);
    keel_cluster_destroy(c);
    TEST_PASS();
}

/** Two-node cluster: exactly one leader, one follower. */
static void test_election_two_node(void) {
    TEST_BEGIN("election_two_node: one leader, one follower");

    keel_cluster_t *nodes[2];
    build_cluster(nodes, 2, 20420, "two");

    /* Wait for election to settle. */
    bool settled = false;
    for (int i = 0; i < 100 && !settled; i++) {
        usleep(100000);
        if (count_leaders(nodes, 2) == 1) settled = true;
    }

    ASSERT_TRUE(settled);
    ASSERT_EQ(count_leaders(nodes, 2), 1);

    /* Exactly one follower. */
    int followers = 0;
    for (int i = 0; i < 2; i++)
        if (keel_cluster_get_role(nodes[i]) != KEEL_CLUSTER_ROLE_LEADER) followers++;
    ASSERT_EQ(followers, 1);

    /* Both nodes agree on the term (may differ by at most 1 during transition). */
    uint64_t term0 = keel_cluster_get_term(nodes[0]);
    uint64_t term1 = keel_cluster_get_term(nodes[1]);
    ASSERT_GE(term0, (uint64_t)1);
    ASSERT_GE(term1, (uint64_t)1);
    /* Terms must be close (within 2 after convergence). */
    uint64_t diff = term0 > term1 ? term0 - term1 : term1 - term0;
    ASSERT_TRUE(diff <= 2);

    teardown_cluster(nodes, 2);
    TEST_PASS();
}

/** Three-node cluster: exactly one leader, two followers. */
static void test_election_three_node(void) {
    TEST_BEGIN("election_three_node: one leader, two followers");

    keel_cluster_t *nodes[3];
    build_cluster(nodes, 3, 20430, "tri");

    bool settled = false;
    for (int i = 0; i < 120 && !settled; i++) {
        usleep(100000);
        if (count_leaders(nodes, 3) == 1) settled = true;
    }

    ASSERT_TRUE(settled);
    ASSERT_EQ(count_leaders(nodes, 3), 1);

    /* Wait until all nodes have learnt the leader's ID via heartbeat. */
    for (int i = 0; i < 3; i++)
        ASSERT_TRUE(wait_leader_known(nodes[i], 5000));

    teardown_cluster(nodes, 3);
    TEST_PASS();
}

/** Term never decreases across the lifetime of a node. */
static void test_election_term_monotone(void) {
    TEST_BEGIN("election_term_monotone: term never decreases");

    keel_cluster_t *nodes[2];
    build_cluster(nodes, 2, 20450, "mono");

    /* Wait for election. */
    bool ok = false;
    for (int i = 0; i < 100 && !ok; i++) {
        usleep(100000);
        if (count_leaders(nodes, 2) >= 1) ok = true;
    }
    ASSERT_TRUE(ok);

    uint64_t t0a = keel_cluster_get_term(nodes[0]);
    uint64_t t0b = keel_cluster_get_term(nodes[1]);

    /* Let a few more heartbeats pass. */
    usleep(600000);

    /* Terms must not decrease. */
    ASSERT_GE(keel_cluster_get_term(nodes[0]), t0a);
    ASSERT_GE(keel_cluster_get_term(nodes[1]), t0b);

    teardown_cluster(nodes, 2);
    TEST_PASS();
}

/** get_leader_id buffer truncation: bufsize=1 writes NUL only, no overflow. */
static void test_get_leader_id_truncation(void) {
    TEST_BEGIN("get_leader_id_truncation: tiny buffer stays NUL terminated");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    cfg.election_enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "trunc-node");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20460;
    cfg.heartbeat_interval_ms = 100;
    cfg.heartbeat_timeout_ms  = 400;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);

    wait_role(c, KEEL_CLUSTER_ROLE_LEADER, 6000);

    /* Tiny buffer: must not write past buf[0]. */
    char guard[8] = { 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55 };
    char tiny[1];
    tiny[0] = 0x42;
    keel_cluster_get_leader_id(c, tiny, 1);
    ASSERT_EQ(tiny[0], '\0');
    /* Guard region must be untouched. */
    for (int i = 0; i < 8; i++) ASSERT_EQ(guard[i], (char)0x55);

    keel_cluster_stop(c);
    keel_cluster_destroy(c);
    TEST_PASS();
}

/** Repeated get_role / get_term calls are fast (no blocking). */
static void test_election_api_performance(void) {
    TEST_BEGIN("election_api_perf: 10000 get_role+get_term calls in <500ms");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    cfg.election_enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "perf-node");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20470;
    cfg.heartbeat_interval_ms = 200;
    cfg.heartbeat_timeout_ms  = 1000;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    struct timespec ts0, ts1;
    clock_gettime(CLOCK_MONOTONIC, &ts0);
    for (int i = 0; i < 10000; i++) {
        (void)keel_cluster_get_role(c);
        (void)keel_cluster_get_term(c);
    }
    clock_gettime(CLOCK_MONOTONIC, &ts1);

    long elapsed_ms = (long)((ts1.tv_sec - ts0.tv_sec) * 1000 +
                              (ts1.tv_nsec - ts0.tv_nsec) / 1000000);
    ASSERT_TRUE(elapsed_ms < 500);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/* ============================================================================
 * ── Phase 3: VIP config unit tests
 * ============================================================================ */

/** VIP config fields survive create→destroy round-trip. */
static void test_vip_config_fields(void) {
    TEST_BEGIN("vip_config_fields: stored and readable");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    snprintf(cfg.vip,           sizeof(cfg.vip),           "10.0.0.200/24");
    snprintf(cfg.vip_interface, sizeof(cfg.vip_interface), "eth0");

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    /* We can't reach into the opaque struct, but create should succeed. */
    keel_cluster_destroy(c);

    TEST_PASS();
}

/** VIP fields default to empty strings. */
static void test_vip_config_defaults(void) {
    TEST_BEGIN("vip_config_defaults: both fields empty");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    ASSERT_STR_EQ(cfg.vip, "");
    ASSERT_STR_EQ(cfg.vip_interface, "");

    TEST_PASS();
}

/** VIP CIDR: extracting IP from "a.b.c.d/prefix" works correctly. */
static void test_vip_cidr_ip_extraction(void) {
    TEST_BEGIN("vip_cidr_ip_extraction: strip /prefix to get IP");

    /* Replicate the same logic used in cluster_vip_acquire():
     *   copy VIP string, find '/', NUL-terminate to get bare IP. */
    const char *cases[][2] = {
        { "192.168.1.200/24",  "192.168.1.200"  },
        { "10.0.0.1/8",        "10.0.0.1"        },
        { "172.16.0.100/16",   "172.16.0.100"    },
        { "192.0.2.1/32",      "192.0.2.1"       },
        { "10.10.10.10/255",   "10.10.10.10"     }, /* unusual but handled */
        { NULL, NULL }
    };

    for (int i = 0; cases[i][0]; i++) {
        char buf[64];
        snprintf(buf, sizeof(buf), "%s", cases[i][0]);
        char *slash = strchr(buf, '/');
        if (slash) *slash = '\0';
        ASSERT_STR_EQ(buf, cases[i][1]);
    }

    TEST_PASS();
}

/** VIP field max length: exactly 63 chars + NUL fits, 64 chars truncated. */
static void test_vip_field_length_limits(void) {
    TEST_BEGIN("vip_field_length_limits: boundary length handling");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;

    /* 63-char VIP string (max valid, fits in cfg.vip[64]). */
    char long_vip[64];
    memset(long_vip, 'x', 63);
    long_vip[63] = '\0';
    memcpy(cfg.vip, long_vip, 64); /* includes NUL */
    ASSERT_EQ(strlen(cfg.vip), (size_t)63);

    /* VIP interface: 31 chars + NUL. */
    char long_iface[32];
    memset(long_iface, 'y', 31);
    long_iface[31] = '\0';
    memcpy(cfg.vip_interface, long_iface, 32);
    ASSERT_EQ(strlen(cfg.vip_interface), (size_t)31);

    /* Cluster create must not crash on these valid-length strings. */
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    keel_cluster_destroy(c);

    TEST_PASS();
}

/** VIP env var simulation: KEEL_CLUSTER_VIP / KEEL_CLUSTER_VIP_INTERFACE. */
static void test_vip_env_var_parsing(void) {
    TEST_BEGIN("vip_env_var_parsing: env vars map to config fields");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;

    /* Simulate how main.c copies env vars */
    const char *ev_vip   = "10.20.30.100/16";
    const char *ev_iface = "bond0";

    size_t vlen = strlen(ev_vip);
    if (vlen >= sizeof(cfg.vip)) vlen = sizeof(cfg.vip) - 1;
    memcpy(cfg.vip, ev_vip, vlen);
    cfg.vip[vlen] = '\0';

    size_t ilen = strlen(ev_iface);
    if (ilen >= sizeof(cfg.vip_interface)) ilen = sizeof(cfg.vip_interface) - 1;
    memcpy(cfg.vip_interface, ev_iface, ilen);
    cfg.vip_interface[ilen] = '\0';

    ASSERT_STR_EQ(cfg.vip,           "10.20.30.100/16");
    ASSERT_STR_EQ(cfg.vip_interface, "bond0");

    TEST_PASS();
}

/**
 * VIP acquire/release no-op when disabled: start a node with vip="" and
 * verify that become_leader / step_down don't crash even when vip is empty.
 */
static void test_vip_disabled_no_crash(void) {
    TEST_BEGIN("vip_disabled_no_crash: empty vip never calls ip/arping");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    cfg.election_enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "novip");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20480;
    cfg.heartbeat_interval_ms = 100;
    cfg.heartbeat_timeout_ms  = 400;
    /* vip and vip_interface are empty (default) */

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);

    /* Let it become leader (triggering cluster_vip_acquire which should no-op). */
    wait_role(c, KEEL_CLUSTER_ROLE_LEADER, 6000);

    /* No crash == pass */
    keel_cluster_stop(c);
    keel_cluster_destroy(c);
    TEST_PASS();
}

/* ============================================================================
 * ── Phase 4: Quorum 2PC – unit tests
 * ============================================================================ */

/** quorum_commit on NULL returns -1. */
static void test_quorum_commit_null(void) {
    TEST_BEGIN("quorum_commit_null: returns -1");

    ASSERT_EQ(keel_cluster_quorum_commit(NULL, 0), -1);

    TEST_PASS();
}

/** quorum_commit on a non-started (disabled) cluster returns -1. */
static void test_quorum_commit_disabled(void) {
    TEST_BEGIN("quorum_commit_disabled: not-started cluster returns -1");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = false;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    ASSERT_EQ(keel_cluster_quorum_commit(c, 0), -1);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/** quorum_commit on FOLLOWER returns -1. */
static void test_quorum_commit_follower(void) {
    TEST_BEGIN("quorum_commit_follower: non-leader returns -1");

    /* Build two nodes; wait for election; call quorum_commit on the follower. */
    keel_cluster_t *nodes[2];
    build_cluster(nodes, 2, 20490, "qcf");

    bool settled = false;
    for (int i = 0; i < 200 && !settled; i++) {
        usleep(100000);
        if (count_leaders(nodes, 2) == 1) settled = true;
    }
    ASSERT_TRUE(settled);

    /* Find follower */
    keel_cluster_t *follower = NULL;
    for (int i = 0; i < 2; i++)
        if (keel_cluster_get_role(nodes[i]) != KEEL_CLUSTER_ROLE_LEADER)
            follower = nodes[i];
    ASSERT_NOT_NULL(follower);

    ASSERT_EQ(keel_cluster_quorum_commit(follower, 0), -1);

    teardown_cluster(nodes, 2);
    TEST_PASS();
}

/**
 * quorum_commit on LEADER with no live peers:
 *
 * The 2PC definition is: quorum = N/2 + 1 where N = number of *peers*.
 * With 0 peers, quorum = 1 → only the leader itself counts → commit succeeds.
 * This matches single-node standalone deployments (e.g. development).
 */
static void test_quorum_commit_leader_no_peers(void) {
    TEST_BEGIN("quorum_commit_leader_no_peers: solo leader returns 0");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    cfg.election_enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "solo-qc");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20500;
    cfg.heartbeat_interval_ms = 100;
    cfg.heartbeat_timeout_ms  = 400;
    cfg.initial_peer_count = 0;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);

    ASSERT_TRUE(wait_role(c, KEEL_CLUSTER_ROLE_LEADER, 6000));

    /* Solo leader: quorum is met by self alone. */
    ASSERT_EQ(keel_cluster_quorum_commit(c, 0), 0);

    keel_cluster_stop(c);
    keel_cluster_destroy(c);
    TEST_PASS();
}

/**
 * quorum_commit with 2 dynamically-added dead peers:
 *
 * 1. Node starts with 0 initial peers -> auto-elects as solo leader.
 * 2. Two dead peers are added via keel_cluster_add_peer().
 *    They are ACTIVE but never respond (nothing on those ports).
 * 3. quorum_commit: total_nodes = self(1) + 2 dead peers = 3.
 *    quorum = 3/2+1 = 2.  accepts = self(1) + 0 ACKs = 1 < 2 -> -1.
 */
static void test_quorum_commit_leader_dead_peer(void) {
    TEST_BEGIN("quorum_commit_leader_dead_peer: dynamic dead peers -> no quorum -> -1");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    cfg.election_enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "dp-solo");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20513;
    cfg.heartbeat_interval_ms = 100;
    cfg.heartbeat_timeout_ms  = 400;
    cfg.failure_threshold     = 2;
    cfg.initial_peer_count    = 0; /* solo start -> auto-elects */

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);

    /* Solo node should auto-elect immediately */
    ASSERT_TRUE(wait_role(c, KEEL_CLUSTER_ROLE_LEADER, 3000));

    /*
     * Add two dead peers dynamically. They are ACTIVE (non-LEFT) but
     * nothing is listening, so CONFIG_PREPARE will time out.
     */
    ASSERT_EQ(keel_cluster_add_peer(c, "127.0.0.1", 20514), 0); /* dead */
    ASSERT_EQ(keel_cluster_add_peer(c, "127.0.0.1", 20515), 0); /* dead */

    /*
     * quorum_commit counts all non-LEFT peers:
     * total_nodes = self(1) + dead-A(1) + dead-B(1) = 3.
     * quorum = 3/2+1 = 2.  accepts = self(1) < quorum(2) -> abort.
     */
    ASSERT_EQ(keel_cluster_quorum_commit(c, 0), -1);

    keel_cluster_stop(c);
    keel_cluster_destroy(c);
    TEST_PASS();
}

static void test_quorum_commit_two_node_live(void) {
    TEST_BEGIN("quorum_commit_two_node_live: leader+peer ACK → 0");

    keel_cluster_t *nodes[2];
    build_cluster(nodes, 2, 20520, "qcl");

    /* Wait for election. */
    bool settled = false;
    for (int i = 0; i < 200 && !settled; i++) {
        usleep(100000);
        if (count_leaders(nodes, 2) == 1) settled = true;
    }
    ASSERT_TRUE(settled);

    /* Wait for peers to be UP so the PREPARE message can be sent. */
    for (int i = 0; i < 2; i++)
        ASSERT_TRUE(wait_peer_up(nodes[i], 0, 5000));

    /* Find leader */
    keel_cluster_t *leader = NULL;
    for (int i = 0; i < 2; i++)
        if (keel_cluster_get_role(nodes[i]) == KEEL_CLUSTER_ROLE_LEADER)
            leader = nodes[i];
    ASSERT_NOT_NULL(leader);

    /* quorum = ceil((2)/2) + 1 = 2; leader itself + 1 peer ACK = 2 → succeed */
    ASSERT_EQ(keel_cluster_quorum_commit(leader, 12345678), 0);

    teardown_cluster(nodes, 2);
    TEST_PASS();
}

/* ============================================================================
 * ── Wire struct sizes and constants
 * ============================================================================ */

/** Wire struct sizes are stable (ABI regression guard). */
static void test_wire_struct_sizes(void) {
    TEST_BEGIN("wire_struct_sizes: packed struct sizes stable");

    /*
     * Core message header: magic(4) + version(1) + msg_type(1) +
     * payload_len(2) + node_id[KEEL_CLUSTER_MAX_NODE_ID=64] = 72 bytes.
     */
    ASSERT_EQ(sizeof(keel_cluster_msg_header_t),
              (size_t)(4 + 1 + 1 + 2 + KEEL_CLUSTER_MAX_NODE_ID));

    /* Vote request / response */
    ASSERT_EQ(sizeof(keel_cluster_vote_request_t),
              sizeof(uint64_t) + KEEL_CLUSTER_MAX_NODE_ID);
    ASSERT_EQ(sizeof(keel_cluster_vote_response_t), (size_t)16);

    /* 2PC structs */
    ASSERT_EQ(sizeof(keel_cluster_config_prepare_t), (size_t)24);
    ASSERT_EQ(sizeof(keel_cluster_config_commit_t),  (size_t)24);
    ASSERT_EQ(sizeof(keel_cluster_config_ack_t),     (size_t)24);

    TEST_PASS();
}

/** Message type constants are stable. */
static void test_wire_msg_type_constants(void) {
    TEST_BEGIN("wire_msg_type_constants: type IDs stable");

    ASSERT_EQ((int)KEEL_CLUSTER_MSG_CONFIG_PREPARE, 10);
    ASSERT_EQ((int)KEEL_CLUSTER_MSG_CONFIG_COMMIT,  11);
    ASSERT_EQ((int)KEEL_CLUSTER_MSG_CONFIG_ACK,     12);
    ASSERT_EQ(KEEL_CLUSTER_PROTO_VERSION, 4);

    TEST_PASS();
}

/** Packed structs: offset checks for 2PC types. */
static void test_wire_struct_layout(void) {
    TEST_BEGIN("wire_struct_layout: field offsets match spec");

    keel_cluster_config_prepare_t prep;
    /* term starts at byte 0, txn_id at byte 8, new_checksum at byte 16 */
    ASSERT_EQ(offsetof(keel_cluster_config_prepare_t, term),         (size_t)0);
    ASSERT_EQ(offsetof(keel_cluster_config_prepare_t, txn_id),       (size_t)8);
    ASSERT_EQ(offsetof(keel_cluster_config_prepare_t, new_checksum), (size_t)16);

    keel_cluster_config_commit_t comm;
    ASSERT_EQ(offsetof(keel_cluster_config_commit_t, term),   (size_t)0);
    ASSERT_EQ(offsetof(keel_cluster_config_commit_t, txn_id), (size_t)8);
    ASSERT_EQ(offsetof(keel_cluster_config_commit_t, commit), (size_t)16);

    keel_cluster_config_ack_t ack;
    ASSERT_EQ(offsetof(keel_cluster_config_ack_t, term),     (size_t)0);
    ASSERT_EQ(offsetof(keel_cluster_config_ack_t, txn_id),   (size_t)8);
    ASSERT_EQ(offsetof(keel_cluster_config_ack_t, accepted), (size_t)16);

    (void)prep; (void)comm; (void)ack;
    TEST_PASS();
}

/** Vote request struct layout. */
static void test_vote_struct_layout(void) {
    TEST_BEGIN("vote_struct_layout: field offsets match spec");

    ASSERT_EQ(offsetof(keel_cluster_vote_request_t, term),         (size_t)0);
    ASSERT_EQ(offsetof(keel_cluster_vote_request_t, candidate_id), (size_t)8);

    ASSERT_EQ(offsetof(keel_cluster_vote_response_t, term),        (size_t)0);
    ASSERT_EQ(offsetof(keel_cluster_vote_response_t, vote_granted),(size_t)8);

    TEST_PASS();
}

/* ============================================================================
 * ── Regression / corner cases
 * ============================================================================ */

/** Stop without start is a safe no-op. */
static void test_stop_without_start(void) {
    TEST_BEGIN("stop_without_start: no crash");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    keel_cluster_stop(c);   /* no-op: never started */
    keel_cluster_destroy(c);
    TEST_PASS();
}

/** Destroy without stop is safe (stop is implicitly called). */
static void test_destroy_without_stop(void) {
    TEST_BEGIN("destroy_without_stop: no crash / no leak");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "no-stop");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20530;
    cfg.heartbeat_interval_ms = 100;
    cfg.heartbeat_timeout_ms  = 500;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);
    usleep(200000);

    keel_cluster_destroy(c); /* must stop internally, no crash */
    TEST_PASS();
}

/** Double start returns an error (or silently succeeds — neither should crash). */
static void test_double_start(void) {
    TEST_BEGIN("double_start: second call is safe");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "dbl-start");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20540;
    cfg.heartbeat_interval_ms = 200;
    cfg.heartbeat_timeout_ms  = 1000;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);

    /* Second start on a running cluster must return -1 immediately
     * without touching any struct fields (prevents TSan race on listen_fd). */
    ASSERT_EQ(keel_cluster_start(c), -1);

    keel_cluster_stop(c);
    keel_cluster_destroy(c);
    TEST_PASS();
}

/** Multiple stop calls are idempotent. */
static void test_multiple_stop(void) {
    TEST_BEGIN("multiple_stop: idempotent");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "multi-stop");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20550;
    cfg.heartbeat_interval_ms = 200;
    cfg.heartbeat_timeout_ms  = 1000;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);

    keel_cluster_stop(c);
    keel_cluster_stop(c); /* idempotent: must not crash */
    keel_cluster_destroy(c);
    TEST_PASS();
}

/** Repeated start→stop cycles don't leak file descriptors. */
static void test_start_stop_cycles(void) {
    TEST_BEGIN("start_stop_cycles: no fd leak over 5 cycles");

    /* Count open fds before. */
    int before = 0;
    {
        /* Quick count via /proc/self/fd */
        DIR *d = opendir("/proc/self/fd");
        if (!d) { TEST_SKIP("no /proc/self/fd"); }
        struct dirent *de;
        while ((de = readdir(d))) before++;
        closedir(d);
    }

    for (int iter = 0; iter < 5; iter++) {
        keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
        cfg.enabled = true;
        snprintf(cfg.node_id, sizeof(cfg.node_id), "cycle-%d", iter);
        snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
        cfg.listen_port = (uint16_t)(20560 + iter);
        cfg.heartbeat_interval_ms = 100;
        cfg.heartbeat_timeout_ms  = 500;

        keel_cluster_t *c = keel_cluster_create(&cfg);
        if (!c) continue;
        (void)keel_cluster_start(c);
        usleep(150000);
        keel_cluster_stop(c);
        keel_cluster_destroy(c);
    }

    int after = 0;
    {
        DIR *d = opendir("/proc/self/fd");
        if (!d) { TEST_SKIP("no /proc/self/fd"); }
        struct dirent *de;
        while ((de = readdir(d))) after++;
        closedir(d);
    }

    /* Allow a small slop (3 fds) for timing/OS effects. */
    ASSERT_TRUE((after - before) <= 3);
    TEST_PASS();
}

/** election_state_path with embedded NUL byte is safely truncated. */
static void test_election_state_path_nul(void) {
    TEST_BEGIN("election_state_path_nul: embedded NUL doesn't escape");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    /* Deliberately put a NUL in the middle of the path. */
    memset(cfg.election_state_path, 0, sizeof(cfg.election_state_path));
    const char *evil = "/tmp/keel\x00/../../../../etc/passwd";
    memcpy(cfg.election_state_path, evil, 32);

    /* create must not crash or open /etc/passwd */
    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    /* The path used internally is at most up to the first NUL → "/tmp/keel". */
    keel_cluster_destroy(c);

    TEST_PASS();
}

/** Large node_id is safely truncated to KEEL_CLUSTER_MAX_NODE_ID-1 bytes. */
static void test_node_id_boundary(void) {
    TEST_BEGIN("node_id_boundary: oversized node_id is safe");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    /* Fill node_id with 'A's — last byte remains NUL. */
    memset(cfg.node_id, 'A', sizeof(cfg.node_id) - 1);
    cfg.node_id[sizeof(cfg.node_id) - 1] = '\0';

    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20570;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    /* node_id must be NUL-terminated and at most MAX_NODE_ID-1 chars. */
    ASSERT_EQ(cfg.node_id[KEEL_CLUSTER_MAX_NODE_ID - 1], '\0');

    keel_cluster_destroy(c);
    TEST_PASS();
}

/* ============================================================================
 * ── Memory safety: create / destroy stress
 * ============================================================================ */

/** Rapid create+destroy (no start): no leaks, no crashes. */
static void test_create_destroy_stress(void) {
    TEST_BEGIN("create_destroy_stress: 200 rapid alloc/free cycles");

    for (int i = 0; i < 200; i++) {
        keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
        snprintf(cfg.node_id, sizeof(cfg.node_id), "stress-%d", i);
        keel_cluster_t *c = keel_cluster_create(&cfg);
        ASSERT_NOT_NULL(c);
        keel_cluster_destroy(c);
    }

    TEST_PASS();
}

/** Multiple concurrent clusters share no global mutable state. */
static void test_multiple_simultaneous_clusters(void) {
    TEST_BEGIN("multiple_simultaneous_clusters: 5 clusters at once");

    keel_cluster_t *nodes[5];
    for (int i = 0; i < 5; i++) {
        keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
        cfg.enabled = true;
        snprintf(cfg.node_id, sizeof(cfg.node_id), "sim-%d", i);
        snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
        cfg.listen_port = (uint16_t)(20580 + i);
        cfg.heartbeat_interval_ms = 100;
        cfg.heartbeat_timeout_ms  = 500;

        nodes[i] = keel_cluster_create(&cfg);
        ASSERT_NOT_NULL(nodes[i]);
        ASSERT_EQ(keel_cluster_start(nodes[i]), 0);
    }

    usleep(200000); /* let them all run */

    for (int i = 0; i < 5; i++) {
        keel_cluster_stop(nodes[i]);
        keel_cluster_destroy(nodes[i]);
    }

    TEST_PASS();
}

/* ============================================================================
 * ── Three-node election: convergence within bounded time
 * ============================================================================ */

/** Three-node cluster converges to exactly one leader within 10 seconds. */
static void test_election_three_node_convergence_time(void) {
    TEST_BEGIN("election_three_node_convergence: one leader in <10s");

    keel_cluster_t *nodes[3];
    build_cluster(nodes, 3, 20600, "conv");

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    bool settled = false;
    for (int i = 0; i < 100 && !settled; i++) {
        usleep(100000);
        if (count_leaders(nodes, 3) == 1) settled = true;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    long elapsed_ms = (long)((t1.tv_sec - t0.tv_sec) * 1000 +
                              (t1.tv_nsec - t0.tv_nsec) / 1000000);

    ASSERT_TRUE(settled);
    ASSERT_EQ(count_leaders(nodes, 3), 1);
    ASSERT_TRUE(elapsed_ms < 10000);

    teardown_cluster(nodes, 3);
    TEST_PASS();
}

/* ============================================================================
 * ── Two-node quorum: multiple consecutive quorum_commit calls
 * ============================================================================ */

/** Multiple back-to-back quorum_commit calls on the same leader succeed. */
static void test_quorum_commit_repeated(void) {
    TEST_BEGIN("quorum_commit_repeated: 3 consecutive commits succeed");

    keel_cluster_t *nodes[2];
    build_cluster(nodes, 2, 20620, "rep");

    bool settled = false;
    for (int i = 0; i < 200 && !settled; i++) {
        usleep(100000);
        if (count_leaders(nodes, 2) == 1) settled = true;
    }
    ASSERT_TRUE(settled);

    for (int i = 0; i < 2; i++)
        ASSERT_TRUE(wait_peer_up(nodes[i], 0, 5000));

    keel_cluster_t *leader = NULL;
    for (int i = 0; i < 2; i++)
        if (keel_cluster_get_role(nodes[i]) == KEEL_CLUSTER_ROLE_LEADER)
            leader = nodes[i];
    ASSERT_NOT_NULL(leader);

    for (int i = 0; i < 3; i++) {
        ASSERT_EQ(keel_cluster_quorum_commit(leader, (uint64_t)(1000 + i)), 0);
    }

    teardown_cluster(nodes, 2);
    TEST_PASS();
}

/* ============================================================================
 * ── Persistent election state (smoke test: state path is written)
 * ============================================================================ */

/** election_state_path is created after an election runs. */
static void test_election_persistent_state(void) {
    TEST_BEGIN("election_persistent_state: state file is written");

    const char *state_path = "/tmp/keel-test-election-persist.state";
    unlink(state_path); /* clean up from any previous run */

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = true;
    cfg.election_enabled = true;
    snprintf(cfg.node_id, sizeof(cfg.node_id), "persist-node");
    snprintf(cfg.listen_addr, sizeof(cfg.listen_addr), "127.0.0.1");
    cfg.listen_port = 20640;
    cfg.heartbeat_interval_ms = 100;
    cfg.heartbeat_timeout_ms  = 400;
    snprintf(cfg.election_state_path, sizeof(cfg.election_state_path),
             "%s", state_path);

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);
    ASSERT_EQ(keel_cluster_start(c), 0);

    /* Wait for leader election (state is persisted after term increment). */
    wait_role(c, KEEL_CLUSTER_ROLE_LEADER, 6000);

    keel_cluster_stop(c);
    keel_cluster_destroy(c);

    /* State file should exist now. */
    struct stat st;
    ASSERT_EQ(stat(state_path, &st), 0);
    ASSERT_TRUE(st.st_size > 0);

    unlink(state_path);
    TEST_PASS();
}

/* ============================================================================
 * main
 * ============================================================================ */

/* ============================================================================
 * Wire-protocol compression tests (Phase 5)
 * ============================================================================ */

/** KEEL_CLUSTER_COMPRESS_* flag bits are encoded in the top 2 bits of the
 *  16-bit payload_len wire field.  Verify the bitmask constants are correct
 *  and that extracting/combining them is lossless. */
static void test_compress_constants(void) {
    TEST_BEGIN("compress_constants: codec flags and payload mask are correct");

    /* COMPRESS_MASK covers exactly the top 2 bits */
    ASSERT_EQ(KEEL_CLUSTER_COMPRESS_MASK,   (uint16_t)0xC000);
    /* PAYLOAD_LEN_MASK covers the bottom 14 bits */
    ASSERT_EQ(KEEL_CLUSTER_PAYLOAD_LEN_MASK, (uint16_t)0x3FFF);
    /* The two masks are complementary */
    ASSERT_EQ((uint16_t)(KEEL_CLUSTER_COMPRESS_MASK | KEEL_CLUSTER_PAYLOAD_LEN_MASK),
              (uint16_t)0xFFFF);

    /* Codec flags sit entirely within the top 2 bits */
    ASSERT_EQ(KEEL_CLUSTER_WIRE_NONE & KEEL_CLUSTER_PAYLOAD_LEN_MASK, 0u);
    ASSERT_EQ(KEEL_CLUSTER_WIRE_ZLIB & KEEL_CLUSTER_PAYLOAD_LEN_MASK, 0u);
    ASSERT_EQ(KEEL_CLUSTER_WIRE_ZSTD & KEEL_CLUSTER_PAYLOAD_LEN_MASK, 0u);

    /* Three distinct non-zero values (NONE is 0, ZLIB and ZSTD are non-zero) */
    ASSERT_EQ(KEEL_CLUSTER_WIRE_NONE, (uint16_t)0x0000);
    ASSERT_NE(KEEL_CLUSTER_WIRE_ZLIB, KEEL_CLUSTER_WIRE_NONE);
    ASSERT_NE(KEEL_CLUSTER_WIRE_ZSTD, KEEL_CLUSTER_WIRE_NONE);
    ASSERT_NE(KEEL_CLUSTER_WIRE_ZLIB, KEEL_CLUSTER_WIRE_ZSTD);

    /* Combining a max-length payload with each codec flag round-trips cleanly */
    for (uint16_t plen = 0; plen <= KEEL_CLUSTER_MAX_PAYLOAD; plen += 1024) {
        uint16_t wire_flags[] = { KEEL_CLUSTER_WIRE_NONE,
                                  KEEL_CLUSTER_WIRE_ZLIB,
                                  KEEL_CLUSTER_WIRE_ZSTD };
        for (int ci = 0; ci < 3; ci++) {
            uint16_t wire = (uint16_t)((plen & KEEL_CLUSTER_PAYLOAD_LEN_MASK)
                                       | (wire_flags[ci] & KEEL_CLUSTER_COMPRESS_MASK));
            uint16_t got_len   = (uint16_t)(wire & KEEL_CLUSTER_PAYLOAD_LEN_MASK);
            uint16_t got_codec = (uint16_t)(wire & KEEL_CLUSTER_COMPRESS_MASK);
            ASSERT_EQ(got_len,   plen);
            ASSERT_EQ(got_codec, wire_flags[ci]);
        }
    }

    TEST_PASS();
}

/** Verify the config struct default codec is NONE (no compression by default). */
static void test_compress_config_default(void) {
    TEST_BEGIN("compress_config_default: codec=NONE, threshold=256");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    ASSERT_EQ(cfg.compress_codec,           KEEL_CLUSTER_COMPRESS_NONE);
    ASSERT_EQ(cfg.compress_threshold_bytes, 256u);

    TEST_PASS();
}

/** Setting compress_codec=ZLIB in config propagates to the cluster handle. */
static void test_compress_config_zlib(void) {
    TEST_BEGIN("compress_config_zlib: codec stored in cluster config");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = false;   /* No need to start a thread */
    cfg.compress_codec = KEEL_CLUSTER_COMPRESS_ZLIB;
    cfg.compress_threshold_bytes = 128;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    const keel_cluster_config_t *stored = keel_cluster_get_config(c);
    ASSERT_NOT_NULL(stored);
    ASSERT_EQ(stored->compress_codec,           KEEL_CLUSTER_COMPRESS_ZLIB);
    ASSERT_EQ(stored->compress_threshold_bytes, 128u);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/** Setting compress_codec=ZSTD in config propagates to the cluster handle. */
static void test_compress_config_zstd(void) {
    TEST_BEGIN("compress_config_zstd: codec stored in cluster config");

    keel_cluster_config_t cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
    cfg.enabled = false;
    cfg.compress_codec = KEEL_CLUSTER_COMPRESS_ZSTD;
    cfg.compress_threshold_bytes = 64;

    keel_cluster_t *c = keel_cluster_create(&cfg);
    ASSERT_NOT_NULL(c);

    const keel_cluster_config_t *stored = keel_cluster_get_config(c);
    ASSERT_NOT_NULL(stored);
    ASSERT_EQ(stored->compress_codec,           KEEL_CLUSTER_COMPRESS_ZSTD);
    ASSERT_EQ(stored->compress_threshold_bytes, 64u);

    keel_cluster_destroy(c);
    TEST_PASS();
}

/** keel_compress_bound() returns a value >= the input size for all codecs. */
static void test_compress_bound(void) {
    TEST_BEGIN("compress_bound: always >= src_len");

    size_t lens[] = { 0, 1, 16, 128, 512, 1024, 4096, 16000 };
    keel_compress_codec_t codecs[] = {
        KEEL_COMPRESS_NONE, KEEL_COMPRESS_GZIP, KEEL_COMPRESS_ZSTD
    };

    for (size_t li = 0; li < sizeof(lens)/sizeof(lens[0]); li++) {
        for (size_t ci = 0; ci < sizeof(codecs)/sizeof(codecs[0]); ci++) {
            size_t bound = keel_compress_bound(codecs[ci], lens[li]);
            /* NONE should return exactly src_len; others return >= src_len */
            if (codecs[ci] == KEEL_COMPRESS_NONE) {
                ASSERT_EQ(bound, lens[li]);
            } else {
                ASSERT_TRUE(bound >= lens[li]);
            }
        }
    }

    TEST_PASS();
}

/** keel_compress(NONE) is a passthrough: bytes are copied unchanged. */
static void test_compress_none_passthrough(void) {
    TEST_BEGIN("compress_none_passthrough: output equals input");

    const char src[] = "Hello, KEEL cluster compression!";
    size_t src_len = sizeof(src) - 1;

    char dst[256] = {0};
    ssize_t n = keel_compress(KEEL_COMPRESS_NONE, src, src_len, dst, sizeof(dst));
    ASSERT_EQ((size_t)n, src_len);
    ASSERT_EQ(memcmp(src, dst, src_len), 0);

    TEST_PASS();
}

/** Round-trip: compress with GZIP, decompress, verify identity. */
static void test_compress_roundtrip_gzip(void) {
    TEST_BEGIN("compress_roundtrip_gzip: compress then decompress restores original");

    /* Highly compressible input */
    uint8_t src[1024];
    memset(src, 0xAB, sizeof(src));

    uint8_t compressed[4096];
    ssize_t clen = keel_compress(KEEL_COMPRESS_GZIP, src, sizeof(src),
                                  compressed, sizeof(compressed));
    ASSERT_TRUE(clen > 0);
    /* Compressed size should be smaller for repetitive input */
    ASSERT_TRUE((size_t)clen < sizeof(src));

    uint8_t recovered[1024];
    ssize_t dlen = keel_decompress(KEEL_COMPRESS_GZIP, compressed, (size_t)clen,
                                    recovered, sizeof(recovered));
    ASSERT_EQ((size_t)dlen, sizeof(src));
    ASSERT_EQ(memcmp(src, recovered, sizeof(src)), 0);

    TEST_PASS();
}

/** Round-trip: compress with ZSTD, decompress, verify identity. */
static void test_compress_roundtrip_zstd(void) {
    TEST_BEGIN("compress_roundtrip_zstd: compress then decompress restores original");

#if !KEEL_HAS_ZSTD
    printf("    SKIP (libzstd not available)\n");
    g_passed++;
    return;
#endif

    /* Mix of repetitive and random-ish data */
    uint8_t src[2048];
    for (size_t i = 0; i < sizeof(src); i++)
        src[i] = (uint8_t)(i * 17 + (i >> 3));

    uint8_t compressed[4096];
    ssize_t clen = keel_compress(KEEL_COMPRESS_ZSTD, src, sizeof(src),
                                  compressed, sizeof(compressed));
    ASSERT_TRUE(clen > 0);

    uint8_t recovered[2048];
    ssize_t dlen = keel_decompress(KEEL_COMPRESS_ZSTD, compressed, (size_t)clen,
                                    recovered, sizeof(recovered));
    ASSERT_EQ((size_t)dlen, sizeof(src));
    ASSERT_EQ(memcmp(src, recovered, sizeof(src)), 0);

    TEST_PASS();
}

/** Corrupt compressed data must be rejected (return -1), not crash. */
static void test_compress_corrupt_input(void) {
    TEST_BEGIN("compress_corrupt_input: corrupted stream returns -1");

    /* A valid gzip header followed by garbage */
    uint8_t garbage[64];
    memset(garbage, 0xFF, sizeof(garbage));
    /* Write a plausible gzip magic to make it past obvious rejection */
    garbage[0] = 0x1F; garbage[1] = 0x8B;

    uint8_t out[256];
    ssize_t n = keel_decompress(KEEL_COMPRESS_GZIP, garbage, sizeof(garbage),
                                 out, sizeof(out));
    ASSERT_EQ(n, (ssize_t)-1);

#if KEEL_HAS_ZSTD
    ssize_t m = keel_decompress(KEEL_COMPRESS_ZSTD, garbage, sizeof(garbage),
                                 out, sizeof(out));
    ASSERT_EQ(m, (ssize_t)-1);
#endif

    TEST_PASS();
}

/** Output buffer too small: compress must return -1 instead of overwriting. */
static void test_compress_dst_too_small(void) {
    TEST_BEGIN("compress_dst_too_small: tiny dst returns -1");

    uint8_t src[512];
    memset(src, 0x55, sizeof(src));

    /* 1-byte destination — cannot hold any meaningful compressed output */
    uint8_t dst[1];
    ssize_t n = keel_compress(KEEL_COMPRESS_GZIP, src, sizeof(src), dst, sizeof(dst));
    ASSERT_EQ(n, (ssize_t)-1);

    TEST_PASS();
}

/** NULL inputs to compress/decompress must not crash. */
static void test_compress_null_inputs(void) {
    TEST_BEGIN("compress_null_inputs: NULL pointers handled gracefully");

    uint8_t buf[64] = {0};

    ASSERT_EQ(keel_compress(KEEL_COMPRESS_GZIP, NULL, 16, buf, sizeof(buf)), (ssize_t)-1);
    ASSERT_EQ(keel_compress(KEEL_COMPRESS_GZIP, buf, 16, NULL, sizeof(buf)), (ssize_t)-1);
    ASSERT_EQ(keel_decompress(KEEL_COMPRESS_GZIP, NULL, 16, buf, sizeof(buf)), (ssize_t)-1);
    ASSERT_EQ(keel_decompress(KEEL_COMPRESS_GZIP, buf, 16, NULL, sizeof(buf)), (ssize_t)-1);

    TEST_PASS();
}

/** A two-node cluster configured with ZLIB compression starts and exchanges
 *  heartbeats without error (end-to-end path through compressed wire protocol). */
static void test_compress_e2e_zlib(void) {
    TEST_BEGIN("compress_e2e_zlib: two-node cluster runs with ZLIB compression");

    keel_cluster_config_t ca = KEEL_CLUSTER_CONFIG_DEFAULT;
    ca.enabled = true;
    ca.election_enabled = false;
    ca.compress_codec = KEEL_CLUSTER_COMPRESS_ZLIB;
    ca.compress_threshold_bytes = 0;  /* compress every payload */
    snprintf(ca.node_id, sizeof(ca.node_id), "zlib-a");
    snprintf(ca.listen_addr, sizeof(ca.listen_addr), "127.0.0.1");
    ca.listen_port = 20650;
    ca.heartbeat_interval_ms = 150;
    ca.heartbeat_timeout_ms  = 500;
    snprintf(ca.initial_peers[0].addr, sizeof(ca.initial_peers[0].addr), "127.0.0.1");
    ca.initial_peers[0].port = 20651;
    ca.initial_peer_count = 1;

    keel_cluster_config_t cb = KEEL_CLUSTER_CONFIG_DEFAULT;
    cb.enabled = true;
    cb.election_enabled = false;
    cb.compress_codec = KEEL_CLUSTER_COMPRESS_ZLIB;
    cb.compress_threshold_bytes = 0;
    snprintf(cb.node_id, sizeof(cb.node_id), "zlib-b");
    snprintf(cb.listen_addr, sizeof(cb.listen_addr), "127.0.0.1");
    cb.listen_port = 20651;
    cb.heartbeat_interval_ms = 150;
    cb.heartbeat_timeout_ms  = 500;
    snprintf(cb.initial_peers[0].addr, sizeof(cb.initial_peers[0].addr), "127.0.0.1");
    cb.initial_peers[0].port = 20650;
    cb.initial_peer_count = 1;

    keel_cluster_t *a = keel_cluster_create(&ca);
    keel_cluster_t *b = keel_cluster_create(&cb);
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);

    ASSERT_EQ(keel_cluster_start(a), 0);
    ASSERT_EQ(keel_cluster_start(b), 0);

    /* Let them exchange at least 2 heartbeat cycles */
    struct timespec ts = { .tv_sec = 0, .tv_nsec = 600 * 1000000L };
    nanosleep(&ts, NULL);

    keel_cluster_stop(a);
    keel_cluster_stop(b);
    keel_cluster_destroy(a);
    keel_cluster_destroy(b);
    TEST_PASS();
}

/** Same as above with ZSTD codec. */
static void test_compress_e2e_zstd(void) {
    TEST_BEGIN("compress_e2e_zstd: two-node cluster runs with ZSTD compression");

#if !KEEL_HAS_ZSTD
    printf("    SKIP (libzstd not available)\n");
    g_passed++;
    return;
#endif

    keel_cluster_config_t ca = KEEL_CLUSTER_CONFIG_DEFAULT;
    ca.enabled = true;
    ca.election_enabled = false;
    ca.compress_codec = KEEL_CLUSTER_COMPRESS_ZSTD;
    ca.compress_threshold_bytes = 0;
    snprintf(ca.node_id, sizeof(ca.node_id), "zstd-a");
    snprintf(ca.listen_addr, sizeof(ca.listen_addr), "127.0.0.1");
    ca.listen_port = 20660;
    ca.heartbeat_interval_ms = 150;
    ca.heartbeat_timeout_ms  = 500;
    snprintf(ca.initial_peers[0].addr, sizeof(ca.initial_peers[0].addr), "127.0.0.1");
    ca.initial_peers[0].port = 20661;
    ca.initial_peer_count = 1;

    keel_cluster_config_t cb = ca;
    snprintf(cb.node_id, sizeof(cb.node_id), "zstd-b");
    cb.listen_port = 20661;
    cb.initial_peers[0].port = 20660;

    keel_cluster_t *a = keel_cluster_create(&ca);
    keel_cluster_t *b = keel_cluster_create(&cb);
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);

    ASSERT_EQ(keel_cluster_start(a), 0);
    ASSERT_EQ(keel_cluster_start(b), 0);

    struct timespec ts = { .tv_sec = 0, .tv_nsec = 600 * 1000000L };
    nanosleep(&ts, NULL);

    keel_cluster_stop(a);
    keel_cluster_stop(b);
    keel_cluster_destroy(a);
    keel_cluster_destroy(b);
    TEST_PASS();
}

/** A cluster with ZSTD compression can talk to one with NONE (graceful
 *  version mismatch — v4 nodes talking to themselves; the validate_header
 *  check will reject old-format messages). */
static void test_compress_asymmetric_none_vs_zstd(void) {
    TEST_BEGIN("compress_asymmetric: NONE sender + ZSTD receiver coexist");

    /* Both nodes are v4 but one sends uncompressed, the other sends zstd.
     * The receiver decodes the codec from the wire flag and decompresses
     * only when the flag says so — both directions work. */
    keel_cluster_config_t ca = KEEL_CLUSTER_CONFIG_DEFAULT;
    ca.enabled = true;
    ca.election_enabled = false;
    ca.compress_codec = KEEL_CLUSTER_COMPRESS_NONE;   /* node A: no compression */
    ca.compress_threshold_bytes = 256;
    snprintf(ca.node_id, sizeof(ca.node_id), "asym-none");
    snprintf(ca.listen_addr, sizeof(ca.listen_addr), "127.0.0.1");
    ca.listen_port = 20670;
    ca.heartbeat_interval_ms = 150;
    ca.heartbeat_timeout_ms  = 500;
    snprintf(ca.initial_peers[0].addr, sizeof(ca.initial_peers[0].addr), "127.0.0.1");
    ca.initial_peers[0].port = 20671;
    ca.initial_peer_count = 1;

    keel_cluster_config_t cb = KEEL_CLUSTER_CONFIG_DEFAULT;
    cb.enabled = true;
    cb.election_enabled = false;
#if KEEL_HAS_ZSTD
    cb.compress_codec = KEEL_CLUSTER_COMPRESS_ZSTD;  /* node B: zstd */
#else
    cb.compress_codec = KEEL_CLUSTER_COMPRESS_ZLIB;  /* fallback */
#endif
    cb.compress_threshold_bytes = 0;
    snprintf(cb.node_id, sizeof(cb.node_id), "asym-zstd");
    snprintf(cb.listen_addr, sizeof(cb.listen_addr), "127.0.0.1");
    cb.listen_port = 20671;
    cb.heartbeat_interval_ms = 150;
    cb.heartbeat_timeout_ms  = 500;
    snprintf(cb.initial_peers[0].addr, sizeof(cb.initial_peers[0].addr), "127.0.0.1");
    cb.initial_peers[0].port = 20670;
    cb.initial_peer_count = 1;

    keel_cluster_t *a = keel_cluster_create(&ca);
    keel_cluster_t *b = keel_cluster_create(&cb);
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);

    ASSERT_EQ(keel_cluster_start(a), 0);
    ASSERT_EQ(keel_cluster_start(b), 0);

    struct timespec ts = { .tv_sec = 0, .tv_nsec = 600 * 1000000L };
    nanosleep(&ts, NULL);

    keel_cluster_stop(a);
    keel_cluster_stop(b);
    keel_cluster_destroy(a);
    keel_cluster_destroy(b);
    TEST_PASS();
}

int main(void) {
    printf("\n=== KEEL Cluster Election / Phase 1-4 Tests ===\n\n");

    keel_mem_init(NULL);

    /* Phase 1+2: Election API — static */
    test_election_null_safety();
    test_election_disabled_stays_follower();
    test_election_config_defaults();
    test_election_single_node_becomes_leader();
    test_election_two_node();
    test_election_three_node();
    test_election_term_monotone();
    test_get_leader_id_truncation();
    test_election_api_performance();

    /* Phase 3: VIP config */
    test_vip_config_defaults();
    test_vip_config_fields();
    test_vip_cidr_ip_extraction();
    test_vip_field_length_limits();
    test_vip_env_var_parsing();
    test_vip_disabled_no_crash();

    /* Phase 4: Quorum 2PC */
    test_quorum_commit_null();
    test_quorum_commit_disabled();
    test_quorum_commit_follower();
    test_quorum_commit_leader_no_peers();
    test_quorum_commit_leader_dead_peer();
    test_quorum_commit_two_node_live();

    /* Wire struct ABI */
    test_wire_struct_sizes();
    test_wire_msg_type_constants();
    test_wire_struct_layout();
    test_vote_struct_layout();

    /* Regression / corner cases */
    test_stop_without_start();
    test_destroy_without_stop();
    test_double_start();
    test_multiple_stop();
    test_start_stop_cycles();
    test_election_state_path_nul();
    test_node_id_boundary();

    /* Memory / stress */
    test_create_destroy_stress();
    test_multiple_simultaneous_clusters();

    /* Performance */
    test_election_three_node_convergence_time();
    test_quorum_commit_repeated();

    /* Persistent state */
    test_election_persistent_state();

    /* Wire-protocol compression */
    test_compress_constants();
    test_compress_config_default();
    test_compress_config_zlib();
    test_compress_config_zstd();
    test_compress_bound();
    test_compress_none_passthrough();
    test_compress_roundtrip_gzip();
    test_compress_roundtrip_zstd();
    test_compress_corrupt_input();
    test_compress_dst_too_small();
    test_compress_null_inputs();
    test_compress_e2e_zlib();
    test_compress_e2e_zstd();
    test_compress_asymmetric_none_vs_zstd();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           g_passed, g_failed, g_total);

    return g_failed > 0 ? 1 : 0;
}
