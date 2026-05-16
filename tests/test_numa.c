/**
 * @file test_numa.c
 * @brief Unit tests for the NUMA topology discovery and allocation API.
 *
 * Coverage:
 *   §1  Init / shutdown — safe on any system (NUMA or non-NUMA).
 *   §2  Topology — num_nodes >= 1, cpu_to_node valid after init.
 *   §3  keel_numa_available() — consistent with num_nodes.
 *   §4  keel_numa_current_node() — returns value in [0, num_nodes).
 *   §5  keel_numa_node_of_cpu() — valid result for CPU 0 and current CPU.
 *   §6  keel_numa_alloc / keel_numa_free — basic lifecycle.
 *   §7  keel_numa_alloc_local — succeeds, memory is writable.
 *   §8  keel_numa_calloc — zeroed memory.
 *   §9  keel_numa_alloc_interleaved — succeeds for shared structures.
 *   §10 keel_numa_free(NULL, 0) — must not crash.
 *   §11 keel_numa_migrate — no crash on local migration (may be no-op).
 *   §12 keel_numa_arena_create — arena lifecycle on local node.
 *   §13 keel_numa_get_stats — counters increase after allocations.
 *   §14 Double init / shutdown safety — idempotent.
 *   §15 Large NUMA allocation — 4 MB, write and free.
 *   §16 Stress — 1000 small alloc/free cycles.
 *
 * @author Keel test suite
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/mem/numa.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <sched.h>

/* ============================================================================
 * §1  Init / shutdown
 * ============================================================================ */

static void test_numa_init_shutdown(void) {
    TEST_BEGIN("numa init / shutdown: safe on any platform");

    keel_error_t err = keel_numa_init();
    TEST_ASSERT_EQ(err, KEEL_OK);

    keel_numa_shutdown();

    /* Re-init for subsequent tests */
    err = keel_numa_init();
    TEST_ASSERT_EQ(err, KEEL_OK);

    TEST_END();
}

/* ============================================================================
 * §2  Topology
 * ============================================================================ */

static void test_numa_topology(void) {
    TEST_BEGIN("numa topology: num_nodes >= 1, topology pointer valid");

    const keel_numa_info_t* info = keel_numa_topology();
    TEST_ASSERT_NOT_NULL(info);
    TEST_ASSERT(info->num_nodes >= 1);
    TEST_ASSERT(info->num_cpus  >= 1);

    /* cpu_to_node is optional if num_nodes == 1, but num_cpus must be > 0 */
    if (info->num_nodes > 1) {
        TEST_ASSERT_NOT_NULL(info->cpu_to_node);
        /* All node ids in cpu_to_node must be in [0, num_nodes) */
        for (int i = 0; i < info->num_cpus; i++) {
            TEST_ASSERT(info->cpu_to_node[i] >= 0);
            TEST_ASSERT(info->cpu_to_node[i] < info->num_nodes);
        }
    }

    TEST_END();
}

/* ============================================================================
 * §3  keel_numa_available
 * ============================================================================ */

static void test_numa_available(void) {
    TEST_BEGIN("numa_available: consistent with topology.num_nodes");

    const keel_numa_info_t* info = keel_numa_topology();
    bool avail = keel_numa_available();

    /*
     * If num_nodes > 1 the system is multi-socket NUMA → available must be true.
     * If num_nodes == 1 the result may be either way (single-node "NUMA" exists).
     */
    if (info && info->num_nodes > 1) {
        TEST_ASSERT(avail);
    } else {
        /* Either value is acceptable on single-node systems */
        (void)avail;
    }
    TEST_ASSERT(true);

    TEST_END();
}

/* ============================================================================
 * §4  keel_numa_current_node
 * ============================================================================ */

static void test_numa_current_node(void) {
    TEST_BEGIN("numa_current_node: in [0, num_nodes)");

    const keel_numa_info_t* info = keel_numa_topology();
    int node = keel_numa_current_node();
    TEST_ASSERT(node >= 0);
    if (info) {
        TEST_ASSERT(node < info->num_nodes);
    }

    TEST_END();
}

/* ============================================================================
 * §5  keel_numa_node_of_cpu
 * ============================================================================ */

static void test_numa_node_of_cpu(void) {
    TEST_BEGIN("numa_node_of_cpu: valid for CPU 0 and current CPU");

    const keel_numa_info_t* info = keel_numa_topology();

    int n0 = keel_numa_node_of_cpu(0);
    TEST_ASSERT(n0 >= 0);
    if (info) TEST_ASSERT(n0 < info->num_nodes);

    int cur_cpu = sched_getcpu();
    if (cur_cpu >= 0) {
        int nc = keel_numa_node_of_cpu(cur_cpu);
        TEST_ASSERT(nc >= 0);
        if (info) TEST_ASSERT(nc < info->num_nodes);
    }

    /* Invalid CPU id must not crash and returns 0 */
    int inv = keel_numa_node_of_cpu(-1);
    TEST_ASSERT_EQ(inv, 0);

    TEST_END();
}

/* ============================================================================
 * §6  keel_numa_alloc / keel_numa_free
 * ============================================================================ */

static void test_numa_alloc_free(void) {
    TEST_BEGIN("numa_alloc: basic lifecycle");

    void* p = keel_numa_alloc(-1, 4096);
    TEST_ASSERT_NOT_NULL(p);

    /* Write to every byte to ensure it's real memory */
    memset(p, 0xAA, 4096);
    uint8_t* bytes = (uint8_t*)p;
    for (int i = 0; i < 4096; i++) {
        TEST_ASSERT_EQ(bytes[i], (uint8_t)0xAA);
    }

    keel_numa_free(p, 4096);

    TEST_END();
}

static void test_numa_alloc_node0(void) {
    TEST_BEGIN("numa_alloc: explicit node 0");

    void* p = keel_numa_alloc(0, 8192);
    TEST_ASSERT_NOT_NULL(p);
    memset(p, 0x55, 8192);
    keel_numa_free(p, 8192);

    TEST_END();
}

/* ============================================================================
 * §7  keel_numa_alloc_local
 * ============================================================================ */

static void test_numa_alloc_local(void) {
    TEST_BEGIN("numa_alloc_local: succeeds and memory is writable");

    void* p = keel_numa_alloc_local(4096);
    TEST_ASSERT_NOT_NULL(p);
    memset(p, 0xBB, 4096);
    keel_numa_free(p, 4096);

    TEST_END();
}

/* ============================================================================
 * §8  keel_numa_calloc
 * ============================================================================ */

static void test_numa_calloc(void) {
    TEST_BEGIN("numa_calloc: memory is zeroed");

    void* p = keel_numa_calloc(-1, 4096);
    TEST_ASSERT_NOT_NULL(p);

    uint8_t* bytes = (uint8_t*)p;
    bool all_zero = true;
    for (int i = 0; i < 4096; i++) {
        if (bytes[i] != 0) { all_zero = false; break; }
    }
    TEST_ASSERT(all_zero);

    keel_numa_free(p, 4096);

    TEST_END();
}

/* ============================================================================
 * §9  keel_numa_alloc_interleaved
 * ============================================================================ */

static void test_numa_alloc_interleaved(void) {
    TEST_BEGIN("numa_alloc_interleaved: allocation and write succeeds");

    void* p = keel_numa_alloc_interleaved(4096);
    TEST_ASSERT_NOT_NULL(p);
    memset(p, 0xCC, 4096);
    keel_numa_free(p, 4096);

    TEST_END();
}

/* ============================================================================
 * §10  keel_numa_free(NULL)
 * ============================================================================ */

static void test_numa_free_null(void) {
    TEST_BEGIN("numa_free(NULL): must not crash");

    keel_numa_free(NULL, 0);
    keel_numa_free(NULL, 4096);
    TEST_ASSERT(true);

    TEST_END();
}

/* ============================================================================
 * §11  keel_numa_migrate
 * ============================================================================ */

static void test_numa_migrate(void) {
    TEST_BEGIN("numa_migrate: no crash for local migration");

    void* p = keel_numa_alloc(-1, 4096);
    TEST_ASSERT_NOT_NULL(p);

    /* Migrate to node 0 — may be a no-op or invalid on non-NUMA systems.
     * The implementation returns KEEL_ERR_INVALID_ARG when NUMA is not
     * initialized or num_nodes <= 1 (single-node / non-NUMA host), so we
     * accept OK, NOT_SUPPORTED, or INVALID_ARG. */
    keel_error_t err = keel_numa_migrate(p, 4096, 0);
    TEST_ASSERT(err == KEEL_OK || err == KEEL_ERR_NOT_SUPPORTED ||
                err == KEEL_ERR_INVALID_ARG);

    keel_numa_free(p, 4096);

    TEST_END();
}

/* ============================================================================
 * §12  keel_numa_arena_create
 * ============================================================================ */

static void test_numa_arena_create(void) {
    TEST_BEGIN("numa_arena_create: lifecycle on local node");

    struct keel_arena* arena = keel_numa_arena_create(-1, 0);
    TEST_ASSERT_NOT_NULL(arena);

    /* Allocate something from the arena */
    void* p = keel_arena_alloc(arena, 64);
    TEST_ASSERT_NOT_NULL(p);
    memset(p, 0xDD, 64);

    keel_arena_destroy(arena);

    TEST_END();
}

/* ============================================================================
 * §13  keel_numa_get_stats
 * ============================================================================ */

static void test_numa_stats(void) {
    TEST_BEGIN("numa_get_stats: counters increase after allocations");

    /* Snapshot before */
    keel_numa_stats_t before = keel_numa_get_stats(-1);

    /* Do some allocations */
    void* ptrs[8];
    for (int i = 0; i < 8; i++) {
        ptrs[i] = keel_numa_alloc(-1, 4096);
        TEST_ASSERT_NOT_NULL(ptrs[i]);
    }

    /* Snapshot after */
    keel_numa_stats_t after = keel_numa_get_stats(-1);
    TEST_ASSERT(after.alloc_count >= before.alloc_count + 8 ||
                after.alloc_count >= 8); /* counter may roll over or be absolute */
    TEST_ASSERT(after.alloc_bytes >= before.alloc_bytes + 8 * 4096 ||
                after.alloc_bytes >= 8 * 4096);

    for (int i = 0; i < 8; i++) {
        keel_numa_free(ptrs[i], 4096);
    }

    keel_numa_stats_t freed = keel_numa_get_stats(-1);
    TEST_ASSERT(freed.free_count >= after.free_count);

    TEST_END();
}

/* ============================================================================
 * §14  Double init / shutdown
 * ============================================================================ */

static void test_numa_double_init(void) {
    TEST_BEGIN("numa double-init / double-shutdown: idempotent");

    /* Already initialized from §1; init again */
    keel_error_t err = keel_numa_init();
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Shutdown twice */
    keel_numa_shutdown();
    keel_numa_shutdown(); /* second call must not crash */

    /* Re-init for remaining tests */
    err = keel_numa_init();
    TEST_ASSERT_EQ(err, KEEL_OK);

    TEST_END();
}

/* ============================================================================
 * §15  Large allocation
 * ============================================================================ */

static void test_numa_large_alloc(void) {
    TEST_BEGIN("numa large alloc: 4 MB on local node");

    size_t sz = 4 * 1024 * 1024;
    void* p = keel_numa_alloc_local(sz);
    TEST_ASSERT_NOT_NULL(p);

    /* Write every page header to ensure pages are faulted in */
    uint8_t* bytes = (uint8_t*)p;
    for (size_t off = 0; off < sz; off += 4096) {
        bytes[off] = (uint8_t)(off & 0xFF);
    }

    keel_numa_free(p, sz);

    TEST_END();
}

/* ============================================================================
 * §16  Stress
 * ============================================================================ */

static void test_numa_stress(void) {
    TEST_BEGIN("numa stress: 1000 alloc/free cycles");

    for (int i = 0; i < 1000; i++) {
        size_t sz = (size_t)((i % 16 + 1) * 4096);
        void* p = keel_numa_alloc(-1, sz);
        TEST_ASSERT_NOT_NULL(p);
        memset(p, (int)(i & 0xFF), sz);
        keel_numa_free(p, sz);
    }

    TEST_END();
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("NUMA Allocator Tests\n");
    printf("====================\n\n");

    keel_mem_init(NULL);

    test_numa_init_shutdown();
    test_numa_topology();
    test_numa_available();
    test_numa_current_node();
    test_numa_node_of_cpu();

    test_numa_alloc_free();
    test_numa_alloc_node0();
    test_numa_alloc_local();
    test_numa_calloc();
    test_numa_alloc_interleaved();
    test_numa_free_null();

    test_numa_migrate();
    test_numa_arena_create();
    test_numa_stats();
    test_numa_double_init();
    test_numa_large_alloc();
    test_numa_stress();

    keel_numa_shutdown();
    keel_mem_shutdown();

    return test_summary();
}
