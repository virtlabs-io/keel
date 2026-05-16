/**
 * @file numa.h
 * @brief NUMA topology discovery and NUMA-aware allocation API.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * KEEL treats NUMA support as an optimization layer, not a hard dependency. The
 * subsystem discovers topology when possible, exposes cheap mapping helpers, and
 * offers allocation/migration primitives that degrade cleanly to ordinary memory
 * allocation on unsupported platforms. That keeps the public API stable across
 * Linux and non-Linux builds while still letting worker threads prefer local-node
 * memory on multi-socket systems.
 */

#ifndef KEEL_NUMA_H
#define KEEL_NUMA_H

#include "keel_types.h"
#include "keel_error.h"

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * NUMA Topology
 * ============================================================================ */

/**
 * @brief NUMA topology information.
 */
typedef struct keel_numa_info {
    int      num_nodes;          /**< Total NUMA nodes (0 = no NUMA) */
    int      num_cpus;           /**< Total online CPUs */
    int*     cpu_to_node;        /**< cpu_to_node[cpu_id] = node_id */
    uint64_t* node_mem_total;    /**< Total memory per node (bytes) */
    uint64_t* node_mem_free;     /**< Free memory per node (bytes, snapshot) */
} keel_numa_info_t;

/**
 * @brief Initialize NUMA subsystem.
 *
 * Detects NUMA topology from /sys/devices/system/node/.
 * Safe to call on non-NUMA systems (sets num_nodes=1).
 *
 * @return KEEL_OK on success
 */
keel_error_t keel_numa_init(void);

/**
 * @brief Shutdown NUMA subsystem and free topology data.
 */
void keel_numa_shutdown(void);

/**
 * @brief Get NUMA topology info.
 *
 * @return Pointer to topology info (valid until keel_numa_shutdown)
 */
const keel_numa_info_t* keel_numa_topology(void);

/**
 * @brief Check if the system has NUMA topology.
 */
bool keel_numa_available(void);

/**
 * @brief Get the NUMA node for a given CPU.
 *
 * @param cpu  CPU id (from sched_getcpu() or similar)
 * @return NUMA node id, or 0 if non-NUMA
 */
int keel_numa_node_of_cpu(int cpu);

/**
 * @brief Get the NUMA node for the current thread.
 *
 * Uses sched_getcpu() to determine the calling thread's CPU,
 * then maps to NUMA node.
 *
 * @return NUMA node id, or 0 if non-NUMA
 */
int keel_numa_current_node(void);

/* ============================================================================
 * NUMA-Aware Allocation
 * ============================================================================ */

/**
 * @brief Allocate memory on a specific NUMA node.
 *
 * Uses mbind(2) to bind pages to the requested node.
 * Falls back to regular mmap if NUMA is not available.
 *
 * @param node  Target NUMA node (-1 = local node)
 * @param size  Allocation size (rounded up to page size)
 * @return Pointer to allocated memory, or NULL on failure
 */
KEEL_NODISCARD
void* keel_numa_alloc(int node, size_t size);

/**
 * @brief Allocate memory on the calling thread's local NUMA node.
 *
 * Equivalent to keel_numa_alloc(keel_numa_current_node(), size).
 *
 * @param size  Allocation size
 * @return Pointer to allocated memory, or NULL on failure
 */
KEEL_NODISCARD
void* keel_numa_alloc_local(size_t size);

/**
 * @brief Allocate zeroed memory on a specific NUMA node.
 *
 * @param node  Target NUMA node (-1 = local node)
 * @param size  Allocation size
 * @return Pointer to zeroed memory, or NULL on failure
 */
KEEL_NODISCARD
void* keel_numa_calloc(int node, size_t size);

/**
 * @brief Allocate interleaved memory across all nodes.
 *
 * Useful for shared data structures accessed by all workers.
 *
 * @param size  Allocation size
 * @return Pointer to allocated memory, or NULL on failure
 */
KEEL_NODISCARD
void* keel_numa_alloc_interleaved(size_t size);

/**
 * @brief Free NUMA-allocated memory.
 *
 * @param ptr   Pointer from keel_numa_alloc*
 * @param size  Original allocation size
 */
void keel_numa_free(void* ptr, size_t size);

/**
 * @brief Move existing pages to a target NUMA node.
 *
 * @param ptr   Start address (page-aligned)
 * @param size  Region size
 * @param node  Target NUMA node
 * @return KEEL_OK on success
 */
keel_error_t keel_numa_migrate(void* ptr, size_t size, int node);

/* ============================================================================
 * NUMA-Aware Arena
 * ============================================================================ */

/**
 * @brief Create an arena that allocates from a specific NUMA node.
 *
 * @param node          Target NUMA node (-1 = local)
 * @param initial_size  Initial arena size (0 = default)
 * @return Arena handle, or NULL on failure
 */
KEEL_NODISCARD
struct keel_arena* keel_numa_arena_create(int node, size_t initial_size);

/* ============================================================================
 * NUMA Statistics
 * ============================================================================ */

typedef struct keel_numa_stats {
    uint64_t alloc_count;       /**< Number of NUMA allocations */
    uint64_t alloc_bytes;       /**< Total bytes allocated */
    uint64_t free_count;        /**< Number of frees */
    uint64_t free_bytes;        /**< Total bytes freed */
    uint64_t migrate_count;     /**< Number of migrations */
    uint64_t fallback_count;    /**< Fallback to non-NUMA alloc */
} keel_numa_stats_t;

/**
 * @brief Snapshot NUMA allocation statistics.
 *
 * @param node NUMA node to inspect, or `-1` for an aggregate view.
 * @return Statistics snapshot for the requested node scope.
 */
keel_numa_stats_t keel_numa_get_stats(int node);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_NUMA_H */
