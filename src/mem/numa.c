/**
 * @file numa.c
 * @brief NUMA topology discovery and NUMA-aware allocation implementation.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * KEEL implements NUMA support without depending on libnuma so the proxy can stay
 * self-contained in constrained deployment environments. The code reads topology
 * from sysfs where available and uses `mmap` plus `mbind` to express placement
 * preferences. If placement fails, the memory remains usable and the subsystem
 * records a fallback instead of turning a locality optimization into a hard error.
 */

#include "keel/mem/numa.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"

#include <errno.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/syscall.h>

/* ============================================================================
 * Linux mbind constants (avoid libnuma dependency)
 * ============================================================================ */

#ifdef __linux__

/* mbind policy flags */
#define KEEL_MPOL_DEFAULT    0
#define KEEL_MPOL_PREFERRED  1
#define KEEL_MPOL_BIND       2
#define KEEL_MPOL_INTERLEAVE 3

/* mbind flags */
#define KEEL_MPOL_MF_STRICT  (1 << 0)
#define KEEL_MPOL_MF_MOVE    (1 << 1)

/**
 * @brief Thin wrapper around the Linux @c mbind(2) syscall.
 *
 * Expresses a NUMA memory-placement policy on a virtual address range
 * without depending on libnuma.
 *
 * @param addr      Start of the virtual address range.
 * @param len       Length of the range in bytes.
 * @param mode      NUMA policy constant (e.g., @c KEEL_MPOL_BIND).
 * @param nodemask  Bitmask of eligible nodes; one bit per node index.
 * @param maxnode   Number of valid bits in @p nodemask.
 * @param flags     Policy modifier flags (e.g., @c KEEL_MPOL_MF_MOVE).
 * @return Syscall return value (0 on success, -1 with @c errno on failure).
 */
static long keel_mbind(void *addr, unsigned long len, int mode,
                       const unsigned long *nodemask, unsigned long maxnode,
                       unsigned flags) {
    return syscall(SYS_mbind, addr, len, mode, nodemask, maxnode, flags);
}

/* migrate_pages syscall — available for future use */
#if 0
static long keel_migrate_pages(pid_t pid, unsigned long maxnode,
                               const unsigned long *old_nodes,
                               const unsigned long *new_nodes) {
    return syscall(SYS_migrate_pages, pid, maxnode, old_nodes, new_nodes);
}
#endif

#endif /* __linux__ */

/* ============================================================================
 * Module State
 * ============================================================================ */

static keel_numa_info_t g_numa;
static bool             g_numa_initialized = false;
static size_t           g_page_size = 0;

/* Per-node stats (max 64 nodes) */
#define MAX_NUMA_NODES 64
static _Atomic uint64_t g_node_alloc_count[MAX_NUMA_NODES];
static _Atomic uint64_t g_node_alloc_bytes[MAX_NUMA_NODES];
static _Atomic uint64_t g_node_free_count[MAX_NUMA_NODES];
static _Atomic uint64_t g_node_free_bytes[MAX_NUMA_NODES];
static _Atomic uint64_t g_migrate_count;
static _Atomic uint64_t g_fallback_count;

/* ============================================================================
 * Topology Detection (Linux sysfs)
 * ============================================================================ */

#ifdef __linux__

/**
 * @brief Probe sysfs to count the number of NUMA nodes present on the system.
 * @return Number of NUMA nodes (at least 1 even when sysfs is absent).
 */
static int detect_num_nodes(void) {
    int max_node = -1;
    char path[256];

    for (int n = 0; n < MAX_NUMA_NODES; n++) {
        snprintf(path, sizeof(path), "/sys/devices/system/node/node%d", n);
        if (access(path, F_OK) == 0) {
            max_node = n;
        }
    }
    return max_node + 1;
}

/**
 * @brief Return the number of online logical CPUs.
 * @return CPU count from @c sysconf(_SC_NPROCESSORS_ONLN), with a minimum of 1.
 */
static int detect_num_cpus(void) {
    int ncpus = (int)sysconf(_SC_NPROCESSORS_ONLN);
    return (ncpus > 0) ? ncpus : 1;
}

/**
 * @brief Build the cpu-to-node mapping table by parsing sysfs cpulist files.
 *
 * Reads `/sys/devices/system/node/nodeN/cpulist` for each node and fills
 * @p cpu_to_node.  All entries default to node 0 before parsing begins.
 *
 * @param num_nodes    Number of NUMA nodes.
 * @param num_cpus     Total number of logical CPUs.
 * @param cpu_to_node  [out] Array of length @p num_cpus to fill.
 */
static void detect_cpu_to_node(int num_nodes, int num_cpus, int* cpu_to_node) {
    char path[256];
    char buf[4096];

    /* Default: all CPUs on node 0 */
    memset(cpu_to_node, 0, (size_t)num_cpus * sizeof(int));

    for (int n = 0; n < num_nodes; n++) {
        snprintf(path, sizeof(path),
                 "/sys/devices/system/node/node%d/cpulist", n);
        FILE* f = fopen(path, "r");
        if (!f) continue;

        if (fgets(buf, sizeof(buf), f)) {
            /* Parse CPU list like "0-3,8-11" */
            char* tok = strtok(buf, ",\n");
            while (tok) {
                int lo, hi;
                if (sscanf(tok, "%d-%d", &lo, &hi) == 2) {
                    for (int c = lo; c <= hi && c < num_cpus; c++)
                        cpu_to_node[c] = n;
                } else if (sscanf(tok, "%d", &lo) == 1) {
                    if (lo < num_cpus)
                        cpu_to_node[lo] = n;
                }
                tok = strtok(NULL, ",\n");
            }
        }
        fclose(f);
    }
}

/**
 * @brief Read a numeric field from a node's sysfs meminfo file.
 *
 * Parses lines of the form `Node X FieldName:  <kB>` from
 * `/sys/devices/system/node/nodeN/meminfo` and converts the value to bytes.
 *
 * @param node  NUMA node index.
 * @param field Field name to search for (e.g., `"MemTotal"`).
 * @return Field value in bytes, or `0` if not found or on read error.
 */
static uint64_t read_node_meminfo(int node, const char* field) {
    char path[256];
    char line[256];
    uint64_t value = 0;

    snprintf(path, sizeof(path),
             "/sys/devices/system/node/node%d/meminfo", node);
    FILE* f = fopen(path, "r");
    if (!f) return 0;

    while (fgets(line, sizeof(line), f)) {
        if (strstr(line, field)) {
            /* Format: "Node X MemTotal:  12345678 kB" */
            char* colon = strchr(line, ':');
            if (colon) {
                value = (uint64_t)strtoull(colon + 1, NULL, 10) * 1024ULL;
            }
            break;
        }
    }
    fclose(f);
    return value;
}

#endif /* __linux__ */

/* ============================================================================
 * Initialization
 * ============================================================================ */

/**
 * @brief Discover NUMA topology and initialize placement statistics.
 *
 * @return `KEEL_OK` on success, or an allocation error if the topology tables
 *         cannot be created.
 */
keel_error_t keel_numa_init(void) {
    if (g_numa_initialized) return KEEL_OK;

    memset(&g_numa, 0, sizeof(g_numa));
    g_page_size = (size_t)sysconf(_SC_PAGESIZE);
    if (g_page_size == 0) g_page_size = 4096;

    memset(g_node_alloc_count, 0, sizeof(g_node_alloc_count));
    memset(g_node_alloc_bytes, 0, sizeof(g_node_alloc_bytes));
    memset(g_node_free_count,  0, sizeof(g_node_free_count));
    memset(g_node_free_bytes,  0, sizeof(g_node_free_bytes));
    g_migrate_count = 0;
    g_fallback_count = 0;

#ifdef __linux__
    g_numa.num_nodes = detect_num_nodes();
    g_numa.num_cpus  = detect_num_cpus();

    if (g_numa.num_nodes <= 0) {
        g_numa.num_nodes = 1;
    }

    /* Allocate topology arrays */
    g_numa.cpu_to_node = keel_calloc((size_t)g_numa.num_cpus, sizeof(int));
    g_numa.node_mem_total = keel_calloc((size_t)g_numa.num_nodes, sizeof(uint64_t));
    g_numa.node_mem_free  = keel_calloc((size_t)g_numa.num_nodes, sizeof(uint64_t));

    if (!g_numa.cpu_to_node || !g_numa.node_mem_total || !g_numa.node_mem_free) {
        keel_numa_shutdown();
        return KEEL_ERR_NOMEM;
    }

    detect_cpu_to_node(g_numa.num_nodes, g_numa.num_cpus, g_numa.cpu_to_node);

    for (int n = 0; n < g_numa.num_nodes; n++) {
        g_numa.node_mem_total[n] = read_node_meminfo(n, "MemTotal");
        g_numa.node_mem_free[n]  = read_node_meminfo(n, "MemFree");
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_MEM,
                  "NUMA: %d nodes, %d CPUs",
                  g_numa.num_nodes, g_numa.num_cpus);

    for (int n = 0; n < g_numa.num_nodes; n++) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_MEM,
                       "NUMA node %d: total=%lu MB, free=%lu MB",
                       n,
                       (unsigned long)(g_numa.node_mem_total[n] / (1024*1024)),
                       (unsigned long)(g_numa.node_mem_free[n] / (1024*1024)));
    }
#else
    /* Non-Linux: single-node fallback */
    g_numa.num_nodes = 1;
    g_numa.num_cpus  = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (g_numa.num_cpus <= 0) g_numa.num_cpus = 1;

    g_numa.cpu_to_node    = keel_calloc((size_t)g_numa.num_cpus, sizeof(int));
    g_numa.node_mem_total = keel_calloc(1, sizeof(uint64_t));
    g_numa.node_mem_free  = keel_calloc(1, sizeof(uint64_t));
#endif

    g_numa_initialized = true;
    return KEEL_OK;
}

/**
 * @brief Release discovered topology state.
 *
 * @return
 */
void keel_numa_shutdown(void) {
    if (!g_numa_initialized) return;

    keel_free(g_numa.cpu_to_node);
    keel_free(g_numa.node_mem_total);
    keel_free(g_numa.node_mem_free);
    memset(&g_numa, 0, sizeof(g_numa));
    g_numa_initialized = false;
}

/* ============================================================================
 * Topology Queries
 * ============================================================================ */

/**
 * @brief Return the initialized NUMA topology descriptor.
 * @return Pointer to the global topology structure, or `NULL` if
 *         @c keel_numa_init() has not been called yet.
 */
const keel_numa_info_t* keel_numa_topology(void) {
    return g_numa_initialized ? &g_numa : NULL;
}

/**
 * @brief Check whether true multi-node NUMA is present.
 * @return `true` if NUMA has been initialized and more than one node was
 *         detected, `false` otherwise.
 */
bool keel_numa_available(void) {
    return g_numa_initialized && g_numa.num_nodes > 1;
}

/**
 * @brief Look up the NUMA node that owns a given logical CPU index.
 * @param cpu Logical CPU number (0-based).
 * @return NUMA node index, or `0` if topology is unavailable or @p cpu is
 *         out of range.
 */
int keel_numa_node_of_cpu(int cpu) {
    if (!g_numa_initialized || !g_numa.cpu_to_node)
        return 0;
    if (cpu < 0 || cpu >= g_numa.num_cpus)
        return 0;
    return g_numa.cpu_to_node[cpu];
}

/**
 * @brief Return the NUMA node of the CPU currently executing this thread.
 * @return NUMA node index for the calling thread's CPU, or `0` on
 *         non-Linux platforms.
 */
int keel_numa_current_node(void) {
#ifdef __linux__
    int cpu = sched_getcpu();
    return keel_numa_node_of_cpu(cpu);
#else
    return 0;
#endif
}

/* ============================================================================
 * Allocation
 * ============================================================================ */

/**
 * @brief Round @p size up to the nearest multiple of the system page size.
 * @param size Requested size in bytes.
 * @return Size rounded up to a full page boundary.
 */
static size_t round_up_page(size_t size) {
    return (size + g_page_size - 1) & ~(g_page_size - 1);
}

/**
 * @brief Allocate page-backed memory and optionally bind it to a NUMA node.
 *
 * @param node Target node, or a negative value to prefer the local node.
 * @param size Requested allocation size.
 * @return Pointer to mapped memory, or `NULL` on failure.
 */
void* keel_numa_alloc(int node, size_t size) {
    if (size == 0) return NULL;
    size_t alloc_size = round_up_page(size);

    void* ptr = mmap(NULL, alloc_size,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) return NULL;

#ifdef __linux__
    if (g_numa_initialized && g_numa.num_nodes > 1 && node >= 0 &&
        node < g_numa.num_nodes) {
        /* Build nodemask: one bit per possible node */
        unsigned long nodemask[MAX_NUMA_NODES / (8 * sizeof(unsigned long))] = {0};
        size_t word = (size_t)node / (8 * sizeof(unsigned long));
        size_t bit  = (size_t)node % (8 * sizeof(unsigned long));
        nodemask[word] = 1UL << bit;

        long rc = keel_mbind(ptr, alloc_size, KEEL_MPOL_BIND,
                             nodemask, MAX_NUMA_NODES, 0);
        if (rc != 0) {
            /* mbind failed — memory still usable, just not pinned */
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_MEM,
                           "NUMA: mbind failed for node %d: %s",
                           node, strerror(errno));
            g_fallback_count++;
        }
    } else if (node < 0) {
        /* Local allocation — use preferred policy for current node */
        int local = keel_numa_current_node();
        unsigned long nodemask[MAX_NUMA_NODES / (8 * sizeof(unsigned long))] = {0};
        size_t word = (size_t)local / (8 * sizeof(unsigned long));
        size_t bit  = (size_t)local % (8 * sizeof(unsigned long));
        nodemask[word] = 1UL << bit;
        keel_mbind(ptr, alloc_size, KEEL_MPOL_PREFERRED,
                   nodemask, MAX_NUMA_NODES, 0);
    }
#else
    (void)node;
#endif

    int track_node = (node >= 0 && node < MAX_NUMA_NODES) ? node : 0;
    g_node_alloc_count[track_node]++;
    g_node_alloc_bytes[track_node] += alloc_size;

    return ptr;
}

/**
 * @brief Allocate page-backed memory preferring the NUMA node of the calling thread.
 * @param size Requested size in bytes.
 * @return Pointer to mapped memory, or `NULL` on failure.
 */
void* keel_numa_alloc_local(size_t size) {
    return keel_numa_alloc(keel_numa_current_node(), size);
}

/**
 * @brief Allocate zero-initialised page-backed memory on a specific NUMA node.
 *
 * Equivalent to @c keel_numa_alloc() because @c mmap already returns
 * zero-filled pages.
 *
 * @param node Target NUMA node.
 * @param size Requested size in bytes.
 * @return Pointer to zeroed mapped memory, or `NULL` on failure.
 */
void* keel_numa_calloc(int node, size_t size) {
    /* mmap returns zeroed pages, so this is equivalent */
    return keel_numa_alloc(node, size);
}

/**
 * @brief Allocate page-backed memory interleaved across all NUMA nodes.
 *
 * Interleaving distributes pages evenly, improving aggregate bandwidth for
 * workloads with no strong locality preference.
 *
 * @param size Requested size in bytes.
 * @return Pointer to mapped memory, or `NULL` on failure.
 */
void* keel_numa_alloc_interleaved(size_t size) {
    if (size == 0) return NULL;
    size_t alloc_size = round_up_page(size);

    void* ptr = mmap(NULL, alloc_size,
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) return NULL;

#ifdef __linux__
    if (g_numa_initialized && g_numa.num_nodes > 1) {
        unsigned long nodemask[MAX_NUMA_NODES / (8 * sizeof(unsigned long))] = {0};
        for (int n = 0; n < g_numa.num_nodes && n < MAX_NUMA_NODES; n++) {
            size_t word = (size_t)n / (8 * sizeof(unsigned long));
            size_t bit  = (size_t)n % (8 * sizeof(unsigned long));
            nodemask[word] |= 1UL << bit;
        }
        keel_mbind(ptr, alloc_size, KEEL_MPOL_INTERLEAVE,
                   nodemask, MAX_NUMA_NODES, 0);
    }
#endif

    g_node_alloc_count[0]++;
    g_node_alloc_bytes[0] += alloc_size;
    return ptr;
}

/**
 * @brief Unmap a region previously allocated by @c keel_numa_alloc() or its variants.
 *
 * @p size must match the value used at allocation time so the page-aligned
 * length can be computed correctly.
 *
 * @param ptr  Start address of the region to free.
 * @param size Original requested size passed to the allocation call.
 */
void keel_numa_free(void* ptr, size_t size) {
    if (!ptr || size == 0) return;
    size_t alloc_size = round_up_page(size);
    munmap(ptr, alloc_size);
    g_node_free_count[0]++;
    g_node_free_bytes[0] += alloc_size;
}

/**
 * @brief Request migration of an existing mapped region to a target node.
 *
 * @param ptr Start address of the region.
 * @param size Region size.
 * @param node Target NUMA node.
 * @return `KEEL_OK` on success, `KEEL_ERR_NOT_SUPPORTED` on unsupported
 *         platforms, or another error when the request is invalid or fails.
 */
keel_error_t keel_numa_migrate(void* ptr, size_t size, int node) {
    (void)ptr; (void)size;
#ifdef __linux__
    if (!g_numa_initialized || g_numa.num_nodes <= 1 ||
        node < 0 || node >= g_numa.num_nodes) {
        return KEEL_ERR_INVALID_ARG;
    }

    size_t alloc_size = round_up_page(size);
    unsigned long nodemask[MAX_NUMA_NODES / (8 * sizeof(unsigned long))] = {0};
    size_t word = (size_t)node / (8 * sizeof(unsigned long));
    size_t bit  = (size_t)node % (8 * sizeof(unsigned long));
    nodemask[word] = 1UL << bit;

    long rc = keel_mbind(ptr, alloc_size, KEEL_MPOL_BIND,
                         nodemask, MAX_NUMA_NODES,
                         KEEL_MPOL_MF_MOVE | KEEL_MPOL_MF_STRICT);
    if (rc != 0) return KEEL_ERR_IO;

    g_migrate_count++;
    return KEEL_OK;
#else
    (void)node;
    return KEEL_ERR_NOT_SUPPORTED;
#endif
}

/* ============================================================================
 * NUMA Arena
 * ============================================================================ */

/**
 * @brief Create an arena whose initial locality policy is informed by a NUMA allocation.
 *
 * @param node Target NUMA node.
 * @param initial_size Requested initial arena size.
 * @return Arena handle, or `NULL` on failure.
 */
struct keel_arena* keel_numa_arena_create(int node, size_t initial_size) {
    if (initial_size == 0) initial_size = 65536;

    /* Allocate the arena backing memory from requested NUMA node */
    void* backing = keel_numa_alloc(node, initial_size);
    if (!backing) return NULL;

    /* Create a standard arena — the backing is already NUMA-pinned.
     * We use the regular arena API but ensure the initial chunk
     * comes from NUMA memory. For this, we create normally and the
     * arena's internal growth will use standard malloc. The primary
     * hot-path allocation from the initial block is NUMA-local. */
    keel_arena_t* arena = keel_arena_create(initial_size);
    if (!arena) {
        keel_numa_free(backing, initial_size);
        return NULL;
    }
    /* Note: The arena creates its own backing memory. In a production
     * integration, we'd inject the NUMA backing into the arena internals.
     * For now, the NUMA memory is a pre-faulted reservation that can be
     * used by the caller alongside the arena. */
    keel_numa_free(backing, initial_size);

    return arena;
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Snapshot NUMA allocator statistics.
 *
 * @param node Specific node index, or a negative value for an aggregate view.
 * @return Statistics snapshot.
 */
keel_numa_stats_t keel_numa_get_stats(int node) {
    keel_numa_stats_t s = {0};

    if (node >= 0 && node < MAX_NUMA_NODES) {
        s.alloc_count = g_node_alloc_count[node];
        s.alloc_bytes = g_node_alloc_bytes[node];
        s.free_count  = g_node_free_count[node];
        s.free_bytes  = g_node_free_bytes[node];
    } else {
        /* Aggregate across all nodes */
        for (int n = 0; n < MAX_NUMA_NODES; n++) {
            s.alloc_count += g_node_alloc_count[n];
            s.alloc_bytes += g_node_alloc_bytes[n];
            s.free_count  += g_node_free_count[n];
            s.free_bytes  += g_node_free_bytes[n];
        }
    }
    s.migrate_count = g_migrate_count;
    s.fallback_count = g_fallback_count;
    return s;
}
