/**
 * @file connpool.h
 * @brief Synchronous, blocking connection pool for routing-layer tests.
 *
 * This module is a TEST SUPPORT UTILITY, not production engine code.
 * Production backend pooling is handled by `src/worker/backend_pool.c`
 * (async, per-worker, reactor-integrated).  This module exists for:
 *
 *   - Routing-layer unit tests that need real TCP connectivity without
 *     spinning up the full io_uring/kqueue engine.
 *   - Simple single-threaded proxy loops used in integration test harnesses.
 *
 * Location: tests/support/ (intentionally separate from src/core/)
 */

#ifndef KEEL_TEST_CONNPOOL_H
#define KEEL_TEST_CONNPOOL_H

#include "keel/core/router.h"
#include "keel_error.h"
#include "keel_types.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Limits
 * ============================================================================ */

/** Maximum number of connection slots in a single pool. */
#define KEEL_CONNPOOL_MAX_CONNS  256

/** Maximum number of server pools managed by one registry.
 *  Matches the 64-server ceiling used by the router internals. */
#define KEEL_CONNPOOL_MAX_SERVERS 64

/* ============================================================================
 * Connection state
 * ============================================================================ */

/**
 * @brief Runtime state of a single pooled connection.
 */
typedef enum keel_conn_state {
    KEEL_CONN_IDLE    = 0,  /**< In pool, available for borrowing       */
    KEEL_CONN_ACTIVE  = 1,  /**< Checked out to a caller               */
    KEEL_CONN_CLOSED  = 2,  /**< Socket closed, slot available          */
} keel_conn_state_t;

/**
 * @brief A single pooled backend connection.
 *
 * Callers interact with this structure only through the pool API; direct
 * field access is permitted for reading `fd` to issue actual I/O.
 */
typedef struct keel_conn {
    int              fd;            /**< Socket fd; -1 when CLOSED          */
    keel_conn_state_t state;        /**< Current state                      */
    uint64_t         created_at_ns; /**< Monotonic ns at creation           */
    uint64_t         last_used_ns;  /**< Monotonic ns at last release       */
    uint32_t         backend_pid;   /**< PG BackendKeyData.pid (0 = unknown)*/
    uint32_t         cancel_secret; /**< PG BackendKeyData.secret           */
    bool             in_transaction;/**< True if backend is in a TX         */
    void*            userdata;      /**< Opaque per-connection state        */
} keel_conn_t;

/* ============================================================================
 * Pool statistics
 * ============================================================================ */

/**
 * @brief Snapshot of a single pool's counters.
 *
 * Populated by `keel_connpool_get_stats()` for monitoring and Prometheus export.
 */
typedef struct keel_connpool_stats {
    uint64_t borrows;       /**< Total successful acquire calls             */
    uint64_t returns;       /**< Total release calls                        */
    uint64_t creates;       /**< New connections opened                     */
    uint64_t destroys;      /**< Connections closed (idle/health/overflow)  */
    uint64_t hits;          /**< Borrows satisfied from idle pool           */
    uint64_t misses;        /**< Borrows that required a new connection     */
    uint64_t timeouts;      /**< Borrows that timed out waiting             */
    uint64_t health_evicts; /**< Connections evicted by health check        */
    uint64_t idle_evicts;   /**< Connections evicted by idle timeout        */
    size_t   active;        /**< Currently checked-out connections          */
    size_t   idle;          /**< Currently idle connections                 */
    size_t   total;         /**< active + idle (open sockets)               */
} keel_connpool_stats_t;

/* ============================================================================
 * Pool configuration
 * ============================================================================ */

/**
 * @brief Configuration parameters for one connection pool.
 *
 * All fields have sane defaults; zero values use the built-in defaults.
 */
typedef struct keel_connpool_config {
    size_t   min_conns;          /**< Keep-alive floor; default 0 (no floor)    */
    size_t   max_conns;          /**< Hard cap; default KEEL_CONNPOOL_MAX_CONNS  */
    uint32_t idle_timeout_ms;    /**< Evict idle connections after this; 0=never */
    uint32_t connect_timeout_ms; /**< TCP connect deadline; 0=5 000 ms          */
    uint32_t acquire_timeout_ms; /**< Max wait for a free slot; 0=5 000 ms      */
    uint32_t health_check_interval_ms; /**< Period between probes; 0=60 000 ms  */

    /**
     * @brief Optional health-check probe callback.
     *
     * Called by `keel_connpool_health_check()` for each idle connection.
     * Return `true` if the connection is usable, `false` to evict it.
     * If NULL, a simple `send(fd, "", 0, MSG_NOSIGNAL)` check is used.
     */
    bool (*health_probe)(keel_conn_t* conn, void* udata);
    void*  health_probe_udata;
} keel_connpool_config_t;

/* ============================================================================
 * Opaque pool handle
 * ============================================================================ */

typedef struct keel_connpool keel_connpool_t;

/* ============================================================================
 * Lifecycle API
 * ============================================================================ */

/**
 * @brief Create a connection pool for a single backend server.
 *
 * Does **not** open connections immediately; initial connections are created
 * lazily on the first `keel_connpool_acquire()` call, up to `min_conns`.
 *
 * @param server  Server descriptor (borrowed; must outlive the pool).
 * @param config  Pool configuration; pass NULL to use all defaults.
 * @return New pool, or NULL on allocation failure.
 */
keel_connpool_t* keel_connpool_create(const keel_route_server_t* server,
                                      const keel_connpool_config_t* config);

/**
 * @brief Destroy a pool and close all connections.
 *
 * Active (checked-out) connections are closed immediately; any caller still
 * holding a `keel_conn_t*` must not use it after this call.
 *
 * @param pool  Pool to destroy (may be NULL).
 */
void keel_connpool_destroy(keel_connpool_t* pool);

/* ============================================================================
 * Connection borrow / return
 * ============================================================================ */

/**
 * @brief Borrow an idle connection from the pool.
 *
 * If an idle connection exists it is returned immediately (pool hit).
 * Otherwise a new connection is opened (up to `max_conns`).  If the pool is
 * full, the call blocks up to `acquire_timeout_ms` polling for a free slot,
 * then returns `KEEL_ERR_POOL_TIMEOUT`.
 *
 * @param pool     Pool to acquire from.
 * @param conn_out Pointer filled with the borrowed connection.
 * @return KEEL_OK, KEEL_ERR_POOL_EXHAUSTED, KEEL_ERR_POOL_TIMEOUT, or
 *         KEEL_ERR_CONNECT on TCP failure.
 */
keel_error_t keel_connpool_acquire(keel_connpool_t*  pool,
                                   keel_conn_t**     conn_out);

/**
 * @brief Return a connection to the pool.
 *
 * If `reusable` is false, or if the connection is in a broken state
 * (`conn->fd < 0`), the socket is closed and the slot is recycled.
 * Otherwise the connection is marked IDLE and becomes available to the
 * next `keel_connpool_acquire()` caller.
 *
 * @param pool     Owning pool.
 * @param conn     Connection to return.
 * @param reusable Pass false to force close (e.g., after a protocol error).
 */
void keel_connpool_release(keel_connpool_t* pool,
                           keel_conn_t*     conn,
                           bool             reusable);

/* ============================================================================
 * Housekeeping
 * ============================================================================ */

/**
 * @brief Close connections that have been idle longer than `idle_timeout_ms`.
 *
 * Should be called periodically from a timer or background thread.
 * No-op when `idle_timeout_ms == 0`.
 *
 * @param pool  Pool to sweep.
 * @return Number of connections evicted.
 */
size_t keel_connpool_evict_idle(keel_connpool_t* pool);

/**
 * @brief Probe every idle connection and evict failed ones.
 *
 * Uses the `health_probe` callback if configured, or a zero-byte send check.
 *
 * @param pool  Pool to check.
 * @return Number of connections evicted.
 */
size_t keel_connpool_health_check(keel_connpool_t* pool);

/**
 * @brief Ensure at least `min_conns` connections are open.
 *
 * Opens new connections up to the configured minimum.  Useful on startup or
 * after a health-check sweep.
 *
 * @param pool  Pool to warm.
 * @return Number of new connections opened; negative on error.
 */
int keel_connpool_warm(keel_connpool_t* pool);

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Fill `stats` with a point-in-time snapshot of pool counters.
 *
 * Safe to call from any thread; individual fields are read with relaxed
 * semantics, so the snapshot may be slightly inconsistent under heavy load.
 *
 * @param pool   Pool to snapshot.
 * @param stats  Output structure to fill.
 */
void keel_connpool_get_stats(const keel_connpool_t* pool,
                              keel_connpool_stats_t* stats);

/* ============================================================================
 * Registry — one pool per router server
 * ============================================================================ */

/**
 * @brief Registry that manages one `keel_connpool_t` per router server.
 *
 * Callers use `keel_connpool_registry_get()` to look up the pool matching
 * a routing decision, then call `keel_connpool_acquire()` on it.
 */
typedef struct keel_connpool_registry keel_connpool_registry_t;

/**
 * @brief Create an empty registry.
 *
 * @param default_config  Default pool config applied to each new pool (may be NULL).
 * @return New registry, or NULL on allocation failure.
 */
keel_connpool_registry_t* keel_connpool_registry_create(
    const keel_connpool_config_t* default_config);

/**
 * @brief Free the registry and all contained pools.
 *
 * @param reg  Registry to destroy (may be NULL).
 */
void keel_connpool_registry_destroy(keel_connpool_registry_t* reg);

/**
 * @brief Look up (or lazily create) the pool for a given server.
 *
 * Pools are indexed by `server->name`; a new pool is created if none exists.
 *
 * @param reg     Registry.
 * @param server  Server descriptor from a routing decision.
 * @return Matching pool, or NULL on allocation failure.
 */
keel_connpool_t* keel_connpool_registry_get(keel_connpool_registry_t* reg,
                                            const keel_route_server_t* server);

/**
 * @brief Run `keel_connpool_evict_idle()` on every pool in the registry.
 *
 * @param reg  Registry to sweep.
 * @return Total number of connections evicted.
 */
size_t keel_connpool_registry_evict_idle(keel_connpool_registry_t* reg);

/**
 * @brief Run `keel_connpool_health_check()` on every pool in the registry.
 *
 * @param reg  Registry to probe.
 * @return Total number of connections evicted.
 */
size_t keel_connpool_registry_health_check(keel_connpool_registry_t* reg);

/**
 * @brief Aggregate stats across all pools in the registry.
 *
 * Each counter in `stats` is the sum across all per-server pools.
 *
 * @param reg    Registry.
 * @param stats  Output structure to fill.
 */
void keel_connpool_registry_get_stats(const keel_connpool_registry_t* reg,
                                       keel_connpool_stats_t* stats);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_CONNPOOL_H */
