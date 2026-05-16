/**
 * @file proxy_session.h
 * @brief Sharding-aware proxy session: integrates the router with the PG
 *        wire-protocol flow and the connection pool registry.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * `keel_client_session_t` is the glue layer that connects three independent
 * sub-systems that previously had no common coordinator in the routing layer:
 *
 *   1. **Router** (`keel_router_t`) — selects the right backend server for a
 *      given SQL statement (single-shard, scatter, or UNSUPPORTED).
 *
 *   2. **Connection pool** (`keel_connpool_registry_t`) — holds live sockets
 *      to each backend server; the session borrows and returns connections.
 *
 *   3. **PG wire protocol state** (`keel_route_session_t`) — tracks transaction
 *      state, pinned server, scatter-write mask, and temp-table flags so the
 *      router can make correct routing decisions.
 *
 * The session does NOT own any sockets or file descriptors from the client
 * side — that is handled by the full engine's `keel_session_t`.  Instead,
 * `keel_client_session_t` is the per-client routing context that a proxy loop
 * attaches to each accepted connection.
 *
 * Typical call sequence in a single-threaded proxy loop:
 *
 * @code
 *   keel_client_session_t* cs = keel_client_session_create(router, reg);
 *
 *   // Client sends a query
 *   keel_dispatch_result_t disp;
 *   keel_error_t err = keel_client_session_dispatch(cs, sql, params,
 *                                                    is_write, &disp);
 *   if (err == KEEL_OK && disp.kind == KEEL_DISPATCH_SINGLE) {
 *       keel_conn_t* conn;
 *       keel_connpool_acquire(keel_connpool_registry_get(reg, disp.single.server),
 *                             &conn);
 *       // ... send query on conn->fd, read response ...
 *       keel_connpool_release(..., conn, reusable);
 *   }
 *
 *   // Client sends BEGIN
 *   keel_client_session_begin_tx(cs);
 *
 *   // Client sends COMMIT / ROLLBACK
 *   keel_client_session_end_tx(cs);
 *
 *   keel_client_session_destroy(cs);
 * @endcode
 */

#ifndef KEEL_TEST_PROXY_SESSION_H
#define KEEL_TEST_PROXY_SESSION_H

#include "keel/core/router.h"
#include "keel/core/sharding.h"
#include "connpool.h"
#include "keel_error.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Query lifecycle events
 * ============================================================================ */

/**
 * @brief Per-query timing recorded by the session.
 */
typedef struct keel_query_timing {
    uint64_t dispatch_ns;   /**< Time spent in router dispatch              */
    uint64_t acquire_ns;    /**< Time spent waiting for a pool connection   */
    uint64_t execute_ns;    /**< Time from send to last byte of response    */
    uint64_t total_ns;      /**< End-to-end wall time for this query        */
} keel_query_timing_t;

/* ============================================================================
 * Session statistics
 * ============================================================================ */

/**
 * @brief Lifetime counters for one client session.
 */
typedef struct keel_client_session_stats {
    uint64_t queries_total;     /**< Total queries dispatched               */
    uint64_t queries_single;    /**< Dispatched to a single shard           */
    uint64_t queries_scatter;   /**< Fanned out across multiple shards      */
    uint64_t queries_timeout;   /**< Aborted due to query_timeout           */
    uint64_t tx_count;          /**< Transactions started                   */
    uint64_t tx_aborted;        /**< Transactions aborted (error/rollback)  */
    uint64_t cross_tx_rejected; /**< Queries rejected for cross-tx shard violation */
} keel_client_session_stats_t;

/* ============================================================================
 * Opaque session handle
 * ============================================================================ */

typedef struct keel_client_session keel_client_session_t;

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

/**
 * @brief Create a new proxy session.
 *
 * The session does not open any network connections at creation time;
 * everything is deferred to the first `keel_client_session_dispatch()` call.
 *
 * @param router  Router to use for all dispatch decisions (borrowed).
 * @param reg     Connection pool registry (borrowed).
 * @return New session, or NULL on allocation failure.
 */
keel_client_session_t* keel_client_session_create(
    keel_router_t*            router,
    keel_connpool_registry_t* reg);

/**
 * @brief Destroy a session.
 *
 * Any pinned server is released and scatter-write tracking is cleared.
 * Does NOT close connections in the pool — those are returned via
 * `keel_client_session_release_conn()` and remain available for the next
 * caller.
 *
 * @param cs  Session to destroy (may be NULL).
 */
void keel_client_session_destroy(keel_client_session_t* cs);

/* ============================================================================
 * Dispatch
 * ============================================================================ */

/**
 * @brief Dispatch a SQL statement through the router.
 *
 * Calls `keel_router_dispatch_sql()` with the session's current routing
 * state, records query timing, and returns the full dispatch result to the
 * caller so it can open the right backend connection(s).
 *
 * On `KEEL_DISPATCH_SINGLE` the caller should:
 *   1. Call `keel_connpool_acquire()` on the pool for `out->single.server`.
 *   2. Execute the query.
 *   3. Call `keel_client_session_release_conn()`.
 *
 * On `KEEL_DISPATCH_SCATTER` the caller iterates `out->scatter.decisions[]`.
 *
 * @param cs       Session context.
 * @param sql      SQL text.
 * @param params   Bound parameters for $N resolution (may be NULL).
 * @param is_write True for INSERT / UPDATE / DELETE / DDL.
 * @param out      Routing decision output.
 * @param timing   Optional per-query timing output (may be NULL).
 * @return KEEL_OK, KEEL_ERR_SHARD_CROSS_TX, KEEL_ERR_NOT_SUPPORTED, or
 *         KEEL_ERR_QUERY_TIMEOUT.
 */
keel_error_t keel_client_session_dispatch(
    keel_client_session_t*            cs,
    keel_str_t                        sql,
    const keel_shard_bound_params_t*  params,
    bool                              is_write,
    keel_dispatch_result_t*           out,
    keel_query_timing_t*              timing);

/**
 * @brief Release a connection back to the pool after a query.
 *
 * Convenience wrapper around `keel_connpool_registry_get()` +
 * `keel_connpool_release()` that also updates per-session scatter-write
 * tracking when the query was a scatter write.
 *
 * @param cs        Session context.
 * @param server    Server the connection was borrowed from.
 * @param conn      Connection to return.
 * @param reusable  False if the connection encountered a protocol error.
 */
void keel_client_session_release_conn(keel_client_session_t*    cs,
                                       const keel_route_server_t* server,
                                       keel_conn_t*               conn,
                                       bool                       reusable);

/* ============================================================================
 * Transaction control
 * ============================================================================ */

/**
 * @brief Notify the session that a transaction has begun.
 *
 * Sets `routing_state.in_transaction = true`.  Subsequent routing decisions
 * will prefer the same shard unless the session is in scatter-write mode.
 *
 * @param cs  Session context.
 */
void keel_client_session_begin_tx(keel_client_session_t* cs);

/**
 * @brief Notify the session that a transaction has ended (COMMIT or ROLLBACK).
 *
 * Clears `in_transaction`, `has_scatter_write`, `scatter_shards_mask`, and
 * releases the pinned server if it was set for the transaction.
 *
 * @param cs       Session context.
 * @param aborted  True if the transaction was rolled back or failed.
 */
void keel_client_session_end_tx(keel_client_session_t* cs, bool aborted);

/**
 * @brief Record a scatter write so future single-shard queries in this
 *        transaction are validated against the participating-shard mask.
 *
 * Thin wrapper around `keel_router_record_scatter_write()` that also
 * increments the session's scatter-query counter.
 *
 * @param cs    Session context.
 * @param plan  Scatter plan from the most recent dispatch.
 */
void keel_client_session_record_scatter_write(keel_client_session_t* cs,
                                               const keel_scatter_plan_t* plan);

/**
 * @brief Pin the session to a specific server.
 *
 * All subsequent routing decisions will return this server regardless of
 * shard analysis.  Typically set after detecting SET commands, cursor
 * operations, or LISTEN that require connection affinity.
 *
 * @param cs      Session context.
 * @param server  Server to pin to (borrowed).
 */
void keel_client_session_pin(keel_client_session_t*    cs,
                              const keel_route_server_t* server);

/**
 * @brief Clear the server pin.
 *
 * @param cs  Session context.
 */
void keel_client_session_unpin(keel_client_session_t* cs);

/* ============================================================================
 * Accessors
 * ============================================================================ */

/**
 * @brief Return the current routing state (read-only).
 *
 * Useful for introspection in tests and admin handlers.
 *
 * @param cs  Session context.
 * @return Pointer to the internal routing state.
 */
const keel_route_session_t* keel_client_session_routing_state(
    const keel_client_session_t* cs);

/**
 * @brief Fill `stats` with the session's lifetime counters.
 *
 * @param cs     Session context.
 * @param stats  Output structure.
 */
void keel_client_session_get_stats(const keel_client_session_t*  cs,
                                    keel_client_session_stats_t*  stats);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PROXY_SESSION_H */
