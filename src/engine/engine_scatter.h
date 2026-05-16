/**
 * @file engine_scatter.h
 * @brief Scatter-merge query execution for sharded KEEL deployments.
 *
 * When the shard router returns KEEL_DISPATCH_SCATTER, the engine must fan
 * the query out to every shard, collect all rows, apply merge operations
 * (aggregate, group, sort, limit) and send the merged result to the client.
 *
 * This module provides a single synchronous entry point that performs that
 * pipeline on the calling worker thread using blocking TCP sockets to each
 * backend.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * GNU Affero General Public License v3.0
 */

#ifndef KEEL_ENGINE_SCATTER_H
#define KEEL_ENGINE_SCATTER_H

#include "keel/engine/engine.h"
#include "keel/engine/worker.h"
#include "keel/core/router.h"
#include "keel/core/scatter_store.h"
#include "keel/trace/trace.h"
#include "keel/log/audit_log.h"
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

/* Forward declaration — full type in keel/engine/backend_pool.h */
struct backend_pool;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Observability context for scatter-merge execution.
 *
 * Pass to keel_engine_scatter_execute() to enable distributed tracing
 * and audit logging.  All fields are optional; NULL/zero disables the
 * corresponding feature.
 */
typedef struct keel_scatter_obs_ctx {
    /* ---- Inputs ---- */
    const keel_trace_ctx_t *trace_ctx;  /**< W3C trace context for SQL comment propagation */
    keel_audit_log_t       *audit_log;  /**< Audit log (NULL = no scatter events logged) */
    const char             *username;   /**< Client username for audit records */
    const char             *database;   /**< Target database for audit records */

    /* ---- Outputs — written by keel_engine_scatter_execute() ---- */
    uint64_t rows_merged_out;  /**< Total merged rows delivered to client */
    bool     spilled_out;      /**< True if the scatter store spilled to disk */
} keel_scatter_obs_ctx_t;

/**
 * @brief Execute a scatter-merge query across all shards.
 *
 * Borrows an authenticated, TLS-wrapped connection from each backend pool,
 * executes @p sql in parallel across all healthy shards, collects rows,
 * applies merge operations from @p dr
 * (aggregation, GROUP BY, ORDER BY, LIMIT), and writes the merged result as
 * a complete PostgreSQL wire-protocol response directly to @p client_fd.
 *
 * Shard queries are dispatched in parallel using one POSIX thread per shard
 * and joined before the merge pipeline runs.  Failed shards are skipped and
 * logged; partial results from successful shards are merged normally.
 *
 * When @p obs is non-NULL and tracing is enabled, the W3C traceparent header
 * is prepended as a SQL block comment to every shard query.  When @p obs is
 * non-NULL and an audit log is configured, a SCATTER event is emitted after
 * all shards complete.
 *
 * Connections are returned to their pools after each shard completes.
 * On I/O failure a connection is closed and its pool slot is reclaimed by
 * the pool's background refill timer.
 *
 * @param server_pool        Engine server pool (provides health/host metadata).
 * @param server_pools       Per-server connection pools ([server_pool_count]).
 * @param server_pool_count  Number of entries in @p server_pools.
 * @param sql                Query text (null-terminated).
 * @param dr                 Dispatch result from keel_router_dispatch_sql().
 * @param client_fd          Client socket to write the result to.
 * @param max_mem_bytes      Scatter store memory limit (0 = default).
 * @param spill_dir          Spill directory for large results (NULL = default).
 * @param obs                Observability context (NULL = no tracing/audit).
 * @return 0 on success, -1 on error.
 */
int keel_engine_scatter_execute(
    const keel_server_pool_t*      server_pool,
    struct backend_pool**          server_pools,
    size_t                         server_pool_count,
    const char*                    sql,
    const keel_dispatch_result_t*  dr,
    int                            client_fd,
    size_t                         max_mem_bytes,
    const char*                    spill_dir,
    keel_scatter_obs_ctx_t*  obs);

/**
 * @brief Execute a scatter write query across multiple shards using 2PC.
 *
 * For scatter write transactions (INSERT/UPDATE/DELETE fan-out across shards),
 * executes the write SQL on each participating shard inside a two-phase commit
 * protocol:
 *
 *   Phase 1 — PREPARE: for each shard, borrow a connection, execute
 *     BEGIN; <write_sql>; PREPARE TRANSACTION '<gid>' and mark the
 *     participant prepared.
 *
 *   Phase 2 — COMMIT or ROLLBACK: if all shards prepared successfully,
 *     issue COMMIT PREPARED '<gid>' on each; otherwise issue ROLLBACK PREPARED
 *     '<gid>' on prepared shards and ROLLBACK on active shards.
 *
 * When @p dr->twopc is NULL or @p dr->twopc_required is false, executes a
 * best-effort fanout (BEGIN; write_sql; COMMIT) without 2PC guarantees.
 *
 * Sends a CommandComplete + ReadyForQuery response to @p client_fd on success,
 * or an ErrorResponse + ReadyForQuery on failure.
 *
 * @param server_pool        Engine server pool (health/host metadata).
 * @param server_pools       Per-server connection pools.
 * @param server_pool_count  Number of entries in @p server_pools.
 * @param sql                Write query text (null-terminated).
 * @param dr                 Dispatch result (provides scatter plan + 2PC coord).
 * @param client_fd          Client socket to send the result to.
 * @return 0 on success, -1 on error.
 */
int keel_engine_scatter_write(
    const keel_server_pool_t*      server_pool,
    struct backend_pool**          server_pools,
    size_t                         server_pool_count,
    const char*                    sql,
    const keel_dispatch_result_t*  dr,
    int                            client_fd);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ENGINE_SCATTER_H */
