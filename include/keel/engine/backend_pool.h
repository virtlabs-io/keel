/**
 * @file backend_pool.h
 * @brief Public API for backend pooling, wait queues, and multiplexing-safe reuse.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This implements a pool of backend connections that can be shared across
 * multiple frontend clients. Supports:
 * - Pre-established connections to backend servers
 * - Transaction-aware session pinning
 * - State tracking (clean, transaction-pinned, state-pinned)
 * - Prepared statement virtualization
 * - Reactor-owned protocol cleanup on stateful connection return
 */

#ifndef KEEL_BACKEND_POOL_H
#define KEEL_BACKEND_POOL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <pthread.h>
#include <netinet/in.h>
#include "keel/protocol/tls_context.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/core/cloud_auth.h"

/* Forward declare state profile */
struct state_profile;
struct keel_reactor;
struct keel_proto_flow_vtable;
struct keel_stats_ctx;

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Types
 * ============================================================================ */

/**
 * @brief Backend connection state
 */
typedef enum backend_conn_state {
    BACKEND_CONN_IDLE = 0,          /**< In pool, available for any client */
    BACKEND_CONN_ACTIVE,            /**< Currently handling a query */
    BACKEND_CONN_TXN_PINNED,        /**< Pinned to session due to transaction */
    BACKEND_CONN_STATE_PINNED,      /**< Pinned due to SET variables or prepared statements */
    BACKEND_CONN_SYNCING,           /**< Synchronizing state before query */
    BACKEND_CONN_CLEANING,          /**< Cleanup command sent, awaiting response */
    BACKEND_CONN_CLOSED,            /**< Connection closed/failed */
} backend_conn_state_t;

/**
 * @brief Reactor-owned cleanup sub-state for BACKEND_CONN_CLEANING slots.
 */
typedef enum backend_cleanup_state {
    BACKEND_CLEANUP_NONE = 0,       /**< No cleanup operation in flight */
    BACKEND_CLEANUP_SEND,           /**< Sending plugin-built cleanup command */
    BACKEND_CLEANUP_DRAIN,          /**< Draining cleanup responses via plugin */
} backend_cleanup_state_t;

/**
 * @brief Why a backend is quarantined from reuse.
 */
typedef enum backend_quarantine_reason {
    BACKEND_QUARANTINE_NONE = 0,      /**< Not quarantined */
    BACKEND_QUARANTINE_DIRTY,         /**< Carries state requiring cleanup/replay */
    BACKEND_QUARANTINE_SYNCING,       /**< Sync operation in progress */
    BACKEND_QUARANTINE_REPLAYING,     /**< Replay operation in progress */
    BACKEND_QUARANTINE_PROTOCOL_DESYNC,/**< Protocol state mismatch detected */
    BACKEND_QUARANTINE_CLEANUP_FAILED,/**< Cleanup failed; slot is not reusable */
} backend_quarantine_reason_t;

/**
 * @brief Why a backend slot transitioned to CLOSED.
 */
typedef enum backend_close_reason {
    BACKEND_CLOSE_REASON_NONE = 0,
    BACKEND_CLOSE_REASON_DEAD_IDLE,
    BACKEND_CLOSE_REASON_CLEANUP_ERROR,
    BACKEND_CLOSE_REASON_CLEANUP_TIMEOUT,
    BACKEND_CLOSE_REASON_CLIENT_DISCONNECT,
    BACKEND_CLOSE_REASON_IO_ERROR,
    BACKEND_CLOSE_REASON_PRUNE_IDLE,
    BACKEND_CLOSE_REASON_PRUNE_AGED,
    BACKEND_CLOSE_REASON_DRAIN_IDLE,
} backend_close_reason_t;

/**
 * @brief Outcome taxonomy for reactor-owned backend cleanup attempts.
 */
typedef enum backend_cleanup_result {
    BACKEND_CLEANUP_RESULT_NONE = 0,
    BACKEND_CLEANUP_RESULT_SUCCESS,
    BACKEND_CLEANUP_RESULT_PROTOCOL_ERROR,
    BACKEND_CLEANUP_RESULT_TIMEOUT,
    BACKEND_CLEANUP_RESULT_BACKEND_EOF,
    BACKEND_CLEANUP_RESULT_SEND_FAILURE,
} backend_cleanup_result_t;

#define KEEL_BACKEND_CLEANUP_RECV_BUFSZ 1024
#define KEEL_BACKEND_CLEANUP_CMD_BUFSZ 2048

/**
 * @brief Backend connection
 */
typedef struct backend_conn {
    int                     fd;                 /**< Socket file descriptor */
    _Atomic backend_conn_state_t state;         /**< Current state (atomic for CAS borrow) */
    uint64_t                current_state_hash; /**< Hash of SET variables */
    uint64_t                stmt_set_hash;      /**< Hash of named prepared statements on this backend.
                                                 *   Non-zero means the backend has prepared statements
                                                 *   and must not receive full cleanup on pool return —
                                                 *   the stmts are kept for reuse by matching sessions.
                                                 *   Cleared when full cleanup is sent. */
    bool                    in_transaction;     /**< Inside BEGIN...COMMIT */
    bool                    needs_sync;         /**< Needs state sync before use */
    bool                    syncing;            /**< Backend sync in progress */
    bool                    replay_active;      /**< Backend replay in progress */
    bool                    protocol_desync;    /**< Protocol desync observed */
    bool                    hard_pinned;        /**< Exclusively owned by session (spec §16) */
    bool                    needs_full_cleanup;  /**< Borrowed with a different stmt hash — engine must
                                                 *   run full cleanup before replaying prepared stmts.
                                                 *   Set by borrow_with_stmts Step 4; cleared by engine. */
    uint32_t                backend_pid;        /**< Protocol backend id for cancel forwarding */
    uint32_t                cancel_secret;      /**< Protocol cancel secret when applicable */
    uint32_t                my_connection_id;   /**< Protocol connection id for command cancellation */
    uint64_t                last_used;          /**< Timestamp of last use */
    uint64_t                created_at;         /**< Timestamp when connection was established (ms) */
    uint64_t                generation;         /**< Monotonic lifecycle generation for stale-ref detection */
    uint64_t                clean_gen;          /**< Monotonic generation counter (bumped on return) */
    backend_quarantine_reason_t quarantine;     /**< Reuse quarantine reason */
    backend_close_reason_t  close_reason;       /**< Last close reason for this generation */
    backend_cleanup_result_t cleanup_last_result; /**< Last cleanup outcome for this generation */
    uint64_t                cleanup_last_duration_ns; /**< Last cleanup duration in ns */
    void*                   active_owner;       /**< Non-NULL while backend is actively owned */
    void*                   pinned_session;     /**< Session pinned to (or NULL) */
    struct state_profile*   profile;            /**< Connection state profile (spec §5) */
    struct backend_pool*    pool;               /**< Pool this connection belongs to */
    struct backend_conn*    next;               /**< Next in linked list */

    backend_cleanup_state_t cleanup_state;      /**< CLEANING sub-state */
    bool                    cleanup_io_armed;   /**< true while reactor send/recv is outstanding */
    uint8_t                 cleanup_send_buf[KEEL_BACKEND_CLEANUP_CMD_BUFSZ];
    size_t                  cleanup_send_len;   /**< Bytes valid in cleanup_send_buf */
    size_t                  cleanup_send_off;   /**< Bytes of cleanup command already sent */
    uint64_t                cleanup_started_ms; /**< Timeout anchor for reactor-owned cleanup */
    uint8_t                 cleanup_recv_buf[KEEL_BACKEND_CLEANUP_RECV_BUFSZ];
    keel_proto_drain_state_t cleanup_drain_state; /**< Protocol-owned cleanup drain state */
} backend_conn_t;

/**
 * @brief Pool configuration
 */
typedef struct backend_pool_config {
    const char*     host;               /**< Backend host */
    uint16_t        port;               /**< Backend port */
    const char*     user;               /**< Username for backend auth */
    const char*     password;           /**< Password for backend auth (SCRAM) */
    const char*     database;           /**< Database name */
    const char*     protocol;           /**< Protocol name: "postgres" or "mysql" */
    size_t          min_connections;    /**< Pre-establish this many connections */
    size_t          max_connections;    /**< Maximum pool size */
    size_t          max_waiting;        /**< Maximum waiting queue size */
    uint64_t        idle_timeout_ms;    /**< Close idle connections after this (ms), 0=never */
    uint64_t        wait_timeout_ms;    /**< Max time a session can wait for a backend (ms), 0=infinite */
    uint64_t        max_connection_age_ms;/**< Close connections older than this (ms), 0=never */
    size_t          max_user_connections;/**< Max active connections per user, 0=unlimited */
    keel_tls_config_t tls_config;       /**< Backend TLS configuration */
    keel_cloud_auth_type_t cloud_auth_type;  /**< Cloud auth provider type (NONE = static pw) */
    void*           cloud_auth_config;       /**< Provider-specific config (opaque, type-dependent) */
} backend_pool_config_t;

/**
 * @brief Waiter in the waiting queue
 */
typedef struct pool_waiter {
    void*               session;        /**< Session waiting */
    void*               userdata;       /**< User data for callback */
    uint64_t            enqueue_time_ms;/**< Timestamp when enqueued (for timeout) */
    struct pool_waiter* next;           /**< Next waiter */
} pool_waiter_t;

/**
 * @brief Callback when connection becomes available
 */
typedef void (*backend_pool_wait_cb)(void* session, void* userdata);

/**
 * @brief Backend connection pool
 *
 * Spec §6: The idle list is logically partitioned into three sublists:
 *   1. clean_list  — connections with no SET state (profile == NULL or empty)
 *   2. idle_list   — connections with known state profiles (bucket by hash)
 *   3. dirty_list  — connections needing full cleanup before reuse
 */
typedef struct backend_pool {
    backend_pool_config_t   config;             /**< Configuration */
    backend_conn_t*         connections;        /**< Array of all connections */
    struct keel_reactor*     reactor;            /**< Reactor for async I/O (set by worker) */

    /* Partitioned idle lists (spec §6) */
    backend_conn_t*         clean_list;         /**< Clean connections (no state) */
    backend_conn_t*         idle_list;          /**< Stateful idle connections */
    backend_conn_t*         dirty_list;         /**< Connections needing reset */
    backend_conn_t*         cleaning_list;      /**< Connections in cleanup state machine */
    backend_conn_t*         quarantined_list;   /**< Non-borrowable quarantined connections */
    backend_conn_t*         closed_list;        /**< Closed slots awaiting refill */

    size_t                  active_count;       /**< Number of active connections */
    size_t                  total_count;        /**< Total connections in pool */
    size_t                  clean_count;        /**< Count on clean_list */
    size_t                  dirty_count;        /**< Count on dirty_list */
    size_t                  cleaning_count;     /**< Count in CLEANING state */

    /* Pinned connection admission control */
    size_t                  pinned_count;       /**< Connections pinned to sessions */
    size_t                  max_pinned;         /**< Max pinned backends (0 = unlimited) */
    
    /* Waiting queue */
    pool_waiter_t*          wait_queue_head;
    pool_waiter_t*          wait_queue_tail;
    size_t                  wait_queue_size;
    backend_pool_wait_cb    wait_callback;

    /* Refill backoff — pause reconnects after repeated backend rejections */
    uint64_t                refill_backoff_until;  /**< Skip refill until this timestamp (ms) */
    uint32_t                refill_fail_count;     /**< Consecutive refill failures (for exp backoff) */

    /* Protocol vtable — eliminates strcmp("mysql") branching */
    const struct keel_proto_flow_vtable* flow_vt;

    /* Waiter pool — eliminates per-wait calloc/free on the hot path.
     * Pre-allocated at pool creation, O(1) free-list alloc/free. */
    struct keel_pool*        waiter_pool;

    /* Resolved backend address (populated once at pool creation using
     * getaddrinfo so hostname lookups never block the reactor thread). */
    struct sockaddr_in       resolved_addr;
    bool                     addr_resolved;

    /* SCRAM-SHA-256 SaltedPassword cache (per-pool, single-threaded).
     * Some protocol auth paths reuse identical salt/iteration parameters for
     * connections from this pool. Caching Hi(password, salt, iters) avoids a
     * repeated blocking PBKDF2 call after the first connection. */
    bool                     scram_cache_valid;              /**< Cache contains a usable entry */
    int                      scram_cache_iterations;         /**< Iteration count of cached entry */
    size_t                   scram_cache_salt_len;           /**< Length of cached salt */
    uint8_t                  scram_cache_salt[64];           /**< Raw salt bytes */
    uint8_t                  scram_cache_salted_pw[32];      /**< Cached SaltedPassword output */

    /* Cloud-native auth token cache (per-pool, single-threaded).
     * When cloud_auth_type != NONE, this cache holds the current token and
     * auto-refreshes before expiry. */
    keel_cloud_token_cache_t cloud_token_cache;

    /* Per-user active connection count tracking (for max_user_connections).
     * Simple hash map: username → active_count.  Single-threaded per worker. */
    struct user_conn_entry {
        char     username[64];       /**< User identifier */
        size_t   active_count;       /**< Currently active/pinned connections */
    }                        user_conn_map[64];      /**< Fixed-size hash map */
    size_t                   user_conn_map_used;     /**< Entries in use */

    /* Stats context (set by worker after pool creation, same thread) */
    struct keel_stats_ctx*   stats_ctx;

    /* Pool-level lock for thread-safe list operations (borrow/return).
     * Uncontended in production (single-threaded per worker). */
    pthread_mutex_t          lock;
} backend_pool_t;

/**
 * @brief Pool statistics
 */
typedef struct backend_pool_stats {
    size_t      total_connections;
    size_t      active_connections;
    size_t      idle_connections;
    size_t      clean_connections;    /**< Connections on clean_list */
    size_t      stateful_connections; /**< Connections on stateful idle_list */
    size_t      dirty_connections;    /**< Connections awaiting cleanup kick */
    size_t      closed_connections;   /**< Closed slots awaiting refill */
    size_t      waiting_sessions;
    size_t      cleaning_count;     /**< Slots in CLEANING state */
    size_t      pinned_count;       /**< Slots pinned to sessions */
} backend_pool_stats_t;

/* ============================================================================
 * Pool Lifecycle
 * ============================================================================ */

/**
 * @brief Create a backend connection pool
 *
 * @param config Pool configuration
 * @return Pool handle, or NULL on failure
 */
backend_pool_t* backend_pool_create(const backend_pool_config_t* config);

/**
 * @brief Destroy a backend connection pool
 *
 * Closes all connections and frees resources.
 *
 * @param pool Pool to destroy
 */
void backend_pool_destroy(backend_pool_t* pool);

/* ============================================================================
 * Connection Borrowing
 * ============================================================================ */

/**
 * @brief Borrow a connection from the pool
 *
 * Returns an idle connection, preferring one with matching state hash.
 * If no connection is available and pool is at max, returns NULL.
 *
 * @param pool Pool to borrow from
 * @param required_state_hash State hash required (0 for clean)
 * @return Connection, or NULL if none available
 */
backend_conn_t* backend_pool_borrow(backend_pool_t* pool, uint64_t required_state_hash);

/**
 * @brief Canonical backend reuse predicate.
 *
 * All borrow decisions must call this predicate instead of ad-hoc checks.
 */
bool backend_pool_can_borrow(const backend_conn_t* conn);

/**
 * @brief Validate a session-held backend reference generation.
 *
 * Returns true only when the backend still matches the session snapshot.
 */
bool backend_pool_validate_generation(const backend_conn_t* conn,
                                      uint64_t expected_generation);

/**
 * @brief Borrow a connection, preferring one with matching prepared-statement set.
 *
 * Used when a session has named prepared statements.  Search order:
 *   1. idle_list entry with stmt_set_hash == required_stmt_hash  (no replay)
 *   2. clean_list entry (stmt_set_hash == 0, will need replay)
 *   3. any available connection (fallback, will need replay)
 *
 * This API uses both the return value and an output parameter to report the
 * borrow result. The returned pointer selects the backend slot, while
 * `out_needs_replay` tells the caller whether it must rebuild the session's
 * prepared statements on that backend before forwarding the blocked client
 * message. That split avoids inventing a heap-allocated result struct on a hot
 * path that is called for every prepared-statement borrow decision.
 *
 * @param pool                Pool to borrow from
 * @param required_state_hash SET-variable state hash (0 for clean)
 * @param required_stmt_hash  Prepared-statement set hash (0 = no preference)
 * @param[out] out_needs_replay Caller-owned flag set to `true` when the chosen
 *                              backend needs Parse replay before reuse. May be
 *                              `NULL` if the caller does not need the reason.
 * @return Connection, or NULL if none available
 */
backend_conn_t* backend_pool_borrow_with_stmts(backend_pool_t* pool,
                                                uint64_t required_state_hash,
                                                uint64_t required_stmt_hash,
                                                bool* out_needs_replay);

/**
 * @brief Borrow a connection with profile-aware matching (spec §6)
 *
 * Search order:
 *   1. exact profile match in idle_list (hash equality)
 *   2. clean connection from clean_list
 *   3. any idle connection (will need sync)
 *   4. dirty connection (will need full cleanup + sync)
 *   5. NULL if pool exhausted
 *
 * @param pool    Pool to borrow from
 * @param profile Desired state profile (NULL = clean)
 * @return Connection, or NULL if none available
 */
backend_conn_t* backend_pool_borrow_profiled(backend_pool_t* pool,
                                              const struct state_profile* profile);

/**
 * @brief Borrow or get pinned connection for a session
 *
 * If the session already has a pinned connection, returns that.
 * Otherwise, borrows a new connection and pins it to the session.
 *
 * @param pool Pool to borrow from
 * @param session Session pointer (used as pin key)
 * @return Connection, or NULL if none available
 */
backend_conn_t* backend_pool_borrow_pinned(backend_pool_t* pool, void* session);

/**
 * @brief Return a connection to the pool
 *
 * If in_transaction is true, connection stays pinned. Otherwise, clean
 * connections return to an idle list immediately and dirty/stateful
 * connections enter reactor-owned cleanup before becoming borrowable.
 *
 * @param pool Pool to return to
 * @param conn Connection to return
 * @param in_transaction Whether a transaction is still open
 */
void backend_pool_return(backend_pool_t* pool, backend_conn_t* conn, bool in_transaction);

/**
 * @brief Discard a borrowed connection that is no longer usable
 *
 * Call this when a connection obtained via backend_pool_borrow() has been
 * closed due to an error (e.g. a failed scatter-write command).  The caller
 * must close the fd and set conn->fd = -1 before calling this.  This
 * function atomically transitions the connection from ACTIVE → CLOSED and
 * decrements pool->active_count so the pool can refill the slot.
 *
 * Calling this on a connection that is not in ACTIVE state is a safe no-op.
 *
 * @param pool Pool the connection belongs to
 * @param conn Connection to discard
 */
void backend_pool_discard(backend_pool_t* pool, backend_conn_t* conn);

/**
 * @brief Close a backend slot and emit exactly one close reason for this close.
 *
 * The function is idempotent while a slot is already CLOSED.
 */
void backend_pool_close_connection(backend_pool_t* pool,
                                   backend_conn_t* conn,
                                   backend_close_reason_t reason);

/**
 * @brief Set or clear backend quarantine reason.
 */
void backend_pool_set_quarantine(backend_conn_t* conn,
                                 backend_quarantine_reason_t reason);

/**
 * @brief Supervise connections in CLEANING state
 *
 * Cleanup send/drain I/O is owned by reactor callbacks. This periodic
 * supervisor only enforces timeouts and re-arms stalled cleanup operations.
 *
 * @param pool Pool
 * @return Number of cleanup slots closed by the supervisor
 */
size_t backend_pool_drain_cleaning(backend_pool_t* pool);

/**
 * @brief Release all connections for a session
 *
 * Called when a client disconnects. Unsafe pinned connections are closed and
 * replaced asynchronously rather than synchronously cleaned on the worker path.
 *
 * @param pool Pool
 * @param session Session being released
 */
void backend_pool_release_session(backend_pool_t* pool, void* session);

/* ============================================================================
 * Waiting Queue
 * ============================================================================ */

/**
 * @brief Queue a session to wait for a connection
 *
 * When a connection becomes available, the wait_callback will be called.
 *
 * @param pool Pool
 * @param session Session to queue
 * @param userdata User data for callback
 * @return 0 on success, -1 if queue is full
 */
int backend_pool_queue_wait(backend_pool_t* pool, void* session, void* userdata);

/**
 * @brief Cancel queued waits for a session.
 *
 * Used when a client disconnects while sitting in the bounded pool wait queue.
 * Removes all matching waiters without invoking the wait callback, so the
 * callback cannot accidentally borrow a backend for a dead session.
 *
 * @param pool Pool
 * @param session Session pointer used when enqueued
 * @return Number of removed waiters
 */
size_t backend_pool_cancel_wait(backend_pool_t* pool, void* session);

/**
 * @brief Set the callback for when connections become available
 *
 * @param pool Pool
 * @param callback Callback function
 */
void backend_pool_set_wait_callback(backend_pool_t* pool, backend_pool_wait_cb callback);

/* ============================================================================
 * State Management
 * ============================================================================ */

/**
 * @brief Mark a connection as in/out of transaction
 *
 * Call with begin=true after BEGIN, begin=false after COMMIT/ROLLBACK.
 *
 * @param pool Pool
 * @param conn Connection
 * @param begin Whether entering (true) or exiting (false) transaction
 */
void backend_pool_mark_transaction(backend_pool_t* pool, backend_conn_t* conn, bool begin);

/**
 * @brief Update the state hash for a connection
 *
 * Called after SET commands to track connection state.
 *
 * @param pool Pool
 * @param conn Connection
 * @param hash New state hash
 */
void backend_pool_update_state_hash(backend_pool_t* pool, backend_conn_t* conn, uint64_t hash);

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Get pool statistics
 *
 * Aggregates the pool's current counters into a caller-provided structure.
 * The pointer is an output channel rather than ownership transfer; the caller
 * allocates the storage and retains it after the call returns.
 *
 * @param pool Pool.
 * @param[out] stats Caller-provided statistics buffer to populate.
 * @return
 */
void backend_pool_get_stats(backend_pool_t* pool, backend_pool_stats_t* stats);

/**
 * @brief Prune idle connections that have exceeded idle_timeout
 *
 * Closes connections that have been idle longer than idle_timeout_ms,
 * but keeps at least min_connections alive in the pool.
 * Call this periodically (e.g., every 30 seconds) from a timer.
 *
 * @param pool Pool to prune
 * @return Number of connections closed
 */
size_t backend_pool_prune_idle(backend_pool_t* pool);

/**
 * @brief Close connections older than max_connection_age_ms.
 *
 * Iterates all connections; any IDLE connection whose created_at + max_age
 * is in the past is CAS-closed and the fd shut down.  Respects min_connections.
 *
 * @param pool Pool to prune
 * @return Number of connections closed
 */
size_t backend_pool_prune_aged(backend_pool_t* pool);

/**
 * @brief Check whether a user is allowed to acquire another connection.
 *
 * Returns true if the user has not reached max_user_connections (or if
 * max_user_connections == 0, i.e. unlimited).
 */
bool backend_pool_user_can_acquire(backend_pool_t* pool, const char* user);

/**
 * @brief Increment the per-user active connection count.
 */
void backend_pool_user_conn_acquire(backend_pool_t* pool, const char* user);

/**
 * @brief Decrement the per-user active connection count.
 */
void backend_pool_user_conn_release(backend_pool_t* pool, const char* user);

/**
 * @brief Reconnect ONE closed backend connection and wake a waiter
 *
 * Called from a fast timer when there are closed slots needing reconnection.
 * Only reconnects one connection per call to limit event-loop blocking.
 *
 * @param pool Pool to refill
 * @return 1 if a connection was restored, 0 if nothing to do
 */
int backend_pool_refill_one(backend_pool_t* pool);

/**
 * @brief Kick async connects to populate min_connections after reactor init.
 *
 * Called once after the reactor is wired into the pool.  Replaces the old
 * synchronous pre-connect loop — all connections are established via
 * io_uring (zero poll() calls).
 *
 * @param pool Pool to warm up
 */
void backend_pool_async_warmup(backend_pool_t* pool);

/**
 * @brief Update the backend host/port that this pool connects to.
 *
 * Called after failover to redirect reconnection attempts to the new server.
 * Only affects the pool's config — existing open connections are left alone
 * and drain naturally as they are returned.
 *
 * @param pool Pool to update
 * @param host New backend host (must outlive the pool — e.g. from server_pool)
 * @param port New backend port
 */
void backend_pool_update_target(backend_pool_t* pool, const char* host, uint16_t port);

/**
 * @brief Close all idle/clean/dirty connections in the pool.
 *
 * Called after failover to discard connections to the old server role.
 * Active (borrowed) connections are left alone — they will be closed
 * when returned.  This accelerates refilling from the correct server.
 *
 * @param pool Pool to drain
 * @return Number of connections closed
 */
size_t backend_pool_drain_idle(backend_pool_t* pool);

/**
 * @brief Check if the pool has any available connections.
 *
 * @param pool Pool to check
 * @return true if at least one idle/clean connection exists
 */
bool backend_pool_has_available(backend_pool_t* pool);

/**
 * @brief Expire waiters that have exceeded wait_timeout_ms.
 *
 * Removes waiters from the front of the queue whose enqueue_time_ms + timeout
 * has elapsed.  Expired waiters are invoked with userdata=NULL to signal timeout.
 *
 * @param pool Pool to check
 * @return Number of waiters expired
 */
size_t backend_pool_expire_waiters(backend_pool_t* pool);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_BACKEND_POOL_H */
