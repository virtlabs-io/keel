/**
 * @file worker.h
 * @brief Public API for per-core workers, timer wheels, and session ownership.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Each worker thread owns:
 *   - One reactor instance (io_uring ring or kqueue)
 *   - A slab of sessions
 *   - A pool of pipes (for splice on Linux)
 *   - A timer wheel for timeout management
 *
 * Thread Model:
 *   - Shared-nothing: workers don't share mutable state
 *   - Connections are affined to workers via accept()
 *   - Cross-worker communication via lock-free queues (if needed)
 *
 * Worker Loop:
 *   1. Submit pending I/O operations
 *   2. Wait for completions (with timeout)
 *   3. Process completions
 *   4. Check timers
 *   5. Repeat
 */

#ifndef KEEL_WORKER_H
#define KEEL_WORKER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>

#include "keel/engine/engine.h"
#include "keel/session/session.h"
#include "keel/session/admission.h"
#include "keel/reactor/reactor.h"
#include "keel/mem/mem.h"
#include "keel/engine/migration.h"
#include "keel/engine/catchup.h"
#include "keel/core/query_cache.h"
#include "keel/core/auth.h"

/* ============================================================================
 * Compile-time constants
 * ============================================================================
 * KEEL_RECV_BUF_SIZE: per-session I/O buffer size embedded in recv_context_t.
 * Changing this value changes the struct size and therefore the pool slot size.
 * Override at cmake configure time:  -DKEEL_RECV_BUF_SIZE=131072
 */
#ifndef KEEL_RECV_BUF_SIZE
#define KEEL_RECV_BUF_SIZE 65536
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
struct keel_engine;
struct backend_pool;

/* ============================================================================
 * Timer Wheel
 * ============================================================================
 * Hierarchical timer wheel for efficient timeout management.
 * O(1) insertion and deletion, O(n) tick where n is expired timers.
 */

#define KEEL_TIMER_WHEEL_BITS    8
#define KEEL_TIMER_WHEEL_SIZE    (1 << KEEL_TIMER_WHEEL_BITS)
#define KEEL_TIMER_WHEEL_MASK    (KEEL_TIMER_WHEEL_SIZE - 1)
#define KEEL_TIMER_WHEEL_LEVELS  4   /* 256^4 = ~49 days range */

typedef struct keel_timer_entry {
    struct keel_timer_entry* next;
    struct keel_timer_entry* prev;
    uint64_t                deadline;   /* Absolute deadline (ns) */
    void*                   userdata;
    void (*callback)(void* userdata);
    uint8_t                 wheel_level;  /* Level in the wheel (0-3) */
    uint16_t                wheel_slot;   /* Slot index within the level */
} keel_timer_entry_t;

typedef struct keel_timer_wheel {
    keel_timer_entry_t*  slots[KEEL_TIMER_WHEEL_LEVELS][KEEL_TIMER_WHEEL_SIZE];
    uint64_t            current_tick;
    uint64_t            tick_ns;        /* Nanoseconds per tick */
    size_t              count;          /* Total active timers */
    uint64_t            min_deadline;   /* O(1) next-deadline hint */
    bool                min_dirty;      /* true if min_deadline may be stale */
} keel_timer_wheel_t;

/**
 * @brief Initialize a timer wheel
 * @param wheel Timer wheel to initialize
 * @param tick_ms Milliseconds per tick (resolution)
 */
void keel_timer_wheel_init(keel_timer_wheel_t* wheel, uint32_t tick_ms);

/**
 * @brief Add a timer
 * @param wheel Timer wheel
 * @param entry Timer entry (caller owns memory)
 * @param delay_ms Delay in milliseconds
 * @param userdata Callback context
 * @param callback Expiration callback
 */
void keel_timer_wheel_add(
    keel_timer_wheel_t* wheel,
    keel_timer_entry_t* entry,
    uint32_t delay_ms,
    void* userdata,
    void (*callback)(void* userdata)
);

/**
 * @brief Cancel a timer
 * @param wheel Timer wheel
 * @param entry Timer entry to cancel
 */
void keel_timer_wheel_cancel(keel_timer_wheel_t* wheel, keel_timer_entry_t* entry);

/**
 * @brief Advance time and fire expired timers
 * @param wheel Timer wheel
 * @param now_ns Current time in nanoseconds
 * @return Number of timers fired
 */
size_t keel_timer_wheel_tick(keel_timer_wheel_t* wheel, uint64_t now_ns);

/**
 * @brief Get next deadline
 * @param wheel Timer wheel
 * @return Next deadline in ns, or UINT64_MAX if no timers
 */
uint64_t keel_timer_wheel_next_deadline(keel_timer_wheel_t* wheel);

/* ============================================================================
 * Pipe Pool (Linux only)
 * ============================================================================
 * Pre-created pipes for zero-copy splice operations.
 */

typedef struct keel_pipe_pool {
    keel_pipe_t*     pipes;
    size_t          capacity;
    size_t          available;
    keel_pipe_t**    free_stack;     /* Stack of free pipes */
    size_t          free_count;
} keel_pipe_pool_t;

/**
 * @brief Initialize pipe pool
 * @param pool Pool to initialize
 * @param capacity Number of pipes to create
 * @return 0 on success, -1 on error
 */
int keel_pipe_pool_init(keel_pipe_pool_t* pool, size_t capacity);

/**
 * @brief Acquire a pipe from the pool
 * @param pool Pipe pool
 * @return Pipe or NULL if pool exhausted
 */
keel_pipe_t* keel_pipe_pool_acquire(keel_pipe_pool_t* pool);

/**
 * @brief Return a pipe to the pool
 * @param pool Pipe pool
 * @param pipe Pipe to return
 */
void keel_pipe_pool_release(keel_pipe_pool_t* pool, keel_pipe_t* pipe);

/**
 * @brief Destroy pipe pool
 * @param pool Pool to destroy
 */
void keel_pipe_pool_destroy(keel_pipe_pool_t* pool);

/* ============================================================================
 * Worker State
 * ============================================================================ */

typedef enum keel_worker_state {
    KEEL_WORKER_INIT = 0,
    KEEL_WORKER_STARTING,
    KEEL_WORKER_RUNNING,
    KEEL_WORKER_STOPPING,
    KEEL_WORKER_STOPPED,
} keel_worker_state_t;

/* ============================================================================
 * Worker Structure
 * ============================================================================ */

typedef struct keel_worker {
    /* Identity */
    uint32_t            id;             /* Worker index (0-based) */
    pthread_t           thread;         /* Worker thread */
    keel_worker_state_t  _Atomic state;          /* Current state */
    
    /* Parent engine */
    struct keel_engine*  engine;
    
    /* Reactor (owns one reactor per worker) */
    keel_reactor_t*      reactor;
    
    /* Sessions (slab allocator) */
    keel_session_slab_t  sessions;
    
    /* Pipes for splice (Linux only) */
    keel_pipe_pool_t     pipes;
    
    /* Timers */
    keel_timer_wheel_t   timers;
    
    /* Listen socket (shared, but accept is distributed) */
    int                 listen_fd;
    
    /* Control */
    _Atomic bool        should_stop;
    _Atomic bool        draining;       /* Stop accepting, drain sessions */
    bool                accept_rearm_needed;  /* Accept SQE needs retry */
    int                 eventfd;        /* For wakeup signals */
    uint64_t            wakeup_buf;     /* Receive buffer for wakeup eventfd read */
    
    /* CPU affinity */
    int                 cpu_affinity;   /* CPU core to pin to, -1 = none */
    
    /* Instrumentation (points into collector's per-worker context) */
    struct keel_stats_ctx *stats_ctx;
    
    /* Statistics */
    struct {
        uint64_t        loops;
        uint64_t        accepts;
        uint64_t        sessions_created;
        uint64_t        sessions_closed;
        uint64_t        bytes_recv;
        uint64_t        bytes_sent;
        uint64_t        bytes_spliced;
        uint64_t        timeouts;
        uint64_t        errors;
        /* Per-worker round-robin counters (avoid cross-core atomic bouncing) */
        uint64_t        rr_read_counter;
        uint64_t        rr_write_counter;
        uint64_t        rr_any_counter;
        /* stale_read_policy=wait observability (Patch 2d-4):
         *   wait_catchup_consulted_total      — router was consulted on a
         *                                       token-bearing replica-eligible read
         *   wait_catchup_degraded_to_primary  — router said WAIT_CATCHUP but
         *                                       the engine could not async-park
         *                                       in this build, so the read was
         *                                       degraded to the primary. v0.5-alpha
         *                                       limitation; v0.5-beta will replace
         *                                       this with `keel_engine_consult_catchup`
         *                                       + a resume continuation. */
        uint64_t        wait_catchup_consulted_total;
        uint64_t        wait_catchup_degraded_to_primary;
    } stats;
    
    /* Backend configuration (from engine config) */
    const char*         backend_host;
    uint16_t            backend_port;
    const char*         backend_user;
    const char*         backend_password;
    const char*         backend_database;
    const char*         backend_protocol;    /* "postgres" or "mysql" */
    
    /* Server pool for read/write splitting */
    keel_server_pool_t*  server_pool;    /* Pointer to engine's server pool */
    
    /* Backend connection pools — one pool per configured server.
     * Indexed identically to server_pool->servers[], so
     * server_pools[i] is the pool for server_pool->servers[i].
     * Routing selects a server by role (RW/RO/WO), then borrows
     * from the corresponding pool. */
    struct backend_pool** server_pools;  /* [server_pool->count] */
    size_t              server_pool_count;
    
    /* Pool prune timer (for idle connection cleanup) */
    keel_timer_entry_t   pool_prune_timer;
    
    /* Pool refill timer (for reconnecting closed slots quickly) */
    keel_timer_entry_t   pool_refill_timer;

    /* Connection rebalance timer (for automatic load-based migration) */
    keel_timer_entry_t   rebalance_timer;

    /* Reactor-owned replica catch-up wait list (Phase 2). Owned by this
     * worker; touched only from this thread's reactor and timer ticks.
     * NULL until keel_worker_init() succeeds; remains NULL when the
     * feature is disabled. */
    keel_catchup_manager_t* catchup;
    keel_timer_entry_t      catchup_tick_timer;

    /* Configurable operational parameters (propagated from keel_engine_config_t
     * at worker init; stored here so hot-path code never touches the config). */
    uint32_t    idle_timeout_ms;           /**< Session idle reap timeout (ms) */
    uint32_t    pool_prune_interval_ms;    /**< Pool idle-conn prune interval (ms) */
    uint32_t    pool_refill_interval_ms;   /**< Pool reconnect poll interval (ms) */
    uint32_t    pool_refill_backoff_ms;    /**< Poll interval when pool is full (ms) */
    uint32_t    hotpath_instr_mask;        /**< KEEL_HOT_INSTR_* runtime gates */

    /* Runtime mode tier (from engine config) */
    keel_tier_t      runtime_mode;          /**< proxy/pool/smart/full feature gating */

    /* Prepared-statement pooling strategy (from engine config) */
    keel_ps_mode_t  ps_mode;               /**< How to handle named prepared statements */

    /* Replication uncertainty tracking */
    bool            txn_tracking;          /**< transaction_tracking = on */

    /* Zero-copy fast network path */
    bool            fast_network_path;     /**< fast_network_path = on (peek+splice bypass) */

    /* Result cache — disables splice bypass for cacheable queries */
    bool            result_cache;          /**< result_cache = on */
    keel_query_cache_t* query_cache;       /**< live cache instance (NULL when disabled) */

    /* Sticky-primary TTL (0 = disabled) */
    uint32_t        sticky_primary_ttl_ms; /**< ms to force reads to primary after a write (0 = off) */

    /* Per-session and per-backend buffer caps (0 = unlimited) */
    size_t          session_max_buffered_bytes; /**< max FE message size; 0 = unlimited */
    size_t          backend_max_replay_bytes;   /**< max PS replay buffer; 0 = unlimited */

    /* TLS configuration (frontend: client→proxy, backend: proxy→database).
     * Copied from engine_config at worker init; worker accesses these on
     * every connection accept to decide whether to start TLS handshake. */
    keel_tls_config_t   tls_config;         /**< Frontend TLS (copy of engine cfg) */
    keel_tls_config_t   backend_tls_config; /**< Backend TLS (copy of engine cfg) */

    /* Shard router — optional, set from engine config.
     * When non-NULL, the hot path calls keel_router_dispatch_sql() to
     * route queries to the correct shard (single dispatch) or fan out
     * to all shards (scatter-merge dispatch). */
    keel_router_t*      router;             /**< Shard router (not owned) */
    size_t              scatter_merge_max_mem_bytes; /**< Scatter merge memory limit */
    const char*         scatter_merge_spill_dir;     /**< Scatter merge spill directory */

    /* Per-session recv context pool — eliminates per-connection malloc/free.
     * Each recv_context_t is ~131KB (two 64KB I/O buffers + flow state).
     * Pre-allocated at worker init, O(1) alloc/free via free-list. */
    keel_pool_t*         recv_ctx_pool;

    /* Cached hook registry pointer — set once at worker init from the engine.
     * Avoids an indirect call through keel_engine_get_hook_registry() on
     * every query.  The registry is owned by the engine and lives for the
     * lifetime of the process; the cached pointer is always valid. */
    keel_hook_registry_t* hooks;
    uint32_t             hook_mask;  /* bitmask of active hook points (cached) */

    /* Audit log — pointer to the engine-global audit log instance.
     * NULL when audit logging is disabled.  Passed to scatter and other
     * subsystems that emit security-relevant events.
     * Forward-declared here; include <keel/log/audit_log.h> when calling
     * keel_audit_emit_*() functions. */
    struct keel_audit_log* audit_log;
    
    /* Padding to prevent false sharing between workers */
    char                _padding[64];

    /* Connection migration channel — inbound from other workers */
    keel_worker_migration_t migration;

    /* Per-worker admission controller — enforces max_clients / max_pool_size
     * limits.  Zero limits mean "unbounded" (the default).  Initialised once
     * at worker start from the engine config; updated on SIGHUP. */
    keel_admission_t        admission;

    /**
     * Per-worker authentication manager.
     *
     * Owns the auth provider(s) configured for this listener
     * (SCRAM-SHA-256, LDAP, PAM, cert, or auth_query).  Created once in
     * keel_worker_init() from keel_engine_config_t and destroyed in
     * keel_worker_cleanup().  NULL in trust mode.
     *
     * The pointer is copied into every pg_flow_ctx_t that this worker
     * creates so the per-connection auth exchange can call
     * keel_auth_manager_start() / keel_auth_process() without touching
     * the engine config on the hot path.
     */
    keel_auth_manager_t*    auth_manager;
} keel_worker_t;

/* ============================================================================
 * Worker Lifecycle
 * ============================================================================ */

/**
 * @brief Initialize a worker
 * @param worker Worker to initialize
 * @param engine Parent engine
 * @param id Worker index
 * @param listen_fd Listening socket
 * @return 0 on success, -1 on error
 */
int keel_worker_init(
    keel_worker_t* worker,
    struct keel_engine* engine,
    uint32_t id,
    int listen_fd
);

/**
 * @brief Start the worker thread
 * @param worker Worker to start
 * @return 0 on success, -1 on error
 */
int keel_worker_start(keel_worker_t* worker);

/**
 * @brief Signal worker to stop
 * @param worker Worker to stop
 */
void keel_worker_stop(keel_worker_t* worker);

/**
 * @brief Enter drain mode: stop accepting new connections but keep processing
 *        existing sessions until they complete naturally.
 *
 * Sets the atomic draining flag. The worker's event loop will stop re-arming
 * accept and will set should_stop once all sessions close.
 *
 * @param worker Worker to drain.
 */
void keel_worker_drain(keel_worker_t* worker);

/**
 * @brief Return the number of currently allocated sessions.
 *
 * @param worker Worker to query.
 * @return Number of active sessions in the worker's slab.
 */
size_t keel_worker_active_sessions(const keel_worker_t* worker);

/**
 * @brief Wait for worker to finish
 * @param worker Worker to join
 * @return 0 on success, -1 on error
 */
int keel_worker_join(keel_worker_t* worker);

/**
 * @brief Cleanup worker resources
 * @param worker Worker to cleanup
 */
void keel_worker_cleanup(keel_worker_t* worker);

/* ============================================================================
 * Worker Operations (called from event callbacks)
 * ============================================================================ */

/**
 * @brief Handle a new connection accept
 * @param worker Worker handling the accept
 * @param client_fd New client file descriptor
 */
void keel_worker_on_accept(keel_worker_t* worker, int client_fd);

/**
 * @brief Attempt to migrate an idle session to a less-loaded worker.
 *
 * Checks keel_migration_can_migrate(), selects the least-loaded peer,
 * and calls keel_migration_send().  On success the caller must remove
 * the session from its own state (the function closes the source session
 * slot and decrements session stats on the source worker).
 *
 * @param worker  Source worker.
 * @param session Session to migrate (must be KEEL_SESSION_READY).
 * @return 0  Migration succeeded — session is now owned by another worker.
 * @return -1 Migration not possible (caller retains session).
 */
int keel_worker_migrate_session(keel_worker_t* worker, keel_session_t* session);

/**
 * @brief Handle data received on client socket
 * @param worker Worker
 * @param session Session
 * @param buf Data received
 * @param len Bytes received
 */
void keel_worker_on_client_recv(
    keel_worker_t* worker,
    keel_session_t* session,
    const void* buf,
    size_t len
);

/**
 * @brief Handle data received on server socket
 * @param worker Worker
 * @param session Session
 * @param buf Data received
 * @param len Bytes received
 */
void keel_worker_on_server_recv(
    keel_worker_t* worker,
    keel_session_t* session,
    const void* buf,
    size_t len
);

/**
 * @brief Handle connection close
 * @param worker Worker
 * @param session Session
 * @param from_client True if client closed, false if server closed
 */
void keel_worker_on_close(
    keel_worker_t* worker,
    keel_session_t* session,
    bool from_client
);

/**
 * @brief Handle session timeout
 * @param worker Worker
 * @param session Session
 */
void keel_worker_on_timeout(keel_worker_t* worker, keel_session_t* session);

/* ============================================================================
 * Worker Pool Management
 * ============================================================================ */

typedef struct keel_worker_pool {
    keel_worker_t*   workers;
    size_t          count;
    size_t          running;
    pthread_mutex_t lock;
} keel_worker_pool_t;

/**
 * @brief Initialize worker pool
 * @param pool Pool to initialize
 * @param count Number of workers
 * @param engine Parent engine
 * @param listen_fd Listening socket
 * @return 0 on success, -1 on error
 */
int keel_worker_pool_init(
    keel_worker_pool_t* pool,
    size_t count,
    struct keel_engine* engine,
    int listen_fd
);

/**
 * @brief Start all workers
 * @param pool Worker pool
 * @return 0 on success, -1 on error
 */
int keel_worker_pool_start(keel_worker_pool_t* pool);

/**
 * @brief Stop all workers
 * @param pool Worker pool
 */
void keel_worker_pool_stop(keel_worker_pool_t* pool);

/**
 * @brief Wait for all workers to finish
 * @param pool Worker pool
 */
void keel_worker_pool_join(keel_worker_pool_t* pool);

/**
 * @brief Destroy worker pool
 * @param pool Pool to destroy
 */
void keel_worker_pool_destroy(keel_worker_pool_t* pool);

/**
 * @brief Get aggregate statistics
 *
 * Reads each worker's reactor counters and accumulates them into a caller-
 * owned output structure. The output pointer is part of the function's return
 * contract even though the function itself returns `void`.
 *
 * @param pool Worker pool.
 * @param[out] stats Caller-provided statistics structure that receives the
 *                   aggregate sum across all workers.
 * @return
 */
void keel_worker_pool_get_stats(keel_worker_pool_t* pool, keel_reactor_stats_t* stats);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_WORKER_H */
