/**
 * @file engine.h
 * @brief Public API for the engine core, worker lifecycle, and routing topology.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This header defines the fundamental types for the high-performance
 * database proxy engine. The engine is designed to be:
 *   - Protocol agnostic (supports PostgreSQL and MySQL via vtables)
 *   - Platform optimized (io_uring on Linux, kqueue on macOS)
 *   - Zero-copy where possible (splice on Linux)
 *
 * Architecture:
 *   ┌─────────────────────────────────────────────────────────┐
 *   │                    Engine (Worker Loop)                  │
 *   ├─────────────────────────────────────────────────────────┤
 *   │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │
 *   │  │   Reactor   │  │   Session   │  │  Protocol VTable │ │
 *   │  │ (io_uring/  │  │   Manager   │  │  (PG/MySQL)      │ │
 *   │  │  kqueue)    │  │   (Slab)    │  │                  │ │
 *   │  └─────────────┘  └─────────────┘  └─────────────────┘ │
 *   └─────────────────────────────────────────────────────────┘
 */

#ifndef KEEL_ENGINE_H
#define KEEL_ENGINE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/tls_context.h"
#include "keel/protocol/protocol.h"
#include "keel/core/stats.h"
#include "keel/engine/runtime_mode.h"
#include "keel/trace/trace.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declaration — see keel_hook.h */
typedef struct keel_hook_registry keel_hook_registry_t;

/* Forward declaration — see keel/core/router.h */
typedef struct keel_router keel_router_t;

/* ============================================================================
 * Operation Types
 * ============================================================================
 * These represent the low-level I/O operations that the reactor can perform.
 * The engine translates high-level protocol actions into these ops.
 */

/**
 * @brief Low-level I/O operations submitted to the reactor layer.
 */
typedef enum keel_op_type {
    KEEL_OP_NONE = 0,
    KEEL_OP_ACCEPT,      /* Accept new connection */
    KEEL_OP_RECV,        /* Receive data into buffer */
    KEEL_OP_SEND,        /* Send data from buffer */
    KEEL_OP_PEEK,        /* Peek at data without consuming (MSG_PEEK) */
    KEEL_OP_SPLICE,      /* Zero-copy data movement (Linux only) */
    KEEL_OP_CONNECT,     /* Connect to backend server */
    KEEL_OP_CLOSE,       /* Close file descriptor */
    KEEL_OP_TIMEOUT,     /* Timer expiration */
} keel_op_type_t;

/**
 * @brief Proxy operation descriptor
 *
 * This structure describes a single I/O operation to be submitted
 * to the reactor. Operations can be linked (chained) for atomic
 * sequences like "peek header then splice body".
 */
typedef struct keel_op {
    keel_op_type_t   type;           /* Operation type */
    int             fd_in;          /* Source file descriptor */
    int             fd_out;         /* Destination file descriptor (for splice) */
    void*           buf;            /* Buffer for recv/send */
    size_t          len;            /* Bytes to transfer */
    size_t          offset;         /* Offset into buffer or file */
    uint32_t        flags;          /* Operation-specific flags */
    void*           userdata;       /* Callback context (usually session_t*) */
    
    /* Completion callback */
    void (*callback)(void* userdata, int result);
    
    /* Linking for chained operations */
    struct keel_op*  next;           /* Next op in chain (NULL if last) */
} keel_op_t;

/* Operation flags */
#define KEEL_OP_FLAG_LINKED      (1 << 0)  /* Part of a linked chain */
#define KEEL_OP_FLAG_MULTISHOT   (1 << 1)  /* Multi-shot (e.g., accept) */
#define KEEL_OP_FLAG_FIXED_BUF   (1 << 2)  /* Use registered buffer */

/* ============================================================================
 * Movement Modes
 * ============================================================================
 * These modes determine how the engine handles data for a session.
 * The protocol vtable tells the engine which mode to use.
 */

typedef enum keel_mode {
    KEEL_MODE_STARTUP = 0,   /* Initial connection handshake */
    KEEL_MODE_PEEK,          /* Inspecting packet header */
    KEEL_MODE_ANALYZE,       /* Full packet in user-space for parsing */
    KEEL_MODE_PIPE,          /* Zero-copy splice (fast path) */
    KEEL_MODE_STREAM,        /* Pass-through until end-of-stream */
    KEEL_MODE_CLOSING,       /* Connection teardown */
} keel_mode_t;

/* ============================================================================
 * Direction
 * ============================================================================ */

typedef enum keel_direction {
    KEEL_DIR_CLIENT_TO_SERVER = 0,
    KEEL_DIR_SERVER_TO_CLIENT,
} keel_direction_t;

/* ============================================================================
 * Backend Selection
 * ============================================================================ */

typedef enum keel_reactor_type {
    KEEL_REACTOR_AUTO = 0,   /* Auto-detect best available */
    KEEL_REACTOR_IOURING,    /* Linux io_uring */
    KEEL_REACTOR_KQUEUE,     /* macOS/BSD kqueue */
    KEEL_REACTOR_EPOLL,      /* Linux epoll (fallback) */
} keel_reactor_type_t;

/* ============================================================================
 * Backend Server Configuration
 * ============================================================================ */

typedef enum keel_server_role {
    KEEL_SERVER_ROLE_RW = 0,         /* Read-write node (accepts reads and writes) */
    KEEL_SERVER_ROLE_RO,             /* Read-only node (accepts reads only) */
    KEEL_SERVER_ROLE_WO,             /* Write-only node (accepts writes only) */
    KEEL_SERVER_ROLE_AUTO,           /* Auto-detect via probe */
} keel_server_role_t;

/* Backward-compatible aliases for the old primary/replica terminology */
#define KEEL_SERVER_ROLE_PRIMARY  KEEL_SERVER_ROLE_RW
#define KEEL_SERVER_ROLE_REPLICA  KEEL_SERVER_ROLE_RO

#define KEEL_MAX_SERVERS 16          /* Maximum servers per pool */

typedef struct keel_backend_server {
    const char*         host;
    uint16_t            port;
    const char*         user;
    const char*         password;
    const char*         database;
    const char*         probe_user;      /* Probe auth user (fallbacks to user) */
    const char*         probe_password;  /* Probe auth password (fallbacks to password) */
    const char*         probe_auth;      /* Probe auth mode: auto|trust|password|md5|scram */
    keel_server_role_t   role;
    uint32_t            weight;     /* Load balancing weight */
    uint32_t            shard_id;   /* Logical shard bucket (for sharded routing) */
    bool                healthy;    /* Health status from probes */
    bool                dynamic;    /* true = strings are heap-allocated (ADD SERVER) */
} keel_backend_server_t;

typedef struct keel_server_pool {
    keel_backend_server_t    servers[KEEL_MAX_SERVERS];
    size_t                  count;

    /* Role-indexed arrays: indices into servers[] for fast lookup */
    size_t                  rw_indices[KEEL_MAX_SERVERS];  /* Servers accepting writes AND reads */
    size_t                  rw_count;
    size_t                  ro_indices[KEEL_MAX_SERVERS];  /* Read-only servers */
    size_t                  ro_count;
    size_t                  wo_indices[KEEL_MAX_SERVERS];  /* Write-only servers */
    size_t                  wo_count;
    size_t                  next_read;          /* Round-robin for read routing */
    size_t                  next_write;         /* Round-robin for write routing */

    /* Shard-id to server-index mapping.
     * shard_pool_index[shard_id] = index into servers[] for that shard.
     * SIZE_MAX means no server is registered for that shard_id. */
    size_t                  shard_pool_index[KEEL_MAX_SERVERS]; /* shard_id -> server index */
    size_t                  shard_count;       /* highest shard_id + 1 (valid range) */
} keel_server_pool_t;

/**
 * @brief Rebuild the role-index arrays from servers[].role values.
 *
 * Must be called after any change to server roles (init, failover, probe).
 */
/**
 * @brief Rebuild per-role index arrays after server-role changes.
 *
 * @param sp Server-pool topology to rebuild.
 * @return
 */
static inline void keel_server_pool_rebuild_indices(keel_server_pool_t* sp) {
    sp->rw_count = 0;
    sp->ro_count = 0;
    sp->wo_count = 0;
    /* Reset shard mapping */
    sp->shard_count = 0;
    for (size_t k = 0; k < KEEL_MAX_SERVERS; k++)
        sp->shard_pool_index[k] = (size_t)-1;
    for (size_t i = 0; i < sp->count; i++) {
        switch (sp->servers[i].role) {
        case KEEL_SERVER_ROLE_RW:
            sp->rw_indices[sp->rw_count++] = i;
            break;
        case KEEL_SERVER_ROLE_RO:
            sp->ro_indices[sp->ro_count++] = i;
            break;
        case KEEL_SERVER_ROLE_WO:
            sp->wo_indices[sp->wo_count++] = i;
            break;
        case KEEL_SERVER_ROLE_AUTO:
            /* AUTO servers are not indexed until probe resolves their role */
            break;
        }
        /* Build shard_id -> pool_index mapping */
        uint32_t sid = sp->servers[i].shard_id;
        if (sid < KEEL_MAX_SERVERS) {
            sp->shard_pool_index[sid] = i;
            if ((size_t)(sid + 1) > sp->shard_count)
                sp->shard_count = (size_t)(sid + 1);
        }
    }
}

/* ============================================================================
 * Engine Configuration
 * ============================================================================ */

typedef struct keel_engine_config {
    /* Reactor settings */
    keel_reactor_type_t  reactor_type;
    uint32_t            queue_depth;        /* SQ/CQ depth for io_uring */
    bool                use_buf_rings;      /* io_uring buf rings for recv (Linux 5.19+) */
    uint32_t            buf_ring_size;      /* Buf ring slots (0 = queue_depth) */
    bool                sqpoll;             /* io_uring SQ polling (kernel thread) */
    uint32_t            sqpoll_idle_ms;     /* SQ poll thread idle timeout */
    
    /* Worker settings */
    uint32_t            num_workers;        /* 0 = one per CPU core */
    bool                pin_workers;        /* Pin threads to CPU cores */
    
    /* Memory settings */
    size_t              slab_buffer_size;   /* Per-buffer size (default 64KB) */
    size_t              session_pool_size;  /* Pre-allocated sessions per worker */
    size_t              pipe_pool_size;     /* Pre-created pipes per worker (Linux) */

    /* Admission control — max concurrent frontend connections per worker.
     * 0 means unlimited.  Set to max_clients / num_workers from config. */
    uint32_t            max_clients_per_worker;

    /* Timeouts */
    uint32_t            idle_timeout_ms;    /* Reap sessions after idle */
    uint32_t            connect_timeout_ms; /* Backend connect timeout */
    uint64_t            pool_idle_timeout_ms;    /* Close idle pool conns after this (0=never) */
    uint64_t            pool_max_connection_age_ms; /* Force-recycle pool conn after this age (0=never) */
    
    /* Protocol */
    const char*         default_protocol;   /* "postgres" or "mysql" */
    
    /* Server pool - multiple backends with read/write splitting */
    keel_server_pool_t   server_pool;
    
    /* Connection pool sizing (from config: min_pool_size / max_pool_size) */
    size_t              pool_min_size;      /* Min backend connections per worker */
    size_t              pool_max_size;      /* Max backend connections per worker */

    /* Pool operational timing */
    uint32_t            pool_prune_interval_ms;   /* How often to drop idle pool conns (default 30 000) */
    uint32_t            pool_refill_interval_ms;  /* Pool reconnect poll period (default 100; min 100) */
    uint32_t            pool_refill_backoff_ms;   /* Slower poll period when pool is full (default 5 000) */
    uint32_t            pool_max_waiting;         /* Max queued sessions per worker (0 = auto) */

    /* TCP accept settings */
    uint32_t            listen_backlog;           /* listen() queue depth (0 = use OS default 4096) */

    /* Legacy single backend (deprecated, use server_pool) */
    const char*         backend_host;       /* Backend server host */
    uint16_t            backend_port;       /* Backend server port */
    const char*         backend_user;       /* Backend authentication user */
    const char*         backend_password;   /* Backend authentication password */
    const char*         backend_database;   /* Backend database name */
    
    /* Stats / Instrumentation */
    int                 stats_level;        /* keel_stats_level_t (0=OFF..5=FULL) */
    uint32_t            stats_interval_ms;  /* Periodic log dump interval (0=disabled) */
    uint32_t            hotpath_instr_mask; /* KEEL_HOT_INSTR_* runtime gates */
    uint32_t            instr_mask;         /* KEEL_INSTR_CAT_* function-level probes */

    /* Hook registry — per-group; NULL means no hooks */
    keel_hook_registry_t* hook_registry;

    /* Prepared-statement pooling strategy */
    keel_ps_mode_t      ps_mode;  /**< How to handle named prepared statements */

    /* Connection rebalancing — automatic load-based migration between workers */
    bool                rebalance_enabled;        /**< Enable automatic rebalancing (default: true) */
    uint32_t            rebalance_interval_ms;    /**< How often each worker checks load (default: 5000) */
    uint32_t            rebalance_threshold_pct;  /**< Move sessions if load > avg * threshold/100 (default: 125 = 1.25x) */
    uint32_t            rebalance_max_per_tick;   /**< Max migrations per rebalance tick (default: 4) */

    /** Runtime mode tier — controls which hot-path features are active.
     *  proxy=minimal pass-through, pool=pooling+PS, smart=+routing+logging,
     *  full=+hooks+txn_tracking+LSN.  Default: pool. */
    keel_tier_t          runtime_mode;

    /** Replication uncertainty tracking (transaction_tracking = on).
     *  When true, KEEL intercepts COMMIT and captures the PostgreSQL XID
     *  before committing.  On backend failure mid-COMMIT the engine checks
     *  txid_status() on the new primary to determine the outcome. */
    bool                txn_tracking;

    /** Zero-copy fast network path (fast_network_path = on).
     *  When true, the engine uses MSG_PEEK + splice() to transfer result-set
     *  DataRow frames directly from backend socket to client socket through
     *  kernel pipes, bypassing userspace copies.  Only safe frames (DataRow)
     *  are spliced; terminal frames (ReadyForQuery, ErrorResponse, etc.) are
     *  always intercepted via the normal protocol path. */
    bool                fast_network_path;

    /** Result cache (result_cache = on|off).
     *  When true, the engine caches result sets and serves repeated identical
     *  queries from the cache without hitting the backend.  Disables the
     *  fast_network_path splice bypass for cacheable queries since the data
     *  must be captured in userspace.
     *  When on, a `keel_query_cache_t` is created per worker in `worker_init()`. */
    bool                result_cache;

    /** Sticky-primary TTL for read-after-write consistency.
     *  After a write, subsequent reads from the same session are forced to
     *  the primary for this many milliseconds.  0 = disabled (all reads can
     *  go to replicas immediately). Default: 100 ms. */
    uint32_t            sticky_primary_ttl_ms;

    /** Scatter-merge in-memory limit (scatter_merge_max_mem_mb in config).
     *  Maximum bytes a single scatter result set may occupy in userspace
     *  before rows are spilled to disk.  When the limit is exceeded the
     *  proxy transparently writes rows to a temporary file in
     *  scatter_merge_spill_dir and reads them back during the merge phase.
     *  0 uses KEEL_SCATTER_DEFAULT_MEM_LIMIT_BYTES (32 MiB).
     *  Values below 1 MiB are silently raised to 1 MiB. */
    size_t              scatter_merge_max_mem_bytes;

    /** Directory for scatter-merge spill files (scatter_merge_spill_dir).
     *  Temporary row-store files are created here when a scatter result
     *  exceeds scatter_merge_max_mem_bytes.  Defaults to "/tmp".
     *  The directory must be writable by the proxy process.
     *  For best performance this should reside on a fast local filesystem
     *  (tmpfs, NVMe) rather than a network mount. */
    const char*         scatter_merge_spill_dir;

    /** Client-facing authentication method.
     *  Controls what challenge is sent to connecting clients.
     *  Default: KEEL_AUTH_SCRAM_SHA_256. */
    keel_auth_method_t  auth_method;

    /** Path to a userlist file for SCRAM-SHA-256 and MD5 authentication.
     *  File format (pgbouncer-compatible):
     *    "username" "password_or_hash"
     *  Plain-text passwords are hashed at load time.
     *  NULL or empty string disables userlist loading. */
    const char*         auth_userlist_file;

    /** LDAP configuration — used when auth_method == KEEL_AUTH_LDAP. */
    const char*         auth_ldap_url;
    const char*         auth_ldap_base_dn;
    const char*         auth_ldap_bind_dn;
    const char*         auth_ldap_bind_password;
    const char*         auth_ldap_search_filter;
    const char*         auth_ldap_dn_suffix;
    bool                auth_ldap_start_tls;
    int                 auth_ldap_timeout_s;

    /** PAM configuration — used when auth_method == KEEL_AUTH_PAM. */
    const char*         auth_pam_service_name;

    /** auth_query configuration — used when auth_method == KEEL_AUTH_QUERY.
     *  query must return a single column (the stored password hash).
     *  conn_string is a libpq connection string for the lookup backend. */
    const char*         auth_query;
    const char*         auth_query_conn_string;

    /** Frontend TLS configuration (client → proxy).
     *  Copied from worker_group_t.tls_config at engine creation.
     *  mode == KEEL_TLS_DISABLE means TLS is off (default). */
    keel_tls_config_t   tls_config;

    /** Backend TLS configuration (proxy → database).
     *  Copied from worker_group_t.backend_tls_config at engine creation. */
    keel_tls_config_t   backend_tls_config;

    /** Distributed tracing configuration.
     *  Parsed from [tracing] config section; default is disabled. */
    keel_trace_config_t  trace_config;

    /** Optional shard router for hash-based shard routing and scatter-merge.
     *  When non-NULL, the engine calls keel_router_dispatch_sql() for each
     *  query to route single-shard queries and scatter aggregation queries.
     *  The router must have servers registered whose shard_id fields match
     *  the zero-based index of the corresponding server in server_pool.
     *  Ownership is NOT transferred; the caller must keep the router alive
     *  as long as the engine is running. */
    keel_router_t*       router;
} keel_engine_config_t;

/* Default configuration */
#define KEEL_ENGINE_CONFIG_DEFAULT { \
    .reactor_type = KEEL_REACTOR_AUTO, \
    .queue_depth = 256, \
    .use_buf_rings = false, \
    .sqpoll = false, \
    .sqpoll_idle_ms = 1000, \
    .buf_ring_size = 0, \
    .num_workers = 0, \
    .pin_workers = true, \
    .slab_buffer_size = 64 * 1024, \
    .session_pool_size = 1024, \
    .pipe_pool_size = 16, \
    .idle_timeout_ms = 300000, \
    .connect_timeout_ms = 10000, \
    .pool_idle_timeout_ms = 300000, \
    .default_protocol = "postgres", \
    .pool_min_size = 10, \
    .pool_max_size = 50, \
    .pool_prune_interval_ms = 30000, \
    .pool_refill_interval_ms = 100, \
    .pool_refill_backoff_ms = 5000, \
    .pool_max_waiting = 0, \
    .rebalance_enabled = true, \
    .rebalance_interval_ms = 5000, \
    .rebalance_threshold_pct = 125, \
    .rebalance_max_per_tick = 4, \
    .listen_backlog = 4096, \
    .server_pool = { .count = 0, .rw_count = 0, .ro_count = 0, .wo_count = 0, .next_read = 0, .next_write = 0, .shard_count = 0 }, \
    .backend_host = "127.0.0.1", \
    .backend_port = 5432, \
    .backend_user = "postgres", \
    .backend_password = NULL, \
    .backend_database = "postgres", \
    .stats_level = 0, \
    .stats_interval_ms = 0, \
    .hotpath_instr_mask = KEEL_HOT_INSTR_ALL, \
    .instr_mask = 0, \
    .hook_registry = NULL, \
    .ps_mode = KEEL_PS_MODE_VIRTUALIZE, \
    .runtime_mode = KEEL_TIER_POOL, \
    .txn_tracking = false, \
    .fast_network_path = true, \
    .result_cache = false, \
    .sticky_primary_ttl_ms = 100U, \
    .scatter_merge_max_mem_bytes = 0, \
    .scatter_merge_spill_dir = NULL, \
    .auth_method = KEEL_AUTH_SCRAM_SHA_256, \
    .auth_ldap_url = NULL, \
    .auth_ldap_base_dn = NULL, \
    .auth_ldap_bind_dn = NULL, \
    .auth_ldap_bind_password = NULL, \
    .auth_ldap_search_filter = NULL, \
    .auth_ldap_dn_suffix = NULL, \
    .auth_ldap_start_tls = false, \
    .auth_ldap_timeout_s = 5, \
    .auth_pam_service_name = NULL, \
    .auth_query = NULL, \
    .auth_query_conn_string = NULL, \
    .pool_max_connection_age_ms = 0, \
    .tls_config = { .mode = KEEL_TLS_DISABLE, .ktls_enabled = true, .read_timeout_ms = 30000, .handshake_timeout_ms = 10000 }, \
    .backend_tls_config = { .mode = KEEL_TLS_DISABLE, .ktls_enabled = true, .read_timeout_ms = 30000, .handshake_timeout_ms = 10000 }, \
    .router = NULL, \
}

/* ============================================================================
 * Engine Lifecycle States
 * ============================================================================ */

/**
 * @brief Lifecycle states exposed by the top-level engine.
 */
typedef enum keel_engine_state {
    KEEL_ENGINE_STATE_CREATED  = 0,  /* Engine allocated, not yet started */
    KEEL_ENGINE_STATE_ACTIVE   = 1,  /* Running, accepting connections */
    KEEL_ENGINE_STATE_DRAINING = 2,  /* Rejecting new connections, finishing active */
    KEEL_ENGINE_STATE_STOPPING = 3,  /* Workers signaled to exit */
    KEEL_ENGINE_STATE_STOPPED  = 4,  /* All workers joined, engine idle */
} keel_engine_state_t;

/* ============================================================================
 * Engine Handle
 * ============================================================================ */

/** Opaque engine handle coordinating workers, sockets, and lifecycle state. */
typedef struct keel_engine keel_engine_t;

/**
 * @brief Create a new engine instance.
 *
 * @param config Engine configuration, or `NULL` to use defaults.
 * @return Engine handle, or `NULL` on allocation failure.
 */
keel_engine_t* keel_engine_create(const keel_engine_config_t* config);

/**
 * @brief Start the engine and spawn worker threads.
 *
 * @param engine Engine handle.
 * @param listen_fd Listening socket file descriptor.
 * @return `0` on success or `-1` on error.
 */
int keel_engine_start(keel_engine_t* engine, int listen_fd);

/**
 * @brief Run the engine control loop in the current thread.
 *
 * @param engine Engine handle.
 * @return `0` on clean shutdown or `-1` on error.
 */
int keel_engine_run(keel_engine_t* engine);

/**
 * @brief Signal the engine to stop and join workers.
 *
 * @param engine Engine handle.
 * @return
 */
void keel_engine_stop(keel_engine_t* engine);

/**
 * @brief Enter drain mode: stop accepting new connections, wait for active
 *        connections to complete or until the drain timeout expires.
 *
 * Call before keel_engine_stop() for graceful shutdown.
 *
 * @param engine Engine handle.
 * @return `0` on success or `-1` on error.
 */
int keel_engine_drain(keel_engine_t* engine);

/**
 * @brief Set the drain timeout.
 *
 * @param engine Engine handle.
 * @param timeout_ms Maximum milliseconds to wait for drain; `0` forces immediate escalation.
 * @return
 */
void keel_engine_set_drain_timeout(keel_engine_t* engine, uint32_t timeout_ms);

/**
 * @brief Check if the engine is in drain mode.
 *
 * @param engine Engine handle.
 * @return `true` if draining.
 */
bool keel_engine_is_draining(keel_engine_t* engine);

/**
 * @brief Rolling restart of all worker threads.
 *
 * Drains existing workers (stop accepting, let sessions finish), then spawns
 * replacements with the current engine configuration. New workers begin
 * accepting connections before old workers are fully joined, so the service
 * gap is minimal.
 *
 * @param engine Running engine handle.
 * @param drain_timeout_ms Maximum time to wait for old workers to drain.
 *                         `0` uses the engine's default drain timeout.
 * @return `0` on success, `-1` when the engine is not running.
 */
int keel_engine_restart_workers(keel_engine_t* engine, uint32_t drain_timeout_ms);

/**
 * @brief Get the current lifecycle state.
 *
 * @param engine Engine handle.
 * @return Current lifecycle-state enum value.
 */
keel_engine_state_t keel_engine_get_state(keel_engine_t* engine);

/**
 * @brief Force-close all active client and backend connections.
 *
 * Walks each worker's session slab and closes every session that still
 * has an open client_fd.  Used as a last resort when drain timeout expires
 * and orphaned sessions must be terminated.
 *
 * @param engine Engine handle.
 * @return Number of sessions force-closed.
 */
uint64_t keel_engine_force_close_all(keel_engine_t* engine);

/**
 * @brief Destroy the engine and free resources.
 *
 * @param engine Engine handle.
 * @return
 */
void keel_engine_destroy(keel_engine_t* engine);

/**
 * @brief Get the current reactor type being used.
 *
 * @param engine Engine handle.
 * @return Reactor type.
 */
keel_reactor_type_t keel_engine_get_reactor_type(keel_engine_t* engine);

/**
 * @brief Get total connections accepted.
 *
 * @param engine Engine handle.
 * @return Total connection count.
 */
uint64_t keel_engine_get_total_connections(keel_engine_t* engine);

/**
 * @brief Get currently active connections.
 *
 * @param engine Engine handle.
 * @return Active connection count.
 */
uint64_t keel_engine_get_active_connections(keel_engine_t* engine);

/**
 * @brief Decrement active connection count.
 *
 * @param engine Engine handle.
 * @return
 */
void keel_engine_dec_connections(keel_engine_t* engine);

/**
 * @brief Get the engine configuration.
 *
 * @param engine Engine handle.
 * @return Pointer to engine configuration (read-only).
 */
const keel_engine_config_t* keel_engine_get_config(keel_engine_t* engine);

/**
 * @brief Get a mutable pointer to the engine configuration.
 *
 * @param engine Engine handle.
 * @return Pointer to engine configuration (mutable) or `NULL`.
 */
keel_engine_config_t* keel_engine_get_config_mut(keel_engine_t* engine);

/* Forward declaration for stats collector */
struct keel_stats_collector;

/**
 * @brief Get the engine's stats collector.
 *
 * @param engine Engine handle.
 * @return Stats collector, or `NULL` if stats are disabled.
 */
struct keel_stats_collector* keel_engine_get_stats_collector(keel_engine_t* engine);

/**
 * @brief Set a periodic callback for stats dump / maintenance
 *
 * The callback is invoked from the main thread on:
 *   - SIGUSR1 signal
 *   - Every stats_interval_ms (if configured)
 *
 * @param engine Engine handle.
 * @param cb Callback function.
 * @param ctx Opaque context passed to the callback.
 * @return
 */
void keel_engine_set_periodic_callback(keel_engine_t* engine,
                                       void (*cb)(void *ctx), void *ctx);

/* Forward declaration for worker */
struct keel_worker;

/**
 * @brief Get the number of workers in the engine.
 *
 * @param engine Engine handle.
 * @return Resolved worker count.
 */
uint32_t keel_engine_get_num_workers(keel_engine_t* engine);

/**
 * @brief Get a worker by index.
 *
 * @param engine Engine handle.
 * @param idx Worker index.
 * @return Read-only pointer, or `NULL` if out of range.
 */
const struct keel_worker* keel_engine_get_worker(keel_engine_t* engine, uint32_t idx);

/**
 * @brief Get a mutable worker by index.
 *
 * @param engine Engine handle.
 * @param idx Worker index.
 * @return Mutable pointer, or `NULL` if out of range.
 */
struct keel_worker* keel_engine_get_worker_mut(keel_engine_t* engine, uint32_t idx);

/**
 * @brief Get the engine's server pool.
 *
 * @param engine Engine handle.
 * @return Mutable server-pool pointer, or `NULL`.
 */
keel_server_pool_t* keel_engine_get_server_pool(keel_engine_t* engine);

/**
 * @brief Get the engine's hook registry.
 *
 * @param engine Engine handle.
 * @return Hook registry for the worker group, or `NULL`.
 */
keel_hook_registry_t* keel_engine_get_hook_registry(keel_engine_t* engine);

/**
 * @brief Attach a tracer to the engine (called once at startup).
 */
void keel_engine_set_tracer(keel_engine_t* engine, struct keel_tracer* tracer);

/**
 * @brief Get the engine's tracer (NULL when tracing is disabled).
 */
struct keel_tracer* keel_engine_get_tracer(keel_engine_t* engine);

/* Forward declaration — keeps engine.h independent of audit_log.h */
struct keel_audit_log;

/**
 * @brief Attach an audit log to the engine (called once at startup).
 *
 * The audit log is owned by the caller and must outlive the engine.
 * NULL disables audit logging.  Workers cache the pointer at init time
 * so this must be called before keel_engine_run().
 */
void keel_engine_set_audit_log(keel_engine_t* engine, struct keel_audit_log* al);

/**
 * @brief Get the engine's audit log (NULL when audit logging is disabled).
 */
struct keel_audit_log* keel_engine_get_audit_log(keel_engine_t* engine);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ENGINE_H */
