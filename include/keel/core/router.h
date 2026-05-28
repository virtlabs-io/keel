/**
 * @file router.h
 * @brief Public API for query routing, load balancing, and read/write split decisions.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This module implements intelligent query routing:
 * - Read/write splitting based on Query Tree analysis
 * - Weighted load balancing across replicas and primary
 * - Support for transactions (always route to primary)
 * - Health-aware routing (skip unhealthy nodes)
 *
 * Weight Strategy:
 * ================
 * By default, the primary receives half the weight of one replica for reads.
 * This means with 1 primary and 2 replicas:
 *   - Replica 1: 40%
 *   - Replica 2: 40%  
 *   - Primary:   20%
 *
 * All writes, DDL, and transaction control go to the primary.
 *
 * Configuration:
 * ==============
 * @code
 * [routing]
 * strategy = weighted_round_robin
 * primary_read_weight = 0.5        # Primary gets 0.5x replica weight for reads
 * read_write_split = true
 * 
 * [servers.primary]
 * host = db-primary.internal
 * port = 5432
 * role = primary
 * weight = 100                     # Base weight for this server
 *
 * [servers.replica1]
 * host = db-replica1.internal
 * port = 5432
 * role = replica
 * weight = 100
 *
 * [servers.replica2]
 * host = db-replica2.internal
 * port = 5432
 * role = replica
 * weight = 100
 * @endcode
 *
 * Usage:
 * ======
 * @code
 * // Create router
 * keel_router_config_t config = keel_router_config_default();
 * config.strategy = KEEL_ROUTE_WEIGHTED_ROUND_ROBIN;
 * config.primary_read_weight = 0.5;
 * config.read_write_split = true;
 *
 * keel_router_t* router = keel_router_create(&config);
 *
 * // Add servers
 * keel_router_add_server(router, &(keel_route_server_t){
 *     .name = "primary",
 *     .host = "db-primary",
 *     .port = 5432,
 *     .role = KEEL_SERVER_PRIMARY,
 *     .weight = 100
 * });
 *
 * // Route a query
 * keel_route_decision_t decision;
 * keel_router_route(router, query, session_state, &decision);
 * 
 * // Use decision.server to send the query
 * @endcode
 */

#ifndef KEEL_ROUTER_H
#define KEEL_ROUTER_H

#include "keel_types.h"
#include "keel_error.h"
#include "keel/engine/engine.h"  /* For keel_server_role_t */
#include "keel/core/sharding.h"
#include "keel/core/ini.h"
#include "keel/sql/query_tree.h"
#include "keel/core/scatter_store.h" /* keel_sort_key_t, KEEL_SCATTER_MAX_ORDER_KEYS */

/* Compatibility aliases for server roles */
#define KEEL_SERVER_PRIMARY  KEEL_SERVER_ROLE_RW
#define KEEL_SERVER_REPLICA  KEEL_SERVER_ROLE_RO
#define KEEL_SERVER_STANDBY  KEEL_SERVER_ROLE_AUTO

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Types
 * ============================================================================ */

/** Opaque router handle that owns server state and routing statistics. */
typedef struct keel_router keel_router_t;

/**
 * @brief Load-balancing strategies supported by the router.
 */
typedef enum keel_route_strategy {
    KEEL_ROUTE_STRATEGY_ROUND_ROBIN = 0,      /**< Simple round-robin */
    KEEL_ROUTE_STRATEGY_WEIGHTED_ROUND_ROBIN, /**< Weighted round-robin */
    KEEL_ROUTE_STRATEGY_LEAST_CONNECTIONS,    /**< Least active connections */
    KEEL_ROUTE_STRATEGY_RANDOM,               /**< Random selection */
    KEEL_ROUTE_STRATEGY_FIRST_AVAILABLE,      /**< First healthy server */
} keel_route_strategy_t;

/**
 * @brief Router-visible health state for a backend server.
 */
#ifndef KEEL_SERVER_HEALTH_DEFINED
#define KEEL_SERVER_HEALTH_DEFINED 1
typedef enum keel_server_health {
    KEEL_HEALTH_UNKNOWN = 0,     /**< Health not yet checked */
    KEEL_HEALTH_UP,              /**< Server is healthy */
    KEEL_HEALTH_DOWN,            /**< Server is unreachable */
    KEEL_HEALTH_DEGRADED,        /**< Server is slow/overloaded */
    KEEL_HEALTH_MAINTENANCE,     /**< Server in maintenance mode */
} keel_server_health_t;
#else
/* keel_health_status (probe.h) uses the same enumerator names; reuse them. */
typedef keel_health_status_t keel_server_health_t;
#define KEEL_HEALTH_MAINTENANCE 4 /* Additional value not in probe.h */
#endif

/**
 * @brief Static configuration plus live runtime state for a routed server.
 */
typedef struct keel_route_server {
    const char*         name;       /**< Server identifier */
    const char*         host;       /**< Hostname or IP */
    uint16_t            port;       /**< Port number */
    keel_server_role_t   role;       /**< Primary or replica */
    int                 weight;     /**< Base weight (1-1000) */
    size_t              shard_id;   /**< Logical shard bucket for sharded routing */
    
    /* Runtime state (managed by router) */
    keel_server_health_t health;     /**< Current health state */
    int                 active_conns; /**< Active connection count */
    uint64_t            total_queries; /**< Total queries routed */
    uint64_t            total_errors;  /**< Total errors */
    keel_time_t          last_health_check; /**< Last health check time */
    
    void*               user_data;  /**< User data pointer */
} keel_route_server_t;

/**
 * @brief Session-local state that constrains routing safety.
 */
typedef struct keel_route_session {
    bool                in_transaction;     /**< Inside explicit transaction */
    bool                has_temp_tables;    /**< Session has temp tables */
    bool                has_session_vars;   /**< Session has SET variables */
    keel_route_server_t* pinned_server;      /**< Pinned server (if any) */
    const char*         preferred_server;   /**< Preferred server name */

    /* Feature 7: Multi-shard transaction coordination */
    bool                has_scatter_write;    /**< A scatter write was recorded for this tx */
    uint64_t            scatter_shards_mask;  /**< Bitmask of participating shard indices */

    /* Commit-in-doubt is sacred: once an ambiguous commit is detected the
     * session MUST NOT route any further query through the normal pipeline
     * until the outcome of the prior COMMIT has been resolved (or surfaced
     * to the client as an explicit failure). The engine mirrors this from
     * `keel_session_flow_t::commit_in_doubt` before every routing call. */
    bool                commit_in_doubt;
} keel_route_session_t;

/**
 * @brief Typed reason code for a routing decision.
 *
 * Added alongside the existing human-readable `reason` string so callers can
 * branch on the outcome programmatically without string comparison.  The
 * numeric value is suitable for structured logging and metrics.
 */
typedef enum keel_route_reason {
    KEEL_ROUTE_REASON_NORMAL = 0,        /**< Normal strategy selection */
    KEEL_ROUTE_REASON_IN_TRANSACTION,    /**< Pinned: active transaction */
    KEEL_ROUTE_REASON_PINNED_SESSION,    /**< Pinned: session-level state (SET vars) */
    KEEL_ROUTE_REASON_PINNED_PS,         /**< Pinned: named prepared statements */
    KEEL_ROUTE_REASON_HARD_PINNED,       /**< Hard-pinned (LISTEN / TEMP / CURSOR) */
    KEEL_ROUTE_REASON_WRITE_REQUIRED,    /**< Query requires a write-capable server */
    KEEL_ROUTE_REASON_READ_SPLIT,        /**< Read dispatched to replica (r/w split) */
    KEEL_ROUTE_REASON_FAILOVER_PRIMARY,  /**< Replicas unavailable, fell back to primary */
    KEEL_ROUTE_REASON_LAG_EXCEEDED,      /**< Replica lag above threshold */
    KEEL_ROUTE_REASON_HEALTH_DEGRADED,   /**< Server degraded, rerouted */
    KEEL_ROUTE_REASON_NO_PRIMARY,        /**< No write-capable server available */
    KEEL_ROUTE_REASON_CID_BLOCKED,       /**< Write blocked: commit-in-doubt in progress */
    KEEL_ROUTE_REASON_TIMELINE_STALE,    /**< LSN token stale after timeline switch */
    KEEL_ROUTE_REASON_PATRONI_UNAVAIL,   /**< Patroni API unavailable, frozen */
    KEEL_ROUTE_REASON_ROLE_FLAPPING,     /**< Server role unstable, conservative routing */
    KEEL_ROUTE_REASON_DDL,               /**< DDL statement routed to primary */
    KEEL_ROUTE_REASON_TRANSACTION_CTRL,  /**< Transaction-control (BEGIN/COMMIT/…) */
    KEEL_ROUTE_REASON_SEMANTIC_UNSAFE,   /**< Read-looking SQL was not proven replica-safe */
    KEEL_ROUTE_REASON_UNKNOWN_FUNCTION,  /**< SQL calls a function not in the metadata
                                              cache; conservative policy forces primary. */
    KEEL_ROUTE_REASON_COMMIT_AMBIGUOUS,  /**< Session has an unresolved commit-in-doubt;
                                              no new query may be routed until the
                                              prior transaction outcome is determined.
                                              See docs/CORRECTNESS_UNDER_FAILURE.md. */
    KEEL_ROUTE_REASON_COUNT,
} keel_route_reason_t;

/**
 * @brief Return a stable ASCII name for a route reason code.
 *
 * Returns `"UNKNOWN"` for values outside the known range.
 */
const char* keel_route_reason_name(keel_route_reason_t r);

/**
 * @brief Bitmask of contributing factors behind a routing decision.
 *
 * The `reason_code` on `keel_route_decision_t` is the single dominant cause
 * (the first one that short-circuits the decision). The `decision_factors`
 * bitmask carries every condition that contributed, so logs and the
 * `/api/diagnostics/route_explain` endpoint can say *why primary* in detail
 * rather than collapsing every primary-only query into "WRITE_REQUIRED".
 *
 * The bitmask is intentionally redundant with `reason_code` for the
 * single-cause case; new factors may be added without changing the dominant
 * reason taxonomy. Values are sparse so new flags can slot in without
 * renumbering.
 */
typedef enum keel_route_factor {
    KEEL_DF_NONE                = 0,
    KEEL_DF_IN_TRANSACTION      = (1u << 0),  /**< Session is inside BEGIN..COMMIT */
    KEEL_DF_SESSION_PINNED      = (1u << 1),  /**< Session pinned to a specific server */
    KEEL_DF_HAS_TEMP_TABLE      = (1u << 2),  /**< Session has temp tables */
    KEEL_DF_STMT_CLASS_WRITE    = (1u << 3),  /**< Statement is a write (INSERT/UPDATE/...) */
    KEEL_DF_STMT_CLASS_DDL      = (1u << 4),  /**< Statement is DDL */
    KEEL_DF_STMT_CLASS_TXN_CTL  = (1u << 5),  /**< Statement is transaction control */
    KEEL_DF_UNKNOWN_FUNCTION    = (1u << 6),  /**< SQL references a function not in
                                                   the metadata cache (conservative) */
    KEEL_DF_VOLATILE_FUNCTION   = (1u << 7),  /**< SQL references a VOLATILE function */
    KEEL_DF_SECURITY_DEFINER    = (1u << 8),  /**< SQL references SECURITY DEFINER function */
    KEEL_DF_WRITE_TRIGGER       = (1u << 9),  /**< Target has a write trigger */
    KEEL_DF_WRITE_RULE          = (1u << 10), /**< Target has a write rule (view) */
    KEEL_DF_PARSE_FAILED        = (1u << 11), /**< Parser failed or produced partial AST */
    KEEL_DF_REPLICA_LAG         = (1u << 12), /**< Selected replicas exceed lag threshold */
    KEEL_DF_NO_REPLICAS         = (1u << 13), /**< No healthy replicas available */
    KEEL_DF_FAILOVER_FALLBACK   = (1u << 14), /**< Read fell back to primary */
    KEEL_DF_STICKY_PRIMARY      = (1u << 15), /**< Within read-after-write TTL window */
    KEEL_DF_COMMIT_IN_DOUBT     = (1u << 16), /**< Prior COMMIT outcome unresolved */
    KEEL_DF_REPLICA_OK          = (1u << 17), /**< Proven safe for a replica */
    KEEL_DF_USER_PINNED         = (1u << 18), /**< SET keel.route = primary */
} keel_route_factor_t;

/**
 * @brief Format a `decision_factors` bitmask as a JSON array of stable names.
 *
 * The output is suitable for embedding into the value produced by
 * `keel_route_decision_to_json()` or the admin diagnostics endpoint.
 * Order is fixed (low bit first) for stable diffing in tests.
 *
 * @return Number of bytes that would have been written, snprintf semantics.
 */
size_t keel_route_factors_to_json_array(uint32_t factors,
                                        char* out,
                                        size_t out_size);

/**
 * @brief Result object populated by routing functions.
 */
typedef struct keel_route_decision {
    keel_route_server_t*  server;       /**< Selected server */
    const char*           reason;       /**< Human-readable reason string */
    keel_route_reason_t   reason_code;  /**< Typed reason code for programmatic use */
    uint32_t              decision_factors; /**< Bitmask of `keel_route_factor_t`
                                                 values that contributed to the
                                                 decision; used by the
                                                 route-explainer surface. */
    bool                  is_read;      /**< Query is read-only */
    bool                  was_pinned;   /**< Decision due to pinning */
    size_t                shard_index;  /**< Resolved shard for sharded routing */
} keel_route_decision_t;

/**
 * @brief Format a route decision as a compact JSON object for logs/admin APIs.
 *
 * The helper is protocol-neutral: it reports the selected backend, read/write
 * class, pin state, shard index, and stable route reason code.  `query_hash`
 * may be zero when the caller does not have a semantic-plan hash.
 *
 * @return Number of bytes that would have been written, excluding the trailing
 *         NUL, matching `snprintf()` semantics.
 */
size_t keel_route_decision_to_json(const keel_route_decision_t* decision,
                                   uint64_t query_hash,
                                   char* out,
                                   size_t out_size);

/**
 * @brief Configuration knobs controlling router behavior and failure policy.
 */
typedef struct keel_router_config {
    /* Strategy */
    keel_route_strategy_t strategy;          /**< Load balancing strategy */
    bool                read_write_split;   /**< Enable read/write splitting */
    
    /* Weight configuration */
    double              primary_read_weight; /**< Primary weight factor for reads (0.0-1.0) */
    
    /* Failover */
    bool                failover_to_primary; /**< Send reads to primary if replicas down */
    bool                auto_detect_readonly; /**< Auto-detect read-only transactions */
    
    /* Health checking */
    bool                health_check_enabled; /**< Enable health checks */
    keel_duration_t      health_check_interval; /**< Health check interval */
    int                 health_check_retries; /**< Retries before marking down */
    
    /* Timeouts */
    keel_duration_t      connect_timeout;    /**< Connection timeout */
    keel_duration_t      query_timeout;      /**< Default query timeout */

    /**
     * Master gate for scatter/sharding dispatch. When false (the default),
     * `keel_router_dispatch_sql()` refuses any statement that classifies as
     * SCATTER and returns @c KEEL_ERR_NOT_SUPPORTED with
     * @ref KEEL_DISPATCH_REJECT_SCATTER_DISABLED so the engine surfaces a
     * clear error to the client. Operators opt in per worker group with
     * `scatter_merge = on`; see docs/PRODUCTION_READINESS.md.
     */
    bool                scatter_merge_enabled;

} keel_router_config_t;

/** Maximum shard count supported in a single scatter call. */
#define KEEL_SCATTER_MAX_SHARDS 64

/**
 * @brief Number of finite upper-bound buckets in the scatter-merge latency histogram.
 *
 * Boundaries (nanoseconds): 1 ms, 5 ms, 10 ms, 25 ms, 50 ms, 100 ms, 250 ms,
 * 500 ms, 1 s, 2.5 s.  A final +Inf bucket accounts for all observations.
 * The histogram array has KEEL_SCATTER_HIST_BUCKETS + 1 entries (0..NBUCKETS),
 * where index NBUCKETS is the +Inf bucket.
 */
#define KEEL_SCATTER_HIST_NBUCKETS  10

/** Upper bounds (in ns) for each finite histogram bucket (ascending). */
#define KEEL_SCATTER_HIST_BOUNDS_NS \
    {   1000000ULL,   5000000ULL,  10000000ULL,  25000000ULL,  50000000ULL, \
      100000000ULL, 250000000ULL, 500000000ULL, 1000000000ULL, 2500000000ULL }

/**
 * @brief Categories of scatter dispatches that the proxy executes but cannot
 *        guarantee globally correct results for.
 *
 * Each kind tags a SQL pattern that goes through the scatter pipeline but
 * relies on local per-shard semantics — operators can scrape the
 * @c keel_scatter_unsupported_pattern_total Prometheus counter to detect
 * silent-wrong-result risk in production workloads.
 *
 * @note See docs/LIMITATIONS.md §1 "Scatter aggregate pipeline" for the full
 *       inventory of patterns that flag here.
 */
typedef enum keel_scatter_unsupported_kind {
    KEEL_SCATTER_UNSUPPORTED_PERCENTILE = 0,    /**< PERCENTILE_CONT / PERCENTILE_DISC */
    KEEL_SCATTER_UNSUPPORTED_WINDOW_FUNC,       /**< Window function (OVER clause) */
    KEEL_SCATTER_UNSUPPORTED_RECURSIVE_CTE,     /**< WITH RECURSIVE (now rejected, not silently scattered) */
    KEEL_SCATTER_UNSUPPORTED_UNION_ALL,         /**< Set-operation across scatter */
    KEEL_SCATTER_UNSUPPORTED_DML_RETURNING,     /**< UPDATE/DELETE … RETURNING via scatter */
    KEEL_SCATTER_UNSUPPORTED_DDL,               /**< DDL passed through scatter */
    KEEL_SCATTER_UNSUPPORTED_GATE_DISABLED,     /**< Scatter rejected because `scatter_merge = off` */
    KEEL_SCATTER_UNSUPPORTED_KIND_COUNT         /**< Sentinel — not a real kind */
} keel_scatter_unsupported_kind_t;

/**
 * @brief Aggregate counters describing router decisions over time.
 */
typedef struct keel_router_stats {
    uint64_t            total_routes;       /**< Total routing decisions */
    uint64_t            read_routes;        /**< Routes to replicas */
    uint64_t            write_routes;       /**< Routes to primary */
    uint64_t            pinned_routes;      /**< Routes due to session pinning */
    uint64_t            failover_routes;    /**< Routes due to failover */

    /* Per-shard counters (index matches shard_id / plan.shard_index) */
    uint64_t            shard_single_routes[KEEL_SCATTER_MAX_SHARDS]; /**< Single-shard hits per shard */
    uint64_t            shard_scatter_hits; /**< Total scatter fanouts dispatched */
    uint64_t            shard_scatter_failed; /**< Scatter shards with no available server */

    /* Scatter merge latency (wall-clock nanoseconds across all merges) */
    uint64_t            scatter_merge_ops;     /**< Completed scatter merges */
    uint64_t            scatter_merge_total_ns;/**< Cumulative merge wall-clock ns */
    uint64_t            scatter_merge_max_ns;  /**< Longest single merge ns */

    /**
     * Prometheus-style histogram for scatter_merge_duration_seconds.
     * Indices 0..KEEL_SCATTER_HIST_NBUCKETS-1 correspond to the finite
     * upper-bound buckets defined by KEEL_SCATTER_HIST_BOUNDS_NS.
     * Index KEEL_SCATTER_HIST_NBUCKETS is the +Inf bucket (equals ops total).
     * All buckets are cumulative (each includes all smaller observations).
     */
    uint64_t            scatter_merge_hist[KEEL_SCATTER_HIST_NBUCKETS + 1];

    /* Two-phase commit (2PC) counters — participant-level operations */
    uint64_t            twopc_started;         /**< Coordinators successfully begun */
    uint64_t            twopc_prepared;        /**< Participants that ACKed PREPARE */
    uint64_t            twopc_prepare_failed;  /**< Participants that rejected PREPARE */
    uint64_t            twopc_committed;       /**< Participants that completed COMMIT PREPARED */
    uint64_t            twopc_rolled_back;     /**< Participants rolled back (normal or abort) */

    /**
     * Per-kind counter for scatter dispatches whose result correctness depends
     * on patterns the engine does not fully merge.  Indexed by
     * @ref keel_scatter_unsupported_kind_t.  Bump via
     * @ref keel_router_track_unsupported_pattern.
     */
    uint64_t            scatter_unsupported_pattern[KEEL_SCATTER_UNSUPPORTED_KIND_COUNT];

    /* Per-server stats available via keel_router_get_server_stats() */
} keel_router_stats_t;

/* ============================================================================
 * Configuration
 * ============================================================================ */

/**
 * @brief Construct a router configuration populated with safe defaults.
 *
 * @return Default-initialized router configuration.
 */
keel_router_config_t keel_router_config_default(void);

/* ============================================================================
 * Router Lifecycle
 * ============================================================================ */

/**
 * @brief Create a new router
 *
 * @param config Router configuration
 * @return Router handle, or `NULL` if allocation or initialization fails.
 */
keel_router_t* keel_router_create(const keel_router_config_t* config);

/**
 * @brief Destroy a router and release all owned server state.
 * @return
 */
void keel_router_destroy(keel_router_t* router);

/* ============================================================================
 * Server Management
 * ============================================================================ */

/**
 * @brief Add a server to the router
 *
 * @param router Router handle
 * @param server Server configuration (copied)
 * @return `KEEL_OK` on success or an error code if validation or allocation fails.
 */
keel_error_t keel_router_add_server(keel_router_t* router, 
                                   const keel_route_server_t* server);

/**
 * @brief Remove a server from the router
 *
 * @param router Router handle
 * @param name   Server name
 * @return `KEEL_OK` on success or an error code if no such server exists.
 */
keel_error_t keel_router_remove_server(keel_router_t* router, const char* name);

/**
 * @brief Get server by name
 *
 * @param router Router handle
 * @param name   Server name
 * @return Mutable server pointer, or `NULL` when the name is unknown.
 */
keel_route_server_t* keel_router_get_server(keel_router_t* router, const char* name);

/**
 * @brief Update the router's health view for a named server.
 *
 * @param router Router handle
 * @param name   Server name
 * @param health New health state to publish for the server.
 * @return
 */
void keel_router_set_server_health(keel_router_t* router, 
                                   const char* name,
                                   keel_server_health_t health);

/**
 * @brief Count healthy servers that match a given role.
 *
 * @param router Router handle.
 * @param role Server role to count.
 * @return Number of healthy servers matching `role`.
 */
size_t keel_router_count_healthy(const keel_router_t* router, keel_server_role_t role);

/* ============================================================================
 * Routing
 * ============================================================================ */

/**
 * @brief Route a query using Query Tree analysis
 *
 * This is the main routing function. It analyzes the query using the
 * Query Tree and selects an appropriate server based on:
 *   - Query type (read/write/DDL/transaction)
 *   - Session state (in transaction, temp tables, etc.)
 *   - Server health
 *   - Load balancing weights
 *
 * @param router   Router handle
 * @param qt       Query Tree from keel_sql_analyze_full()
 * @param session  Session state
 * @param decision [out] Routing decision
 * @return `KEEL_OK` on success or an error code if no safe route is available.
 */
keel_error_t keel_router_route(keel_router_t* router,
                              const keel_qt_query_t* qt,
                              const keel_route_session_t* session,
                              keel_route_decision_t* decision);

/**
 * @brief Route a query using raw SQL (legacy interface)
 *
 * This is a simpler interface that parses the SQL internally.
 * Prefer keel_router_route() with a pre-parsed Query Tree for
 * better performance when processing many queries.
 *
 * @param router   Router handle
 * @param sql      SQL query text
 * @param session  Session state
 * @param decision [out] Routing decision
 * @return `KEEL_OK` on success or an error code if parsing or routing fails.
 */
keel_error_t keel_router_route_sql(keel_router_t* router,
                                  keel_str_t sql,
                                  const keel_route_session_t* session,
                                  keel_route_decision_t* decision);

/**
 * @brief Route raw SQL through Phase 1 single-shard routing.
 *
 * This path requires the statement to constrain the configured shard key to a
 * single concrete value. Queries without an extractable single-shard predicate
 * return an error instead of falling back to arbitrary non-sharded routing.
 */
keel_error_t keel_router_route_sharded_sql(keel_router_t* router,
                                           keel_str_t sql,
                                           const keel_route_session_t* session,
                                           const keel_shard_rule_t* rule,
                                           keel_route_decision_t* decision);

/**
 * @brief Route raw SQL through single-shard routing with bound parameter values.
 *
 * Like keel_router_route_sharded_sql() but accepts a bound-parameter table so
 * that prepared statements using $N placeholders can be resolved to a concrete
 * shard.  Pass NULL for @p params to fall back to literal-only extraction
 * (equivalent to calling keel_router_route_sharded_sql()).
 */
keel_error_t keel_router_route_sharded_sql_bound(keel_router_t* router,
                                                 keel_str_t sql,
                                                 const keel_route_session_t* session,
                                                 const keel_shard_rule_t* rule,
                                                 const keel_shard_bound_params_t* params,
                                                 keel_route_decision_t* decision);

/**
 * @brief Compute a routing plan for @p sql without making a routing decision.
 *
 * Returns a @ref keel_shard_plan_t describing whether the statement targets a
 * single shard (SINGLE), must be scattered across all shards (SCATTER), or
 * cannot be shard-routed at all (UNSUPPORTED).
 *
 * This is useful for proxy-level dispatch loops that need to know the fanout
 * before allocating backend connections.  Pass NULL for @p params to treat
 * $N parameters as unbound (SCATTER result for parameterised statements).
 */
void keel_router_plan_sharded_sql(keel_router_t* router,
                                  keel_str_t sql,
                                  const keel_shard_rule_t* rule,
                                  const keel_shard_bound_params_t* params,
                                  keel_shard_plan_t* plan);

/* ============================================================================
 * Shard rule registry
 * ============================================================================ */

/**
 * @brief Register a shard rule with this router.
 *
 * Rules are keyed by table name.  Re-registering the same table name
 * overwrites the existing entry.  Strings are copied internally.
 *
 * @param router      Router handle.
 * @param table       Sharded table name (case-insensitive match at plan time).
 * @param column      Shard key column name.
 * @param shard_count Number of shards for this table.
 * @return KEEL_OK on success, KEEL_ERR_OVERFLOW when the registry is full,
 *         KEEL_ERR_INVALID_ARG on NULL inputs.
 */
keel_error_t keel_router_add_shard_rule(keel_router_t* router,
                                        const char* table,
                                        const char* column,
                                        size_t shard_count);

/**
 * @brief Remove a previously registered shard rule by table name.
 *
 * @return KEEL_OK if removed, KEEL_ERR_NOT_FOUND if no rule exists for @p table.
 */
keel_error_t keel_router_remove_shard_rule(keel_router_t* router,
                                            const char* table);

/**
 * @brief Return the registered shard rule for @p table, or NULL if none.
 */
const keel_shard_rule_t* keel_router_get_shard_rule(const keel_router_t* router,
                                                     const char* table);

/**
 * @brief Return the number of registered shard rules.
 */
size_t keel_router_shard_rule_count(const keel_router_t* router);

/**
 * @brief Return the registered shard rule at position @p idx (0-based).
 *
 * Provides index-based access to the flat rule registry for consumers
 * (such as the admin virtual table) that need to enumerate all rules
 * without knowing their table names in advance.
 *
 * @return Pointer to the rule, or @c NULL when @p idx is out of range.
 */
const keel_shard_rule_t* keel_router_get_shard_rule_at(const keel_router_t* router,
                                                        size_t idx);

/* ============================================================================
 * Feature 6: Range-based shard rule registration
 * ============================================================================ */

/**
 * @brief Register a RANGE-strategy shard rule.
 *
 * Adds (or overwrites) a shard rule using ordered threshold mapping.
 * @p thresholds must contain @p shard_count entries where @p thresholds[i]
 * is the inclusive upper bound (int64) for shard @c i.  Values that exceed
 * all thresholds map to the last shard.  The final threshold entry is treated
 * as +∞ and may be set to INT64_MAX for clarity.
 *
 * @return KEEL_OK on success,
 *         KEEL_ERR_INVALID_ARG on NULL inputs or @p shard_count == 0,
 *         KEEL_ERR_OVERFLOW when the registry is full or @p shard_count exceeds
 *                           KEEL_SHARD_RANGE_MAX_THRESHOLDS.
 */
keel_error_t keel_router_add_shard_rule_range(keel_router_t* router,
                                               const char*    table,
                                               const char*    column,
                                               const int64_t* thresholds,
                                               size_t         shard_count);

/**
 * @brief Compute a routing plan by trying all registered shard rules.
 *
 * The router iterates over all registered rules.  The first rule that
 * produces a SINGLE or SCATTER outcome wins.  If no rule matches the
 * statement's table, the plan is UNSUPPORTED.
 *
 * This is the primary entry-point for proxy dispatch loops that do not know
 * in advance which table a query targets.
 */
void keel_router_plan_sql(keel_router_t* router,
                          keel_str_t sql,
                          const keel_shard_bound_params_t* params,
                          keel_shard_plan_t* plan);

/* ============================================================================
 * Scatter (fan-out) routing
 * ============================================================================ */

/**
 * @brief Per-shard routing result for a scatter (fan-out) operation.
 *
 * When @ref keel_router_plan_sql returns @c KEEL_SHARD_PLAN_SCATTER, the
 * proxy must fan out the query to every shard.  Populate this structure by
 * calling @ref keel_router_scatter_servers, then iterate @c decisions[0..count-1]
 * and send the query to each non-NULL @c decisions[i].server.
 */
typedef struct keel_scatter_plan {
    keel_route_decision_t  decisions[KEEL_SCATTER_MAX_SHARDS]; /**< One per shard */
    size_t                 count;   /**< Number of shards populated (== rule->shard_count) */
    size_t                 failed;  /**< Shards for which no server could be selected */
    /** Feature 7: Bitmask of shard indices successfully routed for a write.
     *  Bit i is set when decisions[i].server is non-NULL and is_write was true.
     *  Pass to keel_router_record_scatter_write() to update session tracking. */
    uint64_t               participating_shards_mask;
} keel_scatter_plan_t;

/* ============================================================================
 * Feature 7: Multi-shard transaction coordination
 * ============================================================================ */

/**
 * @brief Record the scatter-write shard participation into a session.
 *
 * After a successful scatter write (via keel_router_scatter_servers() with
 * is_write=true), call this to mark which shards participated.  Subsequent
 * single-shard queries within the same transaction are validated against the
 * participation mask and return KEEL_ERR_SHARD_CROSS_TX if they target a
 * non-participating shard.
 *
 * @param session  Mutable session state to update.  Must not be NULL.
 * @param plan     Completed scatter plan with participating_shards_mask set.
 */
void keel_router_record_scatter_write(keel_route_session_t*      session,
                                      const keel_scatter_plan_t* plan);

/**
 * @brief Clear scatter-write participation tracking (e.g. at COMMIT/ROLLBACK).
 *
 * Resets @c has_scatter_write and @c scatter_shards_mask to zero.
 */
void keel_router_clear_scatter_participation(keel_route_session_t* session);

/* ============================================================================
 * Feature 8: Shard migration
 * ============================================================================ */

/**
 * @brief Begin live shard migration for a rule.
 *
 * Marks the named rule as KEEL_SHARD_STATE_MIGRATING and records the source
 * and destination shard indices.  While migration is active:
 * - Read queries for shards involved in the migration → routed to @p dst_shard
 *   (read-from-new).
 * - Write queries → dual-written to both @p src_shard and @p dst_shard, returned
 *   as KEEL_DISPATCH_SCATTER with exactly two decisions.
 *
 * @return KEEL_OK on success, KEEL_ERR_NOT_FOUND if @p table is not registered,
 *         KEEL_ERR_INVALID_ARG on NULL inputs or same src/dst shard.
 */
keel_error_t keel_router_set_shard_migration(keel_router_t* router,
                                              const char*    table,
                                              size_t         src_shard,
                                              size_t         dst_shard);

/**
 * @brief Complete (or cancel) shard migration for a rule.
 *
 * Clears KEEL_SHARD_STATE_MIGRATING; all traffic returns to normal routing.
 *
 * @return KEEL_OK on success, KEEL_ERR_NOT_FOUND if @p table is not registered.
 */
keel_error_t keel_router_clear_shard_migration(keel_router_t* router,
                                                const char*    table);

/**
 * @brief Resolve a SCATTER plan to one routing decision per shard.
 *
 * For each shard index 0..rule->shard_count-1, selects a concrete backend
 * using the router's configured balancing strategy.  @p is_write controls the
 * read/write split decision: pass @c true for INSERT/UPDATE/DELETE scatter
 * (routes each shard to its primary), @c false for SELECT scatter (routes
 * each shard to a replica, with primary fallover when configured).
 *
 * Session state is honoured: in-transaction and temp-table sessions always
 * route to the primary regardless of @p is_write.
 *
 * Shards for which no server is available are counted in @c out->failed; their
 * @c decisions[i].server will be @c NULL.  The function still returns
 * @c KEEL_OK so the caller can decide whether to proceed with partial results.
 *
 * @param router   Router handle.
 * @param session  Session state, or @c NULL to use defaults.
 * @param rule     Shard rule that defines @c shard_count.
 * @param is_write @c true if the scatter query is a write, @c false for reads.
 * @param out      [out] Per-shard routing decisions.
 * @return @c KEEL_OK on success,
 *         @c KEEL_ERR_INVALID_ARG if @p router, @p rule, or @p out is @c NULL
 *                                  or @c rule->shard_count is zero,
 *         @c KEEL_ERR_OVERFLOW if @c rule->shard_count exceeds
 *                               @ref KEEL_SCATTER_MAX_SHARDS.
 */
keel_error_t keel_router_scatter_servers(keel_router_t*              router,
                                          const keel_route_session_t* session,
                                          const keel_shard_rule_t*    rule,
                                          bool                        is_write,
                                          keel_scatter_plan_t*        out);

/**
 * @brief Check if a query can be routed to replicas
 *
 * Quick check without full routing decision.
 *
 * @param router  Router handle  
 * @param qt      Query Tree
 * @param session Session state
 * @return `true` if the query may safely execute on a replica, otherwise `false`.
 */
bool keel_router_can_use_replica(const keel_router_t* router,
                                 const keel_qt_query_t* qt,
                                 const keel_route_session_t* session);

/* ============================================================================
 * Rule persistence from config
 * ============================================================================ */

/**
 * @brief Load shard rules from an INI configuration object.
 *
 * Scans every section whose name begins with @c "shard_rule." and registers the
 * described rule with @p router.  The expected section format is:
 *
 * @code
 * [shard_rule.users]
 * column      = id
 * shard_count = 8
 *
 * [shard_rule.orders]
 * column      = order_id
 * shard_count = 4
 * @endcode
 *
 * Sections with a missing or invalid @c column or @c shard_count value are
 * silently skipped.  Rules for tables that are already registered are
 * overwritten (matching `keel_router_add_shard_rule()` semantics).
 *
 * Shard rules are scoped to the given worker group: only sections matching
 * `[worker_group.<group_name>.shard_rule.<table>]` are loaded.  This allows
 * multiple groups in the same keel instance to have completely independent
 * shard topologies without conflicts.
 *
 * @param router      Router handle.
 * @param config      Parsed INI configuration.
 * @param group_name  Worker group name (e.g. "myapp").  Must not be NULL.
 * @return Number of rules successfully loaded (0 when no sections match or
 *         when any argument is @c NULL).
 */
size_t keel_router_load_shard_rules_from_config(keel_router_t*        router,
                                                const keel_config_t*  config,
                                                const char*           group_name);

/* ============================================================================
 * Combined plan + dispatch
 * ============================================================================ */

/* ============================================================================
 * Ordered-aggregate scatter spec (STRING_AGG, ARRAY_AGG, JSONB_AGG,
 * PERCENTILE_CONT, PERCENTILE_DISC)
 * ============================================================================ */

#define KEEL_SCATTER_MAX_ORD_AGGS  4

/** @brief Identifies which ordered-aggregate function requires scatter rewrite. */
typedef enum keel_ord_agg_func {
    KEEL_ORD_AGG_NONE             = 0,
    KEEL_ORD_AGG_STRING_AGG       = 1,
    KEEL_ORD_AGG_ARRAY_AGG        = 2,
    KEEL_ORD_AGG_JSONB_AGG        = 3,
    KEEL_ORD_AGG_PERCENTILE_CONT  = 4,
    KEEL_ORD_AGG_PERCENTILE_DISC  = 5,
    /* json_object_agg is *not* rewritten per-row.  Each shard computes its own
     * JSON object natively; the proxy concatenates the per-shard objects
     * after stripping their outer braces. */
    KEEL_ORD_AGG_JSON_OBJECT_AGG  = 6,
} keel_ord_agg_func_t;

/**
 * @brief Spec for an ordered aggregate that requires per-row scatter + post-merge re-aggregation.
 *
 * During scatter the aggregate function is replaced by the raw expression (and
 * optionally the ORDER BY key) so that each shard returns individual rows.
 * After all rows are collected they are sorted by the key column and the
 * aggregate is computed in-proxy.
 */
typedef struct keel_ord_agg_spec {
    keel_ord_agg_func_t func;
    double              percentile;    /**< Fraction 0.0-1.0 for PERCENTILE_CONT/DISC */
    char                separator[64]; /**< Delimiter for STRING_AGG */
    /* Pre-built replacement SQL for the shard query rewrite */
    char                replacement_sql[256]; /**< Replaces the agg fn in the SELECT */
    keel_sort_dir_t     key_dir;       /**< ASC/DESC for ORDER BY key */
    bool                has_key;       /**< True when a separate key column is appended */
    /* Byte offsets within the original SQL string for the agg function text */
    uint32_t            agg_sql_start; /**< Start offset of the aggregate fn in SQL */
    uint32_t            agg_sql_len;   /**< Byte length of the aggregate fn text */
} keel_ord_agg_spec_t;

/**
 * @brief High-level dispatch outcome: single-shard or scatter fan-out.
 */
typedef enum keel_dispatch_kind {
    KEEL_DISPATCH_SINGLE  = 0, /**< Statement targets exactly one shard */
    KEEL_DISPATCH_SCATTER = 1, /**< Statement must be sent to all shards */
} keel_dispatch_kind_t;

/**
 * @brief Reason a dispatch decision rejected the statement.
 *
 * Populated on @ref keel_dispatch_result_t when
 * `keel_router_dispatch_sql()` returns @c KEEL_ERR_NOT_SUPPORTED so the
 * engine can surface a precise client-facing error (PostgreSQL SQLSTATE
 * `0A000` / MySQL `ER_NOT_SUPPORTED_YET`) instead of silently falling back
 * to the non-sharded path. @c KEEL_DISPATCH_REJECT_NONE means the
 * NOT_SUPPORTED result was *not* a fail-closed decision (e.g. no shard
 * rule matched the table) and the caller may fall back as before.
 */
typedef enum keel_dispatch_reject {
    KEEL_DISPATCH_REJECT_NONE = 0,
    KEEL_DISPATCH_REJECT_SCATTER_DISABLED, /**< `scatter_merge = off` blocks fan-out */
    KEEL_DISPATCH_REJECT_RECURSIVE_CTE,    /**< `WITH RECURSIVE` cannot be merged correctly */
} keel_dispatch_reject_t;

/**
 * @brief Result of `keel_router_dispatch_sql()`.
 *
 * When @c kind == @ref KEEL_DISPATCH_SINGLE, @c single is populated and
 * @c scatter is zero-filled.  When @c kind == @ref KEEL_DISPATCH_SCATTER,
 * @c scatter is populated and @c single is zero-filled.
 *
 * @c requires_merge is set to @c true for SCATTER dispatches where the proxy
 * must materialise result rows in userspace (e.g. for ORDER BY merge, GROUP BY
 * reduction, or aggregate computation).  When @c true the engine must NOT use
 * the zero-copy splice fast path (@c KEEL_FLOW_SPLICE_BYPASS) because rows
 * must pass through the scatter store.
 *
 * When @c requires_merge is @c true, @c order_keys[0..norder_keys-1] describe
 * the global sort order to apply after all shard rows have been collected, and
 * @c limit_count / @c limit_offset carry the LIMIT/OFFSET to apply after
 * sorting.  @c limit_count == 0 means no LIMIT (unlimited rows).
 */
typedef struct keel_dispatch_result {
    keel_dispatch_kind_t  kind;
    /** When @c kind is unused because dispatch returned NOT_SUPPORTED, this
     *  describes why the statement was refused. @c KEEL_DISPATCH_REJECT_NONE
     *  means "no rule matched, caller may fall back"; any other value means
     *  "fail closed: surface @ref reject_message to the client." */
    keel_dispatch_reject_t reject_reason;
    char                  reject_message[200];
    keel_route_decision_t single;         /**< Valid only for SINGLE dispatch */
    keel_scatter_plan_t   scatter;        /**< Valid only for SCATTER dispatch */
    /** True when the proxy must collect and merge rows across shards.
     *  Implies fast_network_path splice bypass must be disabled for this
     *  query.  Always false for SINGLE dispatch. */
    bool                  requires_merge;

    /* ORDER BY / LIMIT merge spec (Phase C) — valid only when requires_merge */
    keel_sort_key_t       order_keys[KEEL_SCATTER_MAX_ORDER_KEYS];
    uint16_t              norder_keys;   /**< Entries in order_keys[] */
    uint64_t              limit_count;   /**< LIMIT row count; 0 = no limit */
    uint64_t              limit_offset;  /**< OFFSET; 0 = no offset */

    /* Scalar aggregate merge spec (Phase D) — valid only when requires_merge */
    keel_agg_col_spec_t   agg_specs[KEEL_SCATTER_MAX_AGG_COLS];
    uint16_t              nagg_specs;          /**< Entries in agg_specs[] */
    bool                  requires_avg_rewrite; /**< AVG detected; query must be rewritten */
    bool                  requires_count_distinct; /**< COUNT(DISTINCT col) detected; special handling */
    char                  count_distinct_col[64];  /**< Column name for COUNT(DISTINCT col) */

    /* GROUP BY merge spec (Phase E) — valid only when requires_merge */
    keel_group_col_spec_t group_key_cols[KEEL_SCATTER_MAX_GROUP_COLS];
    uint16_t              ngroup_key_cols; /**< Entries in group_key_cols[] */

    /* Window function flag (Phase F) — set when any OVER clause is present.
     * When true the proxy MUST NOT attempt scatter-merge: window functions
     * require global row context and cannot be recomputed from shard results.
     * The call site should either block the scatter, route to a single shard,
     * or return an error to the client. */
    bool                  has_window_funcs;

    /* Window-function forced-single flag (Phase H).
     * Set when has_window_funcs caused the dispatch to fall back from
     * SCATTER to SINGLE.  The kind field will be KEEL_DISPATCH_SINGLE and
     * scatter will be zero-filled.  Callers may use this flag to return an
     * appropriate warning to the client. */
    bool                  window_forced_single;

    /* Window function recomputation specs (Phase F).
     * Populated for global-ranking window functions (ROW_NUMBER, RANK,
     * DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST) that have no PARTITION BY.
     * When nwindow_col_specs > 0 the proxy scatter-merges as normal, then
     * calls keel_pg_result_window_compute() before Phase C (ORDER BY).
     * Requires scatter_store.h for keel_window_col_spec_t. */
    keel_window_col_spec_t window_col_specs[KEEL_SCATTER_MAX_WINDOW_COLS];
    uint16_t               nwindow_col_specs; /**< Entries in window_col_specs[] */

    /* AVG finalize specs (Phase H) — valid when requires_avg_rewrite is set.
     * After merging SUM and COUNT partial results across shards, pass these
     * specs to keel_pg_result_finalize_avg() to compute the final averages.
     * Requires scatter_store.h for keel_avg_finalize_spec_t. */
    keel_avg_finalize_spec_t avg_finalize_specs[KEEL_SCATTER_MAX_AVG_SPECS];
    uint16_t                 navg_finalize_specs; /**< Entries in avg_finalize_specs[] */

    /* HAVING predicates (Phase H) — valid when requires_merge && ngroup_key_cols > 0.
     * Apply these to the merged result via keel_pg_result_apply_having() after
     * keel_pg_result_group_aggs() returns.
     * Requires scatter_store.h for keel_having_pred_t. */
    keel_having_pred_t having_preds[KEEL_SCATTER_MAX_HAVING_PREDS];
    uint16_t           nhaving_preds; /**< Entries in having_preds[] */

    /* Extra aggregate expressions for HAVING evaluation (not in SELECT).
     * When HAVING references an aggregate not present in the SELECT target list,
     * the expression text is stored here so it can be added to the shard query.
     * The corresponding agg_specs entries use col_index ≥ the original SELECT
     * column count.  After HAVING is applied, these extra columns are stripped. */
    char     having_extra_agg_exprs[4][128];
    uint16_t nhaving_extra_agg_exprs; /**< Number of extra HAVING agg columns */

    /* Outer WHERE stripping: when an outer SELECT wraps a CTE/derived-table
     * with a WHERE on aggregated columns, strip the WHERE from shard SQL and
     * apply it post-merge as HAVING. */
    bool requires_outer_where_strip;

    /* Scatter dedup: when identical rows are expected across shards (e.g. a
     * DISTINCT CTE cross-joined with itself), deduplicate adjacent rows after
     * the ORDER BY sort. */
    bool requires_scatter_dedup;

    /* Outer avg finalize: when a CTE exposes COUNT + SUM columns and the outer
     * SELECT computes avg = ROUND(sum/count), finalize without column removal. */
    bool requires_outer_avg_finalize;

    /* Ordered-aggregate specs (STRING_AGG, ARRAY_AGG, JSONB_AGG,
     * PERCENTILE_CONT, PERCENTILE_DISC).  When nord_agg_specs > 0 the shard
     * SQL is rewritten to return individual rows and the aggregate is computed
     * in-proxy after all rows are collected. */
    keel_ord_agg_spec_t ord_agg_specs[KEEL_SCATTER_MAX_ORD_AGGS];
    uint16_t            nord_agg_specs;

    /* 2PC coordinator (Phase H) — valid when twopc_required is true.
     * Points to a heap-allocated keel_2pc_coord_t initialised and begun by
     * keel_router_dispatch_sql() for scatter write transactions.
     * The caller MUST free this pointer with keel_free() when done.
     * Cast to (keel_2pc_coord_t*) after including scatter_2pc.h. */
    bool  twopc_required; /**< True when a 2PC coordinator was allocated */
    void* twopc;          /**< keel_2pc_coord_t*; caller must keel_free() */
} keel_dispatch_result_t;

/**
 * @brief Release heap resources owned by a dispatch result.
 *
 * Call this after you are finished with a @ref keel_dispatch_result_t that was
 * populated by @ref keel_router_dispatch_sql or
 * @ref keel_router_dispatch_sql_timed.  Currently the only heap-allocated
 * field is the 2PC coordinator (@c twopc), which is freed when
 * @c twopc_required is true.  Safe to call when @c twopc_required is false or
 * on a zero-initialised result.
 */
static inline void keel_dispatch_result_cleanup(keel_dispatch_result_t* out) {
    if (out && out->twopc_required && out->twopc) {
        keel_free(out->twopc);
        out->twopc          = NULL;
        out->twopc_required = false;
    }
}

/**
 * @brief Plan and dispatch a SQL statement in a single call.
 *
 * This is the primary entry-point for proxy call sites.  It replaces the
 * common two-step boilerplate of calling `keel_router_plan_sql()` and then
 * either routing a single shard or building a scatter plan:
 *
 * 1. Iterates the router's registered shard rules to determine the plan.
 * 2. SINGLE  — routes the statement to the one target shard and populates
 *              @c out->single.  The read/write split decision is derived from
 *              the parsed query tree.
 * 3. SCATTER — resolves one routing decision per shard via
 *              `keel_router_scatter_servers()` and populates @c out->scatter.
 *              @p is_write controls the read/write split for each shard.
 * 4. UNSUPPORTED — returns @c KEEL_ERR_NOT_SUPPORTED without modifying @p out.
 *
 * @param router    Router handle with at least one registered shard rule.
 * @param sql       SQL text to analyse and dispatch.
 * @param session   Session state, or @c NULL for defaults.
 * @param params    Bound parameter values for @c $N placeholders, or @c NULL.
 * @param is_write  Hint for scatter routing: @c true if the statement is a
 *                  write (routes each shard to its primary), @c false for
 *                  reads.  Ignored for SINGLE dispatch (derived from the AST).
 * @param out       [out] Dispatch result (always written on @c KEEL_OK).
 * @return @c KEEL_OK on success,
 *         @c KEEL_ERR_INVALID_ARG when @p router, @p sql, or @p out is @c NULL,
 *         @c KEEL_ERR_NOT_SUPPORTED when no registered rule matches the
 *                                   statement.
 */
keel_error_t keel_router_dispatch_sql(keel_router_t*                   router,
                                      keel_str_t                        sql,
                                      const keel_route_session_t*       session,
                                      const keel_shard_bound_params_t*  params,
                                      bool                              is_write,
                                      keel_dispatch_result_t*           out);

/* ============================================================================
 * Feature 15: Query timeout enforcement
 * ============================================================================ */

/**
 * @brief Dispatch a SQL statement with an explicit query timeout.
 *
 * Behaves identically to `keel_router_dispatch_sql()` except that if the
 * routing decision (including shard key extraction and plan selection) takes
 * longer than `timeout`, the call returns `KEEL_ERR_QUERY_TIMEOUT` (-901)
 * immediately instead of continuing.
 *
 * A `timeout` value of zero means "use `router->config.query_timeout`"; if
 * that is also zero the call degrades to an untimed `keel_router_dispatch_sql()`.
 *
 * The timeout is checked once before the routing decision is attempted and
 * once after the shard plan is computed.  It does **not** govern the actual
 * query execution over the wire — that is the responsibility of the engine's
 * timer wheel.
 *
 * @param router    Router handle.
 * @param sql       SQL text.
 * @param session   Per-connection routing state.
 * @param params    Bound parameters (may be NULL).
 * @param is_write  True for INSERT / UPDATE / DELETE / DDL.
 * @param timeout   Explicit timeout; 0 = use config default.
 * @param out       Dispatch result.
 * @return `KEEL_OK`, `KEEL_ERR_QUERY_TIMEOUT`, or any error from
 *         `keel_router_dispatch_sql()`.
 */
keel_error_t keel_router_dispatch_sql_timed(
    keel_router_t*                   router,
    keel_str_t                        sql,
    const keel_route_session_t*       session,
    const keel_shard_bound_params_t*  params,
    bool                              is_write,
    keel_duration_t                   timeout,
    keel_dispatch_result_t*           out);

/* ============================================================================
 * Scatter result aggregation
 * ============================================================================ */

/**
 * @brief Opaque aggregator for scatter (fan-out) responses.
 *
 * Callers allocate this on the stack or heap, initialise it with
 * @ref keel_route_agg_init, and then call @ref keel_route_agg_feed
 * once per shard response — whether that response carried data or not.
 *
 * @code
 * // Row-union example
 * static void my_merge(keel_route_agg_t* r, size_t shard,
 *                      const void* rows, size_t n, void* ctx) {
 *     my_row_t* dst = (my_row_t*)r->data;
 *     memcpy(dst + r->total_rows, rows, n * sizeof(my_row_t));
 * }
 *
 * keel_route_agg_t result;
 * keel_route_agg_init(&result, my_merge, NULL);
 * result.data = calloc(MAX_ROWS, sizeof(my_row_t));
 *
 * for (size_t i = 0; i < scatter.count; i++) {
 *     my_row_t shard_rows[...];
 *     size_t   row_count = fetch_from_shard(scatter.decisions[i].server, shard_rows);
 *     keel_route_agg_feed(&result, i, shard_rows, row_count);
 * }
 * @endcode
 */

/** Forward declaration so the callback typedef can reference the struct. */
typedef struct keel_route_agg keel_route_agg_t;

/**
 * @brief Merge callback invoked once per shard response.
 *
 * @param result      Aggregation context (includes @c data and counters).
 * @param shard_index Zero-based index of the responding shard.
 * @param rows        Opaque pointer to the shard's row data.
 * @param row_count   Number of rows/items in @p rows.
 * @param user_ctx    Caller-supplied context passed to @ref keel_route_agg_init.
 */
typedef void (*keel_route_merge_fn)(keel_route_agg_t* result,
                                      size_t                 shard_index,
                                      const void*            rows,
                                      size_t                 row_count,
                                      void*                  user_ctx);

/**
 * @brief Aggregation state for a scatter fan-out response.
 *
 * Fields below @c shards_completed are set by the framework; @c data is
 * caller-managed (point it at any buffer before calling
 * @ref keel_route_agg_feed).
 */
struct keel_route_agg {
    keel_route_merge_fn  merge;             /**< Optional merge callback */
    void*                  user_ctx;          /**< Forwarded to @c merge */
    void*                  data;             /**< Caller-managed output buffer */
    uint64_t               total_rows;        /**< Cumulative rows across all shards */
    size_t                 shards_completed;  /**< Shards that responded with data */
    size_t                 shards_failed;     /**< Shards that provided NULL rows */
};

/**
 * @brief Zero-fill and configure a scatter aggregation context.
 *
 * @param result   Aggregation context to initialise.
 * @param merge    Merge callback, or @c NULL if no per-row aggregation is needed.
 * @param user_ctx Opaque value forwarded to @p merge on every call.
 */
void keel_route_agg_init(keel_route_agg_t* result,
                              keel_route_merge_fn  merge,
                              void*                  user_ctx);

/**
 * @brief Deliver one shard's response to the aggregation context.
 *
 * If @p rows is @c NULL (shard failed or returned nothing), @c shards_failed
 * is incremented and the @c merge callback is not invoked.  Otherwise the
 * callback is invoked (if non-NULL) and @c shards_completed and @c total_rows
 * are updated.
 *
 * This function is intentionally simple: it does no allocation and never
 * fails; all complex work is delegated to the caller-supplied @c merge function.
 *
 * @param result      Aggregation context.
 * @param shard_index Zero-based shard index (informational, forwarded to @c merge).
 * @param rows        Shard's row data, or @c NULL on failure.
 * @param row_count   Number of rows in @p rows (ignored when @p rows is @c NULL).
 */
void keel_route_agg_feed(keel_route_agg_t* result,
                              size_t                 shard_index,
                              const void*            rows,
                              size_t                 row_count);

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Copy the router's aggregate statistics into a caller-supplied buffer.
 *
 * @param router Router handle.
 * @param stats Output statistics structure to populate.
 * @return
 */
void keel_router_get_stats(const keel_router_t* router, keel_router_stats_t* stats);

/**
 * @brief Record a completed scatter merge operation.
 *
 * The caller must measure wall-clock nanoseconds around the merge step (i.e.
 * all calls to keel_pg_result_sort / keel_pg_result_group_aggs / etc. for a
 * single scatter query) and pass the elapsed time here.  Thread-safe (uses
 * atomic accumulation).
 *
 * @param router       Router handle.
 * @param elapsed_ns   Wall-clock duration of the merge in nanoseconds.
 */
void keel_router_record_scatter_merge_ns(keel_router_t* router, uint64_t elapsed_ns);

/**
 * @brief Increment the per-kind unsupported-scatter-pattern counter.
 *
 * Call this from the dispatcher (or engine) every time a scatter query is
 * dispatched whose result correctness relies on a SQL pattern KEEL does not
 * fully merge (see @ref keel_scatter_unsupported_kind_t).  Operators surface
 * the counter via Prometheus as
 * @c keel_scatter_unsupported_pattern_total{kind="..."} for alerting.
 *
 * Counter increments are non-atomic but tolerated for monotonic counters
 * (matching the convention used by other router_stats counters).
 *
 * @param router Router handle.  No-op if NULL.
 * @param kind   Pattern kind; ignored if out of range.
 */
void keel_router_track_unsupported_pattern(keel_router_t* router,
                                           keel_scatter_unsupported_kind_t kind);

/**
 * @brief Record 2PC participant outcomes for a finished coordinator.
 *
 * Called after a scatter write transaction completes (commit or rollback).
 * Increments the appropriate 2PC counters in the router stats.
 *
 * @param router        Router handle.
 * @param started       1 if a new coordinator was begun, 0 otherwise.
 * @param prepared      Number of shards that returned PREPARE success.
 * @param prepare_failed Number of shards that rejected PREPARE.
 * @param committed     Number of shards that completed COMMIT PREPARED.
 * @param rolled_back   Number of shards that were rolled back.
 */
void keel_router_record_2pc_outcome(keel_router_t* router,
                                    uint64_t started,
                                    uint64_t prepared,
                                    uint64_t prepare_failed,
                                    uint64_t committed,
                                    uint64_t rolled_back);

/**
 * @brief Reset aggregate router statistics to zero.
 *
 * @param router Router handle.
 * @return
 */
void keel_router_reset_stats(keel_router_t* router);

/**
 * @brief Emit a human-readable dump of router state for debugging.
 *
 * @param router Router handle.
 * @param out Output stream that receives the dump.
 * @return
 */
void keel_router_dump(const keel_router_t* router, FILE* out);

/* ============================================================================
 * Feature 14: Prometheus metrics endpoint
 * ============================================================================ */

/**
 * @brief Write Prometheus text-format metrics from the router's live stats.
 *
 * Produces TYPE/HELP + one line per metric family.  The output is valid
 * Prometheus text exposition format 0.0.4.
 *
 * Per-shard gauge: `keel_router_shard_routes{shard="<N>"}` is emitted for
 * every shard index 0…KEEL_SCATTER_MAX_SHARDS-1 that has a non-zero counter.
 *
 * @param router    Router handle.
 * @param buf       Output buffer.
 * @param buf_size  Capacity of `buf` in bytes.
 * @return Number of bytes written (excluding NUL terminator), or 0 on error.
 */
size_t keel_router_write_prometheus(const keel_router_t* router,
                                    char*                buf,
                                    size_t               buf_size);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_ROUTER_H */
