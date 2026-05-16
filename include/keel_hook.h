/**
 * @file keel_hook.h
 * @brief Public API for KEEL's hook registry, execution context, and scripting bridges.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * The hook subsystem is a **traversal subsystem** — it threads through every
 * major KEEL subsystem and exposes them all at well-defined extension points.
 * Operators can inject policy, auditing, routing control, observability, or
 * request mutation at any stage of the query lifecycle without modifying the
 * core engine.
 *
 * Design principles:
 *   - **Zero performance cost when not used.** Every hook site is guarded by a
 *     cheap bitmask check (`KEEL_HOOK_FIRED_FOR`).  The mask is a 32-bit value
 *     cached per-worker at startup from `keel_hook_registry_active_mask()`.
 *     When no hooks are registered for a given point the branch is always-not-
 *     taken and the entire context-fill + fire block is eliminated by the
 *     branch predictor.  There is no dynamic dispatch on the cold path.
 *   - **Full subsystem access.**  Hook contexts carry the complete parsed query
 *     tree (AST), shard key values, full scatter merge plans (ORDER BY keys,
 *     aggregate specs, GROUP BY, HAVING, window functions, AVG rewrite specs,
 *     LIMIT/OFFSET), 2PC coordinator state, per-shard health, backend health,
 *     and spill statistics.  Mutable output channels let hooks influence
 *     routing, query classification, and abort execution.
 *   - **Per-registry isolation.** Each worker group owns its own registry; hooks
 *     registered in one group never fire in another.
 *   - **Priority chains.** Each hook point has a priority-ordered chain.  Lower
 *     numeric priority fires first.  Execution short-circuits on the first hook
 *     that returns false.
 *   - **Scripting bridges.** Callbacks can be native C, Lua, Python, or loaded
 *     from native .so plugins through a versioned descriptor.
 *
 * Complete pipeline (10 hook points):
 *
 *   Query path:
 *     1. AFTER_QUERY_READ      — raw query bytes received from client
 *     2. AFTER_QUERY_PARSE     — SQL parsed / classified
 *     3. BEFORE_ROUTE          — before routing decision (can bias primary/replica)
 *     4. AFTER_ROUTE           — routing decided; shard index and dispatch kind known
 *     5. BEFORE_SCATTER        — before fan-out to all shards (scatter queries only)
 *     6. AFTER_SCATTER         — after scatter merge complete; result stats visible
 *     7. BEFORE_SEND           — before payload forwarded to backend (single-shard)
 *
 *   Infrastructure path:
 *     8. ON_BACKEND_CONNECT    — a backend TCP connection is established
 *     9. ON_BACKEND_DISCONNECT — a backend TCP connection is torn down
 *    10. ON_HEALTH_CHANGE      — probe detected a server health state transition
 *
 * Hook contexts:
 *   Every hook receives the base `keel_hook_ctx_t` which contains session
 *   identity, query text, the full parsed query tree (`ctx->query_tree`),
 *   classification, and routing output channels.
 *   Infrastructure hooks additionally receive a sub-context pointer
 *   (`ctx->ext`) that is populated only for the relevant hook point:
 *     - AFTER_ROUTE / BEFORE_SCATTER / AFTER_SCATTER  → `keel_hook_shard_ctx_t*`
 *     - ON_BACKEND_CONNECT / ON_BACKEND_DISCONNECT    → `keel_hook_backend_ctx_t*`
 *     - ON_HEALTH_CHANGE                              → `keel_hook_health_ctx_t*`
 *   `ctx->ext` is NULL for all other points.  Hooks MUST null-check before use.
 *
 *   The `query_tree` pointer is valid from AFTER_QUERY_PARSE onward and
 *   remains valid for the entire hook invocation.  It is NULL for
 *   AFTER_QUERY_READ (parse not yet done) and for infrastructure hooks.
 */

#ifndef KEEL_HOOK_H
#define KEEL_HOOK_H

#include "keel_types.h"
#include "keel_error.h"
#include "keel/sql/query_tree.h"         /* keel_qt_query_t — full parsed AST */
#include "keel/core/scatter_store.h"     /* keel_sort_key_t, keel_agg_col_spec_t, etc. */
#include "keel/core/sharding.h"          /* keel_shard_key_t */

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Hook Points
 * ============================================================================ */

/**
 * @brief Hook firing points in the query + infrastructure pipeline.
 *
 * Numeric values are stable ABI.  New points are appended before
 * KEEL_HOOK_POINT_COUNT; existing values never change.
 */
typedef enum keel_hook_point {
    /* ---- Query path ---- */
    KEEL_HOOK_AFTER_QUERY_READ   = 0,  /**< After reading raw query from client */
    KEEL_HOOK_AFTER_QUERY_PARSE  = 1,  /**< After SQL parse / classification */
    KEEL_HOOK_BEFORE_ROUTE       = 2,  /**< Before routing decision */
    KEEL_HOOK_AFTER_ROUTE        = 3,  /**< After routing; shard/dispatch kind known */
    KEEL_HOOK_BEFORE_SCATTER     = 4,  /**< Before fan-out to all shards */
    KEEL_HOOK_AFTER_SCATTER      = 5,  /**< After scatter merge; result stats visible */
    KEEL_HOOK_BEFORE_SEND        = 6,  /**< Before forwarding payload to backend */

    /* ---- Infrastructure path ---- */
    KEEL_HOOK_ON_BACKEND_CONNECT    = 7,  /**< Backend TCP connection established */
    KEEL_HOOK_ON_BACKEND_DISCONNECT = 8,  /**< Backend TCP connection torn down */
    KEEL_HOOK_ON_HEALTH_CHANGE      = 9,  /**< Probe detected health state transition */

    KEEL_HOOK_POINT_COUNT = 10,
} keel_hook_point_t;

/**
 * @brief Bitmask helpers — one bit per hook point.
 *
 * `KEEL_HOOK_BIT(pt)` is the bitmask for hook point @p pt.
 * `KEEL_HOOK_FIRED_FOR(reg, pt)` is the zero-cost hot-path guard:
 *   it reads a cached 32-bit mask and returns true only when at least
 *   one hook is registered for @p pt.  No function call, no lock.
 *
 * Usage in engine hot paths:
 * @code
 *   if (KEEL_HOOK_FIRED_FOR(worker->hooks, KEEL_HOOK_AFTER_ROUTE)) {
 *       keel_hook_ctx_t hctx; keel_hook_shard_ctx_t sctx;
 *       engine_fill_hook_ctx(&hctx, session, &act);
 *       engine_fill_shard_ctx(&sctx, &dr);
 *       hctx.ext = &sctx;
 *       keel_hook_fire(worker->hooks, KEEL_HOOK_AFTER_ROUTE, &hctx);
 *   }
 * @endcode
 */
#define KEEL_HOOK_BIT(pt)            (1u << (unsigned)(pt))
#define KEEL_HOOK_FIRED_FOR(reg, pt) \
    ((keel_hook_registry_active_mask(reg) & KEEL_HOOK_BIT(pt)) != 0u)

/**
 * @brief Human-readable hook point names.
 */
static inline const char* keel_hook_point_name(keel_hook_point_t pt) {
    static const char* names[] = {
        "after_query_read",
        "after_query_parse",
        "before_route",
        "after_route",
        "before_scatter",
        "after_scatter",
        "before_send",
        "on_backend_connect",
        "on_backend_disconnect",
        "on_health_change",
    };
    return (pt < KEEL_HOOK_POINT_COUNT) ? names[pt] : "unknown";
}

/* ============================================================================
 * Extension Contexts — populated per hook point; NULL otherwise
 * ============================================================================ */

/** Maximum shard count visible to hook contexts. */
#ifndef KEEL_HOOK_MAX_SHARDS
#define KEEL_HOOK_MAX_SHARDS 64
#endif

/**
 * @brief Dispatch kind as seen by hook contexts.
 */
typedef enum keel_hook_dispatch_kind {
    KEEL_HOOK_DISPATCH_UNKNOWN  = 0, /**< Not yet determined (before AFTER_ROUTE) */
    KEEL_HOOK_DISPATCH_SINGLE   = 1, /**< Query targets exactly one shard */
    KEEL_HOOK_DISPATCH_SCATTER  = 2, /**< Query fans out to all shards */
} keel_hook_dispatch_kind_t;

/**
 * @brief Per-shard routing summary visible to AFTER_ROUTE, BEFORE_SCATTER,
 *        and AFTER_SCATTER hooks.
 *
 * This is a read-only snapshot — it does NOT reference internal routing
 * structures that may change between the snapshot and the hook's use of them.
 *
 * `shard_key` is populated for SINGLE dispatch when the routing decision was
 * driven by a resolved shard key value (kind != KEEL_SHARD_KEY_NONE).  For
 * SCATTER dispatch every element reflects the key kind that produced the
 * fan-out; the key value fields are only meaningful for SINGLE dispatch.
 */
typedef struct keel_hook_shard_info {
    size_t   shard_index;          /**< Zero-based shard index */
    bool     is_write;             /**< True when query is routed as a write */
    bool     is_healthy;           /**< Probe-reported health at dispatch time */
    bool     server_available;     /**< False if no server could be selected */
    const char* host;              /**< Backend hostname (may be NULL) */
    uint16_t    port;              /**< Backend port */
    /** Shard routing key resolved for this decision.
     *  Use `ctx->query_tree->ast` + keel_shard_extract_key_ast() to
     *  re-derive if you need the key in a different form. */
    keel_shard_key_t shard_key;    /**< Resolved routing key (kind==NONE if unknown) */
} keel_hook_shard_info_t;

/**
 * @brief Sharding extension context.
 *
 * Populated and attached to `ctx->ext` for the following hook points:
 *   - KEEL_HOOK_AFTER_ROUTE     (routing decision is final, scatter not yet started)
 *   - KEEL_HOOK_BEFORE_SCATTER  (about to fan-out; hook can veto by returning false)
 *   - KEEL_HOOK_AFTER_SCATTER   (fan-out + merge complete; result stats available)
 *
 * Fields marked [mutable] may be written by the hook; changes are applied
 * before the engine continues.  All other fields are read-only snapshots.
 *
 * **Full merge plan access**: when `requires_merge` is true the following
 * groups mirror the corresponding fields in `keel_dispatch_result_t` exactly:
 *   - `order_keys[0..norder_keys-1]`          — ORDER BY sort specification
 *   - `agg_specs[0..nagg_specs-1]`            — scalar aggregate functions
 *   - `group_key_cols[0..ngroup_key_cols-1]`  — GROUP BY columns
 *   - `having_preds[0..nhaving_preds-1]`      — HAVING predicates
 *   - `avg_finalize_specs[0..navg_finalize_specs-1]` — AVG rewrite specs
 *   - `window_col_specs[0..nwindow_col_specs-1]`     — window function specs
 *   - `limit_count` / `limit_offset`          — LIMIT / OFFSET
 * Fields are zero when `requires_merge` is false.
 */
typedef struct keel_hook_shard_ctx {
    /* Dispatch decision */
    keel_hook_dispatch_kind_t  dispatch_kind;   /**< SINGLE or SCATTER */
    size_t                     shard_count;     /**< Shards involved (1 for SINGLE) */
    keel_hook_shard_info_t     shards[KEEL_HOOK_MAX_SHARDS]; /**< Per-shard info */

    /* Single-dispatch extras (valid when dispatch_kind == SINGLE) */
    size_t                     single_shard_index; /**< Index of the selected shard */

    /* Scatter extras (valid when dispatch_kind == SCATTER) */
    size_t    scatter_shards_ok;    /**< Shards that returned a result */
    size_t    scatter_shards_fail;  /**< Shards that failed or had no server */
    uint64_t  scatter_elapsed_us;   /**< Wall-clock elapsed for scatter+merge (µs) */
    uint64_t  scatter_rows_merged;  /**< Total rows after merge (AFTER_SCATTER only) */
    bool      scatter_spilled;      /**< True if merge spilled to disk */

    /* Merge plan — mirrors keel_dispatch_result_t fields exactly.
     * All fields below are zero/false when requires_merge is false. */
    bool      requires_merge;       /**< True when proxy must collect and merge rows */
    bool      requires_avg_rewrite; /**< AVG detected; rewrite needed */
    bool      requires_count_distinct; /**< COUNT(DISTINCT col) detected */
    char      count_distinct_col[64];  /**< Column name for COUNT(DISTINCT) */
    bool      has_window_funcs;     /**< OVER clause present */
    bool      window_forced_single; /**< Window forced SCATTER→SINGLE fallback */

    /* ORDER BY / LIMIT */
    keel_sort_key_t  order_keys[KEEL_SCATTER_MAX_ORDER_KEYS];
    uint16_t         norder_keys;   /**< Entries in order_keys[] */
    uint64_t         limit_count;   /**< LIMIT row count; 0 = no limit */
    uint64_t         limit_offset;  /**< OFFSET; 0 = no offset */

    /* Aggregate specs */
    keel_agg_col_spec_t agg_specs[KEEL_SCATTER_MAX_AGG_COLS];
    uint16_t            nagg_specs; /**< Entries in agg_specs[] */

    /* GROUP BY specs */
    keel_group_col_spec_t group_key_cols[KEEL_SCATTER_MAX_GROUP_COLS];
    uint16_t              ngroup_key_cols; /**< Entries in group_key_cols[] */

    /* HAVING predicates */
    keel_having_pred_t having_preds[KEEL_SCATTER_MAX_HAVING_PREDS];
    uint16_t           nhaving_preds; /**< Entries in having_preds[] */

    /* AVG finalize specs */
    keel_avg_finalize_spec_t avg_finalize_specs[KEEL_SCATTER_MAX_AVG_SPECS];
    uint16_t                 navg_finalize_specs; /**< Entries in avg_finalize_specs[] */

    /* Window function specs */
    keel_window_col_spec_t window_col_specs[KEEL_SCATTER_MAX_WINDOW_COLS];
    uint16_t               nwindow_col_specs; /**< Entries in window_col_specs[] */

    /* 2PC coordinator state */
    bool     twopc_required;            /**< True when distributed 2PC is active */
    uint64_t participating_shards_mask; /**< Bitmask of shards in the 2PC write */

    /* [mutable] Hooks may force query rejection after routing */
    bool      veto_execution;       /**< [mutable] Set true to abort the query */
    char      veto_reason[128];     /**< [mutable] Error detail sent to client */
} keel_hook_shard_ctx_t;

/**
 * @brief Backend connection extension context.
 *
 * Populated and attached to `ctx->ext` for:
 *   - KEEL_HOOK_ON_BACKEND_CONNECT
 *   - KEEL_HOOK_ON_BACKEND_DISCONNECT
 *
 * For CONNECT hooks, returning false closes the connection immediately and
 * the client receives a connection-refused error.
 */
typedef struct keel_hook_backend_ctx {
    /* Connection identity */
    const char*  host;           /**< Backend hostname */
    uint16_t     port;           /**< Backend port */
    int          backend_fd;     /**< File descriptor (-1 if not yet open) */
    size_t       shard_index;    /**< Shard this backend belongs to */
    const char*  pool_name;      /**< Worker group / pool name */

    /* Connection state */
    bool         is_tls;         /**< TLS-encrypted connection? */
    bool         is_primary;     /**< Primary (writable) server? */
    bool         is_cloud;       /**< Cloud-managed auth (IAM/OAuth)? */
    const char*  cloud_auth_type;/**< "aws_iam", "gcp_iam", "azure_ad", or NULL */

    /* Disconnect-only fields (undefined on CONNECT) */
    bool         was_error;      /**< True if disconnect was due to an error */
    const char*  disconnect_reason; /**< Human-readable reason or NULL */
    uint64_t     connection_age_us; /**< Time the connection was open (µs) */
    uint64_t     queries_served;    /**< Queries routed over this connection */
} keel_hook_backend_ctx_t;

/**
 * @brief Health change extension context.
 *
 * Populated and attached to `ctx->ext` for KEEL_HOOK_ON_HEALTH_CHANGE.
 *
 * Fires on the probe thread — callbacks MUST be non-blocking.
 * Returning false from an ON_HEALTH_CHANGE hook is advisory-only and does
 * not suppress the state transition.
 */
typedef struct keel_hook_health_ctx {
    /* Server identity */
    const char*  host;           /**< Backend hostname */
    uint16_t     port;           /**< Backend port */
    size_t       shard_index;    /**< Shard index in the router */
    bool         is_primary;     /**< Is this the primary (writable) server? */

    /* Health transition */
    int          prev_health;    /**< Previous keel_health_status_t value */
    int          curr_health;    /**< New keel_health_status_t value */
    const char*  prev_health_str;/**< e.g. "UP", "DOWN", "UNKNOWN" */
    const char*  curr_health_str;/**< e.g. "UP", "DOWN", "UNKNOWN" */

    /* Probe measurement */
    uint64_t     probe_latency_us; /**< Round-trip latency of the probe check (µs) */
    const char*  error_detail;   /**< Probe error string, or NULL if healthy */

    /* [mutable] Hook can suppress log emission for this event */
    bool         suppress_log;   /**< [mutable] Set true to skip default log line */
} keel_hook_health_ctx_t;

/* ============================================================================
 * Hook Context — passed to every callback
 * ============================================================================ */

/**
 * @brief Routing hint (mutable by hooks).
 */
typedef enum keel_hook_route {
    KEEL_HOOK_ROUTE_WRITE   = 0,
    KEEL_HOOK_ROUTE_READ    = 1,
    KEEL_HOOK_ROUTE_ANY     = 2,
} keel_hook_route_t;

/* Backward-compatible aliases */
#define KEEL_HOOK_ROUTE_PRIMARY  KEEL_HOOK_ROUTE_WRITE
#define KEEL_HOOK_ROUTE_REPLICA  KEEL_HOOK_ROUTE_READ

/**
 * @brief Context passed to hook callbacks.
 *
 * The context is the central hook interface contract. Some fields are purely
 * observational, while others are intentionally mutable and act as output
 * channels back to the engine. Hooks do not allocate or return a separate
 * result object; instead they edit specific fields in-place before returning.
 * This keeps the hot path allocation-free and makes it obvious which parts of
 * the engine state may be influenced by extensions.
 *
 * Mutability rules:
 *   - Session identity fields are observational only.
 *   - Classification and routing fields may be rewritten by hooks.
 *   - `pin_update` and `pin_clear` are output-style fields consumed by the
 *     engine after the hook chain finishes.
 *   - `error_msg` is an output buffer used only when a hook returns `false`.
 *   - `ext` points to a hook-point-specific extension struct (NULL for points
 *     that have no extension).  Hooks MUST null-check before use.
 *
 * Lifetime: valid only for the duration of the callback invocation.
 */
typedef struct keel_hook_ctx {
    /* ---- Read-only session info ---- */
    uint64_t        session_id;         /**< Unique session ID */
    const char*     username;           /**< Authenticated user (NUL-terminated) */
    const char*     database;           /**< Target database (NUL-terminated) */
    int             client_fd;          /**< Client file descriptor */
    int             server_fd;          /**< Backend file descriptor (-1 if none) */
    bool            in_transaction;     /**< Inside BEGIN..COMMIT? */
    uint32_t        query_count;        /**< Queries processed in session */

    /* ---- Mutable query data ---- */
    const uint8_t*  raw_query;          /**< Raw query bytes (full frame) */
    size_t          raw_query_len;      /**< Length of raw frame */
    const char*     sql_text;           /**< SQL text extracted (NUL-terminated) */
    size_t          sql_text_len;       /**< Length of SQL text */

    /* ---- Mutable classification ---- */
    uint32_t        query_type;         /**< keel_query_type_t */
    uint32_t        query_flags;        /**< keel_query_flags_t */
    uint32_t        effect_flags;       /**< keel_query_effect_flags_t */
    bool            needs_primary;      /**< Routing: must go to primary? */

    /* ---- Mutable routing ---- */
    keel_hook_route_t route_hint;       /**< Routing hint (can be changed) */
    uint32_t        pin_update;         /**< Pin flags to add */
    uint32_t        pin_clear;          /**< Pin flags to remove */
    bool            splice_eligible;    /**< Zero-copy eligible? */

    /* ---- Backend payload (BEFORE_SEND only) ---- */
    const uint8_t*  be_payload;         /**< Payload about to be sent */
    size_t          be_payload_len;     /**< Payload length */

    /* ---- Parsed query tree (NULL for AFTER_QUERY_READ and infra hooks) ----
     *
     * Available from AFTER_QUERY_PARSE onwards.  Provides full structural
     * access to the SQL: table references, column references, target table,
     * subqueries, prepared statement names, and the raw AST node.
     *
     * To extract the shard routing key:
     *   keel_shard_key_t key;
     *   keel_shard_extract_key_ast(ctx->query_tree->ast, rule, &key);
     *
     * The pointer is read-only; the AST lifetime is managed by the engine.
     * Hooks MUST NOT cache or free this pointer.
     */
    const keel_qt_query_t* query_tree;  /**< Full parsed query tree (NULL if unavailable) */

    /* ---- Extension context (hook-point-specific, NULL when unused) ----
     *
     * Point               Type
     * -------             ----
     * AFTER_ROUTE         keel_hook_shard_ctx_t*
     * BEFORE_SCATTER      keel_hook_shard_ctx_t*
     * AFTER_SCATTER       keel_hook_shard_ctx_t*
     * ON_BACKEND_CONNECT  keel_hook_backend_ctx_t*
     * ON_BACKEND_DISCONNECT keel_hook_backend_ctx_t*
     * ON_HEALTH_CHANGE    keel_hook_health_ctx_t*
     * (all other points)  NULL
     */
    void*               ext;                /**< Extension context pointer (NULL or typed above) */

    /** Current hook invocation point — set by the engine before firing.
     *  Bridges use this to safely cast ctx->ext to the correct type. */
    keel_hook_point_t   hook_point;         /**< Which hook point is being invoked */

    /* ---- SQL rewrite output (BEFORE_ROUTE only, set by query-rule hook) ----
     *
     * When a BEFORE_ROUTE hook wants to substitute the SQL with a different
     * statement, it sets `rewrite_sql` + `rewrite_sql_len`.  The engine picks
     * this up after the hook chain returns and replaces `act.sql_view` (used
     * for routing and analysis) and `act.be_payload` (the bytes forwarded to
     * the backend) before dispatching the query.
     *
     * `rewrite_be_payload` must be a pre-built PostgreSQL Simple Query ('Q')
     * wire message whose lifetime exceeds this invocation — typically a
     * pointer into a long-lived config buffer, **not** a stack variable.
     *
     * Leave all four fields as NULL/0 (the zero-init default) to skip rewrite.
     */
    const char*     rewrite_sql;            /**< Replacement SQL text (NUL-terminated) */
    size_t          rewrite_sql_len;        /**< Length of replacement SQL */
    const uint8_t*  rewrite_be_payload;     /**< Pre-built 'Q' wire message for backend */
    size_t          rewrite_be_payload_len; /**< Length of rewrite_be_payload */

    /* ---- Query-rule SQL rewrite policy knobs (BEFORE_ROUTE only) ----
     *
     * When any of these are set by a query-rule hook, engine_flow.c calls
     * keel_sql_rewrite() to prepend the corresponding SET statements before
     * the SQL text is sent to the backend. Zero/NULL = no effect (default).
     * These are consulted AFTER the rewrite_sql/rewrite_be_payload fields.
     */
    bool        sql_rewrite_add_read_only; /**< Prepend SET TRANSACTION READ ONLY */
    bool        sql_rewrite_add_timeout;   /**< Prepend SET LOCAL statement_timeout */
    int         sql_rewrite_timeout_ms;    /**< Timeout value for add_timeout (ms) */
    const char* sql_rewrite_search_path;   /**< Prepend SET search_path TO '...' (not owned) */

    /* ---- User-defined data ---- */
    void*           user_data;          /**< Per-hook registration data */

    /* ---- Hook result scratch ---- */
    char            error_msg[256];     /**< If returning false, set reason */
} keel_hook_ctx_t;

/* ============================================================================
 * Native Hook Callback Type
 * ============================================================================ */

/**
 * @brief Native hook callback signature.
 *
 * @param ctx  Hook context (read/write).
 * @return true to continue processing, false to abort the query.
 *
 * When returning false, set ctx->error_msg to a human-readable reason.
 */
typedef bool (*keel_hook_fn)(keel_hook_ctx_t* ctx);

/* ============================================================================
 * Hook Registration
 * ============================================================================ */

typedef enum keel_hook_type {
    KEEL_HOOK_TYPE_NATIVE = 0,  /**< C function pointer */
    KEEL_HOOK_TYPE_LUA    = 1,  /**< Lua function name */
    KEEL_HOOK_TYPE_PYTHON = 2,  /**< Python callable */
    KEEL_HOOK_TYPE_PLUGIN = 3,  /**< Native .so plugin */
} keel_hook_type_t;

/**
 * @brief Hook registration handle (opaque).
 */
typedef struct keel_hook_handle keel_hook_handle_t;

/**
 * @brief Hook registry (opaque).
 *
 * Each worker group owns its own registry — hooks registered in one
 * group never fire in another.  Create with keel_hook_registry_create(),
 * destroy with keel_hook_registry_destroy().
 */
typedef struct keel_hook_registry keel_hook_registry_t;

/**
 * @brief Create a new, empty hook registry.
 *
 * Registries are intended to be created during startup and then treated as
 * mostly-stable read-mostly structures while queries execute. Group-local
 * registries avoid global cross-talk between separate worker groups.
 *
 * @return New registry handle, or `NULL` on allocation failure.
 */
keel_hook_registry_t* keel_hook_registry_create(void);

/**
 * @brief Destroy a hook registry and all registered hooks/plugins.
 *
 * @param reg Registry to destroy, or `NULL`.
 * @return
 */
void keel_hook_registry_destroy(keel_hook_registry_t* reg);

/**
 * @brief Check if any hooks are registered for a given point.
 *
 * Returns a bitmask of hook points that have at least one hook registered.
 * Use this to skip expensive context-fill + fire in the hot path.
 *
 * @param reg Hook registry (NULL returns 0)
 * @return bitmask where bit N is set if KEEL_HOOK_POINT N has hooks
 */
uint32_t keel_hook_registry_active_mask(const keel_hook_registry_t* reg);

/**
 * @brief Register a native C hook.
 *
 * @param reg Hook registry (from keel_hook_registry_create).
 * @param point Which hook point to fire at.
 * @param name Human-readable name used in logs and diagnostics.
 * @param fn Callback function.
 * @param priority Lower values fire first.
 * @param data Opaque user data exposed through `ctx->user_data` during execution.
 * @return Registration handle on success, or `NULL` on failure.
 */
keel_hook_handle_t* keel_hook_register(
    keel_hook_registry_t* reg,
    keel_hook_point_t point,
    const char*       name,
    keel_hook_fn      fn,
    int               priority,
    void*             data
);

/**
 * @brief Register a Lua hook script.
 *
 * The Lua file must define a function with the given name:
 *   function my_hook(ctx) ... return true end
 *
 * @param reg Hook registry.
 * @param point Hook point.
 * @param name Human-readable name.
 * @param lua_file Path to the `.lua` script.
 * @param lua_func Function name within the script.
 * @param priority Priority (lower values run first).
 * @return Registration handle on success, or `NULL` on failure.
 */
keel_hook_handle_t* keel_hook_register_lua(
    keel_hook_registry_t* reg,
    keel_hook_point_t point,
    const char*       name,
    const char*       lua_file,
    const char*       lua_func,
    int               priority
);

/**
 * @brief Register a Python hook.
 *
 * The Python module must define a callable:
 *   def my_hook(ctx): ... return True
 *
 * @param reg Hook registry.
 * @param point Hook point.
 * @param name Human-readable name.
 * @param py_module Python module name, for example `my_hooks`.
 * @param py_func Function name.
 * @param priority Priority (lower values run first).
 * @return Registration handle on success, or `NULL` on failure.
 */
keel_hook_handle_t* keel_hook_register_python(
    keel_hook_registry_t* reg,
    keel_hook_point_t point,
    const char*       name,
    const char*       py_module,
    const char*       py_func,
    int               priority
);

/**
 * @brief Load a native plugin (.so / .dylib) and register its hooks.
 *
 * The shared library must export:
 *   const keel_hook_plugin_info_t* keel_hook_plugin_init(void);
 *
 * @param reg Hook registry to register the plugin's hooks into.
 * @param path Path to the shared library.
 * @return `KEEL_OK` on success, or an error code if loading or validation fails.
 */
keel_error_t keel_hook_load_plugin(keel_hook_registry_t* reg, const char* path);

/**
 * @brief Unregister a previously registered hook.
 *
 * @param reg Registry containing the hook, or `NULL` to use the legacy global registry.
 * @param handle Registration handle to remove.
 * @return
 */
void keel_hook_unregister(keel_hook_registry_t* reg, keel_hook_handle_t* handle);

/**
 * @brief Fire all hooks for a given point.
 *
 * Hooks execute in priority order. If any hook returns false, processing
 * stops and this function returns false.
 *
 * @param reg Hook registry.
 * @param point Hook point.
 * @param[in,out] ctx Mutable hook context. Fields such as `route_hint`,
 *                    `needs_primary`, `pin_update`, `pin_clear`,
 *                    `splice_eligible`, and `error_msg` act as output channels.
 * @return `true` if all hooks passed, `false` if any hook aborted processing.
 */
bool keel_hook_fire(keel_hook_registry_t* reg, keel_hook_point_t point,
                    keel_hook_ctx_t* ctx);

/* ============================================================================
 * Hook System Lifecycle (deprecated — use registry API)
 * ============================================================================ */

/**
 * @brief Initialize the global hook system (legacy, for tests).
 *
 * Prefer keel_hook_registry_create() for production use.
 */
keel_error_t keel_hook_init(void);

/**
 * @brief Shutdown the global hook system (legacy, for tests).
 *
 * @return
 */
void keel_hook_shutdown(void);

/**
 * @brief Get statistics for a hook point.
 */
typedef struct keel_hook_stats {
    uint64_t fire_count;        /**< Times this point was fired */
    uint64_t abort_count;       /**< Times a hook returned false */
    uint64_t total_ns;          /**< Total time spent in hooks (ns) */
    uint32_t hook_count;        /**< Number of registered hooks */
} keel_hook_stats_t;

/**
 * @brief Return cumulative statistics for one hook point.
 *
 * The statistics object is returned by value because it is small, immutable
 * from the caller's perspective, and avoids an output-parameter dance for a
 * simple read-only query API.
 *
 * @param reg Hook registry.
 * @param point Hook point.
 * @return Snapshot of cumulative hook statistics for `point`.
 */
keel_hook_stats_t keel_hook_get_stats(keel_hook_registry_t* reg,
                                      keel_hook_point_t point);

/* ============================================================================
 * Lua / Python Runtime Init — call before registering hooks of that type
 * ============================================================================ */

/**
 * @brief Check if Lua hook support is compiled in.
 *
 * @return `true` when Lua bridge support is available in this build.
 */
bool keel_lua_available(void);

/**
 * @brief Initialize the Lua bridge (call once at startup).
 *
 * @return `KEEL_OK` on success.
 */
keel_error_t keel_lua_init(void);

/**
 * @brief Shutdown the Lua bridge and release cached interpreter state.
 *
 * @return
 */
void keel_lua_shutdown(void);

/**
 * @brief Check if Python hook support is compiled in.
 *
 * @return `true` when Python bridge support is available in this build.
 */
bool keel_python_available(void);

/**
 * @brief Add a directory to Python's sys.path for script-based hooks.
 *
 * Call this before registering a Python hook whose module lives in a
 * non-standard directory (e.g. when specifying script= with a full path).
 *
 * @param dir Absolute or relative directory to prepend to `sys.path`.
 * @return
 */
void keel_python_add_script_dir(const char* dir);

/**
 * @brief Initialize the Python interpreter bridge (call once at startup).
 *
 * @return `KEEL_OK` on success, or an error code if interpreter startup fails.
 */
keel_error_t keel_python_init(void);

/**
 * @brief Shutdown the Python interpreter bridge.
 *
 * @return
 */
void keel_python_shutdown(void);

/* ============================================================================
 * Native Plugin Interface
 * ============================================================================
 *
 * Shared libraries (.so/.dylib) export this structure from
 *   const keel_hook_plugin_info_t* keel_hook_plugin_init(void);
 */

#define KEEL_HOOK_PLUGIN_API_VERSION 1

typedef struct keel_hook_plugin_def {
    keel_hook_point_t  point;       /**< Hook point */
    const char*        name;        /**< Hook name */
    keel_hook_fn       fn;          /**< Callback */
    int                priority;    /**< Priority */
} keel_hook_plugin_def_t;

typedef struct keel_hook_plugin_info {
    uint32_t                      api_version;  /**< Must be KEEL_HOOK_PLUGIN_API_VERSION */
    const char*                   name;         /**< Plugin name */
    const char*                   version;      /**< Plugin version string */
    const char*                   description;  /**< Plugin description */
    const keel_hook_plugin_def_t* hooks;        /**< Array of hook registrations */
    size_t                        hook_count;   /**< Number of hooks */
    void (*shutdown)(void);                     /**< Optional cleanup callback */
} keel_hook_plugin_info_t;

#ifdef __cplusplus
}
#endif

#endif /* KEEL_HOOK_H */
