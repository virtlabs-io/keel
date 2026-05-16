/**
 * @file sharding.h
 * @brief Horizontal sharding API: shard-key extraction, deterministic mapping,
 *        unified routing plan, and bound-parameter resolution.
 *
 * This API implements in-process shard routing: detect whether a statement
 * constrains one configured shard key to a single literal or bound parameter,
 * map concrete values to a shard index, and expose a unified plan type
 * (SINGLE / SCATTER / UNSUPPORTED) for use by proxy dispatch loops.
 *
 * Phases implemented:
 *   Phase 1 — SELECT shard-key extraction and int64/string/bool mapping
 *   Phase 2 — INSERT / UPDATE / DELETE shard-key extraction
 *   Phase 3 — Bound-parameter ($N) resolution via keel_shard_bound_params_t
 *   Phase 4 — Unified keel_shard_plan_t / keel_shard_plan() API
 *   Phase 5 — Router-embedded shard rule registry (see router.h)
 *   Phase 6 — Scatter fan-out via keel_router_scatter_servers() (see router.h)
 *
 * See docs/SHARDING.md for the full architecture and usage guide.
 */

#ifndef KEEL_CORE_SHARDING_H
#define KEEL_CORE_SHARDING_H

#include "keel_error.h"
#include "keel_types.h"
#include "keel/sql/sql_ast.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum keel_shard_key_kind {
    KEEL_SHARD_KEY_NONE = 0,
    KEEL_SHARD_KEY_INT64,
    KEEL_SHARD_KEY_STRING,
    KEEL_SHARD_KEY_BOOL,
    KEEL_SHARD_KEY_PARAM,
} keel_shard_key_kind_t;

/* ============================================================================
 * Feature 6: Range-based shard strategy
 * ============================================================================ */

/**
 * @brief Shard mapping strategy for a rule.
 *
 * HASH  — default behaviour: xxhash64(value) % N.
 * RANGE — ordered threshold table: shard i receives values where
 *         value <= thresholds[i] (last shard catches all remaining values).
 *         Only INT64 key kinds are range-mapped; others fall back to HASH.
 */
typedef enum keel_shard_strategy {
    KEEL_SHARD_STRATEGY_HASH  = 0, /**< xxhash64 % N (default) */
    KEEL_SHARD_STRATEGY_RANGE = 1, /**< Sorted inclusive threshold table */
} keel_shard_strategy_t;

/** Maximum threshold entries for a single RANGE shard rule. */
#define KEEL_SHARD_RANGE_MAX_THRESHOLDS 64

/* ============================================================================
 * Feature 8: Shard migration state
 * ============================================================================ */

/**
 * @brief Live state of a shard rule.
 *
 * NORMAL    — all traffic routed by the configured strategy.
 * MIGRATING — dual-write (src + dst) for writes; read-from-new for reads.
 */
typedef enum keel_shard_state {
    KEEL_SHARD_STATE_NORMAL    = 0, /**< Normal operation */
    KEEL_SHARD_STATE_MIGRATING = 1, /**< Live migration in progress */
} keel_shard_state_t;

typedef struct keel_shard_rule {
    const char*           table;
    const char*           column;
    size_t                shard_count;
    /* Feature 6: strategy and range thresholds */
    keel_shard_strategy_t strategy;          /**< HASH (default) or RANGE */
    int64_t               thresholds[KEEL_SHARD_RANGE_MAX_THRESHOLDS]; /**< Inclusive upper bound per shard */
    size_t                threshold_count;   /**< Must equal shard_count when RANGE */
    /* Feature 8: migration */
    keel_shard_state_t    state;             /**< Normal or migrating */
    size_t                migrate_src_shard; /**< Source shard index (MIGRATING only) */
    size_t                migrate_dst_shard; /**< Destination shard index (MIGRATING only) */
} keel_shard_rule_t;

typedef struct keel_shard_key {
    keel_shard_key_kind_t kind;
    keel_str_t            table;
    keel_str_t            column;
    union {
        int64_t           int64_value;
        keel_str_t        string_value;
        bool              bool_value;
        int               param_index;
    } value;
} keel_shard_key_t;

/**
 * @brief Bound parameter values for prepared-statement shard routing.
 *
 * Parameters are 1-indexed (matching PostgreSQL $1, $2 …).  The caller fills
 * `values[0]` through `values[count-1]` where `values[i]` holds the bound
 * value for parameter `$i+1`.  PARAM keys with an index outside [1, count]
 * are treated as unresolvable and return KEEL_ERR_NOT_FOUND.
 *
 * Only INT64 / STRING / BOOL kinds are valid as bound values; PARAM and NONE
 * are rejected.
 */
#define KEEL_SHARD_MAX_PARAMS 64

typedef struct keel_shard_bound_params {
    keel_shard_key_t values[KEEL_SHARD_MAX_PARAMS];
    size_t           count;
} keel_shard_bound_params_t;

keel_error_t keel_shard_extract_key_ast(const keel_sql_node_t* ast,
                                        const keel_shard_rule_t* rule,
                                        keel_shard_key_t* key_out);

keel_error_t keel_shard_extract_key_sql(keel_str_t sql,
                                        const keel_shard_rule_t* rule,
                                        keel_shard_key_t* key_out,
                                        keel_arena_t* arena);

keel_error_t keel_shard_map_key(const keel_shard_key_t* key,
                                size_t shard_count,
                                size_t* shard_index_out);

/**
 * @brief Map a shard key using the strategy configured in @p rule.
 *
 * For KEEL_SHARD_STRATEGY_RANGE with INT64 keys, performs a binary threshold
 * lookup: the key maps to the first shard i where key <= rule->thresholds[i],
 * falling back to the last shard if the key exceeds all thresholds.
 * Non-INT64 keys always use the hash strategy regardless of rule->strategy.
 *
 * @return KEEL_OK on success.
 *         KEEL_ERR_INVALID_ARG on NULL/zero inputs.
 *         KEEL_ERR_NOT_SUPPORTED for PARAM / NONE key kinds.
 */
keel_error_t keel_shard_map_key_rule(const keel_shard_key_t*  key,
                                     const keel_shard_rule_t* rule,
                                     size_t*                  shard_index_out);

/**
 * @brief Map a shard key to a shard index, resolving PARAM keys via bindings.
 *
 * If @p key is a PARAM kind, the corresponding entry in @p params is used as
 * the concrete value.  For all other kinds the call behaves identically to
 * keel_shard_map_key().
 *
 * @param key              Extracted shard key (may be PARAM).
 * @param params           Bound parameter values (may be NULL if key is not PARAM).
 * @param shard_count      Number of shards (must be > 0).
 * @param shard_index_out  Destination for the resolved shard index.
 * @return KEEL_OK on success.
 *         KEEL_ERR_NOT_FOUND if the param index is out of range.
 *         KEEL_ERR_NOT_SUPPORTED if the resolved value kind is not mappable.
 *         KEEL_ERR_INVALID_ARG on NULL/bad inputs.
 */
keel_error_t keel_shard_map_key_bound(const keel_shard_key_t*           key,
                                      const keel_shard_bound_params_t*  params,
                                      size_t                            shard_count,
                                      size_t*                           shard_index_out);

/**
 * @brief Map a shard key with bound-parameter resolution using the rule's strategy.
 *
 * Resolves PARAM keys via @p params then calls @ref keel_shard_map_key_rule,
 * honouring the rule's configured HASH or RANGE strategy.
 */
keel_error_t keel_shard_map_key_bound_rule(const keel_shard_key_t*           key,
                                           const keel_shard_bound_params_t*  params,
                                           const keel_shard_rule_t*          rule,
                                           size_t*                           shard_index_out);

/* ============================================================================
 * Routing plan
 * ============================================================================ */

/**
 * @brief High-level routing outcome for a sharded statement.
 *
 * KEEL_SHARD_PLAN_SINGLE   – Statement constrains exactly one shard.
 *                            `shard_index` holds the target shard (0-based).
 * KEEL_SHARD_PLAN_SCATTER  – Statement is valid DML/query but has no single-
 *                            shard predicate; must be sent to all shards.
 * KEEL_SHARD_PLAN_UNSUPPORTED – Statement kind cannot be shard-routed at all
 *                               (e.g. DDL, transaction control, COPY, …).
 */
typedef enum keel_shard_plan_kind {
    KEEL_SHARD_PLAN_SINGLE      = 0,
    KEEL_SHARD_PLAN_SCATTER     = 1,
    KEEL_SHARD_PLAN_UNSUPPORTED = 2,
} keel_shard_plan_kind_t;

typedef struct keel_shard_plan {
    keel_shard_plan_kind_t kind;
    size_t                 shard_index; /**< Valid only when kind == SINGLE */
} keel_shard_plan_t;

/**
 * @brief Compute a routing plan for @p sql against @p rule.
 *
 * Single call that parses the SQL, extracts the shard key (resolving $N via
 * @p params when provided), maps it to a shard index, and returns a plan.
 *
 * - If a single-shard predicate is found:  plan.kind = SINGLE, plan.shard_index set.
 * - If the statement is routable DML/SELECT but has no single-shard predicate:
 *   plan.kind = SCATTER.
 * - If the statement cannot be shard-routed (DDL, transaction, parse error, …):
 *   plan.kind = UNSUPPORTED.
 *
 * This function never returns an error code; all outcomes are encoded in the
 * plan.  It is safe to call with a NULL @p params (treated as no bindings).
 *
 * @param sql     SQL text to analyse.
 * @param rule    Shard rule (table + column + count).
 * @param params  Bound parameter values, or NULL.
 * @param arena   Scratch arena for SQL parsing (reset between calls is fine).
 * @param plan    Output plan (always written, even on parse failure).
 */
void keel_shard_plan(keel_str_t                       sql,
                     const keel_shard_rule_t*         rule,
                     const keel_shard_bound_params_t* params,
                     keel_arena_t*                    arena,
                     keel_shard_plan_t*               plan);

/* ============================================================================
 * Process-wide hash mode (R2)
 * ============================================================================
 *
 * Controls the INT64 mapping in @ref keel_shard_map_key:
 *
 *   KEEL_SHARD_HASH_LEGACY  hash = (uint64_t)key->value.int64_value
 *                           (default; preserves shard placement for existing
 *                            customers — negative keys map differently than
 *                            their positive counterparts)
 *
 *   KEEL_SHARD_HASH_ABS     hash = (uint64_t)llabs(key->value.int64_value)
 *                           (matches the Python contract used by tests/e2e:
 *                            row at id = -7 lands on the same shard as id = 7)
 *
 * Configure via keel.ini:    [keel] shard_key_hash = legacy | abs
 * Or env override:           KEEL_SHARD_KEY_HASH = legacy | abs
 *
 * Migration plan: ABS will become the default in a future major release once
 * downstream operators have had a release cycle to opt in and re-shard if
 * needed.  Until then, LEGACY remains the default to avoid silently moving
 * existing data.
 */
typedef enum keel_shard_hash_mode {
    KEEL_SHARD_HASH_LEGACY = 0, /**< Default; cast int64 to uint64 as-is. */
    KEEL_SHARD_HASH_ABS    = 1, /**< Use llabs() before hashing INT64 keys. */
} keel_shard_hash_mode_t;

/** Returns the currently active process-wide hash mode. */
keel_shard_hash_mode_t keel_shard_get_hash_mode(void);

/** Sets the process-wide hash mode. Safe to call once at startup. */
void keel_shard_set_hash_mode(keel_shard_hash_mode_t mode);

/**
 * Parse a textual mode name ("legacy" | "abs") into the enum.
 * Returns @c KEEL_OK on success and writes @p out_mode; @c KEEL_ERR_INVALID_ARG
 * for NULL inputs or unknown names. Comparison is case-insensitive.
 */
keel_error_t keel_shard_parse_hash_mode(const char*              name,
                                        keel_shard_hash_mode_t*  out_mode);

#ifdef __cplusplus
}
#endif

#endif
