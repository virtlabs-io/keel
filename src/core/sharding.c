/**
 * @file sharding.c
 * @brief Shard-key extraction and mapping implementation.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Implements shard-key extraction from SQL AST nodes and maps extracted keys
 * to shard indices using hash or range strategies.  The public API mirrors
 * the declarations in include/keel/core/sharding.h.
 */

#include "keel/core/sharding.h"

#include "keel/sql/sql.h"
#include "keel/util/util.h"
#include "keel/util/xxhash.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/**
 * @brief Internal context used while walking a WHERE clause to extract a shard key.
 *
 * Accumulates the first matching shard-key candidate and flags a conflict if
 * contradictory values are encountered in the same predicate tree.
 */
typedef struct shard_extract_ctx {
    keel_str_t table_name;   /**< Unqualified target table name. */
    keel_str_t table_alias;  /**< Optional alias for the target table. */
    keel_str_t shard_column; /**< Shard column name taken from the rule. */
    bool       found;        /**< Set to true once a candidate key is recorded. */
    bool       conflict;     /**< Set to true when two different candidate values are found. */
    keel_shard_key_t key;    /**< The first candidate key value found. */
} shard_extract_ctx_t;

/**
 * @brief Case-insensitively compare a @c keel_str_t against a NUL-terminated C string.
 *
 * @param value The string view to compare.
 * @param cstr  The NUL-terminated string to compare against; may be NULL.
 * @return @c true if both strings are equal (case-insensitive), @c false otherwise
 *         or if @p cstr is NULL.
 */
static bool shard_str_eq_cstr_nocase(keel_str_t value, const char* cstr) {
    return cstr != NULL && keel_str_eq_nocase(value, keel_str_from_cstr(cstr));
}

/**
 * @brief Determine whether a column reference targets the shard column in the given context.
 *
 * Accepts an unqualified column reference or one qualified with either the table
 * name or its alias.
 *
 * @param column Parsed column expression from the SQL AST.
 * @param ctx    Extraction context carrying the expected table/column names.
 * @return @c true if the column matches the shard column, @c false otherwise.
 */
static bool shard_column_matches(const keel_sql_expr_column_t* column,
                                 const shard_extract_ctx_t* ctx) {
    if (!keel_str_eq_nocase(column->column, ctx->shard_column)) {
        return false;
    }

    if (column->table.len == 0) {
        return true;
    }

    return keel_str_eq_nocase(column->table, ctx->table_name) ||
           (ctx->table_alias.len > 0 && keel_str_eq_nocase(column->table, ctx->table_alias));
}

/**
 * @brief Test value equality between two shard keys of the same kind.
 *
 * Keys of different kinds are never equal.  PARAM keys are compared by their
 * parameter index.
 *
 * @param left  First shard key.
 * @param right Second shard key.
 * @return @c true if both keys represent the same value, @c false otherwise.
 */
static bool shard_key_equals(const keel_shard_key_t* left, const keel_shard_key_t* right) {
    if (left->kind != right->kind) {
        return false;
    }

    switch (left->kind) {
    case KEEL_SHARD_KEY_INT64:
        return left->value.int64_value == right->value.int64_value;
    case KEEL_SHARD_KEY_STRING:
        return keel_str_eq(left->value.string_value, right->value.string_value);
    case KEEL_SHARD_KEY_BOOL:
        return left->value.bool_value == right->value.bool_value;
    case KEEL_SHARD_KEY_PARAM:
        return left->value.param_index == right->value.param_index;
    default:
        return false;
    }
}

/**
 * @brief Extract a scalar shard key from a literal or parameter AST node.
 *
 * Handles integer, string, UUID, boolean literals and positional query
 * parameters (@c $N).  Composite expressions are not supported.
 *
 * @param node    AST node to inspect; may be NULL.
 * @param key_out Output key populated on success; must not be NULL.
 * @return @c true if a scalar key was extracted, @c false if the node kind is
 *         not a supported scalar.
 */
static bool shard_extract_scalar_key(const keel_sql_node_t* node,
                                     keel_shard_key_t* key_out) {
    if (!node || !key_out) {
        return false;
    }

    switch (node->kind) {
    case KEEL_SQL_NODE_EXPR_LITERAL: {
        const keel_sql_expr_literal_t* lit = (const keel_sql_expr_literal_t*)node;
        switch (lit->lit_type) {
        case KEEL_SQL_LIT_INT:
            key_out->kind = KEEL_SHARD_KEY_INT64;
            key_out->value.int64_value = lit->value.int_val;
            return true;
        case KEEL_SQL_LIT_STRING:
        case KEEL_SQL_LIT_UUID:
            key_out->kind = KEEL_SHARD_KEY_STRING;
            key_out->value.string_value = lit->value.str_val;
            return true;
        case KEEL_SQL_LIT_BOOL:
            key_out->kind = KEEL_SHARD_KEY_BOOL;
            key_out->value.bool_value = lit->value.bool_val;
            return true;
        default:
            return false;
        }
    }
    case KEEL_SQL_NODE_EXPR_PARAM: {
        const keel_sql_expr_param_t* param = (const keel_sql_expr_param_t*)node;
        key_out->kind = KEEL_SHARD_KEY_PARAM;
        key_out->value.param_index = param->index;
        return true;
    }
    case KEEL_SQL_NODE_EXPR_UNARY: {
        /* Handle unary negation of an integer literal: -N */
        const keel_sql_expr_unary_t* unary = (const keel_sql_expr_unary_t*)node;
        if (unary->op == KEEL_SQL_UNOP_NEG &&
            unary->operand &&
            unary->operand->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
            const keel_sql_expr_literal_t* lit =
                (const keel_sql_expr_literal_t*)unary->operand;
            if (lit->lit_type == KEEL_SQL_LIT_INT) {
                key_out->kind = KEEL_SHARD_KEY_INT64;
                key_out->value.int64_value = -lit->value.int_val;
                return true;
            }
        }
        return false;
    }
    default:
        return false;
    }
}

/**
 * @brief Record a shard-key candidate into the extraction context.
 *
 * If this is the first candidate, it is stored in @p ctx.  On subsequent calls
 * the candidate is compared with the stored value; a mismatch sets
 * @c ctx->conflict which aborts further extraction.
 *
 * @param ctx       Extraction context to update.
 * @param candidate Candidate key value derived from the current predicate.
 */
static void shard_record_candidate(shard_extract_ctx_t* ctx,
                                   const keel_shard_key_t* candidate) {
    if (!ctx->found) {
        ctx->key = *candidate;
        ctx->key.table = ctx->table_name;
        ctx->key.column = ctx->shard_column;
        ctx->found = true;
        return;
    }

    if (!shard_key_equals(&ctx->key, candidate)) {
        ctx->conflict = true;
    }
}

/**
 * @brief Recursively walk a WHERE expression tree to find shard-key predicates.
 *
 * Descends into AND conjunctions and processes equality predicates of the form
 * @c shard_column = scalar or @c scalar = shard_column.  Stops early if a
 * conflict has already been detected.
 *
 * @param node AST node representing the (sub-)expression; may be NULL.
 * @param ctx  Extraction context accumulating found candidates.
 */
static void shard_extract_from_where(const keel_sql_node_t* node,
                                     shard_extract_ctx_t* ctx) {
    if (!node || !ctx || ctx->conflict) {
        return;
    }

    /* BETWEEN low AND high is treated as a shard-key predicate only when the
     * bounds are equal literals (degenerate single-value range, e.g.
     *   id BETWEEN 1 AND 1).  Anything wider can span multiple shards and is
     * left to the scatter fallback.  NOT BETWEEN never constrains to one
     * shard. */
    if (node->kind == KEEL_SQL_NODE_EXPR_BETWEEN) {
        const keel_sql_expr_between_t* btw = (const keel_sql_expr_between_t*)node;
        if (btw->negated || !btw->expr || !btw->low || !btw->high) {
            return;
        }
        if (btw->expr->kind != KEEL_SQL_NODE_EXPR_COLUMN) {
            return;
        }
        const keel_sql_expr_column_t* column =
            (const keel_sql_expr_column_t*)btw->expr;
        if (!shard_column_matches(column, ctx)) {
            return;
        }
        keel_shard_key_t low_key = {0}, high_key = {0};
        if (!shard_extract_scalar_key(btw->low, &low_key)) {
            return;
        }
        if (!shard_extract_scalar_key(btw->high, &high_key)) {
            return;
        }
        if (!shard_key_equals(&low_key, &high_key)) {
            return;
        }
        shard_record_candidate(ctx, &low_key);
        return;
    }

    if (node->kind != KEEL_SQL_NODE_EXPR_BINARY) {
        return;
    }

    const keel_sql_expr_binary_t* expr = (const keel_sql_expr_binary_t*)node;

    if (expr->op == KEEL_SQL_BINOP_AND) {
        shard_extract_from_where(expr->left, ctx);
        shard_extract_from_where(expr->right, ctx);
        return;
    }

    if (expr->op != KEEL_SQL_BINOP_EQ) {
        return;
    }

    const keel_sql_expr_column_t* column = NULL;
    const keel_sql_node_t* scalar = NULL;

    if (expr->left && expr->left->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
        column = (const keel_sql_expr_column_t*)expr->left;
        scalar = expr->right;
    } else if (expr->right && expr->right->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
        column = (const keel_sql_expr_column_t*)expr->right;
        scalar = expr->left;
    }

    if (!column || !scalar || !shard_column_matches(column, ctx)) {
        return;
    }

    keel_shard_key_t candidate = {0};
    if (!shard_extract_scalar_key(scalar, &candidate)) {
        return;
    }

    shard_record_candidate(ctx, &candidate);
}

/**
 * @brief Recursively search a FROM-clause tree for a TABLE_REF matching @p table_name.
 *
 * Descends @c KEEL_SQL_NODE_TABLE_JOIN nodes (left and right branches) until a
 * matching leaf @c KEEL_SQL_NODE_TABLE_REF is found.  Returns @c NULL when the
 * table is not present anywhere in the tree.
 *
 * @param from       Root of the FROM-clause node tree; may be @c NULL.
 * @param table_name Unqualified target table name (NUL-terminated, case-insensitive).
 * @return Pointer to the first matching @c keel_sql_table_ref_t leaf, or @c NULL.
 */
static const keel_sql_table_ref_t*
shard_find_table_in_join(const keel_sql_node_t* from, const char* table_name)
{
    if (!from || !table_name) return NULL;

    switch (from->kind) {
    case KEEL_SQL_NODE_TABLE_REF: {
        const keel_sql_table_ref_t* ref = (const keel_sql_table_ref_t*)from;
        return shard_str_eq_cstr_nocase(ref->table, table_name) ? ref : NULL;
    }
    case KEEL_SQL_NODE_TABLE_JOIN: {
        const keel_sql_join_t* join = (const keel_sql_join_t*)from;
        const keel_sql_table_ref_t* r = shard_find_table_in_join(join->left, table_name);
        return r ? r : shard_find_table_in_join(join->right, table_name);
    }
    default:
        return NULL;
    }
}

/**
 * @brief Extract a shard key from a SELECT statement.
 *
 * Handles single-table SELECTs and JOIN queries.  CTEs, set operations, missing
 * WHERE, and subquery FROMs all produce @c KEEL_ERR_NOT_FOUND so the planner
 * falls back to a scatter plan (the SQL is pushed unchanged to every shard).
 *
 * @param select  Parsed SELECT statement.
 * @param rule    Sharding rule describing the target table and column.
 * @param key_out Output shard key on success.
 * @return @c KEEL_OK on success; @c KEEL_ERR_NOT_SUPPORTED for conflicting
 *         predicates; @c KEEL_ERR_NOT_FOUND for everything else that should
 *         scatter.
 */
static keel_error_t shard_extract_select(const keel_sql_stmt_select_t* select,
                                         const keel_shard_rule_t* rule,
                                         keel_shard_key_t* key_out) {
    /* Step 1: confirm the rule's table appears in the FROM clause.
     * If it doesn't, this rule does not apply (NOT_SUPPORTED) so the planner
     * can try the next rule.  Scatter (NOT_FOUND) is only returned once the
     * shard table is confirmed to be present.
     *
     * Special case: a WITH...SELECT may reference a CTE name in its outer
     * FROM rather than the underlying table name — scatter immediately. */
    if (!select->from) return KEEL_ERR_NOT_SUPPORTED; /* SELECT 1, SELECT now() */

    /* CTEs: the proxy cannot peer inside a WITH clause to extract a routing
     * key.  Recursive CTEs are pure computation (no shard table scan needed
     * at the proxy level) → route to a single shard via NOT_SUPPORTED so the
     * default pool routing handles it.  Non-recursive CTEs scatter as before
     * so every shard can contribute its local rows.
     *
     * Phase 8b — Writable CTE single-shard fold: if any CTE body is a writable
     * statement (INSERT/UPDATE/DELETE) targeting the shard table with a
     * single-shard key, the entire statement can be routed to that shard. */
    if (select->with_clause) {
        if (select->with_recursive) return KEEL_ERR_NOT_SUPPORTED;

        const keel_sql_node_t* cn = select->with_clause->head;
        while (cn) {
            if (cn->kind == KEEL_SQL_NODE_CLAUSE_CTE) {
                const keel_sql_cte_t* cte = (const keel_sql_cte_t*)cn;
                if (cte->query &&
                    (cte->query->kind == KEEL_SQL_NODE_STMT_INSERT ||
                     cte->query->kind == KEEL_SQL_NODE_STMT_UPDATE ||
                     cte->query->kind == KEEL_SQL_NODE_STMT_DELETE)) {
                    keel_shard_key_t cte_key;
                    keel_error_t e = keel_shard_extract_key_ast(cte->query, rule, &cte_key);
                    if (e == KEEL_OK) {
                        *key_out = cte_key;
                        return KEEL_OK;
                    }
                }
            }
            cn = cn->next;
        }
        return KEEL_ERR_NOT_FOUND;
    }

    shard_extract_ctx_t ctx = {
        .shard_column = keel_str_from_cstr(rule->column),
    };

    if (select->from->kind == KEEL_SQL_NODE_TABLE_REF) {
        const keel_sql_table_ref_t* tref =
            (const keel_sql_table_ref_t*)select->from;
        if (!shard_str_eq_cstr_nocase(tref->table, rule->table))
            return KEEL_ERR_NOT_SUPPORTED; /* Different table — rule doesn't apply */
        ctx.table_name  = tref->table;
        ctx.table_alias = tref->alias;

    } else if (select->from->kind == KEEL_SQL_NODE_TABLE_JOIN) {
        /* Walk the JOIN tree to find the leaf that holds the shard table. */
        const keel_sql_table_ref_t* ref =
            shard_find_table_in_join(select->from, rule->table);
        if (!ref) return KEEL_ERR_NOT_SUPPORTED; /* Table absent from JOIN — rule doesn't apply */
        ctx.table_name  = ref->table;
        ctx.table_alias = ref->alias;

    } else if (select->from->kind == KEEL_SQL_NODE_TABLE_SUBQUERY) {
        /* Derived table (inline subquery) — peek inside to see if it
         * queries the shard table.  If so, scatter so every shard
         * contributes its partial aggregates; the merge spec extraction
         * in scatter_extract_merge_spec_impl handles the rest.  If not,
         * fall through to UNSUPPORTED so other rules can be tried. */
        const keel_sql_table_subquery_t* tsq =
            (const keel_sql_table_subquery_t*)select->from;
        if (tsq->subquery &&
            tsq->subquery->kind == KEEL_SQL_NODE_STMT_SELECT) {
            const keel_sql_stmt_select_t* inner_sel =
                (const keel_sql_stmt_select_t*)tsq->subquery;
            bool inner_has_shard_table = false;
            if (inner_sel->from) {
                if (inner_sel->from->kind == KEEL_SQL_NODE_TABLE_REF) {
                    const keel_sql_table_ref_t* inner_tref =
                        (const keel_sql_table_ref_t*)inner_sel->from;
                    inner_has_shard_table =
                        shard_str_eq_cstr_nocase(inner_tref->table, rule->table);
                } else if (inner_sel->from->kind == KEEL_SQL_NODE_TABLE_JOIN) {
                    inner_has_shard_table =
                        shard_find_table_in_join(inner_sel->from, rule->table) != NULL;
                }
            }
            if (inner_has_shard_table)
                return KEEL_ERR_NOT_FOUND; /* scatter */
        }
        return KEEL_ERR_NOT_SUPPORTED;

    } else {
        /* Subquery FROM, table function, VALUES, etc. — rule can't determine routing. */
        return KEEL_ERR_NOT_SUPPORTED;
    }

    /* Step 2: shard table is confirmed in FROM.  Cases that force scatter: */

    /* UNION / INTERSECT / EXCEPT: scatter both sides. */
    if (select->set_right) return KEEL_ERR_NOT_FOUND;

    /* No WHERE: full-table scan must touch every shard. */
    if (!select->where) return KEEL_ERR_NOT_FOUND;

    shard_extract_from_where(select->where, &ctx);
    if (ctx.conflict) return KEEL_ERR_NOT_SUPPORTED;
    if (!ctx.found)   return KEEL_ERR_NOT_FOUND;

    *key_out = ctx.key;
    return KEEL_OK;
}

/**
 * @brief Extract a shard key from an UPDATE statement.
 *
 * Supports single-target-table UPDATE (with optional PostgreSQL-style FROM)
 * provided the WHERE clause contains a direct equality predicate on the shard
 * column.  CTEs are rejected.  Statements lacking a WHERE clause return
 * @c KEEL_ERR_NOT_FOUND so they are treated as scatter queries.
 *
 * @param update  Parsed UPDATE statement.
 * @param rule    Sharding rule describing the target table and column.
 * @param key_out Output shard key on success.
 * @return @c KEEL_OK on success; @c KEEL_ERR_NOT_SUPPORTED for unsupported
 *         shapes; @c KEEL_ERR_NOT_FOUND if no qualifying predicate is found.
 * @note See the Feature 9 comment in the implementation for multi-table FROM
 *       semantics.
 */
static keel_error_t shard_extract_update(const keel_sql_stmt_update_t* update,
                                          const keel_shard_rule_t* rule,
                                          keel_shard_key_t* key_out) {
    /* CTE: cannot peer into WITH clause; scatter to all shards. */
    if (update->with_clause) return KEEL_ERR_NOT_FOUND;
    if (!update->where)      return KEEL_ERR_NOT_FOUND;

    if (!update->table || update->table->kind != KEEL_SQL_NODE_TABLE_REF) {
        return KEEL_ERR_NOT_SUPPORTED;
    }

    const keel_sql_table_ref_t* tref = (const keel_sql_table_ref_t*)update->table;
    if (!shard_str_eq_cstr_nocase(tref->table, rule->table)) {
        return KEEL_ERR_NOT_SUPPORTED;
    }

    /* Feature 9: Multi-table UPDATE FROM support.
     * The PostgreSQL-style FROM clause (update->from) is intentionally ignored
     * for shard-key extraction purposes; only the target table and its WHERE
     * predicates matter.
     *
     * The supported pattern is:
     *   UPDATE t1 SET ... FROM t2 WHERE t1.id = t2.id AND t1.id = $1
     *
     * Cross-table equality predicates (t1.id = t2.id) do not yield a shard
     * candidate because the right-hand side is a column reference, not a
     * scalar or bound parameter.  The direct predicate (t1.id = $1) is
     * extracted normally, giving a single-shard plan.
     *
     * Queries where the shard column is only constrained via the JOIN table
     * (e.g. only t2.id = $1, not t1.id = $1) will return KEEL_ERR_NOT_FOUND
     * and be fanned out as a scatter query.
     */
    keel_str_t alias = tref->alias.len > 0 ? tref->alias : update->alias;
    shard_extract_ctx_t ctx = {
        .table_name   = tref->table,
        .table_alias  = alias,
        .shard_column = keel_str_from_cstr(rule->column),
    };

    shard_extract_from_where(update->where, &ctx);
    if (ctx.conflict) return KEEL_ERR_NOT_SUPPORTED;
    if (!ctx.found)   return KEEL_ERR_NOT_FOUND;
    *key_out = ctx.key;
    return KEEL_OK;
}

/**
 * @brief Extract a shard key from a DELETE statement.
 *
 * Requires a WHERE clause with a direct equality predicate on the shard
 * column.  CTEs are rejected.  Statements without a WHERE clause return
 * @c KEEL_ERR_NOT_FOUND and are treated as scatter queries.
 *
 * @param del     Parsed DELETE statement.
 * @param rule    Sharding rule describing the target table and column.
 * @param key_out Output shard key on success.
 * @return @c KEEL_OK on success; @c KEEL_ERR_NOT_SUPPORTED for unsupported
 *         shapes; @c KEEL_ERR_NOT_FOUND if no qualifying predicate is found.
 */
static keel_error_t shard_extract_delete(const keel_sql_stmt_delete_t* del,
                                          const keel_shard_rule_t* rule,
                                          keel_shard_key_t* key_out) {
    /* CTE: cannot peer into WITH clause; scatter to all shards. */
    if (del->with_clause) return KEEL_ERR_NOT_FOUND;
    if (!del->where)      return KEEL_ERR_NOT_FOUND;

    if (!del->table || del->table->kind != KEEL_SQL_NODE_TABLE_REF) {
        return KEEL_ERR_NOT_SUPPORTED;
    }

    const keel_sql_table_ref_t* tref = (const keel_sql_table_ref_t*)del->table;
    if (!shard_str_eq_cstr_nocase(tref->table, rule->table)) {
        return KEEL_ERR_NOT_SUPPORTED;
    }

    keel_str_t alias = tref->alias.len > 0 ? tref->alias : del->alias;
    shard_extract_ctx_t ctx = {
        .table_name   = tref->table,
        .table_alias  = alias,
        .shard_column = keel_str_from_cstr(rule->column),
    };

    shard_extract_from_where(del->where, &ctx);
    if (ctx.conflict) return KEEL_ERR_NOT_SUPPORTED;
    if (!ctx.found)   return KEEL_ERR_NOT_FOUND;
    *key_out = ctx.key;
    return KEEL_OK;
}

/**
 * @brief Extract a shard key from an INSERT statement.
 *
 * Only single-row @c VALUES inserts (not INSERT … SELECT) are supported.  The
 * function locates the shard column in the column list and reads the
 * corresponding value from the first row of the VALUES clause.
 *
 * @param ins     Parsed INSERT statement.
 * @param rule    Sharding rule describing the target table and column.
 * @param key_out Output shard key on success.
 * @return @c KEEL_OK on success; @c KEEL_ERR_NOT_SUPPORTED for unsupported
 *         forms; @c KEEL_ERR_NOT_FOUND if the shard column is absent from the
 *         column list.
 */
static keel_error_t shard_extract_insert(const keel_sql_stmt_insert_t* ins,
                                          const keel_shard_rule_t* rule,
                                          keel_shard_key_t* key_out) {
    /* CTE: cannot peer into WITH clause; scatter to all shards. */
    if (ins->with_clause)    return KEEL_ERR_NOT_FOUND;
    if (!ins->columns || !ins->source) return KEEL_ERR_NOT_FOUND;

    if (!ins->table || ins->table->kind != KEEL_SQL_NODE_TABLE_REF) {
        return KEEL_ERR_NOT_SUPPORTED;
    }

    const keel_sql_table_ref_t* tref = (const keel_sql_table_ref_t*)ins->table;
    if (!shard_str_eq_cstr_nocase(tref->table, rule->table)) {
        return KEEL_ERR_NOT_SUPPORTED;
    }

    /* Only VALUES source is supported (not INSERT … SELECT) */
    if (ins->source->kind != KEEL_SQL_NODE_LIST) {
        return KEEL_ERR_NOT_SUPPORTED;
    }

    /* Find the column index matching the shard column */
    keel_str_t shard_col = keel_str_from_cstr(rule->column);
    size_t col_idx = SIZE_MAX;
    size_t i = 0;
    for (keel_sql_node_t* n = ins->columns->head; n != NULL; n = n->next, i++) {
        if (n->kind != KEEL_SQL_NODE_EXPR_COLUMN) continue;
        const keel_sql_expr_column_t* c = (const keel_sql_expr_column_t*)n;
        if (keel_str_eq_nocase(c->column, shard_col)) {
            col_idx = i;
            break;
        }
    }
    if (col_idx == SIZE_MAX) return KEEL_ERR_NOT_FOUND;

    /* Navigate VALUES structure: outer list -> first row */
    keel_sql_list_t* outer = (keel_sql_list_t*)ins->source;
    keel_sql_list_t* vals  = NULL;
    if (outer->count > 0 && outer->head != NULL &&
        outer->head->kind == KEEL_SQL_NODE_LIST) {
        vals = (keel_sql_list_t*)outer->head; /* first row */
    } else {
        vals = outer; /* flat list = single-row VALUES */
    }
    if (!vals) return KEEL_ERR_NOT_SUPPORTED;

    /* Walk to the shard column's value position */
    keel_sql_node_t* val_node = vals->head;
    for (size_t j = 0; j < col_idx && val_node != NULL; j++) {
        val_node = val_node->next;
    }
    if (!val_node) return KEEL_ERR_NOT_FOUND;

    keel_shard_key_t candidate = {0};
    if (!shard_extract_scalar_key(val_node, &candidate)) {
        return KEEL_ERR_NOT_SUPPORTED;
    }
    *key_out = candidate;
    return KEEL_OK;
}

/**
 * @brief Extract a shard key from a pre-parsed SQL AST node.
 *
 * Dispatches to the appropriate statement-type extractor (SELECT, INSERT,
 * UPDATE, DELETE).  Other statement kinds return @c KEEL_ERR_NOT_SUPPORTED.
 *
 * @param ast     Root AST node produced by @c keel_sql_parse(); must not be NULL.
 * @param rule    Sharding rule; must not be NULL and must have non-NULL
 *                @c table and @c column fields.
 * @param key_out Output shard key zeroed and populated on success; must not be NULL.
 * @return @c KEEL_OK on success; @c KEEL_ERR_INVALID_ARG for NULL inputs;
 *         @c KEEL_ERR_NOT_SUPPORTED for unsupported statement shapes;
 *         @c KEEL_ERR_NOT_FOUND if no shard predicate is present.
 */
keel_error_t keel_shard_extract_key_ast(const keel_sql_node_t* ast,
                                        const keel_shard_rule_t* rule,
                                        keel_shard_key_t* key_out) {
    if (!ast || !rule || !rule->table || !rule->column || !key_out) {
        return KEEL_ERR_INVALID_ARG;
    }

    memset(key_out, 0, sizeof(*key_out));

    switch (ast->kind) {
    case KEEL_SQL_NODE_STMT_SELECT:
        return shard_extract_select((const keel_sql_stmt_select_t*)ast, rule, key_out);
    case KEEL_SQL_NODE_STMT_INSERT:
        return shard_extract_insert((const keel_sql_stmt_insert_t*)ast, rule, key_out);
    case KEEL_SQL_NODE_STMT_UPDATE:
        return shard_extract_update((const keel_sql_stmt_update_t*)ast, rule, key_out);
    case KEEL_SQL_NODE_STMT_DELETE:
        return shard_extract_delete((const keel_sql_stmt_delete_t*)ast, rule, key_out);
    case KEEL_SQL_NODE_STMT_WITH:
        /* Top-level WITH (CTE wrapper): the proxy cannot peer into the inner
         * statement to extract a shard key; always scatter to all shards. */
        return KEEL_ERR_NOT_FOUND;
    default:
        return KEEL_ERR_NOT_SUPPORTED;
    }
}

/**
 * @brief Parse a SQL string and extract a shard key.
 *
 * Convenience wrapper around @c keel_sql_parse() + @c keel_shard_extract_key_ast().
 * All allocations made during parsing are placed in @p arena.
 *
 * @param sql     SQL text to parse; must be non-empty.
 * @param rule    Sharding rule; must not be NULL.
 * @param key_out Output shard key on success; must not be NULL.
 * @param arena   Arena allocator used for the parse tree; must not be NULL.
 * @return @c KEEL_OK on success; @c KEEL_ERR_INVALID_ARG for NULL/empty inputs;
 *         @c KEEL_ERR_SQL_PARSE if the SQL cannot be parsed;
 *         any error forwarded from @c keel_shard_extract_key_ast().
 */
keel_error_t keel_shard_extract_key_sql(keel_str_t sql,
                                        const keel_shard_rule_t* rule,
                                        keel_shard_key_t* key_out,
                                        keel_arena_t* arena) {
    if (!rule || !key_out || !arena || !sql.data || sql.len == 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, sql, arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    if (!ast) {
        return KEEL_ERR_SQL_PARSE;
    }

    return keel_shard_extract_key_ast(ast, rule, key_out);
}

/**
 * @brief Map a concrete shard key to a shard index using a simple hash strategy.
 *
 * Computes a 64-bit hash for the key value and reduces it modulo
 * @p shard_count.  PARAM and NONE key kinds are not supported; callers must
 * resolve parameter bindings before calling this function.
 *
 * @param key             Shard key with a resolved concrete value.
 * @param shard_count     Total number of shards; must be > 0.
 * @param shard_index_out Output index in @c [0, shard_count); must not be NULL.
 * @return @c KEEL_OK on success; @c KEEL_ERR_INVALID_ARG for NULL inputs or
 *         zero @p shard_count; @c KEEL_ERR_NOT_SUPPORTED for PARAM/NONE keys.
 */
keel_error_t keel_shard_map_key(const keel_shard_key_t* key,
                                size_t shard_count,
                                size_t* shard_index_out) {
    if (!key || !shard_index_out || shard_count == 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    uint64_t hash = 0;

    switch (key->kind) {
    case KEEL_SHARD_KEY_INT64: {
        int64_t v = key->value.int64_value;
        if (keel_shard_get_hash_mode() == KEEL_SHARD_HASH_ABS) {
            /* INT64_MIN has no positive counterpart in two's complement;
             * llabs() is undefined there.  Map it to INT64_MAX which is its
             * unsigned-magnitude neighbour and avoids the UB while keeping the
             * "negative and positive land on the same shard" property for
             * every other value. */
            if (v == INT64_MIN) {
                hash = (uint64_t)INT64_MAX;
            } else {
                hash = (uint64_t)llabs((long long)v);
            }
        } else {
            hash = (uint64_t)v;
        }
        break;
    }
    case KEEL_SHARD_KEY_BOOL:
        hash = key->value.bool_value ? 1u : 0u;
        break;
    case KEEL_SHARD_KEY_STRING:
        hash = keel_xxh64(key->value.string_value.data, key->value.string_value.len, 0);
        break;
    case KEEL_SHARD_KEY_PARAM:
    case KEEL_SHARD_KEY_NONE:
    default:
        return KEEL_ERR_NOT_SUPPORTED;
    }

    *shard_index_out = (size_t)(hash % shard_count);
    return KEEL_OK;
}

/* ============================================================================
 * Feature 6: Rule-aware mapping (hash or range)
 * ============================================================================ */

/**
 * @brief Map a shard key to a shard index respecting the rule's strategy.
 *
 * Uses RANGE partitioning when the rule specifies @c KEEL_SHARD_STRATEGY_RANGE
 * and the key is an INT64 with a valid threshold table; otherwise falls back to
 * hash mapping via @c keel_shard_map_key().
 *
 * @param key             Concrete shard key to map.
 * @param rule            Sharding rule supplying strategy, shard count, and
 *                        optional range thresholds.
 * @param shard_index_out Output shard index; must not be NULL.
 * @return @c KEEL_OK on success; @c KEEL_ERR_INVALID_ARG for NULL inputs or
 *         zero @c rule->shard_count.
 */
keel_error_t keel_shard_map_key_rule(const keel_shard_key_t*  key,
                                     const keel_shard_rule_t* rule,
                                     size_t*                  shard_index_out) {
    if (!key || !rule || !shard_index_out || rule->shard_count == 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    /* RANGE strategy: supported for INT64 keys with a valid threshold table */
    if (rule->strategy == KEEL_SHARD_STRATEGY_RANGE
        && key->kind == KEEL_SHARD_KEY_INT64
        && rule->threshold_count == rule->shard_count
        && rule->threshold_count > 0) {
        int64_t v = key->value.int64_value;
        /* Walk thresholds; last shard catches all values exceeding every threshold */
        for (size_t i = 0; i < rule->threshold_count - 1; i++) {
            if (v <= rule->thresholds[i]) {
                *shard_index_out = i;
                return KEEL_OK;
            }
        }
        *shard_index_out = rule->shard_count - 1;
        return KEEL_OK;
    }

    /* Default: hash strategy */
    return keel_shard_map_key(key, rule->shard_count, shard_index_out);
}

/**
 * @brief Map a shard key to a shard index, resolving parameter bindings via a rule.
 *
 * If the key is a @c KEEL_SHARD_KEY_PARAM, the corresponding value is looked
 * up in @p params (1-based index) and then mapped using
 * @c keel_shard_map_key_rule().  Non-param keys are forwarded directly.
 *
 * @param key             Shard key, possibly a parameter reference.
 * @param params          Bound parameter values; may be NULL only when @p key is
 *                        not a PARAM key.
 * @param rule            Sharding rule used for strategy-aware mapping.
 * @param shard_index_out Output shard index; must not be NULL.
 * @return @c KEEL_OK on success; @c KEEL_ERR_INVALID_ARG for invalid inputs;
 *         @c KEEL_ERR_NOT_FOUND if the parameter index is out of range or
 *         @p params is NULL for a PARAM key.
 */
keel_error_t keel_shard_map_key_bound_rule(const keel_shard_key_t*           key,
                                           const keel_shard_bound_params_t*  params,
                                           const keel_shard_rule_t*          rule,
                                           size_t*                           shard_index_out) {
    if (!key || !rule || !shard_index_out || rule->shard_count == 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    if (key->kind != KEEL_SHARD_KEY_PARAM) {
        return keel_shard_map_key_rule(key, rule, shard_index_out);
    }

    /* Resolve the parameter binding */
    if (!params) {
        return KEEL_ERR_NOT_FOUND;
    }

    int idx = key->value.param_index; /* 1-based */
    if (idx < 1 || (size_t)idx > params->count) {
        return KEEL_ERR_NOT_FOUND;
    }

    const keel_shard_key_t* bound = &params->values[idx - 1];
    if (bound->kind == KEEL_SHARD_KEY_PARAM || bound->kind == KEEL_SHARD_KEY_NONE) {
        return KEEL_ERR_NOT_SUPPORTED;
    }

    return keel_shard_map_key_rule(bound, rule, shard_index_out);
}

/**
 * @brief Map a shard key to a shard index, resolving parameter bindings.
 *
 * If the key is a @c KEEL_SHARD_KEY_PARAM, the corresponding value is looked
 * up in @p params (1-based index) and mapped with the hash strategy via
 * @c keel_shard_map_key().  Non-param keys are forwarded directly.
 *
 * @param key             Shard key, possibly a parameter reference.
 * @param params          Bound parameter values; may be NULL only when @p key is
 *                        not a PARAM key.
 * @param shard_count     Total number of shards; must be > 0.
 * @param shard_index_out Output shard index; must not be NULL.
 * @return @c KEEL_OK on success; @c KEEL_ERR_INVALID_ARG for invalid inputs;
 *         @c KEEL_ERR_NOT_FOUND if the parameter index is out of range or
 *         @p params is NULL for a PARAM key.
 */
keel_error_t keel_shard_map_key_bound(const keel_shard_key_t*          key,
                                      const keel_shard_bound_params_t* params,
                                      size_t                           shard_count,
                                      size_t*                          shard_index_out) {
    if (!key || !shard_index_out || shard_count == 0) {
        return KEEL_ERR_INVALID_ARG;
    }

    if (key->kind != KEEL_SHARD_KEY_PARAM) {
        return keel_shard_map_key(key, shard_count, shard_index_out);
    }

    /* Resolve the parameter binding */
    if (!params) {
        return KEEL_ERR_NOT_FOUND;
    }

    int idx = key->value.param_index; /* 1-based */
    if (idx < 1 || (size_t)idx > params->count) {
        return KEEL_ERR_NOT_FOUND;
    }

    const keel_shard_key_t* bound = &params->values[idx - 1];
    if (bound->kind == KEEL_SHARD_KEY_PARAM || bound->kind == KEEL_SHARD_KEY_NONE) {
        return KEEL_ERR_NOT_SUPPORTED;
    }

    return keel_shard_map_key(bound, shard_count, shard_index_out);
}

/**
 * @brief Return whether an AST node kind is eligible for scatter planning.
 *
 * Only DML and SELECT statements are scatter-eligible; DDL, transaction
 * control, and other statement kinds are not.
 *
 * @param kind AST node kind to test.
 * @return @c true for SELECT, INSERT, UPDATE, and DELETE; @c false otherwise.
 */
static bool shard_plan_is_scatter_eligible(keel_sql_node_kind_t kind) {
    switch (kind) {
    case KEEL_SQL_NODE_STMT_SELECT:
    case KEEL_SQL_NODE_STMT_INSERT:
    case KEEL_SQL_NODE_STMT_UPDATE:
    case KEEL_SQL_NODE_STMT_DELETE:
    case KEEL_SQL_NODE_STMT_WITH:    /* WITH ... SELECT/UPDATE/DELETE: always scatter */
        return true;
    default:
        return false;
    }
}

/**
 * @brief Produce a complete shard execution plan for a SQL string.
 *
 * Parses @p sql, extracts the shard key using @p rule, resolves any parameter
 * binding from @p params, and writes the resulting plan into @p plan.  The
 * plan kind is one of:
 * - @c KEEL_SHARD_PLAN_SINGLE  — routed to a single shard.
 * - @c KEEL_SHARD_PLAN_SCATTER — must be broadcast to all shards.
 * - @c KEEL_SHARD_PLAN_UNSUPPORTED — cannot be sharded (DDL, parse error, etc.).
 *
 * @param sql    SQL text to plan; must be non-empty.
 * @param rule   Sharding rule; NULL or invalid inputs yield UNSUPPORTED.
 * @param params Bound query parameters used to resolve @c $N references; may
 *               be NULL if the query contains no parameter predicates.
 * @param arena  Arena allocator for the parse tree; must not be NULL.
 * @param plan   Output plan structure; if NULL the function returns immediately.
 * @note On return, @c plan->shard_index is @c SIZE_MAX for non-SINGLE plans.
 */

/* ============================================================================
 * Process-wide hash mode (R2)
 * ============================================================================ */

static keel_shard_hash_mode_t g_shard_hash_mode = KEEL_SHARD_HASH_LEGACY;

keel_shard_hash_mode_t keel_shard_get_hash_mode(void) {
    return g_shard_hash_mode;
}

void keel_shard_set_hash_mode(keel_shard_hash_mode_t mode) {
    g_shard_hash_mode = mode;
}

keel_error_t keel_shard_parse_hash_mode(const char*              name,
                                        keel_shard_hash_mode_t*  out_mode) {
    if (!name || !out_mode) return KEEL_ERR_INVALID_ARG;
    /* Case-insensitive comparison; tolerate up to 15 chars. */
    char buf[16];
    size_t i = 0;
    for (; i < sizeof(buf) - 1 && name[i]; i++) {
        unsigned char c = (unsigned char)name[i];
        buf[i] = (char)tolower(c);
    }
    buf[i] = '\0';
    if (strcmp(buf, "legacy") == 0) { *out_mode = KEEL_SHARD_HASH_LEGACY; return KEEL_OK; }
    if (strcmp(buf, "abs") == 0)    { *out_mode = KEEL_SHARD_HASH_ABS;    return KEEL_OK; }
    return KEEL_ERR_INVALID_ARG;
}

void keel_shard_plan(keel_str_t                       sql,
                     const keel_shard_rule_t*         rule,
                     const keel_shard_bound_params_t* params,
                     keel_arena_t*                    arena,
                     keel_shard_plan_t*               plan) {
    if (!plan) return;

    plan->kind        = KEEL_SHARD_PLAN_UNSUPPORTED;
    plan->shard_index = SIZE_MAX;

    if (!rule || !rule->table || !rule->column || rule->shard_count == 0 ||
        !arena || !sql.data || sql.len == 0) {
        return;
    }

    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, sql, arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    if (!ast) {
        return; /* parse error → UNSUPPORTED */
    }

    if (!shard_plan_is_scatter_eligible(ast->kind)) {
        return; /* DDL, transaction control, … → UNSUPPORTED */
    }

    keel_shard_key_t key;
    keel_error_t err = keel_shard_extract_key_ast(ast, rule, &key);

    if (err == KEEL_ERR_NOT_FOUND) {
        /* Valid DML/SELECT but no single-shard predicate → scatter */
        plan->kind = KEEL_SHARD_PLAN_SCATTER;
        return;
    }

    if (err != KEEL_OK) {
        /* Conflicting shard predicates or other hard error → unsupported */
        plan->kind = KEEL_SHARD_PLAN_UNSUPPORTED;
        return;
    }

    size_t shard_index = SIZE_MAX;
    err = keel_shard_map_key_bound_rule(&key, params, rule, &shard_index);

    if (err == KEEL_ERR_NOT_FOUND) {
        /* PARAM key but no binding provided → scatter (caller must bind later) */
        plan->kind = KEEL_SHARD_PLAN_SCATTER;
        return;
    }

    if (err != KEEL_OK) {
        plan->kind = KEEL_SHARD_PLAN_UNSUPPORTED;
        return;
    }

    plan->kind        = KEEL_SHARD_PLAN_SINGLE;
    plan->shard_index = shard_index;
}