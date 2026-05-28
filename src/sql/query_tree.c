/**
 * @file query_tree.c
 * @brief Semantic lifting from AST structure into routing and caching facts.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The Query Tree builder deliberately compresses syntax into operational meaning.
 * It does not try to preserve every AST detail; instead it extracts the subset of
 * information the rest of KEEL uses to make decisions: operation class, whether
 * the query is replica-safe, which tables are touched, which columns participate,
 * and whether the query likely invalidates caches or dirties session state.
 */

#include "keel/sql/query_tree.h"
#include "keel/sql/sql_ast.h"
#include "keel/mem/mem.h"
#include "keel/util/util.h"

#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdarg.h>

/* ============================================================================
 * Internal Helpers
 * ============================================================================ */

/**
 * @brief Allocate and zero builder-owned storage from the arena.
 */
static void* qt_alloc(keel_qt_builder_t* b, size_t size) {
    void* ptr = keel_arena_alloc(b->arena, size);
    if (ptr) memset(ptr, 0, size);
    return ptr;
}

/**
 * @brief Prepend a new table-reference record to the query's table list.
 */
static keel_qt_table_ref_t* add_table_ref(keel_qt_builder_t* b, keel_qt_query_t* q) {
    keel_qt_table_ref_t* ref = qt_alloc(b, sizeof(keel_qt_table_ref_t));
    if (!ref) return NULL;
    
    ref->type = KEEL_QT_NODE_TABLE_REF;
    ref->next = q->tables;
    q->tables = ref;
    q->table_count++;
    
    return ref;
}

/**
 * @brief Prepend a new column-reference record to the query's column list.
 */
static keel_qt_column_ref_t* add_column_ref(keel_qt_builder_t* b, keel_qt_query_t* q) {
    keel_qt_column_ref_t* ref = qt_alloc(b, sizeof(keel_qt_column_ref_t));
    if (!ref) return NULL;
    
    ref->type = KEEL_QT_NODE_COLUMN_REF;
    ref->next = q->columns;
    q->columns = ref;
    q->column_count++;
    
    return ref;
}

/**
 * @brief Prepend a new function-reference record to the query's function list.
 */
static keel_qt_func_ref_t* add_func_ref(keel_qt_builder_t* b, keel_qt_query_t* q) {
    keel_qt_func_ref_t* ref = qt_alloc(b, sizeof(keel_qt_func_ref_t));
    if (!ref) return NULL;

    ref->type = KEEL_QT_NODE_FUNC_REF;
    ref->next = q->functions;
    q->functions = ref;
    q->func_count++;

    return ref;
}

/**
 * @brief Set error on builder
 */
static void qt_error(keel_qt_builder_t* b, const char* msg) {
    if (!b->has_error) {
        b->has_error = true;
        snprintf(b->error_msg, sizeof(b->error_msg), "%s", msg);
    }
}

/* ============================================================================
 * AST Visitors for Table/Column Extraction
 * ============================================================================ */

/**
 * @brief Context for table extraction visitor
 */
typedef struct {
    keel_qt_builder_t*   builder;
    keel_qt_query_t*     query;
    keel_qt_table_access_t access;
    int                 context;  /* 0=from, 1=where, 2=select, etc. */
} table_visitor_ctx_t;

/**
 * @brief AST visitor that extracts table references into Query Tree records.
 */
static keel_sql_visit_result_t visit_tables(keel_sql_node_t* node, void* ctx) {
    table_visitor_ctx_t* c = ctx;
    
    if (node->kind == KEEL_SQL_NODE_TABLE_REF) {
        keel_sql_table_ref_t* ast_ref = (keel_sql_table_ref_t*)node;
        keel_qt_table_ref_t* ref = add_table_ref(c->builder, c->query);
        if (ref) {
            ref->schema = ast_ref->schema;
            ref->table = ast_ref->table;
            ref->alias = ast_ref->alias;
            ref->access = c->access;
            ref->loc = ast_ref->base.loc;
        }
    }
    else if (node->kind == KEEL_SQL_NODE_TABLE_JOIN) {
        /* JOINs contain table refs - continue walking */
    }
    else if (node->kind == KEEL_SQL_NODE_EXPR_SUBQUERY) {
        /* Mark that we have subqueries */
        c->query->flags |= KEEL_QT_FLAG_HAS_SUBQUERY;
        /* Could recursively build query tree for subquery */
    }
    
    return KEEL_SQL_VISIT_CONTINUE;
}

/**
 * @brief AST visitor that extracts column references and their usage context.
 */
static keel_sql_visit_result_t visit_columns(keel_sql_node_t* node, void* ctx) {
    table_visitor_ctx_t* c = ctx;
    
    if (node->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
        keel_sql_expr_column_t* ast_col = (keel_sql_expr_column_t*)node;
        keel_qt_column_ref_t* ref = add_column_ref(c->builder, c->query);
        if (ref) {
            ref->table = ast_col->table;
            ref->column = ast_col->column;
            ref->loc = ast_col->base.loc;
            
            /* Set context flags */
            switch (c->context) {
            case 0: ref->in_where = true; break;
            case 1: ref->in_select = true; break;
            case 2: ref->in_group = true; break;
            case 3: ref->in_order = true; break;
            case 4: ref->is_output = true; break;
            case 5: ref->is_target = true; break;
            }
        }
    }
    
    return KEEL_SQL_VISIT_CONTINUE;
}

/**
 * @brief Context for function reference extraction visitor.
 */
typedef struct {
    keel_qt_builder_t* builder;
    keel_qt_query_t*   query;
} func_visitor_ctx_t;

static bool str_eq_lit_ci(keel_str_t s, const char* lit)
{
    size_t len = strlen(lit);
    return s.len == len && strncasecmp(s.data, lit, len) == 0;
}

static bool is_known_replica_safe_builtin(keel_str_t name)
{
    return str_eq_lit_ci(name, "avg") ||
           str_eq_lit_ci(name, "bool_and") ||
           str_eq_lit_ci(name, "bool_or") ||
           str_eq_lit_ci(name, "count") ||
           str_eq_lit_ci(name, "every") ||
           str_eq_lit_ci(name, "max") ||
           str_eq_lit_ci(name, "min") ||
           str_eq_lit_ci(name, "sum");
}

/**
 * @brief AST visitor that collects every user-defined function call into the
 *        Query Tree's function-reference list.
 *
 * Only a tiny allow-list of replica-safe aggregate builtins is skipped here.
 * Catalog-qualified calls are left to metadata-aware routing. Everything else
 * is recorded so the router can fail closed when catalog proof is unavailable.
 *
 * The visitor records KEEL_SQL_NODE_EXPR_FUNC, KEEL_SQL_NODE_EXPR_AGGR
 * (aggregate calls) and KEEL_SQL_NODE_EXPR_WINDOW (window function calls).
 * KEEL_SQL_NODE_STMT_CALL (stored procedure) is handled at the top of
 * keel_qt_build(), which marks the query NEEDS_PRIMARY unconditionally.
 */
static keel_sql_visit_result_t visit_functions(keel_sql_node_t* node, void* ctx) {
    func_visitor_ctx_t* c = ctx;

    if (node->kind != KEEL_SQL_NODE_EXPR_FUNC &&
        node->kind != KEEL_SQL_NODE_EXPR_AGGR &&
        node->kind != KEEL_SQL_NODE_EXPR_WINDOW) {
        return KEEL_SQL_VISIT_CONTINUE;
    }

    keel_sql_expr_func_t* fn = (keel_sql_expr_func_t*)node;

    /* Skip empty names (shouldn't happen in well-formed ASTs) */
    if (!fn->name.data || fn->name.len == 0) {
        return KEEL_SQL_VISIT_CONTINUE;
    }

    /* Skip only functions with built-in merge/read-only semantics. */
    if (is_known_replica_safe_builtin(fn->name)) {
        return KEEL_SQL_VISIT_CONTINUE;
    }

    keel_qt_func_ref_t* ref = add_func_ref(c->builder, c->query);
    if (ref) {
        ref->schema = fn->schema;
        ref->name   = fn->name;
        ref->loc    = fn->base.loc;
    }

    return KEEL_SQL_VISIT_CONTINUE;
}

/* ============================================================================
 * Query Tree Building
 * ============================================================================ */

/**
 * @brief Initialise a Query Tree builder with the given memory arena.
 *
 * Must be called before any keel_qt_build() invocations.  The @p arena
 * owns all allocations made during the build; the builder itself is
 * stack-allocated by the caller.
 *
 * @param builder  Builder struct to initialise.
 * @param arena    Arena for Query Tree node allocation.
 */
void keel_qt_builder_init(keel_qt_builder_t* builder, keel_arena_t* arena) {
    if (!builder) return;
    memset(builder, 0, sizeof(*builder));
    builder->arena = arena;
}

/**
 * @brief Lift a SELECT AST into a semantic Query Tree.
 *
 * The builder starts optimistic for plain SELECTs, then revokes that optimism if
 * it encounters locking clauses or other features that imply primary affinity.
 */
static keel_qt_query_t* build_select(keel_qt_builder_t* b, keel_sql_stmt_select_t* sel) {
    keel_qt_query_t* q = qt_alloc(b, sizeof(keel_qt_query_t));
    if (!q) return NULL;
    
    q->type = KEEL_QT_NODE_SELECT;
    q->operation = KEEL_QT_OP_READ;
    q->flags = KEEL_QT_FLAG_READONLY;
    q->ast = &sel->base;
    
    table_visitor_ctx_t ctx = { b, q, KEEL_QT_ACCESS_READ, 0 };
    
    /* Check for WITH clause */
    if (sel->with_clause && sel->with_clause->count > 0) {
        q->flags |= KEEL_QT_FLAG_HAS_CTE;
    }
    
    /* Extract tables from FROM clause */
    if (sel->from) {
        keel_sql_ast_walk(sel->from, visit_tables, &ctx);
    }
    
    /* Extract columns from SELECT list */
    ctx.context = 1;
    if (sel->targets) {
        keel_sql_ast_walk(&sel->targets->base, visit_columns, &ctx);
    }
    
    /* Extract columns from WHERE */
    ctx.context = 0;
    if (sel->where) {
        keel_sql_ast_walk(sel->where, visit_columns, &ctx);
    }
    
    /* Extract columns from GROUP BY */
    ctx.context = 2;
    if (sel->group_by) {
        keel_sql_ast_walk(&sel->group_by->base, visit_columns, &ctx);
    }
    
    /* Extract columns from ORDER BY */
    ctx.context = 3;
    if (sel->order_by) {
        keel_sql_ast_walk(&sel->order_by->base, visit_columns, &ctx);
    }
    
    /* Check for FOR UPDATE/SHARE (makes it a write operation) */
    if (sel->locking) {
        keel_sql_locking_t* lock = (keel_sql_locking_t*)sel->locking;
        if (lock->mode == KEEL_SQL_LOCK_FOR_UPDATE ||
            lock->mode == KEEL_SQL_LOCK_FOR_NO_KEY_UPDATE) {
            q->operation = KEEL_QT_OP_WRITE;
            q->flags &= ~KEEL_QT_FLAG_READONLY;
            q->flags |= KEEL_QT_FLAG_FOR_UPDATE;
            q->flags |= KEEL_QT_FLAG_NEEDS_PRIMARY;
        }
    }
    
    /* Multiple tables = multi-table query */
    if (q->table_count > 1) {
        q->flags |= KEEL_QT_FLAG_MULTI_TABLE;
    }
    
    /* The current implementation assumes simple SELECTs are deterministic and
     * cacheable unless later logic proves otherwise. This is intentionally a
     * heuristic, not a full volatility analysis. */
    q->flags |= KEEL_QT_FLAG_CACHEABLE;
    q->flags |= KEEL_QT_FLAG_DETERMINISTIC;
    
    return q;
}

/**
 * @brief Lift an INSERT AST into write-oriented semantic form.
 */
static keel_qt_query_t* build_insert(keel_qt_builder_t* b, keel_sql_stmt_insert_t* ins) {
    keel_qt_query_t* q = qt_alloc(b, sizeof(keel_qt_query_t));
    if (!q) return NULL;
    
    q->type = KEEL_QT_NODE_INSERT;
    q->operation = KEEL_QT_OP_WRITE;
    q->flags = KEEL_QT_FLAG_NEEDS_PRIMARY | KEEL_QT_FLAG_INVALIDATES;
    q->ast = &ins->base;
    
    table_visitor_ctx_t ctx = { b, q, KEEL_QT_ACCESS_WRITE, 0 };
    
    /* Target table */
    if (ins->table) {
        ctx.access = KEEL_QT_ACCESS_WRITE;
        keel_sql_ast_walk(ins->table, visit_tables, &ctx);
        q->target_table = q->tables;  /* First table is target */
    }
    
    /* Source tables (if INSERT ... SELECT) */
    if (ins->source && ins->source->kind == KEEL_SQL_NODE_STMT_SELECT) {
        ctx.access = KEEL_QT_ACCESS_READ;
        keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ins->source;
        if (sel->from) {
            keel_sql_ast_walk(sel->from, visit_tables, &ctx);
        }
    }
    
    /* RETURNING clause */
    if (ins->returning && ins->returning->count > 0) {
        q->flags |= KEEL_QT_FLAG_HAS_RETURNING;
        ctx.context = 4;
        keel_sql_ast_walk(&ins->returning->base, visit_columns, &ctx);
    }
    
    return q;
}

/**
 * @brief Lift an UPDATE AST into write-oriented semantic form.
 */
static keel_qt_query_t* build_update(keel_qt_builder_t* b, keel_sql_stmt_update_t* upd) {
    keel_qt_query_t* q = qt_alloc(b, sizeof(keel_qt_query_t));
    if (!q) return NULL;
    
    q->type = KEEL_QT_NODE_UPDATE;
    q->operation = KEEL_QT_OP_WRITE;
    q->flags = KEEL_QT_FLAG_NEEDS_PRIMARY | KEEL_QT_FLAG_INVALIDATES;
    q->ast = &upd->base;
    
    table_visitor_ctx_t ctx = { b, q, KEEL_QT_ACCESS_WRITE, 0 };
    
    /* Target table */
    if (upd->table) {
        ctx.access = KEEL_QT_ACCESS_WRITE;
        keel_sql_ast_walk(upd->table, visit_tables, &ctx);
        q->target_table = q->tables;
    }
    
    /* FROM clause tables (read) */
    if (upd->from) {
        ctx.access = KEEL_QT_ACCESS_READ;
        keel_sql_ast_walk(upd->from, visit_tables, &ctx);
    }
    
    /* SET clause columns (write targets) */
    ctx.context = 5;
    if (upd->set_list) {
        keel_sql_ast_walk(&upd->set_list->base, visit_columns, &ctx);
    }
    
    /* WHERE clause columns */
    ctx.context = 0;
    if (upd->where) {
        keel_sql_ast_walk(upd->where, visit_columns, &ctx);
    }
    
    /* RETURNING clause */
    if (upd->returning && upd->returning->count > 0) {
        q->flags |= KEEL_QT_FLAG_HAS_RETURNING;
        ctx.context = 4;
        keel_sql_ast_walk(&upd->returning->base, visit_columns, &ctx);
    }
    
    return q;
}

/**
 * @brief Lift a DELETE AST into write-oriented semantic form.
 */
static keel_qt_query_t* build_delete(keel_qt_builder_t* b, keel_sql_stmt_delete_t* del) {
    keel_qt_query_t* q = qt_alloc(b, sizeof(keel_qt_query_t));
    if (!q) return NULL;
    
    q->type = KEEL_QT_NODE_DELETE;
    q->operation = KEEL_QT_OP_WRITE;
    q->flags = KEEL_QT_FLAG_NEEDS_PRIMARY | KEEL_QT_FLAG_INVALIDATES;
    q->ast = &del->base;
    
    table_visitor_ctx_t ctx = { b, q, KEEL_QT_ACCESS_WRITE, 0 };
    
    /* Target table */
    if (del->table) {
        ctx.access = KEEL_QT_ACCESS_WRITE;
        keel_sql_ast_walk(del->table, visit_tables, &ctx);
        q->target_table = q->tables;
    }
    
    /* USING tables (read) */
    if (del->using) {
        ctx.access = KEEL_QT_ACCESS_READ;
        keel_sql_ast_walk(del->using, visit_tables, &ctx);
    }
    
    /* WHERE clause columns */
    ctx.context = 0;
    if (del->where) {
        keel_sql_ast_walk(del->where, visit_columns, &ctx);
    }
    
    /* RETURNING clause */
    if (del->returning && del->returning->count > 0) {
        q->flags |= KEEL_QT_FLAG_HAS_RETURNING;
        ctx.context = 4;
        keel_sql_ast_walk(&del->returning->base, visit_columns, &ctx);
    }
    
    return q;
}

/**
 * @brief Convert transaction-control AST nodes into semantic transaction markers.
 */
static keel_qt_query_t* build_transaction(keel_qt_builder_t* b, keel_sql_node_t* node) {
    keel_qt_query_t* q = qt_alloc(b, sizeof(keel_qt_query_t));
    if (!q) return NULL;
    
    q->operation = KEEL_QT_OP_TRANSACTION;
    q->ast = node;
    
    switch (node->kind) {
    case KEEL_SQL_NODE_STMT_BEGIN:
        q->type = KEEL_QT_NODE_BEGIN;
        q->flags = KEEL_QT_FLAG_STARTS_TXN;
        break;
    case KEEL_SQL_NODE_STMT_COMMIT:
        q->type = KEEL_QT_NODE_COMMIT;
        q->flags = KEEL_QT_FLAG_ENDS_TXN;
        break;
    case KEEL_SQL_NODE_STMT_ROLLBACK:
        q->type = KEEL_QT_NODE_ROLLBACK;
        q->flags = KEEL_QT_FLAG_ENDS_TXN;
        break;
    case KEEL_SQL_NODE_STMT_SAVEPOINT:
        q->type = KEEL_QT_NODE_SAVEPOINT;
        q->flags = KEEL_QT_FLAG_IN_TXN;
        break;
    default:
        break;
    }
    
    return q;
}

/**
 * @brief Convert session-mutating statements into semantic session-dirtiness markers.
 */
static keel_qt_query_t* build_session(keel_qt_builder_t* b, keel_sql_node_t* node) {
    keel_qt_query_t* q = qt_alloc(b, sizeof(keel_qt_query_t));
    if (!q) return NULL;
    
    q->operation = KEEL_QT_OP_SESSION;
    q->flags = KEEL_QT_FLAG_MODIFIES_SESSION;
    q->ast = node;
    
    switch (node->kind) {
    case KEEL_SQL_NODE_STMT_SET:
        q->type = KEEL_QT_NODE_SET;
        break;
    default:
        q->type = KEEL_QT_NODE_RESET;
        break;
    }
    
    return q;
}

/**
 * @brief Convert DDL AST nodes into primary-bound invalidating semantic form.
 */
static keel_qt_query_t* build_ddl(keel_qt_builder_t* b, keel_sql_node_t* node) {
    keel_qt_query_t* q = qt_alloc(b, sizeof(keel_qt_query_t));
    if (!q) return NULL;
    
    q->operation = KEEL_QT_OP_DDL;
    q->flags = KEEL_QT_FLAG_NEEDS_PRIMARY | KEEL_QT_FLAG_INVALIDATES;
    q->ast = node;
    
    switch (node->kind) {
    case KEEL_SQL_NODE_STMT_CREATE:
        q->type = KEEL_QT_NODE_CREATE;
        break;
    case KEEL_SQL_NODE_STMT_ALTER:
        q->type = KEEL_QT_NODE_ALTER;
        break;
    case KEEL_SQL_NODE_STMT_DROP:
        q->type = KEEL_QT_NODE_DROP;
        break;
    case KEEL_SQL_NODE_STMT_TRUNCATE:
        q->type = KEEL_QT_NODE_TRUNCATE;
        break;
    default:
        break;
    }
    
    return q;
}

/**
 * @brief Convert prepared-statement control commands into semantic session/query nodes.
 */
static keel_qt_query_t* build_prepared(keel_qt_builder_t* b, keel_sql_node_t* node) {
    keel_qt_query_t* q = qt_alloc(b, sizeof(keel_qt_query_t));
    if (!q) return NULL;
    
    q->operation = KEEL_QT_OP_SESSION;
    q->flags = KEEL_QT_FLAG_MODIFIES_SESSION;
    q->ast = node;
    
    switch (node->kind) {
    case KEEL_SQL_NODE_STMT_PREPARE:
        q->type = KEEL_QT_NODE_PREPARE;
        {
            keel_sql_stmt_prepare_t* prep = (keel_sql_stmt_prepare_t*)node;
            q->stmt_name = prep->name;
        }
        break;
    case KEEL_SQL_NODE_STMT_EXECUTE:
        q->type = KEEL_QT_NODE_EXECUTE;
        {
            keel_sql_stmt_execute_t* exec = (keel_sql_stmt_execute_t*)node;
            q->stmt_name = exec->name;
            if (exec->params) {
                q->param_count = exec->params->count;
            }
        }
        /* EXECUTE might be read or write - unknown until we know the prepared stmt */
        q->operation = KEEL_QT_OP_UNKNOWN;
        q->flags = KEEL_QT_FLAG_PARTIAL_PARSE;
        break;
    case KEEL_SQL_NODE_STMT_DEALLOCATE:
        q->type = KEEL_QT_NODE_DEALLOCATE;
        break;
    default:
        break;
    }
    
    return q;
}

/**
 * @brief Dispatch AST lifting based on top-level statement kind.
 *
 * When semantic certainty is limited, the builder intentionally marks the Query
 * Tree as `PARTIAL_PARSE` rather than overclaiming safety.
 */
keel_qt_query_t* keel_qt_build(keel_qt_builder_t* builder, keel_sql_node_t* ast) {
    if (!builder || !ast) return NULL;
    
    keel_qt_query_t* q = NULL;
    
    switch (ast->kind) {
    case KEEL_SQL_NODE_STMT_SELECT:
        q = build_select(builder, (keel_sql_stmt_select_t*)ast);
        break;
        
    case KEEL_SQL_NODE_STMT_INSERT:
        q = build_insert(builder, (keel_sql_stmt_insert_t*)ast);
        break;
        
    case KEEL_SQL_NODE_STMT_UPDATE:
        q = build_update(builder, (keel_sql_stmt_update_t*)ast);
        break;
        
    case KEEL_SQL_NODE_STMT_DELETE:
        q = build_delete(builder, (keel_sql_stmt_delete_t*)ast);
        break;
        
    case KEEL_SQL_NODE_STMT_BEGIN:
    case KEEL_SQL_NODE_STMT_COMMIT:
    case KEEL_SQL_NODE_STMT_ROLLBACK:
    case KEEL_SQL_NODE_STMT_SAVEPOINT:
        q = build_transaction(builder, ast);
        break;
        
    case KEEL_SQL_NODE_STMT_SET:
        q = build_session(builder, ast);
        break;
        
    case KEEL_SQL_NODE_STMT_CREATE:
    case KEEL_SQL_NODE_STMT_ALTER:
    case KEEL_SQL_NODE_STMT_DROP:
    case KEEL_SQL_NODE_STMT_TRUNCATE:
        q = build_ddl(builder, ast);
        break;
        
    case KEEL_SQL_NODE_STMT_PREPARE:
    case KEEL_SQL_NODE_STMT_EXECUTE:
    case KEEL_SQL_NODE_STMT_DEALLOCATE:
        q = build_prepared(builder, ast);
        break;
        
    case KEEL_SQL_NODE_STMT_SHOW:
    case KEEL_SQL_NODE_STMT_EXPLAIN:
        q = qt_alloc(builder, sizeof(keel_qt_query_t));
        if (q) {
            q->type = (ast->kind == KEEL_SQL_NODE_STMT_SHOW) ? 
                      KEEL_QT_NODE_SHOW : KEEL_QT_NODE_EXPLAIN;
            q->operation = KEEL_QT_OP_READ;
            q->flags = KEEL_QT_FLAG_READONLY;
            q->ast = ast;
        }
        break;
        
    case KEEL_SQL_NODE_STMT_COPY:
        q = qt_alloc(builder, sizeof(keel_qt_query_t));
        if (q) {
            q->type = KEEL_QT_NODE_COPY;
            q->operation = KEEL_QT_OP_WRITE;  /* Assume write, could be read */
            q->flags = KEEL_QT_FLAG_NEEDS_PRIMARY;
            q->ast = ast;
        }
        break;
        
    case KEEL_SQL_NODE_STMT_CALL:
    case KEEL_SQL_NODE_STMT_DO:
        q = qt_alloc(builder, sizeof(keel_qt_query_t));
        if (q) {
            q->type = KEEL_QT_NODE_CALL;
            q->operation = KEEL_QT_OP_UNKNOWN;  /* Could be anything */
            q->flags = KEEL_QT_FLAG_NEEDS_PRIMARY | KEEL_QT_FLAG_PARTIAL_PARSE;
            q->ast = ast;
        }
        break;

    case KEEL_SQL_NODE_STMT_MERGE:
        q = qt_alloc(builder, sizeof(keel_qt_query_t));
        if (q) {
            q->type = KEEL_QT_NODE_MERGE;
            q->operation = KEEL_QT_OP_WRITE;
            q->flags = KEEL_QT_FLAG_NEEDS_PRIMARY;
            q->ast = ast;
        }
        break;

    case KEEL_SQL_NODE_STMT_LOCK:
    case KEEL_SQL_NODE_STMT_VACUUM:
        q = qt_alloc(builder, sizeof(keel_qt_query_t));
        if (q) {
            q->type = KEEL_QT_NODE_UNKNOWN;
            q->operation = KEEL_QT_OP_ADMIN;
            q->flags = KEEL_QT_FLAG_NEEDS_PRIMARY | KEEL_QT_FLAG_PARTIAL_PARSE;
            q->ast = ast;
        }
        break;

    case KEEL_SQL_NODE_STMT_LISTEN:
    case KEEL_SQL_NODE_STMT_NOTIFY:
        q = qt_alloc(builder, sizeof(keel_qt_query_t));
        if (q) {
            q->type = KEEL_QT_NODE_UNKNOWN;
            q->operation = KEEL_QT_OP_SESSION;
            q->flags = KEEL_QT_FLAG_PARTIAL_PARSE;
            q->ast = ast;
        }
        break;
        
    default:
        q = qt_alloc(builder, sizeof(keel_qt_query_t));
        if (q) {
            q->type = KEEL_QT_NODE_UNKNOWN;
            q->operation = KEEL_QT_OP_UNKNOWN;
            q->flags = KEEL_QT_FLAG_PARTIAL_PARSE;
            q->ast = ast;
        }
        break;
    }

    /* Walk the full AST to collect every function call reference.
     * This is done once here, covering all statement types uniformly,
     * rather than repeating the walk in every individual builder.
     * Statements that are unconditionally primary-bound (CALL, DDL, …)
     * still benefit from the function list for logging / tracing purposes. */
    if (q && q->ast) {
        func_visitor_ctx_t fctx = { builder, q };
        keel_sql_ast_walk(q->ast, visit_functions, &fctx);

        if (q->operation == KEEL_QT_OP_READ && q->func_count > 0) {
            q->flags &= ~(uint32_t)(KEEL_QT_FLAG_READONLY |
                                    KEEL_QT_FLAG_CACHEABLE |
                                    KEEL_QT_FLAG_DETERMINISTIC);
            q->flags |= KEEL_QT_FLAG_NEEDS_PRIMARY | KEEL_QT_FLAG_NO_CACHE;
        }
    }

    return q;
}

/* ============================================================================
 * Query Tree Analysis
 * ============================================================================ */

/**
 * @brief Apply KEEL's conservative replica-routing policy to a Query Tree.
 */
bool keel_qt_can_use_replica(const keel_qt_query_t* qt) {
    if (!qt) return false;

    if (qt->has_error ||
        (qt->flags & KEEL_QT_FLAG_PARTIAL_PARSE) ||
        qt->func_count > 0) {
        return false;
    }
    
    /* If explicitly needs primary, no */
    if (qt->flags & KEEL_QT_FLAG_NEEDS_PRIMARY) {
        return false;
    }
    
    /* If read-only flag is set, yes */
    if (qt->flags & KEEL_QT_FLAG_READONLY) {
        return true;
    }
    
    /* Check operation type */
    switch (qt->operation) {
    case KEEL_QT_OP_READ:
        return true;
    case KEEL_QT_OP_WRITE:
    case KEEL_QT_OP_DDL:
    case KEEL_QT_OP_ADMIN:
        return false;
    case KEEL_QT_OP_TRANSACTION:
    case KEEL_QT_OP_SESSION:
        /* Depends on context - usually needs primary for consistency */
        return false;
    case KEEL_QT_OP_UNKNOWN:
    default:
        /* Unknown = assume needs primary */
        return false;
    }
}

/**
 * @brief Check whether deterministic-query analysis flagged the tree as stable.
 */
bool keel_qt_is_deterministic(const keel_qt_query_t* qt) {
    if (!qt) return false;
    return (qt->flags & KEEL_QT_FLAG_DETERMINISTIC) != 0;
}

/**
 * @brief Check whether the Query Tree is currently considered cacheable.
 */
bool keel_qt_is_cacheable(const keel_qt_query_t* qt) {
    if (!qt) return false;
    
    /* Explicitly non-cacheable */
    if (qt->flags & KEEL_QT_FLAG_NO_CACHE) {
        return false;
    }
    
    /* Only read operations are cacheable */
    if (qt->operation != KEEL_QT_OP_READ) {
        return false;
    }
    
    /* Check cacheable flag */
    return (qt->flags & KEEL_QT_FLAG_CACHEABLE) != 0;
}

/**
 * @brief Enumerate write-target tables whose caches should be invalidated.
 */
size_t keel_qt_get_invalidated_tables(
    const keel_qt_query_t* qt,
    keel_qt_table_ref_t**  tables,
    size_t                max
) {
    if (!qt || !tables || max == 0) return 0;
    
    /* Only write operations invalidate tables */
    if (!(qt->flags & KEEL_QT_FLAG_INVALIDATES)) {
        return 0;
    }
    
    size_t count = 0;
    for (keel_qt_table_ref_t* ref = qt->tables; ref && count < max; ref = ref->next) {
        if (ref->access == KEEL_QT_ACCESS_WRITE) {
            tables[count++] = ref;
        }
    }
    
    return count;
}

/**
 * @brief Fold one string slice into the running FNV-1a hash.
 */
static uint64_t hash_str(keel_str_t s, uint64_t hash) {
    for (size_t i = 0; i < s.len; i++) {
        hash ^= (uint64_t)(unsigned char)s.data[i];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

/**
 * @brief Compute a structural query fingerprint from semantic references.
 */
uint64_t keel_qt_compute_cache_key(const keel_qt_query_t* qt) {
    if (!qt) return 0;
    
    uint64_t hash = 0xcbf29ce484222325ULL;  /* FNV offset basis */
    
    /* Hash query type */
    hash ^= (uint64_t)qt->type;
    hash *= 0x100000001b3ULL;
    
    /* Hash tables */
    for (keel_qt_table_ref_t* ref = qt->tables; ref; ref = ref->next) {
        if (ref->schema.len > 0) {
            hash = hash_str(ref->schema, hash);
        }
        hash = hash_str(ref->table, hash);
    }
    
    /* Hash columns */
    for (keel_qt_column_ref_t* col = qt->columns; col; col = col->next) {
        if (col->table.len > 0) {
            hash = hash_str(col->table, hash);
        }
        hash = hash_str(col->column, hash);
    }
    
    return hash;
}

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * @brief Return the canonical string name of a Query Tree operation type.
 *
 * @param op  Operation enum value.
 * @return Static string (e.g. "READ", "WRITE", "DDL"), or "?" for unknown.
 */
const char* keel_qt_operation_name(keel_qt_operation_t op) {
    switch (op) {
    case KEEL_QT_OP_READ:        return "READ";
    case KEEL_QT_OP_WRITE:       return "WRITE";
    case KEEL_QT_OP_DDL:         return "DDL";
    case KEEL_QT_OP_ADMIN:       return "ADMIN";
    case KEEL_QT_OP_TRANSACTION: return "TRANSACTION";
    case KEEL_QT_OP_SESSION:     return "SESSION";
    case KEEL_QT_OP_UNKNOWN:     return "UNKNOWN";
    default:                    return "?";
    }
}

/**
 * @brief Return the canonical string name of a Query Tree node type.
 *
 * @param type  Node type enum value.
 * @return Static string (e.g. "SELECT", "INSERT", "BEGIN"), or "?" for unknown.
 */
const char* keel_qt_node_type_name(keel_qt_node_type_t type) {
    switch (type) {
    case KEEL_QT_NODE_SELECT:     return "SELECT";
    case KEEL_QT_NODE_INSERT:     return "INSERT";
    case KEEL_QT_NODE_UPDATE:     return "UPDATE";
    case KEEL_QT_NODE_DELETE:     return "DELETE";
    case KEEL_QT_NODE_MERGE:      return "MERGE";
    case KEEL_QT_NODE_CREATE:     return "CREATE";
    case KEEL_QT_NODE_ALTER:      return "ALTER";
    case KEEL_QT_NODE_DROP:       return "DROP";
    case KEEL_QT_NODE_TRUNCATE:   return "TRUNCATE";
    case KEEL_QT_NODE_BEGIN:      return "BEGIN";
    case KEEL_QT_NODE_COMMIT:     return "COMMIT";
    case KEEL_QT_NODE_ROLLBACK:   return "ROLLBACK";
    case KEEL_QT_NODE_SAVEPOINT:  return "SAVEPOINT";
    case KEEL_QT_NODE_SET:        return "SET";
    case KEEL_QT_NODE_RESET:      return "RESET";
    case KEEL_QT_NODE_DISCARD:    return "DISCARD";
    case KEEL_QT_NODE_PREPARE:    return "PREPARE";
    case KEEL_QT_NODE_EXECUTE:    return "EXECUTE";
    case KEEL_QT_NODE_DEALLOCATE: return "DEALLOCATE";
    case KEEL_QT_NODE_SHOW:       return "SHOW";
    case KEEL_QT_NODE_EXPLAIN:    return "EXPLAIN";
    case KEEL_QT_NODE_CALL:       return "CALL";
    case KEEL_QT_NODE_COPY:       return "COPY";
    case KEEL_QT_NODE_UNKNOWN:    return "UNKNOWN";
    default:                     return "?";
    }
}

/**
 * @brief Return the canonical display name for a SQL dialect.
 *
 * @param dialect  Dialect enum value.
 * @return Static string (e.g. "PostgreSQL", "MySQL"), or "Unknown" for unrecognised.
 */
const char* keel_sql_dialect_name(keel_sql_dialect_t dialect) {
    switch (dialect) {
    case KEEL_DIALECT_POSTGRESQL: return "PostgreSQL";
    case KEEL_DIALECT_MYSQL:      return "MySQL";
    case KEEL_DIALECT_MARIADB:    return "MariaDB";
    case KEEL_DIALECT_SQLITE:     return "SQLite";
    case KEEL_DIALECT_STANDARD:   return "SQL Standard";
    default:                     return "Unknown";
    }
}

/**
 * @brief Emit a verbose multi-line debug dump of the Query Tree.
 */
void keel_qt_dump(const keel_qt_query_t* qt, FILE* out) {
    if (!qt || !out) return;
    
    fprintf(out, "Query Tree:\n");
    fprintf(out, "  Type: %s\n", keel_qt_node_type_name(qt->type));
    fprintf(out, "  Operation: %s\n", keel_qt_operation_name(qt->operation));
    fprintf(out, "  Flags: 0x%08x\n", qt->flags);
    
    if (qt->flags & KEEL_QT_FLAG_NEEDS_PRIMARY)   fprintf(out, "    - NEEDS_PRIMARY\n");
    if (qt->flags & KEEL_QT_FLAG_READONLY)        fprintf(out, "    - READONLY\n");
    if (qt->flags & KEEL_QT_FLAG_CACHEABLE)       fprintf(out, "    - CACHEABLE\n");
    if (qt->flags & KEEL_QT_FLAG_INVALIDATES)     fprintf(out, "    - INVALIDATES\n");
    if (qt->flags & KEEL_QT_FLAG_STARTS_TXN)      fprintf(out, "    - STARTS_TXN\n");
    if (qt->flags & KEEL_QT_FLAG_ENDS_TXN)        fprintf(out, "    - ENDS_TXN\n");
    if (qt->flags & KEEL_QT_FLAG_FOR_UPDATE)      fprintf(out, "    - FOR_UPDATE\n");
    if (qt->flags & KEEL_QT_FLAG_HAS_RETURNING)   fprintf(out, "    - HAS_RETURNING\n");
    if (qt->flags & KEEL_QT_FLAG_HAS_SUBQUERY)    fprintf(out, "    - HAS_SUBQUERY\n");
    
    fprintf(out, "  Tables (%zu):\n", qt->table_count);
    for (keel_qt_table_ref_t* t = qt->tables; t; t = t->next) {
        fprintf(out, "    - %.*s", (int)t->table.len, t->table.data);
        if (t->schema.len > 0) {
            fprintf(out, " (schema: %.*s)", (int)t->schema.len, t->schema.data);
        }
        if (t->alias.len > 0) {
            fprintf(out, " AS %.*s", (int)t->alias.len, t->alias.data);
        }
        fprintf(out, " [%s]\n", 
                t->access == KEEL_QT_ACCESS_READ ? "READ" :
                t->access == KEEL_QT_ACCESS_WRITE ? "WRITE" : "EXCLUSIVE");
    }
    
    fprintf(out, "  Columns (%zu):\n", qt->column_count);
    for (keel_qt_column_ref_t* c = qt->columns; c; c = c->next) {
        fprintf(out, "    - ");
        if (c->table.len > 0) {
            fprintf(out, "%.*s.", (int)c->table.len, c->table.data);
        }
        fprintf(out, "%.*s", (int)c->column.len, c->column.data);
        fprintf(out, " [");
        if (c->in_select) fprintf(out, "SELECT ");
        if (c->in_where)  fprintf(out, "WHERE ");
        if (c->in_group)  fprintf(out, "GROUP ");
        if (c->in_order)  fprintf(out, "ORDER ");
        if (c->is_target) fprintf(out, "TARGET ");
        fprintf(out, "]\n");
    }
    
    fprintf(out, "  Cache Key: 0x%016llx\n", (unsigned long long)qt->cache_key);
}

/* ============================================================================
 * Query Tree — snprint to buffer
 * ============================================================================ */

/**
 * @brief Append formatted text into a bounded buffer while tracking full logical length.
 */
static int qt_sncat(char* buf, size_t bufsz, int pos, const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int n = 0;
    if ((size_t)pos < bufsz) {
        n = vsnprintf(buf + pos, bufsz - (size_t)pos, fmt, ap);
    } else {
        /* Still count how many chars would be needed */
        n = vsnprintf(NULL, 0, fmt, ap);
    }
    va_end(ap);
    return (n > 0) ? pos + n : pos;
}

/**
 * @brief Serialize a Query Tree into a compact single-buffer textual summary.
 */
int keel_qt_snprint(const keel_qt_query_t* qt, char* buf, size_t bufsz)
{
    if (!buf || bufsz == 0) return 0;

    if (!qt) {
        int n = snprintf(buf, bufsz, "(null query tree)");
        return n > 0 ? n : 0;
    }

    int pos = 0;

    pos = qt_sncat(buf, bufsz, pos, "QT{type=%s op=%s flags=0x%04x",
                   keel_qt_node_type_name(qt->type),
                   keel_qt_operation_name(qt->operation),
                   qt->flags);

    /* Flag annotations */
    if (qt->flags & KEEL_QT_FLAG_NEEDS_PRIMARY)
        pos = qt_sncat(buf, bufsz, pos, " PRIMARY");
    if (qt->flags & KEEL_QT_FLAG_READONLY)
        pos = qt_sncat(buf, bufsz, pos, " RO");
    if (qt->flags & KEEL_QT_FLAG_CACHEABLE)
        pos = qt_sncat(buf, bufsz, pos, " CACHE");
    if (qt->flags & KEEL_QT_FLAG_INVALIDATES)
        pos = qt_sncat(buf, bufsz, pos, " INVAL");
    if (qt->flags & KEEL_QT_FLAG_STARTS_TXN)
        pos = qt_sncat(buf, bufsz, pos, " TXN_START");
    if (qt->flags & KEEL_QT_FLAG_ENDS_TXN)
        pos = qt_sncat(buf, bufsz, pos, " TXN_END");
    if (qt->flags & KEEL_QT_FLAG_FOR_UPDATE)
        pos = qt_sncat(buf, bufsz, pos, " FOR_UPD");
    if (qt->flags & KEEL_QT_FLAG_HAS_RETURNING)
        pos = qt_sncat(buf, bufsz, pos, " RETURNING");
    if (qt->flags & KEEL_QT_FLAG_HAS_CTE)
        pos = qt_sncat(buf, bufsz, pos, " CTE");
    if (qt->flags & KEEL_QT_FLAG_HAS_SUBQUERY)
        pos = qt_sncat(buf, bufsz, pos, " SUBQ");
    if (qt->flags & KEEL_QT_FLAG_MULTI_TABLE)
        pos = qt_sncat(buf, bufsz, pos, " MULTI_TBL");
    if (qt->flags & KEEL_QT_FLAG_MODIFIES_SESSION)
        pos = qt_sncat(buf, bufsz, pos, " SESS_MOD");

    /* Tables */
    if (qt->table_count > 0) {
        pos = qt_sncat(buf, bufsz, pos, " tables=[");
        int idx = 0;
        for (keel_qt_table_ref_t* t = qt->tables; t; t = t->next, idx++) {
            if (idx > 0)
                pos = qt_sncat(buf, bufsz, pos, ", ");
            if (t->schema.len > 0) {
                pos = qt_sncat(buf, bufsz, pos, "%.*s.",
                               (int)t->schema.len, t->schema.data);
            }
            pos = qt_sncat(buf, bufsz, pos, "%.*s",
                           (int)t->table.len, t->table.data);
            if (t->alias.len > 0) {
                pos = qt_sncat(buf, bufsz, pos, " AS %.*s",
                               (int)t->alias.len, t->alias.data);
            }
            pos = qt_sncat(buf, bufsz, pos, "(%s)",
                           t->access == KEEL_QT_ACCESS_READ ? "R" :
                           t->access == KEEL_QT_ACCESS_WRITE ? "W" : "X");
        }
        pos = qt_sncat(buf, bufsz, pos, "]");
    }

    /* Columns */
    if (qt->column_count > 0) {
        pos = qt_sncat(buf, bufsz, pos, " cols=[");
        int idx = 0;
        for (keel_qt_column_ref_t* c = qt->columns; c; c = c->next, idx++) {
            if (idx > 0)
                pos = qt_sncat(buf, bufsz, pos, ", ");
            if (c->table.len > 0) {
                pos = qt_sncat(buf, bufsz, pos, "%.*s.",
                               (int)c->table.len, c->table.data);
            }
            pos = qt_sncat(buf, bufsz, pos, "%.*s",
                           (int)c->column.len, c->column.data);
        }
        pos = qt_sncat(buf, bufsz, pos, "]");
    }

    /* Target table for DML */
    if (qt->target_table && qt->target_table->table.len > 0) {
        pos = qt_sncat(buf, bufsz, pos, " target=%.*s",
                       (int)qt->target_table->table.len,
                       qt->target_table->table.data);
    }

    /* Prepared statement info */
    if (qt->stmt_name.len > 0) {
        pos = qt_sncat(buf, bufsz, pos, " stmt=%.*s",
                       (int)qt->stmt_name.len, qt->stmt_name.data);
    }

    pos = qt_sncat(buf, bufsz, pos, "}");

    return pos;
}
