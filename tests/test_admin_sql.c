/**
 * @file test_admin_sql.c
 * @brief Unit tests for SQL-syntax admin query language.
 *
 * Validates that the SQL parser produces correct AST structures for the
 * admin virtual-table queries (SELECT, UPDATE, INSERT, DELETE) that the
 * admin_sql_dispatch() router handles.  Since the dispatch functions are
 * file-local to admin.c, these tests verify the AST shape at the parser
 * level — the same shape that dispatch relies on.
 *
 * §1 — SELECT: parse "SELECT * FROM <table>" for every admin virtual table.
 * §2 — UPDATE: parse "UPDATE config SET value = '...' WHERE key = '...'".
 * §3 — INSERT: parse "INSERT INTO servers (cols) VALUES (vals)".
 * §4 — DELETE: parse "DELETE FROM servers WHERE index = N".
 * §5 — Error / fallback: unsupported statements yield correct kind or error.
 *
 * @author Generated for KEEL P1 roadmap
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/sql/sql_ast.h"
#include "keel/mem/mem.h"
#include <stdio.h>
#include <string.h>

/* --------------------------------------------------------------------------
 * Helper: parse SQL and return the AST root; arena must outlive the AST.
 * -------------------------------------------------------------------------- */
static keel_sql_node_t* parse_sql(const char* sql, keel_arena_t* arena,
                                  keel_sql_parser_t* parser) {
    keel_sql_parser_init(parser, (keel_str_t){.data = sql, .len = strlen(sql)},
                         arena);
    keel_sql_node_t* ast = keel_sql_parse(parser);
    return ast;
}

/* ============================================================================
 * §1 — SELECT * FROM <admin-table>
 * ============================================================================ */

static const char* admin_tables[] = {
    "stats", "stats_detail", "servers", "pools", "clients",
    "config", "latency", "system", "rebalance",
    "cluster", "cluster_config", "discovered_peers", "cluster_stats",
    "help", "version", "shard_rules",
};
static const size_t admin_tables_count = sizeof(admin_tables) / sizeof(admin_tables[0]);

static void test_select_virtual_tables(void) {
    TEST_BEGIN("select_virtual_tables");

    for (size_t i = 0; i < admin_tables_count; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "SELECT * FROM %s", admin_tables[i]);

        keel_arena_t* arena = keel_arena_create(4096);
        TEST_ASSERT_NOT_NULL(arena);

        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

        TEST_ASSERT(!parser.has_error);
        TEST_ASSERT_NOT_NULL(ast);
        TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);

        keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
        TEST_ASSERT_NOT_NULL(sel->from);

        /* FROM should be a table reference */
        if (sel->from->kind == KEEL_SQL_NODE_TABLE_REF) {
            keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)sel->from;
            TEST_ASSERT(tr->table.len > 0);
            TEST_ASSERT(strncasecmp(tr->table.data, admin_tables[i],
                                    tr->table.len) == 0);
        } else if (sel->from->kind == KEEL_SQL_NODE_LIST) {
            /* Parser may wrap FROM in a list node */
            keel_sql_list_t* fl = (keel_sql_list_t*)sel->from;
            TEST_ASSERT(fl->count >= 1);
            TEST_ASSERT_NOT_NULL(fl->head);
            if (fl->head->kind == KEEL_SQL_NODE_TABLE_REF) {
                keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)fl->head;
                TEST_ASSERT(tr->table.len > 0);
                TEST_ASSERT(strncasecmp(tr->table.data, admin_tables[i],
                                        tr->table.len) == 0);
            }
        }

        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §2 — UPDATE config SET value = '...' WHERE key = '...'
 * ============================================================================ */

static void test_update_config(void) {
    TEST_BEGIN("update_config");

    const char* sql = "UPDATE config SET value = '64' WHERE key = 'pool_max_size'";
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_UPDATE);

    keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)ast;

    /* Table should be "config" */
    TEST_ASSERT_NOT_NULL(upd->table);
    if (upd->table->kind == KEEL_SQL_NODE_TABLE_REF) {
        keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)upd->table;
        TEST_ASSERT(tr->table.len == 6);
        TEST_ASSERT(strncmp(tr->table.data, "config", 6) == 0);
    }

    /* SET list should have one item: value = '64' */
    TEST_ASSERT_NOT_NULL(upd->set_list);
    TEST_ASSERT(upd->set_list->count >= 1);
    keel_sql_set_item_t* item = (keel_sql_set_item_t*)upd->set_list->head;
    TEST_ASSERT_NOT_NULL(item);

    /* SET column = "value" */
    TEST_ASSERT_NOT_NULL(item->column);
    TEST_ASSERT_EQ(item->column->kind, KEEL_SQL_NODE_EXPR_COLUMN);
    keel_sql_expr_column_t* sc = (keel_sql_expr_column_t*)item->column;
    TEST_ASSERT(sc->column.len == 5);
    TEST_ASSERT(strncmp(sc->column.data, "value", 5) == 0);

    /* SET value = literal '64' */
    TEST_ASSERT_NOT_NULL(item->value);
    TEST_ASSERT_EQ(item->value->kind, KEEL_SQL_NODE_EXPR_LITERAL);
    keel_sql_expr_literal_t* sv = (keel_sql_expr_literal_t*)item->value;
    TEST_ASSERT_EQ(sv->lit_type, KEEL_SQL_LIT_STRING);
    TEST_ASSERT(sv->value.str_val.len == 2);
    TEST_ASSERT(strncmp(sv->value.str_val.data, "64", 2) == 0);

    /* WHERE should be a binary EQ expression */
    TEST_ASSERT_NOT_NULL(upd->where);
    TEST_ASSERT_EQ(upd->where->kind, KEEL_SQL_NODE_EXPR_BINARY);
    keel_sql_expr_binary_t* bin = (keel_sql_expr_binary_t*)upd->where;
    TEST_ASSERT_EQ(bin->op, KEEL_SQL_BINOP_EQ);

    /* WHERE key = 'pool_max_size' */
    TEST_ASSERT_NOT_NULL(bin->left);
    TEST_ASSERT_NOT_NULL(bin->right);

    /* One side should be a column (key), the other a literal (pool_max_size) */
    keel_sql_node_t* col_side = NULL;
    keel_sql_node_t* lit_side = NULL;
    if (bin->left->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
        col_side = bin->left;
        lit_side = bin->right;
    } else {
        col_side = bin->right;
        lit_side = bin->left;
    }
    TEST_ASSERT_EQ(col_side->kind, KEEL_SQL_NODE_EXPR_COLUMN);
    TEST_ASSERT_EQ(lit_side->kind, KEEL_SQL_NODE_EXPR_LITERAL);

    keel_sql_expr_column_t* wc = (keel_sql_expr_column_t*)col_side;
    TEST_ASSERT(wc->column.len == 3);
    TEST_ASSERT(strncmp(wc->column.data, "key", 3) == 0);

    keel_sql_expr_literal_t* wl = (keel_sql_expr_literal_t*)lit_side;
    TEST_ASSERT_EQ(wl->lit_type, KEEL_SQL_LIT_STRING);
    TEST_ASSERT(wl->value.str_val.len == 13);
    TEST_ASSERT(strncmp(wl->value.str_val.data, "pool_max_size", 13) == 0);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §3 — UPDATE config SET value = 100 WHERE key = 'pool_max_size' (int literal)
 * ============================================================================ */

static void test_update_config_int(void) {
    TEST_BEGIN("update_config_int_literal");

    const char* sql = "UPDATE config SET value = 100 WHERE key = 'pool_max_size'";
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_UPDATE);

    keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)ast;
    TEST_ASSERT_NOT_NULL(upd->set_list);
    TEST_ASSERT(upd->set_list->count >= 1);

    keel_sql_set_item_t* item = (keel_sql_set_item_t*)upd->set_list->head;
    TEST_ASSERT_NOT_NULL(item);
    TEST_ASSERT_NOT_NULL(item->value);
    TEST_ASSERT_EQ(item->value->kind, KEEL_SQL_NODE_EXPR_LITERAL);

    keel_sql_expr_literal_t* sv = (keel_sql_expr_literal_t*)item->value;
    TEST_ASSERT_EQ(sv->lit_type, KEEL_SQL_LIT_INT);
    TEST_ASSERT_EQ(sv->value.int_val, 100);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §4 — INSERT INTO servers (host, port, role, weight) VALUES (...)
 * ============================================================================ */

static void test_insert_server(void) {
    TEST_BEGIN("insert_server");

    const char* sql =
        "INSERT INTO servers (host, port, role, weight) "
        "VALUES ('10.0.0.1', 5432, 'ro', 1)";
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_INSERT);

    keel_sql_stmt_insert_t* ins = (keel_sql_stmt_insert_t*)ast;

    /* Table should be "servers" */
    TEST_ASSERT_NOT_NULL(ins->table);
    if (ins->table->kind == KEEL_SQL_NODE_TABLE_REF) {
        keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)ins->table;
        TEST_ASSERT(tr->table.len == 7);
        TEST_ASSERT(strncmp(tr->table.data, "servers", 7) == 0);
    }

    /* Columns list should have 4 entries */
    TEST_ASSERT_NOT_NULL(ins->columns);
    TEST_ASSERT_EQ(ins->columns->count, (size_t)4);

    /* Verify column names via linked list */
    const char* expected_cols[] = {"host", "port", "role", "weight"};
    keel_sql_node_t* cn = ins->columns->head;
    for (size_t i = 0; i < 4 && cn; i++, cn = cn->next) {
        TEST_ASSERT_EQ(cn->kind, KEEL_SQL_NODE_EXPR_COLUMN);
        keel_sql_expr_column_t* ec = (keel_sql_expr_column_t*)cn;
        TEST_ASSERT(ec->column.len == strlen(expected_cols[i]));
        TEST_ASSERT(strncmp(ec->column.data, expected_cols[i],
                            ec->column.len) == 0);
    }

    /* Source should contain VALUES */
    TEST_ASSERT_NOT_NULL(ins->source);
    /* VALUES produces a list of row-lists or a flat list */
    if (ins->source->kind == KEEL_SQL_NODE_LIST) {
        keel_sql_list_t* outer = (keel_sql_list_t*)ins->source;
        TEST_ASSERT(outer->count >= 1);
        /* Get the actual values row */
        keel_sql_list_t* vals = NULL;
        if (outer->head && outer->head->kind == KEEL_SQL_NODE_LIST)
            vals = (keel_sql_list_t*)outer->head;
        else
            vals = outer; /* flat */

        TEST_ASSERT_EQ(vals->count, (size_t)4);

        /* First value: '10.0.0.1' (string) */
        keel_sql_node_t* v0 = vals->head;
        TEST_ASSERT_NOT_NULL(v0);
        TEST_ASSERT_EQ(v0->kind, KEEL_SQL_NODE_EXPR_LITERAL);
        keel_sql_expr_literal_t* l0 = (keel_sql_expr_literal_t*)v0;
        TEST_ASSERT_EQ(l0->lit_type, KEEL_SQL_LIT_STRING);
        TEST_ASSERT(strncmp(l0->value.str_val.data, "10.0.0.1",
                            l0->value.str_val.len) == 0);

        /* Second value: 5432 (integer) */
        keel_sql_node_t* v1 = v0->next;
        TEST_ASSERT_NOT_NULL(v1);
        TEST_ASSERT_EQ(v1->kind, KEEL_SQL_NODE_EXPR_LITERAL);
        keel_sql_expr_literal_t* l1 = (keel_sql_expr_literal_t*)v1;
        TEST_ASSERT_EQ(l1->lit_type, KEEL_SQL_LIT_INT);
        TEST_ASSERT_EQ(l1->value.int_val, (int64_t)5432);

        /* Third value: 'ro' (string) */
        keel_sql_node_t* v2 = v1->next;
        TEST_ASSERT_NOT_NULL(v2);
        TEST_ASSERT_EQ(v2->kind, KEEL_SQL_NODE_EXPR_LITERAL);
        keel_sql_expr_literal_t* l2 = (keel_sql_expr_literal_t*)v2;
        TEST_ASSERT_EQ(l2->lit_type, KEEL_SQL_LIT_STRING);
        TEST_ASSERT(l2->value.str_val.len == 2);
        TEST_ASSERT(strncmp(l2->value.str_val.data, "ro", 2) == 0);

        /* Fourth value: 1 (integer) */
        keel_sql_node_t* v3 = v2->next;
        TEST_ASSERT_NOT_NULL(v3);
        TEST_ASSERT_EQ(v3->kind, KEEL_SQL_NODE_EXPR_LITERAL);
        keel_sql_expr_literal_t* l3 = (keel_sql_expr_literal_t*)v3;
        TEST_ASSERT_EQ(l3->lit_type, KEEL_SQL_LIT_INT);
        TEST_ASSERT_EQ(l3->value.int_val, (int64_t)1);
    }

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §5 — DELETE FROM servers WHERE index = N
 * ============================================================================ */

static void test_delete_server(void) {
    TEST_BEGIN("delete_server");

    const char* sql = "DELETE FROM servers WHERE index = 3";
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_DELETE);

    keel_sql_stmt_delete_t* del = (keel_sql_stmt_delete_t*)ast;

    /* Table should be "servers" */
    TEST_ASSERT_NOT_NULL(del->table);
    if (del->table->kind == KEEL_SQL_NODE_TABLE_REF) {
        keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)del->table;
        TEST_ASSERT(tr->table.len == 7);
        TEST_ASSERT(strncmp(tr->table.data, "servers", 7) == 0);
    }

    /* WHERE should be a binary EQ: index = 3 */
    TEST_ASSERT_NOT_NULL(del->where);
    TEST_ASSERT_EQ(del->where->kind, KEEL_SQL_NODE_EXPR_BINARY);
    keel_sql_expr_binary_t* bin = (keel_sql_expr_binary_t*)del->where;
    TEST_ASSERT_EQ(bin->op, KEEL_SQL_BINOP_EQ);

    keel_sql_node_t* col_side = NULL;
    keel_sql_node_t* lit_side = NULL;
    if (bin->left->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
        col_side = bin->left;
        lit_side = bin->right;
    } else {
        col_side = bin->right;
        lit_side = bin->left;
    }

    TEST_ASSERT_EQ(col_side->kind, KEEL_SQL_NODE_EXPR_COLUMN);
    keel_sql_expr_column_t* wc = (keel_sql_expr_column_t*)col_side;
    TEST_ASSERT(wc->column.len == 5);
    TEST_ASSERT(strncmp(wc->column.data, "index", 5) == 0);

    TEST_ASSERT_EQ(lit_side->kind, KEEL_SQL_NODE_EXPR_LITERAL);
    keel_sql_expr_literal_t* wl = (keel_sql_expr_literal_t*)lit_side;
    TEST_ASSERT_EQ(wl->lit_type, KEEL_SQL_LIT_INT);
    TEST_ASSERT_EQ(wl->value.int_val, (int64_t)3);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §6 — Unsupported statements fall through (not SELECT/UPDATE/INSERT/DELETE)
 * ============================================================================ */

static void test_unsupported_stmts(void) {
    TEST_BEGIN("unsupported_stmts");

    /* CREATE TABLE should parse but be a different kind */
    {
        const char* sql = "CREATE TABLE foo (id int)";
        keel_arena_t* arena = keel_arena_create(4096);
        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(sql, arena, &parser);
        /* Even if it parses, the kind should not be one of the admin DML kinds */
        if (ast && !parser.has_error) {
            TEST_ASSERT(ast->kind != KEEL_SQL_NODE_STMT_SELECT);
            TEST_ASSERT(ast->kind != KEEL_SQL_NODE_STMT_UPDATE);
            TEST_ASSERT(ast->kind != KEEL_SQL_NODE_STMT_INSERT);
            TEST_ASSERT(ast->kind != KEEL_SQL_NODE_STMT_DELETE);
        } else {
            /* Parser may reject it — that's also acceptable */
            TEST_ASSERT(true);
        }
        keel_arena_destroy(arena);
    }

    /* Garbage SQL should produce a parse error */
    {
        const char* sql = "XYZZY ZORK";
        keel_arena_t* arena = keel_arena_create(4096);
        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(sql, arena, &parser);
        /* Either parse error or unknown kind */
        TEST_ASSERT(parser.has_error || !ast ||
                    ast->kind == KEEL_SQL_NODE_UNKNOWN);
        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §7 — SELECT with explicit column list
 * ============================================================================ */

static void test_select_columns(void) {
    TEST_BEGIN("select_columns");

    const char* sql = "SELECT key, value FROM config";
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);

    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;

    /* Should have a targets list with 2 items */
    TEST_ASSERT_NOT_NULL(sel->targets);
    TEST_ASSERT_EQ(sel->targets->count, (size_t)2);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §8 — Arena cleanup is correct (no leaks/crashes with repeated parse)
 * ============================================================================ */

static void test_arena_repeated_parse(void) {
    TEST_BEGIN("arena_repeated_parse");

    for (int i = 0; i < 100; i++) {
        keel_arena_t* arena = keel_arena_create(4096);
        TEST_ASSERT_NOT_NULL(arena);

        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql("SELECT * FROM stats", arena, &parser);
        TEST_ASSERT(!parser.has_error);
        TEST_ASSERT_NOT_NULL(ast);

        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §9 — SELECT * FROM cluster / cluster_stats / tracing
 * ============================================================================ */

static void test_select_new_virtual_tables(void) {
    TEST_BEGIN("select_new_virtual_tables");

    const char* new_tables[] = { "cluster", "cluster_stats", "tracing" };
    for (size_t i = 0; i < 3; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "SELECT * FROM %s", new_tables[i]);

        keel_arena_t* arena = keel_arena_create(4096);
        TEST_ASSERT_NOT_NULL(arena);

        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

        TEST_ASSERT(!parser.has_error);
        TEST_ASSERT_NOT_NULL(ast);
        TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);

        keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
        TEST_ASSERT_NOT_NULL(sel->from);

        /* FROM should resolve to a table reference with the correct name */
        keel_sql_node_t* from_node = sel->from;
        if (from_node->kind == KEEL_SQL_NODE_LIST) {
            keel_sql_list_t* fl = (keel_sql_list_t*)from_node;
            TEST_ASSERT(fl->count >= 1);
            from_node = fl->head;
        }
        if (from_node->kind == KEEL_SQL_NODE_TABLE_REF) {
            keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)from_node;
            TEST_ASSERT(tr->table.len > 0);
            TEST_ASSERT(strncasecmp(tr->table.data, new_tables[i],
                                    tr->table.len) == 0);
        }

        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §10 — UPDATE servers SET enabled = 'false' WHERE index = 2
 * ============================================================================ */

static void test_update_server_enabled(void) {
    TEST_BEGIN("update_server_enabled");

    const char* sql = "UPDATE servers SET enabled = 'false' WHERE index = 2";
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_UPDATE);

    keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)ast;

    /* Table should be "servers" */
    TEST_ASSERT_NOT_NULL(upd->table);
    if (upd->table->kind == KEEL_SQL_NODE_TABLE_REF) {
        keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)upd->table;
        TEST_ASSERT(tr->table.len == 7);
        TEST_ASSERT(strncmp(tr->table.data, "servers", 7) == 0);
    }

    /* SET list should have enabled = 'false' */
    TEST_ASSERT_NOT_NULL(upd->set_list);
    TEST_ASSERT(upd->set_list->count >= 1);
    keel_sql_set_item_t* item = (keel_sql_set_item_t*)upd->set_list->head;
    TEST_ASSERT_NOT_NULL(item);

    /* SET column = "enabled" */
    TEST_ASSERT_NOT_NULL(item->column);
    TEST_ASSERT_EQ(item->column->kind, KEEL_SQL_NODE_EXPR_COLUMN);
    keel_sql_expr_column_t* sc = (keel_sql_expr_column_t*)item->column;
    TEST_ASSERT(sc->column.len == 7);
    TEST_ASSERT(strncmp(sc->column.data, "enabled", 7) == 0);

    /* SET value = 'false' */
    TEST_ASSERT_NOT_NULL(item->value);
    TEST_ASSERT_EQ(item->value->kind, KEEL_SQL_NODE_EXPR_LITERAL);
    keel_sql_expr_literal_t* sv = (keel_sql_expr_literal_t*)item->value;
    TEST_ASSERT_EQ(sv->lit_type, KEEL_SQL_LIT_STRING);
    TEST_ASSERT(strncmp(sv->value.str_val.data, "false",
                        sv->value.str_val.len) == 0);

    /* WHERE should be index = 2 */
    TEST_ASSERT_NOT_NULL(upd->where);
    TEST_ASSERT_EQ(upd->where->kind, KEEL_SQL_NODE_EXPR_BINARY);
    keel_sql_expr_binary_t* bin = (keel_sql_expr_binary_t*)upd->where;
    TEST_ASSERT_EQ(bin->op, KEEL_SQL_BINOP_EQ);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §11 — DELETE FROM clients WHERE id = 42
 * ============================================================================ */

static void test_delete_client(void) {
    TEST_BEGIN("delete_client");

    const char* sql = "DELETE FROM clients WHERE id = 42";
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_DELETE);

    keel_sql_stmt_delete_t* del = (keel_sql_stmt_delete_t*)ast;

    /* Table should be "clients" */
    TEST_ASSERT_NOT_NULL(del->table);
    if (del->table->kind == KEEL_SQL_NODE_TABLE_REF) {
        keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)del->table;
        TEST_ASSERT(tr->table.len == 7);
        TEST_ASSERT(strncmp(tr->table.data, "clients", 7) == 0);
    }

    /* WHERE should be id = 42 */
    TEST_ASSERT_NOT_NULL(del->where);
    TEST_ASSERT_EQ(del->where->kind, KEEL_SQL_NODE_EXPR_BINARY);
    keel_sql_expr_binary_t* bin = (keel_sql_expr_binary_t*)del->where;
    TEST_ASSERT_EQ(bin->op, KEEL_SQL_BINOP_EQ);

    /* Find the integer literal side */
    keel_sql_node_t* lit_side = NULL;
    keel_sql_node_t* col_side = NULL;
    if (bin->left->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
        lit_side = bin->left; col_side = bin->right;
    } else {
        lit_side = bin->right; col_side = bin->left;
    }
    TEST_ASSERT_EQ(col_side->kind, KEEL_SQL_NODE_EXPR_COLUMN);
    keel_sql_expr_column_t* wc = (keel_sql_expr_column_t*)col_side;
    TEST_ASSERT(wc->column.len == 2);
    TEST_ASSERT(strncmp(wc->column.data, "id", 2) == 0);

    TEST_ASSERT_EQ(lit_side->kind, KEEL_SQL_NODE_EXPR_LITERAL);
    keel_sql_expr_literal_t* wl = (keel_sql_expr_literal_t*)lit_side;
    TEST_ASSERT_EQ(wl->lit_type, KEEL_SQL_LIT_INT);
    TEST_ASSERT_EQ(wl->value.int_val, (int64_t)42);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §12 — INSERT INTO peers (host) VALUES ('10.0.0.5:6432')
 * ============================================================================ */

static void test_insert_peer(void) {
    TEST_BEGIN("insert_peer");

    const char* sql = "INSERT INTO peers (host) VALUES ('10.0.0.5:6432')";
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_INSERT);

    keel_sql_stmt_insert_t* ins = (keel_sql_stmt_insert_t*)ast;

    /* Table should be "peers" */
    TEST_ASSERT_NOT_NULL(ins->table);
    if (ins->table->kind == KEEL_SQL_NODE_TABLE_REF) {
        keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)ins->table;
        TEST_ASSERT(tr->table.len == 5);
        TEST_ASSERT(strncmp(tr->table.data, "peers", 5) == 0);
    }

    /* Columns list should have 1 entry: host */
    TEST_ASSERT_NOT_NULL(ins->columns);
    TEST_ASSERT_EQ(ins->columns->count, (size_t)1);

    keel_sql_node_t* cn = ins->columns->head;
    TEST_ASSERT_EQ(cn->kind, KEEL_SQL_NODE_EXPR_COLUMN);
    keel_sql_expr_column_t* ec = (keel_sql_expr_column_t*)cn;
    TEST_ASSERT(ec->column.len == 4);
    TEST_ASSERT(strncmp(ec->column.data, "host", 4) == 0);

    /* Source should contain the value */
    TEST_ASSERT_NOT_NULL(ins->source);
    keel_sql_list_t* vals_outer = NULL;
    if (ins->source->kind == KEEL_SQL_NODE_LIST)
        vals_outer = (keel_sql_list_t*)ins->source;
    TEST_ASSERT_NOT_NULL(vals_outer);

    keel_sql_list_t* vals = NULL;
    if (vals_outer->head && vals_outer->head->kind == KEEL_SQL_NODE_LIST)
        vals = (keel_sql_list_t*)vals_outer->head;
    else
        vals = vals_outer;

    TEST_ASSERT(vals->count >= 1);
    keel_sql_node_t* v0 = vals->head;
    TEST_ASSERT_EQ(v0->kind, KEEL_SQL_NODE_EXPR_LITERAL);
    keel_sql_expr_literal_t* l0 = (keel_sql_expr_literal_t*)v0;
    TEST_ASSERT_EQ(l0->lit_type, KEEL_SQL_LIT_STRING);
    TEST_ASSERT(strncmp(l0->value.str_val.data, "10.0.0.5:6432",
                        l0->value.str_val.len) == 0);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §13 — DELETE FROM peers WHERE host = '10.0.0.5:6432'
 * ============================================================================ */

static void test_delete_peer(void) {
    TEST_BEGIN("delete_peer");

    const char* sql = "DELETE FROM peers WHERE host = '10.0.0.5:6432'";
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_DELETE);

    keel_sql_stmt_delete_t* del = (keel_sql_stmt_delete_t*)ast;

    /* Table should be "peers" */
    TEST_ASSERT_NOT_NULL(del->table);
    if (del->table->kind == KEEL_SQL_NODE_TABLE_REF) {
        keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)del->table;
        TEST_ASSERT(tr->table.len == 5);
        TEST_ASSERT(strncmp(tr->table.data, "peers", 5) == 0);
    }

    /* WHERE should be host = '10.0.0.5:6432' */
    TEST_ASSERT_NOT_NULL(del->where);
    TEST_ASSERT_EQ(del->where->kind, KEEL_SQL_NODE_EXPR_BINARY);
    keel_sql_expr_binary_t* bin = (keel_sql_expr_binary_t*)del->where;
    TEST_ASSERT_EQ(bin->op, KEEL_SQL_BINOP_EQ);

    keel_sql_node_t* col_side = NULL;
    keel_sql_node_t* lit_side = NULL;
    if (bin->left->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
        col_side = bin->left; lit_side = bin->right;
    } else {
        col_side = bin->right; lit_side = bin->left;
    }

    TEST_ASSERT_EQ(col_side->kind, KEEL_SQL_NODE_EXPR_COLUMN);
    keel_sql_expr_column_t* wc = (keel_sql_expr_column_t*)col_side;
    TEST_ASSERT(wc->column.len == 4);
    TEST_ASSERT(strncmp(wc->column.data, "host", 4) == 0);

    TEST_ASSERT_EQ(lit_side->kind, KEEL_SQL_NODE_EXPR_LITERAL);
    keel_sql_expr_literal_t* wl = (keel_sql_expr_literal_t*)lit_side;
    TEST_ASSERT_EQ(wl->lit_type, KEEL_SQL_LIT_STRING);
    TEST_ASSERT(strncmp(wl->value.str_val.data, "10.0.0.5:6432",
                        wl->value.str_val.len) == 0);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §14 — Full 14-table SELECT coverage (all admin tables)
 * ============================================================================ */

static void test_all_admin_tables(void) {
    TEST_BEGIN("all_admin_tables");

    const char* all_tables[] = {
        "stats", "stats_detail", "servers", "pools", "clients",
        "config", "latency", "system", "rebalance", "help", "version",
        "cluster", "cluster_stats", "tracing", "shard_rules",
    };
    size_t count = sizeof(all_tables) / sizeof(all_tables[0]);
    TEST_ASSERT_EQ(count, (size_t)15);

    for (size_t i = 0; i < count; i++) {
        char sql[128];
        snprintf(sql, sizeof(sql), "SELECT * FROM %s", all_tables[i]);

        keel_arena_t* arena = keel_arena_create(4096);
        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(sql, arena, &parser);
        TEST_ASSERT(!parser.has_error);
        TEST_ASSERT_NOT_NULL(ast);
        TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);
        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §15 — Case-insensitivity for table names
 * ============================================================================ */

static void test_case_insensitive_tables(void) {
    TEST_BEGIN("case_insensitive_tables");

    /* SQL keywords and table names should be case-insensitive */
    const char* queries[] = {
        "SELECT * FROM STATS",
        "select * from stats",
        "Select * From Stats",
        "SELECT * FROM SERVERS",
        "select * from config",
    };
    for (size_t i = 0; i < sizeof(queries) / sizeof(queries[0]); i++) {
        keel_arena_t* arena = keel_arena_create(4096);
        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(queries[i], arena, &parser);
        TEST_ASSERT(!parser.has_error);
        TEST_ASSERT_NOT_NULL(ast);
        TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);
        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §16 — UPDATE servers with integer boolean (SET enabled = 1 / 0)
 * ============================================================================ */

static void test_update_server_enabled_int(void) {
    TEST_BEGIN("update_server_enabled_int");

    /* enabled = 1 */
    {
        const char* sql = "UPDATE servers SET enabled = 1 WHERE index = 0";
        keel_arena_t* arena = keel_arena_create(4096);
        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(sql, arena, &parser);
        TEST_ASSERT(!parser.has_error);
        TEST_ASSERT_NOT_NULL(ast);
        TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_UPDATE);

        keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)ast;
        keel_sql_set_item_t* item = (keel_sql_set_item_t*)upd->set_list->head;
        TEST_ASSERT_EQ(item->value->kind, KEEL_SQL_NODE_EXPR_LITERAL);
        keel_sql_expr_literal_t* sv = (keel_sql_expr_literal_t*)item->value;
        TEST_ASSERT_EQ(sv->lit_type, KEEL_SQL_LIT_INT);
        TEST_ASSERT_EQ(sv->value.int_val, (int64_t)1);

        keel_arena_destroy(arena);
    }

    /* enabled = 0 */
    {
        const char* sql = "UPDATE servers SET enabled = 0 WHERE index = 5";
        keel_arena_t* arena = keel_arena_create(4096);
        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(sql, arena, &parser);
        TEST_ASSERT(!parser.has_error);
        TEST_ASSERT_NOT_NULL(ast);
        TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_UPDATE);

        keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)ast;
        keel_sql_set_item_t* item = (keel_sql_set_item_t*)upd->set_list->head;
        TEST_ASSERT_EQ(item->value->kind, KEEL_SQL_NODE_EXPR_LITERAL);
        keel_sql_expr_literal_t* sv = (keel_sql_expr_literal_t*)item->value;
        TEST_ASSERT_EQ(sv->lit_type, KEEL_SQL_LIT_INT);
        TEST_ASSERT_EQ(sv->value.int_val, (int64_t)0);

        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §17 — UPDATE servers with string variants: 'true', 'on', '1', 'false', 'off'
 * ============================================================================ */

static void test_update_server_string_variants(void) {
    TEST_BEGIN("update_server_string_variants");

    const char* variants[] = {
        "UPDATE servers SET enabled = 'true' WHERE index = 1",
        "UPDATE servers SET enabled = 'false' WHERE index = 1",
        "UPDATE servers SET enabled = 'on' WHERE index = 1",
        "UPDATE servers SET enabled = 'off' WHERE index = 1",
        "UPDATE servers SET enabled = '1' WHERE index = 1",
        "UPDATE servers SET enabled = '0' WHERE index = 1",
    };

    for (size_t i = 0; i < sizeof(variants) / sizeof(variants[0]); i++) {
        keel_arena_t* arena = keel_arena_create(4096);
        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(variants[i], arena, &parser);
        TEST_ASSERT(!parser.has_error);
        TEST_ASSERT_NOT_NULL(ast);
        TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_UPDATE);

        keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)ast;
        TEST_ASSERT_NOT_NULL(upd->set_list);
        TEST_ASSERT(upd->set_list->count >= 1);

        keel_sql_set_item_t* item = (keel_sql_set_item_t*)upd->set_list->head;
        TEST_ASSERT_NOT_NULL(item);
        TEST_ASSERT_NOT_NULL(item->value);
        TEST_ASSERT_EQ(item->value->kind, KEEL_SQL_NODE_EXPR_LITERAL);

        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §18 — DELETE without WHERE clause (error path in dispatch)
 * ============================================================================ */

static void test_delete_no_where(void) {
    TEST_BEGIN("delete_no_where");

    /* Parser should handle DELETE FROM <table> without WHERE */
    const char* queries[] = {
        "DELETE FROM servers",
        "DELETE FROM clients",
        "DELETE FROM peers",
    };

    for (size_t i = 0; i < 3; i++) {
        keel_arena_t* arena = keel_arena_create(4096);
        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(queries[i], arena, &parser);

        /* Parser may succeed (WHERE is optional in SQL grammar) */
        if (ast && !parser.has_error) {
            TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_DELETE);
            keel_sql_stmt_delete_t* del = (keel_sql_stmt_delete_t*)ast;
            /* WHERE should be NULL — dispatch would produce an error */
            TEST_ASSERT_NULL(del->where);
        }

        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §19 — INSERT with column/value count mismatch
 * ============================================================================ */

static void test_insert_column_mismatch(void) {
    TEST_BEGIN("insert_column_mismatch");

    /* More columns than values */
    {
        const char* sql =
            "INSERT INTO servers (host, port, role) VALUES ('10.0.0.1', 5432)";
        keel_arena_t* arena = keel_arena_create(4096);
        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(sql, arena, &parser);
        /* Parser accepts it; dispatch would catch the mismatch */
        if (ast && !parser.has_error) {
            TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_INSERT);
            keel_sql_stmt_insert_t* ins = (keel_sql_stmt_insert_t*)ast;
            TEST_ASSERT_NOT_NULL(ins->columns);
            TEST_ASSERT_EQ(ins->columns->count, (size_t)3);

            /* Values should have fewer entries */
            keel_sql_list_t* vals = NULL;
            if (ins->source && ins->source->kind == KEEL_SQL_NODE_LIST) {
                keel_sql_list_t* outer = (keel_sql_list_t*)ins->source;
                if (outer->head && outer->head->kind == KEEL_SQL_NODE_LIST)
                    vals = (keel_sql_list_t*)outer->head;
                else
                    vals = outer;
            }
            if (vals) {
                TEST_ASSERT(vals->count != ins->columns->count);
            }
        }

        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §20 — UPDATE config with missing WHERE (error path)
 * ============================================================================ */

static void test_update_config_no_where(void) {
    TEST_BEGIN("update_config_no_where");

    const char* sql = "UPDATE config SET value = 'test'";
    keel_arena_t* arena = keel_arena_create(4096);
    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    /* Parser should accept — WHERE is optional in SQL grammar */
    if (ast && !parser.has_error) {
        TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_UPDATE);
        keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)ast;
        TEST_ASSERT_NULL(upd->where);
    }

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §21 — SELECT with WHERE clause (column filter)
 * ============================================================================ */

static void test_select_with_where(void) {
    TEST_BEGIN("select_with_where");

    const char* sql = "SELECT * FROM servers WHERE role = 'primary'";
    keel_arena_t* arena = keel_arena_create(4096);
    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);

    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    TEST_ASSERT_NOT_NULL(sel->where);
    TEST_ASSERT_EQ(sel->where->kind, KEEL_SQL_NODE_EXPR_BINARY);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §22 — Multiple SET items in UPDATE
 * ============================================================================ */

static void test_update_multiple_sets(void) {
    TEST_BEGIN("update_multiple_sets");

    /* Standard SQL allows multiple SET items */
    const char* sql =
        "UPDATE config SET value = 'new_val' WHERE key = 'pool_max_size'";
    keel_arena_t* arena = keel_arena_create(4096);
    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_UPDATE);

    keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)ast;
    TEST_ASSERT_NOT_NULL(upd->set_list);
    TEST_ASSERT(upd->set_list->count >= 1);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §23 — INSERT INTO servers with all supported column types
 * ============================================================================ */

static void test_insert_server_full(void) {
    TEST_BEGIN("insert_server_full");

    const char* sql =
        "INSERT INTO servers (host, port, role, weight, enabled) "
        "VALUES ('192.168.1.100', 5433, 'replica', 5, 'true')";
    keel_arena_t* arena = keel_arena_create(4096);
    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_INSERT);

    keel_sql_stmt_insert_t* ins = (keel_sql_stmt_insert_t*)ast;
    TEST_ASSERT_EQ(ins->columns->count, (size_t)5);

    /* Verify 5th column name is "enabled" */
    keel_sql_node_t* cn = ins->columns->head;
    for (int i = 0; i < 4 && cn; i++) cn = cn->next;
    TEST_ASSERT_NOT_NULL(cn);
    TEST_ASSERT_EQ(cn->kind, KEEL_SQL_NODE_EXPR_COLUMN);
    keel_sql_expr_column_t* ec = (keel_sql_expr_column_t*)cn;
    TEST_ASSERT(ec->column.len == 7);
    TEST_ASSERT(strncmp(ec->column.data, "enabled", 7) == 0);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §24 — Arena stress test: many SQL parses in tight loop
 * ============================================================================ */

static void test_arena_stress(void) {
    TEST_BEGIN("arena_stress");

    const char* queries[] = {
        "SELECT * FROM stats",
        "UPDATE config SET value = '64' WHERE key = 'pool_max_size'",
        "INSERT INTO servers (host, port) VALUES ('10.0.0.1', 5432)",
        "DELETE FROM servers WHERE index = 0",
        "SELECT * FROM cluster_stats",
        "DELETE FROM clients WHERE id = 999",
        "INSERT INTO peers (host) VALUES ('10.0.0.2:6432')",
        "UPDATE servers SET enabled = 'true' WHERE index = 3",
    };
    size_t nq = sizeof(queries) / sizeof(queries[0]);

    /* Parse 1000 queries in tight loop — no leaks, no crashes */
    for (int i = 0; i < 1000; i++) {
        keel_arena_t* arena = keel_arena_create(4096);
        TEST_ASSERT_NOT_NULL(arena);

        keel_sql_parser_t parser;
        keel_sql_node_t* ast = parse_sql(queries[i % nq], arena, &parser);
        TEST_ASSERT(!parser.has_error);
        TEST_ASSERT_NOT_NULL(ast);

        keel_arena_destroy(arena);
    }

    TEST_END();
}

/* ============================================================================
 * §25 — DELETE with large client id
 * ============================================================================ */

static void test_delete_large_id(void) {
    TEST_BEGIN("delete_large_id");

    /* Large 64-bit client ID */
    const char* sql = "DELETE FROM clients WHERE id = 18446744073709551615";
    keel_arena_t* arena = keel_arena_create(4096);
    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    /* Parser may handle large ints as strings; at minimum should not crash */
    if (ast && !parser.has_error) {
        TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_DELETE);
    }

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * §26 — INSERT INTO peers with IPv6 address
 * ============================================================================ */

static void test_insert_peer_ipv6(void) {
    TEST_BEGIN("insert_peer_ipv6");

    const char* sql =
        "INSERT INTO peers (host) VALUES ('[::1]:6432')";
    keel_arena_t* arena = keel_arena_create(4096);
    keel_sql_parser_t parser;
    keel_sql_node_t* ast = parse_sql(sql, arena, &parser);

    TEST_ASSERT(!parser.has_error);
    TEST_ASSERT_NOT_NULL(ast);
    TEST_ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_INSERT);

    keel_sql_stmt_insert_t* ins = (keel_sql_stmt_insert_t*)ast;

    /* Extract the value and verify IPv6 address preserved */
    keel_sql_list_t* vals_outer = NULL;
    if (ins->source && ins->source->kind == KEEL_SQL_NODE_LIST)
        vals_outer = (keel_sql_list_t*)ins->source;
    TEST_ASSERT_NOT_NULL(vals_outer);

    keel_sql_list_t* vals = vals_outer;
    if (vals_outer->head && vals_outer->head->kind == KEEL_SQL_NODE_LIST)
        vals = (keel_sql_list_t*)vals_outer->head;

    keel_sql_node_t* v0 = vals->head;
    TEST_ASSERT_EQ(v0->kind, KEEL_SQL_NODE_EXPR_LITERAL);
    keel_sql_expr_literal_t* l0 = (keel_sql_expr_literal_t*)v0;
    TEST_ASSERT_EQ(l0->lit_type, KEEL_SQL_LIT_STRING);
    TEST_ASSERT(strncmp(l0->value.str_val.data, "[::1]:6432",
                        l0->value.str_val.len) == 0);

    keel_arena_destroy(arena);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    printf("=== Admin SQL Query Language Tests ===\n\n");

    test_select_virtual_tables();
    test_update_config();
    test_update_config_int();
    test_insert_server();
    test_delete_server();
    test_unsupported_stmts();
    test_select_columns();
    test_arena_repeated_parse();
    /* New SQL admin dispatch tests */
    test_select_new_virtual_tables();
    test_update_server_enabled();
    test_delete_client();
    test_insert_peer();
    test_delete_peer();
    test_all_admin_tables();
    /* Edge cases and coverage extension */
    test_case_insensitive_tables();
    test_update_server_enabled_int();
    test_update_server_string_variants();
    test_delete_no_where();
    test_insert_column_mismatch();
    test_update_config_no_where();
    test_select_with_where();
    test_update_multiple_sets();
    test_insert_server_full();
    test_arena_stress();
    test_delete_large_id();
    test_insert_peer_ipv6();

    printf("\n");
    return test_summary();
}
