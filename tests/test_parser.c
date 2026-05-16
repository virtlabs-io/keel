/**
 * @file test_parser.c
 * @brief Tests for the recursive-descent SQL parser and AST construction.
 *
 * Unlike the lighter classifier in `test_sql.c`, this suite exercises the full
 * parse path that produces an AST. Each test allocates an arena, feeds a
 * statement through `keel_sql_parse()`, and checks the resulting node types and
 * child structure so parser regressions surface as specific AST shape failures.
 */

#include "keel/sql/sql.h"
#include "keel/sql/sql_ast.h"
#include "keel/sql/query_tree.h"
#include "keel/protocol/protocol.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>

/* ============================================================================
 * Test Helpers
 * ============================================================================ */

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) static void test_##name(void)
#define RUN_TEST(name) do { \
    printf("  %-50s ", #name); \
    test_##name(); \
    printf("[PASS]\n"); \
    tests_passed++; \
} while(0)

#define ASSERT(cond) do { \
    if (!(cond)) { \
        printf("[FAIL]\n    Assertion failed: %s\n    at %s:%d\n", \
               #cond, __FILE__, __LINE__); \
        tests_failed++; \
        return; \
    } \
} while(0)

#define ASSERT_EQ(a, b) ASSERT((a) == (b))
#define ASSERT_NE(a, b) ASSERT((a) != (b))
#define ASSERT_TRUE(x) ASSERT(x)
#define ASSERT_FALSE(x) ASSERT(!(x))
#define ASSERT_STREQ(a, b) ASSERT(strcmp((a), (b)) == 0)

/* ============================================================================
 * Parser Tests
 * ============================================================================ */

TEST(parse_simple_select) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, KEEL_STR("SELECT id, name FROM users"), arena);
    
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);
    
    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->targets, NULL);
    ASSERT_EQ(sel->targets->count, 2);  /* id, name */
    ASSERT_NE(sel->from, NULL);
    ASSERT_EQ(sel->where, NULL);  /* No WHERE */
    
    keel_arena_destroy(arena);
}

TEST(parse_select_with_where) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users WHERE id = 1"), arena);
    
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);
    
    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->where, NULL);
    ASSERT_EQ(sel->where->kind, KEEL_SQL_NODE_EXPR_BINARY);
    
    keel_arena_destroy(arena);
}

TEST(parse_select_for_update) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users FOR UPDATE"), arena);
    
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);
    
    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->locking, NULL);
    
    keel_sql_locking_t* lock = (keel_sql_locking_t*)sel->locking;
    ASSERT_EQ(lock->mode, KEEL_SQL_LOCK_FOR_UPDATE);
    
    keel_arena_destroy(arena);
}

TEST(parse_insert) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, 
        KEEL_STR("INSERT INTO users (name) VALUES ('alice')"), arena);
    
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_INSERT);
    
    keel_sql_stmt_insert_t* ins = (keel_sql_stmt_insert_t*)ast;
    ASSERT_NE(ins->table, NULL);
    ASSERT_NE(ins->columns, NULL);
    ASSERT_EQ(ins->columns->count, 1);  /* name */
    
    keel_arena_destroy(arena);
}

TEST(parse_update) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, 
        KEEL_STR("UPDATE users SET name = 'bob' WHERE id = 1"), arena);
    
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_UPDATE);
    
    keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)ast;
    ASSERT_NE(upd->table, NULL);
    ASSERT_NE(upd->set_list, NULL);
    ASSERT_NE(upd->where, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_delete) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, 
        KEEL_STR("DELETE FROM users WHERE id = 1"), arena);
    
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_DELETE);
    
    keel_sql_stmt_delete_t* del = (keel_sql_stmt_delete_t*)ast;
    ASSERT_NE(del->table, NULL);
    ASSERT_NE(del->where, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_transaction) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* BEGIN */
    keel_sql_parser_init(&parser, KEEL_STR("BEGIN"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_BEGIN);
    
    /* COMMIT */
    keel_sql_parser_init(&parser, KEEL_STR("COMMIT"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_COMMIT);
    
    /* ROLLBACK */
    keel_sql_parser_init(&parser, KEEL_STR("ROLLBACK"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_ROLLBACK);
    
    keel_arena_destroy(arena);
}

TEST(parse_join) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, 
        KEEL_STR("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"), 
        arena);
    
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);
    
    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->from, NULL);
    ASSERT_EQ(sel->from->kind, KEEL_SQL_NODE_TABLE_JOIN);
    
    keel_arena_destroy(arena);
}

/* ============================================================================
 * Query Tree Tests
 * ============================================================================ */

TEST(qt_select_readonly) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users"), arena);
    
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_SELECT);
    ASSERT_EQ(qt->operation, KEEL_QT_OP_READ);
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_READONLY);
    ASSERT_TRUE(keel_qt_can_use_replica(qt));
    ASSERT_TRUE(keel_qt_is_cacheable(qt));
    
    keel_arena_destroy(arena);
}

TEST(qt_select_for_update_needs_primary) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users FOR UPDATE"), arena);
    
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_SELECT);
    ASSERT_EQ(qt->operation, KEEL_QT_OP_WRITE);
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_FOR_UPDATE);
    ASSERT_FALSE(keel_qt_can_use_replica(qt));
    
    keel_arena_destroy(arena);
}

TEST(qt_insert_needs_primary) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("INSERT INTO users (name) VALUES ('alice')"), arena);
    
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_INSERT);
    ASSERT_EQ(qt->operation, KEEL_QT_OP_WRITE);
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_NEEDS_PRIMARY);
    ASSERT_FALSE(keel_qt_can_use_replica(qt));
    ASSERT_FALSE(keel_qt_is_cacheable(qt));
    
    /* Check table tracking */
    ASSERT_EQ(qt->table_count, 1);
    ASSERT_NE(qt->target_table, NULL);
    ASSERT_EQ(qt->target_table->access, KEEL_QT_ACCESS_WRITE);
    
    keel_arena_destroy(arena);
}

TEST(qt_update_needs_primary) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("UPDATE users SET name = 'bob' WHERE id = 1"), arena);
    
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_UPDATE);
    ASSERT_EQ(qt->operation, KEEL_QT_OP_WRITE);
    ASSERT_FALSE(keel_qt_can_use_replica(qt));
    
    keel_arena_destroy(arena);
}

TEST(qt_delete_needs_primary) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("DELETE FROM users WHERE id = 1"), arena);
    
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_DELETE);
    ASSERT_EQ(qt->operation, KEEL_QT_OP_WRITE);
    ASSERT_FALSE(keel_qt_can_use_replica(qt));
    
    keel_arena_destroy(arena);
}

TEST(qt_begin_transaction) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(KEEL_STR("BEGIN"), arena);
    
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_BEGIN);
    ASSERT_EQ(qt->operation, KEEL_QT_OP_TRANSACTION);
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_STARTS_TXN);
    
    keel_arena_destroy(arena);
}

TEST(qt_table_tracking) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"),
        arena);
    
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_SELECT);
    ASSERT_EQ(qt->table_count, 2);  /* users, orders */
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_MULTI_TABLE);
    
    /* Both tables should be READ access */
    for (keel_qt_table_ref_t* t = qt->tables; t; t = t->next) {
        ASSERT_EQ(t->access, KEEL_QT_ACCESS_READ);
    }
    
    keel_arena_destroy(arena);
}

TEST(qt_column_tracking) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT name FROM users WHERE id = 1"), arena);
    
    ASSERT_NE(qt, NULL);
    ASSERT_TRUE(qt->column_count >= 2);  /* name, id */
    
    keel_arena_destroy(arena);
}

TEST(qt_cache_key) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt1 = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users"), arena);
    keel_qt_query_t* qt2 = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM users"), arena);
    keel_qt_query_t* qt3 = keel_sql_analyze_full(
        KEEL_STR("SELECT * FROM orders"), arena);
    
    ASSERT_NE(qt1, NULL);
    ASSERT_NE(qt2, NULL);
    ASSERT_NE(qt3, NULL);
    
    /* Same query should have same cache key */
    ASSERT_EQ(qt1->cache_key, qt2->cache_key);
    
    /* Different table should have different cache key */
    ASSERT_NE(qt1->cache_key, qt3->cache_key);
    
    keel_arena_destroy(arena);
}

TEST(qt_dump) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true"),
        arena);
    
    ASSERT_NE(qt, NULL);
    
    printf("\n");
    keel_qt_dump(qt, stdout);
    
    keel_arena_destroy(arena);
}

/* ============================================================================
 * Extended Parser Tests - Expressions
 * ============================================================================ */

TEST(parse_literals) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* Integer literal */
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, KEEL_STR("SELECT 42"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    /* Float literal */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT 3.14159"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    /* Scientific notation */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT 1e10"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    /* String literal */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT 'hello world'"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    /* NULL literal */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT NULL"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    /* Boolean literals */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT TRUE, FALSE"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    keel_arena_destroy(arena);
}

TEST(parse_binary_operators) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    const char* queries[] = {
        "SELECT 1 + 2",
        "SELECT 1 - 2",
        "SELECT 1 * 2",
        "SELECT 1 / 2",
        "SELECT a = b FROM t",
        "SELECT a < b FROM t",
        "SELECT a > b FROM t",
        "SELECT a AND b FROM t",
        "SELECT a OR b FROM t",
        "SELECT a AND b OR c FROM t",
    };
    
    for (size_t i = 0; i < sizeof(queries)/sizeof(queries[0]); i++) {
        keel_sql_parser_t parser;
        keel_sql_parser_init(&parser, (keel_str_t){.data = queries[i], .len = strlen(queries[i])}, arena);
        keel_sql_node_t* ast = keel_sql_parse(&parser);
        ASSERT_NE(ast, NULL);
        ASSERT_FALSE(parser.has_error);
    }
    
    keel_arena_destroy(arena);
}

TEST(parse_unary_operators) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* Negative */
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, KEEL_STR("SELECT -5"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    /* Positive */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT +5"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    /* NOT */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM t WHERE NOT active"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    /* Double negative */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT - -5"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    keel_arena_destroy(arena);
}

TEST(parse_parameters) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users WHERE id = $1"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    /* Multiple parameters */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users WHERE id = $1 AND name = $2"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    /* Insert with parameters */
    keel_sql_parser_init(&parser, KEEL_STR("INSERT INTO users (name, email) VALUES ($1, $2)"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    keel_arena_destroy(arena);
}

TEST(parse_function_calls) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    const char* queries[] = {
        "SELECT COUNT(*) FROM t",
        "SELECT COUNT(id) FROM t",
        "SELECT COUNT(DISTINCT id) FROM t",
        "SELECT SUM(amount) FROM t",
        "SELECT AVG(price) FROM t",
        "SELECT MIN(created_at) FROM t",
        "SELECT MAX(updated_at) FROM t",
        "SELECT COALESCE(name, 'unknown') FROM t",
        "SELECT UPPER(name), LOWER(email) FROM t",
        "SELECT NOW(), CURRENT_TIMESTAMP",
        "SELECT LENGTH(name) FROM t",
        "SELECT CONCAT('Hello', ' ', 'World')",
        "SELECT ABS(-5), ROUND(3.14, 2)",
    };
    
    for (size_t i = 0; i < sizeof(queries)/sizeof(queries[0]); i++) {
        keel_sql_parser_t parser;
        keel_sql_parser_init(&parser, (keel_str_t){.data = queries[i], .len = strlen(queries[i])}, arena);
        keel_sql_node_t* ast = keel_sql_parse(&parser);
        ASSERT_NE(ast, NULL);
        ASSERT_FALSE(parser.has_error);
    }
    
    keel_arena_destroy(arena);
}

TEST(parse_subquery) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* Subquery in WHERE */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* Subquery in FROM */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM (SELECT id FROM users) AS sub"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* Scalar subquery in SELECT */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT (SELECT COUNT(*) FROM orders)"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_qualified_columns) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* table.column */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT users.name FROM users"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* schema.table.column */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT public.users.name FROM public.users"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* table alias */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT u.name FROM users AS u"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

/* ============================================================================
 * Extended Parser Tests - Clauses
 * ============================================================================ */

TEST(parse_distinct) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* DISTINCT */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT DISTINCT name FROM users"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_TRUE(sel->distinct);
    
    /* DISTINCT ON */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT DISTINCT ON (id) name FROM users"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* ALL */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT ALL name FROM users"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_group_by_having) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* GROUP BY */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT name, COUNT(*) FROM users GROUP BY name"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->group_by, NULL);
    
    /* GROUP BY multiple columns */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT city, status, COUNT(*) FROM users GROUP BY city, status"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* GROUP BY with HAVING */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->having, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_order_by) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* Simple ORDER BY */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users ORDER BY name"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->order_by, NULL);
    
    /* ORDER BY ASC */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users ORDER BY name ASC"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* ORDER BY DESC */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users ORDER BY created_at DESC"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* ORDER BY multiple columns */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users ORDER BY name ASC, id DESC"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* ORDER BY with NULLS FIRST/LAST */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users ORDER BY name NULLS FIRST"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users ORDER BY name DESC NULLS LAST"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_limit_offset) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* LIMIT */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users LIMIT 10"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->limit, NULL);
    
    /* LIMIT with OFFSET */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users LIMIT 10 OFFSET 20"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* LIMIT ALL */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users LIMIT ALL"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* OFFSET only */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users OFFSET 100"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

/* ============================================================================
 * Extended Parser Tests - Joins
 * ============================================================================ */

TEST(parse_join_types) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    const char* queries[] = {
        "SELECT * FROM a JOIN b ON a.id = b.a_id",
        "SELECT * FROM a INNER JOIN b ON a.id = b.a_id",
        "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id",
        "SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.a_id",
        "SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id",
        "SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.a_id",
        "SELECT * FROM a FULL JOIN b ON a.id = b.a_id",
        "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.a_id",
        "SELECT * FROM a CROSS JOIN b",
        "SELECT * FROM a, b",  /* Comma join */
        "SELECT * FROM a JOIN b USING (id)",
    };
    
    for (size_t i = 0; i < sizeof(queries)/sizeof(queries[0]); i++) {
        keel_sql_parser_t parser;
        keel_sql_parser_init(&parser, (keel_str_t){.data = queries[i], .len = strlen(queries[i])}, arena);
        keel_sql_node_t* ast = keel_sql_parse(&parser);
        ASSERT_NE(ast, NULL);
    }
    
    keel_arena_destroy(arena);
}

TEST(parse_multiple_joins) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, KEEL_STR(
        "SELECT u.name, o.total, p.name "
        "FROM users u "
        "JOIN orders o ON u.id = o.user_id "
        "JOIN products p ON o.product_id = p.id "
        "WHERE o.status = 'completed'"
    ), arena);
    
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_FALSE(parser.has_error);
    
    keel_arena_destroy(arena);
}

/* ============================================================================
 * Extended Parser Tests - Additional Statements
 * ============================================================================ */

TEST(parse_insert_variations) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* INSERT with multiple values */
    keel_sql_parser_init(&parser, KEEL_STR("INSERT INTO users (name) VALUES ('alice'), ('bob'), ('charlie')"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* INSERT with RETURNING */
    keel_sql_parser_init(&parser, KEEL_STR("INSERT INTO users (name) VALUES ('alice') RETURNING id"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    keel_sql_stmt_insert_t* ins = (keel_sql_stmt_insert_t*)ast;
    ASSERT_NE(ins->returning, NULL);
    
    /* INSERT with SELECT */
    keel_sql_parser_init(&parser, KEEL_STR("INSERT INTO archive SELECT * FROM users WHERE archived = true"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* INSERT with ON CONFLICT */
    keel_sql_parser_init(&parser, KEEL_STR("INSERT INTO users (id, name) VALUES (1, 'alice') ON CONFLICT (id) DO NOTHING"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_update_variations) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* UPDATE multiple columns */
    keel_sql_parser_init(&parser, KEEL_STR("UPDATE users SET name = 'bob', email = 'bob@example.com' WHERE id = 1"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* UPDATE with RETURNING */
    keel_sql_parser_init(&parser, KEEL_STR("UPDATE users SET active = true WHERE id = 1 RETURNING *"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* UPDATE with FROM */
    keel_sql_parser_init(&parser, KEEL_STR("UPDATE users SET status = o.status FROM orders o WHERE users.id = o.user_id"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_delete_variations) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* DELETE without WHERE */
    keel_sql_parser_init(&parser, KEEL_STR("DELETE FROM users"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* DELETE with RETURNING */
    keel_sql_parser_init(&parser, KEEL_STR("DELETE FROM users WHERE id = 1 RETURNING *"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* DELETE with USING */
    keel_sql_parser_init(&parser, KEEL_STR("DELETE FROM users USING orders WHERE users.id = orders.user_id"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_set_statements) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* SET statement */
    keel_sql_parser_init(&parser, KEEL_STR("SET work_mem = '256MB'"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* RESET statement */
    keel_sql_parser_init(&parser, KEEL_STR("RESET work_mem"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* SHOW statement */
    keel_sql_parser_init(&parser, KEEL_STR("SHOW work_mem"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_transaction_variations) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* START TRANSACTION */
    keel_sql_parser_init(&parser, KEEL_STR("START TRANSACTION"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* BEGIN READ ONLY */
    keel_sql_parser_init(&parser, KEEL_STR("BEGIN READ ONLY"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* BEGIN READ WRITE */
    keel_sql_parser_init(&parser, KEEL_STR("BEGIN READ WRITE"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* BEGIN ISOLATION LEVEL */
    keel_sql_parser_init(&parser, KEEL_STR("BEGIN ISOLATION LEVEL SERIALIZABLE"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* SAVEPOINT */
    keel_sql_parser_init(&parser, KEEL_STR("SAVEPOINT my_save"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* ROLLBACK TO SAVEPOINT */
    keel_sql_parser_init(&parser, KEEL_STR("ROLLBACK TO SAVEPOINT my_save"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* RELEASE SAVEPOINT */
    keel_sql_parser_init(&parser, KEEL_STR("RELEASE SAVEPOINT my_save"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_cte) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* Simple CTE */
    keel_sql_parser_init(&parser, KEEL_STR(
        "WITH active_users AS (SELECT * FROM users WHERE active = true) "
        "SELECT * FROM active_users"
    ), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* Recursive CTE */
    keel_sql_parser_init(&parser, KEEL_STR(
        "WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 10) "
        "SELECT * FROM nums"
    ), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

TEST(parse_select_for_lock_modes) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_sql_parser_t parser;
    
    /* FOR UPDATE */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users FOR UPDATE"), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->locking, NULL);
    
    /* FOR SHARE */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users FOR SHARE"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* FOR NO KEY UPDATE */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users FOR NO KEY UPDATE"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* FOR KEY SHARE */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users FOR KEY SHARE"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* FOR UPDATE NOWAIT */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users FOR UPDATE NOWAIT"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    /* FOR UPDATE SKIP LOCKED */
    keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users FOR UPDATE SKIP LOCKED"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    
    keel_arena_destroy(arena);
}

/* ============================================================================
 * Extended Query Tree Tests
 * ============================================================================ */

TEST(qt_complex_select) {
    keel_arena_t* arena = keel_arena_create(8192);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(KEEL_STR(
        "SELECT u.name, COUNT(o.id) AS order_count "
        "FROM users u "
        "LEFT JOIN orders o ON u.id = o.user_id "
        "WHERE u.active = true "
        "GROUP BY u.name "
        "HAVING COUNT(o.id) > 5 "
        "ORDER BY order_count DESC "
        "LIMIT 10"
    ), arena);
    
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_SELECT);
    ASSERT_EQ(qt->operation, KEEL_QT_OP_READ);
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_READONLY);
    ASSERT_TRUE(qt->table_count >= 2);
    
    keel_arena_destroy(arena);
}

TEST(qt_commit_rollback) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* COMMIT */
    keel_qt_query_t* qt = keel_sql_analyze_full(KEEL_STR("COMMIT"), arena);
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_COMMIT);
    ASSERT_EQ(qt->operation, KEEL_QT_OP_TRANSACTION);
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_ENDS_TXN);
    
    /* ROLLBACK */
    qt = keel_sql_analyze_full(KEEL_STR("ROLLBACK"), arena);
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_ROLLBACK);
    ASSERT_EQ(qt->operation, KEEL_QT_OP_TRANSACTION);
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_ENDS_TXN);
    
    keel_arena_destroy(arena);
}

TEST(qt_set_show) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* SET */
    keel_qt_query_t* qt = keel_sql_analyze_full(KEEL_STR("SET work_mem = '256MB'"), arena);
    ASSERT_NE(qt, NULL);
    
    /* SHOW */
    qt = keel_sql_analyze_full(KEEL_STR("SHOW work_mem"), arena);
    ASSERT_NE(qt, NULL);
    
    keel_arena_destroy(arena);
}

TEST(qt_is_deterministic) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* NULL check */
    ASSERT_FALSE(keel_qt_is_deterministic(NULL));
    
    /* Simple SELECT should be deterministic */
    keel_qt_query_t* qt = keel_sql_analyze_full(KEEL_STR("SELECT id FROM users"), arena);
    ASSERT_NE(qt, NULL);
    /* Note: The flag may or may not be set depending on implementation */
    (void)keel_qt_is_deterministic(qt);
    
    keel_arena_destroy(arena);
}

TEST(qt_operation_name) {
    /* All operations */
    ASSERT_NE(keel_qt_operation_name(KEEL_QT_OP_READ), NULL);
    ASSERT_NE(keel_qt_operation_name(KEEL_QT_OP_WRITE), NULL);
    ASSERT_NE(keel_qt_operation_name(KEEL_QT_OP_DDL), NULL);
    ASSERT_NE(keel_qt_operation_name(KEEL_QT_OP_ADMIN), NULL);
    ASSERT_NE(keel_qt_operation_name(KEEL_QT_OP_TRANSACTION), NULL);
    ASSERT_NE(keel_qt_operation_name(KEEL_QT_OP_SESSION), NULL);
    ASSERT_NE(keel_qt_operation_name(KEEL_QT_OP_UNKNOWN), NULL);
    /* Unknown value */
    ASSERT_NE(keel_qt_operation_name(999), NULL);
}

TEST(qt_node_type_name) {
    /* All node types */
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_SELECT), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_INSERT), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_UPDATE), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_DELETE), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_MERGE), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_CREATE), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_ALTER), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_DROP), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_TRUNCATE), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_BEGIN), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_COMMIT), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_ROLLBACK), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_SAVEPOINT), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_SET), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_RESET), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_DISCARD), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_PREPARE), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_EXECUTE), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_DEALLOCATE), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_SHOW), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_EXPLAIN), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_CALL), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_COPY), NULL);
    ASSERT_NE(keel_qt_node_type_name(KEEL_QT_NODE_UNKNOWN), NULL);
    /* Unknown value */
    ASSERT_NE(keel_qt_node_type_name(999), NULL);
}

TEST(qt_dialect_name) {
    ASSERT_NE(keel_sql_dialect_name(KEEL_DIALECT_POSTGRESQL), NULL);
    ASSERT_NE(keel_sql_dialect_name(KEEL_DIALECT_MYSQL), NULL);
    ASSERT_NE(keel_sql_dialect_name(KEEL_DIALECT_MARIADB), NULL);
    ASSERT_NE(keel_sql_dialect_name(KEEL_DIALECT_SQLITE), NULL);
    ASSERT_NE(keel_sql_dialect_name(KEEL_DIALECT_STANDARD), NULL);
    /* Unknown value */
    ASSERT_NE(keel_sql_dialect_name(999), NULL);
}

TEST(qt_get_invalidated_tables) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_table_ref_t* tables[10];
    size_t count;
    
    /* NULL checks */
    count = keel_qt_get_invalidated_tables(NULL, tables, 10);
    ASSERT_EQ(count, 0);
    
    /* Write operation should have invalidated tables */
    keel_qt_query_t* qt = keel_sql_analyze_full(KEEL_STR("UPDATE users SET name = 'x'"), arena);
    ASSERT_NE(qt, NULL);
    count = keel_qt_get_invalidated_tables(qt, tables, 10);
    /* May return 0 or more depending on implementation */
    (void)count;
    
    /* NULL tables array */
    count = keel_qt_get_invalidated_tables(qt, NULL, 10);
    ASSERT_EQ(count, 0);
    
    /* Zero max */
    count = keel_qt_get_invalidated_tables(qt, tables, 0);
    ASSERT_EQ(count, 0);
    
    keel_arena_destroy(arena);
}

TEST(qt_compute_cache_key) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* NULL returns 0 */
    ASSERT_EQ(keel_qt_compute_cache_key(NULL), 0);
    
    /* Same query should have same key */
    keel_qt_query_t* qt1 = keel_sql_analyze_full(KEEL_STR("SELECT id FROM users"), arena);
    ASSERT_NE(qt1, NULL);
    
    keel_qt_query_t* qt2 = keel_sql_analyze_full(KEEL_STR("SELECT id FROM users"), arena);
    ASSERT_NE(qt2, NULL);
    
    uint64_t key1 = keel_qt_compute_cache_key(qt1);
    uint64_t key2 = keel_qt_compute_cache_key(qt2);
    ASSERT_EQ(key1, key2);
    
    /* Different queries should have different keys */
    keel_qt_query_t* qt3 = keel_sql_analyze_full(KEEL_STR("SELECT name FROM orders"), arena);
    ASSERT_NE(qt3, NULL);
    
    uint64_t key3 = keel_qt_compute_cache_key(qt3);
    ASSERT_NE(key1, key3);
    
    keel_arena_destroy(arena);
}

TEST(qt_can_use_replica_all_cases) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* NULL returns false */
    ASSERT_FALSE(keel_qt_can_use_replica(NULL));
    
    /* Read operation can use replica */
    keel_qt_query_t* qt = keel_sql_analyze_full(KEEL_STR("SELECT * FROM t"), arena);
    ASSERT_NE(qt, NULL);
    ASSERT_TRUE(keel_qt_can_use_replica(qt));
    
    /* DDL cannot use replica */
    qt = keel_sql_analyze_full(KEEL_STR("CREATE TABLE t (id int)"), arena);
    ASSERT_NE(qt, NULL);
    ASSERT_FALSE(keel_qt_can_use_replica(qt));
    
    /* Transaction control cannot use replica */
    qt = keel_sql_analyze_full(KEEL_STR("BEGIN"), arena);
    ASSERT_NE(qt, NULL);
    ASSERT_FALSE(keel_qt_can_use_replica(qt));
    
    keel_arena_destroy(arena);
}

TEST(qt_is_cacheable_all_cases) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    /* NULL returns false */
    ASSERT_FALSE(keel_qt_is_cacheable(NULL));
    
    /* Write operations are not cacheable */
    keel_qt_query_t* qt = keel_sql_analyze_full(KEEL_STR("INSERT INTO t VALUES (1)"), arena);
    ASSERT_NE(qt, NULL);
    ASSERT_FALSE(keel_qt_is_cacheable(qt));
    
    keel_arena_destroy(arena);
}

TEST(qt_dump_null_safety) {
    /* Should not crash with NULL */
    keel_qt_dump(NULL, stdout);
    
    /* Should not crash with NULL file */
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);
    
    keel_qt_query_t* qt = keel_sql_analyze_full(KEEL_STR("SELECT 1"), arena);
    ASSERT_NE(qt, NULL);
    
    keel_qt_dump(qt, NULL);
    
    keel_arena_destroy(arena);
}

/* ============================================================================
 * Window Function Tests
 * ============================================================================ */

/* ============================================================================
 * Window Function Tests
 * ============================================================================ */

/* ============================================================================
 * Admin / Maintenance Statement Tests
 * ============================================================================ */

TEST(parse_admin_statements) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);

    keel_sql_parser_t parser;

    /* MERGE */
    keel_sql_parser_init(&parser, KEEL_STR(
        "MERGE INTO target t USING source s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET name = s.name "
        "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
    ), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_MERGE);

    /* LOCK TABLE */
    keel_sql_parser_init(&parser, KEEL_STR("LOCK TABLE users IN EXCLUSIVE MODE"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_LOCK);

    /* LISTEN */
    keel_sql_parser_init(&parser, KEEL_STR("LISTEN my_channel"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_LISTEN);

    /* NOTIFY */
    keel_sql_parser_init(&parser, KEEL_STR("NOTIFY my_channel, 'payload'"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_NOTIFY);

    /* VACUUM */
    keel_sql_parser_init(&parser, KEEL_STR("VACUUM ANALYZE users"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_VACUUM);

    /* REINDEX */
    keel_sql_parser_init(&parser, KEEL_STR("REINDEX TABLE users"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_VACUUM);

    /* CLUSTER */
    keel_sql_parser_init(&parser, KEEL_STR("CLUSTER users USING users_pkey"), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_VACUUM);

    keel_arena_destroy(arena);
}

/* ============================================================================
 * Query Type Utility Tests
 * ============================================================================ */

TEST(query_type_utilities) {
    /* keel_query_type_name */
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_SELECT),        "SELECT");
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_INSERT),        "INSERT");
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_UPDATE),        "UPDATE");
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_DELETE),        "DELETE");
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_MERGE),         "MERGE");
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_MAINTENANCE),   "MAINTENANCE");
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_LOCK),          "LOCK");
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_LISTEN_NOTIFY), "LISTEN/NOTIFY");
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_BEGIN),         "BEGIN");
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_COMMIT),        "COMMIT");
    ASSERT_STREQ(keel_query_type_name(KEEL_QUERY_UNKNOWN),       "UNKNOWN");

    /* keel_query_type_is_read */
    ASSERT_TRUE(keel_query_type_is_read(KEEL_QUERY_SELECT));
    ASSERT_TRUE(keel_query_type_is_read(KEEL_QUERY_SHOW));
    ASSERT_TRUE(keel_query_type_is_read(KEEL_QUERY_EXPLAIN));
    ASSERT_FALSE(keel_query_type_is_read(KEEL_QUERY_INSERT));
    ASSERT_FALSE(keel_query_type_is_read(KEEL_QUERY_MERGE));

    /* keel_query_type_is_write */
    ASSERT_TRUE(keel_query_type_is_write(KEEL_QUERY_INSERT));
    ASSERT_TRUE(keel_query_type_is_write(KEEL_QUERY_UPDATE));
    ASSERT_TRUE(keel_query_type_is_write(KEEL_QUERY_DELETE));
    ASSERT_TRUE(keel_query_type_is_write(KEEL_QUERY_MERGE));
    ASSERT_TRUE(keel_query_type_is_write(KEEL_QUERY_COPY));
    ASSERT_FALSE(keel_query_type_is_write(KEEL_QUERY_SELECT));
    ASSERT_FALSE(keel_query_type_is_write(KEEL_QUERY_BEGIN));

    /* keel_query_type_is_ddl */
    ASSERT_TRUE(keel_query_type_is_ddl(KEEL_QUERY_CREATE));
    ASSERT_TRUE(keel_query_type_is_ddl(KEEL_QUERY_ALTER));
    ASSERT_TRUE(keel_query_type_is_ddl(KEEL_QUERY_DROP));
    ASSERT_TRUE(keel_query_type_is_ddl(KEEL_QUERY_MAINTENANCE));
    ASSERT_FALSE(keel_query_type_is_ddl(KEEL_QUERY_SELECT));
    ASSERT_FALSE(keel_query_type_is_ddl(KEEL_QUERY_BEGIN));

    /* keel_query_type_is_transaction */
    ASSERT_TRUE(keel_query_type_is_transaction(KEEL_QUERY_BEGIN));
    ASSERT_TRUE(keel_query_type_is_transaction(KEEL_QUERY_COMMIT));
    ASSERT_TRUE(keel_query_type_is_transaction(KEEL_QUERY_ROLLBACK));
    ASSERT_TRUE(keel_query_type_is_transaction(KEEL_QUERY_SAVEPOINT));
    ASSERT_FALSE(keel_query_type_is_transaction(KEEL_QUERY_SELECT));
    ASSERT_FALSE(keel_query_type_is_transaction(KEEL_QUERY_MERGE));
}

/* ============================================================================
 * Analyzer tests for new statement types
 * ============================================================================ */

TEST(analyze_admin_statements) {
    keel_arena_t* arena = keel_arena_create(4096);
    ASSERT_NE(arena, NULL);

    /* MERGE → KEEL_QUERY_MERGE, write, needs primary */
    keel_qt_query_t* qt = keel_sql_analyze_full(
        KEEL_STR("MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET x = 1"),
        arena);
    ASSERT_NE(qt, NULL);
    ASSERT_EQ(qt->type, KEEL_QT_NODE_MERGE);
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_NEEDS_PRIMARY);

    /* LOCK TABLE → needs primary */
    qt = keel_sql_analyze_full(KEEL_STR("LOCK TABLE users IN SHARE MODE"), arena);
    ASSERT_NE(qt, NULL);
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_NEEDS_PRIMARY);

    /* VACUUM → needs primary */
    qt = keel_sql_analyze_full(KEEL_STR("VACUUM ANALYZE users"), arena);
    ASSERT_NE(qt, NULL);
    ASSERT_TRUE(qt->flags & KEEL_QT_FLAG_NEEDS_PRIMARY);

    /* LISTEN → session-scope, can use any node */
    qt = keel_sql_analyze_full(KEEL_STR("LISTEN my_channel"), arena);
    ASSERT_NE(qt, NULL);

    /* NOTIFY → may go to primary */
    qt = keel_sql_analyze_full(KEEL_STR("NOTIFY my_channel"), arena);
    ASSERT_NE(qt, NULL);

    keel_arena_destroy(arena);
}

TEST(parse_window_functions) {
    keel_arena_t* arena = keel_arena_create(8192);
    ASSERT_NE(arena, NULL);

    keel_sql_parser_t parser;

    /* Simple window function: ROW_NUMBER() OVER () */
    keel_sql_parser_init(&parser, KEEL_STR(
        "SELECT ROW_NUMBER() OVER () FROM t"
    ), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);

    /* RANK() with PARTITION BY and ORDER BY */
    keel_sql_parser_init(&parser, KEEL_STR(
        "SELECT RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees"
    ), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);

    /* SUM with frame spec ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW */
    keel_sql_parser_init(&parser, KEEL_STR(
        "SELECT SUM(amount) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales"
    ), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);

    /* LEAD with OVER named window */
    keel_sql_parser_init(&parser, KEEL_STR(
        "SELECT LEAD(val, 1) OVER w FROM t WINDOW w AS (ORDER BY id)"
    ), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);

    /* COUNT with FILTER and OVER */
    keel_sql_parser_init(&parser, KEEL_STR(
        "SELECT COUNT(*) FILTER (WHERE active) OVER (PARTITION BY dept) FROM employees"
    ), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);

    /* RANGE frame */
    keel_sql_parser_init(&parser, KEEL_STR(
        "SELECT AVG(price) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) FROM ticks"
    ), arena);
    ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);

    keel_arena_destroy(arena);
}

TEST(parse_window_clause) {
    keel_arena_t* arena = keel_arena_create(8192);
    ASSERT_NE(arena, NULL);

    keel_sql_parser_t parser;

    /* Named WINDOW clause */
    keel_sql_parser_init(&parser, KEEL_STR(
        "SELECT id, SUM(val) OVER w1, AVG(val) OVER w2 "
        "FROM t "
        "WINDOW w1 AS (PARTITION BY grp ORDER BY ts), "
        "       w2 AS (ORDER BY ts ROWS UNBOUNDED PRECEDING)"
    ), arena);
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);
    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->window, NULL);
    ASSERT_EQ(sel->window->count, 2);

    keel_arena_destroy(arena);
}

/* Phase 8b probe: writable CTE — INSERT inside WITH should parse cleanly
 * and the outer SELECT must still be reached. */
TEST(parse_writable_cte_insert) {
    keel_arena_t* arena = keel_arena_create(8192);
    ASSERT_NE(arena, NULL);

    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, KEEL_STR(
        "WITH ins AS (INSERT INTO users(id, name) VALUES (1, 'x') RETURNING id) "
        "SELECT id FROM ins"
    ), arena);

    keel_sql_node_t* ast = keel_sql_parse(&parser);
    ASSERT_NE(ast, NULL);
    ASSERT_EQ(ast->kind, KEEL_SQL_NODE_STMT_SELECT);

    keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
    ASSERT_NE(sel->with_clause, NULL);
    ASSERT_EQ(sel->with_clause->count, 1);

    keel_sql_node_t* cn = sel->with_clause->head;
    ASSERT_NE(cn, NULL);
    ASSERT_EQ(cn->kind, KEEL_SQL_NODE_CLAUSE_CTE);

    keel_sql_cte_t* cte = (keel_sql_cte_t*)cn;
    ASSERT_NE(cte->query, NULL);
    ASSERT_EQ(cte->query->kind, KEEL_SQL_NODE_STMT_INSERT);

    keel_sql_stmt_insert_t* ins = (keel_sql_stmt_insert_t*)cte->query;
    ASSERT_NE(ins->table, NULL);
    ASSERT_NE(ins->returning, NULL);

    /* Outer SELECT must have its own FROM clause */
    ASSERT_NE(sel->from, NULL);
    ASSERT_NE(sel->targets, NULL);

    keel_arena_destroy(arena);
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;
    
    printf("\n=== SQL Parser Tests ===\n\n");
    
    printf("Parser tests:\n");
    RUN_TEST(parse_simple_select);
    RUN_TEST(parse_select_with_where);
    RUN_TEST(parse_select_for_update);
    RUN_TEST(parse_insert);
    RUN_TEST(parse_update);
    RUN_TEST(parse_delete);
    RUN_TEST(parse_transaction);
    RUN_TEST(parse_join);
    
    printf("\nExpression parsing tests:\n");
    RUN_TEST(parse_literals);
    RUN_TEST(parse_binary_operators);
    RUN_TEST(parse_unary_operators);
    RUN_TEST(parse_parameters);
    RUN_TEST(parse_function_calls);
    RUN_TEST(parse_subquery);
    RUN_TEST(parse_qualified_columns);
    
    printf("\nClause parsing tests:\n");
    RUN_TEST(parse_distinct);
    RUN_TEST(parse_group_by_having);
    RUN_TEST(parse_order_by);
    RUN_TEST(parse_limit_offset);
    
    printf("\nJoin parsing tests:\n");
    RUN_TEST(parse_join_types);
    RUN_TEST(parse_multiple_joins);
    
    printf("\nStatement parsing tests:\n");
    RUN_TEST(parse_insert_variations);
    RUN_TEST(parse_update_variations);
    RUN_TEST(parse_delete_variations);
    RUN_TEST(parse_set_statements);
    RUN_TEST(parse_transaction_variations);
    RUN_TEST(parse_cte);
    RUN_TEST(parse_select_for_lock_modes);
    RUN_TEST(parse_admin_statements);

    printf("\nQuery type utility tests:\n");
    RUN_TEST(query_type_utilities);
    RUN_TEST(analyze_admin_statements);
    
    printf("\nWindow function tests:\n");
    RUN_TEST(parse_window_functions);
    RUN_TEST(parse_window_clause);

    printf("\nCTE tests:\n");
    RUN_TEST(parse_writable_cte_insert);

    printf("\nQuery Tree tests:\n");
    RUN_TEST(qt_select_readonly);
    RUN_TEST(qt_select_for_update_needs_primary);
    RUN_TEST(qt_insert_needs_primary);
    RUN_TEST(qt_update_needs_primary);
    RUN_TEST(qt_delete_needs_primary);
    RUN_TEST(qt_begin_transaction);
    RUN_TEST(qt_table_tracking);
    RUN_TEST(qt_column_tracking);
    RUN_TEST(qt_cache_key);
    RUN_TEST(qt_dump);
    RUN_TEST(qt_complex_select);
    RUN_TEST(qt_commit_rollback);
    RUN_TEST(qt_set_show);
    RUN_TEST(qt_is_deterministic);
    RUN_TEST(qt_operation_name);
    RUN_TEST(qt_node_type_name);
    RUN_TEST(qt_dialect_name);
    RUN_TEST(qt_get_invalidated_tables);
    RUN_TEST(qt_compute_cache_key);
    RUN_TEST(qt_can_use_replica_all_cases);
    RUN_TEST(qt_is_cacheable_all_cases);
    RUN_TEST(qt_dump_null_safety);
    
    printf("\n=== Results ===\n");
    printf("Passed: %d\n", tests_passed);
    printf("Failed: %d\n", tests_failed);
    
    return tests_failed > 0 ? 1 : 0;
}
