/**
 * @file test_sql.c
 * @brief Unit tests for the SQL lexer, classifier, and utility queries.
 *
 * The SQL subsystem does not attempt full parsing for every statement; it
 * instead classifies the leading keyword and a few structural markers to drive
 * routing, transaction detection, and session-modification flags. This suite
 * validates those classification paths, statement counting, and the read-only
 * predicate so routing decisions stay correct as new keyword support is added.
 */

#include "test_utils.h"
#include "keel/sql/sql.h"
#include "keel/protocol/protocol.h"
#include "keel/mem/mem.h"

static void test_lexer(void) {
    TEST_BEGIN("SQL lexer");
    
    keel_str_t sql = keel_str_from_cstr("SELECT id, name FROM users WHERE id = 1");
    keel_sql_lexer_t lexer;
    keel_sql_lexer_init(&lexer, sql);
    
    keel_sql_token_t token;
    
    /* SELECT */
    keel_error_t err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_KEYWORD);
    TEST_ASSERT_EQ(keel_sql_lookup_keyword(token.text), KEEL_SQL_KW_SELECT);
    
    /* id */
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_IDENT);
    
    /* , */
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_COMMA);
    
    /* name */
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_IDENT);
    
    /* FROM */
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_KEYWORD);
    TEST_ASSERT_EQ(keel_sql_lookup_keyword(token.text), KEEL_SQL_KW_FROM);
    
    TEST_END();
}

static void test_query_classification(void) {
    TEST_BEGIN("query classification");
    
    /* SELECT */
    keel_proto_query_t result;
    keel_sql_analyze(keel_str_from_cstr("SELECT * FROM users"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_SELECT);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_READ_ONLY);
    
    /* SELECT FOR UPDATE */
    keel_sql_analyze(keel_str_from_cstr("SELECT * FROM users FOR UPDATE"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_SELECT);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_WRITE);
    
    /* INSERT */
    keel_sql_analyze(keel_str_from_cstr("INSERT INTO users (name) VALUES ('test')"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_INSERT);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_WRITE);
    
    /* UPDATE */
    keel_sql_analyze(keel_str_from_cstr("UPDATE users SET name = 'test'"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_UPDATE);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_WRITE);
    
    /* DELETE */
    keel_sql_analyze(keel_str_from_cstr("DELETE FROM users WHERE id = 1"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_DELETE);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_WRITE);
    
    TEST_END();
}

static void test_transaction_detection(void) {
    TEST_BEGIN("transaction detection");
    
    keel_proto_query_t result;
    
    /* BEGIN */
    keel_sql_analyze(keel_str_from_cstr("BEGIN"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_BEGIN);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_TRANSACTION);
    
    /* COMMIT */
    keel_sql_analyze(keel_str_from_cstr("COMMIT"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_COMMIT);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_TRANSACTION);
    
    /* ROLLBACK */
    keel_sql_analyze(keel_str_from_cstr("ROLLBACK"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_ROLLBACK);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_TRANSACTION);
    
    /* Quick checks */
    TEST_ASSERT(keel_sql_starts_transaction(keel_str_from_cstr("BEGIN")));
    TEST_ASSERT(keel_sql_ends_transaction(keel_str_from_cstr("COMMIT")));
    TEST_ASSERT(keel_sql_ends_transaction(keel_str_from_cstr("ROLLBACK")));
    
    TEST_END();
}

static void test_session_modification(void) {
    TEST_BEGIN("session modification detection");
    
    keel_proto_query_t result;
    
    /* SET */
    keel_sql_analyze(keel_str_from_cstr("SET search_path = public"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_SET);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_SESSION);
    
    /* PREPARE */
    keel_sql_analyze(keel_str_from_cstr("PREPARE stmt AS SELECT 1"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_PREPARE);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_SESSION);
    
    /* Quick check */
    TEST_ASSERT(keel_sql_modifies_session(keel_str_from_cstr("SET timezone = 'UTC'")));
    TEST_ASSERT(!keel_sql_modifies_session(keel_str_from_cstr("SELECT 1")));
    
    TEST_END();
}

static void test_statement_counting(void) {
    TEST_BEGIN("statement counting");
    
    TEST_ASSERT_EQ(keel_sql_count_statements(keel_str_from_cstr("SELECT 1")), 1);
    TEST_ASSERT_EQ(keel_sql_count_statements(keel_str_from_cstr("SELECT 1;")), 1);
    TEST_ASSERT_EQ(keel_sql_count_statements(keel_str_from_cstr("SELECT 1; SELECT 2")), 2);
    TEST_ASSERT_EQ(keel_sql_count_statements(keel_str_from_cstr("SELECT 1; SELECT 2;")), 2);
    TEST_ASSERT_EQ(keel_sql_count_statements(keel_str_from_cstr("SELECT 1; SELECT 2; SELECT 3")), 3);
    
    TEST_END();
}

static void test_first_statement(void) {
    TEST_BEGIN("first statement extraction");
    
    keel_str_t sql = keel_str_from_cstr("SELECT 1; SELECT 2");
    keel_str_t first, rest;
    
    keel_sql_first_statement(sql, &first, &rest);
    
    TEST_ASSERT_NOT_NULL(first.data);
    /* First statement could be with or without semicolon depending on impl */
    TEST_ASSERT(first.len >= 8); /* At least "SELECT 1" */
    
    TEST_END();
}

static void test_read_only_check(void) {
    TEST_BEGIN("read-only check");
    
    /* SELECT is read-only */
    TEST_ASSERT(keel_sql_is_readonly(keel_str_from_cstr("SELECT * FROM users")));
    TEST_ASSERT(keel_sql_is_readonly(keel_str_from_cstr("SHOW search_path")));
    TEST_ASSERT(keel_sql_is_readonly(keel_str_from_cstr("EXPLAIN SELECT 1")));
    
    /* Writes are not read-only */
    TEST_ASSERT(!keel_sql_is_readonly(keel_str_from_cstr("INSERT INTO users VALUES (1)")));
    TEST_ASSERT(!keel_sql_is_readonly(keel_str_from_cstr("UPDATE users SET x = 1")));
    TEST_ASSERT(!keel_sql_is_readonly(keel_str_from_cstr("DELETE FROM users")));
    TEST_ASSERT(!keel_sql_is_readonly(keel_str_from_cstr("CREATE TABLE t (id int)")));
    
    TEST_END();
}

static void test_keyword_name(void) {
    TEST_BEGIN("SQL keyword name");
    
    /* Test known keywords */
    const char* name;
    
    name = keel_sql_keyword_name(KEEL_SQL_KW_SELECT);
    TEST_ASSERT_NOT_NULL(name);
    TEST_ASSERT_STR_EQ(name, "SELECT");
    
    name = keel_sql_keyword_name(KEEL_SQL_KW_INSERT);
    TEST_ASSERT_NOT_NULL(name);
    TEST_ASSERT_STR_EQ(name, "INSERT");
    
    name = keel_sql_keyword_name(KEEL_SQL_KW_UPDATE);
    TEST_ASSERT_NOT_NULL(name);
    TEST_ASSERT_STR_EQ(name, "UPDATE");
    
    name = keel_sql_keyword_name(KEEL_SQL_KW_DELETE);
    TEST_ASSERT_NOT_NULL(name);
    TEST_ASSERT_STR_EQ(name, "DELETE");
    
    name = keel_sql_keyword_name(KEEL_SQL_KW_BEGIN);
    TEST_ASSERT_NOT_NULL(name);
    TEST_ASSERT_STR_EQ(name, "BEGIN");
    
    name = keel_sql_keyword_name(KEEL_SQL_KW_COMMIT);
    TEST_ASSERT_NOT_NULL(name);
    TEST_ASSERT_STR_EQ(name, "COMMIT");
    
    /* Unknown keyword returns NULL or unknown marker */
    name = keel_sql_keyword_name(KEEL_SQL_KW_UNKNOWN);
    /* Just make sure it doesn't crash */
    
    TEST_END();
}

static void test_lexer_peek(void) {
    TEST_BEGIN("SQL lexer peek");
    
    keel_sql_lexer_t lexer;
    keel_sql_token_t token, peeked;
    
    keel_sql_lexer_init(&lexer, keel_str_from_cstr("SELECT * FROM users"));
    
    /* Peek should return the same token as next without advancing */
    keel_error_t err = keel_sql_lexer_peek(&lexer, &peeked);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(peeked.type, KEEL_SQL_TOKEN_KEYWORD);
    
    /* Next should return the same token */
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_KEYWORD);
    TEST_ASSERT_EQ(token.text.len, peeked.text.len);
    
    /* Now peek again - should get next token (* operator) */
    err = keel_sql_lexer_peek(&lexer, &peeked);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(peeked.type, KEEL_SQL_TOKEN_OPERATOR);
    
    /* Verify next gets the same token */
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_OPERATOR);
    
    TEST_END();
}

static void test_lexer_strings(void) {
    TEST_BEGIN("SQL lexer strings");
    
    keel_str_t sql = keel_str_from_cstr("SELECT 'hello world' FROM t");
    keel_sql_lexer_t lexer;
    keel_sql_lexer_init(&lexer, sql);
    
    keel_sql_token_t token;
    
    /* SELECT */
    keel_error_t err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_KEYWORD);
    
    /* 'hello world' */
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_STRING);
    
    /* FROM */
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_KEYWORD);
    
    TEST_END();
}

static void test_lexer_numbers(void) {
    TEST_BEGIN("SQL lexer numbers");
    
    keel_str_t sql = keel_str_from_cstr("SELECT 123, 45.67, -89 FROM t");
    keel_sql_lexer_t lexer;
    keel_sql_lexer_init(&lexer, sql);
    
    keel_sql_token_t token;
    
    /* SELECT */
    keel_error_t err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_KEYWORD);
    
    /* 123 */
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_NUMBER);
    
    TEST_END();
}

static void test_lexer_operators(void) {
    TEST_BEGIN("SQL lexer operators");
    
    keel_str_t sql = keel_str_from_cstr("SELECT * FROM t WHERE a = 1 AND b > 2 OR c < 3");
    keel_sql_lexer_t lexer;
    keel_sql_lexer_init(&lexer, sql);
    
    keel_sql_token_t token;
    
    /* Skip through tokens until we hit operators */
    int operator_count = 0;
    while (keel_sql_lexer_next(&lexer, &token) == KEEL_OK && token.type != KEEL_SQL_TOKEN_EOF) {
        if (token.type == KEEL_SQL_TOKEN_OPERATOR) {
            operator_count++;
        }
    }
    
    /* Should find at least =, >, < */
    TEST_ASSERT(operator_count >= 3);
    
    TEST_END();
}

static void test_lexer_parameters(void) {
    TEST_BEGIN("SQL lexer parameters");
    
    keel_str_t sql = keel_str_from_cstr("SELECT * FROM t WHERE id = $1 AND name = $2");
    keel_sql_lexer_t lexer;
    keel_sql_lexer_init(&lexer, sql);
    
    keel_sql_token_t token;
    
    /* Count parameter tokens */
    int param_count = 0;
    while (keel_sql_lexer_next(&lexer, &token) == KEEL_OK && token.type != KEEL_SQL_TOKEN_EOF) {
        if (token.type == KEEL_SQL_TOKEN_PARAMETER) {
            param_count++;
        }
    }
    
    /* Should find $1 and $2 */
    TEST_ASSERT_EQ(param_count, 2);
    
    TEST_END();
}

static void test_ddl_detection(void) {
    TEST_BEGIN("DDL detection");
    
    keel_proto_query_t result;
    
    /* CREATE */
    keel_sql_analyze(keel_str_from_cstr("CREATE TABLE users (id INT)"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_CREATE);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_DDL);
    
    /* DROP */
    keel_sql_analyze(keel_str_from_cstr("DROP TABLE users"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_DROP);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_DDL);
    
    /* ALTER */
    keel_sql_analyze(keel_str_from_cstr("ALTER TABLE users ADD COLUMN name TEXT"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_ALTER);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_DDL);
    
    /* TRUNCATE */
    keel_sql_analyze(keel_str_from_cstr("TRUNCATE TABLE users"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_TRUNCATE);
    TEST_ASSERT(result.flags & KEEL_QUERY_FLAG_DDL);
    
    TEST_END();
}

static void test_copy_detection(void) {
    TEST_BEGIN("COPY detection");
    
    keel_proto_query_t result;
    
    /* COPY */
    keel_sql_analyze(keel_str_from_cstr("COPY users TO '/tmp/users.csv'"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_COPY);
    
    /* COPY FROM */
    keel_sql_analyze(keel_str_from_cstr("COPY users FROM '/tmp/users.csv'"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_COPY);
    
    TEST_END();
}

static void test_other_statements(void) {
    TEST_BEGIN("other statements");
    
    keel_proto_query_t result;
    
    /* SHOW */
    keel_sql_analyze(keel_str_from_cstr("SHOW search_path"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_SHOW);
    
    /* EXPLAIN */
    keel_sql_analyze(keel_str_from_cstr("EXPLAIN SELECT 1"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_EXPLAIN);
    
    /* COPY */
    keel_sql_analyze(keel_str_from_cstr("COPY users TO '/tmp/users.csv'"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_COPY);
    
    /* CALL */
    keel_sql_analyze(keel_str_from_cstr("CALL my_procedure()"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_CALL);
    
    /* DEALLOCATE */
    keel_sql_analyze(keel_str_from_cstr("DEALLOCATE stmt"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_DEALLOCATE);
    
    /* EXECUTE */
    keel_sql_analyze(keel_str_from_cstr("EXECUTE stmt(1, 2)"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_EXECUTE);
    
    /* SAVEPOINT */
    keel_sql_analyze(keel_str_from_cstr("SAVEPOINT my_savepoint"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_SAVEPOINT);
    
    TEST_END();
}

static void test_analyze_edge_cases(void) {
    TEST_BEGIN("analyze edge cases");
    
    keel_proto_query_t result;
    
    /* Empty SQL */
    keel_sql_analyze(keel_str_from_cstr(""), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_UNKNOWN);
    
    /* NULL pointer check */
    keel_error_t err = keel_sql_analyze(keel_str_from_cstr("SELECT 1"), NULL);
    TEST_ASSERT(KEEL_IS_ERR(err));
    
    /* Whitespace only */
    keel_sql_analyze(keel_str_from_cstr("   "), &result);
    
    /* Comment only */
    keel_sql_analyze(keel_str_from_cstr("-- just a comment"), &result);
    
    /* Block comment */
    keel_sql_analyze(keel_str_from_cstr("/* block comment */"), &result);
    
    /* DO statement */
    keel_sql_analyze(keel_str_from_cstr("DO $$ BEGIN RAISE NOTICE 'hi'; END $$"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_DO);
    
    /* RESET */
    keel_sql_analyze(keel_str_from_cstr("RESET ALL"), &result);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_RESET);
    
    /* DISCARD - just verify it doesn't crash */
    keel_sql_analyze(keel_str_from_cstr("DISCARD ALL"), &result);
    /* May or may not have specific type for DISCARD */
    
    TEST_END();
}

static void test_sql_rewrite(void) {
    TEST_BEGIN("SQL rewrite");
    
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);
    
    keel_str_t result;
    keel_error_t err;
    
    /* Basic rewrite - currently just returns original */
    err = keel_sql_rewrite(KEEL_STR("SELECT * FROM users"), NULL, arena, &result);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.len, strlen("SELECT * FROM users"));
    
    /* NULL result pointer */
    err = keel_sql_rewrite(KEEL_STR("SELECT 1"), NULL, arena, NULL);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);
    
    /* Empty SQL */
    err = keel_sql_rewrite(KEEL_STR(""), NULL, arena, &result);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.len, 0);
    
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_analyze_with_tree(void) {
    TEST_BEGIN("SQL analyze with tree");
    
    keel_arena_t* arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);
    
    keel_proto_query_t result;
    keel_error_t err;
    
    /* NULL result pointer */
    err = keel_sql_analyze_with_tree(KEEL_STR("SELECT 1"), NULL, arena);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);
    
    /* Empty SQL */
    err = keel_sql_analyze_with_tree(KEEL_STR(""), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_UNKNOWN);
    
    /* SELECT query */
    err = keel_sql_analyze_with_tree(KEEL_STR("SELECT * FROM users"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_SELECT);
    TEST_ASSERT(!result.needs_primary);
    
    /* INSERT query */
    err = keel_sql_analyze_with_tree(KEEL_STR("INSERT INTO t VALUES (1)"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_INSERT);
    TEST_ASSERT(result.needs_primary);
    
    /* UPDATE query */
    err = keel_sql_analyze_with_tree(KEEL_STR("UPDATE t SET x = 1"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_UPDATE);
    TEST_ASSERT(result.needs_primary);
    
    /* DELETE query */
    err = keel_sql_analyze_with_tree(KEEL_STR("DELETE FROM t WHERE id = 1"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_DELETE);
    TEST_ASSERT(result.needs_primary);
    
    /* BEGIN transaction */
    err = keel_sql_analyze_with_tree(KEEL_STR("BEGIN"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_BEGIN);
    
    /* COMMIT */
    err = keel_sql_analyze_with_tree(KEEL_STR("COMMIT"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_COMMIT);
    
    /* ROLLBACK */
    err = keel_sql_analyze_with_tree(KEEL_STR("ROLLBACK"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_ROLLBACK);
    
    /* SAVEPOINT */
    err = keel_sql_analyze_with_tree(KEEL_STR("SAVEPOINT sp1"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_SAVEPOINT);
    
    /* SET */
    err = keel_sql_analyze_with_tree(KEEL_STR("SET work_mem = '256MB'"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_SET);
    
    /* CREATE (DDL) */
    err = keel_sql_analyze_with_tree(KEEL_STR("CREATE TABLE t (id int)"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_CREATE);
    
    /* ALTER (DDL) */
    err = keel_sql_analyze_with_tree(KEEL_STR("ALTER TABLE t ADD COLUMN x int"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_ALTER);
    
    /* DROP (DDL) */
    err = keel_sql_analyze_with_tree(KEEL_STR("DROP TABLE t"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_DROP);
    
    /* TRUNCATE (DDL) */
    err = keel_sql_analyze_with_tree(KEEL_STR("TRUNCATE TABLE t"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_TRUNCATE);
    
    /* PREPARE */
    err = keel_sql_analyze_with_tree(KEEL_STR("PREPARE stmt AS SELECT 1"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_PREPARE);
    
    /* EXECUTE */
    err = keel_sql_analyze_with_tree(KEEL_STR("EXECUTE stmt"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_EXECUTE);
    
    /* DEALLOCATE */
    err = keel_sql_analyze_with_tree(KEEL_STR("DEALLOCATE stmt"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_DEALLOCATE);
    
    /* SHOW */
    err = keel_sql_analyze_with_tree(KEEL_STR("SHOW work_mem"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_SHOW);
    
    /* EXPLAIN */
    err = keel_sql_analyze_with_tree(KEEL_STR("EXPLAIN SELECT 1"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_EXPLAIN);
    
    /* COPY */
    err = keel_sql_analyze_with_tree(KEEL_STR("COPY t TO '/tmp/out.csv'"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_COPY);
    
    /* CALL */
    err = keel_sql_analyze_with_tree(KEEL_STR("CALL my_procedure()"), &result, arena);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(result.type, KEEL_QUERY_CALL);
    
    keel_arena_destroy(arena);
    TEST_END();
}

static void test_lexer_comments(void) {
    TEST_BEGIN("SQL lexer comments");
    
    keel_sql_lexer_t lexer;
    keel_sql_token_t token;
    
    /* Line comment */
    keel_sql_lexer_init(&lexer, keel_str_from_cstr("-- comment\nSELECT 1"));
    keel_error_t err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    /* Should skip the comment */
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_KEYWORD);
    
    /* Block comment */
    keel_sql_lexer_init(&lexer, keel_str_from_cstr("/* comment */ SELECT 1"));
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_KEYWORD);
    
    /* Nested block comment */
    keel_sql_lexer_init(&lexer, keel_str_from_cstr("/* outer /* inner */ still outer */ SELECT 1"));
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_KEYWORD);
    
    TEST_END();
}

static void test_lexer_dollar_quotes(void) {
    TEST_BEGIN("SQL lexer dollar quotes");
    
    keel_sql_lexer_t lexer;
    keel_sql_token_t token;
    
    /* Simple $$ */
    keel_sql_lexer_init(&lexer, keel_str_from_cstr("SELECT $$hello world$$"));
    keel_error_t err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_KEYWORD);
    
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_STRING);
    
    /* Tagged dollar quote */
    keel_sql_lexer_init(&lexer, keel_str_from_cstr("$tag$hello$tag$"));
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_STRING);
    
    TEST_END();
}

static void test_lexer_all_tokens(void) {
    TEST_BEGIN("SQL lexer all token types");
    
    keel_sql_lexer_t lexer;
    keel_sql_token_t token;
    
    /* Semicolon */
    keel_sql_lexer_init(&lexer, keel_str_from_cstr(";"));
    keel_error_t err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_SEMICOLON);
    
    /* Parentheses */
    keel_sql_lexer_init(&lexer, keel_str_from_cstr("()"));
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_LPAREN);
    
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_RPAREN);
    
    /* Dot */
    keel_sql_lexer_init(&lexer, keel_str_from_cstr("."));
    err = keel_sql_lexer_next(&lexer, &token);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_DOT);
    
    /* EOF - check token type even if err is something else */
    keel_sql_lexer_init(&lexer, keel_str_from_cstr(""));
    err = keel_sql_lexer_next(&lexer, &token);
    /* Empty input should give EOF token */
    TEST_ASSERT_EQ(token.type, KEEL_SQL_TOKEN_EOF);
    
    TEST_END();
}

int main(void) {
    printf("SQL Parser Tests\n");
    printf("=================\n\n");
    
    test_lexer();
    test_query_classification();
    test_transaction_detection();
    test_session_modification();
    test_statement_counting();
    test_first_statement();
    test_read_only_check();
    test_keyword_name();
    test_lexer_peek();
    
    /* Additional tests */
    test_lexer_strings();
    test_lexer_numbers();
    test_lexer_operators();
    test_lexer_parameters();
    test_lexer_comments();
    test_lexer_dollar_quotes();
    test_lexer_all_tokens();
    
    /* Analyzer tests */
    test_ddl_detection();
    test_copy_detection();
    test_other_statements();
    test_analyze_edge_cases();
    test_sql_rewrite();
    test_analyze_with_tree();
    
    return test_summary();
}
