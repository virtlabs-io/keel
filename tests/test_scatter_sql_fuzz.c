/**
 * @file test_scatter_sql_fuzz.c
 * @brief Adversarial / malformed-input fuzz harness for the scatter SQL parser
 *        path (§1.4 Phase 1 — scatter query surface).
 *
 * Feeds hostile, truncated, degenerate, and semantically-tricky SQL strings
 * into the SQL parser with the goal of exercising the code paths that the
 * scatter dispatch layer walks when classifying a query for scatter vs.
 * single-shard routing.  The following attack classes are covered:
 *
 *   1. Scatter-dispatched SELECT queries with aggregate functions.
 *   2. COUNT(DISTINCT col) — must set requires_count_distinct, not crash.
 *   3. Malformed / truncated aggregate expressions.
 *   4. Queries with no shard key that fall through to scatter.
 *   5. Window functions (OVER clause) mixed with aggregates.
 *   6. Arbitrarily large / repeated / NULL inputs.
 *   7. SQL injection patterns embedded inside aggregate arguments.
 *
 * The contract is identical to test_admin_sql_fuzz.c: the parser must never
 * crash, overflow, or trigger a sanitizer finding.  Whether it accepts or
 * rejects input is allowed to vary; only unsafe failure is forbidden.
 *
 * Run under ASAN/UBSAN for every CI cycle and as an AFL++ seed corpus for
 * dedicated fuzzing campaigns.  Binary seeds in tests/fuzz_seeds/ provide
 * AFL++ with interesting starting points for the PG wire protocol layer.
 */

#include "test_utils.h"
#include "keel/sql/sql_ast.h"
#include "keel/mem/mem.h"

#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

/** Parse @p sql and assert it succeeds (no error, non-NULL AST). */
static void assert_valid(const char *sql)
{
    keel_arena_t *arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t p;
    keel_sql_parser_init(&p,
        (keel_str_t){ .data = sql, .len = strlen(sql) }, arena);
    keel_sql_node_t *ast = keel_sql_parse(&p);
    TEST_ASSERT(!p.has_error);
    TEST_ASSERT(ast != NULL);
    keel_arena_destroy(arena);
}

/**
 * Parse @p sql and assert it fails gracefully (has_error or NULL AST).
 * Crashing is the bug we guard against — a well-formed rejection is fine.
 */
static void assert_survives(const char *sql)
{
    keel_arena_t *arena = keel_arena_create(4096);
    TEST_ASSERT_NOT_NULL(arena);

    keel_sql_parser_t p;
    keel_sql_parser_init(&p,
        (keel_str_t){ .data = sql, .len = strlen(sql) }, arena);
    keel_sql_node_t *ast = keel_sql_parse(&p);
    (void)ast;   /* may be NULL — that's acceptable */
    keel_arena_destroy(arena);
}

/** Parse a binary buffer (may not be NUL-terminated). */
static void assert_survives_buf(const char *buf, size_t len)
{
    keel_arena_t *arena = keel_arena_create(4096);
    if (!arena) return;   /* OOM under ASAN — skip rather than crash */

    keel_sql_parser_t p;
    keel_sql_parser_init(&p,
        (keel_str_t){ .data = buf, .len = (uint32_t)len }, arena);
    keel_sql_node_t *ast = keel_sql_parse(&p);
    (void)ast;
    keel_arena_destroy(arena);
}

/* ============================================================================
 * Test: well-formed scatter SELECT queries
 * ============================================================================ */

static void test_valid_scatter_queries(void)
{
    TEST_BEGIN("scatter sql fuzz: well-formed scatter SELECT queries survive");

    /* Plain aggregate — no GROUP BY, typical scatter target */
    assert_valid("SELECT COUNT(*) FROM orders");
    assert_valid("SELECT SUM(amount) FROM orders");
    assert_valid("SELECT MIN(amount), MAX(amount) FROM orders");
    assert_valid("SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders");

    /* GROUP BY + aggregate — scatter fan-out with partial merge */
    assert_valid(
        "SELECT region, COUNT(*), SUM(amount) "
        "FROM orders GROUP BY region");
    assert_survives(
        "SELECT region, AVG(amount) FROM orders GROUP BY region "
        "HAVING COUNT(*) > 5 ORDER BY SUM(amount) DESC LIMIT 10");

    /* Multi-table join with aggregate */
    assert_survives(
        "SELECT o.region, COUNT(o.id) "
        "FROM orders o JOIN customers c ON o.cust_id = c.id "
        "GROUP BY o.region");

    /* Subquery in aggregate argument */
    assert_survives(
        "SELECT COUNT(CASE WHEN amount > 100 THEN 1 END) FROM orders");

    /* COALESCE wrapper on aggregate */
    assert_survives(
        "SELECT COALESCE(SUM(amount), 0) FROM orders WHERE region = 'EU'");

    /* ORDER BY non-aggregate column (valid in PG with GROUP BY) */
    assert_survives(
        "SELECT region, SUM(amount) FROM orders "
        "GROUP BY region ORDER BY region");

    TEST_END();
}

/* ============================================================================
 * Test: COUNT(DISTINCT col) — must not crash, dispatch flag checked elsewhere
 * ============================================================================ */

static void test_count_distinct_variants(void)
{
    TEST_BEGIN("scatter sql fuzz: COUNT(DISTINCT ...) variants survive");

    /* Standard form */
    assert_valid("SELECT COUNT(DISTINCT user_id) FROM orders");

    /* With WHERE */
    assert_survives(
        "SELECT COUNT(DISTINCT user_id) FROM orders WHERE region='EU'");

    /* Mixed with other aggregates */
    assert_survives(
        "SELECT COUNT(DISTINCT user_id), SUM(amount) FROM orders");

    /* In GROUP BY context */
    assert_survives(
        "SELECT region, COUNT(DISTINCT user_id) FROM orders "
        "GROUP BY region");

    /* Nested DISTINCT — parser should reject or survive gracefully */
    assert_survives("SELECT COUNT(DISTINCT DISTINCT user_id) FROM orders");

    /* DISTINCT on expression */
    assert_valid(
        "SELECT COUNT(DISTINCT LOWER(email)) FROM customers");

    /* DISTINCT on multi-col tuple — non-standard but must not crash */
    assert_survives(
        "SELECT COUNT(DISTINCT (a, b)) FROM t");

    /* Empty DISTINCT argument */
    assert_survives("SELECT COUNT(DISTINCT) FROM orders");

    /* DISTINCT with * */
    assert_survives("SELECT COUNT(DISTINCT *) FROM orders");

    TEST_END();
}

/* ============================================================================
 * Test: malformed / truncated aggregate expressions
 * ============================================================================ */

static void test_malformed_aggregates(void)
{
    TEST_BEGIN("scatter sql fuzz: malformed aggregate expressions survive");

    /* Unclosed parenthesis */
    assert_survives("SELECT COUNT( FROM orders");
    assert_survives("SELECT SUM(amount FROM orders");
    assert_survives("SELECT MIN(MAX(amount FROM orders");

    /* Missing argument */
    assert_survives("SELECT SUM() FROM orders");
    assert_survives("SELECT AVG() FROM orders");
    assert_survives("SELECT MIN() FROM orders");
    assert_survives("SELECT MAX() FROM orders");

    /* Unknown aggregate function name */
    assert_survives("SELECT KEEL_INTERNAL_AGG(val) FROM t");
    assert_survives("SELECT PERCENTILE(val, 0.95) FROM t");

    /* Aggregate without FROM */
    assert_survives("SELECT COUNT(*)");
    assert_survives("SELECT SUM(1)");

    /* Chained aggregates (invalid in standard SQL) */
    assert_survives("SELECT SUM(COUNT(*)) FROM orders");
    assert_survives("SELECT MAX(MIN(val)) FROM t");

    /* Keyword as column name inside aggregate */
    assert_survives("SELECT SUM(SELECT) FROM t");
    assert_survives("SELECT COUNT(FROM) FROM t");
    assert_survives("SELECT AVG(GROUP) FROM t");

    /* Aggregate with FILTER clause (PostgreSQL extension) */
    assert_survives(
        "SELECT COUNT(*) FILTER (WHERE amount > 100) FROM orders");
    assert_survives(
        "SELECT COUNT(*) FILTER FROM orders");  /* malformed FILTER */
    assert_survives(
        "SELECT COUNT(*) FILTER (amount > 100) FROM orders"); /* missing WHERE */

    /* Giant aggregate column list */
    {
        char buf[4096];
        int n = snprintf(buf, sizeof buf, "SELECT ");
        for (int i = 0; i < 50 && n < (int)sizeof buf - 20; i++) {
            n += snprintf(buf + n, sizeof buf - (size_t)n,
                          "SUM(col%d)%s", i, i < 49 ? "," : "");
        }
        snprintf(buf + n, sizeof buf - (size_t)n, " FROM t");
        assert_survives(buf);
    }

    TEST_END();
}

/* ============================================================================
 * Test: queries with no shard key (fall-through to scatter routing)
 * ============================================================================ */

static void test_no_shard_key_queries(void)
{
    TEST_BEGIN("scatter sql fuzz: queries without shard key survive parser");

    /* Full-table scan — routed as scatter */
    assert_valid("SELECT * FROM orders");
    assert_valid("SELECT id, amount FROM orders LIMIT 1000");
    assert_valid("SELECT * FROM orders WHERE status = 'shipped'");

    /* Cross-join without shard key in filter */
    assert_survives(
        "SELECT a.id, b.amount "
        "FROM customers a, orders b "
        "WHERE a.id = b.cust_id");

    /* Subquery that might reference a shard key */
    assert_survives(
        "SELECT * FROM orders WHERE id IN (SELECT order_id FROM items)");

    /* CTE (WITH clause) */
    assert_survives(
        "WITH regional AS (SELECT region, SUM(amount) AS total FROM orders "
        "GROUP BY region) SELECT * FROM regional WHERE total > 1000");

    /* UNION ALL across different tables — degenerate scatter case */
    assert_survives(
        "SELECT id, amount FROM orders_2023 "
        "UNION ALL "
        "SELECT id, amount FROM orders_2024");

    TEST_END();
}

/* ============================================================================
 * Test: window functions mixed with aggregates
 * ============================================================================ */

static void test_window_function_variants(void)
{
    TEST_BEGIN("scatter sql fuzz: window functions mixed with aggregates");

    /* Basic window function — should set has_window_funcs in dispatch */
    assert_valid(
        "SELECT id, SUM(amount) OVER (PARTITION BY region) FROM orders");
    assert_valid(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY amount DESC) FROM orders");
    assert_valid(
        "SELECT id, RANK() OVER (PARTITION BY region ORDER BY amount) "
        "FROM orders");

    /* Window + scalar aggregate — dispatch must handle both */
    assert_survives(
        "SELECT region, COUNT(*) AS cnt, "
        "RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk "
        "FROM orders GROUP BY region");

    /* Window with frame clause */
    assert_survives(
        "SELECT id, SUM(amount) "
        "OVER (PARTITION BY region ORDER BY ts "
        "ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) "
        "FROM orders");

    /* OVER with empty window — valid in some PGs */
    assert_survives(
        "SELECT id, SUM(amount) OVER () FROM orders");

    /* Malformed OVER clause — must not crash */
    assert_survives("SELECT id, SUM(amount) OVER FROM orders");
    assert_survives("SELECT id, SUM(amount) OVER ( FROM orders");
    assert_survives("SELECT id, SUM(amount) OVER (PARTITION FROM orders");
    assert_survives(
        "SELECT id, SUM(amount) OVER (PARTITION BY ORDER BY) FROM orders");

    /* OVER keyword without function */
    assert_survives("SELECT OVER (ORDER BY id) FROM orders");
    assert_survives("SELECT id OVER (ORDER BY id) FROM orders");

    TEST_END();
}

/* ============================================================================
 * Test: boundary / stress inputs
 * ============================================================================ */

static void test_boundary_inputs(void)
{
    TEST_BEGIN("scatter sql fuzz: boundary and stress inputs survive");

    /* Empty string */
    assert_survives_buf("", 0);
    assert_survives_buf("\0", 1);

    /* Single characters */
    for (unsigned char c = 0; c < 128; c++) {
        char buf[1] = { (char)c };
        assert_survives_buf(buf, 1);
    }

    /* Very long table name */
    {
        char buf[8192];
        memset(buf, 'x', sizeof buf - 1);
        buf[sizeof buf - 1] = '\0';
        char sql[8220];
        snprintf(sql, sizeof sql, "SELECT COUNT(*) FROM %s", buf);
        assert_survives(sql);
    }

    /* Deep nesting */
    {
        char buf[4096];
        int n = 0;
        for (int i = 0; i < 200 && n < (int)sizeof buf - 10; i++)
            n += snprintf(buf + n, sizeof buf - (size_t)n, "COUNT(");
        n += snprintf(buf + n, sizeof buf - (size_t)n, "*");
        for (int i = 0; i < 200 && n < (int)sizeof buf - 3; i++)
            n += snprintf(buf + n, sizeof buf - (size_t)n, ")");
        n += snprintf(buf + n, sizeof buf - (size_t)n, " FROM t");
        assert_survives_buf(buf, (size_t)n);
    }

    /* Repeated keyword spam */
    assert_survives(
        "SELECT SELECT SELECT COUNT(*) SELECT FROM orders SELECT");
    assert_survives(
        "GROUP BY GROUP BY GROUP BY region");
    assert_survives(
        "HAVING HAVING HAVING COUNT(*) > 1");

    /* NUL bytes embedded mid-query */
    {
        const char q[] = "SELECT\0COUNT(*) FROM orders";
        assert_survives_buf(q, sizeof q - 1);
    }

    /* All-whitespace */
    assert_survives("                                   ");
    assert_survives("\t\t\t\n\n\n\r\r\r");

    /* Comment-only */
    assert_survives("-- just a comment");
    assert_survives("/* block comment only */");
    assert_survives("/* unclosed block comment");

    /* Extremely long literal string inside aggregate */
    {
        char buf[2048];
        int n = snprintf(buf, sizeof buf, "SELECT SUM(CASE WHEN region='");
        memset(buf + n, 'A', 1000);
        n += 1000;
        snprintf(buf + n, sizeof buf - (size_t)n, "' THEN amount ELSE 0 END) FROM orders");
        assert_survives(buf);
    }

    TEST_END();
}

/* ============================================================================
 * Test: SQL injection patterns inside aggregate arguments
 * ============================================================================ */

static void test_injection_patterns(void)
{
    TEST_BEGIN("scatter sql fuzz: injection patterns in aggregate arguments survive");

    /* Classic tautology inside aggregate */
    assert_survives(
        "SELECT COUNT(*) FROM orders WHERE 1=1 OR 1=1");
    assert_survives(
        "SELECT SUM(amount) FROM orders WHERE ''='' OR ''=''");

    /* UNION injection in subquery */
    assert_survives(
        "SELECT COUNT(*) FROM ("
        "SELECT 1 UNION ALL SELECT 2"
        ") AS x");

    /* Stacked queries */
    assert_survives(
        "SELECT COUNT(*) FROM orders; DROP TABLE orders;");
    assert_survives(
        "SELECT 1; SELECT COUNT(*) FROM orders");

    /* Comment-injection */
    assert_survives(
        "SELECT COUNT(*)--\nFROM orders");
    assert_survives(
        "SELECT /* injected */ COUNT(*) FROM /* */ orders");

    /* Function name substitution */
    assert_survives("SELECT pg_sleep(10) FROM orders");
    assert_survives("SELECT dblink('host=attacker', 'SELECT 1') FROM orders");

    /* Deeply quoted identifier */
    assert_survives(
        "SELECT COUNT(\"a\".\"b\".\"c\".\"d\".\"e\") FROM orders");
    assert_survives(
        "SELECT SUM(\"col with spaces and 'quotes'\") FROM orders");

    /* Dollar-quoting */
    assert_survives(
        "SELECT $tag$some content$tag$ FROM orders");
    assert_survives(
        "SELECT COUNT($1) FROM orders");  /* prepared statement param */

    TEST_END();
}

/* ============================================================================
 * Test: repeated parse cycles — detect state leaks
 * ============================================================================ */

static void test_repeated_cycles(void)
{
    TEST_BEGIN("scatter sql fuzz: repeated parse cycles detect state leaks");

    const char *queries[] = {
        "SELECT COUNT(*) FROM orders",
        "SELECT SUM(amount) FROM orders GROUP BY region",
        "SELECT COUNT(DISTINCT user_id) FROM orders",
        "SELECT * FROM orders",
        "SELECT COUNT(*) FILTER (WHERE amount > 0) FROM orders",
        "SELECT region, AVG(amount) OVER (PARTITION BY region) FROM orders",
        "",
        "SELECT",
        "GARBAGE INPUT !@#$%^&*()",
        NULL,
    };

    for (int cycle = 0; cycle < 3; cycle++) {
        for (int i = 0; queries[i]; i++) {
            assert_survives(queries[i]);
        }
    }

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    keel_mem_init(NULL);

    test_valid_scatter_queries();
    test_count_distinct_variants();
    test_malformed_aggregates();
    test_no_shard_key_queries();
    test_window_function_variants();
    test_boundary_inputs();
    test_injection_patterns();
    test_repeated_cycles();

    keel_mem_shutdown();
    return test_summary();
}
