/**
 * @file analyzer.c
 * @brief Fast SQL classifier for routing, stickiness, and session-dirtiness decisions.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This module is the hot-path analysis tier used by workers before they decide
 * where a statement can run and whether it threatens multiplexing safety. It is
 * intentionally shallower than the AST/Query Tree pipeline: most decisions are
 * derived from early keywords and a few targeted scans, with no AST allocation.
 *
 * That design is a deliberate tradeoff. It gives up full syntactic certainty for
 * throughput, but it still handles the high-value exceptions that would be unsafe
 * to miss, such as `SELECT ... FOR UPDATE`, writable CTEs, and multi-statement
 * transaction boundaries.
 */

#include "keel/sql/sql.h"
#include "keel/protocol/protocol.h"
#include <stdio.h>
#include <string.h>
#include "keel/mem/mem.h"

#include <string.h>
#include <ctype.h>

/* ============================================================================
 * Query Analysis
 * ============================================================================ */

/**
 * @brief Classify a statement with minimal work for routing and pooling policy.
 *
 * The algorithm is intentionally front-loaded around the first keyword, then uses
 * narrow follow-up scans only for cases where a naive first-token decision would
 * be wrong or unsafe. This keeps the common point-select path cheap while still
 * catching key semantic escalations.
 *
 * @param sql SQL query text.
 * @param result [out] Receives query type, flags, and primary-routing hints.
 * @return `KEEL_OK` on success.
 */
keel_error_t keel_sql_analyze(keel_str_t sql, keel_proto_query_t* result) {
    if (!result) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    memset(result, 0, sizeof(*result));
    result->sql = sql;
    result->type = KEEL_QUERY_UNKNOWN;
    result->flags = KEEL_QUERY_FLAG_NONE;
    
    if (sql.len == 0 || !sql.data) {
        return KEEL_OK;
    }
    
    keel_sql_lexer_t lexer;
    keel_sql_lexer_init(&lexer, sql);
    
    keel_sql_token_t token;
    keel_error_t err = keel_sql_lexer_next(&lexer, &token);
    
    if (KEEL_IS_ERR(err) || token.type != KEEL_SQL_TOKEN_KEYWORD) {
        return KEEL_OK;
    }
    
    keel_sql_keyword_t kw = keel_sql_lookup_keyword(token.text);
    
    switch (kw) {
    case KEEL_SQL_KW_SELECT:
        result->type = KEEL_QUERY_SELECT;
        result->flags = KEEL_QUERY_FLAG_READ_ONLY;
        
        /* Fast reject nearly all plain SELECTs by doing a raw byte scan for a
         * `FOR` token before paying for a full token walk. The point is not to
         * prove the query is harmless, but to avoid the secondary lex loop unless
         * a locking clause is even plausible. */
        {
            bool might_have_for = false;
            for (size_t _i = 0; _i + 2 < sql.len; _i++) {
                char _c = sql.data[_i];
                if ((_c == 'f' || _c == 'F') &&
                    (sql.data[_i+1] == 'o' || sql.data[_i+1] == 'O') &&
                    (sql.data[_i+2] == 'r' || sql.data[_i+2] == 'R')) {
                    bool left_ok  = (_i == 0) ||
                                    !isalnum((unsigned char)sql.data[_i-1]);
                    bool right_ok = (_i + 3 >= sql.len) ||
                                    !isalnum((unsigned char)sql.data[_i+3]);
                    if (left_ok && right_ok) {
                        might_have_for = true;
                        break;
                    }
                }
            }
            if (might_have_for) {
                /* Only once the cheap scan suggests a candidate do we pay for a
                 * proper token walk to confirm locking semantics. */
                while (keel_sql_lexer_next(&lexer, &token) == KEEL_OK) {
                    if (token.type == KEEL_SQL_TOKEN_KEYWORD) {
                        keel_sql_keyword_t k = keel_sql_lookup_keyword(token.text);
                        if (k == KEEL_SQL_KW_FOR) {
                            err = keel_sql_lexer_next(&lexer, &token);
                            if (err == KEEL_OK && token.type == KEEL_SQL_TOKEN_KEYWORD) {
                                k = keel_sql_lookup_keyword(token.text);
                                if (k == KEEL_SQL_KW_UPDATE || k == KEEL_SQL_KW_WRITE) {
                                    result->flags = KEEL_QUERY_FLAG_WRITE;
                                    result->needs_primary = true;
                                }
                            }
                        }
                    }
                }
            }
        }
        break;
        
    case KEEL_SQL_KW_SHOW:
        result->type = KEEL_QUERY_SHOW;
        result->flags = KEEL_QUERY_FLAG_READ_ONLY;
        break;
        
    case KEEL_SQL_KW_EXPLAIN:
        result->type = KEEL_QUERY_EXPLAIN;
        result->flags = KEEL_QUERY_FLAG_READ_ONLY;
        break;
        
    case KEEL_SQL_KW_INSERT:
        result->type = KEEL_QUERY_INSERT;
        result->flags = KEEL_QUERY_FLAG_WRITE;
        result->needs_primary = true;
        break;
        
    case KEEL_SQL_KW_UPDATE:
        result->type = KEEL_QUERY_UPDATE;
        result->flags = KEEL_QUERY_FLAG_WRITE;
        result->needs_primary = true;
        break;
        
    case KEEL_SQL_KW_DELETE:
        result->type = KEEL_QUERY_DELETE;
        result->flags = KEEL_QUERY_FLAG_WRITE;
        result->needs_primary = true;
        break;
        
    case KEEL_SQL_KW_TRUNCATE:
        result->type = KEEL_QUERY_TRUNCATE;
        result->flags = KEEL_QUERY_FLAG_WRITE | KEEL_QUERY_FLAG_DDL;
        result->needs_primary = true;
        break;
        
    case KEEL_SQL_KW_CREATE:
        result->type = KEEL_QUERY_CREATE;
        result->flags = KEEL_QUERY_FLAG_DDL;
        result->needs_primary = true;
        break;
        
    case KEEL_SQL_KW_ALTER:
        result->type = KEEL_QUERY_ALTER;
        result->flags = KEEL_QUERY_FLAG_DDL;
        result->needs_primary = true;
        break;
        
    case KEEL_SQL_KW_DROP:
        result->type = KEEL_QUERY_DROP;
        result->flags = KEEL_QUERY_FLAG_DDL;
        result->needs_primary = true;
        break;
        
    case KEEL_SQL_KW_BEGIN:
        result->type = KEEL_QUERY_BEGIN;
        result->flags = KEEL_QUERY_FLAG_TRANSACTION;
        break;
        
    case KEEL_SQL_KW_START:
        /* START TRANSACTION */
        err = keel_sql_lexer_next(&lexer, &token);
        if (err == KEEL_OK && token.type == KEEL_SQL_TOKEN_KEYWORD) {
            keel_sql_keyword_t k = keel_sql_lookup_keyword(token.text);
            if (k == KEEL_SQL_KW_TRANSACTION) {
                result->type = KEEL_QUERY_BEGIN;
                result->flags = KEEL_QUERY_FLAG_TRANSACTION;
            }
        }
        break;
        
    case KEEL_SQL_KW_COMMIT:
    case KEEL_SQL_KW_END:
        result->type = KEEL_QUERY_COMMIT;
        result->flags = KEEL_QUERY_FLAG_TRANSACTION;
        break;
        
    case KEEL_SQL_KW_ROLLBACK:
        result->type = KEEL_QUERY_ROLLBACK;
        result->flags = KEEL_QUERY_FLAG_TRANSACTION;
        break;
        
    case KEEL_SQL_KW_SAVEPOINT:
    case KEEL_SQL_KW_RELEASE:
        result->type = KEEL_QUERY_SAVEPOINT;
        result->flags = KEEL_QUERY_FLAG_TRANSACTION;
        break;
        
    case KEEL_SQL_KW_SET:
        result->type = KEEL_QUERY_SET;
        result->flags = KEEL_QUERY_FLAG_SESSION;
        break;
        
    case KEEL_SQL_KW_RESET:
    case KEEL_SQL_KW_DISCARD:
        result->type = KEEL_QUERY_RESET;
        result->flags = KEEL_QUERY_FLAG_SESSION;
        break;
        
    case KEEL_SQL_KW_PREPARE:
        result->type = KEEL_QUERY_PREPARE;
        result->flags = KEEL_QUERY_FLAG_SESSION;
        break;
        
    case KEEL_SQL_KW_EXECUTE:
        result->type = KEEL_QUERY_EXECUTE;
        break;
        
    case KEEL_SQL_KW_DEALLOCATE:
        result->type = KEEL_QUERY_DEALLOCATE;
        result->flags = KEEL_QUERY_FLAG_SESSION;
        break;
        
    case KEEL_SQL_KW_COPY:
        result->type = KEEL_QUERY_COPY;
        result->flags = KEEL_QUERY_FLAG_WRITE;
        result->needs_primary = true;
        break;
        
    case KEEL_SQL_KW_CALL:
        result->type = KEEL_QUERY_CALL;
        result->needs_primary = true; /* Assume writes */
        break;
        
    case KEEL_SQL_KW_DO:
        result->type = KEEL_QUERY_DO;
        result->needs_primary = true;
        break;

    case KEEL_SQL_KW_MERGE:
        result->type = KEEL_QUERY_MERGE;
        result->flags = KEEL_QUERY_FLAG_WRITE;
        result->needs_primary = true;
        break;

    case KEEL_SQL_KW_LOCK:
        result->type = KEEL_QUERY_LOCK;
        result->needs_primary = true;
        break;

    case KEEL_SQL_KW_LISTEN:
    case KEEL_SQL_KW_NOTIFY:
        result->type = KEEL_QUERY_LISTEN_NOTIFY;
        result->flags = KEEL_QUERY_FLAG_SESSION;
        break;
    case KEEL_SQL_KW_UNLISTEN:
        result->type = KEEL_QUERY_UNLISTEN;
        result->flags = KEEL_QUERY_FLAG_SESSION;
        break;

    case KEEL_SQL_KW_VACUUM:
    case KEEL_SQL_KW_REINDEX:
    case KEEL_SQL_KW_CLUSTER:
        result->type = KEEL_QUERY_MAINTENANCE;
        result->flags = KEEL_QUERY_FLAG_DDL;
        result->needs_primary = true;
        break;
        
    case KEEL_SQL_KW_GRANT:
    case KEEL_SQL_KW_REVOKE:
        result->type = KEEL_QUERY_UNKNOWN;
        result->flags = KEEL_QUERY_FLAG_DDL;
        result->needs_primary = true;
        break;
        
    case KEEL_SQL_KW_WITH:
        /* CTEs complicate first-keyword classification because the real semantic
         * operation may appear only after one or more parenthesized definitions. */
        result->type = KEEL_QUERY_SELECT;
        result->flags = KEEL_QUERY_FLAG_READ_ONLY;
        
        /* Scan for the actual query after CTEs, tracking parenthesis depth
         * to skip CTE definitions like: WITH name AS (subquery) */
        {
            int paren_depth = 0;
            bool found_main_keyword = false;
            
            while (keel_sql_lexer_next(&lexer, &token) == KEEL_OK && !found_main_keyword) {
                if (token.type == KEEL_SQL_TOKEN_LPAREN) {
                    paren_depth++;
                } else if (token.type == KEEL_SQL_TOKEN_RPAREN) {
                    paren_depth--;
                } else if (token.type == KEEL_SQL_TOKEN_KEYWORD && paren_depth == 0) {
                    /* Only check for main statement keywords when outside parentheses */
                    keel_sql_keyword_t k = keel_sql_lookup_keyword(token.text);
                    if (k == KEEL_SQL_KW_INSERT) {
                        result->type = KEEL_QUERY_INSERT;
                        result->flags = KEEL_QUERY_FLAG_WRITE;
                        result->needs_primary = true;
                        found_main_keyword = true;
                    } else if (k == KEEL_SQL_KW_UPDATE) {
                        result->type = KEEL_QUERY_UPDATE;
                        result->flags = KEEL_QUERY_FLAG_WRITE;
                        result->needs_primary = true;
                        found_main_keyword = true;
                    } else if (k == KEEL_SQL_KW_DELETE) {
                        result->type = KEEL_QUERY_DELETE;
                        result->flags = KEEL_QUERY_FLAG_WRITE;
                        result->needs_primary = true;
                        found_main_keyword = true;
                    } else if (k == KEEL_SQL_KW_SELECT) {
                        /* SELECT is the default we already set */
                        found_main_keyword = true;
                    }
                }
            }
        }
        break;
        
    default:
        result->type = KEEL_QUERY_UNKNOWN;
        break;
    }
    
    return KEEL_OK;
}

/* ============================================================================
 * Quick Query Checks
 *
 * These wrappers intentionally trade repeated analysis for API simplicity. Call
 * sites that need several answers about the same statement should invoke
 * `keel_sql_analyze()` once and inspect the returned flags directly.
 * ============================================================================ */

/**
 * @brief Check if a query is read-only
 *
 * @param sql  SQL query text
 * @return true if query can be routed to a replica
 */
bool keel_sql_is_readonly(keel_str_t sql) {
    keel_proto_query_t result;
    keel_sql_analyze(sql, &result);
    return (result.flags & KEEL_QUERY_FLAG_READ_ONLY) != 0;
}

/**
 * @brief Check if a query starts a transaction
 *
 * @param sql  SQL query text
 * @return true if this is BEGIN or START TRANSACTION
 */
bool keel_sql_starts_transaction(keel_str_t sql) {
    keel_proto_query_t result;
    keel_sql_analyze(sql, &result);
    return result.type == KEEL_QUERY_BEGIN;
}

/**
 * @brief Check if a query ends a transaction
 *
 * @param sql  SQL query text
 * @return true if this is COMMIT or ROLLBACK
 */
bool keel_sql_ends_transaction(keel_str_t sql) {
    keel_proto_query_t result;
    keel_sql_analyze(sql, &result);
    return result.type == KEEL_QUERY_COMMIT || result.type == KEEL_QUERY_ROLLBACK;
}

/**
 * @brief Check if a query modifies session state
 *
 * Session-modifying queries (SET, PREPARE, etc.) make the
 * connection "dirty" and may require session reset before reuse.
 *
 * @param sql  SQL query text
 * @return true if query modifies session state
 */
bool keel_sql_modifies_session(keel_str_t sql) {
    keel_proto_query_t result;
    keel_sql_analyze(sql, &result);
    return (result.flags & KEEL_QUERY_FLAG_SESSION) != 0;
}

/* ============================================================================
 * Statement Counting
 *
 * Utilities for working with multi-statement queries.
 * PostgreSQL's simple query protocol allows multiple statements
 * separated by semicolons.
 * ============================================================================ */

/**
 * @brief Count the number of SQL statements in a query
 *
 * Counts semicolon-separated statements. Handles:
 *   - Empty statements (;;)
 *   - Trailing semicolon
 *   - No trailing semicolon
 *
 * @code
 * size_t n = keel_sql_count_statements(KEEL_STR("SELECT 1; SELECT 2"));
 * // n == 2
 * @endcode
 *
 * @param sql  SQL text (may contain multiple statements)
 * @return Number of statements (0 for empty input)
 */
size_t keel_sql_count_statements(keel_str_t sql) {
    if (sql.len == 0 || !sql.data) {
        return 0;
    }
    
    keel_sql_lexer_t lexer;
    keel_sql_lexer_init(&lexer, sql);
    
    size_t count = 0;
    bool has_content = false;
    
    keel_sql_token_t token;
    while (keel_sql_lexer_next(&lexer, &token) == KEEL_OK) {
        if (token.type == KEEL_SQL_TOKEN_SEMICOLON) {
            if (has_content) {
                count++;
                has_content = false;
            }
        } else if (token.type != KEEL_SQL_TOKEN_EOF) {
            has_content = true;
        }
    }
    
    /* Count final statement if no trailing semicolon */
    if (has_content) {
        count++;
    }
    
    return count;
}

/**
 * @brief Split first statement from the rest
 *
 * Useful for processing multi-statement queries one at a time.
 *
 * @code
 * keel_str_t stmt, rest;
 * keel_sql_first_statement(sql, &stmt, &rest);
 * // Process stmt...
 * while (rest.len > 0) {
 *     keel_sql_first_statement(rest, &stmt, &rest);
 *     // Process stmt...
 * }
 * @endcode
 *
 * @param sql   Input SQL (may contain multiple statements)
 * @param stmt  [out] First statement (up to and including semicolon)
 * @param rest  [out] Remaining SQL after first statement (may be NULL)
 */
void keel_sql_first_statement(keel_str_t sql, keel_str_t* stmt, keel_str_t* rest) {
    if (!stmt) return;
    
    stmt->data = NULL;
    stmt->len = 0;
    
    if (rest) {
        rest->data = NULL;
        rest->len = 0;
    }
    
    if (sql.len == 0 || !sql.data) {
        return;
    }
    
    keel_sql_lexer_t lexer;
    keel_sql_lexer_init(&lexer, sql);
    
    keel_sql_token_t token;
    size_t stmt_end = 0;
    
    while (keel_sql_lexer_next(&lexer, &token) == KEEL_OK) {
        if (token.type == KEEL_SQL_TOKEN_SEMICOLON) {
            stmt_end = token.offset + 1;
            break;
        }
        stmt_end = token.offset + token.text.len;
    }
    
    if (stmt_end > 0) {
        stmt->data = sql.data;
        stmt->len = stmt_end;
        
        if (rest && stmt_end < sql.len) {
            /* Skip whitespace */
            size_t rest_start = stmt_end;
            while (rest_start < sql.len && isspace((unsigned char)sql.data[rest_start])) {
                rest_start++;
            }
            if (rest_start < sql.len) {
                rest->data = sql.data + rest_start;
                rest->len = sql.len - rest_start;
            }
        }
    }
}

/* ============================================================================
 * Multi-Statement Transaction Scanning
 *
 * Iterates ALL semicolon-delimited statements in a query string and checks
 * each one individually for transaction control keywords.  This catches the
 * critical "SELECT 1; BEGIN;" pattern that single-statement analysis misses.
 * ============================================================================ */

/**
 * @brief Scan all semicolon-delimited statements for a transaction-start keyword.
 *
 * Iterates each statement individually so the pattern "SELECT 1; BEGIN;" is
 * correctly detected even though a single-statement parse of the whole string
 * would classify it as SELECT.
 *
 * @param sql  SQL text (may contain multiple statements).
 * @return true if any statement is classified as KEEL_QUERY_BEGIN.
 */
bool keel_sql_contains_transaction_start(keel_str_t sql)
{
    if (sql.len == 0 || !sql.data) return false;

    keel_str_t remaining = sql;

    while (remaining.len > 0) {
        keel_str_t stmt = {0};
        keel_str_t rest = {0};
        keel_sql_first_statement(remaining, &stmt, &rest);

        if (stmt.len == 0) break;

        /* Analyze this individual statement */
        keel_proto_query_t result;
        keel_sql_analyze(stmt, &result);
        if (result.type == KEEL_QUERY_BEGIN) {
            return true;
        }

        remaining = rest;
    }
    return false;
}

/**
 * @brief Scan all semicolon-delimited statements for a transaction-end keyword.
 *
 * Same multi-statement iteration logic as keel_sql_contains_transaction_start();
 * detects COMMIT or ROLLBACK anywhere in a compound query string.
 *
 * @param sql  SQL text (may contain multiple statements).
 * @return true if any statement is classified as KEEL_QUERY_COMMIT or
 *         KEEL_QUERY_ROLLBACK.
 */
bool keel_sql_contains_transaction_end(keel_str_t sql)
{
    if (sql.len == 0 || !sql.data) return false;

    keel_str_t remaining = sql;

    while (remaining.len > 0) {
        keel_str_t stmt = {0};
        keel_str_t rest = {0};
        keel_sql_first_statement(remaining, &stmt, &rest);

        if (stmt.len == 0) break;

        keel_proto_query_t result;
        keel_sql_analyze(stmt, &result);
        if (result.type == KEEL_QUERY_COMMIT ||
            result.type == KEEL_QUERY_ROLLBACK) {
            return true;
        }

        remaining = rest;
    }
    return false;
}

/* ============================================================================
 * Query Rewriting (stub for now)
 *
 * Placeholder for SQL rewriting functionality. Could be used for:
 *   - Adding query hints
 *   - Injecting search_path
 *   - Adding RETURNING clauses
 *   - Load balancing annotations
 * ============================================================================ */

/**
 * @brief Rewrite a SQL query according to optional policy knobs.
 *
 * Applies zero or more transformations requested by @p opts:
 *
 *  - @c opts->search_path  — prepend `SET search_path TO '<path>'; ` before
 *    the query so the backend uses the specified schema search path for this
 *    statement.
 *  - @c opts->add_statement_timeout — prepend
 *    `SET LOCAL statement_timeout = '<ms>ms'; ` before the query.
 *  - @c opts->add_read_only — prepend
 *    `SET TRANSACTION READ ONLY; ` before the query.
 *
 * When no opts apply the output is the original SQL view (zero-copy fast path).
 * When rewrites are applied the result is allocated from @p arena.
 *
 * @param sql     Original SQL
 * @param opts    Rewrite options (may be NULL → no rewrites)
 * @param arena   Arena for result allocation when a new string is produced
 * @param result  [out] Rewritten SQL (may alias original when unchanged)
 * @return KEEL_OK on success, KEEL_ERR_OVERFLOW when arena is exhausted
 */
keel_error_t keel_sql_rewrite(
    keel_str_t                    sql,
    const keel_sql_rewrite_opts_t* opts,
    keel_arena_t*                 arena,
    keel_str_t*                   result
) {
    if (!result) return KEEL_ERR_INVALID_ARG;

    /* Fast path: nothing to do */
    if (!opts) {
        *result = sql;
        return KEEL_OK;
    }

    /* Build a prefix string from enabled options.
     * Each prefix fragment is at most ~80 bytes; 512 bytes total is safe. */
    char prefix[512];
    size_t prefix_len = 0;

#define PFX_APPEND(s) do { \
    size_t _l = strlen(s); \
    if (prefix_len + _l >= sizeof prefix) goto overflow; \
    memcpy(prefix + prefix_len, s, _l); \
    prefix_len += _l; \
} while (0)

    if (opts->search_path && opts->search_path[0]) {
        /* Inject SET search_path TO 'value'; */
        PFX_APPEND("SET search_path TO '");
        PFX_APPEND(opts->search_path);
        PFX_APPEND("'; ");
    }

    if (opts->add_statement_timeout && opts->statement_timeout > 0) {
        char timeout_buf[64];
        snprintf(timeout_buf, sizeof timeout_buf,
                 "SET LOCAL statement_timeout = '%dms'; ",
                 opts->statement_timeout);
        PFX_APPEND(timeout_buf);
    }

    if (opts->add_read_only) {
        PFX_APPEND("SET TRANSACTION READ ONLY; ");
    }

#undef PFX_APPEND

    if (prefix_len == 0) {
        /* No opts applied */
        *result = sql;
        return KEEL_OK;
    }

    /* Allocate combined string from arena: prefix + original SQL */
    size_t total = prefix_len + sql.len + 1; /* +1 for NUL */
    char* buf = (char*)keel_arena_alloc(arena, total);
    if (!buf) goto overflow;

    memcpy(buf, prefix, prefix_len);
    memcpy(buf + prefix_len, sql.data, sql.len);
    buf[total - 1] = '\0';

    result->data = buf;
    result->len  = total - 1;
    return KEEL_OK;

overflow:
    /* Arena exhausted — return original SQL unchanged rather than failing */
    *result = sql;
    return KEEL_ERR_OVERFLOW;
}

/* ============================================================================
 * Full Query Tree Analysis
 *
 * These functions use the full parser and query tree for detailed analysis.
 * Use keel_sql_analyze() for fast routing decisions, and these functions
 * when you need detailed information about table/column references.
 * ============================================================================ */

#include "keel/sql/sql_ast.h"
#include "keel/sql/query_tree.h"

/**
 * @brief Parse and analyze SQL using full parse tree
 *
 * This performs a full parse of the SQL statement and builds a Query Tree
 * for detailed analysis. Use this when you need:
 *   - Table reference tracking
 *   - Column reference tracking
 *   - Cache key computation
 *   - SQL dialect translation
 *
 * For simple routing decisions (read vs write), use keel_sql_analyze() which
 * is faster but provides less detail.
 *
 * @code
 * keel_arena_t arena;
 * keel_arena_init(&arena, 4096);
 *
 * keel_qt_query_t* qt = keel_sql_analyze_full(sql, &arena);
 * if (qt) {
 *     if (keel_qt_can_use_replica(qt)) {
 *         route_to_replica();
 *     }
 *     
 *     // Check what tables are accessed
 *     for (keel_qt_table_ref_t* t = qt->tables; t; t = t->next) {
 *         printf("Table: %.*s\n", (int)t->table.len, t->table.data);
 *     }
 * }
 *
 * keel_arena_destroy(&arena);
 * @endcode
 *
 * @param sql   SQL text to analyze
 * @param arena Memory arena for allocations
 * @return Query Tree, or NULL on parse error
 */
/**
 * @brief Run the full parse-plus-semantic-lift pipeline.
 */
keel_qt_query_t* keel_sql_analyze_full(keel_str_t sql, keel_arena_t* arena) {
    if (!arena || sql.len == 0 || !sql.data) {
        return NULL;
    }
    
    /* Build syntax first, then lift that syntax into semantic routing/caching
     * facts only if parsing succeeded cleanly enough to be useful. */
    keel_sql_parser_t parser;
    keel_sql_parser_init(&parser, sql, arena);
    
    keel_sql_node_t* ast = keel_sql_parse(&parser);
    if (!ast || parser.has_error) {
        return NULL;
    }
    
    /* Build Query Tree from AST */
    keel_qt_builder_t builder;
    keel_qt_builder_init(&builder, arena);
    
    keel_qt_query_t* qt = keel_qt_build(&builder, ast);
    if (qt) {
        qt->sql = sql;
        qt->cache_key = keel_qt_compute_cache_key(qt);
    }
    
    return qt;
}

/**
 * @brief Analyze SQL and fill proto_query result with Query Tree detail
 *
 * This is a bridge between the old analyzer API and the new Query Tree.
 * It fills in the proto_query result structure like keel_sql_analyze() but
 * uses the full parser for more accurate classification.
 *
 * @param sql    SQL text
 * @param result [out] Analysis result
 * @param arena  Memory arena for parse tree
 * @return KEEL_OK on success, KEEL_ERR_PARSE on parse error
 */
/**
 * @brief Use the deep parser when available, but degrade gracefully to fast classification.
 */
keel_error_t keel_sql_analyze_with_tree(
    keel_str_t           sql,
    keel_proto_query_t*  result,
    keel_arena_t*        arena
) {
    if (!result) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    memset(result, 0, sizeof(*result));
    result->sql = sql;
    result->type = KEEL_QUERY_UNKNOWN;
    result->flags = KEEL_QUERY_FLAG_NONE;
    
    if (sql.len == 0 || !sql.data) {
        return KEEL_OK;
    }
    
    /* Build Query Tree */
    keel_qt_query_t* qt = keel_sql_analyze_full(sql, arena);
    if (!qt) {
        /* Fall back to token-based analysis */
        return keel_sql_analyze(sql, result);
    }
    
    /* Map Query Tree to proto_query result */
    switch (qt->type) {
    case KEEL_QT_NODE_SELECT:
        result->type = KEEL_QUERY_SELECT;
        break;
    case KEEL_QT_NODE_INSERT:
        result->type = KEEL_QUERY_INSERT;
        break;
    case KEEL_QT_NODE_UPDATE:
        result->type = KEEL_QUERY_UPDATE;
        break;
    case KEEL_QT_NODE_DELETE:
        result->type = KEEL_QUERY_DELETE;
        break;
    case KEEL_QT_NODE_BEGIN:
        result->type = KEEL_QUERY_BEGIN;
        break;
    case KEEL_QT_NODE_COMMIT:
        result->type = KEEL_QUERY_COMMIT;
        break;
    case KEEL_QT_NODE_ROLLBACK:
        result->type = KEEL_QUERY_ROLLBACK;
        break;
    case KEEL_QT_NODE_SAVEPOINT:
        result->type = KEEL_QUERY_SAVEPOINT;
        break;
    case KEEL_QT_NODE_SET:
        result->type = KEEL_QUERY_SET;
        break;
    case KEEL_QT_NODE_CREATE:
        result->type = KEEL_QUERY_CREATE;
        break;
    case KEEL_QT_NODE_ALTER:
        result->type = KEEL_QUERY_ALTER;
        break;
    case KEEL_QT_NODE_DROP:
        result->type = KEEL_QUERY_DROP;
        break;
    case KEEL_QT_NODE_TRUNCATE:
        result->type = KEEL_QUERY_TRUNCATE;
        break;
    case KEEL_QT_NODE_PREPARE:
        result->type = KEEL_QUERY_PREPARE;
        break;
    case KEEL_QT_NODE_EXECUTE:
        result->type = KEEL_QUERY_EXECUTE;
        break;
    case KEEL_QT_NODE_DEALLOCATE:
        result->type = KEEL_QUERY_DEALLOCATE;
        break;
    case KEEL_QT_NODE_SHOW:
        result->type = KEEL_QUERY_SHOW;
        break;
    case KEEL_QT_NODE_EXPLAIN:
        result->type = KEEL_QUERY_EXPLAIN;
        break;
    case KEEL_QT_NODE_COPY:
        result->type = KEEL_QUERY_COPY;
        break;
    case KEEL_QT_NODE_CALL:
        result->type = KEEL_QUERY_CALL;
        break;
    default:
        result->type = KEEL_QUERY_UNKNOWN;
        break;
    }
    
    /* Map flags */
    if (qt->flags & KEEL_QT_FLAG_READONLY) {
        result->flags |= KEEL_QUERY_FLAG_READ_ONLY;
    }
    if (qt->operation == KEEL_QT_OP_WRITE) {
        result->flags |= KEEL_QUERY_FLAG_WRITE;
    }
    if (qt->operation == KEEL_QT_OP_DDL) {
        result->flags |= KEEL_QUERY_FLAG_DDL;
    }
    if (qt->operation == KEEL_QT_OP_TRANSACTION) {
        result->flags |= KEEL_QUERY_FLAG_TRANSACTION;
    }
    if (qt->operation == KEEL_QT_OP_SESSION) {
        result->flags |= KEEL_QUERY_FLAG_SESSION;
    }
    if (qt->flags & KEEL_QT_FLAG_CACHEABLE) {
        result->flags |= KEEL_QUERY_FLAG_CACHEABLE;
    }
    
    result->needs_primary = !keel_qt_can_use_replica(qt);
    
    return KEEL_OK;
}

/* ============================================================================
 * Query Type Utility Functions
 * ============================================================================ */

/**
 * @brief Return whether a query-type enum is treated as a read operation.
 */
bool keel_query_type_is_read(keel_query_type_t type) {
    return type == KEEL_QUERY_SELECT ||
           type == KEEL_QUERY_SHOW   ||
           type == KEEL_QUERY_EXPLAIN;
}

/**
 * @brief Return whether a query-type enum is treated as a write operation.
 */
bool keel_query_type_is_write(keel_query_type_t type) {
    return type == KEEL_QUERY_INSERT    ||
           type == KEEL_QUERY_UPDATE    ||
           type == KEEL_QUERY_DELETE    ||
           type == KEEL_QUERY_TRUNCATE  ||
           type == KEEL_QUERY_MERGE     ||
           type == KEEL_QUERY_COPY;
}

/**
 * @brief Return whether a query-type enum represents DDL or maintenance work.
 */
bool keel_query_type_is_ddl(keel_query_type_t type) {
    return type == KEEL_QUERY_CREATE      ||
           type == KEEL_QUERY_ALTER       ||
           type == KEEL_QUERY_DROP        ||
           type == KEEL_QUERY_TRUNCATE    ||
           type == KEEL_QUERY_MAINTENANCE;
}

/**
 * @brief Return whether a query-type enum represents transaction control.
 */
bool keel_query_type_is_transaction(keel_query_type_t type) {
    return type == KEEL_QUERY_BEGIN     ||
           type == KEEL_QUERY_COMMIT    ||
           type == KEEL_QUERY_ROLLBACK  ||
           type == KEEL_QUERY_SAVEPOINT;
}

/**
 * @brief Map a query-type enum to a stable human-readable name.
 */
const char* keel_query_type_name(keel_query_type_t type) {
    switch (type) {
    case KEEL_QUERY_SELECT:        return "SELECT";
    case KEEL_QUERY_SHOW:          return "SHOW";
    case KEEL_QUERY_EXPLAIN:       return "EXPLAIN";
    case KEEL_QUERY_INSERT:        return "INSERT";
    case KEEL_QUERY_UPDATE:        return "UPDATE";
    case KEEL_QUERY_DELETE:        return "DELETE";
    case KEEL_QUERY_TRUNCATE:      return "TRUNCATE";
    case KEEL_QUERY_CREATE:        return "CREATE";
    case KEEL_QUERY_ALTER:         return "ALTER";
    case KEEL_QUERY_DROP:          return "DROP";
    case KEEL_QUERY_BEGIN:         return "BEGIN";
    case KEEL_QUERY_COMMIT:        return "COMMIT";
    case KEEL_QUERY_ROLLBACK:      return "ROLLBACK";
    case KEEL_QUERY_SAVEPOINT:     return "SAVEPOINT";
    case KEEL_QUERY_SET:           return "SET";
    case KEEL_QUERY_RESET:         return "RESET";
    case KEEL_QUERY_DISCARD:       return "DISCARD";
    case KEEL_QUERY_PREPARE:       return "PREPARE";
    case KEEL_QUERY_EXECUTE:       return "EXECUTE";
    case KEEL_QUERY_DEALLOCATE:    return "DEALLOCATE";
    case KEEL_QUERY_COPY:          return "COPY";
    case KEEL_QUERY_CALL:          return "CALL";
    case KEEL_QUERY_DO:            return "DO";
    case KEEL_QUERY_MERGE:         return "MERGE";
    case KEEL_QUERY_MAINTENANCE:   return "MAINTENANCE";
    case KEEL_QUERY_LOCK:          return "LOCK";
    case KEEL_QUERY_LISTEN_NOTIFY: return "LISTEN/NOTIFY";
    case KEEL_QUERY_UNLISTEN:       return "UNLISTEN";
    default:                       return "UNKNOWN";
    }
}