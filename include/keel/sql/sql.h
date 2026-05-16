/**
 * @file sql.h
 * @brief Public entry points for SQL tokenization, fast classification, and optional deep analysis.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The SQL subsystem gives KEEL two analysis tiers that serve different runtime
 * budgets:
 *
 * - a fast path that tokenizes just enough to classify routing-critical queries
 *   without heap allocation;
 * - a deeper path that builds AST and Query Tree structures when workers need
 *   table/column tracking, cache keys, or richer semantic hints.
 *
 * The distinction matters operationally. Most queries only need an answer to
 * questions such as "read or write?" and "does this start a transaction?".
 * Parsing the entire statement grammar for every packet would be wasteful. At the
 * same time, some features such as cache invalidation and dialect-aware rewriting
 * require a more structural representation. This header defines the stable API
 * boundary for both modes.
 */

#ifndef KEEL_SQL_H
#define KEEL_SQL_H

#include "keel_types.h"
#include "keel_error.h"
#include "keel/protocol/protocol.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * SQL Token Types
 * ============================================================================ */

/**
 * @brief Token classes emitted by the SQL lexer.
 *
 * These categories are intentionally coarser than a full SQL grammar. They are
 * sufficient for keyword-driven routing analysis and for the recursive-descent
 * parser implemented in the deeper analysis tier.
 */
typedef enum keel_sql_token_type {
    KEEL_SQL_TOKEN_EOF = 0,
    KEEL_SQL_TOKEN_KEYWORD,
    KEEL_SQL_TOKEN_IDENT,
    KEEL_SQL_TOKEN_STRING,
    KEEL_SQL_TOKEN_NUMBER,
    KEEL_SQL_TOKEN_OPERATOR,
    KEEL_SQL_TOKEN_SEMICOLON,
    KEEL_SQL_TOKEN_LPAREN,
    KEEL_SQL_TOKEN_RPAREN,
    KEEL_SQL_TOKEN_COMMA,
    KEEL_SQL_TOKEN_DOT,
    KEEL_SQL_TOKEN_PARAMETER,    /* $1, $2, etc. */
    KEEL_SQL_TOKEN_COMMENT,
    KEEL_SQL_TOKEN_WHITESPACE,
    KEEL_SQL_TOKEN_OTHER,
} keel_sql_token_type_t;

/**
 * @brief One token view over the original SQL byte stream.
 *
 * Tokens borrow slices from the input string; no copy is made. The `offset`
 * field lets callers map parse errors and AST locations back to the original
 * statement text.
 */
typedef struct keel_sql_token {
    keel_sql_token_type_t type;
    keel_str_t            text;
    size_t               offset;    /**< Offset in original SQL */
} keel_sql_token_t;

/* ============================================================================
 * SQL Lexer
 * ============================================================================ */

/**
 * @brief Stateful lexer cursor over a SQL string.
 *
 * The lexer is designed to be reusable and allocation-free. Whitespace and
 * comments can be skipped at scan time so higher layers pay only for the detail
 * they need.
 */
typedef struct keel_sql_lexer {
    const char* sql;
    size_t      len;
    size_t      pos;
    bool        skip_whitespace;
    bool        skip_comments;
} keel_sql_lexer_t;

/**
 * @brief Initialize a lexer over caller-owned SQL text.
 *
 * @param lexer Lexer state to initialize.
 * @param sql SQL text to scan; storage must remain valid while lexing.
 * @return
 */
void keel_sql_lexer_init(keel_sql_lexer_t* lexer, keel_str_t sql);

/**
 * @brief Advance the lexer and return the next token.
 *
 * @param lexer Lexer state.
 * @param token [out] Receives the next token.
 * @return `KEEL_OK` on success or `KEEL_ERR_IO_EOF` when input is exhausted.
 */
keel_error_t keel_sql_lexer_next(keel_sql_lexer_t* lexer, keel_sql_token_t* token);

/**
 * @brief Look ahead at the next token without consuming it.
 *
 * @param lexer Lexer state.
 * @param token [out] Receives the upcoming token.
 * @return `KEEL_OK` on success or `KEEL_ERR_IO_EOF` at end of input.
 */
keel_error_t keel_sql_lexer_peek(keel_sql_lexer_t* lexer, keel_sql_token_t* token);

/* ============================================================================
 * SQL Analysis
 * ============================================================================ */

/**
 * @brief Perform low-latency query classification for routing and pooling policy.
 *
 * This fast path deliberately stops after learning what the engine usually needs
 * on the hot path: broad statement kind, read/write/session/transaction flags,
 * and whether the query must prefer the primary. It avoids AST construction and
 * relies mostly on early keyword inspection with a few targeted scans for
 * important cases such as `SELECT ... FOR UPDATE` and writable CTEs.
 *
 * @param sql SQL text to classify.
 * @param result [out] Receives the classification result.
 * @return `KEEL_OK` on success.
 */
keel_error_t keel_sql_analyze(keel_str_t sql, keel_proto_query_t* result);

/* Forward declare Query Tree type */
typedef struct keel_qt_query keel_qt_query_t;

/**
 * @brief Parse SQL into AST and Query Tree form for richer semantic analysis.
 *
 * This is the heavyweight path used when KEEL needs more than coarse routing
 * classification, for example table-reference extraction, cache-key generation,
 * or future dialect-aware transformations.
 *
 * @param sql SQL text to analyze.
 * @param arena Arena used for all temporary allocations.
 * @return Query Tree root, or `NULL` if parsing or semantic lifting fails.
 */
keel_qt_query_t* keel_sql_analyze_full(keel_str_t sql, keel_arena_t* arena);

/**
 * @brief Run full parsing but project the result back into the legacy proto-query shape.
 *
 * @param sql SQL text.
 * @param result [out] Receives the projected classification result.
 * @param arena Arena for AST/Query Tree allocations.
 * @return `KEEL_OK` on success, with fast-path fallback if deep parsing fails.
 */
keel_error_t keel_sql_analyze_with_tree(
    keel_str_t           sql,
    keel_proto_query_t*  result,
    keel_arena_t*        arena
);

/**
 * @brief Convenience wrapper that reports whether the fast classifier sees a read-only query.
 */
bool keel_sql_is_readonly(keel_str_t sql);

/**
 * @brief Convenience wrapper that detects transaction-start statements.
 */
bool keel_sql_starts_transaction(keel_str_t sql);

/**
 * @brief Convenience wrapper that detects transaction-ending statements.
 */
bool keel_sql_ends_transaction(keel_str_t sql);

/**
 * @brief Convenience wrapper that detects session-dirtying statements such as `SET`.
 */
bool keel_sql_modifies_session(keel_str_t sql);

/**
 * @brief Count semicolon-delimited statements in a SQL string using lexer-aware scanning.
 */
size_t keel_sql_count_statements(keel_str_t sql);

/**
 * @brief Split the first lexical statement from the remainder of a SQL string.
 *
 * @param sql Full SQL text.
 * @param stmt [out] Receives the first statement slice.
 * @param rest [out] Receives the remaining tail, or may be `NULL`.
 * @return
 */
void keel_sql_first_statement(keel_str_t sql, keel_str_t* stmt, keel_str_t* rest);

/* ============================================================================
 * SQL Keywords
 * ============================================================================ */

/**
 * @brief SQL keyword type
 */
typedef enum keel_sql_keyword {
    KEEL_SQL_KW_UNKNOWN = 0,
    
    /* DML */
    KEEL_SQL_KW_SELECT,
    KEEL_SQL_KW_INSERT,
    KEEL_SQL_KW_UPDATE,
    KEEL_SQL_KW_DELETE,
    KEEL_SQL_KW_MERGE,
    KEEL_SQL_KW_TRUNCATE,
    
    /* DDL */
    KEEL_SQL_KW_CREATE,
    KEEL_SQL_KW_ALTER,
    KEEL_SQL_KW_DROP,
    
    /* Transaction */
    KEEL_SQL_KW_BEGIN,
    KEEL_SQL_KW_START,
    KEEL_SQL_KW_COMMIT,
    KEEL_SQL_KW_ROLLBACK,
    KEEL_SQL_KW_SAVEPOINT,
    KEEL_SQL_KW_RELEASE,
    KEEL_SQL_KW_END,
    
    /* Session */
    KEEL_SQL_KW_SET,
    KEEL_SQL_KW_RESET,
    KEEL_SQL_KW_DISCARD,
    KEEL_SQL_KW_DEALLOCATE,
    
    /* Query */
    KEEL_SQL_KW_WITH,
    KEEL_SQL_KW_VALUES,
    KEEL_SQL_KW_TABLE,
    
    /* Other */
    KEEL_SQL_KW_SHOW,
    KEEL_SQL_KW_EXPLAIN,
    KEEL_SQL_KW_ANALYZE,
    KEEL_SQL_KW_COPY,
    KEEL_SQL_KW_PREPARE,
    KEEL_SQL_KW_EXECUTE,
    KEEL_SQL_KW_CALL,
    KEEL_SQL_KW_DO,
    KEEL_SQL_KW_DECLARE,
    KEEL_SQL_KW_FETCH,
    KEEL_SQL_KW_MOVE,
    KEEL_SQL_KW_CLOSE,
    KEEL_SQL_KW_GRANT,
    KEEL_SQL_KW_REVOKE,
    KEEL_SQL_KW_LOCK,
    KEEL_SQL_KW_UNLISTEN,
    KEEL_SQL_KW_LISTEN,
    KEEL_SQL_KW_NOTIFY,
    KEEL_SQL_KW_VACUUM,
    KEEL_SQL_KW_REINDEX,
    KEEL_SQL_KW_CLUSTER,
    KEEL_SQL_KW_CHECKPOINT,
    KEEL_SQL_KW_LOAD,
    
    /* Clauses (for context) */
    KEEL_SQL_KW_FROM,
    KEEL_SQL_KW_WHERE,
    KEEL_SQL_KW_FOR,
    KEEL_SQL_KW_ONLY,
    KEEL_SQL_KW_READ,
    KEEL_SQL_KW_WRITE,
    KEEL_SQL_KW_TRANSACTION,
    KEEL_SQL_KW_WORK,
    KEEL_SQL_KW_ISOLATION,
    KEEL_SQL_KW_LEVEL,
    KEEL_SQL_KW_ALL,
    KEEL_SQL_KW_LOCAL,
    KEEL_SQL_KW_SESSION,
    
    /* Parser keywords */
    KEEL_SQL_KW_AND,
    KEEL_SQL_KW_OR,
    KEEL_SQL_KW_NOT,
    KEEL_SQL_KW_JOIN,
    KEEL_SQL_KW_LEFT,
    KEEL_SQL_KW_RIGHT,
    KEEL_SQL_KW_INNER,
    KEEL_SQL_KW_OUTER,
    KEEL_SQL_KW_FULL,
    KEEL_SQL_KW_CROSS,
    KEEL_SQL_KW_ON,
    KEEL_SQL_KW_USING,
    KEEL_SQL_KW_AS,
    KEEL_SQL_KW_DISTINCT,
    KEEL_SQL_KW_RETURNING,
    KEEL_SQL_KW_INTO,
    KEEL_SQL_KW_GROUP,
    KEEL_SQL_KW_HAVING,
    KEEL_SQL_KW_ORDER,
    KEEL_SQL_KW_LIMIT,
    KEEL_SQL_KW_OFFSET,
    KEEL_SQL_KW_BY,
    KEEL_SQL_KW_ASC,
    KEEL_SQL_KW_DESC,
    KEEL_SQL_KW_LIKE,
    KEEL_SQL_KW_ILIKE,
    KEEL_SQL_KW_IN,
    KEEL_SQL_KW_IS,
    KEEL_SQL_KW_BETWEEN,
    KEEL_SQL_KW_CASE,
    KEEL_SQL_KW_WHEN,
    KEEL_SQL_KW_THEN,
    KEEL_SQL_KW_ELSE,
    KEEL_SQL_KW_NULL_KW,
    KEEL_SQL_KW_TRUE,
    KEEL_SQL_KW_FALSE,
    KEEL_SQL_KW_DEFAULT,
    KEEL_SQL_KW_SHARE,
    KEEL_SQL_KW_NOWAIT,
    KEEL_SQL_KW_SKIP,
    KEEL_SQL_KW_LOCKED,

    /* Window function keywords */
    KEEL_SQL_KW_OVER,
    KEEL_SQL_KW_PARTITION,
    KEEL_SQL_KW_WINDOW,
    KEEL_SQL_KW_ROWS,
    KEEL_SQL_KW_RANGE,
    KEEL_SQL_KW_GROUPS,
    KEEL_SQL_KW_PRECEDING,
    KEEL_SQL_KW_FOLLOWING,
    KEEL_SQL_KW_UNBOUNDED,
    KEEL_SQL_KW_CURRENT_ROW,
    KEEL_SQL_KW_EXCLUDE,
    KEEL_SQL_KW_TIES,
    KEEL_SQL_KW_FILTER,

    /* CTE keywords */
    KEEL_SQL_KW_RECURSIVE,
    KEEL_SQL_KW_MATERIALIZED,

    /* Set operation keywords */
    KEEL_SQL_KW_UNION,
    KEEL_SQL_KW_INTERSECT,
    KEEL_SQL_KW_EXCEPT,
    KEEL_SQL_KW_CORRESPONDING,

    /* UDF/DO block keywords */
    KEEL_SQL_KW_LANGUAGE,
    KEEL_SQL_KW_RETURNS,
    KEEL_SQL_KW_FUNCTION,
    KEEL_SQL_KW_PROCEDURE,
    KEEL_SQL_KW_REPLACE,
    
} keel_sql_keyword_t;

/**
 * @brief Map a token slice to a known keyword enum using case-insensitive matching.
 */
KEEL_PURE keel_sql_keyword_t keel_sql_lookup_keyword(keel_str_t text);

/**
 * @brief Return the canonical uppercase spelling of a keyword enum.
 */
KEEL_PURE const char* keel_sql_keyword_name(keel_sql_keyword_t kw);

/* ============================================================================
 * Multi-Statement Transaction Scanning
 *
 * These functions scan ALL semicolon-delimited statements for transaction
 * control keywords.  Critical for multiplexing: a query like
 *   "SELECT 1; BEGIN;"
 * must be detected as containing a transaction start, even though the
 * FIRST statement is a harmless SELECT.
 * ============================================================================ */

/**
 * @brief Scan every statement in a multi-statement string for transaction start.
 */
bool keel_sql_contains_transaction_start(keel_str_t sql);

/**
 * @brief Scan every statement in a multi-statement string for transaction end.
 */
bool keel_sql_contains_transaction_end(keel_str_t sql);

/* ============================================================================
 * Query Rewriting (optional, for advanced features)
 * ============================================================================ */

/**
 * @brief Rewrite options
 */
typedef struct keel_sql_rewrite_opts {
    bool        add_read_only;      /**< Add READ ONLY to transactions */
    bool        add_statement_timeout;
    int         statement_timeout;  /**< Timeout in ms */
    const char* search_path;        /**< Set search_path */
} keel_sql_rewrite_opts_t;

/**
 * @brief Rewrite SQL according to optional policy knobs.
 *
 * The current implementation is intentionally conservative and may return the
 * input unchanged. The API exists so more advanced rewriting can be added without
 * changing callers.
 *
 * @param sql Original SQL text.
 * @param opts Rewrite options, or `NULL` for default behavior.
 * @param arena Arena for rewritten storage if a new string is produced.
 * @param result [out] Receives the resulting SQL view.
 * @return `KEEL_OK` on success.
 */
keel_error_t keel_sql_rewrite(
    keel_str_t                    sql,
    const keel_sql_rewrite_opts_t* opts,
    keel_arena_t*                 arena,
    keel_str_t*                   result
);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_SQL_H */
