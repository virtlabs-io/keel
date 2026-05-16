/**
 * @file lexer.c
 * @brief Allocation-free SQL tokenization for KEEL's analysis layers.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The lexer is the shared front end for both the fast classifier and the deeper
 * AST parser. It deliberately recognizes only the token boundaries KEEL needs to
 * reason about statements safely, while staying permissive enough to survive
 * dialect-specific syntax it does not fully understand.
 *
 * The implementation favors streaming behavior and low overhead:
 *
 * - tokens are slices into the original SQL string, never heap-allocated copies;
 * - scanning is single-pass and restartable after skipped whitespace/comments;
 * - PostgreSQL-specific constructs such as dollar strings and nested comments are
 *   handled because missing them would corrupt statement splitting and routing
 *   decisions.
 */

#include "keel/sql/sql.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <ctype.h>

/* ============================================================================
 * Keyword Lookup Table
 *
 * Maps keyword strings to enum values for fast lookup.
 * The table is organized by category for maintainability.
 * ============================================================================ */

/**
 * @brief Entry in the keyword lookup table
 */
typedef struct keyword_entry {
    const char*         name;
    keel_sql_keyword_t   keyword;
} keyword_entry_t;

static const keyword_entry_t keyword_table[] = {
    /* DML */
    {"SELECT",      KEEL_SQL_KW_SELECT},
    {"INSERT",      KEEL_SQL_KW_INSERT},
    {"UPDATE",      KEEL_SQL_KW_UPDATE},
    {"DELETE",      KEEL_SQL_KW_DELETE},
    {"MERGE",       KEEL_SQL_KW_MERGE},
    {"TRUNCATE",    KEEL_SQL_KW_TRUNCATE},
    
    /* DDL */
    {"CREATE",      KEEL_SQL_KW_CREATE},
    {"ALTER",       KEEL_SQL_KW_ALTER},
    {"DROP",        KEEL_SQL_KW_DROP},
    
    /* Transaction */
    {"BEGIN",       KEEL_SQL_KW_BEGIN},
    {"START",       KEEL_SQL_KW_START},
    {"COMMIT",      KEEL_SQL_KW_COMMIT},
    {"ROLLBACK",    KEEL_SQL_KW_ROLLBACK},
    {"SAVEPOINT",   KEEL_SQL_KW_SAVEPOINT},
    {"RELEASE",     KEEL_SQL_KW_RELEASE},
    {"END",         KEEL_SQL_KW_END},
    
    /* Session */
    {"SET",         KEEL_SQL_KW_SET},
    {"RESET",       KEEL_SQL_KW_RESET},
    {"DISCARD",     KEEL_SQL_KW_DISCARD},
    {"DEALLOCATE",  KEEL_SQL_KW_DEALLOCATE},
    
    /* Query */
    {"WITH",        KEEL_SQL_KW_WITH},
    {"VALUES",      KEEL_SQL_KW_VALUES},
    {"TABLE",       KEEL_SQL_KW_TABLE},
    
    /* Other */
    {"SHOW",        KEEL_SQL_KW_SHOW},
    {"EXPLAIN",     KEEL_SQL_KW_EXPLAIN},
    {"ANALYZE",     KEEL_SQL_KW_ANALYZE},
    {"COPY",        KEEL_SQL_KW_COPY},
    {"PREPARE",     KEEL_SQL_KW_PREPARE},
    {"EXECUTE",     KEEL_SQL_KW_EXECUTE},
    {"CALL",        KEEL_SQL_KW_CALL},
    {"DO",          KEEL_SQL_KW_DO},
    {"DECLARE",     KEEL_SQL_KW_DECLARE},
    {"FETCH",       KEEL_SQL_KW_FETCH},
    {"MOVE",        KEEL_SQL_KW_MOVE},
    {"CLOSE",       KEEL_SQL_KW_CLOSE},
    {"GRANT",       KEEL_SQL_KW_GRANT},
    {"REVOKE",      KEEL_SQL_KW_REVOKE},
    {"LOCK",        KEEL_SQL_KW_LOCK},
    {"UNLISTEN",    KEEL_SQL_KW_UNLISTEN},
    {"LISTEN",      KEEL_SQL_KW_LISTEN},
    {"NOTIFY",      KEEL_SQL_KW_NOTIFY},
    {"VACUUM",      KEEL_SQL_KW_VACUUM},
    {"REINDEX",     KEEL_SQL_KW_REINDEX},
    {"CLUSTER",     KEEL_SQL_KW_CLUSTER},
    {"CHECKPOINT",  KEEL_SQL_KW_CHECKPOINT},
    {"LOAD",        KEEL_SQL_KW_LOAD},
    
    /* Clauses */
    {"FROM",        KEEL_SQL_KW_FROM},
    {"WHERE",       KEEL_SQL_KW_WHERE},
    {"FOR",         KEEL_SQL_KW_FOR},
    {"ONLY",        KEEL_SQL_KW_ONLY},
    {"READ",        KEEL_SQL_KW_READ},
    {"WRITE",       KEEL_SQL_KW_WRITE},
    {"TRANSACTION", KEEL_SQL_KW_TRANSACTION},
    {"WORK",        KEEL_SQL_KW_WORK},
    {"ISOLATION",   KEEL_SQL_KW_ISOLATION},
    {"LEVEL",       KEEL_SQL_KW_LEVEL},
    {"ALL",         KEEL_SQL_KW_ALL},
    {"LOCAL",       KEEL_SQL_KW_LOCAL},
    {"SESSION",     KEEL_SQL_KW_SESSION},
    
    /* Parser keywords */
    {"AND",         KEEL_SQL_KW_AND},
    {"OR",          KEEL_SQL_KW_OR},
    {"NOT",         KEEL_SQL_KW_NOT},
    {"JOIN",        KEEL_SQL_KW_JOIN},
    {"LEFT",        KEEL_SQL_KW_LEFT},
    {"RIGHT",       KEEL_SQL_KW_RIGHT},
    {"INNER",       KEEL_SQL_KW_INNER},
    {"OUTER",       KEEL_SQL_KW_OUTER},
    {"FULL",        KEEL_SQL_KW_FULL},
    {"CROSS",       KEEL_SQL_KW_CROSS},
    {"ON",          KEEL_SQL_KW_ON},
    {"USING",       KEEL_SQL_KW_USING},
    {"AS",          KEEL_SQL_KW_AS},
    {"DISTINCT",    KEEL_SQL_KW_DISTINCT},
    {"RETURNING",   KEEL_SQL_KW_RETURNING},
    {"INTO",        KEEL_SQL_KW_INTO},
    {"GROUP",       KEEL_SQL_KW_GROUP},
    {"HAVING",      KEEL_SQL_KW_HAVING},
    {"ORDER",       KEEL_SQL_KW_ORDER},
    {"LIMIT",       KEEL_SQL_KW_LIMIT},
    {"OFFSET",      KEEL_SQL_KW_OFFSET},
    {"BY",          KEEL_SQL_KW_BY},
    {"ASC",         KEEL_SQL_KW_ASC},
    {"DESC",        KEEL_SQL_KW_DESC},
    {"LIKE",        KEEL_SQL_KW_LIKE},
    {"ILIKE",       KEEL_SQL_KW_ILIKE},
    {"IN",          KEEL_SQL_KW_IN},
    {"IS",          KEEL_SQL_KW_IS},
    {"BETWEEN",     KEEL_SQL_KW_BETWEEN},
    {"CASE",        KEEL_SQL_KW_CASE},
    {"WHEN",        KEEL_SQL_KW_WHEN},
    {"THEN",        KEEL_SQL_KW_THEN},
    {"ELSE",        KEEL_SQL_KW_ELSE},
    {"NULL",        KEEL_SQL_KW_NULL_KW},
    {"TRUE",        KEEL_SQL_KW_TRUE},
    {"FALSE",       KEEL_SQL_KW_FALSE},
    {"DEFAULT",     KEEL_SQL_KW_DEFAULT},
    {"SHARE",       KEEL_SQL_KW_SHARE},
    {"NOWAIT",      KEEL_SQL_KW_NOWAIT},
    {"SKIP",        KEEL_SQL_KW_SKIP},
    {"LOCKED",      KEEL_SQL_KW_LOCKED},

    /* Window function keywords */
    {"OVER",        KEEL_SQL_KW_OVER},
    {"PARTITION",   KEEL_SQL_KW_PARTITION},
    {"WINDOW",      KEEL_SQL_KW_WINDOW},
    {"ROWS",        KEEL_SQL_KW_ROWS},
    {"RANGE",       KEEL_SQL_KW_RANGE},
    {"GROUPS",      KEEL_SQL_KW_GROUPS},
    {"PRECEDING",   KEEL_SQL_KW_PRECEDING},
    {"FOLLOWING",   KEEL_SQL_KW_FOLLOWING},
    {"UNBOUNDED",   KEEL_SQL_KW_UNBOUNDED},
    {"EXCLUDE",     KEEL_SQL_KW_EXCLUDE},
    {"TIES",        KEEL_SQL_KW_TIES},
    {"FILTER",      KEEL_SQL_KW_FILTER},

    /* CTE keywords */
    {"RECURSIVE",   KEEL_SQL_KW_RECURSIVE},
    {"MATERIALIZED",KEEL_SQL_KW_MATERIALIZED},

    /* Set operation keywords */
    {"UNION",       KEEL_SQL_KW_UNION},
    {"INTERSECT",   KEEL_SQL_KW_INTERSECT},
    {"EXCEPT",      KEEL_SQL_KW_EXCEPT},

    /* UDF keywords */
    {"LANGUAGE",    KEEL_SQL_KW_LANGUAGE},
    {"RETURNS",     KEEL_SQL_KW_RETURNS},
    {"FUNCTION",    KEEL_SQL_KW_FUNCTION},
    {"PROCEDURE",   KEEL_SQL_KW_PROCEDURE},
    {"REPLACE",     KEEL_SQL_KW_REPLACE},
    
    {NULL, KEEL_SQL_KW_UNKNOWN}
};

/* ============================================================================
 * Keyword Functions
 * ============================================================================ */

/**
 * @brief Map token text to a known keyword enum using a filtered linear table scan.
 *
 * The implementation keeps the table simple and branch-friendly rather than
 * introducing a heavier generated perfect hash. First-character and length checks
 * remove most candidates before any case-insensitive compare is attempted.
 *
 * @param text Token text to classify.
 * @return Keyword enum value, or `KEEL_SQL_KW_UNKNOWN`.
 */
keel_sql_keyword_t keel_sql_lookup_keyword(keel_str_t text) {
    if (text.len == 0) return KEEL_SQL_KW_UNKNOWN;

    /* All entries in keyword_table use uppercase names, so we only need
     * to uppercase the first character of the input for the fast filter.
     * This eliminates ~96% of comparisons before any strncasecmp call. */
    char first = (char)toupper((unsigned char)text.data[0]);

    for (const keyword_entry_t* e = keyword_table; e->name; e++) {
        /* First-character filter — cheapest check, eliminates most entries */
        if ((char)(unsigned char)e->name[0] != first) continue;

        /* Length check — avoids strncasecmp on wrong-length candidates */
        size_t len = strlen(e->name);
        if (len != text.len) continue;

        if (strncasecmp(text.data, e->name, len) == 0) {
            return e->keyword;
        }
    }
    return KEEL_SQL_KW_UNKNOWN;
}

/**
 * @brief Return the canonical uppercase spelling for a keyword enum.
 */
const char* keel_sql_keyword_name(keel_sql_keyword_t kw) {
    for (const keyword_entry_t* e = keyword_table; e->name; e++) {
        if (e->keyword == kw) {
            return e->name;
        }
    }
    return "UNKNOWN";
}

/* ============================================================================
 * Lexer Implementation
 * ============================================================================ */

/**
 * @brief Initialize lexer state over caller-owned SQL bytes.
 */
void keel_sql_lexer_init(keel_sql_lexer_t* lexer, keel_str_t sql) {
    if (!lexer) return;
    
    lexer->sql = sql.data;
    lexer->len = sql.len;
    lexer->pos = 0;
    lexer->skip_whitespace = true;
    lexer->skip_comments = true;
}

/**
 * @brief Return the current byte without advancing the lexer cursor.
 */
static char peek_char(const keel_sql_lexer_t* lexer) {
    if (lexer->pos >= lexer->len) {
        return '\0';
    }
    return lexer->sql[lexer->pos];
}

/**
 * @brief Return a future byte without advancing the lexer cursor.
 */
static char peek_char_n(const keel_sql_lexer_t* lexer, size_t n) {
    if (lexer->pos + n >= lexer->len) {
        return '\0';
    }
    return lexer->sql[lexer->pos + n];
}

/**
 * @brief Consume and return the current byte.
 */
static char advance_char(keel_sql_lexer_t* lexer) {
    if (lexer->pos >= lexer->len) {
        return '\0';
    }
    return lexer->sql[lexer->pos++];
}

/**
 * @brief Advance past ASCII whitespace.
 */
static void skip_whitespace(keel_sql_lexer_t* lexer) {
    while (lexer->pos < lexer->len && isspace((unsigned char)lexer->sql[lexer->pos])) {
        lexer->pos++;
    }
}

/**
 * @brief Skip a `--` line comment through the next line break.
 */
static void skip_line_comment(keel_sql_lexer_t* lexer) {
    while (lexer->pos < lexer->len) {
        char c = lexer->sql[lexer->pos++];
        if (c == '\n' || c == '\r') {
            break;
        }
    }
}

/**
 * @brief Skip a block comment (slash-star ... star-slash), including nested PostgreSQL-style blocks.
 */
static void skip_block_comment(keel_sql_lexer_t* lexer) {
    int depth = 1;
    lexer->pos += 2; /* Skip opening slash-star */
    
    while (lexer->pos < lexer->len && depth > 0) {
        char c = lexer->sql[lexer->pos++];
        if (c == '/' && peek_char(lexer) == '*') {
            lexer->pos++;
            depth++;
        } else if (c == '*' && peek_char(lexer) == '/') {
            lexer->pos++;
            depth--;
        }
    }
}

/**
 * @brief Scan a quoted SQL string or quoted identifier token.
 *
 * @param lexer Lexer positioned immediately after the opening quote.
 * @param quote Opening quote character.
 * @param token [out] Receives the token slice.
 * @return `KEEL_OK` on success.
 */
static keel_error_t scan_string(keel_sql_lexer_t* lexer, char quote, keel_sql_token_t* token) {
    size_t start = lexer->pos - 1;
    
    while (lexer->pos < lexer->len) {
        char c = advance_char(lexer);
        if (c == quote) {
            if (peek_char(lexer) == quote) {
                lexer->pos++; /* Escaped quote */
            } else {
                break;
            }
        } else if (c == '\\' && quote == '\'') {
            advance_char(lexer); /* Backslash escape */
        }
    }
    
    token->type = KEEL_SQL_TOKEN_STRING;
    token->text.data = lexer->sql + start;
    token->text.len = lexer->pos - start;
    token->offset = start;
    
    return KEEL_OK;
}

/**
 * @brief Scan a PostgreSQL dollar-quoted string or fall back to parameter handling.
 *
 * @param lexer Lexer positioned after the initial `$`.
 * @param token [out] Receives the token slice.
 * @return `KEEL_OK` on success.
 */
static keel_error_t scan_dollar_string(keel_sql_lexer_t* lexer, keel_sql_token_t* token) {
    size_t start = lexer->pos - 1;
    
    /* Find end of tag */
    size_t tag_start = start;
    while (lexer->pos < lexer->len && lexer->sql[lexer->pos] != '$') {
        lexer->pos++;
    }
    
    if (peek_char(lexer) != '$') {
        /* Not a dollar string, treat as parameter */
        token->type = KEEL_SQL_TOKEN_PARAMETER;
        token->text.data = lexer->sql + start;
        token->text.len = lexer->pos - start;
        token->offset = start;
        return KEEL_OK;
    }
    
    size_t tag_len = lexer->pos - tag_start + 1;
    lexer->pos++; /* Skip closing $ of tag */
    
    /* Find matching end tag */
    while (lexer->pos < lexer->len) {
        if (lexer->sql[lexer->pos] == '$') {
            if (lexer->pos + tag_len <= lexer->len &&
                strncmp(lexer->sql + lexer->pos, lexer->sql + tag_start, tag_len) == 0) {
                lexer->pos += tag_len;
                break;
            }
        }
        lexer->pos++;
    }
    
    token->type = KEEL_SQL_TOKEN_STRING;
    token->text.data = lexer->sql + start;
    token->text.len = lexer->pos - start;
    token->offset = start;
    
    return KEEL_OK;
}

/**
 * @brief Scan an identifier-like token and classify it as keyword or identifier.
 */
static keel_error_t scan_identifier(keel_sql_lexer_t* lexer, keel_sql_token_t* token) {
    size_t start = lexer->pos - 1;
    
    while (lexer->pos < lexer->len) {
        char c = peek_char(lexer);
        if (isalnum((unsigned char)c) || c == '_') {
            lexer->pos++;
        } else {
            break;
        }
    }
    
    token->text.data = lexer->sql + start;
    token->text.len = lexer->pos - start;
    token->offset = start;
    
    /* Check if it's a keyword */
    keel_sql_keyword_t kw = keel_sql_lookup_keyword(token->text);
    token->type = (kw != KEEL_SQL_KW_UNKNOWN) ? KEEL_SQL_TOKEN_KEYWORD : KEEL_SQL_TOKEN_IDENT;
    
    return KEEL_OK;
}

/**
 * @brief Scan a numeric literal token with simple decimal/exponent support.
 */
static keel_error_t scan_number(keel_sql_lexer_t* lexer, keel_sql_token_t* token) {
    size_t start = lexer->pos - 1;
    
    while (lexer->pos < lexer->len) {
        char c = peek_char(lexer);
        if (isdigit((unsigned char)c) || c == '.') {
            lexer->pos++;
        } else if (c == 'e' || c == 'E') {
            lexer->pos++;
            if (peek_char(lexer) == '+' || peek_char(lexer) == '-') {
                lexer->pos++;
            }
        } else {
            break;
        }
    }
    
    token->type = KEEL_SQL_TOKEN_NUMBER;
    token->text.data = lexer->sql + start;
    token->text.len = lexer->pos - start;
    token->offset = start;
    
    return KEEL_OK;
}

/**
 * @brief Advance the lexer to the next token, skipping configured trivia.
 *
 * `restart:` is used intentionally to keep trivia-skipping tail-recursion-free and
 * cheap. The lexer remains permissive: any unclassified single byte becomes an
 * operator token rather than a hard lexical failure, because later layers often
 * only need coarse shape rather than full dialect validation.
 *
 * @param lexer Lexer to advance.
 * @param token [out] Receives the token.
 * @return `KEEL_OK` on success or `KEEL_ERR_IO_EOF` at end of input.
 */
keel_error_t keel_sql_lexer_next(keel_sql_lexer_t* lexer, keel_sql_token_t* token) {
    if (!lexer || !token) {
        return KEEL_ERR_INVALID_ARG;
    }
    
restart:
    if (lexer->skip_whitespace) {
        skip_whitespace(lexer);
    }
    
    if (lexer->pos >= lexer->len) {
        token->type = KEEL_SQL_TOKEN_EOF;
        token->text.data = NULL;
        token->text.len = 0;
        token->offset = lexer->len;
        return KEEL_ERR_IO_EOF;
    }
    
    char c = advance_char(lexer);
    
    /* Comments */
    if (c == '-' && peek_char(lexer) == '-') {
        if (lexer->skip_comments) {
            skip_line_comment(lexer);
            goto restart;
        }
        size_t start = lexer->pos - 1;
        skip_line_comment(lexer);
        token->type = KEEL_SQL_TOKEN_COMMENT;
        token->text.data = lexer->sql + start;
        token->text.len = lexer->pos - start;
        token->offset = start;
        return KEEL_OK;
    }
    
    if (c == '/' && peek_char(lexer) == '*') {
        if (lexer->skip_comments) {
            skip_block_comment(lexer);
            goto restart;
        }
        size_t start = lexer->pos - 1;
        skip_block_comment(lexer);
        token->type = KEEL_SQL_TOKEN_COMMENT;
        token->text.data = lexer->sql + start;
        token->text.len = lexer->pos - start;
        token->offset = start;
        return KEEL_OK;
    }
    
    /* Strings */
    if (c == '\'' || c == '"') {
        return scan_string(lexer, c, token);
    }
    
    /* Dollar-quoted strings or parameters */
    if (c == '$') {
        if (isdigit((unsigned char)peek_char(lexer))) {
            /* Parameter: $1, $2, etc. */
            size_t start = lexer->pos - 1;
            while (isdigit((unsigned char)peek_char(lexer))) {
                lexer->pos++;
            }
            token->type = KEEL_SQL_TOKEN_PARAMETER;
            token->text.data = lexer->sql + start;
            token->text.len = lexer->pos - start;
            token->offset = start;
            return KEEL_OK;
        }
        return scan_dollar_string(lexer, token);
    }
    
    /* Identifiers and keywords */
    if (isalpha((unsigned char)c) || c == '_') {
        return scan_identifier(lexer, token);
    }
    
    /* Numbers */
    if (isdigit((unsigned char)c) || (c == '.' && isdigit((unsigned char)peek_char(lexer)))) {
        return scan_number(lexer, token);
    }
    
    /* Single-character tokens */
    token->text.data = lexer->sql + lexer->pos - 1;
    token->text.len = 1;
    token->offset = lexer->pos - 1;
    
    switch (c) {
    case ';':
        token->type = KEEL_SQL_TOKEN_SEMICOLON;
        break;
    case '(':
        token->type = KEEL_SQL_TOKEN_LPAREN;
        break;
    case ')':
        token->type = KEEL_SQL_TOKEN_RPAREN;
        break;
    case ',':
        token->type = KEEL_SQL_TOKEN_COMMA;
        break;
    case '.':
        token->type = KEEL_SQL_TOKEN_DOT;
        break;
    default:
        token->type = KEEL_SQL_TOKEN_OPERATOR;
        /* Extend single-char operators into multi-char PostgreSQL operators.
         * This covers: <=, >=, <>, !=, ||, ->, ->>, @>, <@, #>, #>>, :: */
        if (lexer->pos < lexer->len) {
            char next = lexer->sql[lexer->pos];
            bool extend = false;
            switch (c) {
            case '<': extend = (next == '=' || next == '>' || next == '@'); break;
            case '>': extend = (next == '='); break;
            case '!': extend = (next == '='); break;
            case '|': extend = (next == '|'); break;
            case '@': extend = (next == '>'); break;
            case '#': extend = (next == '>'); break;
            case ':': extend = (next == ':'); break;
            case '-': extend = (next == '>'); break;
            default:  break;
            }
            if (extend) {
                lexer->pos++;   /* consume second char */
                token->text.len = 2;
                /* 3-char operators: ->>, #>> */
                if ((c == '-' || c == '#') && next == '>' &&
                    lexer->pos < lexer->len && lexer->sql[lexer->pos] == '>') {
                    lexer->pos++;
                    token->text.len = 3;
                }
            }
        }
        break;
    }
    
    return KEEL_OK;
}

/**
 * @brief Look ahead one token by saving and restoring the cursor position.
 */
keel_error_t keel_sql_lexer_peek(keel_sql_lexer_t* lexer, keel_sql_token_t* token) {
    if (!lexer || !token) {
        return KEEL_ERR_INVALID_ARG;
    }
    
    size_t saved_pos = lexer->pos;
    keel_error_t err = keel_sql_lexer_next(lexer, token);
    lexer->pos = saved_pos;
    
    return err;
}
