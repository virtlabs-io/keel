/**
 * @file parser.c
 * @brief Pragmatic recursive-descent SQL parser with partial-parse fallbacks.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This parser turns token streams into a structurally useful AST without trying
 * to be a validating, dialect-complete SQL frontend. Its primary job is to make
 * routing and cache decisions explainable and transformable, not to accept every
 * extension a backend might parse.
 *
 * Three implementation choices define the design:
 *
 * - common DML, transaction, and session statements get real structural nodes;
 * - expression parsing uses precedence climbing, which is compact and easy to
 *   extend for the operator set KEEL cares about;
 * - complex statements that are expensive or low value to model fully often fall
 *   back to coarse statement nodes after token skipping, preserving useful top-
 *   level semantics without pretending to understand every inner clause.
 */

#include "keel/sql/sql_ast.h"
#include "keel/sql/sql.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>

/* ============================================================================
 * Parser Utilities
 * ============================================================================ */

/**
 * @brief Record the first parse error and its byte position.
 */
static void parse_error(keel_sql_parser_t* p, const char* fmt, ...) {
    if (p->has_error) return;  /* Keep first error */
    
    p->has_error = true;
    p->error_pos = p->current.offset;
    
    va_list args;
    va_start(args, fmt);
    vsnprintf(p->error_msg, sizeof(p->error_msg), fmt, args);
    va_end(args);
}

/**
 * @brief Advance the parser window by one token.
 */
static void advance(keel_sql_parser_t* p) {
    p->current = p->lookahead;
    keel_sql_lexer_next(&p->lexer, &p->lookahead);
}

/**
 * @brief Check if current token is a keyword
 */
static bool check_keyword(keel_sql_parser_t* p, keel_sql_keyword_t kw) {
    if (p->current.type != KEEL_SQL_TOKEN_KEYWORD) return false;
    return keel_sql_lookup_keyword(p->current.text) == kw;
}

/**
 * @brief Check if current token matches type
 */
static bool check(keel_sql_parser_t* p, keel_sql_token_type_t type) {
    return p->current.type == type;
}

/**
 * @brief Consume token if it matches, return true
 */
static bool match(keel_sql_parser_t* p, keel_sql_token_type_t type) {
    if (check(p, type)) {
        advance(p);
        return true;
    }
    return false;
}

/**
 * @brief Consume keyword if it matches, return true
 */
static bool match_keyword(keel_sql_parser_t* p, keel_sql_keyword_t kw) {
    if (check_keyword(p, kw)) {
        advance(p);
        return true;
    }
    return false;
}

/**
 * @brief Expect token type, error if not found
 */
static bool expect(keel_sql_parser_t* p, keel_sql_token_type_t type, const char* what) {
    if (!check(p, type)) {
        parse_error(p, "expected %s", what);
        return false;
    }
    advance(p);
    return true;
}

/**
 * @brief Expect keyword, error if not found
 */
static bool expect_keyword(keel_sql_parser_t* p, keel_sql_keyword_t kw, const char* what) {
    if (!check_keyword(p, kw)) {
        parse_error(p, "expected %s", what);
        return false;
    }
    advance(p);
    return true;
}

/**
 * @brief Check if at end of input or statement
 */
static bool at_end(keel_sql_parser_t* p) {
    return p->current.type == KEEL_SQL_TOKEN_EOF ||
           p->current.type == KEEL_SQL_TOKEN_SEMICOLON;
}

/* ============================================================================
 * Node Allocation
 * ============================================================================ */

void* keel_sql_node_alloc(keel_sql_parser_t* p, size_t size) {
    void* node = keel_arena_alloc(p->arena, size);
    if (node) {
        memset(node, 0, size);
    }
    return node;
}

keel_sql_list_t* keel_sql_list_new(keel_sql_parser_t* p) {
    keel_sql_list_t* list = KEEL_SQL_NODE_NEW(p, keel_sql_list_t);
    if (list) {
        list->base.kind = KEEL_SQL_NODE_LIST;
    }
    return list;
}

void keel_sql_list_append(keel_sql_list_t* list, keel_sql_node_t* node) {
    if (!list || !node) return;
    
    if (!list->head) {
        list->head = node;
        list->tail = node;
    } else {
        list->tail->next = node;
        list->tail = node;
    }
    node->next = NULL;
    list->count++;
}

/**
 * @brief Seed an AST node's source location from the current token.
 */
static void set_location(keel_sql_node_t* node, const keel_sql_token_t* token) {
    node->loc.offset = token->offset;
    node->loc.length = token->text.len;
}

/* ============================================================================
 * Forward Declarations
 * ============================================================================ */

static keel_sql_node_t* parse_expression(keel_sql_parser_t* p);
static keel_sql_node_t* parse_select_stmt(keel_sql_parser_t* p);
static keel_sql_node_t* parse_table_ref(keel_sql_parser_t* p);
static keel_sql_node_t* parse_window_spec(keel_sql_parser_t* p);
static keel_sql_node_t* parse_frame_spec(keel_sql_parser_t* p);
static keel_sql_node_t* parse_cte_definition(keel_sql_parser_t* p);
static keel_sql_node_t* parse_order_item(keel_sql_parser_t* p);

/* ============================================================================
 * Expression Parsing
 * ============================================================================ */

/**
 * @brief Parse a literal value
 */
static keel_sql_node_t* parse_literal(keel_sql_parser_t* p) {
    keel_sql_expr_literal_t* lit = KEEL_SQL_NODE_NEW(p, keel_sql_expr_literal_t);
    if (!lit) return NULL;
    
    lit->base.kind = KEEL_SQL_NODE_EXPR_LITERAL;
    set_location(&lit->base, &p->current);
    
    if (p->current.type == KEEL_SQL_TOKEN_NUMBER) {
        /* Check if float or int */
        bool is_float = false;
        for (size_t i = 0; i < p->current.text.len; i++) {
            if (p->current.text.data[i] == '.' || 
                p->current.text.data[i] == 'e' ||
                p->current.text.data[i] == 'E') {
                is_float = true;
                break;
            }
        }
        
        if (is_float) {
            lit->lit_type = KEEL_SQL_LIT_FLOAT;
            lit->value.float_val = strtod(p->current.text.data, NULL);
        } else {
            lit->lit_type = KEEL_SQL_LIT_INT;
            lit->value.int_val = strtoll(p->current.text.data, NULL, 10);
        }
        advance(p);
    } else if (p->current.type == KEEL_SQL_TOKEN_STRING) {
        lit->lit_type = KEEL_SQL_LIT_STRING;
        /* Remove quotes from string */
        lit->value.str_val.data = p->current.text.data + 1;
        lit->value.str_val.len = p->current.text.len - 2;
        advance(p);
    } else if (p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
        keel_sql_keyword_t kw = keel_sql_lookup_keyword(p->current.text);
        if (kw == KEEL_SQL_KW_NULL_KW) {
            lit->lit_type = KEEL_SQL_LIT_NULL;
            advance(p);
        } else if (kw == KEEL_SQL_KW_TRUE) {
            lit->lit_type = KEEL_SQL_LIT_BOOL;
            lit->value.bool_val = true;
            advance(p);
        } else if (kw == KEEL_SQL_KW_FALSE) {
            lit->lit_type = KEEL_SQL_LIT_BOOL;
            lit->value.bool_val = false;
            advance(p);
        } else {
            return NULL;  /* Not a literal */
        }
    } else {
        return NULL;
    }
    
    return &lit->base;
}

/**
 * @brief Parse column reference
 */
static keel_sql_node_t* parse_column_ref(keel_sql_parser_t* p) {
    if (p->current.type != KEEL_SQL_TOKEN_IDENT &&
        p->current.type != KEEL_SQL_TOKEN_KEYWORD) {
        return NULL;
    }
    
    keel_sql_expr_column_t* col = KEEL_SQL_NODE_NEW(p, keel_sql_expr_column_t);
    if (!col) return NULL;
    
    col->base.kind = KEEL_SQL_NODE_EXPR_COLUMN;
    set_location(&col->base, &p->current);
    
    /* First part */
    col->column = p->current.text;
    advance(p);
    
    /* Check for table.column or schema.table.column */
    if (match(p, KEEL_SQL_TOKEN_DOT)) {
        if (p->current.type == KEEL_SQL_TOKEN_IDENT ||
            p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
            col->table = col->column;
            col->column = p->current.text;
            advance(p);
            
            if (match(p, KEEL_SQL_TOKEN_DOT)) {
                if (p->current.type == KEEL_SQL_TOKEN_IDENT ||
                    p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
                    col->schema = col->table;
                    col->table = col->column;
                    col->column = p->current.text;
                    advance(p);
                }
            }
        }
    }
    
    return &col->base;
}

/**
 * @brief Parse a star expression
 */
static keel_sql_node_t* parse_star(keel_sql_parser_t* p) {
    keel_sql_expr_star_t* star = KEEL_SQL_NODE_NEW(p, keel_sql_expr_star_t);
    if (!star) return NULL;
    
    star->base.kind = KEEL_SQL_NODE_EXPR_STAR;
    set_location(&star->base, &p->current);
    advance(p);  /* Consume * */
    
    return &star->base;
}

/**
 * @brief Parse parameter ($1, $2, etc.)
 */
static keel_sql_node_t* parse_parameter(keel_sql_parser_t* p) {
    if (p->current.type != KEEL_SQL_TOKEN_PARAMETER) return NULL;
    
    keel_sql_expr_param_t* param = KEEL_SQL_NODE_NEW(p, keel_sql_expr_param_t);
    if (!param) return NULL;
    
    param->base.kind = KEEL_SQL_NODE_EXPR_PARAM;
    set_location(&param->base, &p->current);
    
    /* Extract parameter number */
    param->index = atoi(p->current.text.data + 1);  /* Skip $ */
    advance(p);
    
    return &param->base;
}

/**
 * @brief Parse function call
 */
static keel_sql_node_t* parse_function_call(keel_sql_parser_t* p, keel_str_t name) {
    /* Special case: CAST(expr AS type) — standard SQL type cast syntax */
    if (name.len == 4 && strncasecmp(name.data, "CAST", 4) == 0) {
        if (!expect(p, KEEL_SQL_TOKEN_LPAREN, "(")) return NULL;
        keel_sql_expr_cast_t* cast = KEEL_SQL_NODE_NEW(p, keel_sql_expr_cast_t);
        if (!cast) return NULL;
        cast->base.kind   = KEEL_SQL_NODE_EXPR_CAST;
        cast->expr        = parse_expression(p);
        cast->target_type = NULL;
        /* Consume AS type_name */
        if (check_keyword(p, KEEL_SQL_KW_AS)) {
            advance(p);  /* consume AS */
            if (!at_end(p) &&
                (p->current.type == KEEL_SQL_TOKEN_IDENT ||
                 p->current.type == KEEL_SQL_TOKEN_KEYWORD)) {
                advance(p);  /* consume type name */
                /* Handle type modifiers: CHAR(n), DECIMAL(p,s), etc. */
                if (check(p, KEEL_SQL_TOKEN_LPAREN)) {
                    advance(p);
                    int depth = 1;
                    while (!at_end(p) && depth > 0) {
                        if (check(p, KEEL_SQL_TOKEN_LPAREN))      depth++;
                        else if (check(p, KEEL_SQL_TOKEN_RPAREN)) depth--;
                        advance(p);
                    }
                }
            }
        }
        expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
        return &cast->base;
    }

    keel_sql_expr_func_t* func = KEEL_SQL_NODE_NEW(p, keel_sql_expr_func_t);
    if (!func) return NULL;
    
    func->base.kind = KEEL_SQL_NODE_EXPR_FUNC;
    func->name = name;
    
    /* Consume ( */
    if (!expect(p, KEEL_SQL_TOKEN_LPAREN, "(")) {
        return NULL;
    }
    
    /* Parse arguments */
    func->args = keel_sql_list_new(p);
    
    /* Check for DISTINCT */
    if (match_keyword(p, KEEL_SQL_KW_ALL)) {
        /* ALL is default, just consume */
    } else if (check_keyword(p, KEEL_SQL_KW_DISTINCT)) {
        func->distinct = true;
        advance(p);
    }
    
    /* Check for * (for COUNT(*)) */
    if (check(p, KEEL_SQL_TOKEN_OPERATOR) && p->current.text.data[0] == '*') {
        keel_sql_node_t* star = parse_star(p);
        keel_sql_list_append(func->args, star);
    } else if (!check(p, KEEL_SQL_TOKEN_RPAREN)) {
        /* Parse argument list */
        do {
            keel_sql_node_t* arg = parse_expression(p);
            if (arg) {
                keel_sql_list_append(func->args, arg);
            }
        } while (match(p, KEEL_SQL_TOKEN_COMMA));
    }

    /* ORDER BY within aggregate (e.g., STRING_AGG(expr, sep ORDER BY key),
     * ARRAY_AGG(expr ORDER BY key), JSONB_AGG(expr ORDER BY key)) */
    if (check_keyword(p, KEEL_SQL_KW_ORDER)) {
        advance(p); /* consume ORDER */
        if (match_keyword(p, KEEL_SQL_KW_BY)) {
            /* order_by is a single keel_sql_node_t* — store first key only */
            func->order_by = parse_order_item(p);
            /* consume any additional ORDER BY keys (we ignore them for routing) */
            while (match(p, KEEL_SQL_TOKEN_COMMA) &&
                   !check(p, KEEL_SQL_TOKEN_RPAREN)) {
                parse_order_item(p); /* parsed but discarded */
            }
        }
    }

    if (!expect(p, KEEL_SQL_TOKEN_RPAREN, ")")) {
        return NULL;
    }

    /* WITHIN GROUP (ORDER BY ...) — for PERCENTILE_CONT/DISC and similar
     * ordered-set aggregate functions. */
    if (check(p, KEEL_SQL_TOKEN_IDENT) &&
        p->current.text.len == 6 &&
        strncasecmp(p->current.text.data, "within", 6) == 0) {
        advance(p); /* consume WITHIN */
        if (check_keyword(p, KEEL_SQL_KW_GROUP)) {
            advance(p); /* consume GROUP */
        }
        if (match(p, KEEL_SQL_TOKEN_LPAREN)) {
            if (check_keyword(p, KEEL_SQL_KW_ORDER)) {
                advance(p); /* consume ORDER */
                if (match_keyword(p, KEEL_SQL_KW_BY)) {
                    func->order_by = parse_order_item(p);
                    while (match(p, KEEL_SQL_TOKEN_COMMA) &&
                           !check(p, KEEL_SQL_TOKEN_RPAREN)) {
                        parse_order_item(p);
                    }
                }
            }
            expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
        }
    }

    /* Check for FILTER clause: FILTER (WHERE expr) */
    if (check_keyword(p, KEEL_SQL_KW_FILTER)) {
        advance(p);
        if (match(p, KEEL_SQL_TOKEN_LPAREN)) {
            match_keyword(p, KEEL_SQL_KW_WHERE);
            func->filter = parse_expression(p);
            expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
        }
    }

    /* Check for OVER clause (window function) */
    if (check_keyword(p, KEEL_SQL_KW_OVER)) {
        advance(p);
        func->over = parse_window_spec(p);
        /* Upgrade node kind from FUNC to WINDOW when OVER is present */
        func->base.kind = KEEL_SQL_NODE_EXPR_WINDOW;
    }

    return &func->base;
}

/**
 * @brief Parse the highest-binding expression forms: literals, refs, calls, and subqueries.
 */
static keel_sql_node_t* parse_primary(keel_sql_parser_t* p) {
    /* Parenthesized expression or subquery */
    if (check(p, KEEL_SQL_TOKEN_LPAREN)) {
        advance(p);
        
        /* Check for subquery */
        if (check_keyword(p, KEEL_SQL_KW_SELECT) ||
            check_keyword(p, KEEL_SQL_KW_WITH)) {
            keel_sql_expr_subquery_t* sq = KEEL_SQL_NODE_NEW(p, keel_sql_expr_subquery_t);
            if (!sq) return NULL;
            
            sq->base.kind = KEEL_SQL_NODE_EXPR_SUBQUERY;
            sq->select = parse_select_stmt(p);
            
            if (!expect(p, KEEL_SQL_TOKEN_RPAREN, ")")) {
                return NULL;
            }
            return &sq->base;
        }
        
        /* Regular parenthesized expression */
        keel_sql_node_t* expr = parse_expression(p);
        if (!expect(p, KEEL_SQL_TOKEN_RPAREN, ")")) {
            return NULL;
        }
        return expr;
    }
    
    /* Parameter */
    if (check(p, KEEL_SQL_TOKEN_PARAMETER)) {
        return parse_parameter(p);
    }
    
    /* Literal */
    if (check(p, KEEL_SQL_TOKEN_NUMBER) || check(p, KEEL_SQL_TOKEN_STRING)) {
        return parse_literal(p);
    }
    
    /* Star */
    if (check(p, KEEL_SQL_TOKEN_OPERATOR) && p->current.text.data[0] == '*') {
        return parse_star(p);
    }
    
    /* Identifier - could be column, function, or keyword literal */
    if (check(p, KEEL_SQL_TOKEN_IDENT) || check(p, KEEL_SQL_TOKEN_KEYWORD)) {
        /* Check for NULL, TRUE, FALSE */
        if (check(p, KEEL_SQL_TOKEN_KEYWORD)) {
            keel_sql_keyword_t kw = keel_sql_lookup_keyword(p->current.text);
            if (kw == KEEL_SQL_KW_NULL_KW || kw == KEEL_SQL_KW_TRUE || kw == KEEL_SQL_KW_FALSE) {
                return parse_literal(p);
            }

            /* CASE [expr] WHEN ... THEN ... [ELSE ...] END */
            if (kw == KEEL_SQL_KW_CASE) {
                advance(p);  /* consume CASE */
                keel_sql_expr_case_t* cexpr = KEEL_SQL_NODE_NEW(p, keel_sql_expr_case_t);
                if (!cexpr) return NULL;
                cexpr->base.kind   = KEEL_SQL_NODE_EXPR_CASE;
                cexpr->when_clauses = keel_sql_list_new(p);
                cexpr->operand     = NULL;
                cexpr->else_result = NULL;

                /* Optional subject expression: CASE expr WHEN ... */
                if (!check_keyword(p, KEEL_SQL_KW_WHEN)) {
                    cexpr->operand = parse_expression(p);
                }

                /* WHEN condition THEN result clauses */
                while (check_keyword(p, KEEL_SQL_KW_WHEN)) {
                    advance(p);  /* consume WHEN */
                    keel_sql_node_t* condition = parse_expression(p);
                    if (check_keyword(p, KEEL_SQL_KW_THEN)) {
                        advance(p);  /* consume THEN */
                    }
                    keel_sql_node_t* result = parse_expression(p);
                    /* Represent WHEN/THEN as a binary pair node */
                    keel_sql_expr_binary_t* pair = KEEL_SQL_NODE_NEW(p, keel_sql_expr_binary_t);
                    if (pair) {
                        pair->base.kind = KEEL_SQL_NODE_EXPR_BINARY;
                        pair->op        = KEEL_SQL_BINOP_EQ;  /* placeholder */
                        pair->left      = condition;
                        pair->right     = result;
                        keel_sql_list_append(cexpr->when_clauses, &pair->base);
                    }
                }

                /* Optional ELSE */
                if (check_keyword(p, KEEL_SQL_KW_ELSE)) {
                    advance(p);  /* consume ELSE */
                    cexpr->else_result = parse_expression(p);
                }

                /* Consume END */
                if (check_keyword(p, KEEL_SQL_KW_END)) {
                    advance(p);
                }

                return &cexpr->base;
            }
        }
        
        /* Save first identifier for potential qualified name or function */
        keel_str_t first_name = p->current.text;
        keel_sql_token_t first_token = p->current;
        advance(p);
        
        /* Check for function call: name( */
        if (check(p, KEEL_SQL_TOKEN_LPAREN)) {
            return parse_function_call(p, first_name);
        }
        
        /* Treat dotted identifiers as qualified column references rather than
         * trying to resolve them semantically at parse time. */
        keel_sql_expr_column_t* col = KEEL_SQL_NODE_NEW(p, keel_sql_expr_column_t);
        if (!col) return NULL;
        
        col->base.kind = KEEL_SQL_NODE_EXPR_COLUMN;
        set_location(&col->base, &first_token);
        col->column = first_name;
        
        /* Check for table.column or schema.table.column */
        if (match(p, KEEL_SQL_TOKEN_DOT)) {
            if (p->current.type == KEEL_SQL_TOKEN_IDENT ||
                p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
                col->table = col->column;
                col->column = p->current.text;
                advance(p);
                
                if (match(p, KEEL_SQL_TOKEN_DOT)) {
                    if (p->current.type == KEEL_SQL_TOKEN_IDENT ||
                        p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
                        col->schema = col->table;
                        col->table = col->column;
                        col->column = p->current.text;
                        advance(p);
                    }
                }
            }
        }
        
        return &col->base;
    }
    
    parse_error(p, "expected expression");
    return NULL;
}

/**
 * @brief Parse unary expression
 */
static keel_sql_node_t* parse_unary(keel_sql_parser_t* p) {
    /* Check for unary operators */
    if (check(p, KEEL_SQL_TOKEN_OPERATOR)) {
        char op = p->current.text.data[0];
        if (op == '-' || op == '+') {
            keel_sql_expr_unary_t* unary = KEEL_SQL_NODE_NEW(p, keel_sql_expr_unary_t);
            if (!unary) return NULL;
            
            unary->base.kind = KEEL_SQL_NODE_EXPR_UNARY;
            unary->op = (op == '-') ? KEEL_SQL_UNOP_NEG : KEEL_SQL_UNOP_POS;
            set_location(&unary->base, &p->current);
            advance(p);
            
            unary->operand = parse_unary(p);
            return &unary->base;
        }
    }
    
    /* Check for NOT */
    if (check_keyword(p, KEEL_SQL_KW_NOT)) {
        keel_sql_expr_unary_t* unary = KEEL_SQL_NODE_NEW(p, keel_sql_expr_unary_t);
        if (!unary) return NULL;
        
        unary->base.kind = KEEL_SQL_NODE_EXPR_UNARY;
        unary->op = KEEL_SQL_UNOP_NOT;
        set_location(&unary->base, &p->current);
        advance(p);
        
        unary->operand = parse_unary(p);
        return &unary->base;
    }
    
    return parse_primary(p);
}

/**
 * @brief Get binary operator from token
 */
static keel_sql_binop_t get_binop(const keel_sql_token_t* token) {
    if (token->type == KEEL_SQL_TOKEN_OPERATOR) {
        if (token->text.len == 1) {
            switch (token->text.data[0]) {
                case '+': return KEEL_SQL_BINOP_ADD;
                case '-': return KEEL_SQL_BINOP_SUB;
                case '*': return KEEL_SQL_BINOP_MUL;
                case '/': return KEEL_SQL_BINOP_DIV;
                case '%': return KEEL_SQL_BINOP_MOD;
                case '<': return KEEL_SQL_BINOP_LT;
                case '>': return KEEL_SQL_BINOP_GT;
                case '=': return KEEL_SQL_BINOP_EQ;
            }
        } else if (token->text.len == 2) {
            if (strncmp(token->text.data, "<=", 2) == 0) return KEEL_SQL_BINOP_LE;
            if (strncmp(token->text.data, ">=", 2) == 0) return KEEL_SQL_BINOP_GE;
            if (strncmp(token->text.data, "<>", 2) == 0) return KEEL_SQL_BINOP_NE;
            if (strncmp(token->text.data, "!=", 2) == 0) return KEEL_SQL_BINOP_NE;
            if (strncmp(token->text.data, "||", 2) == 0) return KEEL_SQL_BINOP_CONCAT;
            if (strncmp(token->text.data, "->", 2) == 0) return KEEL_SQL_BINOP_JSON_GET;
            if (strncmp(token->text.data, "@>", 2) == 0) return KEEL_SQL_BINOP_JSON_CONTAINS;
            if (strncmp(token->text.data, "<@", 2) == 0) return KEEL_SQL_BINOP_JSON_CONTAINED;
            if (strncmp(token->text.data, "#>", 2) == 0) return KEEL_SQL_BINOP_JSON_PATH;
            /* "::" is handled as a postfix type cast in parse_binary, not as a binop */
        } else if (token->text.len == 3) {
            if (strncmp(token->text.data, "->>", 3) == 0) return KEEL_SQL_BINOP_JSON_GET_TEXT;
            if (strncmp(token->text.data, "#>>", 3) == 0) return KEEL_SQL_BINOP_JSON_PATH_TEXT;
        }
    }
    return (keel_sql_binop_t)-1;  /* Invalid */
}

/**
 * @brief Get operator precedence
 */
static int get_precedence(const keel_sql_token_t* token) {
    if (token->type == KEEL_SQL_TOKEN_KEYWORD) {
        keel_sql_keyword_t kw = keel_sql_lookup_keyword(token->text);
        if (kw == KEEL_SQL_KW_OR) return 1;
        if (kw == KEEL_SQL_KW_AND) return 2;
        return 0;
    }
    
    if (token->type == KEEL_SQL_TOKEN_OPERATOR) {
        keel_sql_binop_t op = get_binop(token);
        switch (op) {
            case KEEL_SQL_BINOP_EQ:
            case KEEL_SQL_BINOP_NE:
            case KEEL_SQL_BINOP_LT:
            case KEEL_SQL_BINOP_LE:
            case KEEL_SQL_BINOP_GT:
            case KEEL_SQL_BINOP_GE:
            case KEEL_SQL_BINOP_JSON_CONTAINS:
            case KEEL_SQL_BINOP_JSON_CONTAINED:
                return 3;
            case KEEL_SQL_BINOP_CONCAT:
                return 4;
            case KEEL_SQL_BINOP_ADD:
            case KEEL_SQL_BINOP_SUB:
                return 5;
            case KEEL_SQL_BINOP_MUL:
            case KEEL_SQL_BINOP_DIV:
            case KEEL_SQL_BINOP_MOD:
                return 6;
            case KEEL_SQL_BINOP_JSON_GET:
            case KEEL_SQL_BINOP_JSON_GET_TEXT:
            case KEEL_SQL_BINOP_JSON_PATH:
            case KEEL_SQL_BINOP_JSON_PATH_TEXT:
                return 7;
            default:
                return 0;
        }
    }
    
    return 0;
}

/**
 * @brief Parse infix expressions using precedence climbing.
 *
 * This algorithm is compact, iterative in the common case, and far easier to
 * maintain than a separate function per precedence tier for the operator subset
 * KEEL models.
 *
 * In addition to standard binary operators, this function handles SQL-specific
 * infix/postfix forms: BETWEEN, IN, LIKE, ILIKE, IS [NOT] NULL, NOT BETWEEN,
 * NOT IN, NOT LIKE, and the PostgreSQL :: type-cast postfix operator.
 */
static keel_sql_node_t* parse_binary(keel_sql_parser_t* p, int min_prec) {
    keel_sql_node_t* left = parse_unary(p);
    if (!left) return NULL;
    
    while (!at_end(p)) {
        /* ---- PostgreSQL :: type-cast postfix operator (prec 8) ---- */
        if (p->current.type == KEEL_SQL_TOKEN_OPERATOR &&
            p->current.text.len == 2 &&
            p->current.text.data[0] == ':' && p->current.text.data[1] == ':') {
            if (8 < min_prec) break;
            advance(p);  /* consume :: */
            /* Consume the type name (identifier or keyword like INT, BOOLEAN…) */
            if (!at_end(p) &&
                (p->current.type == KEEL_SQL_TOKEN_IDENT ||
                 p->current.type == KEEL_SQL_TOKEN_KEYWORD)) {
                advance(p);  /* consume type name */
                /* Handle type modifiers: VARCHAR(100), NUMERIC(10,2) */
                if (check(p, KEEL_SQL_TOKEN_LPAREN)) {
                    advance(p);  /* consume ( */
                    int depth = 1;
                    while (!at_end(p) && depth > 0) {
                        if (check(p, KEEL_SQL_TOKEN_LPAREN))       depth++;
                        else if (check(p, KEEL_SQL_TOKEN_RPAREN))  depth--;
                        advance(p);
                    }
                }
            }
            keel_sql_expr_cast_t* cast = KEEL_SQL_NODE_NEW(p, keel_sql_expr_cast_t);
            if (!cast) return left;
            cast->base.kind   = KEEL_SQL_NODE_EXPR_CAST;
            cast->expr        = left;
            cast->target_type = NULL;
            left = &cast->base;
            continue;
        }

        /* ---- SQL keyword infix / postfix operators ---- */
        if (p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
            keel_sql_keyword_t kw = keel_sql_lookup_keyword(p->current.text);

            /* BETWEEN lo AND hi */
            if (kw == KEEL_SQL_KW_BETWEEN) {
                if (3 < min_prec) break;
                advance(p);  /* consume BETWEEN */
                keel_sql_expr_between_t* btn = KEEL_SQL_NODE_NEW(p, keel_sql_expr_between_t);
                if (!btn) return left;
                btn->base.kind  = KEEL_SQL_NODE_EXPR_BETWEEN;
                set_location(&btn->base, &p->current);
                btn->expr      = left;
                btn->negated   = false;
                btn->symmetric = false;
                btn->low       = parse_binary(p, 4);  /* prec above AND(2) */
                if (check_keyword(p, KEEL_SQL_KW_AND)) advance(p);  /* consume AND */
                btn->high      = parse_binary(p, 4);
                left = &btn->base;
                continue;
            }

            /* IN (list | subquery) */
            if (kw == KEEL_SQL_KW_IN) {
                if (3 < min_prec) break;
                advance(p);  /* consume IN */
                keel_sql_expr_in_t* in_expr = KEEL_SQL_NODE_NEW(p, keel_sql_expr_in_t);
                if (!in_expr) return left;
                in_expr->base.kind = KEEL_SQL_NODE_EXPR_IN;
                set_location(&in_expr->base, &p->current);
                in_expr->expr    = left;
                in_expr->negated = false;
                in_expr->list    = keel_sql_list_new(p);
                in_expr->subquery = NULL;
                if (match(p, KEEL_SQL_TOKEN_LPAREN)) {
                    if (check_keyword(p, KEEL_SQL_KW_SELECT) ||
                        check_keyword(p, KEEL_SQL_KW_WITH)) {
                        in_expr->subquery = parse_select_stmt(p);
                    } else {
                        do {
                            keel_sql_node_t* val = parse_binary(p, 1);
                            if (val) keel_sql_list_append(in_expr->list, val);
                        } while (match(p, KEEL_SQL_TOKEN_COMMA));
                    }
                    match(p, KEEL_SQL_TOKEN_RPAREN);
                }
                left = &in_expr->base;
                continue;
            }

            /* LIKE / ILIKE pattern */
            if (kw == KEEL_SQL_KW_LIKE || kw == KEEL_SQL_KW_ILIKE) {
                if (3 < min_prec) break;
                bool icase = (kw == KEEL_SQL_KW_ILIKE);
                advance(p);  /* consume LIKE/ILIKE */
                keel_sql_expr_like_t* like_expr = KEEL_SQL_NODE_NEW(p, keel_sql_expr_like_t);
                if (!like_expr) return left;
                like_expr->base.kind = KEEL_SQL_NODE_EXPR_LIKE;
                set_location(&like_expr->base, &p->current);
                like_expr->expr    = left;
                like_expr->negated = false;
                like_expr->icase   = icase;
                like_expr->pattern = parse_binary(p, 4);
                like_expr->escape  = NULL;
                left = &like_expr->base;
                continue;
            }

            /* IS [NOT] NULL / IS [NOT] TRUE / IS [NOT] FALSE */
            if (kw == KEEL_SQL_KW_IS) {
                if (3 < min_prec) break;
                advance(p);  /* consume IS */
                keel_sql_expr_is_null_t* isnull = KEEL_SQL_NODE_NEW(p, keel_sql_expr_is_null_t);
                if (!isnull) return left;
                isnull->base.kind = KEEL_SQL_NODE_EXPR_IS_NULL;
                set_location(&isnull->base, &p->current);
                isnull->expr    = left;
                isnull->negated = false;
                if (check_keyword(p, KEEL_SQL_KW_NOT)) {
                    isnull->negated = true;
                    advance(p);  /* consume NOT */
                }
                /* Consume the predicate keyword (NULL, TRUE, FALSE) */
                if (check_keyword(p, KEEL_SQL_KW_NULL_KW) ||
                    check_keyword(p, KEEL_SQL_KW_TRUE)     ||
                    check_keyword(p, KEEL_SQL_KW_FALSE)) {
                    advance(p);
                }
                left = &isnull->base;
                continue;
            }

            /* NOT BETWEEN / NOT IN / NOT LIKE / NOT ILIKE (infix NOT) */
            if (kw == KEEL_SQL_KW_NOT) {
                keel_sql_keyword_t next_kw = KEEL_SQL_KW_UNKNOWN;
                if (p->lookahead.type == KEEL_SQL_TOKEN_KEYWORD) {
                    next_kw = keel_sql_lookup_keyword(p->lookahead.text);
                }
                if (next_kw == KEEL_SQL_KW_BETWEEN || next_kw == KEEL_SQL_KW_IN ||
                    next_kw == KEEL_SQL_KW_LIKE    || next_kw == KEEL_SQL_KW_ILIKE) {
                    if (3 < min_prec) break;
                    advance(p);  /* consume NOT */
                    keel_sql_keyword_t op_kw = keel_sql_lookup_keyword(p->current.text);

                    if (op_kw == KEEL_SQL_KW_BETWEEN) {
                        advance(p);  /* consume BETWEEN */
                        keel_sql_expr_between_t* btn = KEEL_SQL_NODE_NEW(p, keel_sql_expr_between_t);
                        if (!btn) return left;
                        btn->base.kind  = KEEL_SQL_NODE_EXPR_BETWEEN;
                        set_location(&btn->base, &p->current);
                        btn->expr      = left;
                        btn->negated   = true;
                        btn->symmetric = false;
                        btn->low       = parse_binary(p, 4);
                        if (check_keyword(p, KEEL_SQL_KW_AND)) advance(p);
                        btn->high      = parse_binary(p, 4);
                        left = &btn->base;
                    } else if (op_kw == KEEL_SQL_KW_IN) {
                        advance(p);  /* consume IN */
                        keel_sql_expr_in_t* in_expr = KEEL_SQL_NODE_NEW(p, keel_sql_expr_in_t);
                        if (!in_expr) return left;
                        in_expr->base.kind = KEEL_SQL_NODE_EXPR_IN;
                        set_location(&in_expr->base, &p->current);
                        in_expr->expr     = left;
                        in_expr->negated  = true;
                        in_expr->list     = keel_sql_list_new(p);
                        in_expr->subquery = NULL;
                        if (match(p, KEEL_SQL_TOKEN_LPAREN)) {
                            if (check_keyword(p, KEEL_SQL_KW_SELECT) ||
                                check_keyword(p, KEEL_SQL_KW_WITH)) {
                                in_expr->subquery = parse_select_stmt(p);
                            } else {
                                do {
                                    keel_sql_node_t* val = parse_binary(p, 1);
                                    if (val) keel_sql_list_append(in_expr->list, val);
                                } while (match(p, KEEL_SQL_TOKEN_COMMA));
                            }
                            match(p, KEEL_SQL_TOKEN_RPAREN);
                        }
                        left = &in_expr->base;
                    } else {
                        /* NOT LIKE / NOT ILIKE */
                        bool icase = (op_kw == KEEL_SQL_KW_ILIKE);
                        advance(p);  /* consume LIKE/ILIKE */
                        keel_sql_expr_like_t* like_expr = KEEL_SQL_NODE_NEW(p, keel_sql_expr_like_t);
                        if (!like_expr) return left;
                        like_expr->base.kind = KEEL_SQL_NODE_EXPR_LIKE;
                        set_location(&like_expr->base, &p->current);
                        like_expr->expr    = left;
                        like_expr->negated = true;
                        like_expr->icase   = icase;
                        like_expr->pattern = parse_binary(p, 4);
                        like_expr->escape  = NULL;
                        left = &like_expr->base;
                    }
                    continue;
                }
                /* Plain NOT — not an infix operator here; fall through to prec check */
            }
        }

        int prec = get_precedence(&p->current);
        if (prec < min_prec) break;
        
        keel_sql_expr_binary_t* binary = KEEL_SQL_NODE_NEW(p, keel_sql_expr_binary_t);
        if (!binary) return NULL;
        
        binary->base.kind = KEEL_SQL_NODE_EXPR_BINARY;
        set_location(&binary->base, &p->current);
        
        /* Handle keyword operators (AND, OR) */
        if (p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
            keel_sql_keyword_t kw = keel_sql_lookup_keyword(p->current.text);
            if (kw == KEEL_SQL_KW_AND) {
                binary->op = KEEL_SQL_BINOP_AND;
            } else if (kw == KEEL_SQL_KW_OR) {
                binary->op = KEEL_SQL_BINOP_OR;
            }
            advance(p);
        } else {
            binary->op = get_binop(&p->current);
            advance(p);
        }
        
        binary->left = left;
        binary->right = parse_binary(p, prec + 1);
        
        left = &binary->base;
    }
    
    return left;
}

/**
 * @brief Parse full expression
 */
static keel_sql_node_t* parse_expression(keel_sql_parser_t* p) {
    return parse_binary(p, 1);
}

/* ============================================================================
 * Table Reference Parsing
 * ============================================================================ */

/**
 * @brief Parse a basic table reference with optional schema and alias.
 */
static keel_sql_node_t* parse_table_ref(keel_sql_parser_t* p) {
    keel_sql_table_ref_t* ref = KEEL_SQL_NODE_NEW(p, keel_sql_table_ref_t);
    if (!ref) return NULL;
    
    ref->base.kind = KEEL_SQL_NODE_TABLE_REF;
    set_location(&ref->base, &p->current);
    
    /* Parse table name */
    if (p->current.type != KEEL_SQL_TOKEN_IDENT &&
        p->current.type != KEEL_SQL_TOKEN_KEYWORD) {
        parse_error(p, "expected table name");
        return NULL;
    }
    
    ref->table = p->current.text;
    advance(p);
    
    /* Check for schema.table */
    if (match(p, KEEL_SQL_TOKEN_DOT)) {
        ref->schema = ref->table;
        if (p->current.type != KEEL_SQL_TOKEN_IDENT &&
            p->current.type != KEEL_SQL_TOKEN_KEYWORD) {
            parse_error(p, "expected table name after schema");
            return NULL;
        }
        ref->table = p->current.text;
        advance(p);
    }
    
    /* Check for alias */
    if (match_keyword(p, KEEL_SQL_KW_AS)) {
        /* AS consumed, now expect identifier */
    }
    
    if (p->current.type == KEEL_SQL_TOKEN_IDENT) {
        ref->alias = p->current.text;
        advance(p);
    }
    
    return &ref->base;
}

/**
 * @brief Parse a FROM clause including a bounded set of JOIN syntaxes.
 *
 * The parser models join topology and ON/USING expressions because that is enough
 * for table/column extraction later. It does not attempt full join semantics.
 */
static keel_sql_node_t* parse_from_clause(keel_sql_parser_t* p) {
    if (!match_keyword(p, KEEL_SQL_KW_FROM)) {
        return NULL;
    }

    /* Helper lambda — parse one FROM item: either a subquery or a table ref */
    keel_sql_node_t* result;
    if (check(p, KEEL_SQL_TOKEN_LPAREN)) {
        advance(p); /* consume '(' */
        /* Parse inner SELECT */
        keel_sql_table_subquery_t* tsq =
            KEEL_SQL_NODE_NEW(p, keel_sql_table_subquery_t);
        if (!tsq) return NULL;
        tsq->base.kind = KEEL_SQL_NODE_TABLE_SUBQUERY;
        tsq->subquery   = parse_select_stmt(p);
        if (!expect(p, KEEL_SQL_TOKEN_RPAREN, ")")) return NULL;
        /* Optional AS alias */
        match_keyword(p, KEEL_SQL_KW_AS);
        if (p->current.type == KEEL_SQL_TOKEN_IDENT) {
            tsq->alias = p->current.text;
            advance(p);
        }
        result = &tsq->base;
    } else {
        result = parse_table_ref(p);
    }
    
    /* Parse JOINs */
    while (!at_end(p)) {
        keel_sql_join_type_t join_type = KEEL_SQL_JOIN_INNER;
        bool is_join = false;
        
        /* Check for comma join */
        if (match(p, KEEL_SQL_TOKEN_COMMA)) {
            is_join = true;
            join_type = KEEL_SQL_JOIN_CROSS;
        }
        /* Check for JOIN keyword */
        else if (check_keyword(p, KEEL_SQL_KW_JOIN)) {
            is_join = true;
            advance(p);
        }
        /* Check for LEFT [OUTER] JOIN */
        else if (check_keyword(p, KEEL_SQL_KW_LEFT)) {
            is_join = true;
            join_type = KEEL_SQL_JOIN_LEFT;
            advance(p);
            match_keyword(p, KEEL_SQL_KW_OUTER);  /* Optional OUTER */
            match_keyword(p, KEEL_SQL_KW_JOIN);   /* Optional JOIN */
        }
        /* Check for RIGHT [OUTER] JOIN */
        else if (check_keyword(p, KEEL_SQL_KW_RIGHT)) {
            is_join = true;
            join_type = KEEL_SQL_JOIN_RIGHT;
            advance(p);
            match_keyword(p, KEEL_SQL_KW_OUTER);
            match_keyword(p, KEEL_SQL_KW_JOIN);
        }
        /* Check for INNER JOIN */
        else if (check_keyword(p, KEEL_SQL_KW_INNER)) {
            is_join = true;
            advance(p);
            match_keyword(p, KEEL_SQL_KW_JOIN);
        }
        /* Check for CROSS JOIN */
        else if (check_keyword(p, KEEL_SQL_KW_CROSS)) {
            is_join = true;
            join_type = KEEL_SQL_JOIN_CROSS;
            advance(p);
            match_keyword(p, KEEL_SQL_KW_JOIN);
        }
        /* Check for FULL [OUTER] JOIN */
        else if (check_keyword(p, KEEL_SQL_KW_FULL)) {
            is_join = true;
            join_type = KEEL_SQL_JOIN_FULL;
            advance(p);
            match_keyword(p, KEEL_SQL_KW_OUTER);
            match_keyword(p, KEEL_SQL_KW_JOIN);
        }
        
        if (!is_join) break;
        
        keel_sql_join_t* join = KEEL_SQL_NODE_NEW(p, keel_sql_join_t);
        if (!join) return NULL;
        
        join->base.kind = KEEL_SQL_NODE_TABLE_JOIN;
        join->join_type = join_type;
        join->left = result;
        join->right = parse_table_ref(p);
        
        /* Parse ON clause */
        if (check_keyword(p, KEEL_SQL_KW_ON)) {
            advance(p);
            join->on_clause = parse_expression(p);
        }
        /* Or USING clause */
        else if (check_keyword(p, KEEL_SQL_KW_USING)) {
            advance(p);
            if (!expect(p, KEEL_SQL_TOKEN_LPAREN, "(")) {
                return NULL;
            }
            join->using_cols = keel_sql_list_new(p);
            do {
                if (p->current.type == KEEL_SQL_TOKEN_IDENT) {
                    keel_sql_expr_column_t* col = KEEL_SQL_NODE_NEW(p, keel_sql_expr_column_t);
                    col->base.kind = KEEL_SQL_NODE_EXPR_COLUMN;
                    col->column = p->current.text;
                    keel_sql_list_append(join->using_cols, &col->base);
                    advance(p);
                }
            } while (match(p, KEEL_SQL_TOKEN_COMMA));
            expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
        }
        
        result = &join->base;
    }
    
    return result;
}

/* ============================================================================
 * Statement Parsing
 * ============================================================================ */

/**
 * @brief Parse select target (column/expression in SELECT list)
 */
static keel_sql_node_t* parse_select_target(keel_sql_parser_t* p) {
    keel_sql_select_target_t* target = KEEL_SQL_NODE_NEW(p, keel_sql_select_target_t);
    if (!target) return NULL;
    
    target->base.kind = KEEL_SQL_NODE_SELECT_TARGET;
    set_location(&target->base, &p->current);
    
    target->expr = parse_expression(p);
    
    /* Check for alias */
    if (check_keyword(p, KEEL_SQL_KW_AS)) {
        advance(p);
        if (p->current.type == KEEL_SQL_TOKEN_IDENT ||
            p->current.type == KEEL_SQL_TOKEN_STRING) {
            target->alias = p->current.text;
            advance(p);
        }
    } else if (p->current.type == KEEL_SQL_TOKEN_IDENT) {
        /* Alias without AS */
        target->alias = p->current.text;
        advance(p);
    }
    
    return &target->base;
}

/**
 * @brief Parse ORDER BY item
 */
static keel_sql_node_t* parse_order_item(keel_sql_parser_t* p) {
    keel_sql_order_item_t* item = KEEL_SQL_NODE_NEW(p, keel_sql_order_item_t);
    if (!item) return NULL;
    
    item->base.kind = KEEL_SQL_NODE_ORDER_ITEM;
    item->expr = parse_expression(p);
    item->direction = KEEL_SQL_ORDER_ASC;
    item->nulls = KEEL_SQL_NULLS_DEFAULT;
    
    /* Check for ASC/DESC */
    if (check_keyword(p, KEEL_SQL_KW_ASC)) {
        item->direction = KEEL_SQL_ORDER_ASC;
        advance(p);
    } else if (check_keyword(p, KEEL_SQL_KW_DESC)) {
        item->direction = KEEL_SQL_ORDER_DESC;
        advance(p);
    }
    
    /* Check for NULLS FIRST/LAST - these are not in our keyword table, use ident check */
    if ((check(p, KEEL_SQL_TOKEN_IDENT) || check(p, KEEL_SQL_TOKEN_KEYWORD)) &&
        p->current.text.len == 5 &&
        strncasecmp(p->current.text.data, "NULLS", 5) == 0) {
        advance(p);
        if ((check(p, KEEL_SQL_TOKEN_IDENT) || check(p, KEEL_SQL_TOKEN_KEYWORD)) &&
            p->current.text.len == 5 &&
            strncasecmp(p->current.text.data, "FIRST", 5) == 0) {
            item->nulls = KEEL_SQL_NULLS_FIRST;
            advance(p);
        } else if ((check(p, KEEL_SQL_TOKEN_IDENT) || check(p, KEEL_SQL_TOKEN_KEYWORD)) &&
                   p->current.text.len == 4 &&
                   strncasecmp(p->current.text.data, "LAST", 4) == 0) {
            item->nulls = KEEL_SQL_NULLS_LAST;
            advance(p);
        }
    }
    
    return &item->base;
}

/* ============================================================================
 * Forward declarations for recursive parsing
 * ============================================================================ */
static keel_sql_node_t* parse_select_stmt(keel_sql_parser_t* p);
static keel_sql_node_t* parse_insert_stmt(keel_sql_parser_t* p);
static keel_sql_node_t* parse_update_stmt(keel_sql_parser_t* p);
static keel_sql_node_t* parse_delete_stmt(keel_sql_parser_t* p);
static keel_sql_node_t* parse_expression(keel_sql_parser_t* p);

/* ============================================================================
 * Identifier / keyword text comparison helpers
 * ============================================================================ */

/**
 * @brief Check if current token (ident or keyword) matches word (case-insensitive)
 */
static bool check_word(keel_sql_parser_t* p, const char* word, size_t wlen) {
    if (p->current.type != KEEL_SQL_TOKEN_IDENT &&
        p->current.type != KEEL_SQL_TOKEN_KEYWORD) return false;
    return p->current.text.len == wlen &&
           strncasecmp(p->current.text.data, word, wlen) == 0;
}

/**
 * @brief Advance if current token matches word, return true
 */
static bool match_word(keel_sql_parser_t* p, const char* word, size_t wlen) {
    if (check_word(p, word, wlen)) { advance(p); return true; }
    return false;
}

/* ============================================================================
 * Window Frame Parsing
 * ============================================================================ */

/**
 * @brief Parse a window frame bound
 * Returns the bound type and sets *offset_expr if applicable.
 */
static keel_sql_frame_bound_t parse_frame_bound(keel_sql_parser_t* p,
                                                 keel_sql_node_t** offset_expr) {
    *offset_expr = NULL;

    /* UNBOUNDED PRECEDING / UNBOUNDED FOLLOWING */
    if (check_keyword(p, KEEL_SQL_KW_UNBOUNDED)) {
        advance(p);
        if (check_keyword(p, KEEL_SQL_KW_PRECEDING)) {
            advance(p);
            return KEEL_SQL_FRAME_UNBOUNDED_PRECEDING;
        } else if (check_keyword(p, KEEL_SQL_KW_FOLLOWING)) {
            advance(p);
            return KEEL_SQL_FRAME_UNBOUNDED_FOLLOWING;
        }
        /* UNBOUNDED without direction — treat as UNBOUNDED PRECEDING */
        return KEEL_SQL_FRAME_UNBOUNDED_PRECEDING;
    }

    /* CURRENT ROW */
    if (check_keyword(p, KEEL_SQL_KW_CURRENT_ROW) ||
        check_word(p, "CURRENT", 7)) {
        advance(p);
        /* consume optional ROW */
        match_word(p, "ROW", 3);
        return KEEL_SQL_FRAME_CURRENT_ROW;
    }

    /* expr PRECEDING / expr FOLLOWING */
    *offset_expr = parse_expression(p);
    if (check_keyword(p, KEEL_SQL_KW_PRECEDING)) {
        advance(p);
        return KEEL_SQL_FRAME_OFFSET_PRECEDING;
    } else if (check_keyword(p, KEEL_SQL_KW_FOLLOWING)) {
        advance(p);
        return KEEL_SQL_FRAME_OFFSET_FOLLOWING;
    }
    /* Default: treat as PRECEDING */
    return KEEL_SQL_FRAME_OFFSET_PRECEDING;
}

/**
 * @brief Parse a window frame specification.
 *
 * Support here is intentionally structural rather than exhaustive; the goal is to
 * preserve enough shape for analysis and regeneration, not to normalize every
 * backend-specific nuance.
 */
static keel_sql_node_t* parse_frame_spec(keel_sql_parser_t* p) {
    keel_sql_frame_spec_t* fs = KEEL_SQL_NODE_NEW(p, keel_sql_frame_spec_t);
    if (!fs) return NULL;
    fs->base.kind = KEEL_SQL_NODE_FRAME_SPEC;

    if (check_keyword(p, KEEL_SQL_KW_ROWS)) {
        fs->mode = KEEL_SQL_FRAME_ROWS;   advance(p);
    } else if (check_keyword(p, KEEL_SQL_KW_RANGE)) {
        fs->mode = KEEL_SQL_FRAME_RANGE;  advance(p);
    } else if (check_keyword(p, KEEL_SQL_KW_GROUPS)) {
        fs->mode = KEEL_SQL_FRAME_GROUPS; advance(p);
    }

    if (check_keyword(p, KEEL_SQL_KW_BETWEEN)) {
        advance(p);
        fs->has_between = true;
        fs->start_bound = parse_frame_bound(p, &fs->start_offset);
        /* AND */
        if (check_keyword(p, KEEL_SQL_KW_AND)) advance(p);
        fs->end_bound   = parse_frame_bound(p, &fs->end_offset);
    } else {
        /* Single bound (no BETWEEN) */
        fs->start_bound = parse_frame_bound(p, &fs->start_offset);
        fs->end_bound   = KEEL_SQL_FRAME_CURRENT_ROW;
    }

    /* EXCLUDE { CURRENT ROW | GROUP | TIES | NO OTHERS } */
    if (check_keyword(p, KEEL_SQL_KW_EXCLUDE)) {
        advance(p);
        if (check_word(p, "CURRENT", 7)) {
            advance(p); match_word(p, "ROW", 3);
            fs->exclude = KEEL_SQL_FRAME_EXCL_CURRENT_ROW;
        } else if (check_word(p, "GROUP", 5)) {
            advance(p);
            fs->exclude = KEEL_SQL_FRAME_EXCL_GROUP;
        } else if (check_keyword(p, KEEL_SQL_KW_TIES)) {
            advance(p);
            fs->exclude = KEEL_SQL_FRAME_EXCL_TIES;
        } else {
            /* NO OTHERS — default, just consume */
            match_word(p, "NO", 2);
            match_word(p, "OTHERS", 6);
            fs->exclude = KEEL_SQL_FRAME_EXCL_NONE;
        }
    }

    return &fs->base;
}

/**
 * @brief Parse OVER clause window specification
 *
 * Called after OVER token is consumed. Handles:
 *   OVER window_name               — named reference
 *   OVER (window_spec)             — inline specification
 *   OVER ()                        — empty spec (whole partition)
 */
static keel_sql_node_t* parse_window_spec(keel_sql_parser_t* p) {
    keel_sql_window_spec_t* ws = KEEL_SQL_NODE_NEW(p, keel_sql_window_spec_t);
    if (!ws) return NULL;
    ws->base.kind = KEEL_SQL_NODE_WINDOW_SPEC;

    /* OVER window_name (ident, not a reserved keyword) */
    if (!check(p, KEEL_SQL_TOKEN_LPAREN)) {
        /* Named window reference */
        if (check(p, KEEL_SQL_TOKEN_IDENT) ||
            (check(p, KEEL_SQL_TOKEN_KEYWORD) &&
             keel_sql_lookup_keyword(p->current.text) > KEEL_SQL_KW_LOCKED)) {
            ws->ref_name.data = p->current.text.data;
            ws->ref_name.len  = p->current.text.len;
            advance(p);
        }
        return &ws->base;
    }

    /* OVER ( ... ) */
    advance(p);  /* consume '(' */

    /* Optional base window name (window reference inside spec) */
    if (check(p, KEEL_SQL_TOKEN_IDENT) &&
        !check_keyword(p, KEEL_SQL_KW_PARTITION) &&
        !check_keyword(p, KEEL_SQL_KW_ORDER)) {
        ws->ref_name.data = p->current.text.data;
        ws->ref_name.len  = p->current.text.len;
        advance(p);
    }

    /* PARTITION BY expr_list */
    if (check_keyword(p, KEEL_SQL_KW_PARTITION)) {
        advance(p);
        if (check_keyword(p, KEEL_SQL_KW_BY)) advance(p);
        ws->partition_by = keel_sql_list_new(p);
        do {
            keel_sql_node_t* expr = parse_expression(p);
            if (expr) keel_sql_list_append(ws->partition_by, expr);
        } while (match(p, KEEL_SQL_TOKEN_COMMA));
    }

    /* ORDER BY sort_key_list */
    if (check_keyword(p, KEEL_SQL_KW_ORDER)) {
        advance(p);
        if (check_keyword(p, KEEL_SQL_KW_BY)) advance(p);
        ws->order_by = keel_sql_list_new(p);
        do {
            keel_sql_node_t* item = parse_order_item(p);
            if (item) keel_sql_list_append(ws->order_by, item);
        } while (match(p, KEEL_SQL_TOKEN_COMMA));
    }

    /* Frame specification: ROWS | RANGE | GROUPS */
    if (check_keyword(p, KEEL_SQL_KW_ROWS) ||
        check_keyword(p, KEEL_SQL_KW_RANGE) ||
        check_keyword(p, KEEL_SQL_KW_GROUPS)) {
        ws->frame_spec = parse_frame_spec(p);
    }

    expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
    return &ws->base;
}

/* ============================================================================
 * CTE Parsing
 * ============================================================================ */

/**
 * @brief Parse a single CTE definition
 * Syntax: name [(column_list)] AS [MATERIALIZED|NOT MATERIALIZED] (query)
 */
static keel_sql_node_t* parse_cte_definition(keel_sql_parser_t* p) {
    keel_sql_cte_t* cte = KEEL_SQL_NODE_NEW(p, keel_sql_cte_t);
    if (!cte) return NULL;
    cte->base.kind = KEEL_SQL_NODE_CLAUSE_CTE;
    set_location(&cte->base, &p->current);

    /* CTE name */
    if (!check(p, KEEL_SQL_TOKEN_IDENT) && !check(p, KEEL_SQL_TOKEN_KEYWORD)) {
        parse_error(p, "expected CTE name");
        return NULL;
    }
    cte->name.data = p->current.text.data;
    cte->name.len  = p->current.text.len;
    advance(p);

    /* Optional column list: (col1, col2, ...) */
    if (check(p, KEEL_SQL_TOKEN_LPAREN)) {
        /* Look ahead: if next non-paren is followed by another ident or comma
         * before a SELECT, this is a column list, not the AS query.
         * Simple heuristic: parse AS keyword must NOT follow immediately. */
        advance(p);
        cte->column_names = keel_sql_list_new(p);
        do {
            if (!check(p, KEEL_SQL_TOKEN_IDENT) && !check(p, KEEL_SQL_TOKEN_KEYWORD)) break;
            keel_sql_expr_column_t* col = KEEL_SQL_NODE_NEW(p, keel_sql_expr_column_t);
            if (col) {
                col->base.kind = KEEL_SQL_NODE_EXPR_COLUMN;
                col->column.data = p->current.text.data;
                col->column.len  = p->current.text.len;
                advance(p);
                keel_sql_list_append(cte->column_names, &col->base);
            }
        } while (match(p, KEEL_SQL_TOKEN_COMMA));
        expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
    }

    /* AS keyword */
    if (!match_keyword(p, KEEL_SQL_KW_AS)) {
        parse_error(p, "expected AS in CTE definition");
        return NULL;
    }

    /* Optional MATERIALIZED / NOT MATERIALIZED */
    if (check_keyword(p, KEEL_SQL_KW_MATERIALIZED)) {
        cte->materialized = true;
        advance(p);
    } else if (check_keyword(p, KEEL_SQL_KW_NOT)) {
        advance(p);
        if (check_keyword(p, KEEL_SQL_KW_MATERIALIZED)) {
            cte->not_materialized = true;
            advance(p);
        }
    }

    /* (query) */
    if (!expect(p, KEEL_SQL_TOKEN_LPAREN, "(")) return NULL;

    /* Parse the CTE body (SELECT, INSERT, UPDATE, DELETE) */
    if (check_keyword(p, KEEL_SQL_KW_SELECT) || check_keyword(p, KEEL_SQL_KW_WITH)) {
        cte->query = parse_select_stmt(p);
    } else if (check_keyword(p, KEEL_SQL_KW_INSERT)) {
        cte->query = parse_insert_stmt(p);
    } else if (check_keyword(p, KEEL_SQL_KW_UPDATE)) {
        cte->query = parse_update_stmt(p);
    } else if (check_keyword(p, KEEL_SQL_KW_DELETE)) {
        cte->query = parse_delete_stmt(p);
    } else {
        /* Unknown CTE body — skip to matching paren so the rest of the
         * statement still parses; routing will fall back to scatter. */
        int depth = 1;
        while (depth > 0 && !at_end(p)) {
            if (match(p, KEEL_SQL_TOKEN_LPAREN)) depth++;
            else if (check(p, KEEL_SQL_TOKEN_RPAREN)) { depth--; if (depth > 0) advance(p); }
            else advance(p);
        }
    }

    if (!expect(p, KEEL_SQL_TOKEN_RPAREN, ")")) return NULL;

    return &cte->base;
}

/**
 * @brief Parse a structurally rich SELECT statement, including major PostgreSQL extensions.
 */
static keel_sql_node_t* parse_select_stmt(keel_sql_parser_t* p) {
    keel_sql_stmt_select_t* stmt = KEEL_SQL_NODE_NEW(p, keel_sql_stmt_select_t);
    if (!stmt) return NULL;
    
    stmt->base.kind = KEEL_SQL_NODE_STMT_SELECT;
    set_location(&stmt->base, &p->current);
    
    /* WITH clause */
    if (match_keyword(p, KEEL_SQL_KW_WITH)) {
        stmt->with_clause = keel_sql_list_new(p);
        /* Check for RECURSIVE - not in keyword table, check as identifier */
        if ((check(p, KEEL_SQL_TOKEN_IDENT) || check(p, KEEL_SQL_TOKEN_KEYWORD)) &&
            p->current.text.len == 9 &&
            strncasecmp(p->current.text.data, "RECURSIVE", 9) == 0) {
            stmt->with_recursive = true;
            advance(p);
        }
        /* Parse CTE definitions: name [(cols)] AS [MATERIALIZED] (query) [, ...] */
        do {
            keel_sql_node_t* cte = parse_cte_definition(p);
            if (cte) keel_sql_list_append(stmt->with_clause, cte);
            else break;  /* stop on parse error */
        } while (match(p, KEEL_SQL_TOKEN_COMMA));

        /* WITH ... UPDATE: attach CTEs to the UPDATE statement's with_clause. */
        if (check_keyword(p, KEEL_SQL_KW_UPDATE)) {
            keel_sql_node_t* inner = parse_update_stmt(p);
            if (inner) {
                ((keel_sql_stmt_update_t*)inner)->with_clause = stmt->with_clause;
            }
            return inner;
        }

        /* WITH ... DELETE: attach CTEs to the DELETE statement's with_clause. */
        if (check_keyword(p, KEEL_SQL_KW_DELETE)) {
            keel_sql_node_t* inner = parse_delete_stmt(p);
            if (inner) {
                ((keel_sql_stmt_delete_t*)inner)->with_clause = stmt->with_clause;
            }
            return inner;
        }
    }

    /* SELECT keyword */
    if (!match_keyword(p, KEEL_SQL_KW_SELECT)) {
        parse_error(p, "expected SELECT");
        return NULL;
    }
    
    /* DISTINCT */
    if (check_keyword(p, KEEL_SQL_KW_DISTINCT)) {
        stmt->distinct = true;
        advance(p);
        
        /* DISTINCT ON */
        if (check_keyword(p, KEEL_SQL_KW_ON)) {
            advance(p);
            if (match(p, KEEL_SQL_TOKEN_LPAREN)) {
                stmt->distinct_on = keel_sql_list_new(p);
                do {
                    keel_sql_node_t* expr = parse_expression(p);
                    keel_sql_list_append(stmt->distinct_on, expr);
                } while (match(p, KEEL_SQL_TOKEN_COMMA));
                expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
            }
        }
    } else if (match_keyword(p, KEEL_SQL_KW_ALL)) {
        /* ALL is default */
    }
    
    /* Target list */
    stmt->targets = keel_sql_list_new(p);
    do {
        keel_sql_node_t* target = parse_select_target(p);
        if (target) {
            keel_sql_list_append(stmt->targets, target);
        }
    } while (match(p, KEEL_SQL_TOKEN_COMMA));
    
    /* FROM clause */
    stmt->from = parse_from_clause(p);
    
    /* WHERE clause */
    if (match_keyword(p, KEEL_SQL_KW_WHERE)) {
        stmt->where = parse_expression(p);
    }
    
    /* GROUP BY clause */
    if (check_keyword(p, KEEL_SQL_KW_GROUP)) {
        advance(p);
        if (check_keyword(p, KEEL_SQL_KW_BY)) {
            advance(p);
            stmt->group_by = keel_sql_list_new(p);
            do {
                keel_sql_node_t* expr = parse_expression(p);
                keel_sql_list_append(stmt->group_by, expr);
            } while (match(p, KEEL_SQL_TOKEN_COMMA));
        }
    }
    
    /* HAVING clause */
    if (check_keyword(p, KEEL_SQL_KW_HAVING)) {
        advance(p);
        stmt->having = parse_expression(p);
    }

    /* WINDOW clause: WINDOW name AS (window_spec) [, ...] */
    if (check_keyword(p, KEEL_SQL_KW_WINDOW)) {
        advance(p);
        if (!stmt->window) stmt->window = keel_sql_list_new(p);
        do {
            /* window_name AS (window_spec) */
            if (!check(p, KEEL_SQL_TOKEN_IDENT) && !check(p, KEEL_SQL_TOKEN_KEYWORD)) break;
            keel_sql_window_spec_t* ws = KEEL_SQL_NODE_NEW(p, keel_sql_window_spec_t);
            if (!ws) break;
            ws->base.kind   = KEEL_SQL_NODE_CLAUSE_WINDOW_DEF;
            ws->ref_name.data = p->current.text.data;
            ws->ref_name.len  = p->current.text.len;
            advance(p);
            if (!match_keyword(p, KEEL_SQL_KW_AS)) {
                parse_error(p, "expected AS in WINDOW clause");
                break;
            }
            if (!expect(p, KEEL_SQL_TOKEN_LPAREN, "(")) break;
            /* Parse PARTITION BY, ORDER BY, frame spec */
            if (check_keyword(p, KEEL_SQL_KW_PARTITION)) {
                advance(p);
                if (check_keyword(p, KEEL_SQL_KW_BY)) advance(p);
                ws->partition_by = keel_sql_list_new(p);
                do {
                    keel_sql_node_t* expr = parse_expression(p);
                    if (expr) keel_sql_list_append(ws->partition_by, expr);
                } while (match(p, KEEL_SQL_TOKEN_COMMA));
            }
            if (check_keyword(p, KEEL_SQL_KW_ORDER)) {
                advance(p);
                if (check_keyword(p, KEEL_SQL_KW_BY)) advance(p);
                ws->order_by = keel_sql_list_new(p);
                do {
                    keel_sql_node_t* item = parse_order_item(p);
                    if (item) keel_sql_list_append(ws->order_by, item);
                } while (match(p, KEEL_SQL_TOKEN_COMMA));
            }
            if (check_keyword(p, KEEL_SQL_KW_ROWS) ||
                check_keyword(p, KEEL_SQL_KW_RANGE) ||
                check_keyword(p, KEEL_SQL_KW_GROUPS)) {
                ws->frame_spec = parse_frame_spec(p);
            }
            expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
            keel_sql_list_append(stmt->window, &ws->base);
        } while (match(p, KEEL_SQL_TOKEN_COMMA));
    }

    /* ORDER BY clause */
    if (check_keyword(p, KEEL_SQL_KW_ORDER)) {
        advance(p);
        if (check_keyword(p, KEEL_SQL_KW_BY)) {
            advance(p);
            stmt->order_by = keel_sql_list_new(p);
            do {
                keel_sql_node_t* item = parse_order_item(p);
                keel_sql_list_append(stmt->order_by, item);
            } while (match(p, KEEL_SQL_TOKEN_COMMA));
        }
    }
    
    /* LIMIT / OFFSET clause — accept in either order:
     *   LIMIT n [OFFSET m]
     *   OFFSET m [LIMIT n]
     * Both orderings are valid PostgreSQL syntax.
     */
    if (check_keyword(p, KEEL_SQL_KW_LIMIT) || check_keyword(p, KEEL_SQL_KW_OFFSET)) {
        keel_sql_limit_t* limit = KEEL_SQL_NODE_NEW(p, keel_sql_limit_t);
        limit->base.kind = KEEL_SQL_NODE_CLAUSE_LIMIT;

        /* First keyword: LIMIT or OFFSET */
        if (check_keyword(p, KEEL_SQL_KW_LIMIT)) {
            advance(p);
            if (check_keyword(p, KEEL_SQL_KW_ALL)) {
                advance(p); /* LIMIT ALL = no limit */
            } else {
                limit->count = parse_expression(p);
            }
            /* Optional OFFSET after LIMIT */
            if (check_keyword(p, KEEL_SQL_KW_OFFSET)) {
                advance(p);
                limit->offset = parse_expression(p);
            }
        } else {
            /* OFFSET first */
            advance(p); /* consume OFFSET */
            limit->offset = parse_expression(p);
            /* Optional LIMIT after OFFSET */
            if (check_keyword(p, KEEL_SQL_KW_LIMIT)) {
                advance(p);
                if (check_keyword(p, KEEL_SQL_KW_ALL)) {
                    advance(p); /* LIMIT ALL = no limit */
                } else {
                    limit->count = parse_expression(p);
                }
            }
        }

        stmt->limit = &limit->base;
    }
    
    /* FOR UPDATE/SHARE */
    if (match_keyword(p, KEEL_SQL_KW_FOR)) {
        keel_sql_locking_t* lock = KEEL_SQL_NODE_NEW(p, keel_sql_locking_t);
        lock->base.kind = KEEL_SQL_NODE_CLAUSE_LOCKING;
        
        if (match_keyword(p, KEEL_SQL_KW_UPDATE)) {
            lock->mode = KEEL_SQL_LOCK_FOR_UPDATE;
        } else if (check_keyword(p, KEEL_SQL_KW_SHARE)) {
            lock->mode = KEEL_SQL_LOCK_FOR_SHARE;
            advance(p);
        }
        
        /* NOWAIT / SKIP LOCKED */
        if (check_keyword(p, KEEL_SQL_KW_NOWAIT)) {
            lock->nowait = true;
            advance(p);
        } else if (check_keyword(p, KEEL_SQL_KW_SKIP)) {
            advance(p);
            if (check_keyword(p, KEEL_SQL_KW_LOCKED)) {
                lock->skip_locked = true;
                advance(p);
            }
        }
        
        stmt->locking = &lock->base;
    }
    
    /* Represent set operations by storing the left SELECT in this node and the
     * right branch as another SELECT subtree. This keeps the tree compact while
     * still making compound-query semantics visible to later analysis. */
    if (check_keyword(p, KEEL_SQL_KW_UNION) ||
        check_keyword(p, KEEL_SQL_KW_INTERSECT) ||
        check_keyword(p, KEEL_SQL_KW_EXCEPT)) {
        keel_sql_keyword_t op_kw = keel_sql_lookup_keyword(p->current.text);
        advance(p);
        bool is_all = false;
        if (check_keyword(p, KEEL_SQL_KW_ALL)) {
            is_all = true;
            advance(p);
        } else if (check_keyword(p, KEEL_SQL_KW_DISTINCT)) {
            advance(p);
        }
        stmt->set_op = (op_kw == KEEL_SQL_KW_UNION)
            ? (is_all ? KEEL_SQL_SET_UNION_ALL : KEEL_SQL_SET_UNION)
            : (op_kw == KEEL_SQL_KW_INTERSECT)
                ? KEEL_SQL_SET_INTERSECT
                : KEEL_SQL_SET_EXCEPT;
        stmt->set_right = parse_select_stmt(p);
    }

    return &stmt->base;
}
/**
 * @brief Parse an INSERT statement with VALUES, SELECT source, and RETURNING support.
 */
static keel_sql_node_t* parse_insert_stmt(keel_sql_parser_t* p) {
    keel_sql_stmt_insert_t* stmt = KEEL_SQL_NODE_NEW(p, keel_sql_stmt_insert_t);
    if (!stmt) return NULL;
    
    stmt->base.kind = KEEL_SQL_NODE_STMT_INSERT;
    set_location(&stmt->base, &p->current);
    
    if (!match_keyword(p, KEEL_SQL_KW_INSERT)) {
        parse_error(p, "expected INSERT");
        return NULL;
    }
    
    /* INTO (optional in some dialects) */
    if (check_keyword(p, KEEL_SQL_KW_INTO)) {
        advance(p);
    }
    
    /* Target table */
    stmt->table = parse_table_ref(p);
    
    /* Column list (optional) */
    if (match(p, KEEL_SQL_TOKEN_LPAREN)) {
        stmt->columns = keel_sql_list_new(p);
        do {
            if (p->current.type == KEEL_SQL_TOKEN_IDENT ||
                p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
                keel_sql_expr_column_t* col = KEEL_SQL_NODE_NEW(p, keel_sql_expr_column_t);
                col->base.kind = KEEL_SQL_NODE_EXPR_COLUMN;
                col->column = p->current.text;
                keel_sql_list_append(stmt->columns, &col->base);
                advance(p);
            }
        } while (match(p, KEEL_SQL_TOKEN_COMMA));
        expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
    }
    
    /* VALUES or SELECT */
    if (match_keyword(p, KEEL_SQL_KW_VALUES)) {
        /* Parse VALUES list - create a list node to hold value tuples */
        keel_sql_list_t* values = keel_sql_list_new(p);
        do {
            if (!expect(p, KEEL_SQL_TOKEN_LPAREN, "(")) break;
            
            keel_sql_list_t* tuple = keel_sql_list_new(p);
            do {
                keel_sql_node_t* val = parse_expression(p);
                keel_sql_list_append(tuple, val);
            } while (match(p, KEEL_SQL_TOKEN_COMMA));
            
            expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
            keel_sql_list_append(values, &tuple->base);
        } while (match(p, KEEL_SQL_TOKEN_COMMA));
        
        stmt->source = &values->base;
    } else if (check_keyword(p, KEEL_SQL_KW_SELECT) ||
               check_keyword(p, KEEL_SQL_KW_WITH)) {
        stmt->source = parse_select_stmt(p);
    } else if (check_keyword(p, KEEL_SQL_KW_DEFAULT)) {
        advance(p);
        if (match_keyword(p, KEEL_SQL_KW_VALUES)) {
            /* DEFAULT VALUES consumed */
        }
        /* DEFAULT VALUES - no source */
    }
    
    /* ON CONFLICT */
    if (check_keyword(p, KEEL_SQL_KW_ON)) {
        advance(p);
        /* Check for CONFLICT - not in keyword table, check as identifier */
        if ((check(p, KEEL_SQL_TOKEN_IDENT) || check(p, KEEL_SQL_TOKEN_KEYWORD)) &&
            p->current.text.len == 8 &&
            strncasecmp(p->current.text.data, "CONFLICT", 8) == 0) {
            advance(p);
            /* The top-level presence of ON CONFLICT matters more than its exact inner
             * shape for current routing needs, so the parser deliberately skips the
             * remaining clause once detected. */
            int depth = 0;
            while (!at_end(p)) {
                if (match(p, KEEL_SQL_TOKEN_LPAREN)) depth++;
                else if (match(p, KEEL_SQL_TOKEN_RPAREN)) depth--;
                else if (check_keyword(p, KEEL_SQL_KW_RETURNING)) {
                    break;
                } else {
                    advance(p);
                }
                if (depth < 0) break;
            }
        }
    }
    
    /* RETURNING */
    if (check_keyword(p, KEEL_SQL_KW_RETURNING)) {
        advance(p);
        stmt->returning = keel_sql_list_new(p);
        do {
            keel_sql_node_t* expr = parse_expression(p);
            keel_sql_list_append(stmt->returning, expr);
        } while (match(p, KEEL_SQL_TOKEN_COMMA));
    }
    
    return &stmt->base;
}

/**
 * @brief Parse an UPDATE statement including PostgreSQL-style `FROM` and `RETURNING`.
 */
static keel_sql_node_t* parse_update_stmt(keel_sql_parser_t* p) {
    keel_sql_stmt_update_t* stmt = KEEL_SQL_NODE_NEW(p, keel_sql_stmt_update_t);
    if (!stmt) return NULL;
    
    stmt->base.kind = KEEL_SQL_NODE_STMT_UPDATE;
    set_location(&stmt->base, &p->current);
    
    if (!match_keyword(p, KEEL_SQL_KW_UPDATE)) {
        parse_error(p, "expected UPDATE");
        return NULL;
    }
    
    /* Target table */
    stmt->table = parse_table_ref(p);
    
    /* SET clause */
    if (!expect_keyword(p, KEEL_SQL_KW_SET, "SET")) {
        return NULL;
    }
    
    stmt->set_list = keel_sql_list_new(p);
    do {
        keel_sql_set_item_t* item = KEEL_SQL_NODE_NEW(p, keel_sql_set_item_t);
        item->base.kind = KEEL_SQL_NODE_SET_ITEM;
        
        /* Column */
        item->column = parse_column_ref(p);
        
        /* = */
        if (!check(p, KEEL_SQL_TOKEN_OPERATOR) || p->current.text.data[0] != '=') {
            parse_error(p, "expected = in SET clause");
            return NULL;
        }
        advance(p);
        
        /* Value */
        item->value = parse_expression(p);
        
        keel_sql_list_append(stmt->set_list, &item->base);
    } while (match(p, KEEL_SQL_TOKEN_COMMA));
    
    /* FROM clause (PostgreSQL extension) */
    stmt->from = parse_from_clause(p);
    
    /* WHERE clause */
    if (match_keyword(p, KEEL_SQL_KW_WHERE)) {
        stmt->where = parse_expression(p);
    }
    
    /* RETURNING */
    if (check_keyword(p, KEEL_SQL_KW_RETURNING)) {
        advance(p);
        stmt->returning = keel_sql_list_new(p);
        do {
            keel_sql_node_t* expr = parse_expression(p);
            keel_sql_list_append(stmt->returning, expr);
        } while (match(p, KEEL_SQL_TOKEN_COMMA));
    }
    
    return &stmt->base;
}

/**
 * @brief Parse a DELETE statement including PostgreSQL `USING` and `RETURNING` clauses.
 */
static keel_sql_node_t* parse_delete_stmt(keel_sql_parser_t* p) {
    keel_sql_stmt_delete_t* stmt = KEEL_SQL_NODE_NEW(p, keel_sql_stmt_delete_t);
    if (!stmt) return NULL;
    
    stmt->base.kind = KEEL_SQL_NODE_STMT_DELETE;
    set_location(&stmt->base, &p->current);
    
    if (!match_keyword(p, KEEL_SQL_KW_DELETE)) {
        parse_error(p, "expected DELETE");
        return NULL;
    }
    
    /* FROM */
    if (!match_keyword(p, KEEL_SQL_KW_FROM)) {
        parse_error(p, "expected FROM after DELETE");
        return NULL;
    }
    
    /* Target table */
    stmt->table = parse_table_ref(p);
    
    /* USING clause (PostgreSQL) */
    if (check_keyword(p, KEEL_SQL_KW_USING)) {
        advance(p);
        stmt->using = parse_table_ref(p);
        /* Could have multiple tables */
        while (match(p, KEEL_SQL_TOKEN_COMMA)) {
            /* Skip additional tables for now */
            parse_table_ref(p);
        }
    }
    
    /* WHERE clause */
    if (match_keyword(p, KEEL_SQL_KW_WHERE)) {
        stmt->where = parse_expression(p);
    }
    
    /* RETURNING */
    if (check_keyword(p, KEEL_SQL_KW_RETURNING)) {
        advance(p);
        stmt->returning = keel_sql_list_new(p);
        do {
            keel_sql_node_t* expr = parse_expression(p);
            keel_sql_list_append(stmt->returning, expr);
        } while (match(p, KEEL_SQL_TOKEN_COMMA));
    }
    
    return &stmt->base;
}

/**
 * @brief Parse transaction-control statements and selected option clauses.
 */
static keel_sql_node_t* parse_transaction_stmt(keel_sql_parser_t* p, keel_sql_node_kind_t kind) {
    keel_sql_stmt_transaction_t* stmt = KEEL_SQL_NODE_NEW(p, keel_sql_stmt_transaction_t);
    if (!stmt) return NULL;
    
    stmt->base.kind = kind;
    set_location(&stmt->base, &p->current);
    advance(p);  /* Consume BEGIN/COMMIT/ROLLBACK */
    
    /* Transaction options */
    while (!at_end(p)) {
        if (p->current.type != KEEL_SQL_TOKEN_KEYWORD) break;
        
        keel_sql_keyword_t kw = keel_sql_lookup_keyword(p->current.text);
        if (kw == KEEL_SQL_KW_TRANSACTION || kw == KEEL_SQL_KW_WORK) {
            advance(p);
        } else if (kw == KEEL_SQL_KW_READ) {
            advance(p);
            if (check_keyword(p, KEEL_SQL_KW_ONLY)) {
                stmt->read_only = true;
                advance(p);
            } else if (check_keyword(p, KEEL_SQL_KW_WRITE)) {
                stmt->read_only = false;
                advance(p);
            }
        } else if (kw == KEEL_SQL_KW_ISOLATION) {
            advance(p);
            /* Skip LEVEL ... */
            while (!at_end(p) && p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
                advance(p);
            }
        } else if (kw == KEEL_SQL_KW_NOT) {
            advance(p);  /* NOT DEFERRABLE */
        } else if (kw == KEEL_SQL_KW_UNKNOWN) {
            /* DEFERRABLE or other - check as string */
            if (p->current.text.len == 10 &&
                strncasecmp(p->current.text.data, "DEFERRABLE", 10) == 0) {
                advance(p);
            } else {
                break;
            }
        } else {
            break;
        }
    }
    
    /* For ROLLBACK TO savepoint */
    if (kind == KEEL_SQL_NODE_STMT_ROLLBACK) {
        /* Check for TO - not in keyword table */
        if ((check(p, KEEL_SQL_TOKEN_IDENT) || check(p, KEEL_SQL_TOKEN_KEYWORD)) &&
            p->current.text.len == 2 &&
            strncasecmp(p->current.text.data, "TO", 2) == 0) {
            advance(p);
            if (check_keyword(p, KEEL_SQL_KW_SAVEPOINT)) {
                advance(p);
            }
            if (p->current.type == KEEL_SQL_TOKEN_IDENT) {
                stmt->savepoint_name = p->current.text;
                advance(p);
            }
        }
    }
    
    return &stmt->base;
}

/**
 * @brief Dispatch parsing based on the first statement keyword.
 *
 * Many non-core statements intentionally collapse to coarse nodes after skipping
 * their internals. That preserves high-level semantics for routing without paying
 * parser complexity for constructs KEEL does not currently transform deeply.
 */
static keel_sql_node_t* parse_statement(keel_sql_parser_t* p) {
    if (at_end(p)) return NULL;
    
    /* Look at first keyword */
    if (p->current.type != KEEL_SQL_TOKEN_KEYWORD) {
        parse_error(p, "expected SQL statement");
        return NULL;
    }
    
    keel_sql_keyword_t kw = keel_sql_lookup_keyword(p->current.text);
    
    switch (kw) {
    case KEEL_SQL_KW_SELECT:
        return parse_select_stmt(p);
        
    case KEEL_SQL_KW_INSERT:
        return parse_insert_stmt(p);
        
    case KEEL_SQL_KW_UPDATE:
        return parse_update_stmt(p);
        
    case KEEL_SQL_KW_DELETE:
        return parse_delete_stmt(p);
        
    case KEEL_SQL_KW_WITH:
        return parse_select_stmt(p);  /* WITH ... SELECT */
        
    case KEEL_SQL_KW_BEGIN:
    case KEEL_SQL_KW_START:
        return parse_transaction_stmt(p, KEEL_SQL_NODE_STMT_BEGIN);
        
    case KEEL_SQL_KW_COMMIT:
    case KEEL_SQL_KW_END:
        return parse_transaction_stmt(p, KEEL_SQL_NODE_STMT_COMMIT);
        
    case KEEL_SQL_KW_ROLLBACK:
        return parse_transaction_stmt(p, KEEL_SQL_NODE_STMT_ROLLBACK);
        
    case KEEL_SQL_KW_SAVEPOINT:
    case KEEL_SQL_KW_RELEASE:
        return parse_transaction_stmt(p, KEEL_SQL_NODE_STMT_SAVEPOINT);
        
    case KEEL_SQL_KW_SET: {
        keel_sql_stmt_set_t* stmt = KEEL_SQL_NODE_NEW(p, keel_sql_stmt_set_t);
        stmt->base.kind = KEEL_SQL_NODE_STMT_SET;
        set_location(&stmt->base, &p->current);
        advance(p);
        
        /* LOCAL/SESSION */
        if (p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
            if (strncasecmp(p->current.text.data, "LOCAL", 5) == 0) {
                stmt->local = true;
                advance(p);
            } else if (strncasecmp(p->current.text.data, "SESSION", 7) == 0) {
                stmt->session = true;
                advance(p);
            }
        }
        
        /* Name */
        if (p->current.type == KEEL_SQL_TOKEN_IDENT ||
            p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
            stmt->name = p->current.text;
            advance(p);
        }
        
        /* Skip = or TO */
        if ((p->current.type == KEEL_SQL_TOKEN_OPERATOR && p->current.text.data[0] == '=') ||
            (p->current.type == KEEL_SQL_TOKEN_KEYWORD &&
             strncasecmp(p->current.text.data, "TO", 2) == 0)) {
            advance(p);
        }
        
        /* Value */
        stmt->value = parse_expression(p);
        
        return &stmt->base;
    }
        
    case KEEL_SQL_KW_SHOW: {
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = KEEL_SQL_NODE_STMT_SHOW;
        set_location(stmt, &p->current);
        /* Skip to end */
        while (!at_end(p)) advance(p);
        return stmt;
    }
        
    case KEEL_SQL_KW_EXPLAIN: {
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = KEEL_SQL_NODE_STMT_EXPLAIN;
        set_location(stmt, &p->current);
        advance(p);
        /* Skip ANALYZE/VERBOSE etc and parse inner statement */
        while (p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
            keel_sql_keyword_t inner = keel_sql_lookup_keyword(p->current.text);
            if (inner == KEEL_SQL_KW_SELECT || inner == KEEL_SQL_KW_INSERT ||
                inner == KEEL_SQL_KW_UPDATE || inner == KEEL_SQL_KW_DELETE) {
                break;
            }
            advance(p);
        }
        /* Could parse inner statement here */
        while (!at_end(p)) advance(p);
        return stmt;
    }
        
    case KEEL_SQL_KW_PREPARE: {
        keel_sql_stmt_prepare_t* stmt = KEEL_SQL_NODE_NEW(p, keel_sql_stmt_prepare_t);
        stmt->base.kind = KEEL_SQL_NODE_STMT_PREPARE;
        set_location(&stmt->base, &p->current);
        advance(p);
        
        if (p->current.type == KEEL_SQL_TOKEN_IDENT) {
            stmt->name = p->current.text;
            advance(p);
        }
        
        /* Skip AS and parse inner query */
        while (!at_end(p) && p->current.type == KEEL_SQL_TOKEN_KEYWORD) {
            keel_sql_keyword_t inner = keel_sql_lookup_keyword(p->current.text);
            if (inner == KEEL_SQL_KW_SELECT || inner == KEEL_SQL_KW_INSERT ||
                inner == KEEL_SQL_KW_UPDATE || inner == KEEL_SQL_KW_DELETE) {
                stmt->query = parse_statement(p);
                break;
            }
            advance(p);
        }
        return &stmt->base;
    }
        
    case KEEL_SQL_KW_EXECUTE: {
        keel_sql_stmt_execute_t* stmt = KEEL_SQL_NODE_NEW(p, keel_sql_stmt_execute_t);
        stmt->base.kind = KEEL_SQL_NODE_STMT_EXECUTE;
        set_location(&stmt->base, &p->current);
        advance(p);
        
        if (p->current.type == KEEL_SQL_TOKEN_IDENT) {
            stmt->name = p->current.text;
            advance(p);
        }
        
        /* Parameters */
        if (match(p, KEEL_SQL_TOKEN_LPAREN)) {
            stmt->params = keel_sql_list_new(p);
            do {
                keel_sql_node_t* param = parse_expression(p);
                keel_sql_list_append(stmt->params, param);
            } while (match(p, KEEL_SQL_TOKEN_COMMA));
            expect(p, KEEL_SQL_TOKEN_RPAREN, ")");
        }
        return &stmt->base;
    }
        
    case KEEL_SQL_KW_DEALLOCATE: {
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = KEEL_SQL_NODE_STMT_DEALLOCATE;
        set_location(stmt, &p->current);
        while (!at_end(p)) advance(p);
        return stmt;
    }
        
    case KEEL_SQL_KW_CREATE:
    case KEEL_SQL_KW_ALTER:
    case KEEL_SQL_KW_DROP: {
        keel_sql_node_kind_t kind = (kw == KEEL_SQL_KW_CREATE) ? KEEL_SQL_NODE_STMT_CREATE :
                                   (kw == KEEL_SQL_KW_ALTER) ? KEEL_SQL_NODE_STMT_ALTER :
                                   KEEL_SQL_NODE_STMT_DROP;
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = kind;
        set_location(stmt, &p->current);
        /* Skip to end - DDL is complex */
        while (!at_end(p)) advance(p);
        return stmt;
    }
        
    case KEEL_SQL_KW_TRUNCATE: {
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = KEEL_SQL_NODE_STMT_TRUNCATE;
        set_location(stmt, &p->current);
        while (!at_end(p)) advance(p);
        return stmt;
    }
        
    case KEEL_SQL_KW_COPY: {
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = KEEL_SQL_NODE_STMT_COPY;
        set_location(stmt, &p->current);
        while (!at_end(p)) advance(p);
        return stmt;
    }
        
    case KEEL_SQL_KW_CALL: {
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = KEEL_SQL_NODE_STMT_CALL;
        set_location(stmt, &p->current);
        while (!at_end(p)) advance(p);
        return stmt;
    }
        
    case KEEL_SQL_KW_DO: {
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = KEEL_SQL_NODE_STMT_DO;
        set_location(stmt, &p->current);
        while (!at_end(p)) advance(p);
        return stmt;
    }

    case KEEL_SQL_KW_MERGE: {
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = KEEL_SQL_NODE_STMT_MERGE;
        set_location(stmt, &p->current);
        while (!at_end(p)) advance(p);
        return stmt;
    }

    case KEEL_SQL_KW_LOCK: {
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = KEEL_SQL_NODE_STMT_LOCK;
        set_location(stmt, &p->current);
        while (!at_end(p)) advance(p);
        return stmt;
    }

    case KEEL_SQL_KW_LISTEN:
    case KEEL_SQL_KW_NOTIFY: {
        keel_sql_node_kind_t kind = (keel_sql_lookup_keyword(p->current.text) == KEEL_SQL_KW_LISTEN)
                                    ? KEEL_SQL_NODE_STMT_LISTEN
                                    : KEEL_SQL_NODE_STMT_NOTIFY;
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = kind;
        set_location(stmt, &p->current);
        while (!at_end(p)) advance(p);
        return stmt;
    }

    case KEEL_SQL_KW_VACUUM:
    case KEEL_SQL_KW_REINDEX:
    case KEEL_SQL_KW_CLUSTER: {
        keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
        stmt->kind = KEEL_SQL_NODE_STMT_VACUUM;
        set_location(stmt, &p->current);
        while (!at_end(p)) advance(p);
        return stmt;
    }

    default:
        /* Unknown statement - skip to end */
        {
            keel_sql_node_t* stmt = keel_sql_node_alloc(p, sizeof(keel_sql_node_t));
            stmt->kind = KEEL_SQL_NODE_UNKNOWN;
            set_location(stmt, &p->current);
            while (!at_end(p)) advance(p);
            return stmt;
        }
    }
}

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Initialize parser state and prime the current/lookahead token window.
 */
void keel_sql_parser_init(keel_sql_parser_t* parser, keel_str_t sql, keel_arena_t* arena) {
    if (!parser) return;
    
    memset(parser, 0, sizeof(*parser));
    parser->sql = sql;
    parser->arena = arena;
    
    keel_sql_lexer_init(&parser->lexer, sql);
    
    /* Prime the parser with first two tokens */
    keel_sql_lexer_next(&parser->lexer, &parser->current);
    keel_sql_lexer_next(&parser->lexer, &parser->lookahead);
}

/**
 * @brief Parse one statement from the current parser position.
 */
keel_sql_node_t* keel_sql_parse(keel_sql_parser_t* parser) {
    if (!parser) return NULL;
    return parse_statement(parser);
}

/**
 * @brief Parse all remaining statements into one AST list.
 */
keel_sql_list_t* keel_sql_parse_statements(keel_sql_parser_t* parser) {
    if (!parser) return NULL;
    
    keel_sql_list_t* list = keel_sql_list_new(parser);
    
    while (!parser->has_error && parser->current.type != KEEL_SQL_TOKEN_EOF) {
        keel_sql_node_t* stmt = parse_statement(parser);
        if (stmt) {
            keel_sql_list_append(list, stmt);
        }
        
        /* Consume semicolons between statements */
        while (match(parser, KEEL_SQL_TOKEN_SEMICOLON)) {
            /* Skip empty statements */
        }
    }
    
    return list;
}

/**
 * @brief Parse one standalone expression from the current parser position.
 */
keel_sql_node_t* keel_sql_parse_expr(keel_sql_parser_t* parser) {
    if (!parser) return NULL;
    return parse_expression(parser);
}

/* ============================================================================
 * AST Walking
 * ============================================================================ */

const char* keel_sql_node_kind_name(keel_sql_node_kind_t kind) {
    switch (kind) {
    case KEEL_SQL_NODE_UNKNOWN:          return "UNKNOWN";
    case KEEL_SQL_NODE_STMT_SELECT:      return "SELECT";
    case KEEL_SQL_NODE_STMT_INSERT:      return "INSERT";
    case KEEL_SQL_NODE_STMT_UPDATE:      return "UPDATE";
    case KEEL_SQL_NODE_STMT_DELETE:      return "DELETE";
    case KEEL_SQL_NODE_STMT_CREATE:      return "CREATE";
    case KEEL_SQL_NODE_STMT_ALTER:       return "ALTER";
    case KEEL_SQL_NODE_STMT_DROP:        return "DROP";
    case KEEL_SQL_NODE_STMT_TRUNCATE:    return "TRUNCATE";
    case KEEL_SQL_NODE_STMT_BEGIN:       return "BEGIN";
    case KEEL_SQL_NODE_STMT_COMMIT:      return "COMMIT";
    case KEEL_SQL_NODE_STMT_ROLLBACK:    return "ROLLBACK";
    case KEEL_SQL_NODE_STMT_SAVEPOINT:   return "SAVEPOINT";
    case KEEL_SQL_NODE_STMT_SET:         return "SET";
    case KEEL_SQL_NODE_STMT_SHOW:        return "SHOW";
    case KEEL_SQL_NODE_STMT_EXPLAIN:     return "EXPLAIN";
    case KEEL_SQL_NODE_STMT_PREPARE:     return "PREPARE";
    case KEEL_SQL_NODE_STMT_EXECUTE:     return "EXECUTE";
    case KEEL_SQL_NODE_STMT_DEALLOCATE:  return "DEALLOCATE";
    case KEEL_SQL_NODE_STMT_COPY:        return "COPY";
    case KEEL_SQL_NODE_STMT_CALL:        return "CALL";
    case KEEL_SQL_NODE_STMT_DO:          return "DO";
    case KEEL_SQL_NODE_STMT_WITH:        return "WITH";
    case KEEL_SQL_NODE_STMT_COMPOUND:    return "COMPOUND";
    case KEEL_SQL_NODE_STMT_MERGE:       return "MERGE";
    case KEEL_SQL_NODE_STMT_LOCK:        return "LOCK";
    case KEEL_SQL_NODE_STMT_LISTEN:      return "LISTEN";
    case KEEL_SQL_NODE_STMT_NOTIFY:      return "NOTIFY";
    case KEEL_SQL_NODE_STMT_VACUUM:      return "VACUUM";
    case KEEL_SQL_NODE_EXPR_LITERAL:     return "LITERAL";
    case KEEL_SQL_NODE_EXPR_COLUMN:      return "COLUMN";
    case KEEL_SQL_NODE_EXPR_STAR:        return "STAR";
    case KEEL_SQL_NODE_EXPR_PARAM:       return "PARAM";
    case KEEL_SQL_NODE_EXPR_UNARY:       return "UNARY";
    case KEEL_SQL_NODE_EXPR_BINARY:      return "BINARY";
    case KEEL_SQL_NODE_EXPR_BETWEEN:     return "BETWEEN";
    case KEEL_SQL_NODE_EXPR_IN:          return "IN";
    case KEEL_SQL_NODE_EXPR_LIKE:        return "LIKE";
    case KEEL_SQL_NODE_EXPR_IS_NULL:     return "IS_NULL";
    case KEEL_SQL_NODE_EXPR_CASE:        return "CASE";
    case KEEL_SQL_NODE_EXPR_CAST:        return "CAST";
    case KEEL_SQL_NODE_EXPR_FUNC:        return "FUNC";
    case KEEL_SQL_NODE_EXPR_AGGR:        return "AGGR";
    case KEEL_SQL_NODE_EXPR_WINDOW:      return "WINDOW";
    case KEEL_SQL_NODE_EXPR_SUBQUERY:    return "SUBQUERY";
    case KEEL_SQL_NODE_EXPR_EXISTS:      return "EXISTS";
    case KEEL_SQL_NODE_EXPR_ARRAY:       return "ARRAY";
    case KEEL_SQL_NODE_EXPR_ROW:         return "ROW";
    case KEEL_SQL_NODE_EXPR_COLLATE:     return "COLLATE";
    case KEEL_SQL_NODE_CLAUSE_FROM:      return "FROM";
    case KEEL_SQL_NODE_CLAUSE_WHERE:     return "WHERE";
    case KEEL_SQL_NODE_CLAUSE_JOIN:      return "JOIN";
    case KEEL_SQL_NODE_CLAUSE_GROUP:     return "GROUP";
    case KEEL_SQL_NODE_CLAUSE_HAVING:    return "HAVING";
    case KEEL_SQL_NODE_CLAUSE_ORDER:     return "ORDER";
    case KEEL_SQL_NODE_CLAUSE_LIMIT:     return "LIMIT";
    case KEEL_SQL_NODE_CLAUSE_RETURNING: return "RETURNING";
    case KEEL_SQL_NODE_CLAUSE_ON_CONFLICT: return "ON_CONFLICT";
    case KEEL_SQL_NODE_CLAUSE_WINDOW_DEF: return "WINDOW_DEF";
    case KEEL_SQL_NODE_CLAUSE_CTE:       return "CTE";
    case KEEL_SQL_NODE_CLAUSE_LOCKING:   return "LOCKING";
    case KEEL_SQL_NODE_TABLE_REF:        return "TABLE_REF";
    case KEEL_SQL_NODE_TABLE_ALIAS:      return "TABLE_ALIAS";
    case KEEL_SQL_NODE_TABLE_SUBQUERY:   return "TABLE_SUBQUERY";
    case KEEL_SQL_NODE_TABLE_FUNC:       return "TABLE_FUNC";
    case KEEL_SQL_NODE_TABLE_JOIN:       return "TABLE_JOIN";
    case KEEL_SQL_NODE_TABLE_LATERAL:    return "LATERAL";
    case KEEL_SQL_NODE_TYPE:             return "TYPE";
    case KEEL_SQL_NODE_LIST:             return "LIST";
    default:                            return "?";
    }
}

/**
 * @brief Traverse an AST subtree depth-first using node-kind-specific child expansion.
 */
void keel_sql_ast_walk(keel_sql_node_t* root, keel_sql_visitor_fn visitor, void* ctx) {
    if (!root || !visitor) return;
    
    keel_sql_visit_result_t result = visitor(root, ctx);
    if (result == KEEL_SQL_VISIT_STOP) return;
    if (result == KEEL_SQL_VISIT_SKIP_CHILDREN) return;
    
    /* Visit children based on node type */
    switch (root->kind) {
    case KEEL_SQL_NODE_STMT_SELECT: {
        keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)root;
        if (sel->with_clause) keel_sql_ast_walk(&sel->with_clause->base, visitor, ctx);
        if (sel->targets) keel_sql_ast_walk(&sel->targets->base, visitor, ctx);
        if (sel->from) keel_sql_ast_walk(sel->from, visitor, ctx);
        if (sel->where) keel_sql_ast_walk(sel->where, visitor, ctx);
        if (sel->group_by) keel_sql_ast_walk(&sel->group_by->base, visitor, ctx);
        if (sel->having) keel_sql_ast_walk(sel->having, visitor, ctx);
        if (sel->order_by) keel_sql_ast_walk(&sel->order_by->base, visitor, ctx);
        if (sel->limit) keel_sql_ast_walk(sel->limit, visitor, ctx);
        if (sel->locking) keel_sql_ast_walk(sel->locking, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_STMT_INSERT: {
        keel_sql_stmt_insert_t* ins = (keel_sql_stmt_insert_t*)root;
        if (ins->table) keel_sql_ast_walk(ins->table, visitor, ctx);
        if (ins->columns) keel_sql_ast_walk(&ins->columns->base, visitor, ctx);
        if (ins->source) keel_sql_ast_walk(ins->source, visitor, ctx);
        if (ins->returning) keel_sql_ast_walk(&ins->returning->base, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_STMT_UPDATE: {
        keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)root;
        if (upd->table) keel_sql_ast_walk(upd->table, visitor, ctx);
        if (upd->set_list) keel_sql_ast_walk(&upd->set_list->base, visitor, ctx);
        if (upd->from) keel_sql_ast_walk(upd->from, visitor, ctx);
        if (upd->where) keel_sql_ast_walk(upd->where, visitor, ctx);
        if (upd->returning) keel_sql_ast_walk(&upd->returning->base, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_STMT_DELETE: {
        keel_sql_stmt_delete_t* del = (keel_sql_stmt_delete_t*)root;
        if (del->table) keel_sql_ast_walk(del->table, visitor, ctx);
        if (del->using) keel_sql_ast_walk(del->using, visitor, ctx);
        if (del->where) keel_sql_ast_walk(del->where, visitor, ctx);
        if (del->returning) keel_sql_ast_walk(&del->returning->base, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_EXPR_UNARY: {
        keel_sql_expr_unary_t* un = (keel_sql_expr_unary_t*)root;
        if (un->operand) keel_sql_ast_walk(un->operand, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_EXPR_BINARY: {
        keel_sql_expr_binary_t* bin = (keel_sql_expr_binary_t*)root;
        if (bin->left) keel_sql_ast_walk(bin->left, visitor, ctx);
        if (bin->right) keel_sql_ast_walk(bin->right, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_SET_ITEM: {
        keel_sql_set_item_t* item = (keel_sql_set_item_t*)root;
        if (item->column) keel_sql_ast_walk(item->column, visitor, ctx);
        if (item->value) keel_sql_ast_walk(item->value, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_SELECT_TARGET: {
        keel_sql_select_target_t* tgt = (keel_sql_select_target_t*)root;
        if (tgt->expr) keel_sql_ast_walk(tgt->expr, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_ORDER_ITEM: {
        keel_sql_order_item_t* ord = (keel_sql_order_item_t*)root;
        if (ord->expr) keel_sql_ast_walk(ord->expr, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_EXPR_FUNC: {
        keel_sql_expr_func_t* fn = (keel_sql_expr_func_t*)root;
        if (fn->args) keel_sql_ast_walk(&fn->args->base, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_EXPR_SUBQUERY: {
        keel_sql_expr_subquery_t* sq = (keel_sql_expr_subquery_t*)root;
        if (sq->select) keel_sql_ast_walk(sq->select, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_TABLE_JOIN: {
        keel_sql_join_t* join = (keel_sql_join_t*)root;
        if (join->left) keel_sql_ast_walk(join->left, visitor, ctx);
        if (join->right) keel_sql_ast_walk(join->right, visitor, ctx);
        if (join->on_clause) keel_sql_ast_walk(join->on_clause, visitor, ctx);
        break;
    }
    case KEEL_SQL_NODE_LIST: {
        keel_sql_list_t* list = (keel_sql_list_t*)root;
        for (keel_sql_node_t* item = list->head; item; item = item->next) {
            keel_sql_ast_walk(item, visitor, ctx);
        }
        break;
    }
    default:
        /* Leaf nodes or unknown */
        break;
    }
    
    /* Visit siblings */
    if (root->next) {
        keel_sql_ast_walk(root->next, visitor, ctx);
    }
}
