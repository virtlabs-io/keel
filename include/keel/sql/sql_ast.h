/**
 * @file sql_ast.h
 * @brief AST types and parser interfaces for KEEL's structural SQL analysis layer.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The AST is KEEL's syntactic middle layer between raw token streams and the
 * higher-level Query Tree. It is not meant to encode every dialect corner case;
 * instead it models the subset of structure that is valuable for routing,
 * session-dirtiness detection, cache invalidation, and future SQL rewriting.
 *
 * The parser is intentionally pragmatic:
 *
 * - common DML and transaction statements are parsed structurally;
 * - many complex or less performance-critical statements fall back to partial
 *   nodes that preserve statement kind even when full inner structure is skipped;
 * - all allocations come from an arena so parse teardown is O(1) at the call
 *   site even for large statements.
 */

#ifndef KEEL_SQL_AST_H
#define KEEL_SQL_AST_H

#include "keel_types.h"
#include "keel_error.h"
#include "keel/mem/mem.h"
#include "keel/sql/sql.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Forward Declarations
 * ============================================================================ */

typedef struct keel_sql_node keel_sql_node_t;
typedef struct keel_sql_expr keel_sql_expr_t;
typedef struct keel_sql_stmt keel_sql_stmt_t;

/* ============================================================================
 * Node Type Enumeration
 * ============================================================================ */

/**
 * @brief AST node type categories
 */
typedef enum keel_sql_node_kind {
    KEEL_SQL_NODE_UNKNOWN = 0,
    
    /* Statement nodes */
    KEEL_SQL_NODE_STMT_SELECT,
    KEEL_SQL_NODE_STMT_INSERT,
    KEEL_SQL_NODE_STMT_UPDATE,
    KEEL_SQL_NODE_STMT_DELETE,
    KEEL_SQL_NODE_STMT_CREATE,
    KEEL_SQL_NODE_STMT_ALTER,
    KEEL_SQL_NODE_STMT_DROP,
    KEEL_SQL_NODE_STMT_TRUNCATE,
    KEEL_SQL_NODE_STMT_BEGIN,
    KEEL_SQL_NODE_STMT_COMMIT,
    KEEL_SQL_NODE_STMT_ROLLBACK,
    KEEL_SQL_NODE_STMT_SAVEPOINT,
    KEEL_SQL_NODE_STMT_SET,
    KEEL_SQL_NODE_STMT_SHOW,
    KEEL_SQL_NODE_STMT_EXPLAIN,
    KEEL_SQL_NODE_STMT_PREPARE,
    KEEL_SQL_NODE_STMT_EXECUTE,
    KEEL_SQL_NODE_STMT_DEALLOCATE,
    KEEL_SQL_NODE_STMT_COPY,
    KEEL_SQL_NODE_STMT_CALL,
    KEEL_SQL_NODE_STMT_DO,
    KEEL_SQL_NODE_STMT_WITH,         /**< Common Table Expression (CTE) */
    KEEL_SQL_NODE_STMT_COMPOUND,     /**< Multiple statements */
    KEEL_SQL_NODE_STMT_MERGE,        /**< MERGE / UPSERT */
    KEEL_SQL_NODE_STMT_LOCK,         /**< LOCK TABLE */
    KEEL_SQL_NODE_STMT_LISTEN,       /**< LISTEN / UNLISTEN */
    KEEL_SQL_NODE_STMT_NOTIFY,       /**< NOTIFY */
    KEEL_SQL_NODE_STMT_VACUUM,       /**< VACUUM, REINDEX, CLUSTER */
    
    /* Expression nodes */
    KEEL_SQL_NODE_EXPR_LITERAL,      /**< String, number, boolean, null */
    KEEL_SQL_NODE_EXPR_COLUMN,       /**< Column reference: col, t.col, s.t.col */
    KEEL_SQL_NODE_EXPR_STAR,         /**< * or t.* */
    KEEL_SQL_NODE_EXPR_PARAM,        /**< $1, $2, ? (placeholder) */
    KEEL_SQL_NODE_EXPR_UNARY,        /**< NOT, -, + */
    KEEL_SQL_NODE_EXPR_BINARY,       /**< +, -, *, /, AND, OR, =, <, etc. */
    KEEL_SQL_NODE_EXPR_BETWEEN,      /**< BETWEEN ... AND ... */
    KEEL_SQL_NODE_EXPR_IN,           /**< IN (list) or IN (subquery) */
    KEEL_SQL_NODE_EXPR_LIKE,         /**< LIKE, ILIKE, SIMILAR TO */
    KEEL_SQL_NODE_EXPR_IS_NULL,      /**< IS NULL, IS NOT NULL */
    KEEL_SQL_NODE_EXPR_CASE,         /**< CASE WHEN ... THEN ... END */
    KEEL_SQL_NODE_EXPR_CAST,         /**< CAST(x AS type), x::type */
    KEEL_SQL_NODE_EXPR_FUNC,         /**< Function call */
    KEEL_SQL_NODE_EXPR_AGGR,         /**< Aggregate function (COUNT, SUM, etc.) */
    KEEL_SQL_NODE_EXPR_WINDOW,       /**< Window function */
    KEEL_SQL_NODE_EXPR_SUBQUERY,     /**< Scalar subquery */
    KEEL_SQL_NODE_EXPR_EXISTS,       /**< EXISTS (subquery) */
    KEEL_SQL_NODE_EXPR_ARRAY,        /**< ARRAY[...] or PostgreSQL arrays */
    KEEL_SQL_NODE_EXPR_ROW,          /**< ROW(...) or composite */
    KEEL_SQL_NODE_EXPR_COLLATE,      /**< COLLATE clause */
    
    /* Clause nodes */
    KEEL_SQL_NODE_CLAUSE_FROM,       /**< FROM clause */
    KEEL_SQL_NODE_CLAUSE_WHERE,      /**< WHERE clause */
    KEEL_SQL_NODE_CLAUSE_JOIN,       /**< JOIN clause */
    KEEL_SQL_NODE_CLAUSE_GROUP,      /**< GROUP BY clause */
    KEEL_SQL_NODE_CLAUSE_HAVING,     /**< HAVING clause */
    KEEL_SQL_NODE_CLAUSE_ORDER,      /**< ORDER BY clause */
    KEEL_SQL_NODE_CLAUSE_LIMIT,      /**< LIMIT/OFFSET or FETCH/OFFSET */
    KEEL_SQL_NODE_CLAUSE_RETURNING,  /**< RETURNING clause */
    KEEL_SQL_NODE_CLAUSE_ON_CONFLICT,/**< ON CONFLICT (upsert) */
    KEEL_SQL_NODE_CLAUSE_WINDOW_DEF, /**< Window definition */
    KEEL_SQL_NODE_CLAUSE_CTE,        /**< Single CTE definition */
    KEEL_SQL_NODE_CLAUSE_LOCKING,    /**< FOR UPDATE/SHARE */
    KEEL_SQL_NODE_WINDOW_SPEC,       /**< OVER (partition order frame) spec */
    KEEL_SQL_NODE_FRAME_SPEC,        /**< Window frame specification */
    KEEL_SQL_NODE_SET_ITEM,          /**< SET clause item (col = value) */
    KEEL_SQL_NODE_SELECT_TARGET,     /**< SELECT list item (expr AS alias) */
    KEEL_SQL_NODE_ORDER_ITEM,        /**< ORDER BY item (expr ASC/DESC) */
    
    /* Table reference nodes */
    KEEL_SQL_NODE_TABLE_REF,         /**< Simple table reference */
    KEEL_SQL_NODE_TABLE_ALIAS,       /**< Table with alias */
    KEEL_SQL_NODE_TABLE_SUBQUERY,    /**< Subquery as table */
    KEEL_SQL_NODE_TABLE_FUNC,        /**< Table-valued function */
    KEEL_SQL_NODE_TABLE_JOIN,        /**< Joined tables */
    KEEL_SQL_NODE_TABLE_LATERAL,     /**< LATERAL subquery */
    
    /* Type nodes */
    KEEL_SQL_NODE_TYPE,              /**< Data type specification */
    
    /* List nodes */
    KEEL_SQL_NODE_LIST,              /**< Generic list of nodes */
    
} keel_sql_node_kind_t;

/* ============================================================================
 * Expression Types
 * ============================================================================ */

/**
 * @brief Literal value types
 */
typedef enum keel_sql_literal_type {
    KEEL_SQL_LIT_NULL = 0,
    KEEL_SQL_LIT_BOOL,
    KEEL_SQL_LIT_INT,
    KEEL_SQL_LIT_FLOAT,
    KEEL_SQL_LIT_STRING,
    KEEL_SQL_LIT_BYTEA,          /**< Binary data */
    KEEL_SQL_LIT_INTERVAL,
    KEEL_SQL_LIT_TIMESTAMP,
    KEEL_SQL_LIT_DATE,
    KEEL_SQL_LIT_TIME,
    KEEL_SQL_LIT_JSON,
    KEEL_SQL_LIT_UUID,
} keel_sql_literal_type_t;

/**
 * @brief Binary operator types
 */
typedef enum keel_sql_binop {
    /* Arithmetic */
    KEEL_SQL_BINOP_ADD = 0,      /**< + */
    KEEL_SQL_BINOP_SUB,          /**< - */
    KEEL_SQL_BINOP_MUL,          /**< * */
    KEEL_SQL_BINOP_DIV,          /**< / */
    KEEL_SQL_BINOP_MOD,          /**< % or MOD */
    KEEL_SQL_BINOP_EXP,          /**< ^ (PostgreSQL) or ** */
    
    /* Comparison */
    KEEL_SQL_BINOP_EQ,           /**< = */
    KEEL_SQL_BINOP_NE,           /**< <> or != */
    KEEL_SQL_BINOP_LT,           /**< < */
    KEEL_SQL_BINOP_LE,           /**< <= */
    KEEL_SQL_BINOP_GT,           /**< > */
    KEEL_SQL_BINOP_GE,           /**< >= */
    
    /* Logical */
    KEEL_SQL_BINOP_AND,          /**< AND */
    KEEL_SQL_BINOP_OR,           /**< OR */
    
    /* String */
    KEEL_SQL_BINOP_CONCAT,       /**< || */
    KEEL_SQL_BINOP_REGEX,        /**< ~ (regex match) */
    KEEL_SQL_BINOP_REGEX_I,      /**< ~* (case-insensitive regex) */
    
    /* Bit */
    KEEL_SQL_BINOP_BITAND,       /**< & */
    KEEL_SQL_BINOP_BITOR,        /**< | */
    KEEL_SQL_BINOP_BITXOR,       /**< # (PostgreSQL) */
    KEEL_SQL_BINOP_LSHIFT,       /**< << */
    KEEL_SQL_BINOP_RSHIFT,       /**< >> */
    
    /* JSON */
    KEEL_SQL_BINOP_JSON_GET,     /**< -> */
    KEEL_SQL_BINOP_JSON_GET_TEXT,/**< ->> */
    KEEL_SQL_BINOP_JSON_PATH,    /**< #> */
    KEEL_SQL_BINOP_JSON_PATH_TEXT,/**< #>> */
    KEEL_SQL_BINOP_JSON_CONTAINS,/**< @> */
    KEEL_SQL_BINOP_JSON_CONTAINED,/**< <@ */
    
    /* Array */
    KEEL_SQL_BINOP_ARRAY_CAT,    /**< || for arrays */
    KEEL_SQL_BINOP_ARRAY_ELEM,   /**< [] subscript */
    
    /* Range */
    KEEL_SQL_BINOP_OVERLAP,      /**< && */
    
} keel_sql_binop_t;

/**
 * @brief Unary operator types
 */
typedef enum keel_sql_unop {
    KEEL_SQL_UNOP_NOT = 0,       /**< NOT */
    KEEL_SQL_UNOP_NEG,           /**< - (negation) */
    KEEL_SQL_UNOP_POS,           /**< + (positive) */
    KEEL_SQL_UNOP_BITNOT,        /**< ~ (bitwise NOT) */
    KEEL_SQL_UNOP_SQRT,          /**< |/ (square root) */
    KEEL_SQL_UNOP_CBRT,          /**< ||/ (cube root) */
    KEEL_SQL_UNOP_FACTORIAL,     /**< ! or !! */
    KEEL_SQL_UNOP_ABS,           /**< @ (absolute value) */
} keel_sql_unop_t;

/**
 * @brief JOIN types
 */
typedef enum keel_sql_join_type {
    KEEL_SQL_JOIN_INNER = 0,
    KEEL_SQL_JOIN_LEFT,
    KEEL_SQL_JOIN_RIGHT,
    KEEL_SQL_JOIN_FULL,
    KEEL_SQL_JOIN_CROSS,
    KEEL_SQL_JOIN_NATURAL,       /**< Modifier: NATURAL JOIN */
    KEEL_SQL_JOIN_LATERAL,       /**< Modifier: LATERAL */
} keel_sql_join_type_t;

/**
 * @brief ORDER BY direction
 */
typedef enum keel_sql_order_dir {
    KEEL_SQL_ORDER_ASC = 0,
    KEEL_SQL_ORDER_DESC,
} keel_sql_order_dir_t;

/**
 * @brief NULL ordering
 */
typedef enum keel_sql_null_order {
    KEEL_SQL_NULLS_DEFAULT = 0,
    KEEL_SQL_NULLS_FIRST,
    KEEL_SQL_NULLS_LAST,
} keel_sql_null_order_t;

/**
 * @brief Locking mode (FOR UPDATE/SHARE)
 */
typedef enum keel_sql_lock_mode {
    KEEL_SQL_LOCK_NONE = 0,
    KEEL_SQL_LOCK_FOR_UPDATE,
    KEEL_SQL_LOCK_FOR_NO_KEY_UPDATE,
    KEEL_SQL_LOCK_FOR_SHARE,
    KEEL_SQL_LOCK_FOR_KEY_SHARE,
} keel_sql_lock_mode_t;

/**
 * @brief Set operation types (UNION, INTERSECT, EXCEPT)
 */
typedef enum keel_sql_set_op {
    KEEL_SQL_SET_NONE = 0,
    KEEL_SQL_SET_UNION,
    KEEL_SQL_SET_UNION_ALL,
    KEEL_SQL_SET_INTERSECT,
    KEEL_SQL_SET_INTERSECT_ALL,
    KEEL_SQL_SET_EXCEPT,
    KEEL_SQL_SET_EXCEPT_ALL,
} keel_sql_set_op_t;

/* ============================================================================
 * AST Node Structures
 * ============================================================================ */

/**
 * @brief Location in source SQL
 */
typedef struct keel_sql_location {
    size_t offset;              /**< Byte offset in SQL text */
    size_t length;              /**< Length of source text */
} keel_sql_location_t;

/**
 * @brief Base node structure (common to all nodes)
 */
struct keel_sql_node {
    keel_sql_node_kind_t kind;   /**< Node type */
    keel_sql_location_t  loc;    /**< Source location */
    keel_sql_node_t*     next;   /**< For linked lists (e.g., columns) */
};

/**
 * @brief Node list
 */
typedef struct keel_sql_list {
    keel_sql_node_t  base;       /**< Base node */
    keel_sql_node_t* head;       /**< First item */
    keel_sql_node_t* tail;       /**< Last item (for O(1) append) */
    size_t          count;      /**< Number of items */
} keel_sql_list_t;

/* ============================================================================
 * Expression Nodes
 * ============================================================================ */

/**
 * @brief Literal value expression
 */
typedef struct keel_sql_expr_literal {
    keel_sql_node_t          base;
    keel_sql_literal_type_t  lit_type;
    union {
        bool                bool_val;
        int64_t             int_val;
        double              float_val;
        keel_str_t           str_val;    /**< String/JSON/UUID */
    } value;
} keel_sql_expr_literal_t;

/**
 * @brief Column reference expression
 *
 * Represents: column, table.column, schema.table.column
 */
typedef struct keel_sql_expr_column {
    keel_sql_node_t  base;
    keel_str_t       schema;         /**< Schema name (optional) */
    keel_str_t       table;          /**< Table name or alias (optional) */
    keel_str_t       column;         /**< Column name */
} keel_sql_expr_column_t;

/**
 * @brief Star (wildcard) expression
 *
 * Represents: *, table.*
 */
typedef struct keel_sql_expr_star {
    keel_sql_node_t  base;
    keel_str_t       table;          /**< Table name (optional for *) */
} keel_sql_expr_star_t;

/**
 * @brief Parameter expression
 *
 * Represents: $1, $2 (PostgreSQL), ? (MySQL)
 */
typedef struct keel_sql_expr_param {
    keel_sql_node_t  base;
    int             index;          /**< Parameter index (1-based) */
} keel_sql_expr_param_t;

/**
 * @brief Unary expression
 */
typedef struct keel_sql_expr_unary {
    keel_sql_node_t  base;
    keel_sql_unop_t  op;
    keel_sql_node_t* operand;
} keel_sql_expr_unary_t;

/**
 * @brief Binary expression
 */
typedef struct keel_sql_expr_binary {
    keel_sql_node_t  base;
    keel_sql_binop_t op;
    keel_sql_node_t* left;
    keel_sql_node_t* right;
} keel_sql_expr_binary_t;

/**
 * @brief BETWEEN expression
 */
typedef struct keel_sql_expr_between {
    keel_sql_node_t  base;
    keel_sql_node_t* expr;           /**< Value to test */
    keel_sql_node_t* low;            /**< Lower bound */
    keel_sql_node_t* high;           /**< Upper bound */
    bool            negated;        /**< NOT BETWEEN */
    bool            symmetric;      /**< SYMMETRIC */
} keel_sql_expr_between_t;

/**
 * @brief IN expression
 */
typedef struct keel_sql_expr_in {
    keel_sql_node_t  base;
    keel_sql_node_t* expr;           /**< Value to test */
    keel_sql_list_t* list;           /**< List of values, or NULL for subquery */
    keel_sql_node_t* subquery;       /**< Subquery, or NULL for list */
    bool            negated;        /**< NOT IN */
} keel_sql_expr_in_t;

/**
 * @brief LIKE expression
 */
typedef struct keel_sql_expr_like {
    keel_sql_node_t  base;
    keel_sql_node_t* expr;           /**< Value to test */
    keel_sql_node_t* pattern;        /**< Pattern */
    keel_sql_node_t* escape;         /**< ESCAPE character (optional) */
    bool            negated;        /**< NOT LIKE */
    bool            icase;          /**< ILIKE (case-insensitive) */
} keel_sql_expr_like_t;

/**
 * @brief IS NULL expression
 */
typedef struct keel_sql_expr_is_null {
    keel_sql_node_t  base;
    keel_sql_node_t* expr;
    bool            negated;        /**< IS NOT NULL */
} keel_sql_expr_is_null_t;

/**
 * @brief CASE expression
 */
typedef struct keel_sql_expr_case {
    keel_sql_node_t  base;
    keel_sql_node_t* operand;        /**< CASE operand (optional for searched CASE) */
    keel_sql_list_t* when_clauses;   /**< List of WHEN clauses */
    keel_sql_node_t* else_result;    /**< ELSE result (optional) */
} keel_sql_expr_case_t;

/**
 * @brief WHEN clause (for CASE)
 */
typedef struct keel_sql_when_clause {
    keel_sql_node_t  base;
    keel_sql_node_t* condition;      /**< WHEN condition */
    keel_sql_node_t* result;         /**< THEN result */
} keel_sql_when_clause_t;

/**
 * @brief CAST expression
 */
typedef struct keel_sql_expr_cast {
    keel_sql_node_t  base;
    keel_sql_node_t* expr;           /**< Expression to cast */
    keel_sql_node_t* target_type;    /**< Target type */
} keel_sql_expr_cast_t;

/**
 * @brief Function call expression
 */
typedef struct keel_sql_expr_func {
    keel_sql_node_t  base;
    keel_str_t       schema;         /**< Schema name (optional) */
    keel_str_t       name;           /**< Function name */
    keel_sql_list_t* args;           /**< Argument list */
    bool            distinct;       /**< DISTINCT in args */
    keel_sql_node_t* filter;         /**< FILTER clause (optional) */
    keel_sql_node_t* over;           /**< OVER clause (optional, makes it window fn) */
    keel_sql_node_t* order_by;       /**< ORDER BY within aggregate */
} keel_sql_expr_func_t;

/**
 * @brief Subquery expression (scalar subquery)
 */
typedef struct keel_sql_expr_subquery {
    keel_sql_node_t  base;
    keel_sql_node_t* select;         /**< SELECT statement */
} keel_sql_expr_subquery_t;

/**
 * @brief EXISTS expression
 */
typedef struct keel_sql_expr_exists {
    keel_sql_node_t  base;
    keel_sql_node_t* subquery;       /**< SELECT statement */
    bool            negated;        /**< NOT EXISTS */
} keel_sql_expr_exists_t;

/**
 * @brief Aliased expression (for select list, CTEs)
 */
typedef struct keel_sql_expr_alias {
    keel_sql_node_t  base;
    keel_sql_node_t* expr;           /**< Expression */
    keel_str_t       alias;          /**< AS alias */
} keel_sql_expr_alias_t;

/* ============================================================================
 * Table Reference Nodes
 * ============================================================================ */

/**
 * @brief Simple table reference
 */
typedef struct keel_sql_table_ref {
    keel_sql_node_t  base;
    keel_str_t       catalog;        /**< Catalog name (optional) */
    keel_str_t       schema;         /**< Schema name (optional) */
    keel_str_t       table;          /**< Table name */
    keel_str_t       alias;          /**< Table alias (optional) */
    keel_sql_list_t* column_aliases; /**< Column aliases (optional) */
} keel_sql_table_ref_t;

/**
 * @brief Subquery as table source
 */
typedef struct keel_sql_table_subquery {
    keel_sql_node_t  base;
    keel_sql_node_t* subquery;       /**< SELECT statement */
    keel_str_t       alias;          /**< Required alias */
    keel_sql_list_t* column_aliases; /**< Column aliases (optional) */
    bool            lateral;        /**< LATERAL subquery */
} keel_sql_table_subquery_t;

/**
 * @brief JOIN node
 */
typedef struct keel_sql_join {
    keel_sql_node_t      base;
    keel_sql_join_type_t join_type;
    keel_sql_node_t*     left;       /**< Left table/join */
    keel_sql_node_t*     right;      /**< Right table/join */
    keel_sql_node_t*     on_clause;  /**< ON condition */
    keel_sql_list_t*     using_cols; /**< USING columns */
    bool                natural;    /**< NATURAL join */
} keel_sql_join_t;

/* ============================================================================
 * Clause Nodes
 * ============================================================================ */

/**
 * @brief SELECT target (column in select list)
 */
typedef struct keel_sql_select_target {
    keel_sql_node_t  base;
    keel_sql_node_t* expr;           /**< Expression */
    keel_str_t       alias;          /**< AS alias (optional) */
} keel_sql_select_target_t;

/**
 * @brief ORDER BY item
 */
typedef struct keel_sql_order_item {
    keel_sql_node_t      base;
    keel_sql_node_t*     expr;       /**< Sort expression */
    keel_sql_order_dir_t direction;  /**< ASC/DESC */
    keel_sql_null_order_t nulls;     /**< NULLS FIRST/LAST */
} keel_sql_order_item_t;

/**
 * @brief GROUP BY item
 */
typedef struct keel_sql_group_item {
    keel_sql_node_t  base;
    keel_sql_node_t* expr;           /**< Grouping expression */
    /* For GROUPING SETS, CUBE, ROLLUP - flags/lists would go here */
} keel_sql_group_item_t;

/**
 * @brief LIMIT/OFFSET clause
 */
typedef struct keel_sql_limit {
    keel_sql_node_t  base;
    keel_sql_node_t* count;          /**< LIMIT value (NULL for no limit) */
    keel_sql_node_t* offset;         /**< OFFSET value (NULL for no offset) */
    bool            with_ties;      /**< FETCH ... WITH TIES */
    bool            percent;        /**< FETCH ... PERCENT (SQL Server) */
} keel_sql_limit_t;

/**
 * @brief FOR UPDATE/SHARE locking clause
 */
typedef struct keel_sql_locking {
    keel_sql_node_t      base;
    keel_sql_lock_mode_t mode;       /**< FOR UPDATE/SHARE */
    keel_sql_list_t*     tables;     /**< OF table list (optional) */
    bool                nowait;     /**< NOWAIT */
    bool                skip_locked;/**< SKIP LOCKED */
} keel_sql_locking_t;

/* ============================================================================
 * Window Function Nodes
 * ============================================================================ */

/**
 * @brief Window frame bound type
 */
typedef enum keel_sql_frame_bound {
    KEEL_SQL_FRAME_UNBOUNDED_PRECEDING = 0,
    KEEL_SQL_FRAME_OFFSET_PRECEDING,
    KEEL_SQL_FRAME_CURRENT_ROW,
    KEEL_SQL_FRAME_OFFSET_FOLLOWING,
    KEEL_SQL_FRAME_UNBOUNDED_FOLLOWING,
} keel_sql_frame_bound_t;

/**
 * @brief Window frame mode
 */
typedef enum keel_sql_frame_mode {
    KEEL_SQL_FRAME_ROWS   = 0,
    KEEL_SQL_FRAME_RANGE,
    KEEL_SQL_FRAME_GROUPS,
} keel_sql_frame_mode_t;

/**
 * @brief Window frame exclusion (PostgreSQL extension)
 */
typedef enum keel_sql_frame_exclusion {
    KEEL_SQL_FRAME_EXCL_NONE        = 0,
    KEEL_SQL_FRAME_EXCL_CURRENT_ROW,
    KEEL_SQL_FRAME_EXCL_GROUP,
    KEEL_SQL_FRAME_EXCL_TIES,
} keel_sql_frame_exclusion_t;

/**
 * @brief Window frame specification
 * Represents: ROWS/RANGE/GROUPS BETWEEN start AND end [EXCLUDE ...]
 */
typedef struct keel_sql_frame_spec {
    keel_sql_node_t          base;
    keel_sql_frame_mode_t    mode;          /**< ROWS / RANGE / GROUPS */
    keel_sql_frame_bound_t   start_bound;   /**< Start of frame */
    keel_sql_node_t*         start_offset;  /**< Offset expr for PRECEDING/FOLLOWING */
    keel_sql_frame_bound_t   end_bound;     /**< End of frame (CURRENT ROW if no BETWEEN) */
    keel_sql_node_t*         end_offset;    /**< Offset expr for PRECEDING/FOLLOWING */
    bool                     has_between;   /**< BETWEEN ... AND ... was present */
    keel_sql_frame_exclusion_t exclude;     /**< EXCLUDE clause (PostgreSQL 13+) */
} keel_sql_frame_spec_t;

/**
 * @brief Window specification (OVER clause)
 * Represents: OVER (name? PARTITION BY ... ORDER BY ... frame_spec?)
 *         or: OVER window_name
 */
typedef struct keel_sql_window_spec {
    keel_sql_node_t  base;
    keel_str_t       ref_name;       /**< Named window reference (if inline spec is absent) */
    keel_sql_list_t* partition_by;   /**< PARTITION BY expressions */
    keel_sql_list_t* order_by;       /**< ORDER BY sort keys */
    keel_sql_node_t* frame_spec;     /**< Frame specification (optional) */
} keel_sql_window_spec_t;

/**
 * @brief CTE (WITH clause) definition
 */
typedef struct keel_sql_cte {
    keel_sql_node_t  base;
    keel_str_t       name;           /**< CTE name */
    keel_sql_list_t* column_names;   /**< Column names (optional) */
    keel_sql_node_t* query;          /**< CTE query */
    bool            recursive;      /**< RECURSIVE */
    bool            materialized;   /**< MATERIALIZED (PostgreSQL 12+) */
    bool            not_materialized;/**< NOT MATERIALIZED */
} keel_sql_cte_t;

/* ============================================================================
 * Statement Nodes
 * ============================================================================ */

/**
 * @brief SELECT statement
 */
typedef struct keel_sql_stmt_select {
    keel_sql_node_t  base;
    
    /* WITH clause */
    keel_sql_list_t* with_clause;    /**< CTEs */
    bool            with_recursive; /**< WITH RECURSIVE */
    
    /* SELECT list */
    keel_sql_list_t* targets;        /**< Select targets (columns/expressions) */
    bool            distinct;       /**< DISTINCT */
    keel_sql_list_t* distinct_on;    /**< DISTINCT ON expressions (PostgreSQL) */
    
    /* FROM clause */
    keel_sql_node_t* from;           /**< Table references (joined) */
    
    /* WHERE clause */
    keel_sql_node_t* where;          /**< WHERE condition */
    
    /* GROUP BY / HAVING */
    keel_sql_list_t* group_by;       /**< GROUP BY expressions */
    keel_sql_node_t* having;         /**< HAVING condition */
    
    /* WINDOW clause */
    keel_sql_list_t* window;         /**< WINDOW definitions */
    
    /* ORDER BY */
    keel_sql_list_t* order_by;       /**< ORDER BY items */
    
    /* LIMIT/OFFSET */
    keel_sql_node_t* limit;          /**< LIMIT clause */
    
    /* FOR UPDATE/SHARE */
    keel_sql_node_t* locking;        /**< Locking clause */
    
    /* Set operations */
    keel_sql_set_op_t set_op;        /**< UNION/INTERSECT/EXCEPT */
    keel_sql_node_t*  set_right;     /**< Right side of set operation */
    
    /* INTO clause (SELECT INTO) */
    keel_sql_node_t* into;           /**< INTO target (optional) */
    
} keel_sql_stmt_select_t;

/**
 * @brief INSERT statement
 */
typedef struct keel_sql_stmt_insert {
    keel_sql_node_t  base;
    
    /* WITH clause */
    keel_sql_list_t* with_clause;
    
    /* Target table */
    keel_sql_node_t* table;          /**< Table reference */
    keel_str_t       alias;          /**< Table alias (optional) */
    
    /* Columns */
    keel_sql_list_t* columns;        /**< Column list (optional) */
    
    /* Values */
    keel_sql_node_t* source;         /**< VALUES or SELECT */
    
    /* ON CONFLICT */
    keel_sql_node_t* on_conflict;    /**< ON CONFLICT clause (optional) */
    
    /* RETURNING */
    keel_sql_list_t* returning;      /**< RETURNING clause (optional) */
    
    /* OVERRIDING */
    bool            overriding_system;
    bool            overriding_user;
    
} keel_sql_stmt_insert_t;

/**
 * @brief UPDATE statement
 */
typedef struct keel_sql_stmt_update {
    keel_sql_node_t  base;
    
    /* WITH clause */
    keel_sql_list_t* with_clause;
    
    /* Target table */
    keel_sql_node_t* table;          /**< Table reference */
    keel_str_t       alias;          /**< Table alias (optional) */
    
    /* SET clause */
    keel_sql_list_t* set_list;       /**< column = value pairs */
    
    /* FROM clause */
    keel_sql_node_t* from;           /**< FROM clause (optional) */
    
    /* WHERE clause */
    keel_sql_node_t* where;          /**< WHERE condition */
    
    /* RETURNING */
    keel_sql_list_t* returning;      /**< RETURNING clause (optional) */
    
} keel_sql_stmt_update_t;

/**
 * @brief SET clause item (for UPDATE)
 */
typedef struct keel_sql_set_item {
    keel_sql_node_t  base;
    keel_sql_node_t* column;         /**< Column(s) - can be multi-column */
    keel_sql_node_t* value;          /**< Value expression */
} keel_sql_set_item_t;

/**
 * @brief DELETE statement
 */
typedef struct keel_sql_stmt_delete {
    keel_sql_node_t  base;
    
    /* WITH clause */
    keel_sql_list_t* with_clause;
    
    /* Target table */
    keel_sql_node_t* table;          /**< Table reference */
    keel_str_t       alias;          /**< Table alias (optional) */
    
    /* USING clause */
    keel_sql_node_t* using;          /**< USING clause (optional) */
    
    /* WHERE clause */
    keel_sql_node_t* where;          /**< WHERE condition */
    
    /* RETURNING */
    keel_sql_list_t* returning;      /**< RETURNING clause (optional) */
    
} keel_sql_stmt_delete_t;

/**
 * @brief Transaction statement (BEGIN, COMMIT, ROLLBACK)
 */
typedef struct keel_sql_stmt_transaction {
    keel_sql_node_t  base;
    bool            read_only;      /**< READ ONLY transaction */
    bool            deferrable;     /**< DEFERRABLE */
    int             isolation_level;/**< Isolation level */
    keel_str_t       savepoint_name; /**< For SAVEPOINT/ROLLBACK TO */
} keel_sql_stmt_transaction_t;

/**
 * @brief SET statement
 */
typedef struct keel_sql_stmt_set {
    keel_sql_node_t  base;
    keel_str_t       name;           /**< Parameter name */
    keel_sql_node_t* value;          /**< Value expression */
    bool            local;          /**< SET LOCAL */
    bool            session;        /**< SET SESSION */
} keel_sql_stmt_set_t;

/**
 * @brief PREPARE statement
 */
typedef struct keel_sql_stmt_prepare {
    keel_sql_node_t  base;
    keel_str_t       name;           /**< Statement name */
    keel_sql_list_t* param_types;    /**< Parameter types (optional) */
    keel_sql_node_t* query;          /**< Query to prepare */
} keel_sql_stmt_prepare_t;

/**
 * @brief EXECUTE statement
 */
typedef struct keel_sql_stmt_execute {
    keel_sql_node_t  base;
    keel_str_t       name;           /**< Statement name */
    keel_sql_list_t* params;         /**< Parameter values */
} keel_sql_stmt_execute_t;

/**
 * @brief Type specification
 */
typedef struct keel_sql_type {
    keel_sql_node_t  base;
    keel_str_t       schema;         /**< Schema (optional) */
    keel_str_t       name;           /**< Type name */
    keel_sql_node_t* length;         /**< Length (optional) */
    keel_sql_node_t* precision;      /**< Precision (optional) */
    keel_sql_node_t* scale;          /**< Scale (optional) */
    bool            is_array;       /**< Array type */
    int             array_dims;     /**< Number of array dimensions */
} keel_sql_type_t;

/* ============================================================================
 * Parser Context and Functions
 * ============================================================================ */

/**
 * @brief Mutable parser state for recursive-descent AST construction.
 *
 * The parser keeps both current and lookahead tokens so it can resolve simple
 * syntactic ambiguities without secondary token buffers.
 */
typedef struct keel_sql_parser {
    keel_sql_lexer_t lexer;          /**< Lexer */
    keel_sql_token_t current;        /**< Current token */
    keel_sql_token_t lookahead;      /**< Lookahead token */
    keel_arena_t*    arena;          /**< Memory arena for nodes */
    keel_str_t       sql;            /**< Original SQL text */
    bool            has_error;      /**< Parse error occurred */
    char            error_msg[256]; /**< Error message */
    size_t          error_pos;      /**< Error position */
} keel_sql_parser_t;

/**
 * @brief Initialize a parser over caller-owned SQL text and an arena.
 *
 * @param parser Parser state to initialize.
 * @param sql SQL text to parse.
 * @param arena Arena used for all AST allocations.
 * @return
 */
void keel_sql_parser_init(keel_sql_parser_t* parser, keel_str_t sql, keel_arena_t* arena);

/**
 * @brief Parse a SQL statement
 *
 * Parses a single SQL statement and returns the AST root node.
 *
 * @code
 * keel_arena_t arena;
 * keel_arena_init(&arena, 4096);
 * 
 * keel_sql_parser_t parser;
 * keel_sql_parser_init(&parser, KEEL_STR("SELECT * FROM users"), &arena);
 * 
 * keel_sql_node_t* ast = keel_sql_parse(&parser);
 * if (parser.has_error) {
 *     printf("Parse error: %s\n", parser.error_msg);
 * }
 * 
 * keel_arena_destroy(&arena);  // Frees all nodes
 * @endcode
 *
 * @param parser Parser state
 * @return AST root node, or NULL on error
 */
keel_sql_node_t* keel_sql_parse(keel_sql_parser_t* parser);

/**
 * @brief Parse all semicolon-delimited statements into a list node.
 *
 * @param parser Parser state.
 * @return List of parsed statement nodes, possibly partial if parsing stops on error.
 */
keel_sql_list_t* keel_sql_parse_statements(keel_sql_parser_t* parser);

/**
 * @brief Parse a standalone expression using the parser's precedence rules.
 *
 * @param parser Parser state.
 * @return AST node for the parsed expression, or `NULL` on error.
 */
keel_sql_node_t* keel_sql_parse_expr(keel_sql_parser_t* parser);

/* ============================================================================
 * AST Node Creation Helpers
 * ============================================================================ */

/**
 * @brief Allocate a node from the arena
 */
#define KEEL_SQL_NODE_NEW(parser, type) \
    ((type*)keel_sql_node_alloc((parser), sizeof(type)))

/**
 * @brief Allocate and zero one AST node from the parser arena.
 *
 * @param parser Active parser state.
 * @param size Node size in bytes.
 * @return Pointer to zeroed storage, or `NULL` on allocation failure.
 */
void* keel_sql_node_alloc(keel_sql_parser_t* parser, size_t size);

/**
 * @brief Create an empty AST list node in the parser arena.
 */
keel_sql_list_t* keel_sql_list_new(keel_sql_parser_t* parser);

/**
 * @brief Append a node to an AST list while preserving O(1) tail insertion.
 */
void keel_sql_list_append(keel_sql_list_t* list, keel_sql_node_t* node);

/* ============================================================================
 * AST Walking and Visiting
 * ============================================================================ */

/**
 * @brief Visitor callback result
 */
typedef enum keel_sql_visit_result {
    KEEL_SQL_VISIT_CONTINUE = 0,     /**< Continue visiting */
    KEEL_SQL_VISIT_SKIP_CHILDREN,    /**< Skip children of this node */
    KEEL_SQL_VISIT_STOP,             /**< Stop visiting entirely */
} keel_sql_visit_result_t;

/**
 * @brief Visitor callback invoked during AST traversal.
 */
typedef keel_sql_visit_result_t (*keel_sql_visitor_fn)(
    keel_sql_node_t* node,
    void*           context
);

/**
 * @brief Depth-first walk over an AST subtree.
 *
 * The walker provides a shared traversal primitive for table extraction, column
 * extraction, debugging, and future transformations.
 *
 * @param root AST root to traverse.
 * @param visitor Callback invoked for each visited node.
 * @param context Opaque caller context forwarded to `visitor`.
 * @return
 *
 * @param root    Root node to start from
 * @param visitor Callback function
 * @param context User context passed to callback
 */
void keel_sql_ast_walk(
    keel_sql_node_t*     root,
    keel_sql_visitor_fn  visitor,
    void*               context
);

/**
 * @brief Get human-readable name for node kind
 */
const char* keel_sql_node_kind_name(keel_sql_node_kind_t kind);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_SQL_AST_H */
