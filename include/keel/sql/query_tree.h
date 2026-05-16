/**
 * @file query_tree.h
 * @brief Semantic query representation used for routing, caching, and invalidation decisions.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The Query Tree is the semantic projection of the AST. It collapses syntactic
 * detail into the properties the rest of KEEL actually needs: operation class,
 * routing hints, table access modes, column references, cache eligibility, and
 * invalidation scope.
 *
 * This layer exists because syntax alone is often too detailed for control-plane
 * decisions, while the fast classifier is intentionally too shallow. The Query
 * Tree sits in between: rich enough to inform policy, cheap enough to build only
 * on demand, and explicit about partial analysis when full certainty is not worth
 * the runtime cost.
 */

#ifndef KEEL_SQL_QUERY_TREE_H
#define KEEL_SQL_QUERY_TREE_H

#include <stdio.h>

#include "keel_types.h"
#include "keel_error.h"
#include "keel/mem/mem.h"
#include "keel/sql/sql_ast.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Forward Declarations
 * ============================================================================ */

typedef struct keel_qt_node keel_qt_node_t;
typedef struct keel_qt_query keel_qt_query_t;
typedef struct keel_qt_table_ref keel_qt_table_ref_t;
typedef struct keel_qt_column_ref keel_qt_column_ref_t;

/* ============================================================================
 * Query Tree Node Types
 * ============================================================================ */

/**
 * @brief Query Tree node types
 */
typedef enum keel_qt_node_type {
    KEEL_QT_NODE_UNKNOWN = 0,
    
    /* Query nodes */
    KEEL_QT_NODE_SELECT,         /**< SELECT query */
    KEEL_QT_NODE_INSERT,         /**< INSERT statement */
    KEEL_QT_NODE_UPDATE,         /**< UPDATE statement */
    KEEL_QT_NODE_DELETE,         /**< DELETE statement */
    KEEL_QT_NODE_MERGE,          /**< MERGE/UPSERT */
    
    /* DDL nodes */
    KEEL_QT_NODE_CREATE,         /**< CREATE ... */
    KEEL_QT_NODE_ALTER,          /**< ALTER ... */
    KEEL_QT_NODE_DROP,           /**< DROP ... */
    KEEL_QT_NODE_TRUNCATE,       /**< TRUNCATE */
    
    /* Transaction nodes */
    KEEL_QT_NODE_BEGIN,          /**< BEGIN/START TRANSACTION */
    KEEL_QT_NODE_COMMIT,         /**< COMMIT */
    KEEL_QT_NODE_ROLLBACK,       /**< ROLLBACK */
    KEEL_QT_NODE_SAVEPOINT,      /**< SAVEPOINT/RELEASE */
    
    /* Session nodes */
    KEEL_QT_NODE_SET,            /**< SET ... */
    KEEL_QT_NODE_RESET,          /**< RESET ... */
    KEEL_QT_NODE_DISCARD,        /**< DISCARD ... */
    
    /* Prepared statement nodes */
    KEEL_QT_NODE_PREPARE,        /**< PREPARE ... */
    KEEL_QT_NODE_EXECUTE,        /**< EXECUTE ... */
    KEEL_QT_NODE_DEALLOCATE,     /**< DEALLOCATE ... */
    
    /* Other */
    KEEL_QT_NODE_SHOW,           /**< SHOW ... */
    KEEL_QT_NODE_EXPLAIN,        /**< EXPLAIN ... */
    KEEL_QT_NODE_CALL,           /**< CALL ... */
    KEEL_QT_NODE_COPY,           /**< COPY ... */
    
    /* Reference nodes */
    KEEL_QT_NODE_TABLE_REF,      /**< Table reference */
    KEEL_QT_NODE_COLUMN_REF,     /**< Column reference */
    KEEL_QT_NODE_FUNC_REF,       /**< Function call reference */
    KEEL_QT_NODE_EXPRESSION,     /**< Generic expression */
    
    /* Compound */
    KEEL_QT_NODE_COMPOUND,       /**< Multiple statements */
    KEEL_QT_NODE_SUBQUERY,       /**< Subquery */
    KEEL_QT_NODE_CTE,            /**< Common Table Expression */
    
} keel_qt_node_type_t;

/* ============================================================================
 * Query Classification
 * ============================================================================ */

/**
 * @brief Query operation type (for routing)
 */
typedef enum keel_qt_operation {
    KEEL_QT_OP_READ = 0,         /**< Read-only operation */
    KEEL_QT_OP_WRITE,            /**< Data modification */
    KEEL_QT_OP_DDL,              /**< Schema modification */
    KEEL_QT_OP_ADMIN,            /**< Administrative (VACUUM, etc.) */
    KEEL_QT_OP_TRANSACTION,      /**< Transaction control */
    KEEL_QT_OP_SESSION,          /**< Session state change */
    KEEL_QT_OP_UNKNOWN,          /**< Unknown/unparseable */
} keel_qt_operation_t;

/**
 * @brief Query flags
 */
typedef enum keel_qt_flags {
    KEEL_QT_FLAG_NONE            = 0,
    
    /* Routing hints */
    KEEL_QT_FLAG_NEEDS_PRIMARY   = (1 << 0),  /**< Must go to primary */
    KEEL_QT_FLAG_READONLY        = (1 << 1),  /**< Safe for replica */
    KEEL_QT_FLAG_DETERMINISTIC   = (1 << 2),  /**< No random/time functions */
    
    /* Caching hints */
    KEEL_QT_FLAG_CACHEABLE       = (1 << 3),  /**< Result can be cached */
    KEEL_QT_FLAG_NO_CACHE        = (1 << 4),  /**< Explicitly non-cacheable */
    KEEL_QT_FLAG_INVALIDATES     = (1 << 5),  /**< Invalidates cache */
    
    /* Transaction state */
    KEEL_QT_FLAG_STARTS_TXN      = (1 << 6),  /**< Starts transaction */
    KEEL_QT_FLAG_ENDS_TXN        = (1 << 7),  /**< Ends transaction */
    KEEL_QT_FLAG_IN_TXN          = (1 << 8),  /**< Requires transaction context */
    
    /* Session state */
    KEEL_QT_FLAG_MODIFIES_SESSION = (1 << 9), /**< Changes session state */
    
    /* Special */
    KEEL_QT_FLAG_FOR_UPDATE      = (1 << 10), /**< SELECT ... FOR UPDATE */
    KEEL_QT_FLAG_HAS_RETURNING   = (1 << 11), /**< Has RETURNING clause */
    KEEL_QT_FLAG_HAS_CTE         = (1 << 12), /**< Has WITH clause */
    KEEL_QT_FLAG_HAS_SUBQUERY    = (1 << 13), /**< Contains subqueries */
    KEEL_QT_FLAG_MULTI_TABLE     = (1 << 14), /**< References multiple tables */
    
    /* Parse info */
    KEEL_QT_FLAG_PARTIAL_PARSE   = (1 << 15), /**< Partially parsed (complex SQL) */
    
} keel_qt_flags_t;

/**
 * @brief Table access mode
 */
typedef enum keel_qt_table_access {
    KEEL_QT_ACCESS_READ = 0,     /**< Read from table */
    KEEL_QT_ACCESS_WRITE,        /**< Write to table (INSERT/UPDATE/DELETE) */
    KEEL_QT_ACCESS_EXCLUSIVE,    /**< Exclusive lock (DDL) */
} keel_qt_table_access_t;

/**
 * @brief DDL object type
 */
typedef enum keel_qt_ddl_object {
    KEEL_QT_DDL_UNKNOWN = 0,
    KEEL_QT_DDL_TABLE,
    KEEL_QT_DDL_INDEX,
    KEEL_QT_DDL_VIEW,
    KEEL_QT_DDL_FUNCTION,
    KEEL_QT_DDL_PROCEDURE,
    KEEL_QT_DDL_TRIGGER,
    KEEL_QT_DDL_SCHEMA,
    KEEL_QT_DDL_DATABASE,
    KEEL_QT_DDL_SEQUENCE,
    KEEL_QT_DDL_TYPE,
    KEEL_QT_DDL_EXTENSION,
    KEEL_QT_DDL_POLICY,
    KEEL_QT_DDL_PUBLICATION,
    KEEL_QT_DDL_SUBSCRIPTION,
} keel_qt_ddl_object_t;

/* ============================================================================
 * Table Reference
 * ============================================================================ */

/**
 * @brief Table reference in query
 *
 * Tracks which tables are accessed and how.
 */
struct keel_qt_table_ref {
    keel_qt_node_type_t  type;       /**< Always KEEL_QT_NODE_TABLE_REF */
    keel_qt_table_ref_t* next;       /**< Next table in list */
    
    keel_str_t           catalog;    /**< Catalog name (optional) */
    keel_str_t           schema;     /**< Schema name (optional) */
    keel_str_t           table;      /**< Table name */
    keel_str_t           alias;      /**< Alias in query (optional) */
    
    keel_qt_table_access_t access;   /**< Read/write/exclusive */
    
    /* For join tracking */
    bool                is_outer;   /**< In outer side of join */
    bool                is_lateral; /**< LATERAL reference */
    
    /* Source location */
    keel_sql_location_t  loc;        /**< Location in source SQL */
};

/**
 * @brief Column reference in query
 */
struct keel_qt_column_ref {
    keel_qt_node_type_t    type;     /**< Always KEEL_QT_NODE_COLUMN_REF */
    keel_qt_column_ref_t*  next;     /**< Next column in list */
    
    keel_str_t             table;    /**< Table name/alias (optional) */
    keel_str_t             column;   /**< Column name */
    
    /* Context */
    bool                  in_select;/**< In SELECT list */
    bool                  in_where; /**< In WHERE clause */
    bool                  in_group; /**< In GROUP BY */
    bool                  in_order; /**< In ORDER BY */
    bool                  is_output;/**< In RETURNING/output */
    
    /* For UPDATE SET */
    bool                  is_target;/**< Being written to */
    
    keel_sql_location_t    loc;      /**< Location in source SQL */
};

/* ============================================================================
 * Function Reference
 * ============================================================================ */

/**
 * @brief Forward declaration for linked-list self-reference.
 */
typedef struct keel_qt_func_ref keel_qt_func_ref_t;

/**
 * @brief Function call reference in a query.
 *
 * Tracks every user-defined function invocation (KEEL_SQL_NODE_EXPR_FUNC,
 * KEEL_SQL_NODE_EXPR_AGGR, KEEL_SQL_NODE_STMT_CALL) so that write-safety
 * analysis can check the metadata cache for VOLATILE / SECURITY DEFINER
 * functions that force primary routing.
 */
struct keel_qt_func_ref {
    keel_qt_node_type_t  type;    /**< Always KEEL_QT_NODE_FUNC_REF */
    keel_qt_func_ref_t*  next;    /**< Next function in list */

    keel_str_t           schema;  /**< Schema name (optional, may be empty) */
    keel_str_t           name;    /**< Function name */

    keel_sql_location_t  loc;     /**< Location in source SQL */
};

/* ============================================================================
 * Query Tree Node
 * ============================================================================ */

/**
 * @brief Query Tree root node
 *
 * Contains all semantic information about a query.
 */
struct keel_qt_query {
    keel_qt_node_type_t  type;           /**< Query type */
    keel_qt_operation_t  operation;      /**< Read/write/DDL */
    uint32_t            flags;          /**< Query flags */
    
    /* Original SQL */
    keel_str_t           sql;            /**< Original SQL text */
    
    /* Table references */
    keel_qt_table_ref_t* tables;         /**< All table references */
    size_t              table_count;    /**< Number of tables */
    
    /* Column references */
    keel_qt_column_ref_t* columns;       /**< All column references */
    size_t               column_count;  /**< Number of columns */

    /* Function call references (for write-safety analysis) */
    keel_qt_func_ref_t*  functions;      /**< All function call references */
    size_t               func_count;    /**< Number of function references */
    
    /* Target table (for DML) */
    keel_qt_table_ref_t* target_table;   /**< Table being modified */
    
    /* For DDL */
    keel_qt_ddl_object_t ddl_object;     /**< Object type being modified */
    keel_str_t           object_name;    /**< Object name */
    
    /* For prepared statements */
    keel_str_t           stmt_name;      /**< Statement name */
    size_t              param_count;    /**< Number of parameters */
    
    /* Subqueries */
    keel_qt_query_t*     subqueries;     /**< List of subqueries */
    size_t              subquery_count; /**< Number of subqueries */
    
    /* For compound statements */
    keel_qt_query_t*     next;           /**< Next statement in compound */
    
    /* Caching information */
    uint64_t            cache_key;      /**< Hash for cache lookup */
    int                 cache_ttl;      /**< Suggested TTL in seconds */
    
    /* AST reference (for SQL generation) */
    keel_sql_node_t*     ast;            /**< Original AST node */
    
    /* Error information */
    bool                has_error;      /**< Parse/analysis error */
    const char*         error_msg;      /**< Error message */
};

/* ============================================================================
 * Query Tree Builder
 * ============================================================================ */

/**
 * @brief Query Tree builder context
 */
typedef struct keel_qt_builder {
    keel_arena_t*    arena;          /**< Memory arena */
    keel_qt_query_t* current;        /**< Current query being built */
    
    /* Table tracking during build */
    keel_qt_table_ref_t* table_stack;/**< Table reference stack */
    
    /* Context */
    bool            in_subquery;    /**< Inside subquery */
    bool            in_cte;         /**< Inside CTE */
    int             context_flags;  /**< Current context (select/where/etc) */
    
    /* Error handling */
    bool            has_error;
    char            error_msg[256];
} keel_qt_builder_t;

/**
 * @brief Initialize a Query Tree builder over an arena.
 *
 * @param builder Builder context to initialize.
 * @param arena Arena used for all Query Tree allocations.
 * @return
 */
void keel_qt_builder_init(keel_qt_builder_t* builder, keel_arena_t* arena);

/**
 * @brief Build Query Tree from AST
 *
 * Transforms a parsed AST into a semantic Query Tree.
 *
 * @code
 * // Parse SQL to AST
 * keel_sql_parser_t parser;
 * keel_sql_parser_init(&parser, sql, &arena);
 * keel_sql_node_t* ast = keel_sql_parse(&parser);
 *
 * // Build Query Tree
 * keel_qt_builder_t builder;
 * keel_qt_builder_init(&builder, &arena);
 * keel_qt_query_t* qt = keel_qt_build(&builder, ast);
 *
 * // Analyze
 * if (qt->operation == KEEL_QT_OP_READ && 
 *     !(qt->flags & KEEL_QT_FLAG_FOR_UPDATE)) {
 *     // Safe to route to replica
 * }
 * @endcode
 *
 * @param builder Builder context
 * @param ast     AST root node
 * @return Query Tree, or NULL on error
 */
keel_qt_query_t* keel_qt_build(keel_qt_builder_t* builder, keel_sql_node_t* ast);

/* ============================================================================
 * Query Tree Analysis
 * ============================================================================ */

/**
 * @brief Decide whether a query tree is replica-safe under KEEL's conservative policy.
 *
 * @param qt Query Tree to inspect.
 * @return `true` if the query may be routed to a replica.
 */
bool keel_qt_can_use_replica(const keel_qt_query_t* qt);

/**
 * @brief Report whether the builder marked the query deterministic.
 */
bool keel_qt_is_deterministic(const keel_qt_query_t* qt);

/**
 * @brief Report whether the query is eligible for result caching.
 */
bool keel_qt_is_cacheable(const keel_qt_query_t* qt);

/**
 * @brief Collect the write-target tables whose caches should be invalidated.
 *
 * @param qt Query tree to inspect.
 * @param tables [out] Caller-provided output array.
 * @param max Capacity of `tables`.
 * @return Number of table references written into `tables`.
 */
size_t keel_qt_get_invalidated_tables(
    const keel_qt_query_t* qt,
    keel_qt_table_ref_t**  tables,
    size_t                max
);

/**
 * @brief Compute a structural cache key from semantic query content.
 *
 * The current implementation hashes query type plus discovered table and column
 * references. It is intentionally lightweight and should be read as a routing
 * cache key rather than a cryptographically unique statement fingerprint.
 *
 * @param qt Query tree to hash.
 * @return 64-bit structural cache key.
 */
uint64_t keel_qt_compute_cache_key(const keel_qt_query_t* qt);

/* ============================================================================
 * SQL Generation (Dialect Translation)
 * ============================================================================ */

/**
 * @brief SQL dialect for generation
 */
typedef enum keel_sql_dialect {
    KEEL_DIALECT_POSTGRESQL = 0,
    KEEL_DIALECT_MYSQL,
    KEEL_DIALECT_MARIADB,
    KEEL_DIALECT_SQLITE,
    KEEL_DIALECT_STANDARD,       /**< SQL Standard (ISO) */
} keel_sql_dialect_t;

/**
 * @brief SQL generation options
 */
typedef struct keel_sql_gen_opts {
    keel_sql_dialect_t   dialect;        /**< Target dialect */
    bool                pretty;         /**< Pretty-print output */
    bool                uppercase;      /**< Uppercase keywords */
    bool                quote_idents;   /**< Quote all identifiers */
    const char*         indent;         /**< Indentation string */
} keel_sql_gen_opts_t;

/**
 * @brief SQL generator state
 */
typedef struct keel_sql_generator {
    keel_arena_t*        arena;          /**< Memory arena */
    keel_sql_gen_opts_t  opts;           /**< Generation options */
    
    /* Output buffer */
    char*               buffer;
    size_t              len;
    size_t              cap;
    
    /* Formatting state */
    int                 indent_level;
    bool                at_line_start;
} keel_sql_generator_t;

/**
 * @brief Initialize SQL generator
 *
 * @param gen   Generator to initialize
 * @param arena Memory arena
 * @param opts  Generation options
 */
void keel_sql_gen_init(
    keel_sql_generator_t* gen,
    keel_arena_t*         arena,
    const keel_sql_gen_opts_t* opts
);

/**
 * @brief Generate SQL from Query Tree
 *
 * Generates SQL in the target dialect from a Query Tree.
 *
 * @code
 * // Parse PostgreSQL
 * keel_qt_query_t* qt = ...;
 *
 * // Generate MySQL
 * keel_sql_gen_opts_t opts = { .dialect = KEEL_DIALECT_MYSQL };
 * keel_sql_generator_t gen;
 * keel_sql_gen_init(&gen, &arena, &opts);
 * 
 * keel_str_t mysql_sql = keel_sql_gen_query(&gen, qt);
 * @endcode
 *
 * @param gen Generator
 * @param qt  Query tree
 * @return Generated SQL string
 */
keel_str_t keel_sql_gen_query(keel_sql_generator_t* gen, const keel_qt_query_t* qt);

/**
 * @brief Generate SQL from AST
 *
 * Generates SQL directly from AST (when Query Tree not needed).
 *
 * @param gen Generator
 * @param ast AST node
 * @return Generated SQL string
 */
keel_str_t keel_sql_gen_ast(keel_sql_generator_t* gen, const keel_sql_node_t* ast);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

/**
 * @brief Get operation name
 */
const char* keel_qt_operation_name(keel_qt_operation_t op);

/**
 * @brief Get node type name
 */
const char* keel_qt_node_type_name(keel_qt_node_type_t type);

/**
 * @brief Get dialect name
 */
const char* keel_sql_dialect_name(keel_sql_dialect_t dialect);

/**
 * @brief Print a multi-line debug dump of the Query Tree to a stream.
 */
void keel_qt_dump(const keel_qt_query_t* qt, FILE* out);

/**
 * @brief Serialise a Query Tree into a string buffer
 *
 * Writes a human-readable multi-line representation of the tree
 * (type, operation, flags, tables, columns) into @p buf.
 *
 * @param qt    Query tree (may be NULL)
 * @param buf   Destination buffer
 * @param bufsz Size of buffer in bytes
 * @return Number of characters written (excluding NUL), or that
 *         would have been written if the buffer were large enough.
 */
int keel_qt_snprint(const keel_qt_query_t* qt, char* buf, size_t bufsz);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_SQL_QUERY_TREE_H */
