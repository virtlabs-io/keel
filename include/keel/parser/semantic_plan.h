/**
 * @file semantic_plan.h
 * @brief Parser-neutral semantic contract consumed by routing policy.
 */

#ifndef KEEL_PARSER_SEMANTIC_PLAN_H
#define KEEL_PARSER_SEMANTIC_PLAN_H

#include "keel_types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum keel_language_id {
    KEEL_LANG_SQL = 1,
    KEEL_LANG_GRAPHQL,
    KEEL_LANG_JSON_QUERY,
    KEEL_LANG_MCP,
    KEEL_LANG_NATURAL_LANGUAGE,
    KEEL_LANG_CUSTOM,
} keel_language_id_t;

typedef enum keel_dialect_id {
    KEEL_DIALECT_UNKNOWN = 0,
    KEEL_DIALECT_SQL_POSTGRESQL,
    KEEL_DIALECT_SQL_MYSQL,
    KEEL_DIALECT_SQL_SQLITE,
    KEEL_DIALECT_SQL_ANSI,
    KEEL_DIALECT_GRAPHQL_DEFAULT,
    KEEL_DIALECT_MCP_DEFAULT,
    KEEL_DIALECT_JSON_DEFAULT,
} keel_dialect_id_t;

typedef enum keel_semantic_class {
    KEEL_SEM_UNKNOWN = 0,
    KEEL_SEM_READ_ONLY,
    KEEL_SEM_WRITE,
    KEEL_SEM_DDL,
    KEEL_SEM_TRANSACTION_CONTROL,
    KEEL_SEM_SESSION_STATE,
    KEEL_SEM_SECURITY,
    KEEL_SEM_ADMIN,
    KEEL_SEM_EXTERNAL_EFFECT,
    KEEL_SEM_MIXED,
} keel_semantic_class_t;

typedef enum keel_safety_level {
    KEEL_SAFETY_SAFE_REPLICA = 1,
    KEEL_SAFETY_PRIMARY_REQUIRED,
    KEEL_SAFETY_PINNED_BACKEND_REQUIRED,
    KEEL_SAFETY_REJECT_REQUIRED,
    KEEL_SAFETY_UNKNOWN_FAIL_CLOSED,
} keel_safety_level_t;

typedef struct keel_semantic_plan {
    keel_language_id_t      language;
    keel_dialect_id_t       dialect;
    keel_semantic_class_t   semantic_class;
    keel_safety_level_t     safety;

    bool is_read_only;
    bool may_write;
    bool changes_schema;
    bool changes_session_state;
    bool transaction_control;
    bool requires_primary;
    bool requires_pinned_backend;
    bool safe_for_replica;
    bool cacheable;
    bool shardable;
    bool uses_temp_objects;
    bool uses_prepared_statement;
    bool calls_function;
    bool external_side_effects_possible;
    bool parser_confident;

    uint64_t normalized_hash;
    uint64_t referenced_objects_hash;

    char reason[256];
} keel_semantic_plan_t;

void keel_semantic_plan_init(keel_semantic_plan_t* plan,
                             keel_language_id_t language,
                             keel_dialect_id_t dialect);

bool keel_semantic_plan_valid(const keel_semantic_plan_t* plan);

const char* keel_semantic_class_name(keel_semantic_class_t cls);
const char* keel_safety_level_name(keel_safety_level_t safety);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PARSER_SEMANTIC_PLAN_H */
