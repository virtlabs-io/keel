/**
 * @file pg_sql_parser.c
 * @brief Builtin PostgreSQL SQL parser plugin.
 */

#include "keel/parser/parser_registry.h"
#include "keel/mem/mem.h"
#include "keel/sql/query_tree.h"
#include "keel/sql/sql.h"
#include "keel/util/util.h"

#include <stdio.h>
#include <string.h>
#include <strings.h>

typedef struct pg_sql_result_private {
    keel_arena_t* arena;
    keel_qt_query_t* qt;
} pg_sql_result_private_t;

static void plan_reason(keel_semantic_plan_t* plan, const char* reason)
{
    if (!plan || !reason) return;
    snprintf(plan->reason, sizeof plan->reason, "%s", reason);
}

static uint64_t hash_table_refs(const keel_qt_query_t* qt)
{
    uint64_t h = 1469598103934665603ULL;
    for (const keel_qt_table_ref_t* t = qt ? qt->tables : NULL; t; t = t->next) {
        if (t->table.data && t->table.len > 0) {
            h ^= keel_hash_fnv1a_64(t->table.data, t->table.len);
            h *= 1099511628211ULL;
        }
    }
    return h;
}

static bool is_ident_byte(uint8_t c)
{
    return (c >= 'a' && c <= 'z') ||
           (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') ||
           c == '_';
}

static bool sql_contains_function_call(const uint8_t* data,
                                       size_t len,
                                       const char* name)
{
    size_t name_len = strlen(name);
    if (!data || name_len == 0 || len < name_len) return false;

    for (size_t i = 0; i + name_len <= len; i++) {
        if (i > 0 && is_ident_byte(data[i - 1])) {
            continue;
        }
        if (strncasecmp((const char*)data + i, name, name_len) != 0) {
            continue;
        }
        size_t j = i + name_len;
        if (j < len && is_ident_byte(data[j])) {
            continue;
        }
        while (j < len && (data[j] == ' ' || data[j] == '\t' ||
                           data[j] == '\r' || data[j] == '\n')) {
            j++;
        }
        if (j < len && data[j] == '(') {
            return true;
        }
    }
    return false;
}

static void mark_hazardous_function_plan(keel_semantic_plan_t* plan,
                                         const char* reason)
{
    if (!plan) return;
    plan->semantic_class = KEEL_SEM_EXTERNAL_EFFECT;
    plan->is_read_only = false;
    plan->may_write = true;
    plan->requires_primary = true;
    plan->safe_for_replica = false;
    plan->cacheable = false;
    plan->calls_function = true;
    plan->external_side_effects_possible = true;
    plan->safety = KEEL_SAFETY_PRIMARY_REQUIRED;
    plan_reason(plan, reason);
}

static void classify_plan_from_qt(const keel_qt_query_t* qt,
                                  keel_semantic_plan_t* plan)
{
    keel_semantic_plan_init(plan, KEEL_LANG_SQL, KEEL_DIALECT_SQL_POSTGRESQL);
    if (!qt) {
        plan_reason(plan, "PostgreSQL SQL parser did not produce a query tree");
        return;
    }

    plan->normalized_hash = qt->cache_key;
    plan->referenced_objects_hash = hash_table_refs(qt);
    plan->parser_confident = !qt->has_error;
    plan->cacheable = keel_qt_is_cacheable(qt);
    plan->shardable = qt->table_count > 0;
    plan->uses_prepared_statement =
        qt->type == KEEL_QT_NODE_PREPARE ||
        qt->type == KEEL_QT_NODE_EXECUTE ||
        qt->type == KEEL_QT_NODE_DEALLOCATE;
    plan->calls_function = qt->func_count > 0;
    plan->uses_temp_objects = false;

    switch (qt->operation) {
    case KEEL_QT_OP_READ:
        plan->semantic_class = KEEL_SEM_READ_ONLY;
        plan->is_read_only = true;
        plan->safe_for_replica = keel_qt_can_use_replica(qt) && qt->func_count == 0;
        plan->requires_primary = !plan->safe_for_replica;
        plan->external_side_effects_possible = qt->func_count > 0;
        plan->safety = plan->safe_for_replica
            ? KEEL_SAFETY_SAFE_REPLICA
            : KEEL_SAFETY_PRIMARY_REQUIRED;
        plan_reason(plan, qt->func_count > 0
            ? "function call may write or modify state; route conservatively"
            : "read-only PostgreSQL SQL");
        break;
    case KEEL_QT_OP_WRITE:
        plan->semantic_class = KEEL_SEM_WRITE;
        plan->may_write = true;
        plan->requires_primary = true;
        plan->safety = KEEL_SAFETY_PRIMARY_REQUIRED;
        plan_reason(plan, "PostgreSQL write statement");
        break;
    case KEEL_QT_OP_DDL:
        plan->semantic_class = KEEL_SEM_DDL;
        plan->may_write = true;
        plan->changes_schema = true;
        plan->requires_primary = true;
        plan->requires_pinned_backend = true;
        plan->safety = KEEL_SAFETY_PINNED_BACKEND_REQUIRED;
        plan_reason(plan, "PostgreSQL DDL changes schema");
        break;
    case KEEL_QT_OP_TRANSACTION:
        plan->semantic_class = KEEL_SEM_TRANSACTION_CONTROL;
        plan->transaction_control = true;
        plan->requires_primary = true;
        plan->requires_pinned_backend = true;
        plan->safety = KEEL_SAFETY_PINNED_BACKEND_REQUIRED;
        plan_reason(plan, "PostgreSQL transaction control");
        break;
    case KEEL_QT_OP_SESSION:
        plan->semantic_class = KEEL_SEM_SESSION_STATE;
        plan->changes_session_state = true;
        plan->requires_primary = true;
        plan->requires_pinned_backend = true;
        plan->safety = KEEL_SAFETY_PINNED_BACKEND_REQUIRED;
        plan_reason(plan, "PostgreSQL session state change");
        break;
    case KEEL_QT_OP_ADMIN:
        plan->semantic_class = KEEL_SEM_ADMIN;
        plan->requires_primary = true;
        plan->requires_pinned_backend = true;
        plan->safety = KEEL_SAFETY_PINNED_BACKEND_REQUIRED;
        plan_reason(plan, "PostgreSQL administrative statement");
        break;
    case KEEL_QT_OP_UNKNOWN:
    default:
        plan->semantic_class = KEEL_SEM_UNKNOWN;
        plan->requires_primary = true;
        plan->safety = KEEL_SAFETY_UNKNOWN_FAIL_CLOSED;
        plan_reason(plan, "unknown PostgreSQL SQL semantics; fail closed to primary");
        break;
    }

    if (qt->flags & KEEL_QT_FLAG_MODIFIES_SESSION) {
        plan->changes_session_state = true;
        plan->requires_pinned_backend = true;
        plan->safe_for_replica = false;
        plan->requires_primary = true;
        plan->safety = KEEL_SAFETY_PINNED_BACKEND_REQUIRED;
    }
}

static int pg_sql_init(const void* config)
{
    (void)config;
    return 0;
}

static keel_parse_status_t pg_sql_parse(const keel_parse_input_t* input,
                                        keel_parse_result_t* result)
{
    if (!input || !result) return KEEL_PARSE_INTERNAL_ERROR;
    keel_parse_result_init(result);
    result->plan.language = KEEL_LANG_SQL;
    result->plan.dialect = KEEL_DIALECT_SQL_POSTGRESQL;

    if (input->language != KEEL_LANG_SQL ||
        input->dialect != KEEL_DIALECT_SQL_POSTGRESQL) {
        result->status = KEEL_PARSE_UNSUPPORTED;
        snprintf(result->error_message, sizeof result->error_message,
                 "sql.postgresql cannot parse requested language/dialect");
        return result->status;
    }
    if (!input->data || input->len == 0) {
        result->status = KEEL_PARSE_INCOMPLETE;
        snprintf(result->error_message, sizeof result->error_message,
                 "empty SQL input");
        return result->status;
    }

    pg_sql_result_private_t* priv =
        (pg_sql_result_private_t*)keel_calloc(1, sizeof *priv);
    if (!priv) {
        result->status = KEEL_PARSE_RESOURCE_LIMIT;
        snprintf(result->error_message, sizeof result->error_message,
                 "failed to allocate parser result state");
        return result->status;
    }
    priv->arena = keel_arena_create(64 * 1024);
    if (!priv->arena) {
        keel_free(priv);
        result->status = KEEL_PARSE_RESOURCE_LIMIT;
        snprintf(result->error_message, sizeof result->error_message,
                 "failed to allocate parser arena");
        return result->status;
    }

    keel_str_t sql = {
        .data = (const char*)input->data,
        .len = input->len,
    };
    priv->qt = keel_sql_analyze_full(sql, priv->arena);
    if (!priv->qt) {
        keel_arena_destroy(priv->arena);
        keel_free(priv);
        result->status = KEEL_PARSE_ERROR;
        snprintf(result->error_message, sizeof result->error_message,
                 "PostgreSQL SQL parse failed");
        return result->status;
    }

    result->ast = priv->qt->ast;
    result->plugin_private = priv;
    classify_plan_from_qt(priv->qt, &result->plan);
    if (sql_contains_function_call(input->data, input->len, "nextval") ||
        sql_contains_function_call(input->data, input->len, "setval") ||
        sql_contains_function_call(input->data, input->len, "pg_advisory_lock")) {
        mark_hazardous_function_plan(&result->plan,
            "PostgreSQL function has side effects; route conservatively");
    }
    result->status = KEEL_PARSE_OK;
    return result->status;
}

static void pg_sql_free_result(keel_parse_result_t* result)
{
    if (!result || !result->plugin_private) return;
    pg_sql_result_private_t* priv =
        (pg_sql_result_private_t*)result->plugin_private;
    keel_arena_destroy(priv->arena);
    keel_free(priv);
    result->plugin_private = NULL;
    result->ast = NULL;
}

static void pg_sql_shutdown(void)
{
}

static bool pg_sql_supports_feature(const char* feature)
{
    if (!feature) return false;
    return strcmp(feature, "semantic_plan") == 0 ||
           strcmp(feature, "legacy_query_tree") == 0 ||
           strcmp(feature, "postgresql_sql") == 0;
}

const keel_parser_plugin_ops_t* keel_parser_builtin_postgresql_sql(void)
{
    static const keel_parser_plugin_ops_t ops = {
        .name = "sql.postgresql",
        .version = "builtin-1",
        .language = KEEL_LANG_SQL,
        .dialect = KEEL_DIALECT_SQL_POSTGRESQL,
        .init = pg_sql_init,
        .parse = pg_sql_parse,
        .free_result = pg_sql_free_result,
        .shutdown = pg_sql_shutdown,
        .supports_feature = pg_sql_supports_feature,
    };
    return &ops;
}

struct keel_qt_query* keel_parse_result_legacy_qt(
    const keel_parse_result_t* result)
{
    if (!result || !result->plugin_private) return NULL;
    const pg_sql_result_private_t* priv =
        (const pg_sql_result_private_t*)result->plugin_private;
    return priv->qt;
}
