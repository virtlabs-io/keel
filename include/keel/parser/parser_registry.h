/**
 * @file parser_registry.h
 * @brief Registry and dispatch helpers for frontend-bound parser plugins.
 */

#ifndef KEEL_PARSER_REGISTRY_H
#define KEEL_PARSER_REGISTRY_H

#include "keel_error.h"
#include "keel/parser/parser_plugin.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct keel_parser_registry keel_parser_registry_t;

keel_parser_registry_t* keel_parser_registry_create(void);
void keel_parser_registry_destroy(keel_parser_registry_t* registry);

keel_error_t keel_parser_registry_register(keel_parser_registry_t* registry,
                                           const keel_parser_plugin_ops_t* ops,
                                           bool enabled);

keel_error_t keel_parser_registry_set_enabled(keel_parser_registry_t* registry,
                                              const char* name,
                                              bool enabled);

const keel_parser_plugin_ops_t*
keel_parser_registry_lookup(const keel_parser_registry_t* registry,
                            const char* name);

const keel_parser_plugin_ops_t*
keel_parser_registry_lookup_dialect(const keel_parser_registry_t* registry,
                                    keel_language_id_t language,
                                    keel_dialect_id_t dialect);

keel_parse_status_t keel_parser_registry_parse(
    const keel_parser_registry_t* registry,
    const char* parser_name,
    const keel_parse_input_t* input,
    keel_parse_result_t* result);

keel_error_t keel_parser_registry_register_builtins(
    keel_parser_registry_t* registry);

const keel_parser_plugin_ops_t* keel_parser_builtin_postgresql_sql(void);

/* Transitional bridge for Phase 1.  The returned pointer is owned by
 * result->plugin_private and is invalidated by keel_parse_result_free(). */
struct keel_qt_query;
struct keel_qt_query* keel_parse_result_legacy_qt(
    const keel_parse_result_t* result);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PARSER_REGISTRY_H */
