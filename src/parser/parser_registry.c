/**
 * @file parser_registry.c
 * @brief Parser plugin registry and dispatch.
 */

#include "keel/parser/parser_registry.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <string.h>

#define KEEL_PARSER_REGISTRY_MAX 32u

typedef struct keel_parser_entry {
    const keel_parser_plugin_ops_t* ops;
    bool enabled;
    bool initialized;
} keel_parser_entry_t;

struct keel_parser_registry {
    keel_parser_entry_t entries[KEEL_PARSER_REGISTRY_MAX];
    size_t count;
};

static bool same_name(const char* a, const char* b)
{
    if (!a || !b) return false;
    return strcmp(a, b) == 0;
}

keel_parser_registry_t* keel_parser_registry_create(void)
{
    return (keel_parser_registry_t*)keel_calloc(1, sizeof(keel_parser_registry_t));
}

void keel_parser_registry_destroy(keel_parser_registry_t* registry)
{
    if (!registry) return;
    for (size_t i = 0; i < registry->count; i++) {
        keel_parser_entry_t* e = &registry->entries[i];
        if (e->initialized && e->ops && e->ops->shutdown) {
            e->ops->shutdown();
        }
    }
    keel_free(registry);
}

keel_error_t keel_parser_registry_register(keel_parser_registry_t* registry,
                                           const keel_parser_plugin_ops_t* ops,
                                           bool enabled)
{
    if (!registry || !ops || !ops->name || !ops->parse) {
        return KEEL_ERR_INVALID_ARG;
    }
    if (registry->count >= KEEL_PARSER_REGISTRY_MAX) {
        return KEEL_ERR_OVERFLOW;
    }
    for (size_t i = 0; i < registry->count; i++) {
        const keel_parser_plugin_ops_t* cur = registry->entries[i].ops;
        if (cur && (same_name(cur->name, ops->name) ||
                    (cur->language == ops->language &&
                     cur->dialect == ops->dialect))) {
            return KEEL_ERR_ALREADY_EXISTS;
        }
    }

    keel_parser_entry_t* e = &registry->entries[registry->count++];
    e->ops = ops;
    e->enabled = enabled;
    e->initialized = false;
    if (enabled && ops->init) {
        int rc = ops->init(NULL);
        if (rc != 0) {
            registry->count--;
            memset(e, 0, sizeof *e);
            return KEEL_ERR_INVALID_STATE;
        }
        e->initialized = true;
    }
    return KEEL_OK;
}

keel_error_t keel_parser_registry_set_enabled(keel_parser_registry_t* registry,
                                              const char* name,
                                              bool enabled)
{
    if (!registry || !name) return KEEL_ERR_INVALID_ARG;
    for (size_t i = 0; i < registry->count; i++) {
        keel_parser_entry_t* e = &registry->entries[i];
        if (!e->ops || !same_name(e->ops->name, name)) continue;
        if (enabled && !e->initialized && e->ops->init) {
            int rc = e->ops->init(NULL);
            if (rc != 0) return KEEL_ERR_INVALID_STATE;
            e->initialized = true;
        }
        e->enabled = enabled;
        return KEEL_OK;
    }
    return KEEL_ERR_NOT_FOUND;
}

const keel_parser_plugin_ops_t*
keel_parser_registry_lookup(const keel_parser_registry_t* registry,
                            const char* name)
{
    if (!registry || !name) return NULL;
    for (size_t i = 0; i < registry->count; i++) {
        const keel_parser_entry_t* e = &registry->entries[i];
        if (e->enabled && e->ops && same_name(e->ops->name, name)) {
            return e->ops;
        }
    }
    return NULL;
}

const keel_parser_plugin_ops_t*
keel_parser_registry_lookup_dialect(const keel_parser_registry_t* registry,
                                    keel_language_id_t language,
                                    keel_dialect_id_t dialect)
{
    if (!registry) return NULL;
    for (size_t i = 0; i < registry->count; i++) {
        const keel_parser_entry_t* e = &registry->entries[i];
        if (e->enabled && e->ops &&
            e->ops->language == language &&
            e->ops->dialect == dialect) {
            return e->ops;
        }
    }
    return NULL;
}

keel_parse_status_t keel_parser_registry_parse(
    const keel_parser_registry_t* registry,
    const char* parser_name,
    const keel_parse_input_t* input,
    keel_parse_result_t* result)
{
    if (!registry || !parser_name || !input || !result) {
        return KEEL_PARSE_INTERNAL_ERROR;
    }
    keel_parse_result_init(result);
    const keel_parser_plugin_ops_t* ops =
        keel_parser_registry_lookup(registry, parser_name);
    if (!ops) {
        result->status = KEEL_PARSE_UNSUPPORTED;
        snprintf(result->error_message, sizeof result->error_message,
                 "parser plugin '%s' is not registered or not enabled",
                 parser_name);
        return result->status;
    }
    result->status = ops->parse(input, result);
    return result->status;
}

keel_error_t keel_parser_registry_register_builtins(
    keel_parser_registry_t* registry)
{
    return keel_parser_registry_register(
        registry, keel_parser_builtin_postgresql_sql(), true);
}
