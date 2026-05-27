/**
 * @file parser_dispatch.c
 * @brief Generic parser dispatch helpers.
 */

#include "keel/parser/parser_plugin.h"

#include <string.h>

void keel_parse_result_init(keel_parse_result_t* result)
{
    if (!result) return;
    memset(result, 0, sizeof *result);
    result->status = KEEL_PARSE_INTERNAL_ERROR;
    keel_semantic_plan_init(&result->plan,
                            KEEL_LANG_SQL,
                            KEEL_DIALECT_UNKNOWN);
}

void keel_parse_result_free(const keel_parser_plugin_ops_t* ops,
                            keel_parse_result_t* result)
{
    if (!result) return;
    if (ops && ops->free_result) {
        ops->free_result(result);
    }
    memset(result, 0, sizeof *result);
}

const char* keel_parse_status_name(keel_parse_status_t status)
{
    switch (status) {
    case KEEL_PARSE_OK: return "ok";
    case KEEL_PARSE_INCOMPLETE: return "incomplete";
    case KEEL_PARSE_UNSUPPORTED: return "unsupported";
    case KEEL_PARSE_ERROR: return "error";
    case KEEL_PARSE_RESOURCE_LIMIT: return "resource_limit";
    case KEEL_PARSE_INTERNAL_ERROR: return "internal_error";
    default: return "invalid";
    }
}
