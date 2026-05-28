/**
 * @file parser_plugin.h
 * @brief Parser plugin ABI and parse result ownership contract.
 */

#ifndef KEEL_PARSER_PLUGIN_H
#define KEEL_PARSER_PLUGIN_H

#include "keel/parser/semantic_plan.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define KEEL_PARSER_PLUGIN_ABI_VERSION 1u

typedef struct keel_parse_input {
    const uint8_t* data;
    size_t         len;

    keel_language_id_t language;
    keel_dialect_id_t  dialect;

    const char* database;
    const char* user;
    const char* application_name;

    uint64_t session_id;
    uint64_t transaction_id;

    uint32_t protocol_flags;
    uint32_t parser_flags;
} keel_parse_input_t;

typedef enum keel_parse_status {
    KEEL_PARSE_OK = 0,
    KEEL_PARSE_INCOMPLETE,
    KEEL_PARSE_UNSUPPORTED,
    KEEL_PARSE_ERROR,
    KEEL_PARSE_RESOURCE_LIMIT,
    KEEL_PARSE_INTERNAL_ERROR,
} keel_parse_status_t;

typedef struct keel_parse_result {
    keel_parse_status_t status;

    keel_semantic_plan_t plan;

    void* ast;
    void* plugin_private;

    char     error_message[256];
    uint32_t error_position;
} keel_parse_result_t;

typedef struct keel_parser_plugin_ops {
    const char* name;
    const char* version;

    keel_language_id_t language;
    keel_dialect_id_t  dialect;

    int (*init)(const void* config);
    keel_parse_status_t (*parse)(const keel_parse_input_t* input,
                                 keel_parse_result_t* result);
    void (*free_result)(keel_parse_result_t* result);
    void (*shutdown)(void);
    bool (*supports_feature)(const char* feature);
} keel_parser_plugin_ops_t;

typedef int (*keel_parser_plugin_init_v1_fn)(keel_parser_plugin_ops_t* ops);

void keel_parse_result_init(keel_parse_result_t* result);
void keel_parse_result_free(const keel_parser_plugin_ops_t* ops,
                            keel_parse_result_t* result);

const char* keel_parse_status_name(keel_parse_status_t status);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PARSER_PLUGIN_H */
