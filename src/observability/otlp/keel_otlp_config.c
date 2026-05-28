/**
 * @file keel_otlp_config.c
 * @brief INI-driven configuration loader for the OTLP exporter.
 */

#include "keel_otlp_config.h"

#include "keel/core/ini.h"

#include <stdint.h>
#include <string.h>

#define SECTION "observability"

int keel_otlp_config_load(const keel_config_t* config,
                          keel_otlp_exporter_config_t* out_cfg,
                          bool* out_enabled)
{
    if (!config || !out_cfg || !out_enabled)
        return -1;

    /* Apply baseline defaults regardless of whether the section exists. */
    out_cfg->http.endpoint_url = NULL;
    out_cfg->http.timeout_ms   = 5000;
    out_cfg->http.bearer_token = NULL;
    out_cfg->interval_ms       = 5000;
    out_cfg->max_retries       = 2;
    out_cfg->queue_capacity    = 4;
    out_cfg->encode_buf_bytes  = 65536;
    *out_enabled               = false;

    if (!keel_config_has_section(config, SECTION))
        return 0;

    bool        enabled = keel_config_get_bool(config, SECTION,
                                               "otlp_enabled", false);
    const char* url     = keel_config_get_string(config, SECTION,
                                                 "otlp_endpoint_url", NULL);
    const char* token   = keel_config_get_string(config, SECTION,
                                                 "otlp_bearer_token", NULL);

    out_cfg->http.endpoint_url = url;
    out_cfg->http.bearer_token = (token && token[0] != '\0') ? token : NULL;

    int64_t v;
    v = keel_config_get_duration_ms(config, SECTION, "otlp_timeout", 5000);
    if (v > 0 && v <= UINT32_MAX) out_cfg->http.timeout_ms = (uint32_t)v;

    v = keel_config_get_duration_ms(config, SECTION, "otlp_interval", 5000);
    if (v > 0 && v <= UINT32_MAX) out_cfg->interval_ms = (uint32_t)v;

    v = keel_config_get_int(config, SECTION, "otlp_max_retries", 2);
    if (v >= 0 && v <= UINT32_MAX) out_cfg->max_retries = (uint32_t)v;

    v = keel_config_get_int(config, SECTION, "otlp_queue_capacity", 4);
    if (v > 0 && v <= UINT32_MAX) out_cfg->queue_capacity = (uint32_t)v;

    v = keel_config_get_bytes(config, SECTION, "otlp_encode_buf", 65536);
    if (v > 0 && v <= UINT32_MAX) out_cfg->encode_buf_bytes = (uint32_t)v;

    *out_enabled = enabled && url && url[0] != '\0';
    return 0;
}
