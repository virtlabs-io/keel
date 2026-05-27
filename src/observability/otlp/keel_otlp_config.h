/**
 * @file keel_otlp_config.h
 * @brief Parse `[observability]` INI section into a
 *        `keel_otlp_exporter_config_t`.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Recognized keys (all optional, sensible defaults applied):
 * - `otlp_enabled`           (bool,   default false)
 * - `otlp_endpoint_url`      (string, no default; required when enabled)
 * - `otlp_timeout_ms`        (int,    default 5000)
 * - `otlp_bearer_token`      (string, no default; optional)
 * - `otlp_interval_ms`       (int,    default 5000)
 * - `otlp_max_retries`       (int,    default 2)
 * - `otlp_queue_capacity`    (int,    default 4)
 * - `otlp_encode_buf_bytes`  (int,    default 65536)
 *
 * String pointers in the returned config alias config-owned memory. The
 * caller must keep `config` alive for the lifetime of the exporter.
 */
#ifndef KEEL_OTLP_CONFIG_H
#define KEEL_OTLP_CONFIG_H

#include <stdbool.h>

#include "keel_otlp_exporter.h"

struct keel_config;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Parse the `[observability]` section.
 *
 * @param config       Parsed INI handle (non-NULL).
 * @param out_cfg      Populated with exporter configuration on return.
 * @param out_enabled  Set to true when `otlp_enabled=true` AND a non-empty
 *                     `otlp_endpoint_url` was provided.
 * @return 0 on success, -1 on invalid arguments.
 */
int keel_otlp_config_load(const struct keel_config* config,
                          keel_otlp_exporter_config_t* out_cfg,
                          bool* out_enabled);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_OTLP_CONFIG_H */
