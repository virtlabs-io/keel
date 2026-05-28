/**
 * @file config_yaml.h
 * @brief YAML configuration loader and INI<->YAML converter.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Since config schema v2, KEEL accepts both INI and YAML configuration files.
 * The YAML form is structural (top-level mappings per section, a
 * `worker_groups:` list with embedded `servers:` lists) and flattens onto the
 * exact same key namespace as the INI form via `keel_config_set`, so all
 * downstream `keel_config_get_*` accessors are unchanged.
 *
 * The YAML loader supports:
 *   - top-level scalar `config_version: 2` (recorded under the `[keel]` section)
 *   - one mapping per INI section: `keel:`, `logging:`, `security:`,
 *     `observability:`, etc. — flat scalar keys are passed through 1:1.
 *   - `worker_groups:` as a list of mappings. Each entry's `name:` becomes
 *     part of the synthetic section name `[worker_group.<name>]`. All other
 *     scalar keys in the entry are recorded in that section. The optional
 *     `servers:` sub-list is recorded under `[worker_group.<name>.servers]`
 *     with each entry's `name:` as the key and the remaining fields packed
 *     into a single `host=H port=P role=R weight=W [...]` value string, so
 *     downstream router code can keep parsing both forms uniformly.
 *   - `${ENV}` interpolation inside scalar values (escape with `$$`).
 *
 * Unknown keys (per the schema in config_schema.c) are reported and rejected,
 * giving the operator a clear hint about typos.
 */

#ifndef KEEL_CORE_CONFIG_YAML_H
#define KEEL_CORE_CONFIG_YAML_H

#include "keel/core/ini.h"
#include "keel_error.h"

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Load a YAML configuration file from disk.
 *
 * On success the returned object behaves identically to the INI loader's
 * result: pass it to `keel_config_get_*` and `keel_config_free` as usual.
 *
 * @param path Filesystem path to the YAML file.
 * @return Configuration object, or `NULL` on parse/IO failure. Errors are
 *         logged via `KEEL_LOG_ERROR`.
 */
keel_config_t* keel_config_load_yaml(const char* path);

/**
 * @brief Detect the configuration format from a file path's extension.
 *
 * Recognized extensions: `.yaml`, `.yml` (YAML); anything else (or no
 * extension) is treated as INI. The returned enum drives the dispatch
 * inside `keel_config_load_auto`.
 */
typedef enum {
    KEEL_CONFIG_FORMAT_INI  = 0,
    KEEL_CONFIG_FORMAT_YAML = 1
} keel_config_format_t;

keel_config_format_t keel_config_detect_format(const char* path);

/**
 * @brief Load a configuration file, auto-detecting INI vs YAML by extension.
 */
keel_config_t* keel_config_load_auto(const char* path);

/**
 * @brief Convert an INI file to YAML on disk (lossless round-trip).
 *
 * Reads @p in_path as INI, emits an equivalent YAML document to @p out_path.
 *
 * @return KEEL_OK on success.
 */
keel_error_t keel_config_convert_ini_to_yaml(const char* in_path,
                                             const char* out_path);

/**
 * @brief Convert a YAML file to INI on disk (lossless round-trip).
 *
 * Reads @p in_path as YAML, emits an equivalent INI document to @p out_path.
 *
 * @return KEEL_OK on success.
 */
keel_error_t keel_config_convert_yaml_to_ini(const char* in_path,
                                             const char* out_path);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_CORE_CONFIG_YAML_H */
