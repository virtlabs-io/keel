/**
 * @file ini.h
 * @brief Public API for INI-style configuration parsing and typed lookups.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This header exposes the lightweight configuration interface used across the
 * KEEL runtime. Callers load a parsed configuration tree once, then resolve
 * string, numeric, boolean, and duration values repeatedly during subsystem
 * initialization or configuration reload. The implementation preserves simple
 * INI semantics while supporting repeated section iteration for compound
 * constructs such as worker groups, server pools, and plugin blocks.
 */

#ifndef KEEL_INI_H
#define KEEL_INI_H

#include "keel_types.h"
#include "keel_error.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Opaque configuration handle returned by the parser. */
typedef struct keel_config keel_config_t;

/**
 * @brief Parse an INI configuration file into an in-memory representation.
 *
 * The parser reads the file eagerly, normalizes sections and key/value pairs,
 * and returns an owned configuration object. On failure, the function returns
 * `NULL` after logging the parse or I/O issue.
 *
 * @param path Absolute or relative path to the configuration file.
 * @return Parsed configuration handle, or `NULL` if the file could not be read
 *         or contained invalid configuration content.
 */
keel_config_t* keel_config_load(const char* path);

/**
 * @brief Release a parsed configuration object and all nested allocations.
 *
 * Passing `NULL` is allowed and results in a no-op, which simplifies cleanup
 * paths after partially successful initialization.
 *
 * @param config Configuration object to free, or `NULL`.
 * @return
 */
void keel_config_free(keel_config_t* config);

/**
 * @brief Resolve a configuration value as a string.
 *
 * The returned pointer aliases memory owned by the configuration object. The
 * caller must not free or modify it. If the section or key is not present,
 * the provided default is returned unchanged.
 *
 * @param config Parsed configuration handle.
 * @param section Section name to search.
 * @param key Key name within the section.
 * @param default_val Fallback returned when the key is absent.
 * @return Stored string value, or `default_val` when the key does not exist.
 */
const char* keel_config_get_string(const keel_config_t* config,
                                   const char* section,
                                   const char* key,
                                   const char* default_val);

/**
 * @brief Resolve a configuration value as a signed integer.
 *
 * The parser accepts decimal integer text. Missing keys, empty values, or
 * invalid numeric content all fall back to the supplied default.
 *
 * @param config Parsed configuration handle.
 * @param section Section name to search.
 * @param key Key name within the section.
 * @param default_val Fallback value used when parsing fails or the key is not present.
 * @return Parsed integer value, or `default_val` on absence or conversion failure.
 */
int64_t keel_config_get_int(const keel_config_t* config,
                            const char* section,
                            const char* key,
                            int64_t default_val);

/**
 * @brief Resolve a configuration value as a floating-point number.
 *
 * @param config Parsed configuration handle.
 * @param section Section name to search.
 * @param key Key name within the section.
 * @param default_val Fallback value used when parsing fails or the key is missing.
 * @return Parsed floating-point value, or `default_val` when unavailable.
 */
double keel_config_get_float(const keel_config_t* config,
                             const char* section,
                             const char* key,
                             double default_val);

/**
 * @brief Resolve a configuration value as a boolean flag.
 *
 * Typical accepted representations include `true` or `false`, `yes` or `no`,
 * and `on` or `off`. Unknown or missing values use the supplied default.
 *
 * @param config Parsed configuration handle.
 * @param section Section name to search.
 * @param key Key name within the section.
 * @param default_val Fallback value when the key is absent or unrecognized.
 * @return Parsed boolean value, or `default_val` if no valid boolean is present.
 */
bool keel_config_get_bool(const keel_config_t* config,
                          const char* section,
                          const char* key,
                          bool default_val);

/**
 * @brief Resolve a configuration value as a duration.
 *
 * Supported suffixes are `ns`, `us`, `ms`, `s`, `m`, and `h`. A bare numeric
 * value (no suffix) is interpreted as **milliseconds**, which matches the
 * v2 INI/YAML schema where duration keys no longer carry a `_ms` suffix in
 * the key name and the unit lives in the value instead.
 *
 * Missing or malformed values fall back to `default_val` so that callers can
 * provide subsystem-specific safe defaults.
 *
 * @param config Parsed configuration handle.
 * @param section Section name to search.
 * @param key Key name within the section.
 * @param default_val Fallback duration when the key is absent or invalid.
 * @return Parsed duration value, or `default_val` on failure.
 */
keel_duration_t keel_config_get_duration(const keel_config_t* config,
                                        const char* section,
                                        const char* key,
                                        keel_duration_t default_val);

/**
 * @brief Resolve a configuration value as a byte count.
 *
 * Supported suffixes (case-insensitive) are:
 *   - `B`                  — bytes (no scaling)
 *   - `KB`  / `K`          — 1000 bytes        (decimal)
 *   - `KiB`                — 1024 bytes        (binary)
 *   - `MB`  / `M`          — 1000² bytes
 *   - `MiB`                — 1024² bytes
 *   - `GB`  / `G`          — 1000³ bytes
 *   - `GiB`                — 1024³ bytes
 *
 * A bare numeric value (no suffix) is interpreted as **bytes**. Missing or
 * malformed values fall back to `default_val`.
 *
 * @param config Parsed configuration handle.
 * @param section Section name to search.
 * @param key Key name within the section.
 * @param default_val Fallback byte count when the key is absent or invalid.
 * @return Parsed byte count, or `default_val` on failure.
 */
int64_t keel_config_get_bytes(const keel_config_t* config,
                              const char* section,
                              const char* key,
                              int64_t default_val);

/**
 * @brief Check whether a named section exists in the loaded configuration.
 *
 * @param config Parsed configuration handle.
 * @param section Section name to test.
 * @return `true` if the section exists, otherwise `false`.
 */
bool keel_config_has_section(const keel_config_t* config, const char* section);

/**
 * @brief Visit every section in the configuration.
 *
 * Iteration order follows the internal linked-list representation and should
 * not be treated as stable across parser changes. The callback is invoked once
 * per section name.
 *
 * @param config Parsed configuration handle.
 * @param callback Callback invoked for each section name.
 * @param ctx Opaque caller context forwarded to `callback`.
 * @return
 */
void keel_config_iter_sections(const keel_config_t* config,
                               void (*callback)(const char* section, void* ctx),
                               void* ctx);

/**
 * @brief Visit every key/value pair stored in a given section.
 *
 * If the section does not exist, no callbacks are issued.
 *
 * @param config Parsed configuration handle.
 * @param section Section whose keys should be enumerated.
 * @param callback Callback invoked with each key and value.
 * @param ctx Opaque caller context forwarded to `callback`.
 * @return
 */
void keel_config_iter_keys(const keel_config_t* config,
                           const char* section,
                           void (*callback)(const char* key, const char* value, void* ctx),
                           void* ctx);

/**
 * @brief Visit every section whose name begins with a given prefix.
 * 
 * This helper is used for dynamic blocks such as `worker_group.*` or
 * `servers.*`, where the caller wants all peer sections without knowing their
 * exact names in advance.
 *
 * @param config Parsed configuration handle.
 * @param prefix Section name prefix to match, for example `worker_group.`.
 * @param callback Callback invoked for each matching full section name.
 * @param ctx Opaque caller context forwarded to `callback`.
 * @return
 */
void keel_config_iter_sections_prefix(const keel_config_t* config,
                                      const char* prefix,
                                      void (*callback)(const char* section, void* ctx),
                                      void* ctx);

/**
 * @brief Count the number of sections whose names start with a prefix.
 *
 * @param config Parsed configuration handle.
 * @param prefix Section prefix to match.
 * @return Number of matching sections.
 */
size_t keel_config_count_sections_prefix(const keel_config_t* config, const char* prefix);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_INI_H */
