/**
 * @file config_internal.h
 * @brief Internal builder API for `keel_config_t` (shared by the INI parser,
 *        the YAML loader, and the migrator).
 *
 * Not part of the public include surface. Stable only within the keelcore
 * library; do not include from anything outside `src/core/`.
 */

#ifndef KEEL_CORE_CONFIG_INTERNAL_H
#define KEEL_CORE_CONFIG_INTERNAL_H

#include "keel/core/ini.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Allocate an empty configuration store.
 *
 * The returned object owns @p path (duplicated) and starts with no sections.
 * Free with `keel_config_free`. Returns `NULL` on allocation failure.
 */
keel_config_t* keel_config_create_empty(const char* path);

/**
 * @brief Set a (section, key) -> value triple in the configuration store.
 *
 * If the (section, key) pair already exists, the existing value is replaced.
 * Both @p section and @p key are required and non-empty; @p value may be the
 * empty string but must be non-NULL.
 *
 * @return 0 on success, -1 on allocation failure or invalid argument.
 */
int keel_config_set(keel_config_t* config,
                    const char* section,
                    const char* key,
                    const char* value);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_CORE_CONFIG_INTERNAL_H */
