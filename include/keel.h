/**
 * @file keel.h
 * @brief Main KEEL header - includes all public APIs
 *
 * This is the main entry point for the KEEL library.
 * Include this header to access all KEEL functionality.
 */

#ifndef KEEL_H
#define KEEL_H

#include "keel/core/ini.h"
#include "keel_types.h"
#include "keel_error.h"
#include "keel/log/log.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Initialize the KEEL library
 *
 * Must be called before any other KEEL function.
 * This function is thread-safe and can be called multiple times;
 * only the first call has effect.
 *
 * @return KEEL_OK on success, error code otherwise
 */
keel_error_t keel_init(void);

/**
 * @brief Shutdown the KEEL library
 *
 * Must be called when done using KEEL. Cleans up all resources.
 * After calling this, keel_init() must be called again to use KEEL.
 */
void keel_shutdown(void);

/**
 * @brief Get KEEL version string
 *
 * @return Version string in format "major.minor.patch"
 */
const char* keel_version(void);

/**
 * @brief Get KEEL version components
 *
 * @param[out] major Major version number
 * @param[out] minor Minor version number
 * @param[out] patch Patch version number
 */
void keel_version_numbers(int* major, int* minor, int* patch);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_H */
