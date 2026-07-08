/**
 * @file stats_display.h
 * @brief Stats snapshot printer, instrumentation mask builders, and
 *        boolean environment-variable helpers.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <stdbool.h>
#include <stdint.h>
#include "keel/core/ini.h"

/* ============================================================================
 * Boolean / Environment Helpers
 * (also used by worker_group.c and config_load.c)
 * ============================================================================ */

/**
 * @brief Read a boolean env-var that defaults to enabled when unset.
 *
 * @param name Environment-variable name.
 * @return true when unset or affirmative; false only for explicit negative tokens.
 */
bool env_enabled_default_true(const char* name);

/**
 * @brief Read a boolean env-var that defaults to disabled when unset.
 *
 * @param name Environment-variable name.
 * @return true only for explicit affirmative tokens; otherwise false.
 */
bool env_enabled_default_false(const char* name);

/**
 * @brief Parse a textual boolean ("true"/"1"/"on"/"yes" or "false"/"0"/"off"/"no").
 *
 * @param v   Candidate string.
 * @param[out] out Parsed result when the function returns true.
 * @return true on success, false if @p v is NULL, empty, or unrecognised.
 */
bool parse_bool_string(const char* v, bool* out);

/* ============================================================================
 * Instrumentation Mask Builders
 * ============================================================================ */

/** @brief Build the system-probe mask from KEEL_INSTR_* environment variables. */
uint32_t build_system_instr_mask_from_env(void);

/** @brief Build the hot-path timing mask from KEEL_HOT_INSTR_* env vars. */
uint32_t build_hotpath_instr_mask_from_env(void);

/**
 * @brief Refine the hot-path mask with overrides from a [stats] config section.
 *
 * @param cfg           Loaded configuration; no-op when NULL.
 * @param current_mask  Mask seeded from environment defaults.
 * @return Effective hot-path bitmask.
 */
uint32_t apply_hotpath_instr_mask_from_config(const keel_config_t* cfg,
                                              uint32_t current_mask);

/**
 * @brief Refine the function-probe category mask from an [instrument] config section.
 *
 * @param cfg           Loaded configuration; no-op when NULL.
 * @param current_mask  Mask seeded from defaults.
 * @return Effective KEEL_INSTR_CAT_* bitmask.
 */
uint32_t apply_instr_mask_from_config(const keel_config_t* cfg,
                                      uint32_t current_mask);

/* ============================================================================
 * Stats Dump
 * ============================================================================ */

/**
 * @brief Format and print a stats snapshot to stdout.
 *
 * Reads g_engine (declared in config_load.h) and prints all active
 * instrumentation levels.  Called on SIGUSR1 and on the periodic timer.
 */
void stats_dump(void);
