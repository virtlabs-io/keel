/**
 * @file cli.h
 * @brief Command-line interface: option types, parser, and usage/version printers.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <stdint.h>
#include <stdbool.h>

/* ============================================================================
 * Version Info (shared with main() startup banner)
 * ============================================================================ */

#ifndef KEEL_VERSION_MAJOR
#define KEEL_VERSION_MAJOR 0
#endif
#ifndef KEEL_VERSION_MINOR
#define KEEL_VERSION_MINOR 5
#endif
#ifndef KEEL_VERSION_PATCH
#define KEEL_VERSION_PATCH 4
#endif

/* ============================================================================
 * Command-Line Options
 * ============================================================================ */

typedef struct options {
    const char*     config_file;
    const char*     listen_addr;
    uint16_t        listen_port;
    const char*     backend_host;
    uint16_t        backend_port;
    uint32_t        num_workers;
    int             log_level;
    bool            daemonize;
    bool            help;
    bool            version;
    bool            strict_auth;   /**< --strict-auth: reject deprecated auth methods at startup */
    bool            check_config;  /**< --check-config: validate config file and exit */
    const char*     migrate_in;    /**< --migrate-config IN: migrate INI config to v2 and exit */
    const char*     migrate_out;   /**< --output OUT: destination for --migrate-config */
    const char*     convert_in;    /**< --convert-config IN: convert INI<->YAML and exit */
    const char*     convert_out;   /**< --output OUT: destination for --convert-config (required) */
} options_t;

/**
 * @brief Print CLI usage for the standalone KEEL executable.
 * @param prog Program name used in the generated examples.
 */
void print_usage(const char* prog);

/**
 * @brief Print build, backend, and compiler version details.
 */
void print_version(void);

/**
 * @brief Parse command-line flags into a normalized startup options structure.
 *
 * @param argc Raw argument count provided by the C runtime.
 * @param argv Raw argument vector provided by the C runtime.
 * @param[out] opts Destination structure populated with defaults and overrides.
 * @return 0 on success, or -1 when getopt reports an invalid option.
 */
int cli_parse(int argc, char** argv, options_t* opts);
