/**
 * @file cli.c
 * @brief Command-line interface: option parsing, usage, and version output.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Extracted from src/main/main.c — no logic changes, only relocation.
 */

#include "keel/main/cli.h"

#include "keel/core/config_migrate.h"  /* KEEL_CONFIG_SCHEMA_VERSION */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

/* ============================================================================
 * Internal: Long Options Table
 * ============================================================================ */

static const struct option long_options[] = {
    {"config",         required_argument, NULL, 'c'},
    {"listen",         required_argument, NULL, 'l'},
    {"port",           required_argument, NULL, 'p'},
    {"host",           required_argument, NULL, 'H'},
    {"backend-port",   required_argument, NULL, 'P'},
    {"workers",        required_argument, NULL, 'w'},
    {"verbose",        no_argument,       NULL, 'v'},
    {"daemon",         no_argument,       NULL, 'd'},
    {"help",           no_argument,       NULL, 'h'},
    {"version",        no_argument,       NULL, 'V'},
    {"strict-auth",    no_argument,       NULL, 1001},
    {"check-config",   no_argument,       NULL, 1002},
    {"migrate-config", required_argument, NULL, 1003},
    {"convert-config", required_argument, NULL, 1004},
    {"output",         required_argument, NULL, 'o'},
    {NULL, 0, NULL, 0}
};

/* ============================================================================
 * Public Functions
 * ============================================================================ */

void print_usage(const char* prog) {
    printf("Usage: %s [OPTIONS]\n", prog);
    printf("\n");
    printf("KEEL - High-Performance Database Proxy\n");
    printf("\n");
    printf("A zero-copy, thread-per-core database proxy for PostgreSQL and MySQL.\n");
    printf("\n");
    printf("Options:\n");
    printf("  -c, --config <file>       Configuration file path\n");
    printf("  -l, --listen <addr>       Listen address (default: 0.0.0.0)\n");
    printf("  -p, --port <port>         Listen port (default: 6432)\n");
    printf("  -H, --host <host>         Backend database host (default: 127.0.0.1)\n");
    printf("  -P, --backend-port <port> Backend database port (default: 5432)\n");
    printf("  -w, --workers <n>         Number of worker threads (default: CPU count)\n");
    printf("  -v, --verbose             Increase verbosity (can be repeated)\n");
    printf("  -d, --daemon              Run as daemon\n");
    printf("  -h, --help                Show this help message\n");
    printf("  -V, --version             Show version information\n");
    printf("      --strict-auth         Reject deprecated auth methods (md5, trust) at startup\n");
    printf("      --check-config         Validate configuration file and exit (0=ok, 1=error)\n");
    printf("      --migrate-config FILE  Migrate an INI config to the current schema (v%d) and exit\n",
           KEEL_CONFIG_SCHEMA_VERSION);
    printf("      --convert-config FILE  Convert config between INI and YAML (format chosen by\n");
    printf("                             output file's extension); requires --output and exits\n");
    printf("      --output FILE          Destination for --migrate-config / --convert-config\n");
    printf("\n");
    printf("Examples:\n");
    printf("  %s -c /etc/keel/keel.ini\n", prog);
    printf("  %s -p 6432 -H localhost -P 5432 -w 4\n", prog);
    printf("  %s --help\n", prog);
    printf("\n");
}

void print_version(void) {
    printf("keel version %d.%d.%d\n", KEEL_VERSION_MAJOR, KEEL_VERSION_MINOR, KEEL_VERSION_PATCH);
    printf("\n");
    printf("Architecture:\n");
    printf("  + Thread-per-core shared-nothing model\n");
    printf("  + Zero-copy I/O where possible\n");
    printf("  + Slab-based session management\n");
    printf("\n");
    printf("I/O Backends:\n");
#ifdef __linux__
    printf("  + io_uring (Linux 5.6+, optimal)\n");
    printf("  + epoll (Linux fallback)\n");
#endif
#ifdef __APPLE__
    printf("  + kqueue (macOS/BSD)\n");
#endif
    printf("\n");
    printf("Protocols:\n");
    printf("  + PostgreSQL (v3 protocol)\n");
    printf("  + MySQL (hardening)\n");
    printf("\n");
    printf("Build info:\n");
    printf("  Compiler: %s\n",
#if defined(__clang__)
        "Clang " __clang_version__
#elif defined(__GNUC__)
        "GCC " __VERSION__
#else
        "Unknown"
#endif
    );
    printf("\n");
}

int cli_parse(int argc, char** argv, options_t* opts) {
    memset(opts, 0, sizeof(*opts));

    /* Defaults */
    opts->listen_addr  = "0.0.0.0";
    opts->listen_port  = 6432;
    opts->backend_host = "127.0.0.1";
    opts->backend_port = 5432;
    opts->num_workers  = 0;   /* 0 = auto-detect */
    opts->log_level    = 2;   /* INFO */

    int c;
    while ((c = getopt_long(argc, argv, "c:l:p:H:P:w:o:vdhV", long_options, NULL)) != -1) {
        switch (c) {
        case 'c':
            opts->config_file = optarg;
            break;
        case 'l':
            opts->listen_addr = optarg;
            break;
        case 'p':
            opts->listen_port = (uint16_t)atoi(optarg);
            break;
        case 'H':
            opts->backend_host = optarg;
            break;
        case 'P':
            opts->backend_port = (uint16_t)atoi(optarg);
            break;
        case 'w':
            opts->num_workers = (uint32_t)atoi(optarg);
            break;
        case 'v':
            opts->log_level++;
            break;
        case 'd':
            opts->daemonize = true;
            break;
        case 'h':
            opts->help = true;
            return 0;
        case 'V':
            opts->version = true;
            return 0;
        case 1001:
            opts->strict_auth = true;
            break;
        case 1002:
            opts->check_config = true;
            break;
        case 1003:
            opts->migrate_in = optarg;
            break;
        case 1004:
            opts->convert_in = optarg;
            break;
        case 'o':
            opts->migrate_out = optarg;
            opts->convert_out = optarg;
            break;
        case '?':
            return -1;
        default:
            break;
        }
    }

    return 0;
}
