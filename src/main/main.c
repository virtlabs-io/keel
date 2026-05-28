/**
 * @file main.c
 * @brief Process bootstrap, configuration materialization, and runtime policy wiring for KEEL.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License along
 * with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 * This translation unit is the control-plane bridge between static configuration
 * data and the runtime subsystems implemented elsewhere in the tree. It does not
 * implement query forwarding itself; instead it performs the orchestration work
 * required to launch the real data-plane components in a consistent order:
 *
 * - parse CLI flags and optional configuration files;
 * - discover worker groups and expand each section into concrete runtime state;
 * - derive per-group listener, server-pool, probe, hook, TLS, and security
 *   settings from user configuration while preserving safe defaults;
 * - instantiate admin, logging, statistics, probes, engines, and worker pools;
 * - apply process-level hardening such as privilege dropping and seccomp;
 * - coordinate operational signals for shutdown, stats dumping, and reload.
 *
 * The implementation favors a front-loaded bootstrap pipeline instead of a more
 * dynamic dependency-injection framework. That choice keeps startup transparent
 * in C, reduces heap churn before the workers are online, and makes it easy to
 * reason about failure rollback. The cost is that this file is intentionally
 * broad: it understands a large slice of subsystem interfaces and therefore must
 * document the order, invariants, and tradeoffs of that orchestration clearly.
 *
 * A second notable design choice is the split between restart-required and
 * live-reloadable settings. Listener topology, worker counts, protocol tiering,
 * and some deep routing semantics are fixed at startup because changing them in
 * place would invalidate thread-local assumptions or require cross-thread
 * migration logic. In contrast, pool sizing, probe timing, and similar knobs are
 * propagated into the live engine because they can be updated without tearing
 * down active sessions.
 */

#include "keel/engine/engine.h"
#include "keel/engine/worker.h"
#include "keel/engine/backend_pool.h"
#include "keel/reactor/reactor.h"
#include "keel/core/stats.h"
#include "keel/core/admin.h"
#include "keel/core/cluster.h"
#include "keel/core/config_reload.h"
#include "keel/probe/probe.h"
#include "keel/mem/mem.h"
#include "keel_error.h"
#include "keel/core/ini.h"
#include "keel/core/config_migrate.h"
#include "keel/core/auth.h"
#include "keel/log/log.h"
#include "keel/log/log_plugin.h"
#include "keel/log/query_log.h"
#include "keel/log/audit_log.h"
#include "keel/protocol/tls_context.h"
#include "keel/protocol/tls_auto.h"
#include "keel_hook.h"
#include "keel/core/query_rules.h"
#include "keel/core/throttle.h"
#include "keel/core/router_discovery.h"
#include "keel/trace/trace.h"
#include "keel/core/router.h"
#include "keel/core/sharding.h"
#include "keel/core/router_plugin.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <limits.h>
#include <getopt.h>
#include <unistd.h>
#include <signal.h>
#include <stdatomic.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <pthread.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#include <sys/prctl.h>

#ifdef __linux__
#include <linux/seccomp.h>
#include <linux/filter.h>
#include <linux/audit.h>
#include <sys/syscall.h>

#if defined(__x86_64__)
#define KEEL_AUDIT_ARCH_NATIVE AUDIT_ARCH_X86_64
#elif defined(__aarch64__)
#define KEEL_AUDIT_ARCH_NATIVE AUDIT_ARCH_AARCH64
#elif defined(__i386__)
#define KEEL_AUDIT_ARCH_NATIVE AUDIT_ARCH_I386
#else
#define KEEL_AUDIT_ARCH_NATIVE 0u
#endif
#endif

#include "keel/util/platform_compat.h"

/* ============================================================================
 * Version Info
 * ============================================================================ */

#ifndef KEEL_VERSION_MAJOR
#define KEEL_VERSION_MAJOR 0
#endif
#ifndef KEEL_VERSION_MINOR
#define KEEL_VERSION_MINOR 2
#endif
#ifndef KEEL_VERSION_PATCH
#define KEEL_VERSION_PATCH 0
#endif

/* ============================================================================
 * Command Line Options
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
    const char*     migrate_out;   /**< --output OUT: destination for --migrate-config (default: stdout) */
} options_t;

static const struct option long_options[] = {
    {"config",       required_argument, NULL, 'c'},
    {"listen",       required_argument, NULL, 'l'},
    {"port",         required_argument, NULL, 'p'},
    {"host",         required_argument, NULL, 'H'},
    {"backend-port", required_argument, NULL, 'P'},
    {"workers",      required_argument, NULL, 'w'},
    {"verbose",      no_argument,       NULL, 'v'},
    {"daemon",       no_argument,       NULL, 'd'},
    {"help",         no_argument,       NULL, 'h'},
    {"version",      no_argument,       NULL, 'V'},
    {"strict-auth",    no_argument,       NULL, 1001},
    {"check-config",   no_argument,       NULL, 1002},
    {"migrate-config", required_argument, NULL, 1003},
    {"output",         required_argument, NULL, 'o'},
    {NULL, 0, NULL, 0}
};

/**
 * @brief Print CLI usage for the standalone KEEL executable.
 *
 * The usage text intentionally mirrors the minimal command-line surface exposed
 * by this file. Most production deployments are expected to rely on the INI
 * configuration path, but the executable keeps a small direct-override surface
 * for quick local tests, smoke runs, and container entrypoints.
 *
 * @param prog Program name used in the generated examples.
 * @return
 */
static void print_usage(const char* prog) {
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
    printf("      --output FILE          Destination for --migrate-config (default: stdout)\n");
    printf("\n");
    printf("Examples:\n");
    printf("  %s -c /etc/keel/keel.ini\n", prog);
    printf("  %s -p 6432 -H localhost -P 5432 -w 4\n", prog);
    printf("  %s --help\n", prog);
    printf("\n");
}

/**
 * @brief Print build, backend, and compiler version details.
 *
 * The version output is more than cosmetic: operators use it to confirm which
 * async-I/O backends, protocol implementations, and toolchain family were built
 * into the binary when debugging field issues. Keeping this close to the entry
 * point avoids a dependency on a heavier runtime metadata subsystem during very
 * early startup.
 *
 * @return
 */
static void print_version(void) {
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
    printf("  - MySQL (planned)\n");
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

/**
 * @brief Parse command-line flags into a normalized startup options structure.
 *
 * The parser intentionally does only syntactic decoding. It records direct
 * overrides and leaves semantic validation to later bootstrap stages where file
 * configuration, environment defaults, and subsystem-specific constraints are
 * all visible together. This keeps the CLI layer lightweight and avoids having
 * to duplicate validation rules that already exist in the configuration path.
 *
 * @param argc Raw argument count provided by the C runtime.
 * @param argv Raw argument vector provided by the C runtime.
 * @param[out] opts Destination structure populated with defaults and overrides.
 * @return 0 on success, or -1 when getopt reports an invalid option.
 */
static int parse_options(int argc, char** argv, options_t* opts) {
    memset(opts, 0, sizeof(*opts));
    
    /* Defaults */
    opts->listen_addr = "0.0.0.0";
    opts->listen_port = 6432;
    opts->backend_host = "127.0.0.1";
    opts->backend_port = 5432;
    opts->num_workers = 0;  /* 0 = auto-detect */
    opts->log_level = 2;    /* INFO */
    
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
        case 'o':
            opts->migrate_out = optarg;
            break;
        case '?':
            return -1;
        default:
            break;
        }
    }
    
    return 0;
}

/* ============================================================================
 * Daemonization
 * ============================================================================ */

/**
 * @brief Detach the process from the controlling terminal using the classic double-fork pattern.
 *
 * The implementation uses the conventional sequence of fork, setsid, and second
 * fork so the final process cannot accidentally reacquire a controlling terminal.
 * That behavior matters for service-style deployments because accidental terminal
 * attachment changes signal delivery and lifecycle expectations. KEEL performs
 * only the minimal daemonization steps here; it does not reopen stdio onto log
 * files because logging is handled by the pluggable logging subsystem later in
 * startup.
 *
 * @return 0 when daemonization succeeds, or -1 when any fork, setsid, or chdir
 *         step fails.
 */
static int daemonize_process(void) {
    pid_t pid = fork();
    
    if (pid < 0) {
        perror("fork");
        return -1;
    }
    
    if (pid > 0) {
        /* Parent exits */
        _exit(0);
    }
    
    /* Child continues as daemon */
    if (setsid() < 0) {
        perror("setsid");
        return -1;
    }
    
    /* Second fork to prevent acquiring controlling terminal */
    pid = fork();
    if (pid < 0) {
        perror("fork");
        return -1;
    }
    
    if (pid > 0) {
        _exit(0);
    }
    
    /* Change to root directory */
    if (chdir("/") < 0) {
        perror("chdir");
        return -1;
    }
    
    /* Close standard file descriptors */
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
    
    return 0;
}

/* ============================================================================
 * Socket Creation
 * ============================================================================ */

/**
 * @brief Create, tune, bind, and listen on a frontend TCP socket.
 *
 * The listener is configured once in the control plane and then handed to the
 * engine/worker layer. Socket options are chosen for low-latency proxy service:
 * `SO_REUSEADDR` eases restarts, `SO_REUSEPORT` allows scalable accept patterns
 * on platforms that support it, `TCP_NODELAY` prevents small-response latency
 * from being amplified by Nagle, and non-blocking mode keeps the descriptor
 * compatible with reactor-driven accept loops. Address parsing deliberately
 * falls back to `INADDR_ANY` when the text address is not an IPv4 literal so
 * misconfigured values degrade into a broad bind rather than crashing during the
 * parse step; the later bind call still provides the real success/failure result.
 *
 * @param addr Text IPv4 listen address. Non-literals fall back to `0.0.0.0`.
 * @param port TCP port to bind.
 * @param backlog Requested listen backlog. Values less than or equal to zero are
 *                normalized to a conservative default.
 * @return A non-negative socket descriptor on success, or -1 on failure.
 */
static int create_listen_socket(const char* addr, uint16_t port, uint32_t backlog) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Failed to create socket: %s", strerror(errno));
        return -1;
    }
    
    /* Set socket options */
    int opt = 1;
    
    /* SO_REUSEADDR - allow quick restart */
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "Failed to set SO_REUSEADDR: %s", strerror(errno));
    }
    
    /* SO_REUSEPORT - allow multiple threads to accept */
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "Failed to set SO_REUSEPORT: %s", strerror(errno));
    }
    
    /* TCP_NODELAY - disable Nagle's algorithm */
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) < 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "Failed to set TCP_NODELAY: %s", strerror(errno));
    }
    
    /* Set non-blocking */
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
    
    /* Bind */
    struct sockaddr_in bind_addr;
    memset(&bind_addr, 0, sizeof(bind_addr));
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, addr, &bind_addr.sin_addr) != 1) {
        bind_addr.sin_addr.s_addr = INADDR_ANY;
    }
    
    if (bind(fd, (struct sockaddr*)&bind_addr, sizeof(bind_addr)) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Failed to bind to %s:%d: %s", addr, port, strerror(errno));
        close(fd);
        return -1;
    }
    
    /* Listen with configurable backlog */
    int bl = (backlog > 0) ? (int)backlog : 4096;
    if (listen(fd, bl) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Failed to listen: %s", strerror(errno));
        close(fd);
        return -1;
    }
    
    return fd;
}

/* ============================================================================
 * Signal Handling
 * ============================================================================ */

static volatile sig_atomic_t g_should_stop = 0;
static volatile sig_atomic_t g_dump_stats = 0;
static keel_engine_t* g_engine = NULL;

/**
 * @brief Read a boolean-like environment variable that defaults to enabled.
 *
 * This helper implements the permissive interpretation used for instrumentation
 * toggles: an unset variable means "leave enabled," while an explicit negative
 * token disables the feature. That policy is useful for master switches such as
 * `KEEL_INSTR_ALL`, where operators often want full visibility unless they have
 * a reason to reduce overhead.
 *
 * @param name Environment-variable name.
 * @return `true` when the variable is unset or contains an affirmative value,
 *         and `false` only for explicit negative tokens.
 */
static bool env_enabled_default_true(const char *name)
{
    const char *v = getenv(name);
    if (!v || !*v) return true;
    if (strcasecmp(v, "0") == 0 ||
        strcasecmp(v, "false") == 0 ||
        strcasecmp(v, "off") == 0 ||
        strcasecmp(v, "no") == 0) {
        return false;
    }
    return true;
}

/**
 * @brief Read a boolean-like environment variable that defaults to disabled.
 *
 * This is the complement of `env_enabled_default_true()` and is used for feature
 * bits that should remain off unless explicitly requested.
 *
 * @param name Environment-variable name.
 * @return `true` only for explicit affirmative tokens, otherwise `false`.
 */
static bool env_enabled_default_false(const char *name)
{
    const char *v = getenv(name);
    if (!v || !*v) return false;
    if (strcasecmp(v, "1") == 0 ||
        strcasecmp(v, "true") == 0 ||
        strcasecmp(v, "on") == 0 ||
        strcasecmp(v, "yes") == 0) {
        return true;
    }
    return false;
}

/**
 * @brief Parse a textual boolean into a caller-provided output slot.
 *
 * The function accepts multiple operator-friendly spellings so both environment
 * variables and INI settings can share the same parser. It reports parse failure
 * instead of silently choosing a default so callers can distinguish between
 * "unset/invalid" and an explicit on/off override.
 *
 * @param v Candidate boolean string.
 * @param[out] out Parsed boolean value when the function returns `true`.
 * @return `true` when parsing succeeded, otherwise `false`.
 */
static bool parse_bool_string(const char *v, bool *out)
{
    if (!v || !*v || !out)
        return false;

    if (strcasecmp(v, "1") == 0 ||
        strcasecmp(v, "true") == 0 ||
        strcasecmp(v, "on") == 0 ||
        strcasecmp(v, "yes") == 0) {
        *out = true;
        return true;
    }

    if (strcasecmp(v, "0") == 0 ||
        strcasecmp(v, "false") == 0 ||
        strcasecmp(v, "off") == 0 ||
        strcasecmp(v, "no") == 0) {
        *out = false;
        return true;
    }

    return false;
}

/**
 * @brief Build the system-probe instrumentation mask from environment variables.
 *
 * The mask is assembled bit-by-bit so operators can globally enable all probes
 * and then subtract expensive categories, or start from zero and opt specific
 * categories in. This two-level policy is easier to operate than a single comma-
 * separated string and avoids parsing allocations during early startup.
 *
 * @return Bitmask composed from `KEEL_STAT_SYS_*` flags.
 */
static uint32_t build_system_instr_mask_from_env(void)
{
    bool default_all = env_enabled_default_true("KEEL_INSTR_ALL");

    uint32_t mask = 0;
    if (default_all ? env_enabled_default_true("KEEL_INSTR_CPU")
                    : env_enabled_default_false("KEEL_INSTR_CPU"))
        mask |= KEEL_STAT_SYS_CPU;
    if (default_all ? env_enabled_default_true("KEEL_INSTR_MEMORY")
                    : env_enabled_default_false("KEEL_INSTR_MEMORY"))
        mask |= KEEL_STAT_SYS_MEMORY;
    if (default_all ? env_enabled_default_true("KEEL_INSTR_FD")
                    : env_enabled_default_false("KEEL_INSTR_FD"))
        mask |= KEEL_STAT_SYS_FD;
    if (default_all ? env_enabled_default_true("KEEL_INSTR_DISK")
                    : env_enabled_default_false("KEEL_INSTR_DISK"))
        mask |= KEEL_STAT_SYS_DISK;
    if (default_all ? env_enabled_default_true("KEEL_INSTR_NETWORK")
                    : env_enabled_default_false("KEEL_INSTR_NETWORK"))
        mask |= KEEL_STAT_SYS_NETWORK;

    return mask;
}

/**
 * @brief Build the hot-path timing instrumentation mask from environment variables.
 *
 * Hot-path probes are treated separately from broad subsystem instrumentation
 * because they can add cost in extremely latency-sensitive regions. Keeping them
 * behind a distinct mask lets operators enable coarse statistics continuously and
 * only enable nanosecond timing around queueing or deferred-send hotspots during
 * focused investigations.
 *
 * @return Bitmask composed from `KEEL_HOT_INSTR_*` flags.
 */
static uint32_t build_hotpath_instr_mask_from_env(void)
{
    bool default_all = env_enabled_default_true("KEEL_HOT_INSTR_ALL");

    uint32_t mask = 0;
    if (default_all ? env_enabled_default_true("KEEL_HOT_INSTR_WAIT_POOL")
                    : env_enabled_default_false("KEEL_HOT_INSTR_WAIT_POOL"))
        mask |= KEEL_HOT_INSTR_WAIT_POOL;
    if (default_all ? env_enabled_default_true("KEEL_HOT_INSTR_WAIT_BACKEND")
                    : env_enabled_default_false("KEEL_HOT_INSTR_WAIT_BACKEND"))
        mask |= KEEL_HOT_INSTR_WAIT_BACKEND;
    if (default_all ? env_enabled_default_true("KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT")
                    : env_enabled_default_false("KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT"))
        mask |= KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT;
    if (default_all ? env_enabled_default_true("KEEL_HOT_INSTR_DEFERRED_SEND")
                    : env_enabled_default_false("KEEL_HOT_INSTR_DEFERRED_SEND"))
        mask |= KEEL_HOT_INSTR_DEFERRED_SEND;

    return mask;
}

/**
 * @brief Apply hot-path instrumentation overrides from the loaded configuration.
 *
 * Environment variables establish the bootstrap mask before the config file is
 * parsed. This helper then lets the file refine that baseline so deployment-wide
 * defaults can live in configuration rather than service wrappers.
 *
 * @param cfg Loaded configuration object, or `NULL` when no config file exists.
 * @param current_mask Environment-derived mask to refine.
 * @return Effective hot-path instrumentation bitmask.
 */
static uint32_t apply_hotpath_instr_mask_from_config(const keel_config_t *cfg,
                                                      uint32_t current_mask)
{
    if (!cfg || !keel_config_has_section(cfg, "stats"))
        return current_mask;

    uint32_t mask = current_mask;
    bool enabled = false;

    const char *v = keel_config_get_string(cfg, "stats", "hotpath_all", NULL);
    if (parse_bool_string(v, &enabled)) {
        mask = enabled ? KEEL_HOT_INSTR_ALL : 0;
    }

    v = keel_config_get_string(cfg, "stats", "hotpath_wait_pool", NULL);
    if (parse_bool_string(v, &enabled)) {
        if (enabled) mask |= KEEL_HOT_INSTR_WAIT_POOL;
        else mask &= ~KEEL_HOT_INSTR_WAIT_POOL;
    }

    v = keel_config_get_string(cfg, "stats", "hotpath_wait_backend", NULL);
    if (parse_bool_string(v, &enabled)) {
        if (enabled) mask |= KEEL_HOT_INSTR_WAIT_BACKEND;
        else mask &= ~KEEL_HOT_INSTR_WAIT_BACKEND;
    }

    v = keel_config_get_string(cfg, "stats", "hotpath_wait_backend_query_split", NULL);
    if (parse_bool_string(v, &enabled)) {
        if (enabled) mask |= KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT;
        else mask &= ~KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT;
    }

    v = keel_config_get_string(cfg, "stats", "hotpath_deferred_send", NULL);
    if (parse_bool_string(v, &enabled)) {
        if (enabled) mask |= KEEL_HOT_INSTR_DEFERRED_SEND;
        else mask &= ~KEEL_HOT_INSTR_DEFERRED_SEND;
    }

    return mask;
}

/**
 * @brief Request graceful shutdown from an async signal context.
 *
 * The handler performs the minimum async-signal-safe work possible: it sets a
 * flag that the main control loop polls later. All real shutdown work is deferred
 * to normal control flow where logging, locking, and worker coordination are safe.
 *
 * @param sig Delivered signal number, ignored after acknowledging receipt.
 * @return
 */
static void signal_handler(int sig) {
    (void)sig;
    g_should_stop = 1;
}

/**
 * @brief Apply broad instrumentation-category overrides from the configuration file.
 *
 * This function mirrors the bitwise override policy used by the hot-path mask,
 * but targets the heavier function-level probe categories. The result is a stable
 * category mask that workers can consult without reparsing configuration data.
 *
 * @param cfg Loaded configuration object, or `NULL` when unavailable.
 * @param current_mask Current category mask, typically seeded from defaults.
 * @return Effective `KEEL_INSTR_CAT_*` mask.
 */
static uint32_t apply_instr_mask_from_config(const keel_config_t *cfg,
                                              uint32_t current_mask)
{
    if (!cfg || !keel_config_has_section(cfg, "instrument"))
        return current_mask;

    uint32_t mask = current_mask;
    bool enabled = false;

    /* Master switch */
    const char *v = keel_config_get_string(cfg, "instrument", "enabled", NULL);
    if (parse_bool_string(v, &enabled)) {
        mask = enabled ? KEEL_INSTR_CAT_ALL : KEEL_INSTR_CAT_NONE;
    }

    /* Per-category overrides */
    static const struct { const char *key; uint32_t bit; } cats[] = {
        { "cat_engine", KEEL_INSTR_CAT_ENGINE },
        { "cat_pool",   KEEL_INSTR_CAT_POOL },
        { "cat_proto",  KEEL_INSTR_CAT_PROTO },
        { "cat_io",     KEEL_INSTR_CAT_IO },
        { "cat_hook",   KEEL_INSTR_CAT_HOOK },
        { "cat_route",  KEEL_INSTR_CAT_ROUTE },
        { "cat_ps",     KEEL_INSTR_CAT_PS },
        { "cat_state",  KEEL_INSTR_CAT_STATE },
    };

    for (size_t i = 0; i < sizeof(cats) / sizeof(cats[0]); i++) {
        v = keel_config_get_string(cfg, "instrument", cats[i].key, NULL);
        if (parse_bool_string(v, &enabled)) {
            if (enabled) mask |= cats[i].bit;
            else mask &= ~cats[i].bit;
        }
    }

    return mask;
}

/**
 * @brief Request an out-of-band statistics dump from a signal context.
 *
 * Like the shutdown handler, this only sets a flag. Actual snapshot generation
 * is deferred so the stats subsystem can walk counters and histograms safely.
 *
 * @param sig Delivered signal number, ignored after acknowledging receipt.
 * @return
 */
static void stats_signal_handler(int sig) {
    (void)sig;
    g_dump_stats = 1;
}

/**
 * @brief Format and log a stats snapshot.
 *
 * Called on SIGUSR1 or on the periodic timer.
 * Output goes to stdout so operators can tail -f.
 */
static void stats_dump(void) {
    if (!g_engine) return;
    
    keel_stats_collector_t *sc = keel_engine_get_stats_collector(g_engine);
    if (!sc) return;

    /* Refresh system sample immediately before snapshot on demand/interval. */
    keel_stats_sample_system(sc);
    
    keel_stats_snapshot_t snap;
    keel_stats_snapshot_take(sc, &snap);
    
    double uptime_s = (double)snap.uptime_ns / 1.0e9;
    
    printf("\n");
    printf("╔═══════════════════════ KEEL Stats ════════════════════════════╗\n");
    printf("║ Level: %-10s  Uptime: %.1fs  Workers: %zu              \n",
           keel_stats_level_to_str(snap.level), uptime_s, snap.num_workers);
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    
    if (snap.level >= KEEL_STATS_BASIC) {
        printf("║ Sessions:  created=%-8llu  closed=%-8llu  active=%-6lld  peak=%-6llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.sessions_created),
               (unsigned long long)keel_counter_get(&snap.basic.sessions_closed),
               (long long)keel_gauge_get(&snap.basic.sessions_active),
               (unsigned long long)snap.basic.sessions_peak);
        printf("║ Pool:      borrow=%-8llu  return=%-8llu  hit=%-8llu  miss=%-7llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.pool_borrows),
               (unsigned long long)keel_counter_get(&snap.basic.pool_returns),
               (unsigned long long)keel_counter_get(&snap.basic.pool_hits),
               (unsigned long long)keel_counter_get(&snap.basic.pool_misses));
        printf("║ Pool:      create=%-8llu  destroy=%-7llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.pool_creates),
               (unsigned long long)keel_counter_get(&snap.basic.pool_destroys));
        printf("║ Queries:   total=%-9llu  read=%-9llu  write=%-8llu  tx=%-8llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.queries_total),
               (unsigned long long)keel_counter_get(&snap.basic.queries_read),
               (unsigned long long)keel_counter_get(&snap.basic.queries_write),
               (unsigned long long)keel_counter_get(&snap.basic.queries_tx));
        printf("║ Errors:    total=%-9llu  auth=%-9llu  proto=%-8llu  backend=%-5llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.errors_total),
               (unsigned long long)keel_counter_get(&snap.basic.errors_auth),
               (unsigned long long)keel_counter_get(&snap.basic.errors_proto),
               (unsigned long long)keel_counter_get(&snap.basic.errors_backend));
        printf("║ I/O:       recv=%-10llu  sent=%-10llu  spliced=%-8llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.bytes_recv),
               (unsigned long long)keel_counter_get(&snap.basic.bytes_sent),
               (unsigned long long)keel_counter_get(&snap.basic.bytes_spliced));
        printf("║ Reactor:   loops=%-9llu  submit=%-8llu  complete=%-7llu\n",
               (unsigned long long)keel_counter_get(&snap.basic.loop_iterations),
               (unsigned long long)keel_counter_get(&snap.basic.ops_submitted),
               (unsigned long long)keel_counter_get(&snap.basic.ops_completed));
         {
             uint64_t pool_ev = keel_counter_get(&snap.basic.flow_wait_pool_events);
             uint64_t pool_ns = keel_counter_get(&snap.basic.flow_wait_pool_ns_total);
             uint64_t be_ev   = keel_counter_get(&snap.basic.flow_wait_backend_events);
             uint64_t be_ns   = keel_counter_get(&snap.basic.flow_wait_backend_ns_total);
             printf("║ FlowWait:  pool_ev=%-7llu pool_ms=%-10.2f be_ev=%-7llu be_ms=%-10.2f\n",
                 (unsigned long long)pool_ev,
                 (double)pool_ns / 1000000.0,
                 (unsigned long long)be_ev,
                 (double)be_ns / 1000000.0);
         }
    }
    
    if (snap.level >= KEEL_STATS_EXTENDED) {
        /* Show p50/p95/p99 latencies */
        uint64_t qp50 = keel_histogram_percentile(&snap.extended.query_latency_ns, 0.50);
        uint64_t qp95 = keel_histogram_percentile(&snap.extended.query_latency_ns, 0.95);
        uint64_t qp99 = keel_histogram_percentile(&snap.extended.query_latency_ns, 0.99);
        uint64_t bp50 = keel_histogram_percentile(&snap.extended.backend_latency_ns, 0.50);
        uint64_t bp95 = keel_histogram_percentile(&snap.extended.backend_latency_ns, 0.95);
        uint64_t bp99 = keel_histogram_percentile(&snap.extended.backend_latency_ns, 0.99);
        printf("╠══════════════════════════════════════════════════════════════╣\n");
        printf("║ Query latency (ns):   p50=%-10llu  p95=%-10llu  p99=%-10llu\n",
               (unsigned long long)qp50, (unsigned long long)qp95, (unsigned long long)qp99);
        printf("║ Backend latency (ns): p50=%-10llu  p95=%-10llu  p99=%-10llu\n",
               (unsigned long long)bp50, (unsigned long long)bp95, (unsigned long long)bp99);
    }
    
    if (snap.level >= KEEL_STATS_SYSTEM) {
         uint32_t m = snap.system.probe_mask;
        printf("╠══════════════════════════════════════════════════════════════╣\n");
         if (m & KEEL_STAT_SYS_CPU) {
             printf("║ CPU:  user=%.1f%%  sys=%.1f%%\n",
                 snap.system.cpu_user_pct, snap.system.cpu_sys_pct);
             printf("║ Ctx switches:  voluntary=%llu  involuntary=%llu\n",
                 (unsigned long long)snap.system.ctx_switches_vol,
                 (unsigned long long)snap.system.ctx_switches_inv);
         }
         if (m & KEEL_STAT_SYS_MEMORY) {
             printf("║ Memory: RSS=%.1f MB  VM=%.1f MB\n",
                 (double)snap.system.rss_bytes / (1024.0 * 1024.0),
                 (double)snap.system.vm_bytes / (1024.0 * 1024.0));
         }
         if (m & KEEL_STAT_SYS_FD) {
             printf("║ FDs: open=%u  limit=%u\n",
                 snap.system.fd_open, snap.system.fd_limit);
         }
         if (m & KEEL_STAT_SYS_DISK) {
             printf("║ Disk IO: read=%llu B  write=%llu B\n",
                 (unsigned long long)snap.system.disk_read_bytes,
                 (unsigned long long)snap.system.disk_write_bytes);
         }
         if (m & KEEL_STAT_SYS_NETWORK) {
             printf("║ Net IO: rx=%llu B  tx=%llu B\n",
                 (unsigned long long)snap.system.net_rx_bytes,
                 (unsigned long long)snap.system.net_tx_bytes);
         }
    }

    /* Function-level instrumentation probes */
    {
        bool any_active = false;
        for (int p = 0; p < KEEL_INSTR__COUNT; p++) {
            if (snap.instr.probes[p].call_count > 0) {
                any_active = true;
                break;
            }
        }
        if (any_active) {
            printf("╠══════════════════════════════════════════════════════════════╣\n");
            printf("║ Instrumentation Probes:                                     \n");
            for (int p = 0; p < KEEL_INSTR__COUNT; p++) {
                const keel_instr_probe_t *pr = &snap.instr.probes[p];
                if (pr->call_count == 0) continue;
                uint64_t avg_ns = pr->total_ns / pr->call_count;
                double total_ms = (double)pr->total_ns / 1000000.0;
                printf("║   %-20s calls=%-10llu total=%8.2fms  avg=%-7lluns  min=%-7lluns  max=%-7lluns\n",
                    keel_instr_probe_name((keel_instr_id_t)p),
                    (unsigned long long)pr->call_count,
                    total_ms,
                    (unsigned long long)avg_ns,
                    (unsigned long long)(pr->min_ns == UINT64_MAX ? 0 : pr->min_ns),
                    (unsigned long long)pr->max_ns);
            }
        }
    }
    
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("\n");
    fflush(stdout);
}

/**
 * @brief Fatal signal handler that writes a brief diagnostic and re-raises.
 *
 * Uses only async-signal-safe operations: `write()` to stderr followed by
 * restoring the default handler and `raise()` to produce a core dump.
 * Handles `SIGSEGV`, `SIGABRT`, and `SIGBUS`; unknown signals are labelled
 * `"UNKNOWN"`.
 *
 * @param sig Signal number delivered by the kernel.
 */
static void crash_handler(int sig) {
    const char* name = (sig == SIGSEGV) ? "SIGSEGV" :
                       (sig == SIGABRT) ? "SIGABRT" :
                       (sig == SIGBUS)  ? "SIGBUS"  : "UNKNOWN";
    /* Write directly to stderr (async-signal-safe).
     * Use if() to suppress GCC warn_unused_result — nothing to do on error
     * inside a crash handler anyway. */
    if (write(STDERR_FILENO, "FATAL: ", 7)) {}
    if (write(STDERR_FILENO, name, strlen(name))) {}
    if (write(STDERR_FILENO, " received — aborting\n", 21)) {}
    /* Re-raise to get core dump */
    signal(sig, SIG_DFL);
    raise(sig);
}

/**
 * @brief Collect server-key/value entries from a `worker_group.*.servers` section.
 *
 * The INI iterator API is callback-based. This helper copies lightweight string
 * references into a bounded side buffer so the actual reload pass can parse the
 * entries afterward without mixing tree iteration and mutation logic.
 *
 * @param key Server entry key supplied by the config iterator.
 * @param value Server connection-string value supplied by the config iterator.
 * @param[in,out] ctx Reload accumulator cast to `reload_srv_ctx_t *`.
 * @return
 */
/* ============================================================================
 * Config helpers
 * ============================================================================ */

#define KEEL_MAX_WORKER_GROUPS 8

/**
 * Per-worker-group runtime state.
 * Each group gets its own listen socket, engine, server pool, and probe.
 */
typedef struct worker_group {
    /* INI section names */
    char            section[256];
    char            servers_section[280];

    /* Per-group proxy settings */
    const char*     name;
    const char*     listen_addr;
    uint16_t        listen_port;
    const char*     default_protocol;
    uint32_t        num_workers;
    bool            pin_workers;
    size_t          pool_min_size;
    size_t          pool_max_size;
    size_t          session_pool_size;
    size_t          buffer_size;
    uint32_t        max_clients;              /* Total max frontend connections (0=unlimited) */
    uint64_t        idle_timeout_ms;
    uint64_t        connect_timeout_ms;
    uint32_t        pool_prune_interval_ms;   /* How often to drop idle pool conns */
    uint32_t        pool_refill_interval_ms;  /* Pool reconnect poll period */
    uint32_t        pool_refill_backoff_ms;   /* Slower poll when pool is full */
    uint32_t        pool_max_waiting;         /* Max queued sessions (0=auto) */
    uint64_t        pool_wait_timeout_ms;     /* Max wait for pool slot — 0=use connect_timeout_ms */
    size_t          session_max_buffered_bytes; /* Per-session buffer cap — 0=unlimited */
    size_t          backend_max_replay_bytes;   /* Per-backend PS replay cap — 0=unlimited */
    uint32_t        listen_backlog;           /* TCP listen() queue depth */
    bool            use_buf_rings;            /* io_uring buf rings for recv */
    uint32_t        buf_ring_size;            /* Buf ring slots (0 = queue_depth) */
    bool            sqpoll;                   /* io_uring SQ polling (kernel thread) */
    uint32_t        sqpoll_idle_ms;           /* SQ poll thread idle timeout */
    const char*     backend_host;
    uint16_t        backend_port;
    const char*     backend_user;
    const char*     backend_password;
    const char*     backend_database;

    /* Server pool for R/W splitting */
    keel_server_pool_t   server_pool;

    /* Probe config */
    keel_probe_config_t  probe_cfg;
    char                probe_type_buf[64];
    char                probe_extra_buf[64];
    char                probe_user_buf[128];
    char                probe_password_buf[128];
    char                probe_auth_buf[32];

    /* Runtime handles */
    int                 listen_fd;
    keel_engine_t*       engine;
    keel_probe_manager_t* probe_mgr;
    keel_hook_registry_t* hook_registry;

    /* Prepared-statement pooling strategy */
    keel_ps_mode_t       ps_mode;   /**< virtualize / pinning / tracking / anonymous */

    /* Runtime mode tier */
    keel_tier_t           runtime_mode; /**< proxy / pool / smart / full */
    bool                 experimental_features; /**< experimental_features = on|off */

    /* Replication uncertainty tracking */
    bool                 txn_tracking; /**< transaction_tracking = on|off */
    bool                 wal_lsn_capture; /**< wal_lsn_capture = on|off */
    bool                 gtid_capture;    /**< gtid_capture = on|off */

    /* Zero-copy fast network path */
    bool                 fast_network_path; /**< fast_network_path = on|off */
    bool                 result_cache;      /**< result_cache = on|off */
    bool                 scatter_merge_enabled; /**< scatter_merge = on|off */
    bool                 sharding_enabled;      /**< shard routing enabled */
    bool                 hooks_enabled;         /**< any hook chain enabled */

    /* Sticky-primary TTL (0 = disabled) */
    uint32_t             sticky_primary_ttl_ms; /**< ms to pin reads to primary after a write */

    /* Connection rebalancing */
    bool                 rebalance_enabled;
    uint32_t             rebalance_interval_ms;
    uint32_t             rebalance_threshold_pct;
    uint32_t             rebalance_max_per_tick;

    /* Scatter-merge memory budget and spill directory */
    size_t               scatter_merge_max_mem_bytes; /**< 0 = default (32 MiB) */
    char                 scatter_merge_spill_dir_buf[512]; /**< spill dir path */

    /* TLS configuration for client connections */
    keel_tls_config_t    tls_config;   /**< Frontend TLS settings */
    char                 tls_cert_file_buf[512];
    char                 tls_key_file_buf[512];
    char                 tls_ca_file_buf[512];
    char                 tls_mode_buf[32];
    char                 tls_verify_buf[32];

    /* TLS configuration for backend connections */
    keel_tls_config_t    backend_tls_config;  /**< Backend TLS settings */
    char                 backend_tls_cert_file_buf[512];
    char                 backend_tls_key_file_buf[512];
    char                 backend_tls_ca_file_buf[512];
    char                 backend_tls_mode_buf[32];
    char                 backend_tls_verify_buf[32];

    /* Auto-generated TLS certificates */
    bool                 tls_auto_generate;       /**< Generate certs on startup */
    char                 tls_auto_dir_buf[512];   /**< Cert output directory */
    keel_tls_auto_result_t tls_auto_result;       /**< Generated file paths */

    /* Enterprise authentication configuration */
    keel_auth_method_t   auth_method;             /**< Client-facing auth method */
    char                 auth_method_buf[32];     /**< Raw string from INI */
    /* LDAP */
    char                 auth_ldap_url_buf[256];
    char                 auth_ldap_base_dn_buf[256];
    char                 auth_ldap_bind_dn_buf[256];
    char                 auth_ldap_bind_password_buf[128];
    char                 auth_ldap_search_filter_buf[256];
    char                 auth_ldap_dn_suffix_buf[256];
    bool                 auth_ldap_start_tls;
    int                  auth_ldap_timeout_s;
    /* PAM */
    char                 auth_pam_service_buf[64];
    /* auth_query */
    char                 auth_query_buf[512];
    char                 auth_query_conn_buf[512];
    /* userlist */
    char                 auth_userlist_file_buf[512];

    /* Connection lifecycle */
    uint64_t             pool_max_connection_age_ms; /**< Force-recycle age (0=never) */

    /* Static storage for server connection-string fields */
    char server_bufs[KEEL_MAX_SERVERS][7][256];

    /* Shard router — created at startup when shard_rules are configured */
    keel_router_t*          router;

    /* Router plugin manager — wraps the router for per-database plugin routing */
    keel_router_mgr_t*      router_mgr;

    /* Throttle rules — registered as BEFORE_ROUTE hook at startup */
    keel_throttle_rules_t*  throttle_rules;

    /* Background topology discovery — started at startup when configured */
    keel_discovery_t*       discovery;
} worker_group_t;

static size_t         g_num_groups = 0;
static worker_group_t g_groups[KEEL_MAX_WORKER_GROUPS];
static bool           g_experimental_features_enabled = false;
static size_t         g_query_rule_count = 0;
static size_t         g_throttle_rule_count = 0;
static size_t         g_shard_rule_count = 0;

/* ============================================================================
 * Live Configuration Reload (SIGHUP)
 *
 * Re-parses the INI file and applies safe-to-change parameters to running
 * worker groups without restart. Restart-required changes are logged.
 *
 * Safe:     pool sizes, timeouts, server weights, probe intervals, rebalancing
 * Restart:  bind_addr, bind_port, num_workers, protocol, ps_mode, runtime_mode
 * ============================================================================ */

/* Callback for collecting server entries during weight reload scan */
typedef struct {
    struct { const char* key; const char* val; } entries[KEEL_MAX_SERVERS];
    size_t nentries;
} reload_srv_ctx_t;

/**
 * @brief Config-iterator callback that accumulates server key/value pairs.
 *
 * The INI library drives iteration via a callback; this function appends
 * each visited key and value pointer into the `reload_srv_ctx_t` side
 * buffer up to `KEEL_MAX_SERVERS` entries.  The caller then processes the
 * captured entries in a second pass, avoiding mixed iteration and mutation.
 *
 * @param key   Server entry key (e.g. `"primary"`).
 * @param value Connection-string value for that key.
 * @param ctx   Opaque pointer cast to `reload_srv_ctx_t *`.
 */
static void reload_collect_server_keys(const char* key, const char* value, void* ctx) {
    reload_srv_ctx_t* sc = ctx;
    if (sc->nentries < KEEL_MAX_SERVERS) {
        sc->entries[sc->nentries].key = key;
        sc->entries[sc->nentries].val = value;
        sc->nentries++;
    }
}

/**
 * @brief Reload a single worker group's live-tunable parameters from a fresh configuration snapshot.
 *
 * Reload happens in the main thread after reparsing the INI file. The algorithm
 * is intentionally conservative: it first detects restart-required changes and
 * reports them, then applies only parameters that can be updated without tearing
 * down listeners, worker threads, or protocol state. Safe changes are propagated
 * into both the cached `worker_group_t` copy and the live engine/worker objects
 * so future reloads and current runtime behavior stay consistent.
 *
 * This split avoids a dangerous half-measure where topology changes appear to be
 * accepted but are only reflected in one layer. The downside is that some user
 * expectations must be deferred until restart, but the benefit is predictable
 * behavior under load and no in-place mutation of thread-count or routing-mode
 * assumptions baked into existing workers.
 *
 * @param config Freshly loaded configuration tree.
 * @param[in,out] wg Worker-group runtime descriptor to inspect and mutate.
 * @return Number of live settings that were actually changed and applied.
 */
static int reload_worker_group(keel_config_t* config, worker_group_t* wg) {
    const char* section = wg->section;
    int applied = 0;
    int restart_needed = 0;

    /* ------------------------------------------------------------------
     * §1 — Restart-required parameters: detect and warn
     * ------------------------------------------------------------------ */

    /* bind_port */
    int64_t new_port = keel_config_get_int(config, section, "bind_port",
                                            (int64_t)wg->listen_port);
    if (new_port > 0 && (uint16_t)new_port != wg->listen_port) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "[%s] bind_port changed (%u -> %lld) — requires restart",
            wg->name, wg->listen_port, (long long)new_port);
        restart_needed++;
    }

    /* num_workers */
    int64_t new_workers = keel_config_get_int(config, section, "num_workers",
                                               (int64_t)wg->num_workers);
    if (new_workers > 0 && (uint32_t)new_workers != wg->num_workers) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "[%s] num_workers changed (%u -> %lld) — requires restart",
            wg->name, wg->num_workers, (long long)new_workers);
        restart_needed++;
    }

    /* prepared_statement mode */
    {
        const char* ps_str = keel_config_get_string(config, section,
                                "prepared_statement", NULL);
        if (ps_str) {
            keel_ps_mode_t new_ps = KEEL_PS_MODE_VIRTUALIZE;
            if      (strcmp(ps_str, "pinning")   == 0) new_ps = KEEL_PS_MODE_PINNING;
            else if (strcmp(ps_str, "tracking")  == 0) new_ps = KEEL_PS_MODE_TRACKING;
            else if (strcmp(ps_str, "anonymous") == 0) new_ps = KEEL_PS_MODE_ANONYMOUS;
            else if (strcmp(ps_str, "off")       == 0) new_ps = KEEL_PS_MODE_OFF;
            if (new_ps != wg->ps_mode) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "[%s] prepared_statement changed (%d -> %d) — requires restart",
                    wg->name, (int)wg->ps_mode, (int)new_ps);
                restart_needed++;
            }
        }
    }

    /* runtime mode tier */
    {
        const char* mode_str = keel_config_get_string(config, section, "mode", NULL);
        if (mode_str) {
            keel_tier_t new_tier = keel_tier_parse(mode_str);
            if (new_tier != wg->runtime_mode) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "[%s] mode changed (%d -> %d) — requires restart",
                    wg->name, (int)wg->runtime_mode, (int)new_tier);
                restart_needed++;
            }
        }
    }

    /* transaction_tracking */
    {
        const char* tt = keel_config_get_string(config, section,
                            "transaction_tracking", NULL);
        if (tt) {
            bool new_tt = (strcmp(tt, "on") == 0);
            if (new_tt != wg->txn_tracking) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "[%s] transaction_tracking changed — requires restart",
                    wg->name);
                restart_needed++;
            }
        }
    }

    if (restart_needed > 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "[%s] %d parameter(s) require restart to take effect",
            wg->name, restart_needed);
    }

    /* ------------------------------------------------------------------
     * §2 — Pool sizing (safe to change)
     * ------------------------------------------------------------------ */

    keel_engine_config_t* ecfg = wg->engine
                                 ? keel_engine_get_config_mut(wg->engine)
                                 : NULL;

    {
        int64_t new_min = keel_config_get_int(config, section, "min_pool_size",
                                               (int64_t)wg->pool_min_size);
        int64_t new_max = keel_config_get_int(config, section, "max_pool_size",
                                               (int64_t)wg->pool_max_size);
        if (new_min < 1) new_min = 1;
        if (new_max < new_min) new_max = new_min;

        bool pool_changed = false;
        if ((size_t)new_min != wg->pool_min_size) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] min_pool_size: %zu -> %lld",
                wg->name, wg->pool_min_size, (long long)new_min);
            wg->pool_min_size = (size_t)new_min;
            pool_changed = true;
        }
        if ((size_t)new_max != wg->pool_max_size) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] max_pool_size: %zu -> %lld",
                wg->name, wg->pool_max_size, (long long)new_max);
            wg->pool_max_size = (size_t)new_max;
            pool_changed = true;
        }

        if (pool_changed && ecfg) {
            ecfg->pool_min_size = wg->pool_min_size;
            ecfg->pool_max_size = wg->pool_max_size;

            /* Propagate to per-worker backend pools */
            uint32_t nw = ecfg->num_workers > 0 ? ecfg->num_workers : 4;
            size_t per_min = wg->pool_min_size / nw;
            size_t per_max = wg->pool_max_size / nw;
            if (per_min < 1) per_min = 1;
            if (per_max < per_min) per_max = per_min;

            uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
            for (uint32_t wi = 0; wi < nworkers; wi++) {
                keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                if (!w) continue;
                for (size_t si = 0; si < w->server_pool_count; si++) {
                    if (w->server_pools[si]) {
                        w->server_pools[si]->config.min_connections = per_min;
                        w->server_pools[si]->config.max_connections = per_max;
                    }
                }
            }
            applied++;
        }
    }

    /* pool_max_waiting */
    {
        int64_t new_mw = keel_config_get_int(config, section, "pool_max_waiting",
                                              (int64_t)wg->pool_max_waiting);
        if (new_mw >= 0 && (uint32_t)new_mw != wg->pool_max_waiting) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_max_waiting: %u -> %lld",
                wg->name, wg->pool_max_waiting, (long long)new_mw);
            wg->pool_max_waiting = (uint32_t)new_mw;
            if (ecfg) ecfg->pool_max_waiting = wg->pool_max_waiting;

            /* Propagate to per-worker backend pools */
            if (ecfg) {
                uint32_t nw = ecfg->num_workers > 0 ? ecfg->num_workers : 4;
                size_t per_mw = wg->pool_max_waiting > 0
                    ? wg->pool_max_waiting / (nw > 0 ? nw : 1) : 0;
                if (per_mw < 1 && wg->pool_max_waiting > 0) per_mw = 1;

                uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
                for (uint32_t wi = 0; wi < nworkers; wi++) {
                    keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                    if (!w) continue;
                    for (size_t si = 0; si < w->server_pool_count; si++) {
                        if (w->server_pools[si])
                            w->server_pools[si]->config.max_waiting = per_mw;
                    }
                }
            }
            applied++;
        }
    }

    /* pool_wait_timeout_ms */
    {
        int64_t new_wt = keel_config_get_duration_ms(config, section, "pool_wait_timeout",
                                              (int64_t)wg->pool_wait_timeout_ms);
        if (new_wt >= 0 && (uint64_t)new_wt != wg->pool_wait_timeout_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_wait_timeout: %llu -> %lld",
                wg->name, (unsigned long long)wg->pool_wait_timeout_ms, (long long)new_wt);
            wg->pool_wait_timeout_ms = (uint64_t)new_wt;
            if (ecfg) ecfg->pool_wait_timeout_ms = wg->pool_wait_timeout_ms;
            applied++;
        }
    }

    /* session_max_buffered_bytes */
    {
        int64_t new_smb = keel_config_get_bytes(config, section, "session_max_buffered",
                                               (int64_t)wg->session_max_buffered_bytes);
        if (new_smb == 0 || new_smb >= 4096) {
            if ((size_t)new_smb != wg->session_max_buffered_bytes) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[%s] session_max_buffered: %zu -> %lld",
                    wg->name, wg->session_max_buffered_bytes, (long long)new_smb);
                wg->session_max_buffered_bytes = (size_t)new_smb;
                if (ecfg) ecfg->session_max_buffered_bytes = wg->session_max_buffered_bytes;
                applied++;
            }
        } else if (new_smb > 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                "[%s] session_max_buffered=%lld is below minimum 4096; "
                "use 0 (unlimited) or a value >= 4096",
                wg->name, (long long)new_smb);
        }
    }

    /* backend_max_replay_bytes */
    {
        int64_t new_mrb = keel_config_get_bytes(config, section, "backend_max_replay",
                                               (int64_t)wg->backend_max_replay_bytes);
        if (new_mrb == 0 || new_mrb >= 4096) {
            if ((size_t)new_mrb != wg->backend_max_replay_bytes) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[%s] backend_max_replay: %zu -> %lld",
                    wg->name, wg->backend_max_replay_bytes, (long long)new_mrb);
                wg->backend_max_replay_bytes = (size_t)new_mrb;
                if (ecfg) ecfg->backend_max_replay_bytes = wg->backend_max_replay_bytes;
                applied++;
            }
        } else if (new_mrb > 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                "[%s] backend_max_replay=%lld is below minimum 4096; "
                "use 0 (unlimited) or a value >= 4096",
                wg->name, (long long)new_mrb);
        }
    }

    /* ------------------------------------------------------------------
     * §3 — Timeouts (safe to change)
     * ------------------------------------------------------------------ */

    {
        int64_t v;

        /* idle_timeout_ms */
        v = keel_config_get_duration_ms(config, section, "idle_timeout",
                                 (int64_t)wg->idle_timeout_ms);
        /* Also try human-readable key */
        const char* ts = keel_config_get_string(config, section,
                            "client_idle_timeout", NULL);
        if (ts) {
            int tv = atoi(ts);
            if (tv > 0) {
                if      (strchr(ts, 'm')) tv *= 60000;
                else if (strchr(ts, 's')) tv *= 1000;
                v = tv;
            }
        }
        if (v > 0 && (uint64_t)v != wg->idle_timeout_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] idle_timeout: %llu -> %lld",
                wg->name, (unsigned long long)wg->idle_timeout_ms, (long long)v);
            wg->idle_timeout_ms = (uint64_t)v;
            if (ecfg) ecfg->idle_timeout_ms = (uint32_t)v;

            /* Propagate to workers */
            if (wg->engine) {
                uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
                for (uint32_t wi = 0; wi < nworkers; wi++) {
                    keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                    if (w) w->idle_timeout_ms = (uint32_t)v;
                }
            }
            applied++;
        }

        /* connect_timeout_ms */
        v = keel_config_get_duration_ms(config, section, "connect_timeout",
                                 (int64_t)wg->connect_timeout_ms);
        ts = keel_config_get_string(config, section, "client_connect_timeout", NULL);
        if (ts) {
            int tv = atoi(ts);
            if (tv > 0) {
                if      (strchr(ts, 'm')) tv *= 60000;
                else if (strchr(ts, 's')) tv *= 1000;
                v = tv;
            }
        }
        if (v > 0 && (uint64_t)v != wg->connect_timeout_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] connect_timeout: %llu -> %lld",
                wg->name, (unsigned long long)wg->connect_timeout_ms, (long long)v);
            wg->connect_timeout_ms = (uint64_t)v;
            if (ecfg) ecfg->connect_timeout_ms = (uint32_t)v;
            applied++;
        }

        /* pool_prune_interval_ms */
        v = keel_config_get_duration_ms(config, section, "pool_prune_interval",
                                 (int64_t)wg->pool_prune_interval_ms);
        if (v > 0 && (uint32_t)v != wg->pool_prune_interval_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_prune_interval: %u -> %lld",
                wg->name, wg->pool_prune_interval_ms, (long long)v);
            wg->pool_prune_interval_ms = (uint32_t)v;
            if (ecfg) ecfg->pool_prune_interval_ms = (uint32_t)v;

            /* Workers pick up the new interval on next timer re-arm */
            if (wg->engine) {
                uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
                for (uint32_t wi = 0; wi < nworkers; wi++) {
                    keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                    if (w) w->pool_prune_interval_ms = (uint32_t)v;
                }
            }
            applied++;
        }

        /* pool_refill_interval_ms */
        v = keel_config_get_duration_ms(config, section, "pool_refill_interval",
                                 (int64_t)wg->pool_refill_interval_ms);
        if (v >= 100 && (uint32_t)v != wg->pool_refill_interval_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_refill_interval: %u -> %lld",
                wg->name, wg->pool_refill_interval_ms, (long long)v);
            wg->pool_refill_interval_ms = (uint32_t)v;
            if (ecfg) ecfg->pool_refill_interval_ms = (uint32_t)v;

            if (wg->engine) {
                uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
                for (uint32_t wi = 0; wi < nworkers; wi++) {
                    keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                    if (w) w->pool_refill_interval_ms = (uint32_t)v;
                }
            }
            applied++;
        }

        /* pool_refill_backoff_ms */
        v = keel_config_get_duration_ms(config, section, "pool_refill_backoff",
                                 (int64_t)wg->pool_refill_backoff_ms);
        if (v > 0 && (uint32_t)v != wg->pool_refill_backoff_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_refill_backoff: %u -> %lld",
                wg->name, wg->pool_refill_backoff_ms, (long long)v);
            wg->pool_refill_backoff_ms = (uint32_t)v;
            if (ecfg) ecfg->pool_refill_backoff_ms = (uint32_t)v;

            if (wg->engine) {
                uint32_t nworkers = keel_engine_get_num_workers(wg->engine);
                for (uint32_t wi = 0; wi < nworkers; wi++) {
                    keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, wi);
                    if (w) w->pool_refill_backoff_ms = (uint32_t)v;
                }
            }
            applied++;
        }

        /* pool_wait_timeout_ms */
        v = keel_config_get_duration_ms(config, section, "pool_wait_timeout",
                                 (int64_t)wg->pool_wait_timeout_ms);
        if (v >= 0 && (uint64_t)v != wg->pool_wait_timeout_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] pool_wait_timeout: %llu -> %lld",
                wg->name, (unsigned long long)wg->pool_wait_timeout_ms, (long long)v);
            wg->pool_wait_timeout_ms = (uint64_t)v;
            if (ecfg) ecfg->pool_wait_timeout_ms = (uint64_t)v;
            applied++;
        }

        /* session_max_buffered_bytes */
        v = keel_config_get_bytes(config, section, "session_max_buffered",
                                 (int64_t)wg->session_max_buffered_bytes);
        if (v == 0 || v >= 4096) {
            if ((size_t)v != wg->session_max_buffered_bytes) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[%s] session_max_buffered: %zu -> %lld",
                    wg->name, wg->session_max_buffered_bytes, (long long)v);
                wg->session_max_buffered_bytes = (size_t)v;
                if (ecfg) ecfg->session_max_buffered_bytes = (size_t)v;
                applied++;
            }
        } else {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                "[%s] session_max_buffered=%lld is below minimum 4096; "
                "use 0 (unlimited) or a value >= 4096",
                wg->name, (long long)v);
        }

        /* backend_max_replay_bytes */
        v = keel_config_get_bytes(config, section, "backend_max_replay",
                                 (int64_t)wg->backend_max_replay_bytes);
        if (v == 0 || v >= 4096) {
            if ((size_t)v != wg->backend_max_replay_bytes) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[%s] backend_max_replay: %zu -> %lld",
                    wg->name, wg->backend_max_replay_bytes, (long long)v);
                wg->backend_max_replay_bytes = (size_t)v;
                if (ecfg) ecfg->backend_max_replay_bytes = (size_t)v;
                applied++;
            }
        } else {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                "[%s] backend_max_replay=%lld is below minimum 4096; "
                "use 0 (unlimited) or a value >= 4096",
                wg->name, (long long)v);
        }
    }

    /* ------------------------------------------------------------------
     * §4 — Server weights (safe to change)
     * ------------------------------------------------------------------ */

    if (ecfg && keel_config_has_section(config, wg->servers_section)) {
        keel_server_pool_t* sp = &ecfg->server_pool;

        /* Collect server entries from the INI servers section */
        reload_srv_ctx_t wctx = { .nentries = 0 };

        keel_config_iter_keys(config, wg->servers_section,
            reload_collect_server_keys, &wctx);

        for (size_t ei = 0; ei < wctx.nentries; ei++) {
            /* Parse connection string looking for host, port, weight */
            const char* val = wctx.entries[ei].val;
            char h[256] = {0};
            uint16_t pt = 0;
            uint32_t wt = 100;

            const char* p = val;
            while (*p) {
                while (*p && (*p == ' ' || *p == '\t')) p++;
                if (!*p) break;

                if (strncmp(p, "host=", 5) == 0) {
                    p += 5;
                    char* d = h;
                    while (*p && *p != ' ' && *p != '\t' && (size_t)(d - h) < sizeof(h) - 1)
                        *d++ = *p++;
                    *d = '\0';
                } else if (strncmp(p, "port=", 5) == 0) {
                    pt = (uint16_t)atoi(p + 5);
                    while (*p && *p != ' ' && *p != '\t') p++;
                } else if (strncmp(p, "weight=", 7) == 0) {
                    wt = (uint32_t)atoi(p + 7);
                    while (*p && *p != ' ' && *p != '\t') p++;
                } else {
                    while (*p && *p != ' ' && *p != '\t') p++;
                }
            }

            /* Find matching server in pool by host+port */
            for (size_t si = 0; si < sp->count; si++) {
                if (sp->servers[si].host && strcmp(sp->servers[si].host, h) == 0 &&
                    sp->servers[si].port == pt) {
                    if (sp->servers[si].weight != wt) {
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "[%s] server %s:%u weight: %u -> %u",
                            wg->name, h, pt, sp->servers[si].weight, wt);
                        sp->servers[si].weight = wt;

                        /* Also update the worker-group copy */
                        if (si < wg->server_pool.count)
                            wg->server_pool.servers[si].weight = wt;
                        applied++;
                    }
                    break;
                }
            }
        }
    }

    /* ------------------------------------------------------------------
     * §5 — Probe configuration (safe to change)
     * ------------------------------------------------------------------ */

    if (wg->probe_mgr) {
        const char* v;
        uint32_t new_interval = 0, new_timeout = 0, new_retries = 0;

        v = keel_config_get_string(config, section, "probe_interval", NULL);
        if (v) {
            int val = atoi(v);
            if (val > 0) {
                if (strchr(v, 's')) val *= 1000;
                if ((uint32_t)val != wg->probe_cfg.interval_ms) {
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "[%s] probe_interval: %ums -> %dms",
                        wg->name, wg->probe_cfg.interval_ms, val);
                    wg->probe_cfg.interval_ms = (uint32_t)val;
                    new_interval = (uint32_t)val;
                }
            }
        }

        v = keel_config_get_string(config, section, "probe_timeout", NULL);
        if (v) {
            int val = atoi(v);
            if (val > 0) {
                if (strchr(v, 's')) val *= 1000;
                if ((uint32_t)val != wg->probe_cfg.timeout_ms) {
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "[%s] probe_timeout: %ums -> %dms",
                        wg->name, wg->probe_cfg.timeout_ms, val);
                    wg->probe_cfg.timeout_ms = (uint32_t)val;
                    new_timeout = (uint32_t)val;
                }
            }
        }

        int64_t retries = keel_config_get_int(config, section, "probe_retries",
                                               (int64_t)wg->probe_cfg.retries);
        if (retries > 0 && (uint32_t)retries != wg->probe_cfg.retries) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] probe_retries: %u -> %lld",
                wg->name, wg->probe_cfg.retries, (long long)retries);
            wg->probe_cfg.retries = (uint32_t)retries;
            new_retries = (uint32_t)retries;
        }

        if (new_interval || new_timeout || new_retries) {
            keel_probe_manager_update_timing(wg->probe_mgr,
                                              new_interval, new_timeout, new_retries);
            applied++;
        }
    }

    /* ------------------------------------------------------------------
     * §6 — Rebalancing (safe to change)
     * ------------------------------------------------------------------ */

    if (ecfg) {
        const char* rb = keel_config_get_string(config, section, "rebalance", NULL);
        if (rb) {
            bool new_rb = (strcmp(rb, "off") != 0);
            if (new_rb != wg->rebalance_enabled) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "[%s] rebalance: %s -> %s",
                    wg->name, wg->rebalance_enabled ? "on" : "off",
                    new_rb ? "on" : "off");
                wg->rebalance_enabled = new_rb;
                ecfg->rebalance_enabled = new_rb;
                applied++;
            }
        }

        int64_t v;
        v = keel_config_get_duration_ms(config, section, "rebalance_interval",
                                 (int64_t)wg->rebalance_interval_ms);
        if (v > 0 && (uint32_t)v != wg->rebalance_interval_ms) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] rebalance_interval: %u -> %lld",
                wg->name, wg->rebalance_interval_ms, (long long)v);
            wg->rebalance_interval_ms = (uint32_t)v;
            ecfg->rebalance_interval_ms = (uint32_t)v;
            applied++;
        }

        v = keel_config_get_int(config, section, "rebalance_threshold_pct",
                                 (int64_t)wg->rebalance_threshold_pct);
        if (v > 100 && (uint32_t)v != wg->rebalance_threshold_pct) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] rebalance_threshold_pct: %u -> %lld",
                wg->name, wg->rebalance_threshold_pct, (long long)v);
            wg->rebalance_threshold_pct = (uint32_t)v;
            ecfg->rebalance_threshold_pct = (uint32_t)v;
            applied++;
        }

        v = keel_config_get_int(config, section, "rebalance_max_per_tick",
                                 (int64_t)wg->rebalance_max_per_tick);
        if (v > 0 && (uint32_t)v != wg->rebalance_max_per_tick) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "[%s] rebalance_max_per_tick: %u -> %lld",
                wg->name, wg->rebalance_max_per_tick, (long long)v);
            wg->rebalance_max_per_tick = (uint32_t)v;
            ecfg->rebalance_max_per_tick = (uint32_t)v;
            applied++;
        }
    }

    return applied;
}

/**
 * @brief Initialize a worker-group descriptor with conservative runtime defaults.
 *
 * Every discovered `worker_group.*` section starts from this baseline before the
 * configuration file selectively overrides fields. Centralizing defaults here is
 * important because the same structure is reused by bootstrap, reload, probe
 * fallback logic, and server parsing. The defaults bias toward a safe, generic
 * PostgreSQL deployment with TLS disabled until explicitly configured, modest pool
 * sizes, and instrumentation/security knobs in non-disruptive states.
 *
 * @param[out] g Worker-group structure to initialize.
 * @return
 */
static void worker_group_defaults(worker_group_t* g) {
    g->name              = NULL;
    g->listen_addr       = "0.0.0.0";
    g->listen_port       = 6432;
    g->default_protocol  = "postgres";
    g->num_workers       = 0;
    g->pin_workers       = true;
    g->pool_min_size     = 2;
    g->pool_max_size     = 100;
    g->session_pool_size = 1024;
    g->buffer_size       = 65536;
    g->max_clients       = 0;              /* 0 = unlimited */
    g->idle_timeout_ms            = 300000;
    g->connect_timeout_ms         = 10000;
    g->pool_prune_interval_ms     = 30000;
    g->pool_refill_interval_ms    = 100;
    g->pool_refill_backoff_ms     = 5000;
    g->pool_max_waiting           = 0;
    g->pool_wait_timeout_ms       = 0;  /* 0 = use connect_timeout_ms */
    g->session_max_buffered_bytes = 0;  /* 0 = unlimited */
    g->backend_max_replay_bytes   = 0;  /* 0 = unlimited */
    g->listen_backlog             = 4096;
    g->use_buf_rings              = false;
    g->buf_ring_size              = 0;
    g->sqpoll                     = false;
    g->sqpoll_idle_ms             = 1000;
    g->rebalance_enabled          = true;
    g->rebalance_interval_ms      = 5000;
    g->rebalance_threshold_pct    = 125;
    g->rebalance_max_per_tick     = 4;
    g->scatter_merge_max_mem_bytes = 0;
    snprintf(g->scatter_merge_spill_dir_buf,
             sizeof g->scatter_merge_spill_dir_buf, "/tmp");
    g->backend_host      = "127.0.0.1";
    g->backend_port      = 5432;
    g->backend_user      = "postgres";
    g->backend_password  = NULL;
    g->backend_database  = "postgres";
    memset(&g->server_pool, 0, sizeof(g->server_pool));
    g->probe_cfg = (keel_probe_config_t)KEEL_PROBE_CONFIG_DEFAULT;
    snprintf(g->probe_auth_buf, sizeof(g->probe_auth_buf), "auto");
    g->probe_cfg.probe_auth = g->probe_auth_buf;
    g->listen_fd  = -1;
    g->engine     = NULL;
    g->probe_mgr  = NULL;
    g->ps_mode    = KEEL_PS_MODE_VIRTUALIZE;
    g->runtime_mode = KEEL_TIER_POOL;
    g->experimental_features = false;
    g->txn_tracking = false;
    g->wal_lsn_capture = false;
    g->gtid_capture = false;
    g->fast_network_path = true;
    g->result_cache = false;
    g->scatter_merge_enabled = false;
    g->sharding_enabled = false;
    g->hooks_enabled = false;
    g->sticky_primary_ttl_ms = 100U;
    
    /* Initialize TLS configuration to disabled with safe defaults */
    memset(&g->tls_config, 0, sizeof(g->tls_config));
    g->tls_config.mode = KEEL_TLS_DISABLE;
    g->tls_config.verify_peer = KEEL_TLS_VERIFY_REQUIRED;
    g->tls_config.min_version = KEEL_TLS_VERSION_AUTO;
    g->tls_config.max_version = 0;
    g->tls_config.ktls_enabled = true;  /* Enable by default if supported */
    g->tls_config.read_timeout_ms = 30000;
    g->tls_config.handshake_timeout_ms = 10000;
    g->tls_config.cert_file = NULL;
    g->tls_config.key_file = NULL;
    g->tls_config.ca_file = NULL;
    memset(g->tls_cert_file_buf, 0, sizeof(g->tls_cert_file_buf));
    memset(g->tls_key_file_buf, 0, sizeof(g->tls_key_file_buf));
    memset(g->tls_ca_file_buf, 0, sizeof(g->tls_ca_file_buf));
    memset(g->tls_mode_buf, 0, sizeof(g->tls_mode_buf));
    memset(g->tls_verify_buf, 0, sizeof(g->tls_verify_buf));
    
    /* Initialize backend TLS configuration */
    memset(&g->backend_tls_config, 0, sizeof(g->backend_tls_config));
    g->backend_tls_config.mode = KEEL_TLS_DISABLE;
    g->backend_tls_config.verify_peer = KEEL_TLS_VERIFY_REQUIRED;
    g->backend_tls_config.min_version = KEEL_TLS_VERSION_AUTO;
    g->backend_tls_config.max_version = 0;
    g->backend_tls_config.ktls_enabled = true;
    g->backend_tls_config.read_timeout_ms = 30000;
    g->backend_tls_config.handshake_timeout_ms = 10000;
    g->backend_tls_config.cert_file = NULL;
    g->backend_tls_config.key_file = NULL;
    g->backend_tls_config.ca_file = NULL;
    memset(g->backend_tls_cert_file_buf, 0, sizeof(g->backend_tls_cert_file_buf));
    memset(g->backend_tls_key_file_buf, 0, sizeof(g->backend_tls_key_file_buf));
    memset(g->backend_tls_ca_file_buf, 0, sizeof(g->backend_tls_ca_file_buf));
    memset(g->backend_tls_mode_buf, 0, sizeof(g->backend_tls_mode_buf));
    memset(g->backend_tls_verify_buf, 0, sizeof(g->backend_tls_verify_buf));

    /* Enterprise authentication defaults */
    g->auth_method = KEEL_AUTH_SCRAM_SHA_256;
    strncpy(g->auth_method_buf, "scram-sha-256", sizeof(g->auth_method_buf) - 1);
    g->auth_method_buf[sizeof(g->auth_method_buf) - 1] = '\0';
    memset(g->auth_ldap_url_buf, 0, sizeof(g->auth_ldap_url_buf));
    memset(g->auth_ldap_base_dn_buf, 0, sizeof(g->auth_ldap_base_dn_buf));
    memset(g->auth_ldap_bind_dn_buf, 0, sizeof(g->auth_ldap_bind_dn_buf));
    memset(g->auth_ldap_bind_password_buf, 0, sizeof(g->auth_ldap_bind_password_buf));
    memset(g->auth_ldap_search_filter_buf, 0, sizeof(g->auth_ldap_search_filter_buf));
    memset(g->auth_ldap_dn_suffix_buf, 0, sizeof(g->auth_ldap_dn_suffix_buf));
    g->auth_ldap_start_tls = false;
    g->auth_ldap_timeout_s = 5;
    memset(g->auth_pam_service_buf, 0, sizeof(g->auth_pam_service_buf));
    memset(g->auth_query_buf, 0, sizeof(g->auth_query_buf));
    memset(g->auth_query_conn_buf, 0, sizeof(g->auth_query_conn_buf));
    memset(g->auth_userlist_file_buf, 0, sizeof(g->auth_userlist_file_buf));
    g->pool_max_connection_age_ms = 0;
}

typedef struct {
    size_t* count;
    size_t  max;
} wg_collect_ctx_t;

/**
 * @brief Discover top-level worker-group sections from the configuration tree.
 *
 * The collector deliberately ignores nested sections such as `.servers` or
 * `.hooks`; those are parsed later once the owning worker group has been created.
 * That two-phase approach keeps the bootstrap flow simple: first enumerate the
 * groups, then expand each group's subordinate sections in a deterministic order.
 *
 * @param sec Section name supplied by the config iterator.
 * @param[in,out] ctx Collector state cast to `wg_collect_ctx_t *`.
 * @return
 */
static void collect_worker_groups(const char* sec, void* ctx) {
    wg_collect_ctx_t* c = (wg_collect_ctx_t*)ctx;
    /* Match "worker_group.X" but not "worker_group.X.servers" or ".probe" */
    const char* after = sec + 13; /* skip "worker_group." */
    if (*after && strchr(after, '.') == NULL && *c->count < c->max) {
        worker_group_t* g = &g_groups[*c->count];
        snprintf(g->section, sizeof(g->section), "%s", sec);
        snprintf(g->servers_section, sizeof(g->servers_section), "%s.servers", sec);
        (*c->count)++;
    }
}

typedef struct {
    const char** keys;
    const char** vals;
    size_t       count;
    size_t       cap;
} srv_collect_ctx_t;

/**
 * @brief Copy key/value pairs from a config section into a bounded side buffer.
 *
 * This helper is shared by server and hook parsing paths that need a stable list
 * of entries before they start performing multi-step parsing or registration.
 *
 * @param key Section key supplied by the config iterator.
 * @param value Section value supplied by the config iterator.
 * @param[in,out] ctx Collector state cast to `srv_collect_ctx_t *`.
 * @return
 */
static void collect_srv_keys(const char* key, const char* value, void* ctx) {
    srv_collect_ctx_t* c = (srv_collect_ctx_t*)ctx;
    if (c->count < c->cap) {
        c->keys[c->count] = key;
        c->vals[c->count] = value;
        c->count++;
    }
}

static bool config_bool_enabled(const keel_config_t* config,
                                const char* section,
                                const char* key,
                                bool default_val)
{
    const char* val = keel_config_get_string(config, section, key, NULL);
    if (!val) return default_val;
    return (strcasecmp(val, "true") == 0 ||
            strcmp(val, "1") == 0 ||
            strcasecmp(val, "yes") == 0 ||
            strcasecmp(val, "on") == 0);
}

static void append_feature_name(char* buf, size_t cap, const char* name, bool* first)
{
    size_t len;
    if (!buf || cap == 0 || !name || !first) return;
    len = strlen(buf);
    if (len >= cap - 1) return;
    if (!*first) {
        snprintf(buf + len, cap - len, ", ");
        len = strlen(buf);
        if (len >= cap - 1) return;
    }
    snprintf(buf + len, cap - len, "%s", name);
    *first = false;
}

static void build_runtime_feature_list(const worker_group_t* wg,
                                       bool cluster_compression_enabled,
                                       char* out,
                                       size_t out_cap)
{
    bool first = true;
    if (!wg || !out || out_cap == 0) return;
    out[0] = '\0';

    if (KEEL_TIER_HAS_POOLING(wg->runtime_mode))
        append_feature_name(out, out_cap, "pooling", &first);
    if (KEEL_TIER_HAS_ROUTING(wg->runtime_mode))
        append_feature_name(out, out_cap, "routing", &first);
    if (KEEL_TIER_HAS_STATE_SYNC(wg->runtime_mode))
        append_feature_name(out, out_cap, "state_sync", &first);
    if (wg->txn_tracking)
        append_feature_name(out, out_cap, "transaction_tracking", &first);
    if (wg->result_cache)
        append_feature_name(out, out_cap, "result_cache", &first);
    if (wg->hooks_enabled || g_query_rule_count > 0 || g_throttle_rule_count > 0)
        append_feature_name(out, out_cap, "hooks", &first);
    if (wg->sharding_enabled || g_shard_rule_count > 0)
        append_feature_name(out, out_cap, "sharding", &first);
    if (wg->scatter_merge_enabled)
        append_feature_name(out, out_cap, "scatter_merge", &first);
    if (cluster_compression_enabled)
        append_feature_name(out, out_cap, "cluster_compression", &first);
    if (wg->wal_lsn_capture)
        append_feature_name(out, out_cap, "wal_lsn_capture", &first);
    if (wg->gtid_capture)
        append_feature_name(out, out_cap, "gtid_capture", &first);

    if (first)
        snprintf(out, out_cap, "none");
}

static bool validate_experimental_feature_gates(bool cluster_compression_enabled)
{
    bool valid = true;
    if (!g_experimental_features_enabled && g_query_rule_count > 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
            "Experimental feature requires experimental_features=true: query_rule.*");
        valid = false;
    }
    if (!g_experimental_features_enabled && g_throttle_rule_count > 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
            "Experimental feature requires experimental_features=true: throttle.*");
        valid = false;
    }
    if (!g_experimental_features_enabled && g_shard_rule_count > 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
            "Experimental feature requires experimental_features=true: shard_rule.*");
        valid = false;
    }
    if (!g_experimental_features_enabled && cluster_compression_enabled) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
            "Experimental feature requires experimental_features=true: [cluster] compress");
        valid = false;
    }

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        const worker_group_t* wg = &g_groups[gi];
        const char* section = wg->section[0] ? wg->section : "(worker_group)";
        bool allow_group_experimental =
            g_experimental_features_enabled || wg->experimental_features;
        /* mode = full enables hooks, transaction tracking, and LSN capture —
         * all of which are hardening/experimental in v0.2-alpha.  Require an
         * explicit experimental_features opt-in rather than silently enabling
         * them for users who copy a config without reading the fine print. */
        if (KEEL_TIER_IS_EXPERIMENTAL(wg->runtime_mode) && !allow_group_experimental) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s] mode=full. "
                "mode=full enables hardening/experimental subsystems (hooks, transaction "
                "tracking, LSN capture) and is not the recommended production default for "
                "v0.2-alpha. Use mode=pool or mode=smart instead.",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->result_cache) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s] result_cache=on",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->hooks_enabled) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s.hooks] section",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->sharding_enabled) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s.servers] shard_id",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->scatter_merge_enabled) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s] scatter_merge*",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->wal_lsn_capture) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s] wal_lsn_capture=on",
                section);
            valid = false;
        }
        if (!allow_group_experimental && wg->gtid_capture) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "Experimental feature requires experimental_features=true: [%s] gtid_capture=on",
                section);
            valid = false;
        }
    }

    return valid;
}

/* ============================================================================
 * Configuration
 * ============================================================================ */

typedef struct keel_proxy_config {
    const char* listen_addr;
    uint16_t    listen_port;
    const char* backend_host;
    uint16_t    backend_port;
    const char* backend_user;
    const char* backend_password;
    const char* backend_database;
    
    /* Pool settings */
    size_t      pool_min_size;
    size_t      pool_max_size;
    uint64_t    idle_timeout_ms;
    uint64_t    connect_timeout_ms;
    
    /* Worker settings */
    uint32_t    num_workers;
    bool        pin_workers;
    
    /* Session settings */
    size_t      session_pool_size;
    size_t      buffer_size;
    
    /* Protocol */
    const char* default_protocol;
    
    /* Logging */
    int         log_level;
    
    /* Stats / Instrumentation */
    const char* stats_level_str;    /* "off","basic","extended","system" */
    uint32_t    stats_interval_ms;  /* Periodic log dump interval (0=off) */
    uint32_t    hotpath_instr_mask; /* KEEL_HOT_INSTR_* runtime gates */
    uint32_t    instr_mask;         /* KEEL_INSTR_CAT_* function-level probes */

    /* Config file path (for SIGHUP reload) */
    const char* config_file;
    bool        experimental_features;

    /* Graceful shutdown drain timeout */
    uint32_t    shutdown_timeout_ms;

    /* Declarative query rules (BEFORE_ROUTE hook) */
    keel_query_rules_t* query_rules;
} keel_proxy_config_t;

/* Admin + Prometheus configuration (global) */
static keel_admin_config_t g_admin_cfg = KEEL_ADMIN_CONFIG_DEFAULT;
static keel_admin_t *g_admin = NULL;

#ifdef KEEL_HAS_OTLP
#include "../observability/otlp/keel_otlp_aggregator.h"
#include "../observability/otlp/keel_otlp_config.h"
#include "../observability/otlp/keel_otlp_exporter.h"
static keel_otlp_exporter_config_t g_otlp_cfg;
static bool                        g_otlp_enabled = false;
static keel_otlp_exporter_t*       g_otlp_exporter = NULL;
static keel_otlp_aggregator_t*     g_otlp_agg = NULL;
#endif

/* Cluster configuration (global) */
static keel_cluster_config_t g_cluster_cfg = KEEL_CLUSTER_CONFIG_DEFAULT;
static keel_cluster_t *g_cluster = NULL;

/**
 * @brief Cluster server-topology-change callback (H2).
 *
 * Called from the cluster thread when a NOTIFY_SERVER message arrives from a
 * peer.  Iterates all worker groups, finds the backend pool that matches the
 * notified host:port, and calls backend_pool_update_target() to propagate the
 * role/weight change to the connection pool.
 *
 * @param user_data  Pointer to the global g_groups array (worker_group_t[]).
 * @param action     0=add, 1=remove, 2=update.
 * @param host       Affected server hostname or IP.
 * @param port       Affected server port.
 * @param role       Server role (unused — pool derives role from server_pool).
 * @param weight     New routing weight (unused — pool derives from server_pool).
 */
static void cluster_server_notify_handler(void* user_data,
                                          uint8_t action,
                                          const char* host,
                                          uint16_t port,
                                          uint8_t role,
                                          uint32_t weight)
{
    (void)role; (void)weight;
    if (!user_data || !host) return;

    worker_group_t* groups = (worker_group_t*)user_data;

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &groups[gi];
        if (!wg->engine) continue;

        /* Walk all workers in this group — they share the same pool array. */
        uint32_t nw = keel_engine_get_num_workers(wg->engine);
        if (nw == 0) continue;

        keel_worker_t* w = keel_engine_get_worker_mut(wg->engine, 0);
        if (!w || !w->server_pool || !w->server_pools) continue;

        for (size_t si = 0; si < w->server_pool->count; si++) {
            const keel_backend_server_t* srv = &w->server_pool->servers[si];
            if (!srv->host) continue;
            if (strcmp(srv->host, host) != 0 || srv->port != port) continue;

            /* Found matching server — update its pool target. */
            if (action == 1 /* remove */) {
                /* Mark the pool slot as closed so the probe thread will skip it. */
                if (w->server_pools[si])
                    backend_pool_update_target(w->server_pools[si], NULL, 0);
            } else {
                if (w->server_pools[si])
                    backend_pool_update_target(w->server_pools[si], host, port);
            }
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "cluster notify: group=%zu action=%u server=%s:%u",
                gi, (unsigned)action, host, (unsigned)port);
            break;
        }
    }
}

/**
 * @brief Engine stats callback registered with the cluster manager.
 *
 * Supplies live connection counts from the engine so that outgoing heartbeat
 * messages carry real load data instead of zeros.
 */
static void cluster_engine_stats_cb(void* user_data,
                                     uint32_t* out_clients,
                                     uint32_t* out_backends,
                                     uint32_t* out_servers) {
    keel_engine_t* eng = (keel_engine_t*)user_data;
    *out_clients = (uint32_t)keel_engine_get_active_connections(eng);

    /* Aggregate active backend connections across all workers × all server pools. */
    uint64_t backends = 0;
    uint32_t nw = keel_engine_get_num_workers(eng);
    for (uint32_t wi = 0; wi < nw; wi++) {
        const keel_worker_t* w = keel_engine_get_worker(eng, wi);
        if (!w || !w->server_pools) continue;
        for (size_t si = 0; si < w->server_pool_count; si++) {
            backend_pool_t* pool = w->server_pools[si];
            if (!pool) continue;
            backend_pool_stats_t st;
            backend_pool_get_stats(pool, &st);
            backends += st.active_connections;
        }
    }
    *out_backends = (uint32_t)(backends > UINT32_MAX ? UINT32_MAX : backends);

    /* Server count from the engine's authoritative server pool. */
    const keel_server_pool_t* sp = keel_engine_get_server_pool(eng);
    *out_servers = sp ? (uint32_t)sp->count : 0;
}

/* Tracing configuration (global) */
static keel_trace_config_t g_trace_cfg = KEEL_TRACE_CONFIG_DEFAULT;
static keel_tracer_t *g_tracer = NULL;

/* Audit log (global, shared across all worker groups) */
static keel_audit_log_t g_audit_log;
static bool             g_audit_log_initialized = false;

/* ============================================================================
 * Logging Plugin Configuration
 * ============================================================================ */

typedef struct keel_logging_config {
    const char*             plugin_name;    /* "stdout", "file", "syslog", or path to .so */
    const char*             plugin_path;    /* Explicit path to plugin .so (overrides plugin_name) */
    const char*             log_file;       /* File path for file plugin */
    const char*             syslog_ident;   /* Syslog ident string */
    const char*             syslog_facility;/* Syslog facility name */
    const char*             log_level_str;  /* "error","warn","info","debug","all" */
    const char*             query_log_mode_str; /* "none","all","read","write" */
    bool                    log_timestamps;
    bool                    log_source;
    bool                    log_dest;
    bool                    log_username;
    bool                    log_database;
    bool                    log_query_tree;
    size_t                  max_query_len;
    bool                    use_colors;
    bool                    json_format;    /* Emit NDJSON log lines */
} keel_logging_config_t;

static keel_proxy_config_t g_config = {
    .listen_addr         = "0.0.0.0",
    .listen_port         = 6432,
    .backend_host        = "127.0.0.1",
    .backend_port        = 5432,
    .backend_user        = "postgres",
    .backend_password    = NULL,
    .backend_database    = "postgres",
    .pool_min_size       = 2,
    .pool_max_size       = 100,
    .idle_timeout_ms     = 300000,
    .connect_timeout_ms  = 10000,
    .num_workers         = 0,       /* Auto-detect */
    .pin_workers         = true,
    .session_pool_size   = 1024,
    .buffer_size         = 65536,
    .default_protocol    = "postgres",
    .log_level           = 2,
    .stats_level_str     = "basic",
    .stats_interval_ms   = 0,
    .hotpath_instr_mask  = KEEL_HOT_INSTR_ALL,
    .instr_mask          = KEEL_INSTR_CAT_NONE,
    .config_file         = NULL,
    .experimental_features = false,
    .shutdown_timeout_ms = 30000,
};

/* Per-group server pool, probe config, and probe manager
 * are now stored in worker_group_t (g_groups[]). */

/* Global logging configuration */
static keel_logging_config_t g_log_cfg = {
    .plugin_name       = "stdout",
    .plugin_path       = NULL,
    .log_file          = NULL,
    .syslog_ident      = "keel",
    .syslog_facility   = NULL,
    .log_level_str     = "info",
    .query_log_mode_str = "none",
    .log_timestamps    = true,
    .log_source        = true,
    .log_dest          = true,
    .log_username      = true,
    .log_database      = true,
    .log_query_tree    = false,
    .max_query_len     = 0,
    .use_colors        = true,
    .json_format       = false,
};

/* Global log plugin and query logger instances */
static keel_log_plugin_t* g_log_plugin = NULL;
static keel_query_log_t   g_query_log  = {0};

static void cleanup_startup_failure(keel_config_t* config, bool tls_initialized) {
#ifdef KEEL_HAS_OTLP
    if (g_otlp_agg) {
        keel_otlp_aggregator_stop(g_otlp_agg);
        keel_otlp_aggregator_destroy(g_otlp_agg);
        g_otlp_agg = NULL;
    }
    if (g_otlp_exporter) {
        keel_admin_set_otlp_exporter(g_admin, NULL);
        keel_otlp_exporter_stop(g_otlp_exporter);
        keel_otlp_exporter_destroy(g_otlp_exporter);
        g_otlp_exporter = NULL;
    }
#endif
    keel_admin_stop(g_admin);
    g_admin = NULL;

    if (g_cluster) {
        keel_cluster_destroy(g_cluster);
        g_cluster = NULL;
    }

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &g_groups[gi];

        if (wg->probe_mgr) {
            keel_probe_manager_destroy(wg->probe_mgr);
            wg->probe_mgr = NULL;
        }
        if (wg->discovery) {
            keel_discovery_stop(wg->discovery);
            keel_discovery_destroy(wg->discovery);
            wg->discovery = NULL;
        }
        if (wg->throttle_rules) {
            keel_throttle_rules_replace(&wg->throttle_rules, NULL);
        }
        if (wg->router_mgr) {
            keel_router_mgr_destroy(wg->router_mgr);
            wg->router_mgr = NULL;
        }
        if (wg->engine) {
            keel_engine_destroy(wg->engine);
            wg->engine = NULL;
        }
        if (wg->listen_fd >= 0) {
            close(wg->listen_fd);
            wg->listen_fd = -1;
        }
        if (wg->hook_registry) {
            keel_hook_registry_destroy(wg->hook_registry);
            wg->hook_registry = NULL;
        }
    }
    g_engine = NULL;

    if (g_tracer) {
        keel_tracer_destroy(g_tracer);
        g_tracer = NULL;
    }
    if (g_audit_log_initialized) {
        keel_audit_log_close(&g_audit_log);
        g_audit_log_initialized = false;
    }

    keel_config_free(config);

    keel_lua_shutdown();
    keel_python_shutdown();

    keel_query_log_shutdown(&g_query_log);
    if (g_log_plugin) {
        g_log_plugin->flush(g_log_plugin);
        g_log_plugin->close(g_log_plugin);
        g_log_plugin->destroy(g_log_plugin);
        g_log_plugin = NULL;
    }

    if (tls_initialized)
        keel_tls_cleanup();

    keel_mem_shutdown();
}

/* ============================================================================
 * Security Hardening Configuration
 * ============================================================================ */

typedef enum keel_seccomp_mode {
    KEEL_SECCOMP_OFF = 0,
    KEEL_SECCOMP_BASELINE,
    KEEL_SECCOMP_STRICT,
} keel_seccomp_mode_t;

typedef struct keel_security_config {
    bool                privilege_drop;
    const char*         run_user;
    const char*         run_group;
    bool                require_privilege_drop;

    keel_seccomp_mode_t seccomp_mode;
    bool                require_seccomp;
    bool                no_new_privs;

    bool                strict_auth;  /**< Reject deprecated auth methods (md5, trust) at startup */
} keel_security_config_t;

static keel_security_config_t g_security_cfg = {
    .privilege_drop = false,
    .run_user = "nobody",
    .run_group = "nogroup",
    .require_privilege_drop = false,
    .seccomp_mode = KEEL_SECCOMP_BASELINE, /* audit-only by default; set seccomp_mode=strict in production */
    .require_seccomp = false,
    .no_new_privs = true,
    .strict_auth = false,
};

/**
 * @brief Decode the configured seccomp policy mode.
 *
 * Unrecognized values intentionally collapse to `OFF` rather than attempting a
 * guess, because applying the wrong sandbox is far worse than leaving the process
 * unsandboxed and logging the operator error elsewhere.
 *
 * @param s Raw configuration string.
 * @return Parsed seccomp mode.
 */
static keel_seccomp_mode_t parse_seccomp_mode(const char* s) {
    if (!s || !*s) return KEEL_SECCOMP_OFF;
    if (strcasecmp(s, "off") == 0 || strcmp(s, "0") == 0)
        return KEEL_SECCOMP_OFF;
    if (strcasecmp(s, "baseline") == 0)
        return KEEL_SECCOMP_BASELINE;
    if (strcasecmp(s, "strict") == 0)
        return KEEL_SECCOMP_STRICT;
    return KEEL_SECCOMP_OFF;
}

/**
 * @brief Parse a decimal UID or GID string.
 *
 * Numeric identifiers are accepted as a fast path before falling back to passwd
 * or group database lookups.
 *
 * @param s Candidate decimal string.
 * @param[out] out Parsed numeric identifier on success.
 * @return `true` when parsing succeeded, otherwise `false`.
 */
static bool parse_numeric_id(const char* s, unsigned long* out) {
    if (!s || !*s) return false;
    char* end = NULL;
    errno = 0;
    unsigned long v = strtoul(s, &end, 10);
    if (errno != 0 || !end || *end != '\0') return false;
    *out = v;
    return true;
}

/**
 * @brief Resolve the configured runtime user and group into numeric IDs.
 *
 * The function accepts either numeric IDs or symbolic names. If the user lookup
 * succeeds and no explicit group was requested, the user's primary group becomes
 * the default target group. This matches common daemon expectations and avoids a
 * second mandatory configuration field in the simplest deployments.
 *
 * @param[out] out_uid Resolved target UID.
 * @param[out] out_gid Resolved target GID.
 * @return 0 on success, or -1 if either account lookup fails.
 */
static int resolve_security_ids(uid_t* out_uid, gid_t* out_gid) {
    uid_t target_uid = (uid_t)-1;
    gid_t target_gid = (gid_t)-1;

    unsigned long idv = 0;
    if (parse_numeric_id(g_security_cfg.run_user, &idv)) {
        target_uid = (uid_t)idv;
    } else {
        struct passwd* pw = getpwnam(g_security_cfg.run_user);
        if (!pw) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                           "security: unknown run_user '%s'",
                           g_security_cfg.run_user ? g_security_cfg.run_user : "(null)");
            return -1;
        }
        target_uid = pw->pw_uid;
        if (!g_security_cfg.run_group || !*g_security_cfg.run_group) {
            target_gid = pw->pw_gid;
        }
    }

    if (target_gid == (gid_t)-1) {
        if (parse_numeric_id(g_security_cfg.run_group, &idv)) {
            target_gid = (gid_t)idv;
        } else {
            struct group* gr = getgrnam(g_security_cfg.run_group);
            if (!gr) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                               "security: unknown run_group '%s'",
                               g_security_cfg.run_group ? g_security_cfg.run_group : "(null)");
                return -1;
            }
            target_gid = gr->gr_gid;
        }
    }

    *out_uid = target_uid;
    *out_gid = target_gid;
    return 0;
}

/**
 * @brief Drop root privileges after privileged startup work is complete.
 *
 * The bootstrap path may need elevated privileges to bind low-numbered ports or
 * perform other environment-specific setup. Once that work is finished, KEEL can
 * deliberately discard those privileges before entering steady-state operation.
 * The implementation clears supplementary groups, sets the target GID before the
 * UID, and finally checks that privilege re-acquisition is impossible. That last
 * check intentionally fails hard if the platform behaves unexpectedly because a
 * reversible drop would invalidate the entire security model.
 *
 * @return 0 on success, 0 when privilege dropping was optional and not possible,
 *         or -1 when the configured policy requires a drop that cannot be safely
 *         completed.
 */
static int apply_privilege_drop(void) {
    if (!g_security_cfg.privilege_drop)
        return 0;

    if (geteuid() != 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                      "security: privilege_drop requested but process is not root (euid=%u)",
                      (unsigned)geteuid());
        return g_security_cfg.require_privilege_drop ? -1 : 0;
    }

    uid_t target_uid;
    gid_t target_gid;
    if (resolve_security_ids(&target_uid, &target_gid) < 0)
        return -1;

    if (setgroups(0, NULL) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: setgroups(0) failed: %s", strerror(errno));
        return -1;
    }

    if (setgid(target_gid) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: setgid(%u) failed: %s",
                       (unsigned)target_gid, strerror(errno));
        return -1;
    }

    if (setuid(target_uid) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: setuid(%u) failed: %s",
                       (unsigned)target_uid, strerror(errno));
        return -1;
    }

    if (setuid(0) == 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: privilege drop is reversible (unexpected)");
        return -1;
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "security: privileges dropped to uid=%u gid=%u",
                  (unsigned)geteuid(), (unsigned)getegid());
    return 0;
}

#ifdef __linux__
/**
 * @brief Install a baseline seccomp filter that blocks obviously dangerous process-control syscalls.
 *
 * The baseline policy is intentionally permissive: it does not attempt to model
 * the entire KEEL syscall surface, only to prohibit operations such as `execve`,
 * module loading, or ptrace that should never be required in normal proxy work.
 * This gives a useful hardening layer with low compatibility risk.
 *
 * @return 0 on success, or -1 if `no_new_privs` or seccomp installation fails.
 */
static int apply_seccomp_filter_baseline(void) {
    struct sock_filter filter[] = {
#if KEEL_AUDIT_ARCH_NATIVE != 0u
        BPF_STMT(BPF_LD  | BPF_W | BPF_ABS, (uint32_t)offsetof(struct seccomp_data, arch)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, KEEL_AUDIT_ARCH_NATIVE, 1, 0),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
#endif

        BPF_STMT(BPF_LD  | BPF_W | BPF_ABS, (uint32_t)offsetof(struct seccomp_data, nr)),

        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_execve, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#ifdef __NR_execveat
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_execveat, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#endif
#ifdef __NR_ptrace
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_ptrace, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#endif
#ifdef __NR_kexec_load
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_kexec_load, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#endif
#ifdef __NR_init_module
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_init_module, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#endif
#ifdef __NR_finit_module
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, __NR_finit_module, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#endif

        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    };

    struct sock_fprog prog = {
        .len = (unsigned short)(sizeof(filter) / sizeof(filter[0])),
        .filter = filter,
    };

    if (g_security_cfg.no_new_privs) {
        if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                           "security: PR_SET_NO_NEW_PRIVS failed: %s", strerror(errno));
            return -1;
        }
    }

    if (prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog) != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: baseline seccomp install failed: %s", strerror(errno));
        return -1;
    }

    return 0;
}

/**
 * @brief Install a tighter allowlist seccomp policy tailored to KEEL's runtime syscall surface.
 *
 * Unlike the baseline filter, the strict policy enumerates the syscalls needed by
 * sockets, epoll/io_uring, memory management, timers, threading, and basic file
 * inspection. This materially reduces post-exploitation freedom but comes with a
 * compatibility cost: adding a new platform feature may require updating the
 * allowlist. For that reason the policy is opt-in.
 *
 * @return 0 on success, or -1 if installation fails.
 */
static int apply_seccomp_filter_strict(void) {
    struct sock_filter filter[] = {
#if KEEL_AUDIT_ARCH_NATIVE != 0u
        BPF_STMT(BPF_LD  | BPF_W | BPF_ABS, (uint32_t)offsetof(struct seccomp_data, arch)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, KEEL_AUDIT_ARCH_NATIVE, 1, 0),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
#endif

        BPF_STMT(BPF_LD  | BPF_W | BPF_ABS, (uint32_t)offsetof(struct seccomp_data, nr)),

#define KEEL_SC_ALLOW(nr) \
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, nr, 0, 1), \
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW)

        KEEL_SC_ALLOW(__NR_read),
        KEEL_SC_ALLOW(__NR_write),
    #ifdef __NR_readv
        KEEL_SC_ALLOW(__NR_readv),
    #endif
    #ifdef __NR_writev
        KEEL_SC_ALLOW(__NR_writev),
    #endif
        KEEL_SC_ALLOW(__NR_close),
        KEEL_SC_ALLOW(__NR_recvfrom),
        KEEL_SC_ALLOW(__NR_sendto),
        KEEL_SC_ALLOW(__NR_recvmsg),
        KEEL_SC_ALLOW(__NR_sendmsg),
#ifdef __NR_recvmmsg
        KEEL_SC_ALLOW(__NR_recvmmsg),
#endif
#ifdef __NR_sendmmsg
        KEEL_SC_ALLOW(__NR_sendmmsg),
#endif
        KEEL_SC_ALLOW(__NR_socket),
    #ifdef __NR_socketpair
        KEEL_SC_ALLOW(__NR_socketpair),
    #endif
        KEEL_SC_ALLOW(__NR_bind),
        KEEL_SC_ALLOW(__NR_listen),
        KEEL_SC_ALLOW(__NR_accept),
#ifdef __NR_accept4
        KEEL_SC_ALLOW(__NR_accept4),
#endif
        KEEL_SC_ALLOW(__NR_connect),
        KEEL_SC_ALLOW(__NR_shutdown),
        KEEL_SC_ALLOW(__NR_getsockname),
        KEEL_SC_ALLOW(__NR_getpeername),
        KEEL_SC_ALLOW(__NR_setsockopt),
        KEEL_SC_ALLOW(__NR_getsockopt),

        KEEL_SC_ALLOW(__NR_epoll_create1),
        KEEL_SC_ALLOW(__NR_epoll_ctl),
    #ifdef __NR_epoll_wait
        KEEL_SC_ALLOW(__NR_epoll_wait),
    #endif
#ifdef __NR_epoll_pwait
        KEEL_SC_ALLOW(__NR_epoll_pwait),
#endif

#ifdef __NR_io_uring_setup
        KEEL_SC_ALLOW(__NR_io_uring_setup),
#endif
#ifdef __NR_io_uring_enter
        KEEL_SC_ALLOW(__NR_io_uring_enter),
#endif
#ifdef __NR_io_uring_register
        KEEL_SC_ALLOW(__NR_io_uring_register),
#endif

        KEEL_SC_ALLOW(__NR_futex),
        KEEL_SC_ALLOW(__NR_mmap),
        KEEL_SC_ALLOW(__NR_munmap),
#ifdef __NR_mremap
        KEEL_SC_ALLOW(__NR_mremap),
#endif
        KEEL_SC_ALLOW(__NR_mprotect),
#ifdef __NR_madvise
        KEEL_SC_ALLOW(__NR_madvise),
#endif
        KEEL_SC_ALLOW(__NR_brk),

        KEEL_SC_ALLOW(__NR_rt_sigaction),
        KEEL_SC_ALLOW(__NR_rt_sigprocmask),
        KEEL_SC_ALLOW(__NR_rt_sigreturn),
        KEEL_SC_ALLOW(__NR_sigaltstack),
#ifdef __NR_rt_sigtimedwait
        KEEL_SC_ALLOW(__NR_rt_sigtimedwait),  /* sigwait()/sigtimedwait() — used by engine signal loop */
#endif
#ifdef __NR_rt_sigpending
        KEEL_SC_ALLOW(__NR_rt_sigpending),
#endif
        KEEL_SC_ALLOW(__NR_clock_gettime),
        KEEL_SC_ALLOW(__NR_nanosleep),
#ifdef __NR_clock_nanosleep
        KEEL_SC_ALLOW(__NR_clock_nanosleep),
#endif

        KEEL_SC_ALLOW(__NR_getpid),
        KEEL_SC_ALLOW(__NR_gettid),
#ifdef __NR_tgkill
        KEEL_SC_ALLOW(__NR_tgkill),
#endif
        KEEL_SC_ALLOW(__NR_exit),
        KEEL_SC_ALLOW(__NR_exit_group),

        KEEL_SC_ALLOW(__NR_openat),
        KEEL_SC_ALLOW(__NR_newfstatat),
        KEEL_SC_ALLOW(__NR_fstat),
    #ifdef __NR_readlink
        KEEL_SC_ALLOW(__NR_readlink),
    #endif
        KEEL_SC_ALLOW(__NR_lseek),
        KEEL_SC_ALLOW(__NR_fcntl),
        KEEL_SC_ALLOW(__NR_ioctl),

#ifdef __NR_dup
        KEEL_SC_ALLOW(__NR_dup),
#endif
    #ifdef __NR_dup2
        KEEL_SC_ALLOW(__NR_dup2),
    #endif
#ifdef __NR_dup3
        KEEL_SC_ALLOW(__NR_dup3),
#endif
        KEEL_SC_ALLOW(__NR_pipe2),
#ifdef __NR_eventfd2
        KEEL_SC_ALLOW(__NR_eventfd2),
#endif

#ifdef __NR_timerfd_create
        KEEL_SC_ALLOW(__NR_timerfd_create),
#endif
#ifdef __NR_timerfd_settime
        KEEL_SC_ALLOW(__NR_timerfd_settime),
#endif
#ifdef __NR_timerfd_gettime
        KEEL_SC_ALLOW(__NR_timerfd_gettime),
#endif

        KEEL_SC_ALLOW(__NR_set_tid_address),
        KEEL_SC_ALLOW(__NR_set_robust_list),
    #ifdef __NR_rseq
        KEEL_SC_ALLOW(__NR_rseq),
    #endif
        KEEL_SC_ALLOW(__NR_clone),
#ifdef __NR_clone3
        KEEL_SC_ALLOW(__NR_clone3),
#endif
        KEEL_SC_ALLOW(__NR_sched_yield),
#ifdef __NR_sched_getaffinity
        KEEL_SC_ALLOW(__NR_sched_getaffinity),
#endif
#ifdef __NR_sched_setaffinity
        KEEL_SC_ALLOW(__NR_sched_setaffinity),
#endif
        KEEL_SC_ALLOW(__NR_uname),
#ifdef __NR_getrandom
        KEEL_SC_ALLOW(__NR_getrandom),
#endif
        KEEL_SC_ALLOW(__NR_prlimit64),
        KEEL_SC_ALLOW(__NR_getrlimit),
        KEEL_SC_ALLOW(__NR_setrlimit),

        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | (uint32_t)EPERM),
#undef KEEL_SC_ALLOW
    };

    struct sock_fprog prog = {
        .len = (unsigned short)(sizeof(filter) / sizeof(filter[0])),
        .filter = filter,
    };

    if (g_security_cfg.no_new_privs) {
        if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                           "security: PR_SET_NO_NEW_PRIVS failed: %s", strerror(errno));
            return -1;
        }
    }

    if (prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog) != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "security: strict seccomp install failed: %s", strerror(errno));
        return -1;
    }

    return 0;
}
#endif

/**
 * @brief Apply the configured seccomp policy when supported by the platform.
 *
 * The policy dispatcher keeps the platform-specific filter construction hidden
 * behind a single bootstrap hook and enforces the `require_seccomp` contract on
 * non-Linux builds.
 *
 * @return 0 on success or when seccomp is disabled, otherwise -1.
 */
static int apply_seccomp_policy(void) {
    if (g_security_cfg.seccomp_mode == KEEL_SECCOMP_OFF)
        return 0;

#ifdef __linux__
    int rc = (g_security_cfg.seccomp_mode == KEEL_SECCOMP_STRICT)
             ? apply_seccomp_filter_strict()
             : apply_seccomp_filter_baseline();
    if (rc == 0) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                      "security: seccomp policy installed (%s)",
                      g_security_cfg.seccomp_mode == KEEL_SECCOMP_STRICT ? "strict" : "baseline");
    }
    return rc;
#else
    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                  "security: seccomp requested but unsupported on this platform");
    return g_security_cfg.require_seccomp ? -1 : 0;
#endif
}

/**
 * @brief Apply all configured runtime hardening steps in the required order.
 *
 * Privilege dropping runs before seccomp so the process can still make the small
 * set of credential-changing syscalls that would later be blocked by a sandbox.
 * The order is part of the security contract and should not be inverted without
 * revisiting the syscall policy.
 *
 * @return 0 on success, or -1 if any mandatory hardening step fails.
 */
static int apply_runtime_security_policy(void) {
    if (apply_privilege_drop() < 0)
        return -1;
    if (apply_seccomp_policy() < 0)
        return -1;
    return 0;
}

/* ============================================================================
 * Main Entry Point
 * ============================================================================ */

/**
 * @brief Bootstrap the KEEL process and hand control to the engine event loop.
 *
 * The entry sequence is intentionally linear and fail-fast. It raises resource
 * limits, parses command-line and file configuration, expands worker groups,
 * initializes shared services such as logging/admin/stats/hooks, applies runtime
 * hardening, creates listeners and engines, and only then enters steady-state
 * waiting for signals. Each stage validates enough state to keep later layers
 * simple; for example, worker threads receive already-materialized pool, probe,
 * routing, and TLS settings instead of having to decode raw config themselves.
 *
 * The startup path is also where KEEL chooses between flexibility and safety.
 * Rather than lazily constructing subsystems on first use, it performs nearly all
 * dependency creation up front. That increases startup work and makes this file
 * large, but it means deployment failures surface immediately, before the process
 * starts accepting client traffic.
 *
 * @param argc Raw argument count.
 * @param argv Raw argument vector.
 * @return Process exit status suitable for the hosting shell or service manager.
 */
int main(int argc, char** argv) {
    /* Disable buffering for consistent output */
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    /* Initialize memory subsystem (stats tracking, debug features) */
    keel_mem_init(NULL);

    /* Raise file descriptor limit to the hard maximum.
     * The default soft limit (often 1024) is far too low for a proxy
     * that may handle thousands of concurrent client + backend connections. */
    {
        struct rlimit rl;
        if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
            rlim_t target = rl.rlim_max;
            if (target == RLIM_INFINITY || target > 1048576)
                target = 1048576;  /* Cap at 1M to avoid absurd values */
            if (rl.rlim_cur < target) {
                rl.rlim_cur = target;
                if (setrlimit(RLIMIT_NOFILE, &rl) < 0) {
                    fprintf(stderr, "Warning: could not raise NOFILE limit to %lu: %s\n",
                            (unsigned long)target, strerror(errno));
                }
            }
        }
    }

    /* Parse command line */
    options_t opts;
    if (parse_options(argc, argv, &opts) < 0) {
        print_usage(argv[0]);
        return 1;
    }
    
    if (opts.help) {
        print_usage(argv[0]);
        return 0;
    }
    
    if (opts.version) {
        print_version();
        return 0;
    }

    if (opts.migrate_in) {
        keel_error_t rc = keel_config_migrate_file(opts.migrate_in, opts.migrate_out);
        if (rc != KEEL_OK) {
            fprintf(stderr, "keel: --migrate-config: migration failed (%d)\n", (int)rc);
            return 1;
        }
        if (opts.migrate_out && strcmp(opts.migrate_out, "-") != 0) {
            fprintf(stderr, "keel: migrated %s -> %s (schema v%d)\n",
                    opts.migrate_in, opts.migrate_out, KEEL_CONFIG_SCHEMA_VERSION);
        }
        return 0;
    }

    g_config.hotpath_instr_mask = build_hotpath_instr_mask_from_env();

    /* Apply CLI security overrides */
    if (opts.strict_auth)
        g_security_cfg.strict_auth = true;
    
    /* Accept positional argument as config file if -c was not given */
    if (!opts.config_file && optind < argc) {
        opts.config_file = argv[optind];
    }
    
    /* Load configuration file if specified */
    keel_config_t* config = NULL;   /* freed after engines shut down */
    if (opts.config_file) {
        printf("Configuration file: %s\n", opts.config_file);
        g_config.config_file = opts.config_file;
        
        config = keel_config_load(opts.config_file);
        if (!config) {
            fprintf(stderr, "Warning: Failed to load config file: %s\n", opts.config_file);
        } else {
            /* Schema version gate: refuse to start on a v1 (pre-rename) INI.
             * The migrator subcommand (keel --migrate-config IN -o OUT)
             * converts a v1 file to v2 in place. */
            int64_t cfg_ver = keel_config_get_int(config, "keel", "config_version", 0);
            if (cfg_ver != KEEL_CONFIG_SCHEMA_VERSION) {
                fprintf(stderr,
                        "keel: configuration schema mismatch in '%s'\n"
                        "  found:    config_version = %lld\n"
                        "  required: config_version = %d (in [keel] section)\n"
                        "\n"
                        "v2 renames unit-suffixed INI keys (idle_timeout_ms ->\n"
                        "idle_timeout, session_max_buffered_bytes ->\n"
                        "session_max_buffered, etc.) and moves the unit into the\n"
                        "value (e.g. idle_timeout = 5m, session_max_buffered = 64KiB).\n"
                        "\n"
                        "To migrate an existing v1 config in place:\n"
                        "  keel --migrate-config %s -o %s.v2\n",
                        opts.config_file,
                        (long long)cfg_ver,
                        KEEL_CONFIG_SCHEMA_VERSION,
                        opts.config_file, opts.config_file);
                keel_config_free(config);
                return 1;
            }

            /* Compute the config file's directory so that hook script= paths
             * that are relative are resolved correctly regardless of CWD. */
            char config_dir[PATH_MAX] = {0};
            {
                char abs_path[PATH_MAX];
                if (realpath(opts.config_file, abs_path)) {
                    strncpy(config_dir, abs_path, sizeof(config_dir) - 1);
                    char* last_slash = strrchr(config_dir, '/');
                    if (last_slash) *last_slash = '\0';
                }
            }

            /* Load [keel] section - global settings */
            g_config.log_level = (int)keel_config_get_int(config, "keel", "log_level", g_config.log_level);
            g_config.experimental_features = config_bool_enabled(
                config, "keel", "experimental_features", false);
            g_experimental_features_enabled = g_config.experimental_features;
            
            /* Graceful shutdown drain timeout */
            {
                int64_t dt = keel_config_get_duration_ms(config, "keel", "shutdown_timeout", 30000);
                if (dt >= 0) g_config.shutdown_timeout_ms = (uint32_t)dt;
            }

            /* R2: process-wide INT64 shard-key hash mode.
             *
             * Default is "legacy" (preserves placement for existing
             * deployments). Set to "abs" to match the Python test contract
             * (rows at id=-7 and id=7 land on the same shard).  Env var
             * KEEL_SHARD_KEY_HASH overrides the keel.ini value. */
            {
                const char* env = getenv("KEEL_SHARD_KEY_HASH");
                const char* val = env && env[0]
                                  ? env
                                  : keel_config_get_string(config, "keel", "shard_key_hash", "legacy");
                keel_shard_hash_mode_t mode = KEEL_SHARD_HASH_LEGACY;
                if (keel_shard_parse_hash_mode(val, &mode) == KEEL_OK) {
                    keel_shard_set_hash_mode(mode);
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CONFIG,
                                  "shard_key_hash mode = %s (%s)", val,
                                  env ? "from env KEEL_SHARD_KEY_HASH"
                                      : "from [keel] section");
                } else {
                    KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG,
                                  "unknown shard_key_hash value '%s' "
                                  "(expected: legacy|abs); keeping legacy",
                                  val);
                }
            }

            g_query_rule_count = keel_config_count_sections_prefix(config, "query_rule.");
            g_throttle_rule_count = keel_config_count_sections_prefix(config, "throttle.");
            g_shard_rule_count = keel_config_count_sections_prefix(config, "shard_rule.");
            
            /* Discover all worker_group.* sections */
            {
                wg_collect_ctx_t cctx = { &g_num_groups, KEEL_MAX_WORKER_GROUPS };
                keel_config_iter_sections_prefix(config, "worker_group.",
                                                collect_worker_groups, &cctx);
            }

            /* Parse each worker group */
            for (size_t gi = 0; gi < g_num_groups; gi++) {
                worker_group_t* wg = &g_groups[gi];
                worker_group_defaults(wg);
                /* section / servers_section already set by collect_worker_groups */
                const char* section = wg->section;

                wg->name = keel_config_get_string(config, section, "name", section + 13);

                const char* addr = keel_config_get_string(config, section, "bind_addr", NULL);
                if (addr) wg->listen_addr = addr;

                int64_t port = keel_config_get_int(config, section, "bind_port", 0);
                if (port > 0 && port <= 65535) wg->listen_port = (uint16_t)port;

                int64_t workers = keel_config_get_int(config, section, "num_workers", 0);
                if (workers > 0) wg->num_workers = (uint32_t)workers;

                const char* protocol = keel_config_get_string(config, section, "protocol", NULL);
                if (protocol) wg->default_protocol = protocol;

                const char* database = keel_config_get_string(config, section, "default_database", NULL);
                if (database) wg->backend_database = database;

                const char* server_user = keel_config_get_string(config, section, "server_user", NULL);
                if (server_user) wg->backend_user = server_user;

                const char* server_password = keel_config_get_string(config, section, "server_password", NULL);
                if (server_password) wg->backend_password = server_password;

                int64_t min_ps = keel_config_get_int(config, section, "min_pool_size", (int64_t)wg->pool_min_size);
                if (min_ps > 0) wg->pool_min_size = (size_t)min_ps;

                int64_t max_ps = keel_config_get_int(config, section, "max_pool_size", (int64_t)wg->pool_max_size);
                if (max_ps > 0) wg->pool_max_size = (size_t)max_ps;

                int64_t mcpw = keel_config_get_int(config, section, "max_conns_per_worker", (int64_t)wg->session_pool_size);
                if (mcpw > 0) wg->session_pool_size = (size_t)mcpw;

                int64_t mc = keel_config_get_int(config, section, "max_clients", (int64_t)wg->max_clients);
                if (mc >= 0) wg->max_clients = (uint32_t)mc;

                /* Optional operational tuning (sensible defaults applied above) */
                {
                    int64_t v;
                    v = keel_config_get_duration_ms(config, section, "idle_timeout",
                                            (int64_t)wg->idle_timeout_ms);
                    if (v > 0) wg->idle_timeout_ms = (uint64_t)v;

                    /* Also accept human-readable keys: client_idle_timeout = 5m / 300s */
                    {
                        const char* ts = keel_config_get_string(config, section,
                                            "client_idle_timeout", NULL);
                        if (ts) {
                            int tv = atoi(ts);
                            if (tv > 0) {
                                if      (strchr(ts, 'm')) tv *= 60000;
                                else if (strchr(ts, 's')) tv *= 1000;
                                wg->idle_timeout_ms = (uint64_t)tv;
                            }
                        }
                    }

                    v = keel_config_get_duration_ms(config, section, "connect_timeout",
                                            (int64_t)wg->connect_timeout_ms);
                    if (v > 0) wg->connect_timeout_ms = (uint64_t)v;

                    /* Also accept human-readable keys: client_connect_timeout = 30s */
                    {
                        const char* ts = keel_config_get_string(config, section,
                                            "client_connect_timeout", NULL);
                        if (ts) {
                            int tv = atoi(ts);
                            if (tv > 0) {
                                if      (strchr(ts, 'm')) tv *= 60000;
                                else if (strchr(ts, 's')) tv *= 1000;
                                wg->connect_timeout_ms = (uint64_t)tv;
                            }
                        }
                    }

                    v = keel_config_get_duration_ms(config, section, "pool_prune_interval",
                                            (int64_t)wg->pool_prune_interval_ms);
                    if (v > 0) wg->pool_prune_interval_ms = (uint32_t)v;

                    v = keel_config_get_duration_ms(config, section, "pool_refill_interval",
                                            (int64_t)wg->pool_refill_interval_ms);
                    if (v >= 100) wg->pool_refill_interval_ms = (uint32_t)v;

                    v = keel_config_get_duration_ms(config, section, "pool_refill_backoff",
                                            (int64_t)wg->pool_refill_backoff_ms);
                    if (v > 0) wg->pool_refill_backoff_ms = (uint32_t)v;

                    v = keel_config_get_int(config, section, "pool_max_waiting", 0);
                    if (v >= 0) wg->pool_max_waiting = (uint32_t)v;

                    v = keel_config_get_duration_ms(config, section, "pool_wait_timeout", 0);
                    if (v >= 0) wg->pool_wait_timeout_ms = (uint64_t)v;

                    v = keel_config_get_bytes(config, section, "session_max_buffered", 0);
                    if (v == 0 || v >= 4096)
                        wg->session_max_buffered_bytes = (size_t)v;
                    else if (v > 0)
                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                            "[%s] session_max_buffered=%lld below minimum 4096 — ignored",
                            wg->name, (long long)v);

                    v = keel_config_get_bytes(config, section, "backend_max_replay", 0);
                    if (v == 0 || v >= 4096)
                        wg->backend_max_replay_bytes = (size_t)v;
                    else if (v > 0)
                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                            "[%s] backend_max_replay=%lld below minimum 4096 — ignored",
                            wg->name, (long long)v);

                    v = keel_config_get_int(config, section, "listen_backlog",
                                            (int64_t)wg->listen_backlog);
                    if (v > 0) wg->listen_backlog = (uint32_t)v;

                    /* io_uring buffer ring settings */
                    int64_t ubr = keel_config_get_int(config, section,
                                                      "use_buf_rings", 0);
                    wg->use_buf_rings = (ubr != 0);
                    v = keel_config_get_int(config, section, "buf_ring_size", 0);
                    if (v >= 0) wg->buf_ring_size = (uint32_t)v;

                    /* io_uring SQ polling */
                    int64_t sqp = keel_config_get_int(config, section,
                                                      "sqpoll", 0);
                    wg->sqpoll = (sqp != 0);
                    v = keel_config_get_duration_ms(config, section, "sqpoll_idle", 1000);
                    if (v > 0) wg->sqpoll_idle_ms = (uint32_t)v;

                    /* Prepared-statement pooling mode */
                    {
                        const char* ps_str = keel_config_get_string(
                            config, section, "prepared_statement", "virtualize");
                        if      (ps_str && strcmp(ps_str, "pinning")   == 0)
                            wg->ps_mode = KEEL_PS_MODE_PINNING;
                        else if (ps_str && strcmp(ps_str, "tracking")  == 0)
                            wg->ps_mode = KEEL_PS_MODE_TRACKING;
                        else if (ps_str && strcmp(ps_str, "anonymous") == 0)
                            wg->ps_mode = KEEL_PS_MODE_ANONYMOUS;
                        else if (ps_str && strcmp(ps_str, "off")       == 0)
                            wg->ps_mode = KEEL_PS_MODE_OFF;
                        else
                            wg->ps_mode = KEEL_PS_MODE_VIRTUALIZE;
                    }

                    /* Runtime mode tier (proxy / pool / smart / full) */
                    {
                        const char* mode_str = keel_config_get_string(
                            config, section, "mode", "pool");
                        wg->runtime_mode = keel_tier_parse(mode_str);
                    }

                    /* Per-group override for experimental gates */
                    wg->experimental_features = config_bool_enabled(
                        config, section, "experimental_features",
                        g_config.experimental_features);

                    /* Replication uncertainty tracking */
                    {
                        const char* tt = keel_config_get_string(
                            config, section, "transaction_tracking", "off");
                        wg->txn_tracking = (tt && strcmp(tt, "on") == 0);
                    }

                    /* Zero-copy fast network path (default: on) */
                    wg->fast_network_path = keel_config_get_bool(
                        config, section, "fast_network_path", true);

                    /* Result cache — reserved for future use (default: off) */
                    wg->result_cache = keel_config_get_bool(
                        config, section, "result_cache", false);

                    /* Explicit experimental feature toggles */
                    wg->scatter_merge_enabled = config_bool_enabled(
                        config, section, "scatter_merge", false);
                    wg->wal_lsn_capture = config_bool_enabled(
                        config, section, "wal_lsn_capture", false);
                    wg->gtid_capture = config_bool_enabled(
                        config, section, "gtid_capture", false);

                    /* Sticky-primary TTL */
                    {
                        int64_t spt = keel_config_get_duration_ms(config, section, "sticky_primary_ttl", -1);
                        if (spt >= 0)
                            wg->sticky_primary_ttl_ms = (uint32_t)spt;
                    }

                    /* Connection rebalancing */
                    {
                        const char* rb = keel_config_get_string(
                            config, section, "rebalance", "on");
                        wg->rebalance_enabled = (!rb || strcmp(rb, "off") != 0);

                        v = keel_config_get_duration_ms(config, section, "rebalance_interval",
                                                (int64_t)wg->rebalance_interval_ms);
                        if (v > 0) wg->rebalance_interval_ms = (uint32_t)v;

                        v = keel_config_get_int(config, section, "rebalance_threshold_pct",
                                                (int64_t)wg->rebalance_threshold_pct);
                        if (v > 100) wg->rebalance_threshold_pct = (uint32_t)v;

                        v = keel_config_get_int(config, section, "rebalance_max_per_tick",
                                                (int64_t)wg->rebalance_max_per_tick);
                        if (v > 0) wg->rebalance_max_per_tick = (uint32_t)v;
                    }

                    /* Scatter-merge memory budget and spill directory */
                    {
                        /* scatter_merge_max_mem: memory limit, bytes (e.g. "32MiB"; 0 = default 32 MiB) */
                        int64_t smm = keel_config_get_bytes(config, section, "scatter_merge_max_mem", 0);
                        if (smm > 0)
                            wg->scatter_merge_max_mem_bytes = (size_t)smm;
                        if (smm > 0)
                            wg->scatter_merge_enabled = true;

                        /* scatter_merge_spill_dir: directory for temporary spill files */
                        const char* ssd = keel_config_get_string(config, section,
                                                                  "scatter_merge_spill_dir",
                                                                  NULL);
                        if (ssd && ssd[0] != '\0')
                            snprintf(wg->scatter_merge_spill_dir_buf,
                                     sizeof wg->scatter_merge_spill_dir_buf,
                                     "%s", ssd);
                        if (ssd && ssd[0] != '\0')
                            wg->scatter_merge_enabled = true;
                    }

                    /* Frontend TLS configuration */
                    {
                        /* TLS mode (disable/prefer/require) */
                        const char* tls_mode_str = keel_config_get_string(
                            config, section, "tls_mode", "disable");
                        if (tls_mode_str) {
                            snprintf(wg->tls_mode_buf, sizeof(wg->tls_mode_buf), "%s", tls_mode_str);
                            if      (strcmp(tls_mode_str, "prefer") == 0)  wg->tls_config.mode = KEEL_TLS_PREFER;
                            else if (strcmp(tls_mode_str, "require") == 0) wg->tls_config.mode = KEEL_TLS_REQUIRE;
                            else                                            wg->tls_config.mode = KEEL_TLS_DISABLE;
                        }

                        /* Certificate and key files */
                        const char* cert_file = keel_config_get_string(config, section, "tls_cert_file", NULL);
                        if (cert_file) {
                            snprintf(wg->tls_cert_file_buf, sizeof(wg->tls_cert_file_buf), "%s", cert_file);
                            wg->tls_config.cert_file = wg->tls_cert_file_buf;
                        }

                        const char* key_file = keel_config_get_string(config, section, "tls_key_file", NULL);
                        if (key_file) {
                            snprintf(wg->tls_key_file_buf, sizeof(wg->tls_key_file_buf), "%s", key_file);
                            wg->tls_config.key_file = wg->tls_key_file_buf;
                        }

                        const char* ca_file = keel_config_get_string(config, section, "tls_ca_file", NULL);
                        if (ca_file) {
                            snprintf(wg->tls_ca_file_buf, sizeof(wg->tls_ca_file_buf), "%s", ca_file);
                            wg->tls_config.ca_file = wg->tls_ca_file_buf;
                        }

                        /* Peer verification mode — defaults to "require" (safe default).
                         * Explicitly setting "none" with TLS enabled emits a security warning. */
                        const char* verify_str = keel_config_get_string(
                            config, section, "tls_verify_peer", "require");
                        if (verify_str) {
                            snprintf(wg->tls_verify_buf, sizeof(wg->tls_verify_buf), "%s", verify_str);
                            if      (strcmp(verify_str, "optional") == 0) wg->tls_config.verify_peer = KEEL_TLS_VERIFY_OPTIONAL;
                            else if (strcmp(verify_str, "require") == 0 || strcmp(verify_str, "required") == 0)
                                wg->tls_config.verify_peer = KEEL_TLS_VERIFY_REQUIRED;
                            else {
                                wg->tls_config.verify_peer = KEEL_TLS_VERIFY_NONE;
                                if (wg->tls_config.mode != KEEL_TLS_DISABLE) {
                                    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                                        "SECURITY: [%s] tls_verify_peer=none disables client certificate "
                                        "verification — connections are vulnerable to MITM attacks. "
                                        "Set tls_verify_peer=require for production deployments.",
                                        section);
                                }
                            }
                        }

                        /* Kernel TLS acceleration */
                        int64_t ktls = keel_config_get_int(config, section, "ktls_enabled", 1);
                        wg->tls_config.ktls_enabled = (ktls != 0);

                        /* Cipher policy */
                        const char* tls_ciphers = keel_config_get_string(
                            config, section, "tls_ciphers", NULL);
                        if (tls_ciphers) {
                            wg->tls_config.ciphers = (char*)tls_ciphers;
                        }
                        const char* tls_ciphersuites = keel_config_get_string(
                            config, section, "tls_ciphersuites", NULL);
                        if (tls_ciphersuites) {
                            wg->tls_config.ciphersuites = (char*)tls_ciphersuites;
                        }

                        /* Minimum TLS version */
                        const char* tls_min_ver = keel_config_get_string(
                            config, section, "tls_min_version", NULL);
                        if (tls_min_ver) {
                            if (strcmp(tls_min_ver, "1.3") == 0 || strcmp(tls_min_ver, "TLSv1.3") == 0)
                                wg->tls_config.min_version = KEEL_TLS_VERSION_1_3;
                            else if (strcmp(tls_min_ver, "1.2") == 0 || strcmp(tls_min_ver, "TLSv1.2") == 0)
                                wg->tls_config.min_version = KEEL_TLS_VERSION_1_2;
                        }

                        /* Timeouts */
                        int64_t hs_timeout = keel_config_get_duration_ms(config, section, "tls_handshake_timeout", 10000);
                        if (hs_timeout > 0) wg->tls_config.handshake_timeout_ms = (size_t)hs_timeout;

                        int64_t read_timeout = keel_config_get_duration_ms(config, section, "tls_read_timeout", 30000);
                        if (read_timeout > 0) wg->tls_config.read_timeout_ms = (size_t)read_timeout;
                    }

                    /* Backend TLS configuration */
                    {
                        /* Backend TLS mode */
                        const char* backend_tls_mode = keel_config_get_string(
                            config, section, "backend_tls_mode", "disable");
                        if (backend_tls_mode) {
                            snprintf(wg->backend_tls_mode_buf, sizeof(wg->backend_tls_mode_buf), "%s", backend_tls_mode);
                            if      (strcmp(backend_tls_mode, "prefer") == 0)  wg->backend_tls_config.mode = KEEL_TLS_PREFER;
                            else if (strcmp(backend_tls_mode, "require") == 0) wg->backend_tls_config.mode = KEEL_TLS_REQUIRE;
                            else                                               wg->backend_tls_config.mode = KEEL_TLS_DISABLE;
                        }

                        /* Backend certificate and key files */
                        const char* backend_cert = keel_config_get_string(config, section, "backend_tls_cert_file", NULL);
                        if (backend_cert) {
                            snprintf(wg->backend_tls_cert_file_buf, sizeof(wg->backend_tls_cert_file_buf), "%s", backend_cert);
                            wg->backend_tls_config.cert_file = wg->backend_tls_cert_file_buf;
                        }

                        const char* backend_key = keel_config_get_string(config, section, "backend_tls_key_file", NULL);
                        if (backend_key) {
                            snprintf(wg->backend_tls_key_file_buf, sizeof(wg->backend_tls_key_file_buf), "%s", backend_key);
                            wg->backend_tls_config.key_file = wg->backend_tls_key_file_buf;
                        }

                        const char* backend_ca = keel_config_get_string(config, section, "backend_tls_ca_file", NULL);
                        if (backend_ca) {
                            snprintf(wg->backend_tls_ca_file_buf, sizeof(wg->backend_tls_ca_file_buf), "%s", backend_ca);
                            wg->backend_tls_config.ca_file = wg->backend_tls_ca_file_buf;
                        }

                        /* Backend peer verification mode — defaults to "require" (safe default).
                         * Explicitly setting "none" with TLS enabled emits a security warning. */
                        const char* backend_verify = keel_config_get_string(
                            config, section, "backend_tls_verify_peer", "require");
                        if (backend_verify) {
                            snprintf(wg->backend_tls_verify_buf, sizeof(wg->backend_tls_verify_buf), "%s", backend_verify);
                            if      (strcmp(backend_verify, "optional") == 0) wg->backend_tls_config.verify_peer = KEEL_TLS_VERIFY_OPTIONAL;
                            else if (strcmp(backend_verify, "require") == 0 || strcmp(backend_verify, "required") == 0)
                                wg->backend_tls_config.verify_peer = KEEL_TLS_VERIFY_REQUIRED;
                            else {
                                wg->backend_tls_config.verify_peer = KEEL_TLS_VERIFY_NONE;
                                if (wg->backend_tls_config.mode != KEEL_TLS_DISABLE) {
                                    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                                        "SECURITY: [%s] backend_tls_verify_peer=none disables backend "
                                        "certificate verification — connections to PostgreSQL backends are "
                                        "vulnerable to MITM attacks. "
                                        "Set backend_tls_verify_peer=require for production deployments.",
                                        section);
                                }
                            }
                        }

                        /* Backend kTLS acceleration */
                        int64_t backend_ktls = keel_config_get_int(config, section, "backend_ktls_enabled", 1);
                        wg->backend_tls_config.ktls_enabled = (backend_ktls != 0);

                        /* Backend cipher policy */
                        const char* be_ciphers = keel_config_get_string(
                            config, section, "backend_tls_ciphers", NULL);
                        if (be_ciphers) {
                            wg->backend_tls_config.ciphers = (char*)be_ciphers;
                        }
                        const char* be_ciphersuites = keel_config_get_string(
                            config, section, "backend_tls_ciphersuites", NULL);
                        if (be_ciphersuites) {
                            wg->backend_tls_config.ciphersuites = (char*)be_ciphersuites;
                        }

                        /* Backend minimum TLS version */
                        const char* be_min_ver = keel_config_get_string(
                            config, section, "backend_tls_min_version", NULL);
                        if (be_min_ver) {
                            if (strcmp(be_min_ver, "1.3") == 0 || strcmp(be_min_ver, "TLSv1.3") == 0)
                                wg->backend_tls_config.min_version = KEEL_TLS_VERSION_1_3;
                            else if (strcmp(be_min_ver, "1.2") == 0 || strcmp(be_min_ver, "TLSv1.2") == 0)
                                wg->backend_tls_config.min_version = KEEL_TLS_VERSION_1_2;
                        }

                        /* Backend TLS timeouts */
                        int64_t backend_hs_timeout = keel_config_get_duration_ms(config, section, "backend_tls_handshake_timeout", 10000);
                        if (backend_hs_timeout > 0) wg->backend_tls_config.handshake_timeout_ms = (size_t)backend_hs_timeout;

                        int64_t backend_read_timeout = keel_config_get_duration_ms(config, section, "backend_tls_read_timeout", 30000);
                        if (backend_read_timeout > 0) wg->backend_tls_config.read_timeout_ms = (size_t)backend_read_timeout;
                    }
                }

                /* -------------------------------------------------------
                 * Auto-generate TLS certificates
                 * ------------------------------------------------------- */
                {
                    int64_t auto_gen = keel_config_get_int(config, section, "tls_auto_generate", 0);
                    wg->tls_auto_generate = (auto_gen != 0);

                    const char* auto_dir = keel_config_get_string(
                        config, section, "tls_auto_dir", "/var/lib/keel/certs");
                    if (auto_dir) {
                        snprintf(wg->tls_auto_dir_buf, sizeof(wg->tls_auto_dir_buf), "%s", auto_dir);
                    }
                }

                /* -------------------------------------------------------
                 * Parse backend servers
                 * ------------------------------------------------------- */
                if (keel_config_has_section(config, wg->servers_section)) {
                    const char* srv_keys[KEEL_MAX_SERVERS];
                    const char* srv_vals[KEEL_MAX_SERVERS];
                    srv_collect_ctx_t srv_ctx = { srv_keys, srv_vals, 0, KEEL_MAX_SERVERS };
                    keel_config_iter_keys(config, wg->servers_section, collect_srv_keys, &srv_ctx);

                    for (size_t i = 0; i < srv_ctx.count && wg->server_pool.count < KEEL_MAX_SERVERS; i++) {
                        const char* server_def = srv_ctx.vals[i];
                        if (!server_def) continue;

                        keel_backend_server_t* server = &wg->server_pool.servers[wg->server_pool.count];
                        char* host_buf = wg->server_bufs[wg->server_pool.count][0];
                        char* user_buf = wg->server_bufs[wg->server_pool.count][1];
                        char* pass_buf = wg->server_bufs[wg->server_pool.count][2];
                        char* db_buf   = wg->server_bufs[wg->server_pool.count][3];
                        char* puser_buf = wg->server_bufs[wg->server_pool.count][4];
                        char* ppass_buf = wg->server_bufs[wg->server_pool.count][5];
                        char* pauth_buf = wg->server_bufs[wg->server_pool.count][6];

                        server->host     = "127.0.0.1";
                        server->port     = 5432;
                        server->user     = "postgres";
                        server->password = NULL;
                        server->database = "postgres";
                        server->probe_user = wg->probe_cfg.probe_user;
                        server->probe_password = wg->probe_cfg.probe_password;
                        server->probe_auth = wg->probe_cfg.probe_auth;
                        server->role     = KEEL_SERVER_ROLE_AUTO;
                        server->weight   = 100;
                        server->healthy  = true;

                        const char* p = server_def;
                        while (*p) {
                            while (*p && isspace((unsigned char)*p)) p++;
                            if (!*p) break;

                            if (strncmp(p, "host=", 5) == 0) {
                                p += 5;
                                char* end = host_buf;
                                while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                                *end = '\0';
                                server->host = host_buf;
                            } else if (strncmp(p, "port=", 5) == 0) {
                                p += 5;
                                server->port = (uint16_t)atoi(p);
                                while (*p && !isspace((unsigned char)*p)) p++;
                            } else if (strncmp(p, "user=", 5) == 0) {
                                p += 5;
                                char* end = user_buf;
                                while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                                *end = '\0';
                                server->user = user_buf;
                            } else if (strncmp(p, "password=", 9) == 0) {
                                p += 9;
                                char* end = pass_buf;
                                while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                                *end = '\0';
                                server->password = pass_buf;
                            } else if (strncmp(p, "probe_user=", 11) == 0) {
                                p += 11;
                                char* end = puser_buf;
                                while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                                *end = '\0';
                                server->probe_user = puser_buf;
                            } else if (strncmp(p, "probe_password=", 15) == 0) {
                                p += 15;
                                char* end = ppass_buf;
                                while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                                *end = '\0';
                                server->probe_password = ppass_buf;
                            } else if (strncmp(p, "probe_auth=", 11) == 0) {
                                p += 11;
                                char* end = pauth_buf;
                                while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                                *end = '\0';
                                server->probe_auth = pauth_buf;
                            } else if (strncmp(p, "dbname=", 7) == 0) {
                                p += 7;
                                char* end = db_buf;
                                while (*p && !isspace((unsigned char)*p)) *end++ = *p++;
                                *end = '\0';
                                server->database = db_buf;
                            } else if (strncmp(p, "role=", 5) == 0) {
                                p += 5;
                                if (strncmp(p, "RW", 2) == 0 || strncmp(p, "rw", 2) == 0 ||
                                    strncmp(p, "primary", 7) == 0) {
                                    server->role = KEEL_SERVER_ROLE_RW;
                                } else if (strncmp(p, "RO", 2) == 0 || strncmp(p, "ro", 2) == 0 ||
                                           strncmp(p, "replica", 7) == 0) {
                                    server->role = KEEL_SERVER_ROLE_RO;
                                } else if (strncmp(p, "WO", 2) == 0 || strncmp(p, "wo", 2) == 0) {
                                    server->role = KEEL_SERVER_ROLE_WO;
                                } else if (strncmp(p, "auto", 4) == 0 || strncmp(p, "AUTO", 4) == 0) {
                                    server->role = KEEL_SERVER_ROLE_AUTO;
                                }
                                while (*p && !isspace((unsigned char)*p)) p++;
                            } else if (strncmp(p, "weight=", 7) == 0) {
                                p += 7;
                                server->weight = (uint32_t)atoi(p);
                                while (*p && !isspace((unsigned char)*p)) p++;
                            } else if (strncmp(p, "shard_id=", 9) == 0) {
                                p += 9;
                                server->shard_id = (uint32_t)atoi(p);
                                if (server->shard_id > 0)
                                    wg->sharding_enabled = true;
                                while (*p && !isspace((unsigned char)*p)) p++;
                            } else {
                                while (*p && !isspace((unsigned char)*p)) p++;
                            }
                        }

                        wg->server_pool.count++;

                        /* Set legacy backend config from first RW server */
                        if (server->role == KEEL_SERVER_ROLE_RW &&
                            wg->backend_host == NULL) {
                            wg->backend_host     = server->host;
                            wg->backend_port     = server->port;
                            wg->backend_user     = server->user;
                            wg->backend_password = server->password;
                            wg->backend_database = server->database;
                        }
                    }

                    /* Build role-index arrays from the parsed servers */
                    keel_server_pool_rebuild_indices(&wg->server_pool);

                    printf("  [%s] Parsed %zu backend servers (RW=%zu, RO=%zu, WO=%zu)\n",
                           wg->name, wg->server_pool.count,
                           wg->server_pool.rw_count,
                           wg->server_pool.ro_count,
                           wg->server_pool.wo_count);
                }

                /* -------------------------------------------------------
                 * Parse probe config
                 * ------------------------------------------------------- */
                const char* probe_str = keel_config_get_string(config, section, "probe", NULL);
                if (probe_str) {
                    const char* colon = strchr(probe_str, ':');
                    if (colon) {
                        size_t tlen = (size_t)(colon - probe_str);
                        if (tlen >= sizeof(wg->probe_type_buf)) tlen = sizeof(wg->probe_type_buf) - 1;
                        memcpy(wg->probe_type_buf, probe_str, tlen);
                        wg->probe_type_buf[tlen] = '\0';
                        strncpy(wg->probe_extra_buf, colon + 1, sizeof(wg->probe_extra_buf) - 1);
                        wg->probe_extra_buf[sizeof(wg->probe_extra_buf) - 1] = '\0';
                        wg->probe_cfg.probe_type  = wg->probe_type_buf;
                        wg->probe_cfg.probe_extra = wg->probe_extra_buf;
                    } else {
                        strncpy(wg->probe_type_buf, probe_str, sizeof(wg->probe_type_buf) - 1);
                        wg->probe_type_buf[sizeof(wg->probe_type_buf) - 1] = '\0';
                        wg->probe_cfg.probe_type = wg->probe_type_buf;
                    }
                }

                const char* v;
                v = keel_config_get_string(config, section, "probe_interval", NULL);
                if (v) {
                    int val = atoi(v);
                    if (val > 0) {
                        if (strchr(v, 's')) val *= 1000;
                        wg->probe_cfg.interval_ms = (uint32_t)val;
                    }
                }

                v = keel_config_get_string(config, section, "probe_timeout", NULL);
                if (v) {
                    int val = atoi(v);
                    if (val > 0) {
                        if (strchr(v, 's')) val *= 1000;
                        wg->probe_cfg.timeout_ms = (uint32_t)val;
                    }
                }

                int64_t retries = keel_config_get_int(config, section, "probe_retries",
                                                     (int64_t)wg->probe_cfg.retries);
                if (retries > 0) wg->probe_cfg.retries = (uint32_t)retries;

                v = keel_config_get_string(config, section, "probe_user", NULL);
                if (v && *v) {
                    strncpy(wg->probe_user_buf, v, sizeof(wg->probe_user_buf) - 1);
                    wg->probe_user_buf[sizeof(wg->probe_user_buf) - 1] = '\0';
                    wg->probe_cfg.probe_user = wg->probe_user_buf;
                }

                v = keel_config_get_string(config, section, "probe_password", NULL);
                if (v && *v) {
                    strncpy(wg->probe_password_buf, v, sizeof(wg->probe_password_buf) - 1);
                    wg->probe_password_buf[sizeof(wg->probe_password_buf) - 1] = '\0';
                    wg->probe_cfg.probe_password = wg->probe_password_buf;
                }

                v = keel_config_get_string(config, section, "probe_auth", NULL);
                if (v && *v) {
                    strncpy(wg->probe_auth_buf, v, sizeof(wg->probe_auth_buf) - 1);
                    wg->probe_auth_buf[sizeof(wg->probe_auth_buf) - 1] = '\0';
                    wg->probe_cfg.probe_auth = wg->probe_auth_buf;
                }

                v = keel_config_get_string(config, section, "failover_delay", NULL);
                if (v) {
                    int val = atoi(v);
                    if (val > 0) {
                        if (strchr(v, 's')) val *= 1000;
                        wg->probe_cfg.failover_delay_ms = (uint32_t)val;
                    }
                }

                /* Apply group-level probe auth defaults to servers that did not
                 * define probe_user/probe_password/probe_auth explicitly. */
                for (size_t si = 0; si < wg->server_pool.count; si++) {
                    keel_backend_server_t* s = &wg->server_pool.servers[si];
                    if (!s->probe_user) s->probe_user = wg->probe_cfg.probe_user;
                    if (!s->probe_password) s->probe_password = wg->probe_cfg.probe_password;
                    if (!s->probe_auth) s->probe_auth = wg->probe_cfg.probe_auth;
                }

                /* -------------------------------------------------------
                 * Parse enterprise authentication configuration
                 * ------------------------------------------------------- */
                v = keel_config_get_string(config, section, "auth_method", NULL);
                if (v && *v) {
                    strncpy(wg->auth_method_buf, v, sizeof(wg->auth_method_buf) - 1);
                    wg->auth_method_buf[sizeof(wg->auth_method_buf) - 1] = '\0';
                    if (strcasecmp(v, "scram-sha-256") == 0 || strcasecmp(v, "scram") == 0)
                        wg->auth_method = KEEL_AUTH_SCRAM_SHA_256;
                    else if (strcasecmp(v, "md5") == 0) {
                        wg->auth_method = KEEL_AUTH_MD5;
                        /* MD5 is deprecated (RFC 6151) and broken for offline attacks.
                         * Always warn; with --strict-auth, abort startup. */
                        if (g_security_cfg.strict_auth) {
                            fprintf(stderr,
                                "FATAL: [%s] auth_method=md5 is rejected by --strict-auth. "
                                "Switch to scram-sha-256 for production deployments.\n",
                                wg->name);
                            return 1;
                        }
                        KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                            "SECURITY: [%s] auth_method=md5 is deprecated (RFC 6151) and vulnerable "
                            "to offline brute-force attacks. Migrate to scram-sha-256. "
                            "Use --strict-auth to reject md5 at startup.",
                            wg->name);
                    }
                    else if (strcasecmp(v, "trust") == 0) {
                        wg->auth_method = KEEL_AUTH_TRUST;
                        /* trust skips all authentication — never safe in production. */
                        if (g_security_cfg.strict_auth) {
                            fprintf(stderr,
                                "FATAL: [%s] auth_method=trust is rejected by --strict-auth. "
                                "Trust authentication accepts any connection without a password.\n",
                                wg->name);
                            return 1;
                        }
                        KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                            "SECURITY: [%s] auth_method=trust accepts all connections without "
                            "authentication. This is only safe on loopback/private networks. "
                            "Use --strict-auth to reject trust at startup.",
                            wg->name);
                    }
                    else if (strcasecmp(v, "password") == 0)
                        wg->auth_method = KEEL_AUTH_PASSWORD;
                    else if (strcasecmp(v, "cert") == 0 || strcasecmp(v, "certificate") == 0)
                        wg->auth_method = KEEL_AUTH_CERTIFICATE;
                    else if (strcasecmp(v, "ldap") == 0)
                        wg->auth_method = KEEL_AUTH_LDAP;
                    else if (strcasecmp(v, "pam") == 0)
                        wg->auth_method = KEEL_AUTH_PAM;
                    else if (strcasecmp(v, "auth_query") == 0 || strcasecmp(v, "authquery") == 0)
                        wg->auth_method = KEEL_AUTH_SCRAM_SHA_256; /* auth_query delegates to SCRAM/MD5 */
                    else {
                        KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                            "[%s] Unknown auth_method '%s', defaulting to scram-sha-256",
                            wg->name, v);
                        wg->auth_method = KEEL_AUTH_SCRAM_SHA_256;
                    }
                    KEEL_LOG_INFO(KEEL_LOG_CAT_AUTH,
                        "[%s] auth_method = %s", wg->name, wg->auth_method_buf);
                }

                /* LDAP config */
                v = keel_config_get_string(config, section, "ldap_url", NULL);
                if (v && *v) {
                    strncpy(wg->auth_ldap_url_buf, v, sizeof(wg->auth_ldap_url_buf) - 1);
                    wg->auth_ldap_url_buf[sizeof(wg->auth_ldap_url_buf) - 1] = '\0';
                }
                v = keel_config_get_string(config, section, "ldap_base_dn", NULL);
                if (v && *v) {
                    strncpy(wg->auth_ldap_base_dn_buf, v, sizeof(wg->auth_ldap_base_dn_buf) - 1);
                    wg->auth_ldap_base_dn_buf[sizeof(wg->auth_ldap_base_dn_buf) - 1] = '\0';
                }
                v = keel_config_get_string(config, section, "ldap_bind_dn", NULL);
                if (v && *v) {
                    strncpy(wg->auth_ldap_bind_dn_buf, v, sizeof(wg->auth_ldap_bind_dn_buf) - 1);
                    wg->auth_ldap_bind_dn_buf[sizeof(wg->auth_ldap_bind_dn_buf) - 1] = '\0';
                }
                v = keel_config_get_string(config, section, "ldap_bind_password", NULL);
                if (v && *v) {
                    strncpy(wg->auth_ldap_bind_password_buf, v,
                            sizeof(wg->auth_ldap_bind_password_buf) - 1);
                    wg->auth_ldap_bind_password_buf[sizeof(wg->auth_ldap_bind_password_buf) - 1] = '\0';
                }
                v = keel_config_get_string(config, section, "ldap_search_filter", NULL);
                if (v && *v) {
                    strncpy(wg->auth_ldap_search_filter_buf, v,
                            sizeof(wg->auth_ldap_search_filter_buf) - 1);
                    wg->auth_ldap_search_filter_buf[sizeof(wg->auth_ldap_search_filter_buf) - 1] = '\0';
                }
                v = keel_config_get_string(config, section, "ldap_dn_suffix", NULL);
                if (v && *v) {
                    strncpy(wg->auth_ldap_dn_suffix_buf, v,
                            sizeof(wg->auth_ldap_dn_suffix_buf) - 1);
                    wg->auth_ldap_dn_suffix_buf[sizeof(wg->auth_ldap_dn_suffix_buf) - 1] = '\0';
                }
                {
                    int64_t ltls = keel_config_get_int(config, section, "ldap_start_tls", 0);
                    wg->auth_ldap_start_tls = (ltls != 0);
                }
                {
                    int64_t lto = keel_config_get_int(config, section, "ldap_timeout", 5);
                    wg->auth_ldap_timeout_s = (int)lto;
                }

                /* PAM config */
                v = keel_config_get_string(config, section, "pam_service_name", NULL);
                if (v && *v) {
                    strncpy(wg->auth_pam_service_buf, v,
                            sizeof(wg->auth_pam_service_buf) - 1);
                    wg->auth_pam_service_buf[sizeof(wg->auth_pam_service_buf) - 1] = '\0';
                }

                /* auth_query config */
                v = keel_config_get_string(config, section, "auth_query", NULL);
                if (v && *v) {
                    strncpy(wg->auth_query_buf, v, sizeof(wg->auth_query_buf) - 1);
                    wg->auth_query_buf[sizeof(wg->auth_query_buf) - 1] = '\0';
                }
                v = keel_config_get_string(config, section, "auth_query_connection", NULL);
                if (v && *v) {
                    strncpy(wg->auth_query_conn_buf, v, sizeof(wg->auth_query_conn_buf) - 1);
                    wg->auth_query_conn_buf[sizeof(wg->auth_query_conn_buf) - 1] = '\0';
                }

                /* userlist file (SCRAM / MD5) */
                v = keel_config_get_string(config, section, "userlist_file", NULL);
                if (v && *v) {
                    strncpy(wg->auth_userlist_file_buf, v,
                            sizeof(wg->auth_userlist_file_buf) - 1);
                    wg->auth_userlist_file_buf[sizeof(wg->auth_userlist_file_buf) - 1] = '\0';
                }

                /* Connection lifecycle */
                v = keel_config_get_string(config, section, "max_connection_age_s", NULL);
                if (v && *v) {
                    double secs = atof(v);
                    if (secs > 0)
                        wg->pool_max_connection_age_ms = (uint64_t)(secs * 1000.0);
                }
                {
                    int64_t mcams = keel_config_get_duration_ms(config, section, "max_connection_age", 0);
                    if (mcams > 0)
                        wg->pool_max_connection_age_ms = (uint64_t)mcams;
                }

                /* -------------------------------------------------------
                 * Parse [worker_group.X.hooks] section
                 * ------------------------------------------------------- */
                {
                    char hooks_section[320];
                    snprintf(hooks_section, sizeof(hooks_section), "%.255s.hooks", section);

                    if (keel_config_has_section(config, hooks_section)) {
                        wg->hooks_enabled = true;
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "[%s] Found hooks section [%s]",
                            wg->name, hooks_section);

                        keel_hook_registry_t* reg = keel_hook_registry_create();
                        if (!reg) {
                            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                "[%s] Failed to create hook registry (out of memory)",
                                wg->name);
                        } else {
                            /* Track whether we need Lua/Python runtimes */
                            bool need_lua = false;
                            bool need_python = false;
                            bool python_inited = false;
                            size_t lua_count = 0;
                            size_t python_count = 0;
                            size_t plugin_count = 0;

                            /* Collect all keys in the hooks section */
                            const char* hk_keys[64];
                            const char* hk_vals[64];
                            srv_collect_ctx_t hk_ctx = { hk_keys, hk_vals, 0, 64 };
                            keel_config_iter_keys(config, hooks_section, collect_srv_keys, &hk_ctx);

                            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                "[%s] Parsing %zu hook entries from [%s]",
                                wg->name, hk_ctx.count, hooks_section);

                            for (size_t hi = 0; hi < hk_ctx.count; hi++) {
                                const char* key = hk_keys[hi];
                                const char* val = hk_vals[hi];
                                if (!key || !val) continue;

                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                    "[%s]   Hook entry: %s = %s",
                                    wg->name, key, val);

                                if (strncmp(key, "hook.plugin.", 12) == 0) {
                                    /* hook.plugin.<name> = <path> */
                                    const char* plugin_name = key + 12;

                                    /* Check file existence */
                                    if (access(val, R_OK) != 0) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Hook plugin file not found or not readable: '%s' (for hook.plugin.%s)",
                                            wg->name, val, plugin_name);
                                        continue;
                                    }

                                    keel_error_t err = keel_hook_load_plugin(reg, val);
                                    if (err != KEEL_OK) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Failed to load hook plugin '%s' from '%s': error %d",
                                            wg->name, plugin_name, val, err);
                                    } else {
                                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                            "[%s] Loaded native plugin '%s' from '%s'",
                                            wg->name, plugin_name, val);
                                        plugin_count++;
                                    }
                                } else if (strncmp(key, "hook.lua.", 9) == 0) {
                                    /* hook.lua.<point>.<name> = script=<path> func=<fn> priority=<n> */
                                    need_lua = true;
                                    const char* rest = key + 9;
                                    const char* dot = strchr(rest, '.');
                                    if (!dot) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Invalid Lua hook key: '%s' (expected hook.lua.<point>.<name>)",
                                            wg->name, key);
                                        continue;
                                    }
                                    char point_str[64];
                                    size_t plen = (size_t)(dot - rest);
                                    if (plen >= sizeof(point_str)) plen = sizeof(point_str) - 1;
                                    memcpy(point_str, rest, plen);
                                    point_str[plen] = '\0';
                                    const char* hook_name = dot + 1;

                                    /* Parse point string to enum */
                                    keel_hook_point_t point = KEEL_HOOK_POINT_COUNT;
                                    if (strcmp(point_str, "after_query_read") == 0)
                                        point = KEEL_HOOK_AFTER_QUERY_READ;
                                    else if (strcmp(point_str, "after_query_parse") == 0)
                                        point = KEEL_HOOK_AFTER_QUERY_PARSE;
                                    else if (strcmp(point_str, "before_route") == 0)
                                        point = KEEL_HOOK_BEFORE_ROUTE;
                                    else if (strcmp(point_str, "before_send") == 0)
                                        point = KEEL_HOOK_BEFORE_SEND;

                                    if (point == KEEL_HOOK_POINT_COUNT) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Unknown hook point '%s' in key '%s'",
                                            wg->name, point_str, key);
                                        continue;
                                    }

                                    /* Parse value: script=<path> func=<fn> priority=<n> */
                                    char lua_script[256] = {0};
                                    char lua_func[128] = {0};
                                    int priority = 100;
                                    const char* p = val;
                                    while (*p) {
                                        while (*p && isspace((unsigned char)*p)) p++;
                                        if (!*p) break;
                                        if (strncmp(p, "script=", 7) == 0) {
                                            p += 7;
                                            char* end = lua_script;
                                            while (*p && !isspace((unsigned char)*p))
                                                *end++ = *p++;
                                            *end = '\0';
                                        } else if (strncmp(p, "func=", 5) == 0) {
                                            p += 5;
                                            char* end = lua_func;
                                            while (*p && !isspace((unsigned char)*p))
                                                *end++ = *p++;
                                            *end = '\0';
                                        } else if (strncmp(p, "priority=", 9) == 0) {
                                            p += 9;
                                            priority = atoi(p);
                                            while (*p && !isspace((unsigned char)*p)) p++;
                                        } else {
                                            while (*p && !isspace((unsigned char)*p)) p++;
                                        }
                                    }

                                    if (!lua_script[0] || !lua_func[0]) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Lua hook '%s': missing script= or func= in value '%s'",
                                            wg->name, key, val);
                                        continue;
                                    }

                                    /* Check Lua script file existence */
                                    if (access(lua_script, R_OK) != 0) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Lua script file not found or not readable: '%s' "
                                            "(for hook.lua.%s.%s)",
                                            wg->name, lua_script, point_str, hook_name);
                                        continue;
                                    }

                                    /* Check Lua runtime availability */
                                    if (!keel_lua_available()) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Lua hook '%s' configured but Lua support is NOT compiled in. "
                                            "Rebuild with -DKEEL_ENABLE_LUA=ON to enable Lua hooks.",
                                            wg->name, hook_name);
                                        continue;
                                    }

                                    keel_hook_handle_t* h = keel_hook_register_lua(
                                        reg, point, hook_name, lua_script, lua_func, priority);
                                    if (h) {
                                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                            "[%s] Registered Lua hook '%s' at %s "
                                            "(script=%s func=%s priority=%d)",
                                            wg->name, hook_name,
                                            keel_hook_point_name(point),
                                            lua_script, lua_func, priority);
                                        lua_count++;
                                    } else {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Failed to register Lua hook '%s' "
                                            "(internal error)",
                                            wg->name, hook_name);
                                    }
                                } else if (strncmp(key, "hook.python.", 12) == 0) {
                                    /* hook.python.<point>.<name> = module=<mod> func=<fn> priority=<n> */
                                    need_python = true;
                                    const char* rest = key + 12;
                                    const char* dot = strchr(rest, '.');
                                    if (!dot) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Invalid Python hook key: '%s' (expected hook.python.<point>.<name>)",
                                            wg->name, key);
                                        continue;
                                    }
                                    char point_str[64];
                                    size_t plen = (size_t)(dot - rest);
                                    if (plen >= sizeof(point_str)) plen = sizeof(point_str) - 1;
                                    memcpy(point_str, rest, plen);
                                    point_str[plen] = '\0';
                                    const char* hook_name = dot + 1;

                                    keel_hook_point_t point = KEEL_HOOK_POINT_COUNT;
                                    if (strcmp(point_str, "after_query_read") == 0)
                                        point = KEEL_HOOK_AFTER_QUERY_READ;
                                    else if (strcmp(point_str, "after_query_parse") == 0)
                                        point = KEEL_HOOK_AFTER_QUERY_PARSE;
                                    else if (strcmp(point_str, "before_route") == 0)
                                        point = KEEL_HOOK_BEFORE_ROUTE;
                                    else if (strcmp(point_str, "before_send") == 0)
                                        point = KEEL_HOOK_BEFORE_SEND;

                                    if (point == KEEL_HOOK_POINT_COUNT) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Unknown hook point '%s' in key '%s'",
                                            wg->name, point_str, key);
                                        continue;
                                    }

                                    char py_module[256] = {0};
                                    char py_script[PATH_MAX] = {0};  /* file path when script= is used */
                                    char py_func[128] = {0};
                                    int priority = 100;
                                    const char* p = val;
                                    while (*p) {
                                        while (*p && isspace((unsigned char)*p)) p++;
                                        if (!*p) break;
                                        if (strncmp(p, "module=", 7) == 0) {
                                            p += 7;
                                            char* end = py_module;
                                            while (*p && !isspace((unsigned char)*p))
                                                *end++ = *p++;
                                            *end = '\0';
                                        } else if (strncmp(p, "script=", 7) == 0) {
                                            p += 7;
                                            char* end = py_script;
                                            while (*p && !isspace((unsigned char)*p))
                                                *end++ = *p++;
                                            *end = '\0';
                                        } else if (strncmp(p, "func=", 5) == 0) {
                                            p += 5;
                                            char* end = py_func;
                                            while (*p && !isspace((unsigned char)*p))
                                                *end++ = *p++;
                                            *end = '\0';
                                        } else if (strncmp(p, "priority=", 9) == 0) {
                                            p += 9;
                                            priority = atoi(p);
                                            while (*p && !isspace((unsigned char)*p)) p++;
                                        } else {
                                            while (*p && !isspace((unsigned char)*p)) p++;
                                        }
                                    }

                                    /* Convert script= file path to module name */
                                    char script_dir[PATH_MAX] = {0};
                                    if (py_script[0] && !py_module[0]) {
                                        /* Resolve relative script paths against the config
                                         * file's directory so the proxy works regardless of CWD. */
                                        if (py_script[0] != '/' && config_dir[0]) {
                                            char resolved[PATH_MAX * 2];
                                            snprintf(resolved, sizeof(resolved),
                                                     "%s/%s", config_dir, py_script);
                                            strncpy(py_script, resolved, sizeof(py_script) - 1);
                                            py_script[sizeof(py_script) - 1] = '\0';
                                        }
                                        /* Extract directory and save for sys.path */
                                        char dir_buf[PATH_MAX];
                                        strncpy(dir_buf, py_script, sizeof(dir_buf) - 1);
                                        dir_buf[sizeof(dir_buf) - 1] = '\0';
                                        char* last_slash = strrchr(dir_buf, '/');
                                        if (last_slash) {
                                            *last_slash = '\0';
                                            strncpy(script_dir, dir_buf, sizeof(script_dir) - 1);
                                            /* Module name = filename without .py */
                                            const char* basename = last_slash + 1;
                                            strncpy(py_module, basename, sizeof(py_module) - 1);
                                        } else {
                                            /* No directory — just strip .py extension */
                                            strncpy(py_module, py_script, sizeof(py_module) - 1);
                                        }
                                        /* Strip .py extension */
                                        size_t mlen = strlen(py_module);
                                        if (mlen > 3 && strcmp(py_module + mlen - 3, ".py") == 0)
                                            py_module[mlen - 3] = '\0';
                                    }

                                    if (!py_module[0] || !py_func[0]) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Python hook '%s': missing script=/module= or func= in value '%s'",
                                            wg->name, key, val);
                                        continue;
                                    }

                                    /* Check Python runtime availability */
                                    if (!keel_python_available()) {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Python hook '%s' configured but Python support is NOT compiled in. "
                                            "Rebuild with -DKEEL_ENABLE_PYTHON=ON to enable Python hooks.",
                                            wg->name, hook_name);
                                        continue;
                                    }

                                    /* Eagerly init Python on first hook so add_script_dir works */
                                    if (!python_inited && keel_python_available()) {
                                        keel_error_t pyerr = keel_python_init();
                                        if (pyerr != KEEL_OK) {
                                            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                                "[%s] Failed to initialize Python interpreter: error %d",
                                                wg->name, pyerr);
                                            continue;
                                        }
                                        python_inited = true;
                                    }

                                    /* Now add script directory to sys.path (after init) */
                                    if (script_dir[0])
                                        keel_python_add_script_dir(script_dir);

                                    keel_hook_handle_t* h = keel_hook_register_python(
                                        reg, point, hook_name, py_module, py_func, priority);
                                    if (h) {
                                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                            "[%s] Registered Python hook '%s' at %s "
                                            "(module=%s func=%s priority=%d)",
                                            wg->name, hook_name,
                                            keel_hook_point_name(point),
                                            py_module, py_func, priority);
                                        python_count++;
                                    } else {
                                        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                            "[%s] Failed to register Python hook '%s' "
                                            "(internal error)",
                                            wg->name, hook_name);
                                    }
                                } else {
                                    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                                        "[%s] Unknown hook key prefix: '%s' "
                                        "(expected hook.plugin.*, hook.lua.*, or hook.python.*)",
                                        wg->name, key);
                                }
                            }

                            /* Initialize Lua runtime if hooks need it */
                            if (need_lua && keel_lua_available() && lua_count > 0) {
                                keel_error_t err = keel_lua_init();
                                if (err != KEEL_OK) {
                                    KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                                        "[%s] Failed to initialize Lua interpreter: error %d",
                                        wg->name, err);
                                } else {
                                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                        "[%s] Lua interpreter initialized for hook execution",
                                        wg->name);
                                }
                            }

                            /* Python init already happened eagerly during hook parsing */
                            if (python_inited) {
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                    "[%s] Python interpreter initialized for hook execution",
                                    wg->name);
                            }

                            wg->hook_registry = reg;

                            /* Print hook summary per hook point */
                            size_t total = lua_count + python_count + plugin_count;
                            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                "[%s] Hook summary: %zu hooks registered "
                                "(%zu native plugin, %zu Lua, %zu Python)",
                                wg->name, total, plugin_count, lua_count, python_count);

                            for (int pt = 0; pt < KEEL_HOOK_POINT_COUNT; pt++) {
                                keel_hook_stats_t st = keel_hook_get_stats(reg, (keel_hook_point_t)pt);
                                if (st.hook_count > 0) {
                                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                        "[%s]   %-20s: %u hook(s)",
                                        wg->name,
                                        keel_hook_point_name((keel_hook_point_t)pt),
                                        st.hook_count);
                                }
                            }
                        }
                    } else {
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "[%s] No hooks section [%s] found (hooks disabled for this group)",
                            wg->name, hooks_section);
                    }
                }
            } /* end for each worker group */

            /* =============================================================
             * Parse [logging] section
             * ============================================================= */
            if (keel_config_has_section(config, "logging")) {
                const char* v;
                
                v = keel_config_get_string(config, "logging", "plugin", NULL);
                if (v) g_log_cfg.plugin_name = v;
                
                v = keel_config_get_string(config, "logging", "plugin_path", NULL);
                if (v) g_log_cfg.plugin_path = v;
                
                v = keel_config_get_string(config, "logging", "log_file", NULL);
                if (v) g_log_cfg.log_file = v;
                
                v = keel_config_get_string(config, "logging", "log_level", NULL);
                if (v) g_log_cfg.log_level_str = v;
                
                v = keel_config_get_string(config, "logging", "query_log_mode", NULL);
                if (v) g_log_cfg.query_log_mode_str = v;
                
                v = keel_config_get_string(config, "logging", "syslog_ident", NULL);
                if (v) g_log_cfg.syslog_ident = v;
                
                v = keel_config_get_string(config, "logging", "syslog_facility", NULL);
                if (v) g_log_cfg.syslog_facility = v;
                
                g_log_cfg.log_timestamps = keel_config_get_bool(
                    config, "logging", "log_timestamps", true);
                g_log_cfg.log_source = keel_config_get_bool(
                    config, "logging", "log_source", true);
                g_log_cfg.log_dest = keel_config_get_bool(
                    config, "logging", "log_dest", true);
                g_log_cfg.log_username = keel_config_get_bool(
                    config, "logging", "log_username", true);
                g_log_cfg.log_database = keel_config_get_bool(
                    config, "logging", "log_database", true);
                g_log_cfg.log_query_tree = keel_config_get_bool(
                    config, "logging", "log_query_tree", false);
                g_log_cfg.use_colors = keel_config_get_bool(
                    config, "logging", "use_colors", true);

                v = keel_config_get_string(config, "logging", "log_format", NULL);
                if (v && strcmp(v, "json") == 0)
                    g_log_cfg.json_format = true;
                
                int64_t mqlen = keel_config_get_int(
                    config, "logging", "max_query_len", 0);
                if (mqlen > 0) g_log_cfg.max_query_len = (size_t)mqlen;
            }
            
            /* =============================================================
             * Parse [stats] section
             * ============================================================= */
            if (keel_config_has_section(config, "stats")) {
                const char *v;
                
                v = keel_config_get_string(config, "stats", "level", NULL);
                if (v) g_config.stats_level_str = v;
                
                int64_t interval = keel_config_get_duration_ms(config, "stats", "log_interval", 0);
                if (interval > 0) g_config.stats_interval_ms = (uint32_t)interval;

                g_config.hotpath_instr_mask =
                    apply_hotpath_instr_mask_from_config(config, g_config.hotpath_instr_mask);
            }

            /* =============================================================
             * Parse [instrument] section — function-level probes
             * ============================================================= */
            g_config.instr_mask =
                apply_instr_mask_from_config(config, g_config.instr_mask);

            /* =============================================================
             * Parse [query_rule.N] sections — declarative routing rules
             * ============================================================= */
            {
                keel_query_rules_t* qr = NULL;
                keel_error_t qr_err = keel_query_rules_load(config, &qr);
                if (qr_err != KEEL_OK) {
                    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                        "Failed to load query rules (err=%d); "
                        "query routing rules disabled", (int)qr_err);
                } else {
                    keel_query_rules_replace(&g_config.query_rules, qr);
                    if (qr && qr->count > 0) {
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "Loaded %zu query rule(s)", qr->count);
                    }
                }
            }

            /* =============================================================
             * Parse [admin] section — PgBouncer-style console
             * ============================================================= */
            if (keel_config_has_section(config, "admin")) {
                const char *v;
                
                v = keel_config_get_string(config, "admin", "enabled", "false");
                g_admin_cfg.admin_enabled = (v && (strcmp(v, "true") == 0 || strcmp(v, "1") == 0 || strcmp(v, "yes") == 0));
                
                v = keel_config_get_string(config, "admin", "listen_addr", NULL);
                if (v) g_admin_cfg.admin_addr = v;
                
                int64_t port = keel_config_get_int(config, "admin", "listen_port", 6433);
                if (port > 0 && port <= 65535) g_admin_cfg.admin_port = (uint16_t)port;
                
                v = keel_config_get_string(config, "admin", "users", NULL);
                if (v) g_admin_cfg.admin_users = v;

                v = keel_config_get_string(config, "admin", "password", NULL);
                if (v) g_admin_cfg.admin_password = v;
            }
            
            /* =============================================================
             * Parse [prometheus] section — /metrics endpoint
             * ============================================================= */
            if (keel_config_has_section(config, "prometheus")) {
                const char *v;
                
                v = keel_config_get_string(config, "prometheus", "enabled", "false");
                g_admin_cfg.prom_enabled = (v && (strcmp(v, "true") == 0 || strcmp(v, "1") == 0 || strcmp(v, "yes") == 0));
                
                v = keel_config_get_string(config, "prometheus", "listen_addr", NULL);
                if (v) g_admin_cfg.prom_addr = v;
                
                int64_t port = keel_config_get_int(config, "prometheus", "port", 9101);
                if (port > 0 && port <= 65535) g_admin_cfg.prom_port = (uint16_t)port;
                
                v = keel_config_get_string(config, "prometheus", "path", NULL);
                if (v) g_admin_cfg.prom_path = v;
            }

            /* =============================================================
             * Parse [observability] section — OTLP metrics exporter
             * ============================================================= */
#ifdef KEEL_HAS_OTLP
            (void)keel_otlp_config_load(config, &g_otlp_cfg, &g_otlp_enabled);
            if (g_otlp_enabled) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "OTLP exporter configured: endpoint=%s interval=%ums "
                    "queue_cap=%u",
                    g_otlp_cfg.http.endpoint_url,
                    g_otlp_cfg.interval_ms,
                    g_otlp_cfg.queue_capacity);
            }
#endif

            /* =============================================================
             * Parse [cluster] section — multi-proxy HA
             * ============================================================= */
            if (keel_config_has_section(config, "cluster")) {
                const char *v;

                v = keel_config_get_string(config, "cluster", "enabled", "false");
                g_cluster_cfg.enabled = (v && (strcmp(v, "true") == 0 ||
                                               strcmp(v, "1") == 0 ||
                                               strcmp(v, "yes") == 0));

                v = keel_config_get_string(config, "cluster", "node_id", NULL);
                if (v) {
                    size_t len = strlen(v);
                    if (len >= KEEL_CLUSTER_MAX_NODE_ID) len = KEEL_CLUSTER_MAX_NODE_ID - 1;
                    memcpy(g_cluster_cfg.node_id, v, len);
                    g_cluster_cfg.node_id[len] = '\0';
                }

                v = keel_config_get_string(config, "cluster", "listen_addr", NULL);
                if (v) {
                    size_t len = strlen(v);
                    if (len >= KEEL_CLUSTER_MAX_ADDR) len = KEEL_CLUSTER_MAX_ADDR - 1;
                    memcpy(g_cluster_cfg.listen_addr, v, len);
                    g_cluster_cfg.listen_addr[len] = '\0';
                }

                int64_t port = keel_config_get_int(config, "cluster", "listen_port", 9100);
                if (port > 0 && port <= 65535) g_cluster_cfg.listen_port = (uint16_t)port;

                int64_t hb_int = keel_config_get_duration_ms(config, "cluster", "heartbeat_interval", 1000);
                if (hb_int > 0) g_cluster_cfg.heartbeat_interval_ms = (uint32_t)hb_int;

                int64_t hb_to = keel_config_get_duration_ms(config, "cluster", "heartbeat_timeout", 5000);
                if (hb_to > 0) g_cluster_cfg.heartbeat_timeout_ms = (uint32_t)hb_to;

                int64_t ft = keel_config_get_int(config, "cluster", "failure_threshold", 3);
                if (ft > 0) g_cluster_cfg.failure_threshold = (uint32_t)ft;

                g_cluster_cfg.auto_sync = keel_config_get_bool(
                    config, "cluster", "auto_sync", true);

                g_cluster_cfg.election_enabled = keel_config_get_bool(
                    config, "cluster", "election_enabled", true);

                v = keel_config_get_string(config, "cluster", "election_state_path", NULL);
                if (v && *v) {
                    size_t len = strlen(v);
                    if (len >= sizeof(g_cluster_cfg.election_state_path))
                        len = sizeof(g_cluster_cfg.election_state_path) - 1;
                    memcpy(g_cluster_cfg.election_state_path, v, len);
                    g_cluster_cfg.election_state_path[len] = '\0';
                }

                v = keel_config_get_string(config, "cluster", "vip", NULL);
                if (v && *v) {
                    size_t len = strlen(v);
                    if (len >= sizeof(g_cluster_cfg.vip))
                        len = sizeof(g_cluster_cfg.vip) - 1;
                    memcpy(g_cluster_cfg.vip, v, len);
                    g_cluster_cfg.vip[len] = '\0';
                }

                v = keel_config_get_string(config, "cluster", "vip_interface", NULL);
                if (v && *v) {
                    size_t len = strlen(v);
                    if (len >= sizeof(g_cluster_cfg.vip_interface))
                        len = sizeof(g_cluster_cfg.vip_interface) - 1;
                    memcpy(g_cluster_cfg.vip_interface, v, len);
                    g_cluster_cfg.vip_interface[len] = '\0';
                }

                /* Cluster wire-protocol compression */
                {
                    const char *codec = keel_config_get_string(
                        config, "cluster", "compress", "none");
                    if (codec && strcasecmp(codec, "zlib") == 0)
                        g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_ZLIB;
                    else if (codec && strcasecmp(codec, "zstd") == 0)
                        g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_ZSTD;
                    else
                        g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_NONE;
                }
                {
                    int64_t thr = keel_config_get_bytes(config, "cluster", "compress_threshold", 256);
                    if (thr > 0 && thr <= KEEL_CLUSTER_MAX_PAYLOAD)
                        g_cluster_cfg.compress_threshold_bytes = (uint32_t)thr;
                }

                /* Parse initial_peers = addr1:port1,addr2:port2,... */
                v = keel_config_get_string(config, "cluster", "initial_peers", NULL);
                if (v && *v) {
                    char buf[2048];
                    size_t vlen = strlen(v);
                    if (vlen >= sizeof(buf)) vlen = sizeof(buf) - 1;
                    memcpy(buf, v, vlen);
                    buf[vlen] = '\0';

                    char *saveptr = NULL;
                    char *tok = strtok_r(buf, ",", &saveptr);
                    while (tok && g_cluster_cfg.initial_peer_count < KEEL_CLUSTER_MAX_PEERS) {
                        /* Trim whitespace */
                        while (*tok == ' ') tok++;
                        char *end = tok + strlen(tok) - 1;
                        while (end > tok && *end == ' ') *end-- = '\0';

                        /* Split host:port */
                        char *colon = strrchr(tok, ':');
                        if (colon && colon != tok) {
                            *colon = '\0';
                            size_t idx = g_cluster_cfg.initial_peer_count;
                            size_t alen = strlen(tok);
                            if (alen >= KEEL_CLUSTER_MAX_ADDR)
                                alen = KEEL_CLUSTER_MAX_ADDR - 1;
                            memcpy(g_cluster_cfg.initial_peers[idx].addr, tok, alen);
                            g_cluster_cfg.initial_peers[idx].addr[alen] = '\0';
                            g_cluster_cfg.initial_peers[idx].port =
                                (uint16_t)atoi(colon + 1);
                            g_cluster_cfg.initial_peer_count++;
                        }
                        tok = strtok_r(NULL, ",", &saveptr);
                    }
                }
            }

            /* =============================================================
             * Parse [security] section — privilege drop + seccomp
             * ============================================================= */
            if (keel_config_has_section(config, "security")) {
                const char *v;

                g_security_cfg.privilege_drop = keel_config_get_bool(
                    config, "security", "privilege_drop", g_security_cfg.privilege_drop);
                g_security_cfg.require_privilege_drop = keel_config_get_bool(
                    config, "security", "require_privilege_drop", g_security_cfg.require_privilege_drop);

                v = keel_config_get_string(config, "security", "run_user", NULL);
                if (v) g_security_cfg.run_user = v;

                v = keel_config_get_string(config, "security", "run_group", NULL);
                if (v) g_security_cfg.run_group = v;

                v = keel_config_get_string(config, "security", "seccomp", NULL);
                if (v) g_security_cfg.seccomp_mode = parse_seccomp_mode(v);

                g_security_cfg.require_seccomp = keel_config_get_bool(
                    config, "security", "require_seccomp", g_security_cfg.require_seccomp);
                g_security_cfg.no_new_privs = keel_config_get_bool(
                    config, "security", "no_new_privs", g_security_cfg.no_new_privs);
            }

            if (keel_config_has_section(config, "tracing")) {
                const char *v;

                g_trace_cfg.enabled = keel_config_get_bool(
                    config, "tracing", "enabled", g_trace_cfg.enabled);

                v = keel_config_get_string(config, "tracing", "endpoint", NULL);
                if (v) {
                    size_t len = strlen(v);
                    if (len >= sizeof(g_trace_cfg.endpoint)) len = sizeof(g_trace_cfg.endpoint) - 1;
                    memcpy(g_trace_cfg.endpoint, v, len);
                    g_trace_cfg.endpoint[len] = '\0';
                }

                v = keel_config_get_string(config, "tracing", "service_name", NULL);
                if (v) {
                    size_t len = strlen(v);
                    if (len >= sizeof(g_trace_cfg.service_name)) len = sizeof(g_trace_cfg.service_name) - 1;
                    memcpy(g_trace_cfg.service_name, v, len);
                    g_trace_cfg.service_name[len] = '\0';
                }

                int64_t sr = keel_config_get_int(config, "tracing", "sample_rate_ppm", g_trace_cfg.sample_rate_ppm);
                if (sr >= 0 && sr <= 1000000) g_trace_cfg.sample_rate_ppm = (uint32_t)sr;

                int64_t bs = keel_config_get_int(config, "tracing", "batch_size", g_trace_cfg.batch_size);
                if (bs > 0) g_trace_cfg.batch_size = (uint32_t)bs;

                int64_t fi = keel_config_get_duration_ms(config, "tracing", "flush_interval", g_trace_cfg.flush_interval_ms);
                if (fi > 0) g_trace_cfg.flush_interval_ms = (uint32_t)fi;

                int64_t rc = keel_config_get_int(config, "tracing", "ring_capacity", g_trace_cfg.ring_capacity);
                if (rc > 0) g_trace_cfg.ring_capacity = (uint32_t)rc;

                int64_t et = keel_config_get_duration_ms(config, "tracing", "export_timeout", g_trace_cfg.export_timeout_ms);
                if (et > 0) g_trace_cfg.export_timeout_ms = (uint32_t)et;

                const char* proto = keel_config_get_string(config, "tracing", "protocol", NULL);
                if (proto) {
                    if (strcmp(proto, "http/protobuf") == 0)
                        g_trace_cfg.protocol = KEEL_OTLP_HTTP_PROTOBUF;
                    else
                        g_trace_cfg.protocol = KEEL_OTLP_HTTP_JSON;
                }
            }
            
            /* Note: Don't free config - strings are referenced */
        }
    }

    /* =========================================================================
     * Apply KEEL_CLUSTER_* environment variable overrides.
     * Environment variables always win over INI file settings, making it
     * easy to configure nodes in Docker/Kubernetes deployments.
     * ========================================================================= */
    {
        const char *ev;

        ev = getenv("KEEL_CLUSTER_ENABLED");
        if (ev)
            g_cluster_cfg.enabled = (strcmp(ev, "true") == 0 ||
                                     strcmp(ev, "1")    == 0 ||
                                     strcmp(ev, "yes")  == 0);

        ev = getenv("KEEL_CLUSTER_NODE_ID");
        if (ev) {
            size_t len = strlen(ev);
            if (len >= KEEL_CLUSTER_MAX_NODE_ID) len = KEEL_CLUSTER_MAX_NODE_ID - 1;
            memcpy(g_cluster_cfg.node_id, ev, len);
            g_cluster_cfg.node_id[len] = '\0';
        }

        ev = getenv("KEEL_CLUSTER_LISTEN_ADDR");
        if (ev) {
            size_t len = strlen(ev);
            if (len >= KEEL_CLUSTER_MAX_ADDR) len = KEEL_CLUSTER_MAX_ADDR - 1;
            memcpy(g_cluster_cfg.listen_addr, ev, len);
            g_cluster_cfg.listen_addr[len] = '\0';
        }

        ev = getenv("KEEL_CLUSTER_LISTEN_PORT");
        if (ev) {
            long p = strtol(ev, NULL, 10);
            if (p > 0 && p <= 65535) g_cluster_cfg.listen_port = (uint16_t)p;
        }

        ev = getenv("KEEL_CLUSTER_HB_INTERVAL");
        if (ev) {
            long v = strtol(ev, NULL, 10);
            if (v > 0) g_cluster_cfg.heartbeat_interval_ms = (uint32_t)v;
        }

        ev = getenv("KEEL_CLUSTER_HB_TIMEOUT");
        if (ev) {
            long v = strtol(ev, NULL, 10);
            if (v > 0) g_cluster_cfg.heartbeat_timeout_ms = (uint32_t)v;
        }

        ev = getenv("KEEL_CLUSTER_FAIL_THRESHOLD");
        if (ev) {
            long v = strtol(ev, NULL, 10);
            if (v > 0) g_cluster_cfg.failure_threshold = (uint32_t)v;
        }

        ev = getenv("KEEL_CLUSTER_AUTO_SYNC");
        if (ev)
            g_cluster_cfg.auto_sync = (strcmp(ev, "true") == 0 ||
                                       strcmp(ev, "1")    == 0 ||
                                       strcmp(ev, "yes")  == 0);

        ev = getenv("KEEL_CLUSTER_ELECTION_ENABLED");
        if (ev)
            g_cluster_cfg.election_enabled = (strcmp(ev, "true") == 0 ||
                                              strcmp(ev, "1")    == 0 ||
                                              strcmp(ev, "yes")  == 0);

        ev = getenv("KEEL_CLUSTER_ELECTION_STATE_PATH");
        if (ev && ev[0] != '\0') {
            size_t len = strlen(ev);
            if (len >= sizeof(g_cluster_cfg.election_state_path))
                len = sizeof(g_cluster_cfg.election_state_path) - 1;
            memcpy(g_cluster_cfg.election_state_path, ev, len);
            g_cluster_cfg.election_state_path[len] = '\0';
        }

        ev = getenv("KEEL_CLUSTER_VIP");
        if (ev && ev[0] != '\0') {
            size_t len = strlen(ev);
            if (len >= sizeof(g_cluster_cfg.vip))
                len = sizeof(g_cluster_cfg.vip) - 1;
            memcpy(g_cluster_cfg.vip, ev, len);
            g_cluster_cfg.vip[len] = '\0';
        }

        ev = getenv("KEEL_CLUSTER_VIP_INTERFACE");
        if (ev && ev[0] != '\0') {
            size_t len = strlen(ev);
            if (len >= sizeof(g_cluster_cfg.vip_interface))
                len = sizeof(g_cluster_cfg.vip_interface) - 1;
            memcpy(g_cluster_cfg.vip_interface, ev, len);
            g_cluster_cfg.vip_interface[len] = '\0';
        }

        ev = getenv("KEEL_CLUSTER_COMPRESS");
        if (ev && ev[0] != '\0') {
            if (strcasecmp(ev, "zlib") == 0)
                g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_ZLIB;
            else if (strcasecmp(ev, "zstd") == 0)
                g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_ZSTD;
            else
                g_cluster_cfg.compress_codec = KEEL_CLUSTER_COMPRESS_NONE;
        }

        /* KEEL_CLUSTER_INITIAL_PEERS: comma-separated "host:port" list.
         * Replaces any peers already parsed from the INI file. */
        ev = getenv("KEEL_CLUSTER_INITIAL_PEERS");
        if (ev && ev[0] != '\0') {
            char peers_buf[4096];
            size_t plen = strlen(ev);
            if (plen >= sizeof(peers_buf)) plen = sizeof(peers_buf) - 1;
            memcpy(peers_buf, ev, plen);
            peers_buf[plen] = '\0';

            g_cluster_cfg.initial_peer_count = 0;
            char *saveptr2 = NULL;
            char *tok = strtok_r(peers_buf, ",", &saveptr2);
            while (tok && g_cluster_cfg.initial_peer_count < KEEL_CLUSTER_MAX_PEERS) {
                while (*tok == ' ') tok++;
                char *colon = strrchr(tok, ':');
                if (colon && colon > tok) {
                    *colon = '\0';
                    size_t idx  = g_cluster_cfg.initial_peer_count;
                    size_t alen = strlen(tok);
                    if (alen >= KEEL_CLUSTER_MAX_ADDR) alen = KEEL_CLUSTER_MAX_ADDR - 1;
                    memcpy(g_cluster_cfg.initial_peers[idx].addr, tok, alen);
                    g_cluster_cfg.initial_peers[idx].addr[alen] = '\0';
                    g_cluster_cfg.initial_peers[idx].port = (uint16_t)atoi(colon + 1);
                    g_cluster_cfg.initial_peer_count++;
                }
                tok = strtok_r(NULL, ",", &saveptr2);
            }
        }
    }

    {
        bool cluster_compression_enabled =
            (g_cluster_cfg.compress_codec != KEEL_CLUSTER_COMPRESS_NONE);
        if (!validate_experimental_feature_gates(cluster_compression_enabled)) {
            KEEL_LOG_FATAL(KEEL_LOG_CAT_CONFIG,
                "Configuration rejected: experimental features are disabled. "
                "Set [keel] experimental_features=true to opt in.");
            cleanup_startup_failure(config, false);
            return 1;
        }
    }

    /* Issue 9: --check-config exits here after successful config validation.
     * No sockets are opened, no threads are started.  Exit 0 = config valid. */
    if (opts.check_config) {
        printf("Configuration OK\n");
        if (config) keel_config_free(config);
        keel_mem_shutdown();
        return 0;
    }

    /* Command line options override config file (global settings only) */
    g_config.log_level = opts.log_level;

    /* If no worker groups were parsed (no config file or no [worker_group.*]
     * sections), create a single implicit group from CLI / g_config defaults. */
    if (g_num_groups == 0) {
        worker_group_t* wg = &g_groups[0];
        worker_group_defaults(wg);
        snprintf(wg->section, sizeof(wg->section), "default");
        wg->name             = "default";
        wg->listen_addr      = g_config.listen_addr;
        wg->listen_port      = g_config.listen_port;
        wg->default_protocol = g_config.default_protocol;
        wg->num_workers      = g_config.num_workers;
        wg->pool_min_size    = g_config.pool_min_size;
        wg->pool_max_size    = g_config.pool_max_size;
        wg->backend_host     = g_config.backend_host;
        wg->backend_port     = g_config.backend_port;
        wg->backend_user     = g_config.backend_user;
        wg->backend_password = g_config.backend_password;
        wg->backend_database = g_config.backend_database;
        g_num_groups = 1;
    }

    /* Apply CLI overrides to the first (or only) group */
    if (opts.listen_addr && strcmp(opts.listen_addr, "0.0.0.0") != 0)
        g_groups[0].listen_addr = opts.listen_addr;
    if (opts.listen_port != 6432)
        g_groups[0].listen_port = opts.listen_port;
    if (opts.backend_host && strcmp(opts.backend_host, "127.0.0.1") != 0)
        g_groups[0].backend_host = opts.backend_host;
    if (opts.backend_port != 5432)
        g_groups[0].backend_port = opts.backend_port;
    if (opts.num_workers > 0)
        g_groups[0].num_workers = opts.num_workers;

    /* Print startup banner */
    printf("\n");
    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║   KEEL - High-Performance Database Proxy  v%d.%d.%d              ║\n",
           KEEL_VERSION_MAJOR, KEEL_VERSION_MINOR, KEEL_VERSION_PATCH);
    printf("╚═══════════════════════════════════════════════════════════════╝\n");
    printf("\n");
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &g_groups[gi];
        char enabled_features[512];
        bool cluster_compression_enabled =
            (g_cluster_cfg.compress_codec != KEEL_CLUSTER_COMPRESS_NONE);
        build_runtime_feature_list(wg, cluster_compression_enabled,
                                   enabled_features, sizeof(enabled_features));
        printf("  [%s]\n", wg->name);
        printf("    Listen:   %s:%d\n", wg->listen_addr, wg->listen_port);
        printf("    Backend:  %s:%d\n", wg->backend_host, wg->backend_port);
        printf("    Workers:  %s\n", wg->num_workers == 0 ? "auto (one per CPU)" : "specified");
        printf("    Pool:     %zu min / %zu max (per server)\n", wg->pool_min_size, wg->pool_max_size);
        printf("    Protocol: %s\n", wg->default_protocol);
        printf("    Runtime tier: %s. Enabled features: [%s]\n",
               keel_tier_name(wg->runtime_mode), enabled_features);
        if (KEEL_TIER_IS_EXPERIMENTAL(wg->runtime_mode)) {
            printf("    WARNING: mode=full enables hardening/experimental subsystems "
                   "(hooks, transaction tracking, LSN capture) and is not the "
                   "recommended production default for v0.2-alpha.\n");
        }
    }
    printf("  Stats:    level=%s", g_config.stats_level_str);
    if (g_config.stats_interval_ms > 0)
        printf("  interval=%ums", g_config.stats_interval_ms);
    printf("\n");
    printf("  HotPath:  pool=%s backend=%s query_split=%s deferred_send=%s\n",
           (g_config.hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_POOL) ? "on" : "off",
           (g_config.hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_BACKEND) ? "on" : "off",
           (g_config.hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT) ? "on" : "off",
           (g_config.hotpath_instr_mask & KEEL_HOT_INSTR_DEFERRED_SEND) ? "on" : "off");
        printf("  Security: priv_drop=%s user=%s group=%s seccomp=%s\n",
            g_security_cfg.privilege_drop ? "on" : "off",
            g_security_cfg.run_user ? g_security_cfg.run_user : "(none)",
            g_security_cfg.run_group ? g_security_cfg.run_group : "(none)",
            g_security_cfg.seccomp_mode == KEEL_SECCOMP_STRICT ? "strict" :
            g_security_cfg.seccomp_mode == KEEL_SECCOMP_BASELINE ? "baseline" : "off");
    printf("\n");
    
    /* ================================================================
     * Initialise the log plugin and query logger
     * ================================================================ */
    {
        /* Choose plugin: explicit .so path > named built-in */
        if (g_log_cfg.plugin_path && g_log_cfg.plugin_path[0]) {
            g_log_plugin = keel_log_plugin_load(g_log_cfg.plugin_path);
            if (!g_log_plugin) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG, "Failed to load log plugin from %s, "
                                "falling back to stdout", g_log_cfg.plugin_path);
            }
        }
        
        if (!g_log_plugin) {
            const char* pname = g_log_cfg.plugin_name ? g_log_cfg.plugin_name : "stdout";
            if (strcmp(pname, "file") == 0) {
                g_log_plugin = keel_log_plugin_file_create();
            } else if (strcmp(pname, "syslog") == 0) {
                g_log_plugin = keel_log_plugin_syslog_create();
            } else {
                g_log_plugin = keel_log_plugin_stdout_create();
            }
        }
        
        if (g_log_plugin) {
            /* Build plugin config from g_log_cfg */
            keel_log_plugin_opt_t opts[8];
            size_t nopts = 0;
            
            if (g_log_cfg.log_file) {
                opts[nopts++] = (keel_log_plugin_opt_t){ "log_file", g_log_cfg.log_file };
            }
            if (g_log_cfg.syslog_facility) {
                opts[nopts++] = (keel_log_plugin_opt_t){ "syslog_facility", g_log_cfg.syslog_facility };
            }
            opts[nopts++] = (keel_log_plugin_opt_t){
                "use_colors", g_log_cfg.use_colors ? "true" : "false"
            };
            opts[nopts++] = (keel_log_plugin_opt_t){
                "json_format", g_log_cfg.json_format ? "true" : "false"
            };
            
            keel_log_plugin_config_t pcfg = {
                .opts      = opts,
                .nopts     = nopts,
                .file_path = g_log_cfg.log_file,
                .ident     = g_log_cfg.syslog_ident,
            };
            
            keel_error_t perr = g_log_plugin->open(g_log_plugin, &pcfg);
            if (perr != KEEL_OK) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG, "Log plugin open() failed (%d), "
                                "falling back to stderr", perr);
                g_log_plugin->destroy(g_log_plugin);
                g_log_plugin = keel_log_plugin_stdout_create();
                if (g_log_plugin) {
                    g_log_plugin->open(g_log_plugin, NULL);
                }
            }
        }
        
        /* Set up query logger */
        keel_query_log_config_t qlcfg = keel_query_log_config_default();
        qlcfg.mode           = keel_query_log_mode_from_string(g_log_cfg.query_log_mode_str);
        qlcfg.min_level      = keel_log_level_from_string(g_log_cfg.log_level_str);
        qlcfg.log_timestamps = g_log_cfg.log_timestamps;
        qlcfg.log_source     = g_log_cfg.log_source;
        qlcfg.log_dest       = g_log_cfg.log_dest;
        qlcfg.log_username   = g_log_cfg.log_username;
        qlcfg.log_database   = g_log_cfg.log_database;
        qlcfg.log_query_tree = g_log_cfg.log_query_tree;
        qlcfg.max_query_len  = g_log_cfg.max_query_len;
        
        keel_query_log_init(&g_query_log, &qlcfg, g_log_plugin);
        
        /* Register as the global query logger for engine/worker code */
        keel_query_log_set_global(&g_query_log);
        
        /* Also update the core log level from the logging section */
        keel_log_set_level(keel_log_level_from_string(g_log_cfg.log_level_str));

        /* Enable JSON (NDJSON) log format if configured */
        if (g_log_cfg.json_format)
            keel_log_set_json_format(true);
        
        printf("  Logging:\n");
        printf("    Plugin:     %s\n", g_log_plugin ? g_log_plugin->name : "(none)");
        printf("    Level:      %s\n", g_log_cfg.log_level_str);
        printf("    Query log:  %s\n", keel_query_log_mode_name(qlcfg.mode));
        if (g_log_cfg.log_query_tree) {
            printf("    Query tree: enabled\n");
        }
        if (g_log_cfg.log_file) {
            printf("    Log file:   %s\n", g_log_cfg.log_file);
        }
        if (g_log_cfg.plugin_path) {
            printf("    Plugin .so: %s\n", g_log_cfg.plugin_path);
        }
        printf("\n");
    }
    
    /* Daemonize if requested */
    if (opts.daemonize) {
        printf("Daemonizing...\n");
        if (daemonize_process() < 0) {
            KEEL_LOG_FATAL(KEEL_LOG_CAT_CORE, "Failed to daemonize");
            cleanup_startup_failure(config, false);
            return 1;
        }
    }
    
    /* Set up signal handlers */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGUSR1, stats_signal_handler);
    signal(SIGPIPE, SIG_IGN);
    signal(SIGSEGV, crash_handler);
    signal(SIGABRT, crash_handler);
    signal(SIGBUS, crash_handler);
    
    /* ====================================================================
     * Phase 1: Create listen sockets and engines (no worker threads yet).
     * All log output in this phase comes from the main thread so it
     * cannot interleave with the banner.
     * ==================================================================== */
    /* Initialize TLS subsystem (OpenSSL + kTLS detection) */
    bool tls_initialized = false;
    if (keel_tls_init() != KEEL_OK) {
        KEEL_LOG_FATAL(KEEL_LOG_CAT_TLS, "Failed to initialize TLS subsystem");
        cleanup_startup_failure(config, false);
        return 1;
    }
    tls_initialized = true;
    KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "TLS subsystem initialized");

    uint32_t sys_instr_mask = build_system_instr_mask_from_env();

    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &g_groups[gi];

        wg->listen_fd = create_listen_socket(wg->listen_addr, wg->listen_port,
                                              wg->listen_backlog);
        if (wg->listen_fd < 0) {
            KEEL_LOG_FATAL(KEEL_LOG_CAT_CORE, "[%s] Failed to create listen socket on %s:%d",
                          wg->name, wg->listen_addr, wg->listen_port);
            cleanup_startup_failure(config, tls_initialized);
            return 1;
        }
        printf("  [%s] Socket: fd=%d (listening on %s:%d)\n",
               wg->name, wg->listen_fd, wg->listen_addr, wg->listen_port);
        printf("  [%s] Mode:   %s\n", wg->name, keel_tier_name(wg->runtime_mode));
        {
            char enabled_features[512];
            bool cluster_compression_enabled =
                (g_cluster_cfg.compress_codec != KEEL_CLUSTER_COMPRESS_NONE);
            build_runtime_feature_list(wg, cluster_compression_enabled,
                                       enabled_features, sizeof(enabled_features));
            KEEL_LOG_INFO(KEEL_LOG_CAT_CONFIG,
                "[%s] Runtime tier: %s. Enabled features: [%s]",
                wg->name, keel_tier_name(wg->runtime_mode), enabled_features);
        }

        /* Build per-group engine config */
        keel_engine_config_t engine_cfg = KEEL_ENGINE_CONFIG_DEFAULT;
        engine_cfg.num_workers       = wg->num_workers;
        engine_cfg.pin_workers       = wg->pin_workers;
        engine_cfg.session_pool_size = wg->session_pool_size;
        engine_cfg.slab_buffer_size  = wg->buffer_size;
        /* Distribute the global max_clients cap evenly across workers */
        if (wg->max_clients > 0) {
            uint32_t nw = wg->num_workers > 0 ? wg->num_workers : 1;
            engine_cfg.max_clients_per_worker = (wg->max_clients + nw - 1) / nw;
        }
        engine_cfg.idle_timeout_ms   = (uint32_t)wg->idle_timeout_ms;
        engine_cfg.connect_timeout_ms = (uint32_t)wg->connect_timeout_ms;
        engine_cfg.pool_idle_timeout_ms = wg->idle_timeout_ms;
        engine_cfg.pool_prune_interval_ms  = wg->pool_prune_interval_ms;
        engine_cfg.pool_refill_interval_ms = wg->pool_refill_interval_ms;
        engine_cfg.pool_refill_backoff_ms  = wg->pool_refill_backoff_ms;
        engine_cfg.pool_max_waiting        = wg->pool_max_waiting;
        engine_cfg.default_protocol  = wg->default_protocol;
        engine_cfg.pool_min_size     = wg->pool_min_size;
        engine_cfg.pool_max_size     = wg->pool_max_size;
        engine_cfg.stats_level       = (int)keel_stats_level_from_str(g_config.stats_level_str);
        engine_cfg.stats_interval_ms = g_config.stats_interval_ms;
        engine_cfg.hotpath_instr_mask = g_config.hotpath_instr_mask;
        engine_cfg.instr_mask         = g_config.instr_mask;
        engine_cfg.backend_host      = wg->backend_host;
        engine_cfg.backend_port      = wg->backend_port;
        engine_cfg.backend_user      = wg->backend_user;
        engine_cfg.backend_password  = wg->backend_password;
        engine_cfg.backend_database  = wg->backend_database;
        engine_cfg.server_pool       = wg->server_pool;
        engine_cfg.hook_registry     = wg->hook_registry;

        /* Register declarative query rules as a BEFORE_ROUTE hook if any
         * rules were loaded from [query_rule.N] config sections. */
        if (g_config.query_rules && g_config.query_rules->count > 0) {
            if (!engine_cfg.hook_registry)
                engine_cfg.hook_registry = keel_hook_registry_create();
            if (engine_cfg.hook_registry) {
                keel_hook_register(engine_cfg.hook_registry,
                                   KEEL_HOOK_BEFORE_ROUTE,
                                   "query_rules",
                                   keel_query_rules_hook,
                                   /* priority */ 10,
                                   g_config.query_rules);
                wg->hook_registry = engine_cfg.hook_registry;
            }
        }

        /* Register throttle rules as a BEFORE_ROUTE hook (H3).
         * Throttle rules apply per-client token-bucket rate limiting before
         * the query is routed to the backend. */
        {
            keel_throttle_rules_t* tr = NULL;
            if (keel_throttle_rules_load(config, &tr) == KEEL_OK && tr) {
                if (!engine_cfg.hook_registry)
                    engine_cfg.hook_registry = keel_hook_registry_create();
                if (engine_cfg.hook_registry) {
                    keel_throttle_rules_register_hook(engine_cfg.hook_registry, tr);
                    wg->hook_registry    = engine_cfg.hook_registry;
                }
                wg->throttle_rules = tr;
            }
        }

        /* Start background topology discovery per worker group (H1).
         * Spawns a background thread that polls Patroni/Consul/etcd for
         * server role changes and updates the shard router accordingly. */
        if (wg->router) {
            keel_discovery_config_t disc_cfg = keel_discovery_config_default();
            keel_discovery_t* disc = keel_discovery_create(&disc_cfg);
            if (disc) {
                if (keel_discovery_start(disc, wg->router) == KEEL_OK) {
                    wg->discovery = disc;
                    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                        "worker group %zu: topology discovery started", gi);
                } else {
                    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                        "worker group %zu: topology discovery start failed", gi);
                    keel_discovery_destroy(disc);
                }
            }
        }
        engine_cfg.use_buf_rings     = wg->use_buf_rings;
        engine_cfg.buf_ring_size     = wg->buf_ring_size;
        engine_cfg.sqpoll            = wg->sqpoll;
        engine_cfg.sqpoll_idle_ms    = wg->sqpoll_idle_ms;
        engine_cfg.ps_mode           = wg->ps_mode;
        engine_cfg.runtime_mode      = wg->runtime_mode;
        engine_cfg.txn_tracking      = wg->txn_tracking;
        engine_cfg.fast_network_path = wg->fast_network_path;
        engine_cfg.result_cache      = wg->result_cache;
        engine_cfg.sticky_primary_ttl_ms = wg->sticky_primary_ttl_ms;
        engine_cfg.scatter_merge_max_mem_bytes = wg->scatter_merge_max_mem_bytes;
        engine_cfg.scatter_merge_spill_dir =
            wg->scatter_merge_spill_dir_buf[0] != '\0'
            ? wg->scatter_merge_spill_dir_buf : NULL;
        engine_cfg.rebalance_enabled       = wg->rebalance_enabled;
        engine_cfg.rebalance_interval_ms   = wg->rebalance_interval_ms;
        engine_cfg.rebalance_threshold_pct = wg->rebalance_threshold_pct;
        engine_cfg.rebalance_max_per_tick  = wg->rebalance_max_per_tick;
        engine_cfg.tls_config        = wg->tls_config;
        engine_cfg.backend_tls_config = wg->backend_tls_config;
        engine_cfg.trace_config      = g_trace_cfg;

        /* Enterprise auth wiring */
        engine_cfg.auth_method              = wg->auth_method;
        engine_cfg.auth_ldap_url            = wg->auth_ldap_url_buf[0]    ? wg->auth_ldap_url_buf    : NULL;
        engine_cfg.auth_ldap_base_dn        = wg->auth_ldap_base_dn_buf[0]? wg->auth_ldap_base_dn_buf: NULL;
        engine_cfg.auth_ldap_bind_dn        = wg->auth_ldap_bind_dn_buf[0]? wg->auth_ldap_bind_dn_buf: NULL;
        engine_cfg.auth_ldap_bind_password  = wg->auth_ldap_bind_password_buf[0]
                                              ? wg->auth_ldap_bind_password_buf : NULL;
        engine_cfg.auth_ldap_search_filter  = wg->auth_ldap_search_filter_buf[0]
                                              ? wg->auth_ldap_search_filter_buf : NULL;
        engine_cfg.auth_ldap_dn_suffix      = wg->auth_ldap_dn_suffix_buf[0]
                                              ? wg->auth_ldap_dn_suffix_buf : NULL;
        engine_cfg.auth_ldap_start_tls      = wg->auth_ldap_start_tls;
        engine_cfg.auth_ldap_timeout_s      = wg->auth_ldap_timeout_s;
        engine_cfg.auth_pam_service_name    = wg->auth_pam_service_buf[0]
                                              ? wg->auth_pam_service_buf : NULL;
        engine_cfg.auth_query               = wg->auth_query_buf[0]      ? wg->auth_query_buf      : NULL;
        engine_cfg.auth_query_conn_string   = wg->auth_query_conn_buf[0] ? wg->auth_query_conn_buf : NULL;
        engine_cfg.auth_userlist_file       = wg->auth_userlist_file_buf[0]
                                              ? wg->auth_userlist_file_buf : NULL;

        /* Connection lifecycle */
        engine_cfg.pool_max_connection_age_ms = wg->pool_max_connection_age_ms;

        /* Auto-generate TLS certificates if requested */
        if (wg->tls_auto_generate) {
            keel_tls_auto_config_t auto_cfg = {
                .output_dir    = wg->tls_auto_dir_buf,
                .key_bits      = 2048,
                .validity_days = 365,
                .cn            = "Keel CA",
                .server_cn     = "keel-server",
                .server_san    = "localhost,127.0.0.1,::1",
            };
            if (keel_tls_auto_generate(&auto_cfg, &wg->tls_auto_result) != 0) {
                KEEL_LOG_FATAL(KEEL_LOG_CAT_TLS,
                    "[%s] Failed to auto-generate TLS certificates in %s",
                    wg->name, wg->tls_auto_dir_buf);
                cleanup_startup_failure(config, tls_initialized);
                return 1;
            }
            KEEL_LOG_INFO(KEEL_LOG_CAT_TLS,
                "[%s] Auto-generated TLS certificates in %s",
                wg->name, wg->tls_auto_dir_buf);

            /* Fill cert paths if not explicitly configured */
            if (!engine_cfg.tls_config.cert_file) {
                engine_cfg.tls_config.cert_file = wg->tls_auto_result.server_cert;
            }
            if (!engine_cfg.tls_config.key_file) {
                engine_cfg.tls_config.key_file = wg->tls_auto_result.server_key;
            }
            if (!engine_cfg.tls_config.ca_file) {
                engine_cfg.tls_config.ca_file = wg->tls_auto_result.ca_cert;
            }

            /* Upgrade TLS mode from DISABLE to PREFER when auto-generating */
            if (engine_cfg.tls_config.mode == KEEL_TLS_DISABLE) {
                engine_cfg.tls_config.mode = KEEL_TLS_PREFER;
                KEEL_LOG_INFO(KEEL_LOG_CAT_TLS,
                    "[%s] TLS mode upgraded to 'prefer' (auto-generated certs)",
                    wg->name);
            }
        }

        /* Create shard router when the server pool has sharded servers.
         * We detect sharding by checking whether any server has a non-zero
         * shard_id or whether there are multiple servers with shard_ids. */
        {
            bool has_shard_ids = false;
            for (size_t si = 0; si < wg->server_pool.count; si++) {
                if (wg->server_pool.servers[si].shard_id != 0) {
                    has_shard_ids = true;
                    break;
                }
            }
            /* Also treat multi-server pools where shard_id=0 is repeated as sharded */
            if (!has_shard_ids && wg->server_pool.count >= 2) {
                /* Check if shard_ids are explicitly set (all zero = no sharding configured) */
                /* We rely on has_shard_ids check above */
            }
            if (has_shard_ids) {
                keel_router_config_t rcfg = keel_router_config_default();
                /* Wire the per-worker-group `scatter_merge` INI flag into the
                 * router's fail-closed gate. Default remains `off`: scatter
                 * dispatches are rejected with KEEL_ERR_NOT_SUPPORTED until
                 * the operator explicitly opts in. */
                rcfg.scatter_merge_enabled = wg->scatter_merge_enabled;
                wg->router = keel_router_create(&rcfg);
                if (wg->router) {
                    /* Register each server in the pool with the router */
                    for (size_t si = 0; si < wg->server_pool.count; si++) {
                        const keel_backend_server_t* srv = &wg->server_pool.servers[si];
                        char srv_name[64];
                        snprintf(srv_name, sizeof(srv_name), "shard%u", srv->shard_id);
                        keel_route_server_t rs = {
                            .name     = srv_name,
                            .host     = srv->host,
                            .port     = srv->port,
                            .role     = srv->role,
                            .weight   = (int)(srv->weight > 0 ? srv->weight : 100),
                            .shard_id = (size_t)srv->shard_id,
                            .health   = KEEL_HEALTH_UP,
                        };
                        keel_router_add_server(wg->router, &rs);
                    }
                    /* Load shard rules from config into this router */
                    keel_reload_result_t rr = {0};
                    if (config) {
                        keel_config_reload_shard_rules(config, wg->name, wg->router, &rr);
                        if (rr.applied > 0) {
                            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                "[%s] Loaded %d shard rule(s) into router",
                                wg->name, rr.applied);
                        }
                    }
                    engine_cfg.router = wg->router;

                    /* Create the router plugin manager wrapping this router.
                     * This wires the full plugin-routing API (per-database plugin
                     * dispatch, metadata cache management, topology discovery). */
                    keel_router_mgr_config_t mgr_cfg = keel_router_mgr_config_default();
                    wg->router_mgr = keel_router_mgr_create(&mgr_cfg, wg->router);
                    if (!wg->router_mgr) {
                        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                            "[%s] router plugin manager creation failed — "
                            "plugin routing unavailable", wg->name);
                    } else {
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "[%s] router plugin manager created", wg->name);
                    }
                }
            }
        }

        wg->engine = keel_engine_create(&engine_cfg);
        if (!wg->engine) {
            KEEL_LOG_FATAL(KEEL_LOG_CAT_CORE, "[%s] Failed to create engine", wg->name);
            cleanup_startup_failure(config, tls_initialized);
            return 1;
        }

        /* Set drain timeout for graceful shutdown */
        keel_engine_set_drain_timeout(wg->engine, g_config.shutdown_timeout_ms);

        {
            keel_stats_collector_t *sc = keel_engine_get_stats_collector(wg->engine);
            if (sc) {
                uint32_t mask = sys_instr_mask;
                keel_stats_set_system_probe_mask(sc, mask);
                KEEL_LOG_INFO(KEEL_LOG_CAT_STATS,
                    "[%s] System instrumentation mask: CPU=%s MEM=%s FD=%s DISK=%s NET=%s",
                    wg->name,
                    (mask & KEEL_STAT_SYS_CPU) ? "on" : "off",
                    (mask & KEEL_STAT_SYS_MEMORY) ? "on" : "off",
                    (mask & KEEL_STAT_SYS_FD) ? "on" : "off",
                    (mask & KEEL_STAT_SYS_DISK) ? "on" : "off",
                    (mask & KEEL_STAT_SYS_NETWORK) ? "on" : "off");
                KEEL_LOG_INFO(KEEL_LOG_CAT_STATS,
                    "[%s] Hot-path instrumentation: WAIT_POOL=%s WAIT_BACKEND=%s QUERY_SPLIT=%s DEFERRED_SEND=%s",
                    wg->name,
                    (engine_cfg.hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_POOL) ? "on" : "off",
                    (engine_cfg.hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_BACKEND) ? "on" : "off",
                    (engine_cfg.hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT) ? "on" : "off",
                    (engine_cfg.hotpath_instr_mask & KEEL_HOT_INSTR_DEFERRED_SEND) ? "on" : "off");
            }

            if (engine_cfg.instr_mask) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_STATS,
                    "[%s] Function probes: ENGINE=%s POOL=%s PROTO=%s IO=%s HOOK=%s ROUTE=%s PS=%s STATE=%s",
                    wg->name,
                    (engine_cfg.instr_mask & KEEL_INSTR_CAT_ENGINE) ? "on" : "off",
                    (engine_cfg.instr_mask & KEEL_INSTR_CAT_POOL)   ? "on" : "off",
                    (engine_cfg.instr_mask & KEEL_INSTR_CAT_PROTO)  ? "on" : "off",
                    (engine_cfg.instr_mask & KEEL_INSTR_CAT_IO)     ? "on" : "off",
                    (engine_cfg.instr_mask & KEEL_INSTR_CAT_HOOK)   ? "on" : "off",
                    (engine_cfg.instr_mask & KEEL_INSTR_CAT_ROUTE)  ? "on" : "off",
                    (engine_cfg.instr_mask & KEEL_INSTR_CAT_PS)     ? "on" : "off",
                    (engine_cfg.instr_mask & KEEL_INSTR_CAT_STATE)  ? "on" : "off");
            }
        }

        if (gi == 0) {
            keel_reactor_type_t rt = keel_engine_get_reactor_type(wg->engine);
            printf("  Reactor:  %s\n",
                   rt == KEEL_REACTOR_IOURING ? "io_uring" :
                   rt == KEEL_REACTOR_KQUEUE  ? "kqueue"   :
                   rt == KEEL_REACTOR_EPOLL   ? "epoll"    : "unknown");
        }
    }

    /* Keep g_engine pointing at the first engine for stats / admin */
    g_engine = g_groups[0].engine;

    /* Create tracer (shared across all worker groups) */
    if (g_trace_cfg.enabled) {
        uint32_t total_workers = 0;
        for (size_t gi = 0; gi < g_num_groups; gi++)
            total_workers += keel_engine_get_num_workers(g_groups[gi].engine);
        g_tracer = keel_tracer_create(&g_trace_cfg, total_workers);
        if (g_tracer) {
            for (size_t gi = 0; gi < g_num_groups; gi++)
                keel_engine_set_tracer(g_groups[gi].engine, g_tracer);
            KEEL_LOG_INFO(KEEL_LOG_CAT_TRACE,
                "Tracing enabled: endpoint=%s sample_rate=%u ppm workers=%u",
                g_trace_cfg.endpoint, g_trace_cfg.sample_rate_ppm, total_workers);
        } else {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TRACE, "Failed to create tracer");
        }
    }

    /* Initialise audit log from [audit] config section and attach to all engines */
    if (keel_audit_log_init_from_config(&g_audit_log, config) == 0) {
        g_audit_log_initialized = true;
        if (g_audit_log.enabled) {
            for (size_t gi = 0; gi < g_num_groups; gi++)
                keel_engine_set_audit_log(g_groups[gi].engine,
                                         (struct keel_audit_log*)&g_audit_log);
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "Audit log enabled: path=%s format=%s",
                g_audit_log.config.path,
                g_audit_log.config.format == KEEL_AUDIT_FORMAT_NDJSON
                    ? "ndjson" : "text");
        }
    } else {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Failed to initialise audit log");
    }

    /* ====================================================================
     * Print the complete "Ready" banner, admin/prometheus info, probe
     * config, and test instructions BEFORE worker threads start.
     * ==================================================================== */
    printf("\n");
    printf("╔═══════════════════════════════════════════════════════════════╗\n");
    printf("║   Ready to accept connections                                  ║\n");
    printf("╚═══════════════════════════════════════════════════════════════╝\n");
    printf("\n");

    /* Protocol-aware test instructions per group */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &g_groups[gi];
        bool is_mysql = (strcmp(wg->default_protocol, "mysql") == 0);
        if (is_mysql) {
            printf("  Test with: mysql -h 127.0.0.1 -P %d -u <user> -p <database>\n",
                   wg->listen_port);
        } else {
            printf("  Test with: psql -h 127.0.0.1 -p %d -U <user> -d <database>\n",
                   wg->listen_port);
        }
    }
    printf("  Dump stats: kill -USR1 <pid>\n");

    /* Start admin console + Prometheus metrics (uses first engine) */
    if (g_admin_cfg.admin_enabled || g_admin_cfg.prom_enabled) {
        g_admin = keel_admin_start(&g_admin_cfg, g_engine);
        if (g_admin) {
            if (g_admin_cfg.admin_enabled)
                printf("  Admin:    psql -h %s -p %u -U admin -c 'SHOW STATS'\n",
                       g_admin_cfg.admin_addr, g_admin_cfg.admin_port);
            if (g_admin_cfg.prom_enabled)
                printf("  Metrics:  curl http://%s:%u/metrics\n",
                       g_admin_cfg.prom_addr, g_admin_cfg.prom_port);
            /* Wire query rules for SHOW QUERY RULES introspection */
            if (g_config.query_rules)
                keel_admin_set_query_rules(g_admin, g_config.query_rules);
            /* Wire throttle rules and discovery for SHOW THROTTLE RULES / SHOW TOPOLOGY */
            if (g_admin) {
                for (size_t gi = 0; gi < g_num_groups; gi++) {
                    if (g_groups[gi].throttle_rules)
                        keel_admin_set_throttle_rules(g_admin,
                                                      g_groups[gi].throttle_rules);
                    if (g_groups[gi].discovery)
                        keel_admin_set_discovery(g_admin,
                                                 g_groups[gi].discovery);
                }
                /* Wire router for SHOW SHARDS / Prometheus router metrics
                 * (uses first group with a router attached). */
                for (size_t gi = 0; gi < g_num_groups; gi++) {
                    if (g_groups[gi].router) {
                        keel_admin_set_router(g_admin, g_groups[gi].router);
                        break;
                    }
                }
            }
        }
    }

#ifdef KEEL_HAS_OTLP
    /* Start OTLP exporter + aggregator (cold-path stats fan-out to OTel collector) */
    if (g_otlp_enabled && g_engine) {
        g_otlp_exporter = keel_otlp_exporter_create(&g_otlp_cfg);
        if (g_otlp_exporter && keel_otlp_exporter_start(g_otlp_exporter) == 0) {
            keel_stats_collector_t* coll = keel_engine_get_stats_collector(g_engine);
            if (coll) {
                g_otlp_agg = keel_otlp_aggregator_create(
                    coll, g_otlp_exporter, g_otlp_cfg.interval_ms);
                if (g_otlp_agg && keel_otlp_aggregator_start(g_otlp_agg) == 0) {
                    keel_admin_set_otlp_exporter(g_admin, g_otlp_exporter);
                    printf("  OTLP:     %s (interval=%ums)\n",
                           g_otlp_cfg.http.endpoint_url, g_otlp_cfg.interval_ms);
                } else {
                    fprintf(stderr,
                            "Warning: failed to start OTLP aggregator; disabling exporter\n");
                    if (g_otlp_agg) { keel_otlp_aggregator_destroy(g_otlp_agg); g_otlp_agg = NULL; }
                    keel_otlp_exporter_stop(g_otlp_exporter);
                    keel_otlp_exporter_destroy(g_otlp_exporter);
                    g_otlp_exporter = NULL;
                }
            } else {
                fprintf(stderr,
                        "Warning: stats collector unavailable; disabling OTLP exporter\n");
                keel_otlp_exporter_stop(g_otlp_exporter);
                keel_otlp_exporter_destroy(g_otlp_exporter);
                g_otlp_exporter = NULL;
            }
        } else {
            fprintf(stderr, "Warning: failed to start OTLP exporter\n");
            if (g_otlp_exporter) {
                keel_otlp_exporter_destroy(g_otlp_exporter);
                g_otlp_exporter = NULL;
            }
        }
    }
#endif

    /* Start cluster manager (if enabled) */
    if (g_cluster_cfg.enabled) {
        g_cluster = keel_cluster_create(&g_cluster_cfg);
        if (g_cluster) {
            /* Wire engine stats so heartbeats carry live client-connection count */
            if (g_engine)
                keel_cluster_set_stats_cb(g_cluster,
                                          cluster_engine_stats_cb, g_engine);
            if (keel_cluster_start(g_cluster) == 0) {
                printf("  Cluster:  %s:%u (node=%s, peers=%zu)\n",
                       g_cluster_cfg.listen_addr, g_cluster_cfg.listen_port,
                       g_cluster_cfg.node_id, g_cluster_cfg.initial_peer_count);
                /* Wire server-topology-change notifications to backend pools (H2) */
                keel_cluster_set_server_notify_cb(g_cluster,
                                                  cluster_server_notify_handler,
                                                  g_groups);
                /* Wire cluster into admin console for SHOW CLUSTER commands */
                if (g_admin)
                    keel_admin_set_cluster(g_admin, g_cluster);
            } else {
                fprintf(stderr, "Warning: failed to start cluster manager\n");
                keel_cluster_destroy(g_cluster);
                g_cluster = NULL;
            }
        }
    }

    /* Create probe managers (but don't start threads yet) */
    {
        bool probes_registered = false;
        for (size_t gi = 0; gi < g_num_groups; gi++) {
            worker_group_t* wg = &g_groups[gi];
            if (wg->server_pool.count == 0) continue;
            if (!probes_registered) {
                keel_probe_register_builtins();
                probes_registered = true;
            }
            wg->probe_mgr = keel_probe_manager_create(
                &wg->probe_cfg,
                keel_engine_get_server_pool(wg->engine),
                wg->engine);
            if (wg->probe_mgr) {
                printf("  [%s] Probe: type=%s interval=%ums timeout=%ums retries=%u\n",
                       wg->name, wg->probe_cfg.probe_type,
                       wg->probe_cfg.interval_ms, wg->probe_cfg.timeout_ms,
                       wg->probe_cfg.retries);
            }
        }
    }

    printf("  Press Ctrl+C to stop\n");
    printf("\n");
    fflush(stdout);

    /* Apply runtime security hardening after privileged setup (bind, RLIMIT)
     * and before worker threads start so policy applies process-wide. */
    if (apply_runtime_security_policy() < 0) {
        KEEL_LOG_FATAL(KEEL_LOG_CAT_CORE,
                       "security: failed to apply runtime security policy");
        cleanup_startup_failure(config, tls_initialized);
        return 1;
    }

    /* ====================================================================
     * Phase 2: Start engines (spawns worker threads) and probe threads.
     * Log output from workers may now appear, but the banner is done.
     * ==================================================================== */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        worker_group_t* wg = &g_groups[gi];
        if (keel_engine_start(wg->engine, wg->listen_fd) < 0) {
            KEEL_LOG_FATAL(KEEL_LOG_CAT_CORE, "[%s] Failed to start engine", wg->name);
            keel_engine_destroy(wg->engine);
            wg->engine = NULL;
            close(wg->listen_fd);
            wg->listen_fd = -1;
            cleanup_startup_failure(config, tls_initialized);
            return 1;
        }
    }

    /* Start probe threads */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].probe_mgr)
            keel_probe_manager_start(g_groups[gi].probe_mgr);
    }
    
    /* Register stats dump callback on first engine */
    keel_engine_set_periodic_callback(g_engine, (void(*)(void*))stats_dump, NULL);
    
    /* ================================================================
     * Main loop — wait for signals.
     * Workers run in their own threads; the main thread just waits.
     * This replaces keel_engine_run() to manage multiple engines.
     * ================================================================ */
    {
        sigset_t sigset;
        sigemptyset(&sigset);
        sigaddset(&sigset, SIGINT);
        sigaddset(&sigset, SIGTERM);
        sigaddset(&sigset, SIGUSR1);
        sigaddset(&sigset, SIGHUP);
        pthread_sigmask(SIG_BLOCK, &sigset, NULL);

        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: entering main loop");

        uint32_t interval_ms = g_config.stats_interval_ms;

        while (!g_should_stop) {
            int sig;

            if (interval_ms > 0) {
                struct timespec ts = {
                    .tv_sec  = interval_ms / 1000,
                    .tv_nsec = (long)(interval_ms % 1000) * 1000000L,
                };
                sig = keel_sigtimedwait(&sigset, &ts);
                if (sig < 0) {
                    if (errno == EAGAIN) {
                        stats_dump();
                        continue;
                    }
                    continue;  /* EINTR */
                }
            } else {
                if (sigwait(&sigset, &sig) != 0)
                    continue;
            }

            if (sig == SIGUSR1) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_STATS,
                             "engine: SIGUSR1 received, stats dump requested");
                stats_dump();
                keel_pool_dump_active_allocations();
                continue;
            }

            if (sig == SIGHUP) {
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                             "engine: SIGHUP received, reloading configuration");

                /* Reload TLS certificates for all worker groups */
                for (size_t gi = 0; gi < g_num_groups; gi++) {
                    worker_group_t* wg = &g_groups[gi];
                    keel_error_t rc = keel_tls_reload_certs(
                        wg->tls_config.mode != KEEL_TLS_DISABLE ? &wg->tls_config : NULL,
                        wg->backend_tls_config.mode != KEEL_TLS_DISABLE ? &wg->backend_tls_config : NULL);
                    if (rc == KEEL_OK) {
                        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS,
                            "[%s] TLS certificates reloaded successfully", wg->name);
                    } else if (wg->tls_config.mode != KEEL_TLS_DISABLE ||
                               wg->backend_tls_config.mode != KEEL_TLS_DISABLE) {
                        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                            "[%s] TLS certificate reload failed (existing certs still active)", wg->name);
                    }
                }

                /* Re-parse config and apply all safe-to-change parameters */
                if (g_config.config_file) {
                    keel_config_t* reload_cfg = keel_config_load(g_config.config_file);
                    if (reload_cfg) {
                        /* Log level */
                        const char* new_level = keel_config_get_string(
                            reload_cfg, "logging", "log_level", NULL);
                        if (new_level) {
                            int lvl = keel_log_level_from_string(new_level);
                            if (lvl >= 0) {
                                keel_log_set_level(lvl);
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                    "Log level changed to: %s", new_level);
                            }
                        }

                        /* Live-reload operational parameters per worker group */
                        int total_applied = 0;
                        for (size_t gi = 0; gi < g_num_groups; gi++) {
                            int n = reload_worker_group(reload_cfg, &g_groups[gi]);
                            total_applied += n;
                        }

                        if (total_applied > 0) {
                            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                "Live reload: %d parameter(s) applied across %zu group(s)",
                                total_applied, g_num_groups);
                        }

                        /* Hot-reload shard rules for each worker group router */
                        for (size_t gi = 0; gi < g_num_groups; gi++) {
                            worker_group_t* wg = &g_groups[gi];
                            if (!wg->router) continue;
                            keel_reload_result_t rr = {0};
                            keel_config_reload_shard_rules(reload_cfg, wg->name, wg->router, &rr);
                            if (rr.applied > 0 || rr.errors > 0) {
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                    "[%s] Shard rules reloaded: applied=%d skipped=%d unchanged=%d errors=%d",
                                    wg->name, rr.applied, rr.skipped, rr.unchanged, rr.errors);
                            }
                        }

                        /* Hot-reload declarative query rules */
                        {
                            keel_query_rules_t* new_qr = NULL;
                            if (keel_query_rules_load(reload_cfg, &new_qr) == KEEL_OK) {
                                size_t old_count = g_config.query_rules
                                    ? g_config.query_rules->count : 0;
                                keel_query_rules_replace(&g_config.query_rules, new_qr);
                                if (g_admin)
                                    keel_admin_set_query_rules(g_admin,
                                                               g_config.query_rules);
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                    "Query rules reloaded: %zu rule(s) (was %zu)",
                                    new_qr ? new_qr->count : 0, old_count);
                            } else {
                                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                                    "Query rules reload failed; previous rules still active");
                            }
                        }

                        /* Hot-reload throttle rules (H3) */
                        for (size_t gi = 0; gi < g_num_groups; gi++) {
                            worker_group_t* wg = &g_groups[gi];
                            keel_throttle_rules_t* new_tr = NULL;
                            if (keel_throttle_rules_load(reload_cfg, &new_tr) == KEEL_OK) {
                                keel_throttle_rules_replace(&wg->throttle_rules, new_tr);
                                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                    "[%s] Throttle rules reloaded", wg->name);
                            }
                        }

                        keel_config_free(reload_cfg);
                    }
                }

                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Configuration reload complete");
                continue;
            }

            /* SIGINT / SIGTERM */
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                         "engine: received signal %d, initiating shutdown", sig);
            g_should_stop = 1;
        }
    }
    
    /* ===== Shutdown ===== */
    printf("\n");
    printf("Shutting down...\n");
    
    /* Phase 1: Drain — stop accepting new connections, wait for active ones */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].engine) {
            keel_engine_drain(g_groups[gi].engine);
        }
    }
    
    /* Phase 2: Stop all engines (signal workers to exit) */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].engine)
            keel_engine_stop(g_groups[gi].engine);
    }
    
    /* Print final statistics (first engine) */
    {
        uint64_t total_conns = keel_engine_get_total_connections(g_engine);
        uint64_t active_conns = keel_engine_get_active_connections(g_engine);
        printf("\n");
        printf("Final Statistics:\n");
        printf("  Total connections:  %llu\n", (unsigned long long)total_conns);
        printf("  Active connections: %llu\n", (unsigned long long)active_conns);
    }
    
    /* Dump full instrumentation stats on shutdown */
    stats_dump();
    
#ifdef KEEL_HAS_OTLP
    /* Stop OTLP aggregator first so no more snapshots are queued, then exporter. */
    if (g_otlp_agg) {
        keel_otlp_aggregator_stop(g_otlp_agg);
        keel_otlp_aggregator_destroy(g_otlp_agg);
        g_otlp_agg = NULL;
    }
    if (g_otlp_exporter) {
        keel_admin_set_otlp_exporter(g_admin, NULL);
        keel_otlp_exporter_stop(g_otlp_exporter);
        keel_otlp_exporter_destroy(g_otlp_exporter);
        g_otlp_exporter = NULL;
    }
#endif

    /* Stop admin console + Prometheus */
    keel_admin_stop(g_admin);
    g_admin = NULL;

    /* Stop cluster manager */
    if (g_cluster) {
        keel_cluster_destroy(g_cluster);
        g_cluster = NULL;
    }
    
    /* Stop probe managers */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        keel_probe_manager_destroy(g_groups[gi].probe_mgr);
        g_groups[gi].probe_mgr = NULL;
    }

    /* Stop background topology discovery threads (H1) */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].discovery) {
            keel_discovery_stop(g_groups[gi].discovery);
            keel_discovery_destroy(g_groups[gi].discovery);
            g_groups[gi].discovery = NULL;
        }
    }

    /* Release throttle rules (H3) */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].throttle_rules) {
            keel_throttle_rules_replace(&g_groups[gi].throttle_rules, NULL);
        }
    }

    /* Destroy router plugin managers (A2) */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].router_mgr) {
            keel_router_mgr_destroy(g_groups[gi].router_mgr);
            g_groups[gi].router_mgr = NULL;
        }
    }

    /* Destroy all engines */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        keel_engine_destroy(g_groups[gi].engine);
        g_groups[gi].engine = NULL;
    }
    g_engine = NULL;

    /* Destroy tracer (after engines so all workers have stopped) */
    if (g_tracer) {
        keel_tracer_destroy(g_tracer);
        g_tracer = NULL;
    }

    /* Close audit log (after engines so all in-flight emits have completed) */
    if (g_audit_log_initialized) {
        keel_audit_log_close(&g_audit_log);
        g_audit_log_initialized = false;
    }
    
    /* Destroy per-group hook registries (after engines, since workers may still reference them) */
    for (size_t gi = 0; gi < g_num_groups; gi++) {
        if (g_groups[gi].hook_registry) {
            keel_hook_registry_destroy(g_groups[gi].hook_registry);
            g_groups[gi].hook_registry = NULL;
        }
    }

    /* Release configuration (safe now that all engines and workers have stopped) */
    keel_config_free(config);
    config = NULL;

    /* Shut down scripting runtimes */
    keel_lua_shutdown();
    keel_python_shutdown();
    
    /* Shutdown query logger and log plugin */
    keel_query_log_shutdown(&g_query_log);
    if (g_log_plugin) {
        g_log_plugin->flush(g_log_plugin);
        g_log_plugin->close(g_log_plugin);
        g_log_plugin->destroy(g_log_plugin);
        g_log_plugin = NULL;
    }
    
    /* Shut down memory subsystem */
    keel_mem_shutdown();

    /* Shut down TLS subsystem */
    keel_tls_cleanup();

    printf("Goodbye!\n");
    return 0;
}
