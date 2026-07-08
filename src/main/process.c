/**
 * @file process.c
 * @brief Process management utilities: daemonization, listener socket, crash handler.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Extracted from src/main/main.c — no logic changes, only relocation.
 */

#include "keel/main/process.h"

#include "keel/log/log.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/* execinfo.h is a GNU extension — absent on musl (Alpine). */
#if defined(__GLIBC__)
#include <execinfo.h>
#define KEEL_HAVE_EXECINFO 1
#endif

/* ============================================================================
 * Daemonization
 * ============================================================================ */

int daemonize_process(void) {
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

int create_listen_socket(const char* addr, uint16_t port, uint32_t backlog) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Failed to create socket: %s", strerror(errno));
        return -1;
    }

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
    bind_addr.sin_port   = htons(port);

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

void crash_handler(int sig) {
    const char* name = (sig == SIGSEGV) ? "SIGSEGV" :
                       (sig == SIGABRT) ? "SIGABRT" :
                       (sig == SIGBUS)  ? "SIGBUS"  : "UNKNOWN";
    /* Write directly to stderr (async-signal-safe).
     * Use if() to suppress GCC warn_unused_result — nothing to do on error
     * inside a crash handler anyway. */
    if (write(STDERR_FILENO, "FATAL: ", 7)) {}
    if (write(STDERR_FILENO, name, strlen(name))) {}
    if (write(STDERR_FILENO, " received — aborting\n", 21)) {}
    /* Async-signal-safe backtrace: backtrace(3) itself is safe;
     * backtrace_symbols_fd() is the safe (non-allocating) variant.
     * Only available with glibc — musl (Alpine) has no execinfo.h. */
#if defined(KEEL_HAVE_EXECINFO)
    void* frames[64];
    int   n = backtrace(frames, 64);
    if (write(STDERR_FILENO, "backtrace:\n", 11)) {}
    backtrace_symbols_fd(frames, n, STDERR_FILENO);
#endif
    /* Re-raise to get core dump */
    signal(sig, SIG_DFL);
    raise(sig);
}
