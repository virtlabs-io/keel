/**
 * @file process.h
 * @brief Process management utilities: daemonization, listener socket creation,
 *        and fatal-signal crash handler.
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <stdint.h>

/**
 * @brief Detach the process from the controlling terminal using the classic
 *        double-fork pattern.
 *
 * @return 0 when daemonization succeeds, or -1 on any fork/setsid/chdir failure.
 */
int daemonize_process(void);

/**
 * @brief Create, tune, bind, and listen on a frontend TCP socket.
 *
 * @param addr    Text IPv4 listen address. Non-literals fall back to INADDR_ANY.
 * @param port    TCP port to bind.
 * @param backlog Requested listen backlog. Values <= 0 use a conservative default.
 * @return A non-negative socket descriptor on success, or -1 on failure.
 */
int create_listen_socket(const char* addr, uint16_t port, uint32_t backlog);

/**
 * @brief Fatal signal handler that writes a brief diagnostic and re-raises.
 *
 * Suitable for SIGSEGV, SIGABRT, SIGBUS.  Uses only async-signal-safe
 * operations: write() to stderr then raise() to produce a core dump.
 *
 * @param sig Signal number delivered by the kernel.
 */
void crash_handler(int sig);
