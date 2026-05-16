/**
 * @file probe_common.h
 * @brief Shared TCP connection helper for probe modules.
 *
 * Previously each probe (postgres, mysql, patroni) had its own copy of the
 * same non-blocking TCP connect routine under a different name:
 *   - tcp_connect()         (probe_postgres.c)
 *   - probe_tcp_connect()   (probe_mysql.c)
 *   - patroni_tcp_connect() (probe_patroni.c)
 *
 * All callers should now #include this header and call keel_probe_tcp_connect().
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_PROBE_COMMON_H
#define KEEL_PROBE_COMMON_H

#include <stdint.h>
#include <stddef.h>

/**
 * @brief Establish a TCP connection to @p host:@p port with a timeout.
 *
 * Uses a non-blocking connect(2) followed by epoll_wait (Linux) or select
 * (BSD) to wait for completion without blocking the probe thread.  On success
 * the returned fd is put back into blocking mode so that subsequent read/write
 * calls behave simply.
 *
 * @param host       Target hostname or IP address string.
 * @param port       TCP port number to connect to.
 * @param timeout_ms Maximum milliseconds to wait for the connection.
 * @param errbuf     Buffer to receive a human-readable error message.
 * @param errlen     Size of @p errbuf in bytes.
 * @return Connected file descriptor on success, -1 on failure.
 */
int keel_probe_tcp_connect(const char* host, uint16_t port, uint32_t timeout_ms,
                           char* errbuf, size_t errlen);

#endif /* KEEL_PROBE_COMMON_H */
