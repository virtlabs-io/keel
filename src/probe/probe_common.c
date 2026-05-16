/**
 * @file probe_common.c
 * @brief Shared TCP connection helper for probe modules.
 *
 * See probe_common.h for documentation.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/probe/probe_common.h"
#include "keel/util/platform_compat.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

int keel_probe_tcp_connect(const char* host, uint16_t port, uint32_t timeout_ms,
                           char* errbuf, size_t errlen)
{
    struct addrinfo hints = {
        .ai_family   = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM,
    };
    char portstr[8];
    snprintf(portstr, sizeof(portstr), "%u", port);

    struct addrinfo *res = NULL;
    int gai = getaddrinfo(host, portstr, &hints, &res);
    if (gai != 0) {
        snprintf(errbuf, errlen, "getaddrinfo: %s", gai_strerror(gai));
        return -1;
    }

    int fd = socket(res->ai_family, SOCK_STREAM, 0);
    if (fd < 0) {
        snprintf(errbuf, errlen, "socket: %s", strerror(errno));
        freeaddrinfo(res);
        return -1;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    int rc = connect(fd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);

    if (rc < 0 && errno != EINPROGRESS) {
        snprintf(errbuf, errlen, "connect: %s", strerror(errno));
        close(fd);
        return -1;
    }

    if (rc < 0) {
        int pr = keel_fd_wait(fd, KEEL_FD_WAIT_WRITE, (int)timeout_ms);
        if (pr <= 0) {
            snprintf(errbuf, errlen, pr == 0 ? "connect timeout" : "epoll: %s",
                     strerror(errno));
            close(fd);
            return -1;
        }
        int sockerr = 0;
        socklen_t slen = sizeof(sockerr);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &slen);
        if (sockerr != 0) {
            snprintf(errbuf, errlen, "connect: %s", strerror(sockerr));
            close(fd);
            return -1;
        }
    }

    /* Restore blocking mode — probe callers use simple blocking reads */
    fcntl(fd, F_SETFL, flags);

    /* Disable Nagle for low-latency request/response */
    keel_set_nodelay(fd);

    return fd;
}
