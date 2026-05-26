/**
 * @file keel_otlp_http.c
 * @brief Bounded-timeout HTTP/1.1 POST client for OTLP/HTTP collectors.
 *
 * Runs on the dedicated exporter thread; never on the reactor.
 * Connect + send + recv are all wall-clock-bounded by the caller's
 * timeout_ms (per proposal §1199-1320). HTTPS is out of scope for v0.2;
 * only `http://` is supported.
 */
#include "keel_otlp_http.h"

#include "keel/mem/mem.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define KEEL_OTLP_HTTP_REQ_HEADERS_MAX  1024
#define KEEL_OTLP_HTTP_RESP_BUF_MAX     8192

struct keel_otlp_http {
    keel_otlp_http_config_t cfg;
    char  host[256];
    char  port[16];
    char  path[256];
};

/* ------------------------------------------------------------------------- */
/* URL parsing                                                                */
/* ------------------------------------------------------------------------- */

/* Parse "http://host[:port]/path". Returns 0 on success. */
static int parse_endpoint(const char* url,
                          char* host, size_t host_cap,
                          char* port, size_t port_cap,
                          char* path, size_t path_cap)
{
    static const char prefix[] = "http://";
    if (strncmp(url, prefix, sizeof(prefix) - 1) != 0)
        return -1;
    const char* p = url + sizeof(prefix) - 1;

    /* Host: up to ':' or '/' or end. */
    const char* host_end = p;
    while (*host_end && *host_end != ':' && *host_end != '/')
        ++host_end;
    size_t host_len = (size_t)(host_end - p);
    if (host_len == 0 || host_len + 1 > host_cap)
        return -1;
    memcpy(host, p, host_len);
    host[host_len] = '\0';

    /* Port: optional ':NNNN'. */
    const char* path_start = host_end;
    if (*host_end == ':') {
        const char* port_start = host_end + 1;
        const char* port_end   = port_start;
        while (*port_end && *port_end != '/')
            ++port_end;
        size_t port_len = (size_t)(port_end - port_start);
        if (port_len == 0 || port_len + 1 > port_cap)
            return -1;
        memcpy(port, port_start, port_len);
        port[port_len] = '\0';
        path_start = port_end;
    } else {
        if (port_cap < 3)
            return -1;
        memcpy(port, "80", 3);
    }

    /* Path: '/' onward, default '/' if empty. */
    if (*path_start == '\0') {
        if (path_cap < 2)
            return -1;
        path[0] = '/';
        path[1] = '\0';
    } else {
        size_t path_len = strlen(path_start);
        if (path_len + 1 > path_cap)
            return -1;
        memcpy(path, path_start, path_len + 1);
    }
    return 0;
}

/* ------------------------------------------------------------------------- */
/* Time helpers                                                               */
/* ------------------------------------------------------------------------- */

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)(ts.tv_nsec / 1000000);
}

static int remaining_ms(uint64_t deadline_ms)
{
    uint64_t n = now_ms();
    if (n >= deadline_ms)
        return 0;
    uint64_t r = deadline_ms - n;
    if (r > INT32_MAX)
        return INT32_MAX;
    return (int)r;
}

/* ------------------------------------------------------------------------- */
/* Socket helpers                                                             */
/* ------------------------------------------------------------------------- */

static int set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
        return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* NOLINT(keel-blocking) — exporter thread; bounded by deadline_ms. */
static int connect_with_timeout(const char* host, const char* port,
                                uint64_t deadline_ms,
                                keel_otlp_http_result_t* err)
{
    struct addrinfo hints = {0};
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = NULL;
    if (getaddrinfo(host, port, &hints, &res) != 0 || !res) {
        *err = KEEL_OTLP_HTTP_CONNECT_FAILED;
        return -1;
    }

    int fd = -1;
    for (struct addrinfo* ai = res; ai != NULL; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0)
            continue;
        if (set_nonblock(fd) < 0) {
            close(fd);
            fd = -1;
            continue;
        }
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        int rc = connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (rc == 0)
            break;
        if (errno != EINPROGRESS) {
            close(fd);
            fd = -1;
            continue;
        }

        int wait_ms = remaining_ms(deadline_ms);
        if (wait_ms <= 0) {
            close(fd);
            fd = -1;
            *err = KEEL_OTLP_HTTP_TIMEOUT;
            continue;
        }
        struct pollfd pfd = { .fd = fd, .events = POLLOUT };
        int pr = poll(&pfd, 1, wait_ms);
        if (pr == 0) {
            close(fd);
            fd = -1;
            *err = KEEL_OTLP_HTTP_TIMEOUT;
            continue;
        }
        if (pr < 0) {
            close(fd);
            fd = -1;
            continue;
        }
        int soerr = 0;
        socklen_t slen = sizeof(soerr);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &soerr, &slen) < 0 || soerr != 0) {
            close(fd);
            fd = -1;
            continue;
        }
        break;
    }
    freeaddrinfo(res);
    if (fd < 0 && *err == KEEL_OTLP_HTTP_OK)
        *err = KEEL_OTLP_HTTP_CONNECT_FAILED;
    return fd;
}

/* NOLINT(keel-blocking) — exporter thread; bounded by deadline_ms. */
static int send_all(int fd, const uint8_t* buf, size_t len, uint64_t deadline_ms)
{
    size_t off = 0;
    while (off < len) {
        int wait_ms = remaining_ms(deadline_ms);
        if (wait_ms <= 0)
            return -2; /* timeout */
        struct pollfd pfd = { .fd = fd, .events = POLLOUT };
        int pr = poll(&pfd, 1, wait_ms);
        if (pr == 0)
            return -2;
        if (pr < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        ssize_t n = send(fd, buf + off, len - off, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        off += (size_t)n;
    }
    return 0;
}

/* NOLINT(keel-blocking) — exporter thread; bounded by deadline_ms. */
static int recv_response(int fd, char* buf, size_t cap, size_t* out_len,
                         uint64_t deadline_ms)
{
    size_t off = 0;
    bool   headers_done = false;
    size_t headers_end_off = 0;
    size_t content_length = 0;
    bool   have_content_length = false;
    bool   chunked = false;
    (void)chunked; /* chunked not handled; we set Connection: close so server
                    * normally streams body until EOF. */

    while (off + 1 < cap) {
        int wait_ms = remaining_ms(deadline_ms);
        if (wait_ms <= 0)
            return -2;
        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        int pr = poll(&pfd, 1, wait_ms);
        if (pr == 0)
            return -2;
        if (pr < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        ssize_t n = recv(fd, buf + off, cap - 1 - off, 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0) {
            buf[off] = '\0';
            *out_len = off;
            return 0; /* clean EOF; done */
        }
        off += (size_t)n;
        buf[off] = '\0';

        if (!headers_done) {
            char* hdr_end = strstr(buf, "\r\n\r\n");
            if (hdr_end) {
                headers_done    = true;
                headers_end_off = (size_t)(hdr_end - buf) + 4;
                /* Look for Content-Length (case-insensitive simple scan). */
                for (char* line = buf; line < hdr_end; ) {
                    char* eol = strstr(line, "\r\n");
                    if (!eol)
                        break;
                    *eol = '\0';
                    if (strncasecmp(line, "Content-Length:", 15) == 0) {
                        content_length     = strtoul(line + 15, NULL, 10);
                        have_content_length = true;
                    } else if (strncasecmp(line, "Transfer-Encoding:", 18) == 0 &&
                               strstr(line + 18, "chunked")) {
                        chunked = true;
                    }
                    *eol = '\r';
                    line = eol + 2;
                }
            }
        }
        if (headers_done && have_content_length) {
            if (off >= headers_end_off + content_length) {
                *out_len = off;
                return 0;
            }
        }
    }
    *out_len = off;
    return 0; /* buffer full — we may have truncated body; status line is intact. */
}

static int parse_status_code(const char* resp)
{
    /* "HTTP/1.x SSS Reason\r\n" */
    const char* p = strchr(resp, ' ');
    if (!p)
        return -1;
    return (int)strtol(p + 1, NULL, 10);
}

/* ------------------------------------------------------------------------- */
/* Public API                                                                 */
/* ------------------------------------------------------------------------- */

keel_otlp_http_t* keel_otlp_http_create(const keel_otlp_http_config_t* cfg)
{
    if (!cfg || !cfg->endpoint_url)
        return NULL;
    keel_otlp_http_t* http = keel_calloc(1, sizeof(*http));
    if (!http)
        return NULL;
    http->cfg = *cfg;
    if (parse_endpoint(cfg->endpoint_url,
                       http->host, sizeof(http->host),
                       http->port, sizeof(http->port),
                       http->path, sizeof(http->path)) != 0)
    {
        keel_free(http);
        return NULL;
    }
    if (http->cfg.timeout_ms == 0)
        http->cfg.timeout_ms = 5000;
    return http;
}

void keel_otlp_http_destroy(keel_otlp_http_t* http)
{
    keel_free(http);
}

keel_otlp_http_result_t keel_otlp_http_post(
    keel_otlp_http_t* http,
    const uint8_t* body,
    size_t body_len)
{
    if (!http || (!body && body_len > 0))
        return KEEL_OTLP_HTTP_PROTOCOL_ERROR;

    uint64_t deadline = now_ms() + http->cfg.timeout_ms;
    keel_otlp_http_result_t err = KEEL_OTLP_HTTP_OK;

    int fd = connect_with_timeout(http->host, http->port, deadline, &err);
    if (fd < 0)
        return err == KEEL_OTLP_HTTP_OK ? KEEL_OTLP_HTTP_CONNECT_FAILED : err;

    char headers[KEEL_OTLP_HTTP_REQ_HEADERS_MAX];
    int  hdr_len;
    if (http->cfg.bearer_token && http->cfg.bearer_token[0]) {
        hdr_len = snprintf(headers, sizeof(headers),
            "POST %s HTTP/1.1\r\n"
            "Host: %s:%s\r\n"
            "Content-Type: application/x-protobuf\r\n"
            "Content-Length: %zu\r\n"
            "Authorization: Bearer %s\r\n"
            "Connection: close\r\n"
            "\r\n",
            http->path, http->host, http->port, body_len,
            http->cfg.bearer_token);
    } else {
        hdr_len = snprintf(headers, sizeof(headers),
            "POST %s HTTP/1.1\r\n"
            "Host: %s:%s\r\n"
            "Content-Type: application/x-protobuf\r\n"
            "Content-Length: %zu\r\n"
            "Connection: close\r\n"
            "\r\n",
            http->path, http->host, http->port, body_len);
    }
    if (hdr_len <= 0 || (size_t)hdr_len >= sizeof(headers)) {
        close(fd);
        return KEEL_OTLP_HTTP_PROTOCOL_ERROR;
    }

    int sr = send_all(fd, (const uint8_t*)headers, (size_t)hdr_len, deadline);
    if (sr == -2) { close(fd); return KEEL_OTLP_HTTP_TIMEOUT; }
    if (sr < 0)   { close(fd); return KEEL_OTLP_HTTP_PROTOCOL_ERROR; }

    if (body_len > 0) {
        sr = send_all(fd, body, body_len, deadline);
        if (sr == -2) { close(fd); return KEEL_OTLP_HTTP_TIMEOUT; }
        if (sr < 0)   { close(fd); return KEEL_OTLP_HTTP_PROTOCOL_ERROR; }
    }

    char   resp[KEEL_OTLP_HTTP_RESP_BUF_MAX];
    size_t resp_len = 0;
    int    rr = recv_response(fd, resp, sizeof(resp), &resp_len, deadline);
    close(fd);
    if (rr == -2)
        return KEEL_OTLP_HTTP_TIMEOUT;
    if (rr < 0 || resp_len == 0)
        return KEEL_OTLP_HTTP_PROTOCOL_ERROR;

    int status = parse_status_code(resp);
    if (status < 0)
        return KEEL_OTLP_HTTP_PROTOCOL_ERROR;
    if (status >= 200 && status < 300)
        return KEEL_OTLP_HTTP_OK;
    if (status == 429 || (status >= 500 && status < 600))
        return KEEL_OTLP_HTTP_SERVER_RETRY;
    if (status >= 400 && status < 500)
        return KEEL_OTLP_HTTP_SERVER_REJECT;
    return KEEL_OTLP_HTTP_PROTOCOL_ERROR;
}
