/**
 * @file probe_patroni.c
 * @brief Patroni REST-endpoint health and role probe.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Patroni exposes topology information through a lightweight HTTP API, so this
 * probe can detect role without authenticating to PostgreSQL itself. That makes it
 * especially useful in clusters where the control plane is authoritative and where
 * SQL-level probing might be slower or more operationally invasive. The tradeoff is
 * that the probe trusts Patroni's view of role and currently does so over plain HTTP.
 */

#include "keel/probe/probe.h"
#include "keel/probe/probe_common.h"
#include "keel/engine/engine.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "keel/util/platform_compat.h"

/* ============================================================================
 * Probe Context
 * ============================================================================ */

/**
 * Per-server Patroni probe context.
 * Stores the HTTP port (separate from PG port) and the timeout.
 */
typedef struct patroni_probe_ctx {
    uint16_t    http_port;      /**< Patroni REST API port (default 8008) */
    uint32_t    timeout_ms;     /**< TCP connect + HTTP read timeout */
} patroni_probe_ctx_t;

/* ============================================================================
 * Helpers
 * ============================================================================ */

/** @brief Monotonic microsecond timestamp for latency measurement. */
static uint64_t patroni_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* patroni_tcp_connect() removed: use keel_probe_tcp_connect() from keel/probe/probe_common.h */

/**
 * Send a GET request to `path`, read the HTTP status code.
 * Returns the HTTP status code (e.g. 200, 503), or -1 on error.
 * Reads and discards the body.
 */
static int http_get_status(int fd, const char* host, const char* path,
                           uint32_t timeout_ms, char* errbuf, size_t errlen)
{
    /* Build HTTP/1.0 request (1.0 = server closes connection after response) */
    char req[512];
    int reqlen = snprintf(req, sizeof(req),
        "GET %s HTTP/1.0\r\n"
        "Host: %s\r\n"
        "Connection: close\r\n"
        "\r\n",
        path, host);

    /* Send request */
    const char* p = req;
    int left = reqlen;
    while (left > 0) {
        ssize_t n = write(fd, p, (size_t)left);
        if (n <= 0) {
            snprintf(errbuf, errlen, "write: %s", strerror(errno));
            return -1;
        }
        p += n;
        left -= (int)n;
    }

    /* Read response (at least the status line) */
    char resp[1024];
    size_t resp_len = 0;

    while (resp_len < sizeof(resp) - 1) {
        int pr = keel_fd_wait(fd, KEEL_FD_WAIT_READ, (int)timeout_ms);
        if (pr <= 0) break;

        ssize_t n = read(fd, resp + resp_len, sizeof(resp) - 1 - resp_len);
        if (n <= 0) break;
        resp_len += (size_t)n;

        /* Check if we have the full status line */
        if (resp_len >= 12) break; /* "HTTP/1.x NNN" is 12+ chars */
    }

    resp[resp_len] = '\0';

    /* Parse: "HTTP/1.x <status_code> ..." */
    int status = 0;
    if (resp_len >= 12 && strncmp(resp, "HTTP/1.", 7) == 0) {
        /* Find space after version */
        const char* sp = strchr(resp + 7, ' ');
        if (sp) {
            status = atoi(sp + 1);
        }
    }

    if (status == 0) {
        snprintf(errbuf, errlen, "bad HTTP response: %.40s", resp);
        return -1;
    }

    return status;
}

/* ============================================================================
 * Vtable Implementation
 * ============================================================================ */

/**
 * @brief Create Patroni probe context for one server.
 *
 * Parses the HTTP port from the extra string (e.g. "8008").
 * Falls back to the Patroni default of 8008 if extra is NULL or empty.
 *
 * @param server  Backend server being probed (used for host)
 * @param extra   Port string, e.g. "8008"
 * @return Opaque context pointer, or NULL on allocation failure
 */
static void* patroni_probe_create(const keel_backend_server_t* server,
                                   const char* extra)
{
    (void)server;

    patroni_probe_ctx_t* ctx = keel_calloc(1, sizeof(patroni_probe_ctx_t));
    if (!ctx) return NULL;

    /* Parse port from extra (e.g. "8008") */
    ctx->http_port = 8008;  /* Patroni default */
    if (extra && extra[0]) {
        int p = atoi(extra);
        if (p > 0 && p <= 65535)
            ctx->http_port = (uint16_t)p;
    }
    ctx->timeout_ms = 3000;
    return ctx;
}

/**
 * @brief Execute one health + role check via Patroni REST API.
 *
 * Strategy:
 *   1. Connect to patroni_port, GET /primary
 *   2. If 200 → PRIMARY
 *   3. Else reconnect, GET /replica
 *   4. If 200 → REPLICA
 *   5. Else → role unknown, health DEGRADED
 *
 * @param opaque  Context from patroni_probe_create()
 * @param server  Current backend config (host used for TCP connect)
 * @param result  Output: health, detected_role, latency_us, message
 * @return KEEL_OK always (server health is in result->health)
 */
static keel_error_t patroni_probe_check(void* opaque,
                                        const keel_backend_server_t* server,
                                        keel_probe_check_t* result)
{
    patroni_probe_ctx_t* ctx = (patroni_probe_ctx_t*)opaque;
    memset(result, 0, sizeof(*result));

    uint64_t t0 = patroni_now_us();

    /* 1. Connect to Patroni REST API */
    int fd = keel_probe_tcp_connect(server->host, ctx->http_port, ctx->timeout_ms,
                                  result->message, sizeof(result->message));
    if (fd < 0) {
        result->health = KEEL_HEALTH_DOWN;
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
        result->latency_us = patroni_now_us() - t0;
        result->error = KEEL_ERR_CONNECT;
        return KEEL_OK;
    }

    /* 2. Try GET /primary */
    int status = http_get_status(fd, server->host, "/primary",
                                 ctx->timeout_ms, result->message,
                                 sizeof(result->message));
    close(fd);

    if (status < 0) {
        /* HTTP failed entirely */
        result->health = KEEL_HEALTH_DOWN;
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
        result->latency_us = patroni_now_us() - t0;
        result->error = KEEL_ERR_IO;
        return KEEL_OK;
    }

    if (status == 200) {
        /* This node is the primary */
        result->health = KEEL_HEALTH_UP;
        result->detected_role = KEEL_SERVER_ROLE_PRIMARY;
        result->latency_us = patroni_now_us() - t0;
        snprintf(result->message, sizeof(result->message),
                 "primary (patroni /primary → 200)");
        return KEEL_OK;
    }

    /* 3. Not primary — try GET /replica */
    fd = keel_probe_tcp_connect(server->host, ctx->http_port, ctx->timeout_ms,
                              result->message, sizeof(result->message));
    if (fd < 0) {
        /* Could connect before but not now — degraded */
        result->health = KEEL_HEALTH_DEGRADED;
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
        result->latency_us = patroni_now_us() - t0;
        return KEEL_OK;
    }

    status = http_get_status(fd, server->host, "/replica",
                             ctx->timeout_ms, result->message,
                             sizeof(result->message));
    close(fd);

    result->latency_us = patroni_now_us() - t0;

    if (status == 200) {
        result->health = KEEL_HEALTH_UP;
        result->detected_role = KEEL_SERVER_ROLE_REPLICA;
        snprintf(result->message, sizeof(result->message),
                 "replica (patroni /replica → 200)");
    } else if (status > 0) {
        /* Got HTTP response but neither primary nor replica */
        result->health = KEEL_HEALTH_DEGRADED;
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
        snprintf(result->message, sizeof(result->message),
                 "role unknown (patroni /replica → %d)", status);
    } else {
        result->health = KEEL_HEALTH_DOWN;
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
    }

    return KEEL_OK;
}

/** @brief Free Patroni probe context. */
static void patroni_probe_destroy(void* opaque)
{
    keel_free(opaque);
}

/* ============================================================================
 * Exported Vtable
 * ============================================================================ */

const keel_probe_ops_t keel_probe_patroni_ops = {
    .name    = "patroni",
    .create  = patroni_probe_create,
    .check   = patroni_probe_check,
    .destroy = patroni_probe_destroy,
};
