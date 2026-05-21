/**
 * @file backend_connect_async.c
 * @brief Asynchronous backend connection bootstrap and authentication flow.
 *
 * This file contains the worker-side state machine that turns an empty backend
 * pool slot into a ready authenticated server connection without blocking the
 * reactor thread on DNS, connect completion, or protocol authentication round
 * trips.
 *
 * The implementation is protocol-aware because PostgreSQL and MySQL differ
 * substantially in who speaks first, how TLS is negotiated, and how the auth
 * exchange is sequenced. Keeping those paths in one async module lets the pool
 * layer treat "warm or refill this connection" as one operation.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/engine/backend_connect.h"
#include "keel/engine/backend_pool.h"
#include "keel/core/cloud_auth.h"
#include "keel/reactor/reactor.h"
#include "keel/protocol/backend_auth.h"
#include "keel/util/util.h"
#include "keel/protocol/tls_context.h"
#include "keel/util/endian.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include "keel/util/platform_compat.h"

#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/bio.h>

/* ============================================================================
 * Helpers (same as backend_auth.c — small inline utilities)
 * ============================================================================ */

/**
 * @brief Get the effective password for a backend connection.
 *
 * If cloud-native auth is configured for the pool, this returns a short-lived
 * token (possibly cached); otherwise returns the static config password.
 */
static inline const char* pool_get_password(backend_pool_t* pool) {
    return keel_cloud_auth_get_password(
        &pool->cloud_token_cache,
        pool->config.host, pool->config.port,
        pool->config.user, pool->config.password);
}

/**
 * @brief Return the current wall-clock time in milliseconds.
 *
 * @return Milliseconds since the Unix epoch.
 */
/* be32/wr32be replaced by keel_be32_get/keel_be32_put from keel/util/endian.h */
#define be32(p)    keel_be32_get(p)
#define wr32be(p,v) keel_be32_put((p),(v))

static uint64_t get_time_ms(void) { return keel_time_now_ms(); }

/**
 * @brief Base64-encode a byte buffer into a NUL-terminated string.
 *
 * @param in   Input bytes to encode.
 * @param len  Number of input bytes.
 * @param out  Output character buffer (NUL-terminated on success).
 * @param omax Capacity of @p out including the NUL terminator.
 * @return Number of characters written, or 0 if @p omax is too small.
 */
static size_t b64_encode(const uint8_t* in, size_t len,
                         char* out, size_t omax) {
    static const char t[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t o = 0;
    for (size_t i = 0; i < len; i += 3) {
        if (o + 4 >= omax) return 0;
        uint32_t v = (uint32_t)in[i] << 16;
        if (i+1 < len) v |= (uint32_t)in[i+1] << 8;
        if (i+2 < len) v |= in[i+2];
        out[o++] = t[(v>>18)&63]; out[o++] = t[(v>>12)&63];
        out[o++] = (i+1 < len) ? t[(v>>6)&63] : '=';
        out[o++] = (i+2 < len) ? t[v&63]      : '=';
    }
    out[o] = '\0';
    return o;
}

/**
 * @brief Decode a Base64-encoded string into raw bytes.
 *
 * @param in   Base64-encoded input string.
 * @param len  Length of @p in in characters.
 * @param out  Output byte buffer.
 * @param omax Capacity of @p out in bytes.
 * @return Number of bytes decoded.
 */
static size_t b64_decode(const char* in, size_t len,
                         uint8_t* out, size_t omax) {
    static const int8_t d[] = {
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
        52,53,54,55,56,57,58,59,60,61,-1,-1,-1, 0,-1,-1,
        -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
        15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
        -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50,51
    };
    size_t o = 0;
    for (size_t i = 0; i + 3 < len; i += 4) {
        /* Cast to unsigned char so the < 128 guard is meaningful on platforms
         * where plain char is signed (where a byte >= 0x80 would appear < 128
         * as a signed value, yielding a negative d[] index). */
        int a = ((unsigned char)in[i  ] < 128u) ? d[(unsigned char)in[i  ]] : -1;
        int b = ((unsigned char)in[i+1] < 128u) ? d[(unsigned char)in[i+1]] : -1;
        int c = ((unsigned char)in[i+2] < 128u) ? d[(unsigned char)in[i+2]] : -1;
        int e = ((unsigned char)in[i+3] < 128u) ? d[(unsigned char)in[i+3]] : -1;
        if (a < 0 || b < 0) break;
        if (o < omax) out[o++] = (uint8_t)((a<<2)|(b>>4));
        if (c >= 0 && in[i+2] != '=' && o < omax) out[o++] = (uint8_t)((b<<4)|(c>>2));
        if (e >= 0 && in[i+3] != '=' && o < omax) out[o++] = (uint8_t)((c<<6)|e);
    }
    return o;
}

/* ---- SASL message builders ---------------------------------------------- */

/**
 * @brief Build a PostgreSQL SASLInitialResponse ('p') frontend message.
 *
 * @param mechanism SASL mechanism name (e.g. "SCRAM-SHA-256").
 * @param data      Initial client SASL payload.
 * @param dlen      Length of @p data in bytes.
 * @param out       Output buffer to write the message into.
 * @param omax      Capacity of @p out in bytes.
 * @return Total message length on success, or -1 if @p omax is too small.
 */
static ssize_t pg_sasl_initial_response(const char* mechanism,
                                        const char* data, size_t dlen,
                                        uint8_t* out, size_t omax) {
    size_t mlen = strlen(mechanism);
    size_t total = 1 + 4 + mlen + 1 + 4 + dlen;
    if (total > omax) return -1;
    out[0] = 'p';
    wr32be(out+1, (uint32_t)(total - 1));
    memcpy(out+5, mechanism, mlen+1);
    wr32be(out+5+mlen+1, (uint32_t)dlen);
    memcpy(out+5+mlen+1+4, data, dlen);
    return (ssize_t)total;
}

/**
 * @brief Build a PostgreSQL SASLResponse ('p') frontend message.
 *
 * @param data Client SASL response payload.
 * @param dlen Length of @p data in bytes.
 * @param out  Output buffer to write the message into.
 * @param omax Capacity of @p out in bytes.
 * @return Total message length on success, or -1 if @p omax is too small.
 */
static ssize_t pg_sasl_response(const uint8_t* data, size_t dlen,
                                uint8_t* out, size_t omax) {
    size_t total = 1 + 4 + dlen;
    if (total > omax) return -1;
    out[0] = 'p';
    wr32be(out+1, (uint32_t)(total - 1));
    memcpy(out+5, data, dlen);
    return (ssize_t)total;
}

/* ============================================================================
 * Forward declarations for callback chain
 * ============================================================================ */

static void on_connect_complete(void* userdata, int result);
static void on_startup_sent(void* userdata, int result);
static void on_auth_recv(void* userdata, int result);
static void on_scram_first_sent(void* userdata, int result);
static void on_scram_first_recv(void* userdata, int result);
static void on_scram_final_sent(void* userdata, int result);
static void on_scram_final_recv(void* userdata, int result);
static void on_drain_recv(void* userdata, int result);

/* Backend TLS callbacks */
static void on_tls_sslrequest_sent(void* userdata, int result);
static void on_tls_sslrequest_recv(void* userdata, int result);
static void on_tls_hs_send_complete(void* userdata, int result);
static void on_tls_hs_recv(void* userdata, int result);

/* Helper: start the PostgreSQL startup after TLS negotiation is done */
static void start_pg_startup(backend_async_ctx_t* ctx);

/* MySQL async auth callbacks */
static void on_my_greeting_recv(void* userdata, int result);
static void on_my_response_sent(void* userdata, int result);
static void on_my_auth_recv(void* userdata, int result);
static void on_my_auth_data_sent(void* userdata, int result);
static void on_my_rsa_key_recv_trigger(void* userdata, int result);
static void on_my_rsa_key_recv(void* userdata, int result);
static void on_my_rsa_pw_sent(void* userdata, int result);

static void async_fail(backend_async_ctx_t* ctx);
static void async_succeed(backend_async_ctx_t* ctx);

/* ============================================================================
 * Public API — Start an async connect
 * ============================================================================ */

/**
 * @brief Allocate and launch an asynchronous backend connect attempt.
 *
 * The pool resolves backend addresses ahead of time, so this function can stay
 * entirely on the non-blocking path: create socket, start connect, and hand the
 * rest of the sequence to reactor callbacks.
 *
 * @param pool Owning backend pool.
 * @param conn Connection slot being filled.
 * @param reactor Reactor that will drive the async state machine.
 * @param on_complete Completion callback invoked when the slot either becomes
 *        ready or definitively fails.
 * @param userdata Opaque callback context.
 * @return `0` on successful launch, `-1` on immediate allocation/setup failure.
 */
int backend_async_start(struct backend_pool* pool,
                        struct backend_conn* conn,
                        struct keel_reactor* reactor,
                        backend_async_done_cb on_complete,
                        void* userdata)
{
    backend_async_ctx_t* ctx = keel_calloc(1, sizeof(*ctx));
    if (!ctx) return -1;

    ctx->pool = pool;
    ctx->conn = conn;
    ctx->reactor = reactor;
    ctx->on_complete = on_complete;
    ctx->userdata = userdata;
    ctx->phase = BE_ASYNC_CONNECT;
    ctx->authed = false;

    /* Create non-blocking socket */
    int fd = keel_socket_nonblock(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        keel_free(ctx);
        return -1;
    }

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
#ifdef TCP_QUICKACK
    setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt));
#endif
#ifdef TCP_SYNCNT
    /* Limit SYN retransmissions so unreachable backends are detected in ~3s
     * instead of the kernel default ~127s.  With TCP_SYNCNT=1 the kernel
     * sends one SYN and one retry; on failure the connect completes with
     * ETIMEDOUT and the pool error-handling / backoff logic takes over. */
    int syncnt = 2;
    setsockopt(fd, IPPROTO_TCP, TCP_SYNCNT, &syncnt, sizeof(syncnt));
#endif

    ctx->fd = fd;

    /* Use the address resolved at pool-creation time (getaddrinfo was called
     * there so we never block the reactor thread on DNS here). */
    if (!pool->addr_resolved) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "async_connect: backend host '%s' not resolved — cannot connect",
            pool->config.host);
        close(fd);
        keel_free(ctx);
        return -1;
    }
    ctx->addr = pool->resolved_addr;

    /* Queue async connect via reactor */
    int rc = keel_reactor_connect(reactor, fd,
                                 (struct sockaddr*)&ctx->addr, sizeof(ctx->addr),
                                 ctx, on_connect_complete);
    if (rc < 0) {
        close(fd);
        keel_free(ctx);
        return -1;
    }

    return 0;
}

/* ============================================================================
 * Completion / Failure helpers
 * ============================================================================ */

/**
 * @brief Tear down an in-flight async backend connect as failed.
 *
 * @param ctx Async connect state.
 * @return
 */
static void async_fail(backend_async_ctx_t* ctx)
{
    ctx->phase = BE_ASYNC_DONE;
    if (ctx->tls_ctx) {
        keel_tls_context_destroy(ctx->tls_ctx);
        ctx->tls_ctx = NULL;
    }
    if (ctx->fd >= 0) {
        close(ctx->fd);
        ctx->fd = -1;
    }
    if (ctx->on_complete) {
        ctx->on_complete(ctx->conn, false, ctx->userdata);
    }
    keel_free(ctx);
}

/**
 * @brief Finalize a successful async backend connect and transfer the socket.
 *
 * @param ctx Async connect state.
 * @return
 */
static void async_succeed(backend_async_ctx_t* ctx)
{
    ctx->phase = BE_ASYNC_DONE;
    /* Transfer the fd to the connection */
    ctx->conn->fd = ctx->fd;
    ctx->fd = -1;  /* Prevent double-close */

    /* Record creation timestamp for max-age pruning */
    ctx->conn->created_at = get_time_ms();

    /* Propagate cancel-key data for query cancellation forwarding */
    ctx->conn->backend_pid    = ctx->pg_backend_pid;
    ctx->conn->cancel_secret  = ctx->pg_cancel_secret;
    ctx->conn->my_connection_id = ctx->my_connection_id;

    if (ctx->on_complete) {
        ctx->on_complete(ctx->conn, true, ctx->userdata);
    }
    keel_free(ctx);
}

/* ============================================================================
 * Step 1: Connect complete → send StartupMessage
 * ============================================================================ */

/**
 * @brief Reactor callback invoked when the non-blocking TCP connect finishes.
 *
 * On success, advances to TLS negotiation (if configured) or sends the
 * PostgreSQL StartupMessage. For MySQL backends, arms a receive for the
 * server greeting packet.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   0 on connect success, negative errno on failure.
 */
static void on_connect_complete(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result < 0) {
        /* Rate-limit error logging: apply exponential backoff on the pool.
         * Log the first failure and then every 5th to avoid flooding. */
        backend_pool_t* pool = ctx->pool;
        pool->refill_fail_count++;
        uint64_t delay = 1000ULL << (pool->refill_fail_count > 5 ? 5 : pool->refill_fail_count - 1);
        if (delay > 30000) delay = 30000;
        pool->refill_backoff_until = get_time_ms() + delay;
        if (pool->refill_fail_count <= 1 || pool->refill_fail_count % 10 == 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                    "async_connect: connect to %s:%u failed: %s (fail #%u, backoff %lums)",
                    pool->config.host, pool->config.port,
                    strerror(-result), pool->refill_fail_count,
                    (unsigned long)delay);
        }
        async_fail(ctx);
        return;
    }

    /* Success — reset backoff counter */
    ctx->pool->refill_fail_count = 0;

    /* For MySQL backends, the server speaks first (sends handshake greeting).
     * Use a fully async state machine to avoid blocking the reactor thread. */
    if (ctx->pool->config.protocol &&
        strcmp(ctx->pool->config.protocol, "mysql") == 0) {
        ctx->phase = BE_ASYNC_MY_GREETING_RECV;
        ctx->recv_have = 0;
        ctx->my_auth_rounds = 0;
        int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                                  ctx->recv_buf, sizeof(ctx->recv_buf),
                                  0, ctx, on_my_greeting_recv);
        if (rc < 0) {
            async_fail(ctx);
        }
        return;
    }

    /* PostgreSQL path: check whether to send SSLRequest first */
    if (ctx->pool->config.tls_config.mode != KEEL_TLS_DISABLE) {
        /* Send PostgreSQL SSLRequest (8-byte message: len=8, code=80877103) */
        ctx->phase = BE_ASYNC_TLS_SSLREQUEST_SENT;
        ctx->send_buf[0] = 0; ctx->send_buf[1] = 0;
        ctx->send_buf[2] = 0; ctx->send_buf[3] = 8;    /* length = 8 */
        ctx->send_buf[4] = 0x04; ctx->send_buf[5] = 0xd2; /* 80877103 high */
        ctx->send_buf[6] = 0x16; ctx->send_buf[7] = 0x2f; /* 80877103 low  */
        ctx->send_len = 8;
        int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                                  ctx->send_buf, ctx->send_len,
                                  MSG_NOSIGNAL, ctx, on_tls_sslrequest_sent);
        if (rc < 0) {
            async_fail(ctx);
        }
        return;
    }

    /* No TLS — proceed directly to StartupMessage */
    start_pg_startup(ctx);
}

/* ============================================================================
 * Helper: build and send PostgreSQL StartupMessage
 * ============================================================================ */

/**
 * @brief Build and queue the PostgreSQL startup packet after connect/TLS.
 *
 * @param ctx Async connect state.
 * @return
 */
static void start_pg_startup(backend_async_ctx_t* ctx)
{

    const char* user = ctx->pool->config.user;
    const char* database = ctx->pool->config.database;

    uint8_t* buf = ctx->send_buf;
    size_t pos = 4;                         /* skip length header */
    buf[pos++] = 0; buf[pos++] = 3;        /* protocol 3.0 */
    buf[pos++] = 0; buf[pos++] = 0;

    memcpy(buf + pos, "user", 5);           pos += 5;
    size_t ul = strlen(user) + 1;
    memcpy(buf + pos, user, ul);            pos += ul;

    memcpy(buf + pos, "database", 9);       pos += 9;
    size_t dl = strlen(database) + 1;
    memcpy(buf + pos, database, dl);        pos += dl;

    buf[pos++] = 0;                         /* terminator */
    wr32be(buf, (uint32_t)pos);             /* write length */

    ctx->send_len = pos;

    /* Queue async send */
    int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                              ctx->send_buf, ctx->send_len,
                              MSG_NOSIGNAL,
                              ctx, on_startup_sent);
    if (rc < 0) {
        async_fail(ctx);
    }
}

/* ============================================================================
 * Backend TLS: SSLRequest sent → recv 1-byte 'S'/'N' response
 * ============================================================================ */

/**
 * @brief Reactor callback after the PostgreSQL SSLRequest message is sent.
 *
 * Arms a single-byte receive to read the server's 'S' (accept) or 'N'
 * (decline) TLS negotiation response.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes sent, or negative errno on failure.
 */
static void on_tls_sslrequest_sent(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;
    if (result < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "async_connect: SSLRequest send failed: %d", result);
        async_fail(ctx);
        return;
    }
    ctx->phase = BE_ASYNC_TLS_SSLREQUEST_SENT;
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, 1,  /* 1 byte: 'S' or 'N' */
                              0, ctx, on_tls_sslrequest_recv);
    if (rc < 0) {
        async_fail(ctx);
    }
}

/**
 * @brief Reactor callback after the server's SSLRequest response byte is received.
 *
 * Interprets 'S' as TLS accepted and initiates the handshake; 'N' as
 * declined, falling back to plaintext or failing when mode is REQUIRE.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes received, or negative/zero on failure.
 */
static void on_tls_sslrequest_recv(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result <= 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "async_connect: SSLRequest response recv failed: %d", result);
        async_fail(ctx);
        return;
    }

    uint8_t resp = ctx->recv_buf[0];
    if (resp == 'N') {
        /* Backend declined TLS */
        if (ctx->pool->config.tls_config.mode == KEEL_TLS_REQUIRE) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "async_connect: backend declined TLS but mode=REQUIRE");
            async_fail(ctx);
            return;
        }
        /* PREFER: continue without TLS */
        KEEL_LOG_WARN(KEEL_LOG_CAT_CONN,
            "async_connect: backend declined TLS, continuing plaintext");
        start_pg_startup(ctx);
        return;
    }

    if (resp != 'S') {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "async_connect: unexpected SSLRequest response: 0x%02x", resp);
        async_fail(ctx);
        return;
    }

    /* Backend accepted TLS — create client-side TLS context and start handshake */
    keel_tls_context_t* tls_ctx = NULL;
    keel_error_t ke = keel_tls_context_create(
        &ctx->pool->config.tls_config, false /* client */, &tls_ctx);
    if (ke != KEEL_OK) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "async_connect: failed to create backend TLS context (err=%d)", (int)ke);
        async_fail(ctx);
        return;
    }
    ctx->tls_ctx = tls_ctx;
    ctx->tls_hs_active = true;
    ctx->phase = BE_ASYNC_TLS_HANDSHAKE;

    /* Start handshake — produces ClientHello bytes */
    keel_tls_hs_result_t hs = keel_tls_handshake_step(ctx->tls_ctx, NULL);
    if (hs == KEEL_TLS_HS_ERROR) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "async_connect: TLS handshake initial step failed");
        async_fail(ctx);
        return;
    }

    /* Send any handshake bytes generated (ClientHello) */
    ssize_t n = keel_tls_get_handshake_data(ctx->tls_ctx,
                                            ctx->tls_hs_buf, sizeof(ctx->tls_hs_buf));
    if (n > 0) {
        int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                                  ctx->tls_hs_buf, (size_t)n,
                                  MSG_NOSIGNAL, ctx, on_tls_hs_send_complete);
        if (rc < 0) {
            async_fail(ctx);
        }
    } else {
        /* Wait for server data if nothing to send immediately */
        int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                                  ctx->recv_buf, sizeof(ctx->recv_buf),
                                  0, ctx, on_tls_hs_recv);
        if (rc < 0) {
            async_fail(ctx);
        }
    }
}

/**
 * @brief Reactor callback after a TLS handshake output chunk is sent.
 *
 * Arms a receive for the server's next handshake record.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes sent, or negative errno on failure.
 */
static void on_tls_hs_send_complete(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;
    if (result < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "async_connect: TLS hs send failed: %d", result);
        async_fail(ctx);
        return;
    }
    /* Now wait for server's response */
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_tls_hs_recv);
    if (rc < 0) {
        async_fail(ctx);
    }
}

/**
 * @brief Reactor callback after a TLS handshake record is received.
 *
 * Feeds data into the TLS engine and either sends any generated output,
 * requests more input, or completes the handshake and starts the PG startup.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes received, or negative/zero on failure.
 */
static void on_tls_hs_recv(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;
    if (result <= 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "async_connect: TLS hs recv failed: %d", result);
        async_fail(ctx);
        return;
    }

    /* Feed received bytes into TLS handshake */
    keel_tls_feed_handshake_data(ctx->tls_ctx, ctx->recv_buf, (size_t)result);

    keel_tls_hs_result_t hs = keel_tls_handshake_step(ctx->tls_ctx, NULL);

    if (hs == KEEL_TLS_HS_WANT_WRITE || hs == KEEL_TLS_HS_WANT_READ) {
        /* Drain any output bytes to send */
        ssize_t n = keel_tls_get_handshake_data(ctx->tls_ctx,
                                                ctx->tls_hs_buf, sizeof(ctx->tls_hs_buf));
        if (n > 0) {
            int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                                      ctx->tls_hs_buf, (size_t)n,
                                      MSG_NOSIGNAL, ctx, on_tls_hs_send_complete);
            if (rc < 0) {
                async_fail(ctx);
            }
        } else {
            /* Need more input */
            int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                                      ctx->recv_buf, sizeof(ctx->recv_buf),
                                      0, ctx, on_tls_hs_recv);
            if (rc < 0) {
                async_fail(ctx);
            }
        }
        return;
    }

    if (hs == KEEL_TLS_HS_COMPLETE) {
        ctx->tls_hs_active = false;
        /* Try to activate kernel TLS */
        keel_error_t ke = keel_tls_context_activate_ktls(ctx->tls_ctx, ctx->fd);
        if (ke == KEEL_OK) {
            ctx->tls_ktls_active = true;
        }
        /* Proceed with PostgreSQL startup over TLS */
        start_pg_startup(ctx);
        return;
    }

    KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "async_connect: backend TLS handshake failed");
    async_fail(ctx);
}

/* ============================================================================
 * Step 2: StartupMessage sent → recv auth challenge
 * ============================================================================ */

/**
 * @brief Reactor callback after the PostgreSQL StartupMessage is sent.
 *
 * Arms a receive for the server's first authentication message.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes sent, or negative errno on failure.
 */
static void on_startup_sent(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result < 0 || (size_t)result != ctx->send_len) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "async_connect: startup send failed: %d", result);
        async_fail(ctx);
        return;
    }

    /* Queue recv for auth challenge */
    ctx->phase = BE_ASYNC_AUTH_RECV;
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_auth_recv);
    if (rc < 0) {
        async_fail(ctx);
    }
}

/* ============================================================================
 * Step 3: Auth challenge received → start SCRAM or handle AuthOk
 * ============================================================================ */

/**
 * @brief Reactor callback after the server's authentication message is received.
 *
 * Dispatches on the auth type: starts SCRAM-SHA-256, sends a cleartext
 * password, or handles an immediate AuthOk. Parses ErrorResponse and
 * BackendKeyData messages encountered in the same buffer.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes received, or negative/zero on failure.
 */
static void on_auth_recv(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result <= 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "async_connect: auth recv failed: %d", result);
        async_fail(ctx);
        return;
    }

    /* Parse PG messages */
    size_t rcvd = (size_t)result;
    size_t p = 0;

    while (p + 5 <= rcvd) {
        uint8_t tag = ctx->recv_buf[p];
        uint32_t body_len = be32(ctx->recv_buf + p + 1);
        size_t msg_len = 1 + body_len;
        if (p + msg_len > rcvd) break;  /* partial — need more data */

        switch (tag) {
        case 'R': {
            if (body_len < 8) { p += msg_len; break; }
            uint32_t auth = be32(ctx->recv_buf + p + 5);

            if (auth == 0) {
                /* AuthOk */
                ctx->authed = true;
            } else if (auth == 10) {
                /* SCRAM-SHA-256 requested — build client-first */
                const char* user = ctx->pool->config.user;

                char client_nonce_raw[18];
                RAND_bytes((uint8_t*)client_nonce_raw, sizeof(client_nonce_raw));
                b64_encode((const uint8_t*)client_nonce_raw, sizeof(client_nonce_raw),
                           ctx->client_nonce_b64, sizeof(ctx->client_nonce_b64));

                snprintf(ctx->client_first_bare, sizeof(ctx->client_first_bare),
                         "n=%s,r=%s", user, ctx->client_nonce_b64);

                char client_first[256];
                int cf_len = snprintf(client_first, sizeof(client_first),
                                      "n,,%s", ctx->client_first_bare);

                ssize_t msg = pg_sasl_initial_response("SCRAM-SHA-256",
                    client_first, (size_t)cf_len,
                    ctx->send_buf, sizeof(ctx->send_buf));
                if (msg < 0) {
                    async_fail(ctx);
                    return;
                }
                ctx->send_len = (size_t)msg;
                ctx->phase = BE_ASYNC_SCRAM_FIRST_SEND;

                int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                                          ctx->send_buf, ctx->send_len,
                                          MSG_NOSIGNAL,
                                          ctx, on_scram_first_sent);
                if (rc < 0) {
                    async_fail(ctx);
                }
                return;
            } else if (auth == 3) {
                /* Cleartext password */
                const char* password = pool_get_password(ctx->pool);
                if (!password) { async_fail(ctx); return; }
                size_t plen = strlen(password) + 1;
                ctx->send_buf[0] = 'p';
                wr32be(ctx->send_buf + 1, (uint32_t)(4 + plen));
                memcpy(ctx->send_buf + 5, password, plen);
                ctx->send_len = 5 + plen;
                ctx->phase = BE_ASYNC_SCRAM_FIRST_SEND;  /* reuse for password send */

                int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                                          ctx->send_buf, ctx->send_len,
                                          MSG_NOSIGNAL,
                                          ctx, on_scram_first_sent);
                if (rc < 0) { async_fail(ctx); }
                return;
            }
            break;
        }
        case 'E': {
            /* ErrorResponse — parse message */
            char errmsg[256] = "unknown";
            size_t ep = p + 5;
            while (ep < p + msg_len && ctx->recv_buf[ep] != 0) {
                char field_type = (char)ctx->recv_buf[ep];
                ep++;
                const char* val = (const char*)(ctx->recv_buf + ep);
                size_t vl = strnlen(val, (p + msg_len) - ep);
                if (field_type == 'M') {
                    size_t cl = vl < sizeof(errmsg)-1 ? vl : sizeof(errmsg)-1;
                    memcpy(errmsg, val, cl);
                    errmsg[cl] = '\0';
                }
                ep += vl + 1;
            }
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "async_connect: PG error: %s", errmsg);
            async_fail(ctx);
            return;
        }
        case 'Z':
            /* ReadyForQuery — done! */
            if (ctx->authed) {
                async_succeed(ctx);
            } else {
                async_fail(ctx);
            }
            return;
        case 'K':
            /* BackendKeyData — capture PID + secret for cancel forwarding */
            if (body_len >= 12) {
                ctx->pg_backend_pid    = be32(ctx->recv_buf + p + 5);
                ctx->pg_cancel_secret  = be32(ctx->recv_buf + p + 9);
            }
            break;
        default:
            /* S (ParameterStatus) etc. — skip */
            break;
        }
        p += msg_len;
    }

    /* If we got AuthOk but no ReadyForQuery yet, keep draining */
    if (ctx->authed) {
        ctx->phase = BE_ASYNC_DRAIN_RECV;
        int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                                  ctx->recv_buf, sizeof(ctx->recv_buf),
                                  0, ctx, on_drain_recv);
        if (rc < 0) { async_fail(ctx); }
        return;
    }

    /* Need more data */
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_auth_recv);
    if (rc < 0) { async_fail(ctx); }
}

/* ============================================================================
 * Step 4: SCRAM client-first sent → recv server-first
 * ============================================================================ */

/**
 * @brief Reactor callback after the SCRAM client-first message is sent.
 *
 * Arms a receive for the server-first (SASLContinue) message.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes sent, or negative errno on failure.
 */
static void on_scram_first_sent(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result < 0) {
        async_fail(ctx);
        return;
    }

    /* Recv server-first (SASLContinue, auth type 11) */
    ctx->phase = BE_ASYNC_SCRAM_FIRST_RECV;
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_scram_first_recv);
    if (rc < 0) { async_fail(ctx); }
}

/* ============================================================================
 * Step 5: SCRAM server-first received → compute proof, send client-final
 * ============================================================================ */

/**
 * @brief Reactor callback after the SCRAM server-first message is received.
 *
 * Parses the server nonce, salt, and iteration count, derives the
 * SaltedPassword (using a per-pool cache), computes the client proof,
 * and sends the SCRAM client-final message.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes received, or negative/zero on failure.
 */
static void on_scram_first_recv(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result <= 0) {
        async_fail(ctx);
        return;
    }

    size_t rcvd = (size_t)result;

    /* Expect R message with auth type 11 (SASLContinue) */
    if (rcvd < 9 || ctx->recv_buf[0] != 'R') {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH, "async_connect: expected SASLContinue, got tag=%c",
                    ctx->recv_buf[0]);
        async_fail(ctx);
        return;
    }
    if (be32(ctx->recv_buf + 5) != 11) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH, "async_connect: expected auth type 11, got %u",
                    be32(ctx->recv_buf + 5));
        async_fail(ctx);
        return;
    }

    size_t sf_off = 9;
    size_t sf_len = (size_t)(be32(ctx->recv_buf + 1) - 4) - 4;
    if (sf_len >= sizeof(ctx->server_first)) {
        async_fail(ctx);
        return;
    }
    memcpy(ctx->server_first, ctx->recv_buf + sf_off, sf_len);
    ctx->server_first[sf_len] = '\0';

    /* Parse server-first: r=<nonce>, s=<salt>, i=<iterations> */
    char* sr = strstr(ctx->server_first, "r=");
    char* ss = strstr(ctx->server_first, "s=");
    char* si = strstr(ctx->server_first, "i=");
    if (!sr || !ss || !si) {
        async_fail(ctx);
        return;
    }

    char* combined_nonce = sr + 2;
    char* cn_end = strchr(combined_nonce, ',');
    if (cn_end) *cn_end = '\0';

    char* salt_b64 = ss + 2;
    char* salt_end = strchr(salt_b64, ',');
    if (salt_end) *salt_end = '\0';

    int iterations = atoi(si + 2);
    if (iterations < 1) {
        async_fail(ctx);
        return;
    }

    uint8_t salt[64];
    size_t salt_len = b64_decode(salt_b64, strlen(salt_b64), salt, sizeof(salt));
    if (salt_len == 0) {
        async_fail(ctx);
        return;
    }

    /* Restore commas for auth_msg */
    if (cn_end) *cn_end = ',';
    if (salt_end) *salt_end = ',';

    /* Key derivation: use cached SaltedPassword if the pool has already
     * computed Hi(password, salt, iterations) for this exact (salt, iters)
     * pair.  PostgreSQL's per-user salt is fixed, so every connection from
     * this pool hits the cache after the very first SCRAM handshake. */
    const char* password = pool_get_password(ctx->pool);
    uint8_t salted_password[32];
    backend_pool_t* pool = ctx->pool;

    bool cache_hit = pool->scram_cache_valid &&
                     pool->scram_cache_iterations == iterations &&
                     pool->scram_cache_salt_len   == salt_len &&
                     memcmp(pool->scram_cache_salt, salt, salt_len) == 0;

    if (cache_hit) {
        memcpy(salted_password, pool->scram_cache_salted_pw, 32);
    } else {
        if (!password) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL,
                           "SCRAM auth: no password configured for pool %s:%u",
                           pool->config.host, pool->config.port);
            async_fail(ctx);
            return;
        }
        if (!PKCS5_PBKDF2_HMAC(password, (int)strlen(password),
                                salt, (int)salt_len, iterations,
                                EVP_sha256(), 32, salted_password)) {
            async_fail(ctx);
            return;
        }
        /* Populate cache for all future connections from this pool */
        pool->scram_cache_iterations = iterations;
        pool->scram_cache_salt_len   = salt_len;
        memcpy(pool->scram_cache_salt, salt, salt_len);
        memcpy(pool->scram_cache_salted_pw, salted_password, 32);
        pool->scram_cache_valid = true;
    }

    uint8_t client_key[32];
    unsigned int ck_len = 32;
    HMAC(EVP_sha256(), salted_password, 32,
         (const uint8_t*)"Client Key", 10, client_key, &ck_len);

    uint8_t stored_key[32];
    SHA256(client_key, 32, stored_key);

    char cf_no_proof[256];
    snprintf(cf_no_proof, sizeof(cf_no_proof), "c=biws,r=%s", combined_nonce);

    char auth_msg[512];
    int am_len = snprintf(auth_msg, sizeof(auth_msg), "%s,%s,%s",
                          ctx->client_first_bare, ctx->server_first, cf_no_proof);

    uint8_t client_sig[32];
    unsigned int cs_len = 32;
    HMAC(EVP_sha256(), stored_key, 32,
         (const uint8_t*)auth_msg, (size_t)am_len, client_sig, &cs_len);

    uint8_t client_proof[32];
    for (int i = 0; i < 32; i++)
        client_proof[i] = client_key[i] ^ client_sig[i];

    char proof_b64[64];
    b64_encode(client_proof, 32, proof_b64, sizeof(proof_b64));

    /* Build client-final message */
    char client_final[512];
    int cfin_len = snprintf(client_final, sizeof(client_final),
                            "%s,p=%s", cf_no_proof, proof_b64);

    ssize_t msg = pg_sasl_response((const uint8_t*)client_final,
                                   (size_t)cfin_len,
                                   ctx->send_buf, sizeof(ctx->send_buf));
    if (msg < 0) {
        async_fail(ctx);
        return;
    }
    ctx->send_len = (size_t)msg;
    ctx->phase = BE_ASYNC_SCRAM_FINAL_SEND;

    int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                              ctx->send_buf, ctx->send_len,
                              MSG_NOSIGNAL,
                              ctx, on_scram_final_sent);
    if (rc < 0) { async_fail(ctx); }
}

/* ============================================================================
 * Step 6: SCRAM client-final sent → recv server-final
 * ============================================================================ */

/**
 * @brief Reactor callback after the SCRAM client-final message is sent.
 *
 * Arms a receive for the server-final (SASLFinal / AuthOk) message.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes sent, or negative errno on failure.
 */
static void on_scram_final_sent(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result < 0) {
        async_fail(ctx);
        return;
    }

    ctx->phase = BE_ASYNC_SCRAM_FINAL_RECV;
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_scram_final_recv);
    if (rc < 0) { async_fail(ctx); }
}

/* ============================================================================
 * Step 7: SCRAM server-final received → drain until ReadyForQuery
 * ============================================================================ */

/**
 * @brief Reactor callback after the SCRAM server-final message is received.
 *
 * Validates completion (auth type 12 / 0), then drains ParameterStatus and
 * BackendKeyData messages until ReadyForQuery arrives.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes received, or negative/zero on failure.
 */
static void on_scram_final_recv(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result <= 0) {
        async_fail(ctx);
        return;
    }

    size_t rcvd = (size_t)result;

    /* Parse all messages in the buffer */
    size_t p = 0;
    while (p + 5 <= rcvd) {
        uint8_t tag = ctx->recv_buf[p];
        uint32_t body_len = be32(ctx->recv_buf + p + 1);
        size_t msg_len = 1 + body_len;
        if (p + msg_len > rcvd) break;

        switch (tag) {
        case 'R': {
            if (body_len >= 8) {
                uint32_t auth = be32(ctx->recv_buf + p + 5);
                if (auth == 12) {
                    /* SASLFinal — SCRAM complete */
                } else if (auth == 0) {
                    ctx->authed = true;
                }
            }
            break;
        }
        case 'K':
            /* BackendKeyData — capture PID + secret for cancel forwarding */
            if (body_len >= 12) {
                ctx->pg_backend_pid   = be32(ctx->recv_buf + p + 5);
                ctx->pg_cancel_secret = be32(ctx->recv_buf + p + 9);
            }
            break;
        case 'E': {
            char errmsg[256] = "unknown";
            size_t ep = p + 5;
            while (ep < p + msg_len && ctx->recv_buf[ep] != 0) {
                char ft = (char)ctx->recv_buf[ep]; ep++;
                const char* val = (const char*)(ctx->recv_buf + ep);
                size_t vl = strnlen(val, (p + msg_len) - ep);
                if (ft == 'M') {
                    size_t cl = vl < sizeof(errmsg)-1 ? vl : sizeof(errmsg)-1;
                    memcpy(errmsg, val, cl);
                    errmsg[cl] = '\0';
                }
                ep += vl + 1;
            }
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "async_connect: SCRAM error: %s", errmsg);
            async_fail(ctx);
            return;
        }
        case 'Z':
            /* ReadyForQuery — done! */
            ctx->authed = true;
            async_succeed(ctx);
            return;
        default:
            break;
        }
        p += msg_len;
    }

    /* Need more data (ParameterStatus, BackendKeyData, etc.) */
    ctx->phase = BE_ASYNC_DRAIN_RECV;
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_drain_recv);
    if (rc < 0) { async_fail(ctx); }
}

/* ============================================================================
 * Step 8: Drain remaining messages until ReadyForQuery
 * ============================================================================ */

/**
 * @brief Reactor callback that discards post-auth server messages.
 *
 * Consumes ParameterStatus, BackendKeyData, and any other messages that
 * follow authentication until ReadyForQuery ('Z') is received.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes received, or negative/zero on failure.
 */
static void on_drain_recv(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result <= 0) {
        async_fail(ctx);
        return;
    }

    size_t rcvd = (size_t)result;
    size_t p = 0;

    while (p + 5 <= rcvd) {
        uint8_t tag = ctx->recv_buf[p];
        uint32_t body_len = be32(ctx->recv_buf + p + 1);
        size_t msg_len = 1 + body_len;
        if (p + msg_len > rcvd) break;

        switch (tag) {
        case 'R':
            if (body_len >= 8 && be32(ctx->recv_buf + p + 5) == 0)
                ctx->authed = true;
            break;
        case 'K':
            /* BackendKeyData — capture PID + secret for cancel forwarding */
            if (body_len >= 12) {
                ctx->pg_backend_pid   = be32(ctx->recv_buf + p + 5);
                ctx->pg_cancel_secret = be32(ctx->recv_buf + p + 9);
            }
            break;
        case 'E':
            async_fail(ctx);
            return;
        case 'Z':
            if (ctx->authed) {
                async_succeed(ctx);
            } else {
                async_fail(ctx);
            }
            return;
        default:
            break;
        }
        p += msg_len;
    }

    /* Still draining — more data needed */
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_drain_recv);
    if (rc < 0) { async_fail(ctx); }
}

/* ============================================================================
 * MySQL Async Auth — Wire Helpers
 * ============================================================================ */

#define MY_HDR  4               /* 3-byte LE length + 1-byte seq_id */
#define MY_OK   0x00
#define MY_ERR  0xFF
#define MY_AUTH_SWITCH    0xFE  /* AuthSwitchRequest */
#define MY_AUTH_MORE_DATA 0x01  /* AuthMoreData (caching_sha2) */

#define MY_CAP_LONG_PASSWORD     (1U <<  0)
#define MY_CAP_FOUND_ROWS        (1U <<  1)
#define MY_CAP_LONG_FLAG         (1U <<  2)
#define MY_CAP_CONNECT_WITH_DB   (1U <<  3)
#define MY_CAP_IGNORE_SPACE      (1U <<  8)
#define MY_CAP_PROTOCOL_41       (1U <<  9)
#define MY_CAP_TRANSACTIONS      (1U << 13)
#define MY_CAP_SECURE_CONNECTION (1U << 15)
#define MY_CAP_MULTI_STATEMENTS  (1U << 16)
#define MY_CAP_MULTI_RESULTS     (1U << 17)
#define MY_CAP_PS_MULTI_RESULTS  (1U << 18)
#define MY_CAP_PLUGIN_AUTH       (1U << 19)

/**
 * @brief Read a 2-byte little-endian unsigned integer.
 *
 * @param p Pointer to at least 2 bytes of little-endian data.
 * @return Decoded 16-bit value.
 */
static inline uint16_t my_rdle16(const uint8_t* p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

/**
 * @brief Read a 3-byte little-endian unsigned integer.
 *
 * @param p Pointer to at least 3 bytes of little-endian data.
 * @return Decoded 24-bit value.
 */
static inline uint32_t my_rdle24(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16);
}

/**
 * @brief Write a 24-bit value into a byte buffer in little-endian order.
 *
 * @param p Destination buffer (must hold at least 3 bytes).
 * @param v Value to encode (only the low 24 bits are used).
 */
static inline void my_wrle24(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
}

/* mysql_native_password: SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password))) */
/**
 * @brief Compute a MySQL native-password authentication response.
 *
 * Implements SHA1(password) XOR SHA1(scramble || SHA1(SHA1(password))).
 *
 * @param password    Plaintext password string.
 * @param scramble    Server-provided scramble bytes.
 * @param scramble_len Number of scramble bytes (typically 20).
 * @param out         Output buffer; must hold SHA_DIGEST_LENGTH (20) bytes.
 */
static void my_scramble_native(const char* password,
                               const uint8_t* scramble, size_t scramble_len,
                               uint8_t* out)
{
    uint8_t stage1[SHA_DIGEST_LENGTH];
    SHA1((const uint8_t*)password, strlen(password), stage1);

    uint8_t stage2[SHA_DIGEST_LENGTH];
    SHA1(stage1, SHA_DIGEST_LENGTH, stage2);

    EVP_MD_CTX* md = EVP_MD_CTX_new();
    uint8_t digest[SHA_DIGEST_LENGTH];
    unsigned int dlen = SHA_DIGEST_LENGTH;
    EVP_DigestInit_ex(md, EVP_sha1(), NULL);
    EVP_DigestUpdate(md, scramble, scramble_len);
    EVP_DigestUpdate(md, stage2, SHA_DIGEST_LENGTH);
    EVP_DigestFinal_ex(md, digest, &dlen);
    EVP_MD_CTX_free(md);

    for (int i = 0; i < SHA_DIGEST_LENGTH; i++)
        out[i] = stage1[i] ^ digest[i];
}

/* caching_sha2_password: SHA256(password) XOR SHA256(SHA256(SHA256(password)) + scramble) */
/**
 * @brief Compute a MySQL caching_sha2_password authentication response.
 *
 * Implements SHA256(password) XOR SHA256(SHA256(SHA256(password)) || scramble).
 *
 * @param password    Plaintext password string.
 * @param scramble    Server-provided scramble bytes.
 * @param scramble_len Number of scramble bytes (typically 20).
 * @param out         Output buffer; must hold SHA256_DIGEST_LENGTH (32) bytes.
 */
static void my_scramble_caching_sha2(const char* password,
                                     const uint8_t* scramble, size_t scramble_len,
                                     uint8_t* out)
{
    uint8_t stage1[SHA256_DIGEST_LENGTH];
    SHA256((const uint8_t*)password, strlen(password), stage1);

    uint8_t stage2[SHA256_DIGEST_LENGTH];
    SHA256(stage1, SHA256_DIGEST_LENGTH, stage2);

    EVP_MD_CTX* md = EVP_MD_CTX_new();
    uint8_t digest[SHA256_DIGEST_LENGTH];
    unsigned int dlen = SHA256_DIGEST_LENGTH;
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);
    EVP_DigestUpdate(md, stage2, SHA256_DIGEST_LENGTH);
    EVP_DigestUpdate(md, scramble, scramble_len);
    EVP_DigestFinal_ex(md, digest, &dlen);
    EVP_MD_CTX_free(md);

    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
        out[i] = stage1[i] ^ digest[i];
}

/**
 * @brief Parse MySQL Initial Handshake (protocol v10)
 *
 * Extracts: scramble, server_caps, auth plugin name, seq_id
 */
static int my_parse_greeting(const uint8_t* data, size_t len,
                             backend_async_ctx_t* ctx)
{
    if (len < MY_HDR + 1) return -1;

    uint32_t payload_len = my_rdle24(data);
    ctx->my_seq = data[3];
    const uint8_t* payload = data + MY_HDR;
    size_t plen = payload_len;

    if (plen < 1 || payload[0] != 10) {
        if (payload[0] == MY_ERR && plen >= 3) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "mysql_async: server sent error %u during handshake",
                my_rdle16(payload + 1));
        }
        return -1;
    }

    size_t pos = 1;
    /* Skip server version string */
    size_t vlen = strnlen((const char*)(payload + pos), plen - pos);
    pos += vlen + 1;

    if (pos + 4 > plen) return -1;
    ctx->my_connection_id = (uint32_t)payload[pos]
        | ((uint32_t)payload[pos+1] << 8)
        | ((uint32_t)payload[pos+2] << 16)
        | ((uint32_t)payload[pos+3] << 24);
    pos += 4; /* connection ID */

    /* Scramble part 1 (8 bytes) */
    if (pos + 8 > plen) return -1;
    memcpy(ctx->my_scramble, payload + pos, 8);
    pos += 8;

    if (pos + 1 > plen) return -1;
    pos += 1; /* filler */

    if (pos + 2 > plen) return -1;
    ctx->my_server_caps = my_rdle16(payload + pos);
    pos += 2;

    if (pos >= plen) {
        ctx->my_scramble_len = 8;
        strcpy(ctx->my_plugin, "mysql_native_password");
        return 0;
    }

    pos += 1; /* character set */
    if (pos + 2 > plen) return -1;
    pos += 2; /* status flags */
    if (pos + 2 > plen) return -1;
    ctx->my_server_caps |= ((uint32_t)my_rdle16(payload + pos)) << 16;
    pos += 2;

    uint8_t auth_data_len = 0;
    if (pos < plen) auth_data_len = payload[pos];
    pos += 1;

    if (pos + 10 > plen) return -1;
    pos += 10; /* reserved */

    /* Scramble part 2 */
    if (ctx->my_server_caps & MY_CAP_SECURE_CONNECTION) {
        size_t part2_len = (auth_data_len > 8) ? (size_t)(auth_data_len - 8) : 13;
        if (pos + part2_len > plen) part2_len = plen - pos;
        size_t copy_len = (part2_len > 12) ? 12 : part2_len;
        memcpy(ctx->my_scramble + 8, payload + pos, copy_len);
        ctx->my_scramble_len = 8 + copy_len;
        pos += part2_len;
    } else {
        ctx->my_scramble_len = 8;
    }

    /* Auth plugin name */
    ctx->my_plugin[0] = '\0';
    if (ctx->my_server_caps & MY_CAP_PLUGIN_AUTH) {
        if (pos < plen) {
            size_t nlen = strnlen((const char*)(payload + pos), plen - pos);
            if (nlen < sizeof(ctx->my_plugin)) {
                memcpy(ctx->my_plugin, payload + pos, nlen);
                ctx->my_plugin[nlen] = '\0';
            }
        }
    }
    if (ctx->my_plugin[0] == '\0')
        strcpy(ctx->my_plugin, "mysql_native_password");

    return 0;
}

/**
 * @brief Build MySQL Handshake Response packet into ctx->send_buf
 */
static ssize_t my_build_response(backend_async_ctx_t* ctx)
{
    const char* user = ctx->pool->config.user;
    const char* database = ctx->pool->config.database;
    const char* password = pool_get_password(ctx->pool);

    uint8_t payload[1024];
    size_t pos = 0;

    /* Compute auth data */
    uint8_t auth_data[32];
    size_t auth_data_len = 0;

    if (password && password[0]) {
        if (strcmp(ctx->my_plugin, "caching_sha2_password") == 0) {
            my_scramble_caching_sha2(password, ctx->my_scramble,
                                     ctx->my_scramble_len, auth_data);
            auth_data_len = 32;
        } else {
            my_scramble_native(password, ctx->my_scramble,
                               ctx->my_scramble_len, auth_data);
            auth_data_len = 20;
        }
    }

    /* Capability flags */
    uint32_t caps = MY_CAP_LONG_PASSWORD | MY_CAP_FOUND_ROWS |
                    MY_CAP_LONG_FLAG | MY_CAP_IGNORE_SPACE |
                    MY_CAP_PROTOCOL_41 | MY_CAP_TRANSACTIONS |
                    MY_CAP_SECURE_CONNECTION | MY_CAP_MULTI_STATEMENTS |
                    MY_CAP_MULTI_RESULTS | MY_CAP_PS_MULTI_RESULTS |
                    MY_CAP_PLUGIN_AUTH;
    if (database && database[0])
        caps |= MY_CAP_CONNECT_WITH_DB;
    caps &= ctx->my_server_caps;
    caps |= MY_CAP_PROTOCOL_41 | MY_CAP_SECURE_CONNECTION;

    /* capability_flags (4 bytes LE) */
    payload[pos++] = (uint8_t)(caps);
    payload[pos++] = (uint8_t)(caps >> 8);
    payload[pos++] = (uint8_t)(caps >> 16);
    payload[pos++] = (uint8_t)(caps >> 24);

    /* max_packet_size = 16MB */
    payload[pos++] = 0xFF; payload[pos++] = 0xFF;
    payload[pos++] = 0xFF; payload[pos++] = 0x00;

    /* character_set = utf8mb4 */
    payload[pos++] = 0x2d;

    /* reserved (23 zeros) */
    memset(payload + pos, 0, 23); pos += 23;

    /* username */
    size_t ulen = strlen(user);
    if (pos + ulen + 1 > sizeof(payload)) return -1;
    memcpy(payload + pos, user, ulen); pos += ulen;
    payload[pos++] = 0;

    /* auth_response (length-prefixed) */
    payload[pos++] = (uint8_t)auth_data_len;
    if (auth_data_len > 0) {
        memcpy(payload + pos, auth_data, auth_data_len);
        pos += auth_data_len;
    }

    /* database */
    if (caps & MY_CAP_CONNECT_WITH_DB) {
        size_t dlen = strlen(database);
        if (pos + dlen + 1 > sizeof(payload)) return -1;
        memcpy(payload + pos, database, dlen); pos += dlen;
        payload[pos++] = 0;
    }

    /* auth_plugin_name */
    if (caps & MY_CAP_PLUGIN_AUTH) {
        size_t pnlen = strlen(ctx->my_plugin);
        if (pos + pnlen + 1 > sizeof(payload)) return -1;
        memcpy(payload + pos, ctx->my_plugin, pnlen); pos += pnlen;
        payload[pos++] = 0;
    }

    /* Build MySQL packet with header */
    size_t total = MY_HDR + pos;
    if (total > sizeof(ctx->send_buf)) return -1;

    my_wrle24(ctx->send_buf, (uint32_t)pos);
    ctx->send_buf[3] = ctx->my_seq + 1;
    ctx->my_seq++;
    memcpy(ctx->send_buf + MY_HDR, payload, pos);

    return (ssize_t)total;
}

/* ============================================================================
 * MySQL Async Auth — Step 1: Greeting received
 * ============================================================================ */

/**
 * @brief Reactor callback after the MySQL server greeting is received.
 *
 * Parses the handshake packet, builds the HandshakeResponse, and sends it.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes received, or negative/zero on failure.
 */
static void on_my_greeting_recv(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result <= 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "mysql_async: greeting recv failed: %d", result);
        async_fail(ctx);
        return;
    }

    if (my_parse_greeting(ctx->recv_buf, (size_t)result, ctx) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "mysql_async: failed to parse greeting from %s:%u",
            ctx->pool->config.host, ctx->pool->config.port);
        async_fail(ctx);
        return;
    }

    /* Build and send handshake response */
    ssize_t len = my_build_response(ctx);
    if (len < 0) {
        async_fail(ctx);
        return;
    }
    ctx->send_len = (size_t)len;
    ctx->phase = BE_ASYNC_MY_RESPONSE_SEND;

    int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                              ctx->send_buf, ctx->send_len,
                              MSG_NOSIGNAL, ctx, on_my_response_sent);
    if (rc < 0) { async_fail(ctx); }
}

/* ============================================================================
 * MySQL Async Auth — Step 2: Handshake response sent → recv auth result
 * ============================================================================ */

/**
 * @brief Reactor callback after the MySQL HandshakeResponse packet is sent.
 *
 * Arms a receive for the server's first authentication result packet.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes sent, or negative errno on failure.
 */
static void on_my_response_sent(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "mysql_async: handshake response send failed: %d", result);
        async_fail(ctx);
        return;
    }

    ctx->phase = BE_ASYNC_MY_AUTH_RECV;
    ctx->recv_have = 0;
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_my_auth_recv);
    if (rc < 0) { async_fail(ctx); }
}

/* ============================================================================
 * MySQL Async Auth — Step 3: Auth result received (OK/ERR/Switch/MoreData)
 *
 * This handler may be called multiple times (after auth switch, after RSA).
 * TCP can coalesce MySQL packets, so we handle leftover data in recv_buf.
 * ============================================================================ */

/* Forward declaration — needed for mutual recursion with coalesced packets */
static void my_auth_parse(backend_async_ctx_t* ctx);

/**
 * @brief Reactor callback after MySQL auth data is received.
 *
 * Accumulates bytes into @c recv_buf and delegates parsing to
 * my_auth_parse() once data is available.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes received, or negative/zero on failure.
 */
static void on_my_auth_recv(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result <= 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "mysql_async: auth recv failed: %d (round=%d)",
            result, ctx->my_auth_rounds);
        async_fail(ctx);
        return;
    }

    ctx->recv_have += (size_t)result;
    my_auth_parse(ctx);
}

/**
 * @brief Parse buffered MySQL auth packets.
 *
 * Separated from the I/O callback so that the coalesced-OK reprocessing
 * path (fast-auth 0x03) can re-enter the parser without faking a recv result.
 */
static void my_auth_parse(backend_async_ctx_t* ctx)
{
    size_t rcvd = ctx->recv_have;

    /* Ensure we have a complete MySQL packet header */
    if (rcvd < MY_HDR) {
        /* Need more data */
        int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                                  ctx->recv_buf + rcvd,
                                  sizeof(ctx->recv_buf) - rcvd,
                                  0, ctx, on_my_auth_recv);
        if (rc < 0) { async_fail(ctx); }
        return;
    }

    /* Check if we have the full packet */
    uint32_t pkt_len = my_rdle24(ctx->recv_buf);
    if (rcvd < MY_HDR + pkt_len) {
        /* Need more data for complete packet */
        int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                                  ctx->recv_buf + rcvd,
                                  sizeof(ctx->recv_buf) - rcvd,
                                  0, ctx, on_my_auth_recv);
        if (rc < 0) { async_fail(ctx); }
        return;
    }

    ctx->my_seq = ctx->recv_buf[3];
    const uint8_t* payload = ctx->recv_buf + MY_HDR;
    size_t plen = pkt_len;

    if (plen == 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH, "mysql_async: empty auth packet");
        async_fail(ctx);
        return;
    }

    uint8_t marker = payload[0];

    /* === OK → auth success === */
    if (marker == MY_OK) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_CONN,
            "mysql_async: auth successful for %s:%u",
            ctx->pool->config.host, ctx->pool->config.port);
        async_succeed(ctx);
        return;
    }

    /* === ERR → auth failed === */
    if (marker == MY_ERR) {
        if (plen >= 3) {
            uint16_t errcode = my_rdle16(payload + 1);
            const char* errmsg = "";
            if (plen > 9) errmsg = (const char*)(payload + 9);
            else if (plen > 3) errmsg = (const char*)(payload + 3);
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "mysql_async: server error %u: %.*s",
                errcode, (int)(plen > 9 ? plen - 9 : plen - 3), errmsg);
        }
        async_fail(ctx);
        return;
    }

    if (++ctx->my_auth_rounds > 10) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH, "mysql_async: too many auth rounds");
        async_fail(ctx);
        return;
    }

    /* === AuthSwitchRequest (0xFE) → re-scramble with new plugin === */
    if (marker == MY_AUTH_SWITCH && plen > 1) {
        const char* password = pool_get_password(ctx->pool);
        size_t pos = 1;
        const char* new_plugin = (const char*)(payload + pos);
        size_t nlen = strnlen(new_plugin, plen - pos);
        pos += nlen + 1;

        const uint8_t* new_scramble = payload + pos;
        size_t new_scramble_len = plen - pos;
        if (new_scramble_len > 0 && new_scramble[new_scramble_len - 1] == 0)
            new_scramble_len--;

        /* Update plugin and scramble for potential future rounds */
        if (nlen < sizeof(ctx->my_plugin)) {
            memcpy(ctx->my_plugin, new_plugin, nlen);
            ctx->my_plugin[nlen] = '\0';
        }
        if (new_scramble_len <= 20) {
            memcpy(ctx->my_scramble, new_scramble, new_scramble_len);
            ctx->my_scramble_len = new_scramble_len;
        }

        uint8_t auth_data[32];
        size_t auth_data_len = 0;

        if (strcmp(ctx->my_plugin, "caching_sha2_password") == 0) {
            my_scramble_caching_sha2(password, new_scramble, new_scramble_len,
                                     auth_data);
            auth_data_len = 32;
        } else {
            my_scramble_native(password, new_scramble, new_scramble_len,
                               auth_data);
            auth_data_len = 20;
        }

        /* Build auth response packet */
        my_wrle24(ctx->send_buf, (uint32_t)auth_data_len);
        ctx->send_buf[3] = ctx->my_seq + 1;
        ctx->my_seq++;
        memcpy(ctx->send_buf + MY_HDR, auth_data, auth_data_len);
        ctx->send_len = MY_HDR + auth_data_len;
        ctx->phase = BE_ASYNC_MY_AUTH_SEND;

        int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                                  ctx->send_buf, ctx->send_len,
                                  MSG_NOSIGNAL, ctx, on_my_auth_data_sent);
        if (rc < 0) { async_fail(ctx); }
        return;
    }

    /* === AuthMoreData (0x01) — caching_sha2_password continuation === */
    if (marker == MY_AUTH_MORE_DATA && plen >= 2) {
        uint8_t status = payload[1];

        if (status == 0x03) {
            /* Fast auth success — read final OK from remaining data or next recv.
             * Check if coalesced OK follows in the same buffer. */
            size_t consumed = MY_HDR + pkt_len;
            if (consumed < rcvd) {
                /* Shift leftover to front and re-parse (no new I/O needed) */
                size_t leftover = rcvd - consumed;
                memmove(ctx->recv_buf, ctx->recv_buf + consumed, leftover);
                ctx->recv_have = leftover;
                my_auth_parse(ctx);
                return;
            }
            /* Need to recv the OK packet */
            ctx->recv_have = 0;
            int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                                      ctx->recv_buf, sizeof(ctx->recv_buf),
                                      0, ctx, on_my_auth_recv);
            if (rc < 0) { async_fail(ctx); }
            return;
        }

        if (status == 0x04) {
            /* Full auth needed — request RSA public key (send byte 0x02) */
            my_wrle24(ctx->send_buf, 1);
            ctx->send_buf[3] = ctx->my_seq + 1;
            ctx->my_seq++;
            ctx->send_buf[MY_HDR] = 0x02;
            ctx->send_len = MY_HDR + 1;
            ctx->phase = BE_ASYNC_MY_AUTH_SEND;

            int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                                      ctx->send_buf, ctx->send_len,
                                      MSG_NOSIGNAL, ctx, on_my_rsa_key_recv_trigger);
            if (rc < 0) { async_fail(ctx); }
            return;
        }

        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
            "mysql_async: unexpected caching_sha2 status: 0x%02x", status);
        async_fail(ctx);
        return;
    }

    KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
        "mysql_async: unexpected marker 0x%02x len=%zu", marker, plen);
    async_fail(ctx);
}

/* ============================================================================
 * MySQL Async Auth — Step 4: Auth data sent → recv next auth result
 * ============================================================================ */

/**
 * @brief Reactor callback after a MySQL auth data packet is sent.
 *
 * Arms a receive for the next server auth response (may loop multiple times
 * for plugin switches or caching_sha2_password full-auth).
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes sent, or negative errno on failure.
 */
static void on_my_auth_data_sent(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result < 0) {
        async_fail(ctx);
        return;
    }

    /* Recv next auth response (loops back to on_my_auth_recv) */
    ctx->phase = BE_ASYNC_MY_AUTH_RECV;
    ctx->recv_have = 0;
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_my_auth_recv);
    if (rc < 0) { async_fail(ctx); }
}

/* ============================================================================
 * MySQL Async Auth — RSA key request sent → recv RSA public key
 * ============================================================================ */

/**
 * @brief Reactor callback after the RSA public-key request byte (0x02) is sent.
 *
 * Arms a receive for the server's AuthMoreData packet containing the PEM
 * public key.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes sent, or negative errno on failure.
 */
static void on_my_rsa_key_recv_trigger(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result < 0) {
        async_fail(ctx);
        return;
    }

    /* Now recv the RSA public key response */
    ctx->phase = BE_ASYNC_MY_RSA_KEY_RECV;
    ctx->recv_have = 0;
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_my_rsa_key_recv);
    if (rc < 0) { async_fail(ctx); }
}

/**
 * @brief Reactor callback that accumulates and processes the RSA public key.
 *
 * Reads the complete AuthMoreData packet containing the PEM key, XORs the
 * password with the scramble, RSA-OAEP encrypts it, and sends the result.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes received in this call, or negative/zero on failure.
 */
static void on_my_rsa_key_recv(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result <= 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "mysql_async: RSA key recv failed: %d", result);
        async_fail(ctx);
        return;
    }

    size_t rcvd = ctx->recv_have + (size_t)result;
    ctx->recv_have = rcvd;

    if (rcvd < MY_HDR) {
        int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                                  ctx->recv_buf + rcvd,
                                  sizeof(ctx->recv_buf) - rcvd,
                                  0, ctx, on_my_rsa_key_recv);
        if (rc < 0) { async_fail(ctx); }
        return;
    }

    uint32_t pkt_len = my_rdle24(ctx->recv_buf);
    if (rcvd < MY_HDR + pkt_len) {
        int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                                  ctx->recv_buf + rcvd,
                                  sizeof(ctx->recv_buf) - rcvd,
                                  0, ctx, on_my_rsa_key_recv);
        if (rc < 0) { async_fail(ctx); }
        return;
    }

    ctx->my_seq = ctx->recv_buf[3];
    const uint8_t* payload = ctx->recv_buf + MY_HDR;

    if (payload[0] == MY_ERR) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
            "mysql_async: server refused RSA key request");
        async_fail(ctx);
        return;
    }

    if (payload[0] != MY_AUTH_MORE_DATA) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
            "mysql_async: unexpected RSA response (0x%02x)", payload[0]);
        async_fail(ctx);
        return;
    }

    /* PEM key after the 0x01 marker */
    const char* pem_start = (const char*)(payload + 1);
    size_t pem_len = pkt_len - 1;

    /* Parse PEM → EVP_PKEY */
    BIO* bio = BIO_new_mem_buf(pem_start, (int)pem_len);
    if (!bio) { async_fail(ctx); return; }

    EVP_PKEY* pkey = PEM_read_bio_PUBKEY(bio, NULL, NULL, NULL);
    BIO_free(bio);
    if (!pkey) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
            "mysql_async: failed to parse RSA public key");
        async_fail(ctx);
        return;
    }

    /* XOR password+NUL with scramble */
    const char* password = pool_get_password(ctx->pool);
    size_t pwlen = strlen(password) + 1;
    uint8_t xor_buf[256];
    if (pwlen > sizeof(xor_buf)) {
        EVP_PKEY_free(pkey);
        async_fail(ctx);
        return;
    }
    memcpy(xor_buf, password, pwlen);
    for (size_t i = 0; i < pwlen; i++)
        xor_buf[i] ^= ctx->my_scramble[i % ctx->my_scramble_len];

    /* RSA-OAEP encrypt */
    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new(pkey, NULL);
    EVP_PKEY_free(pkey);
    if (!pctx) { async_fail(ctx); return; }

    if (EVP_PKEY_encrypt_init(pctx) <= 0 ||
        EVP_PKEY_CTX_set_rsa_padding(pctx, RSA_PKCS1_OAEP_PADDING) <= 0) {
        EVP_PKEY_CTX_free(pctx);
        async_fail(ctx);
        return;
    }

    size_t enc_len = 0;
    if (EVP_PKEY_encrypt(pctx, NULL, &enc_len, xor_buf, pwlen) <= 0) {
        EVP_PKEY_CTX_free(pctx);
        async_fail(ctx);
        return;
    }

    /* Build the encrypted password directly into send_buf after MY_HDR */
    if (MY_HDR + enc_len > sizeof(ctx->send_buf)) {
        EVP_PKEY_CTX_free(pctx);
        async_fail(ctx);
        return;
    }

    if (EVP_PKEY_encrypt(pctx, ctx->send_buf + MY_HDR, &enc_len,
                         xor_buf, pwlen) <= 0) {
        EVP_PKEY_CTX_free(pctx);
        async_fail(ctx);
        return;
    }
    EVP_PKEY_CTX_free(pctx);

    /* MySQL packet header */
    my_wrle24(ctx->send_buf, (uint32_t)enc_len);
    ctx->send_buf[3] = ctx->my_seq + 1;
    ctx->my_seq++;
    ctx->send_len = MY_HDR + enc_len;
    ctx->phase = BE_ASYNC_MY_RSA_PW_SEND;

    int rc = keel_reactor_send(ctx->reactor, ctx->fd,
                              ctx->send_buf, ctx->send_len,
                              MSG_NOSIGNAL, ctx, on_my_rsa_pw_sent);
    if (rc < 0) { async_fail(ctx); }
}

/* ============================================================================
 * MySQL Async Auth — RSA password sent → recv final OK
 * ============================================================================ */

/**
 * @brief Reactor callback after the RSA-encrypted password packet is sent.
 *
 * Arms a final receive for the server's OK or ERR packet to conclude the
 * caching_sha2_password full-authentication flow.
 *
 * @param userdata Pointer to the owning ::backend_async_ctx_t.
 * @param result   Bytes sent, or negative errno on failure.
 */
static void on_my_rsa_pw_sent(void* userdata, int result)
{
    backend_async_ctx_t* ctx = (backend_async_ctx_t*)userdata;

    if (result < 0) {
        async_fail(ctx);
        return;
    }

    /* Recv final OK/ERR (loops to on_my_auth_recv) */
    ctx->phase = BE_ASYNC_MY_AUTH_RECV;
    ctx->recv_have = 0;
    int rc = keel_reactor_recv(ctx->reactor, ctx->fd,
                              ctx->recv_buf, sizeof(ctx->recv_buf),
                              0, ctx, on_my_auth_recv);
    if (rc < 0) { async_fail(ctx); }
}