/**
 * @file frontend_tls_async.c
 * @brief Frontend-side TLS handshake bootstrap for accepted client sockets.
 *
 * The worker accept path uses this helper to bridge from a raw non-blocking
 * socket into a live TLS session. The implementation drives the TLS engine with
 * repeated handshake steps, flushing outbound handshake bytes to the client and
 * feeding inbound bytes back into the TLS context until the handshake either
 * completes, times out, or fails.
 *
 * The current design intentionally favors clarity over a fully decomposed
 * reactor-driven micro-state machine: callback handlers may perform a bounded
 * amount of synchronous handshake work before yielding. That keeps the control
 * flow readable while still avoiding a blocking top-level accept path.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/protocol/tls_context.h"
#include "keel/reactor/reactor.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel/util/util.h"
#include "keel_error.h"

#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <fcntl.h>

/* ============================================================================
 * TLS Handshake Tracker (per-connection state)
 * ============================================================================ */

typedef struct frontend_tls_state {
    keel_tls_context_t*     tls_ctx;        /**< TLS context (from phase 2.1) */
    int                     client_fd;      /**< Client socket */
    size_t                  handshake_start_ms; /**< When handshake started */
    size_t                  timeout_ms;     /**< Handshake timeout */
    bool                    ktls_attempted; /**< Whether kTLS was attempted */
    void*                   userdata;       /**< Callback userdata (session) */
    void (*on_complete)(void* userdata, int result); /**< Completion callback */
} frontend_tls_state_t;

/* Forward declarations */
static void on_tls_handshake_send(void* userdata, int result);
static void on_tls_handshake_recv(void* userdata, int result);

/* ============================================================================
 * Helper: Get current time in milliseconds
 * ============================================================================ */

static inline uint64_t get_time_ms(void) { return keel_time_now_ms(); }

/* ============================================================================
 * Core handshake loop: Drive TLS state machine until completion or error
 * ============================================================================ */

/**
 * @brief Drive the TLS state machine until it needs external socket progress.
 *
 * The helper optionally reads already-available client data first, then loops
 * on `keel_tls_handshake_step()` until the handshake completes, fails, or needs
 * another readable/writable socket event. Returning `0` therefore means
 * "handshake still in progress", not success.
 *
 * @param tls_state Per-connection handshake state.
 * @param feed_first Whether to pull pending socket data into the TLS context
 *        before stepping the state machine.
 * @return `1` on completed handshake, `0` when more socket progress is needed,
 *         or a negative errno-style code on failure.
 */
static int tls_handshake_loop(frontend_tls_state_t* tls_state, bool feed_first)
{
    int client_fd = tls_state->client_fd;
    keel_tls_context_t* tls_ctx = tls_state->tls_ctx;
    uint8_t buf[4096];
    ssize_t n;

    /* If caller has data to feed, do that first */
    if (feed_first) {
        n = recv(client_fd, buf, sizeof(buf), MSG_DONTWAIT);
        if (n > 0) {
            if (keel_tls_feed_handshake_data(tls_ctx, buf, (size_t)n) != KEEL_OK) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "Failed to feed handshake data (fd %d)", client_fd);
                return -EIO;
            }
        } else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "recv failed (fd %d): %s", client_fd, strerror(errno));
            return -errno;
        }
    }

    /* Main handshake loop */
    while (1) {
        keel_tls_hs_result_t hs_result = keel_tls_handshake_step(tls_ctx, NULL);
        
        if (hs_result == KEEL_TLS_HS_COMPLETE) {
            /* Handshake done! */
            KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "TLS handshake complete (fd %d)", client_fd);
            return 1; /* Success */
        }

        if (hs_result == KEEL_TLS_HS_ERROR) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "TLS handshake failed (fd %d)", client_fd);
            return -1; /* Error */
        }

        if (hs_result == KEEL_TLS_HS_WANT_WRITE) {
            /* Get data to send */
            ssize_t to_send = keel_tls_get_handshake_data(tls_ctx, buf, sizeof(buf));
            if (to_send > 0) {
                n = send(client_fd, buf, (size_t)to_send, MSG_DONTWAIT);
                if (n < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "send failed (fd %d): %s", 
                                     client_fd, strerror(errno));
                        return -errno;
                    }
                    /* Can't send yet, return 0 for WANT_WRITE */
                    return 0;
                }
                KEEL_LOG_DEBUG(KEEL_LOG_CAT_TLS, "Sent %zd bytes of TLS data (fd %d)", 
                              n, client_fd);
            }
            return 0; /* Need to wait for more events */
        }

        if (hs_result == KEEL_TLS_HS_WANT_READ) {
            /* Need to read more data */
            return 0; /* Return control, wait for socket readable */
        }
    }
}

/* ============================================================================
 * Send-to-client callback: flush encrypted data from TLS bio to socket
 * ============================================================================ */

/**
 * @brief Continue or finish a handshake after outbound TLS bytes were sent.
 *
 * @param userdata Frontend TLS state.
 * @param result Reactor completion result.
 * @return
 */
static void on_tls_handshake_send(void* userdata, int result)
{
    frontend_tls_state_t* tls_state = (frontend_tls_state_t*)userdata;
    int client_fd = tls_state->client_fd;

    if (result < 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "TLS send failed (fd %d): %s",
                     client_fd, strerror(-result));
        tls_state->on_complete(tls_state->userdata, result);
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        return;
    }

    /* Check timeout */
    uint64_t now_ms = get_time_ms();
    if (now_ms - tls_state->handshake_start_ms > tls_state->timeout_ms) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "TLS handshake timeout (fd %d)", client_fd);
        tls_state->on_complete(tls_state->userdata, -ETIMEDOUT);
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        return;
    }

    /* Continue handshake */
    int hs_rc = tls_handshake_loop(tls_state, false);
    
    if (hs_rc > 0) {
        /* Handshake complete! Try kTLS activation */
        if (keel_tls_context_activate_ktls(tls_state->tls_ctx, client_fd) == KEEL_OK) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "Kernel TLS activated for client fd %d", client_fd);
        }
        tls_state->on_complete(tls_state->userdata, 0);
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        return;
    }

    if (hs_rc < 0) {
        tls_state->on_complete(tls_state->userdata, hs_rc);
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        return;
    }

    /* hs_rc == 0: continue waiting */
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_TLS, "TLS handshake in progress (fd %d)", client_fd);
}

/* ============================================================================
 * Recv-from-client callback: process client's TLS data
 * ============================================================================ */

/**
 * @brief Continue or finish a handshake after client bytes were received.
 *
 * @param userdata Frontend TLS state.
 * @param result Reactor completion result.
 * @return
 */
static void on_tls_handshake_recv(void* userdata, int result)
{
    frontend_tls_state_t* tls_state = (frontend_tls_state_t*)userdata;
    int client_fd = tls_state->client_fd;

    if (result < 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "TLS recv failed (fd %d): %s",
                     client_fd, strerror(-result));
        tls_state->on_complete(tls_state->userdata, result);
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        return;
    }

    if (result == 0) {
        /* Client closed connection during handshake */
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "Client closed during TLS handshake (fd %d)", client_fd);
        tls_state->on_complete(tls_state->userdata, -ECONNRESET);
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        return;
    }

    /* Check timeout */
    uint64_t now_ms = get_time_ms();
    if (now_ms - tls_state->handshake_start_ms > tls_state->timeout_ms) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS, "TLS handshake timeout (fd %d)", client_fd);
        tls_state->on_complete(tls_state->userdata, -ETIMEDOUT);
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        return;
    }

    /* Continue handshake, feeding data from socket */
    int hs_rc = tls_handshake_loop(tls_state, true);
    
    if (hs_rc > 0) {
        /* Handshake complete! */
        if (keel_tls_context_activate_ktls(tls_state->tls_ctx, client_fd) == KEEL_OK) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "Kernel TLS activated for client fd %d", client_fd);
        }
        tls_state->on_complete(tls_state->userdata, 0);
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        return;
    }

    if (hs_rc < 0) {
        tls_state->on_complete(tls_state->userdata, hs_rc);
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        return;
    }

    KEEL_LOG_DEBUG(KEEL_LOG_CAT_TLS, "TLS handshake in progress (fd %d)", client_fd);
}

/* ============================================================================
 * Public API: Start TLS handshake on accepted client connection
 * ============================================================================ */

/**
 * @brief Initiate TLS handshake on a client connection
 *
 * Creates TLS context, initializes handshake, queues initial operations.
 * Calls on_complete(userdata, 0) on success or on_complete(userdata, error) on failure.
 *
 * @param client_fd             Client socket file descriptor
 * @param tls_config            TLS configuration (mode, cert/key files, etc.)
 * @param timeout_ms            Handshake timeout in milliseconds
 * @param userdata              Opaque context passed to on_complete callback
 * @param on_complete           Completion callback (called when handshake done or error)
 * @return 0 on success (callback will be called), <0 on immediate error
 */
int keel_frontend_tls_handshake_start(
    int client_fd,
    const keel_tls_config_t* tls_config,
    size_t timeout_ms,
    void* userdata,
    void (*on_complete)(void* userdata, int result)
)
{
    if (!tls_config || !on_complete) {
        return -EINVAL;
    }

    if (tls_config->mode == KEEL_TLS_DISABLE) {
        /* TLS not enabled, call completion immediately */
        on_complete(userdata, 0);
        return 0;
    }

    /* Allocate TLS handshake state */
    frontend_tls_state_t* tls_state = (frontend_tls_state_t*)keel_malloc(sizeof(*tls_state));
    if (!tls_state) {
        return -ENOMEM;
    }

    memset(tls_state, 0, sizeof(*tls_state));
    tls_state->client_fd = client_fd;
    tls_state->handshake_start_ms = get_time_ms();
    tls_state->timeout_ms = timeout_ms;
    tls_state->userdata = userdata;
    tls_state->on_complete = on_complete;

    /* Create TLS context for this connection */
    keel_error_t rc = keel_tls_context_create(tls_config, true, &tls_state->tls_ctx);
    if (rc != KEEL_OK) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "Failed to create TLS context: %d", rc);
        keel_free(tls_state);
        on_complete(userdata, -1);
        return -1;
    }

    /* Start handshake (non-blocking, via Memory BIO) */
    keel_tls_hs_result_t hs_result = keel_tls_handshake_step(tls_state->tls_ctx, NULL);
    
    if (hs_result == KEEL_TLS_HS_ERROR) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, "TLS handshake init failed");
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        on_complete(userdata, -1);
        return -1;
    }

    if (hs_result == KEEL_TLS_HS_COMPLETE) {
        /* Unlikely, but handshake complete immediately (shouldn't happen) */
        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "TLS handshake completed immediately");
        if (keel_tls_context_activate_ktls(tls_state->tls_ctx, client_fd) == KEEL_OK) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, "Kernel TLS activated for client fd %d", client_fd);
        }
        on_complete(userdata, 0);
        keel_tls_context_destroy(tls_state->tls_ctx);
        keel_free(tls_state);
        return 0;
    }

    /* hs_result == WANT_WRITE or WANT_READ: handshake in progress */
    if (hs_result == KEEL_TLS_HS_WANT_WRITE) {
        /* Get data to send to client */
        uint8_t send_buf[4096];
        ssize_t to_send = keel_tls_get_handshake_data(tls_state->tls_ctx, 
                                                       send_buf, sizeof(send_buf));
        if (to_send > 0) {
            ssize_t n = send(client_fd, send_buf, (size_t)to_send, MSG_DONTWAIT);
            if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS, 
                              "Failed to send TLS ServerHello: %s", strerror(errno));
                keel_tls_context_destroy(tls_state->tls_ctx);
                keel_free(tls_state);
                on_complete(userdata, -errno);
                return -errno;
            }
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_TLS, "Sent %zd bytes of TLS data (fd %d)", 
                          to_send, client_fd);
        }
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_TLS, 
                 "TLS handshake started for client fd %d (timeout %zu ms)", 
                 client_fd, timeout_ms);

    /* Success: state allocated, context created, handshake initiated */
    return 0;
}

/* ============================================================================
 * Cleanup helper: destroy TLS context
 * ============================================================================ */

/**
 * @brief Destroy a frontend TLS context.
 *
 * Thin wrapper around keel_tls_context_destroy() exposed for use by
 * the worker layer without a direct include of tls_context internals.
 *
 * @param ctx  TLS context to destroy; no-op if NULL.
 */
void keel_frontend_tls_context_destroy(keel_tls_context_t* ctx)
{
    if (ctx) {
        keel_tls_context_destroy(ctx);
    }
}
