/**
 * @file backend_connect_async.h
 * @brief Public API for asynchronous backend connection establishment and authentication.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Provides a state machine that establishes a backend PostgreSQL connection
 * using the worker's reactor (io_uring) for all I/O. This ensures the
 * worker's event loop is never blocked during backend connection establishment.
 *
 * State machine flow:
 *   CONNECT → STARTUP_SEND → AUTH_RECV → SCRAM_FIRST_SEND →
 *   SCRAM_RECV → SCRAM_FINAL_SEND → DRAIN_RECV → DONE
 *
 * Usage:
 *   backend_async_ctx_t* ctx = backend_async_start(pool, conn, reactor);
 *   // ... reactor callbacks drive the state machine to completion
 *   // on_complete callback fires when done or failed
 */

#ifndef KEEL_BACKEND_CONNECT_ASYNC_H
#define KEEL_BACKEND_CONNECT_ASYNC_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <netinet/in.h>
#include "keel/protocol/tls_context.h"

/* Forward declarations */
struct keel_reactor;
struct backend_pool;
struct backend_conn;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Async connect state machine phases
 */
typedef enum {
    BE_ASYNC_CONNECT,           /**< TCP connect in progress */
    /* PostgreSQL phases */
    BE_ASYNC_STARTUP_SEND,      /**< Sending StartupMessage */
    BE_ASYNC_AUTH_RECV,         /**< Waiting for auth challenge */
    BE_ASYNC_SCRAM_FIRST_SEND,  /**< Sending SCRAM client-first */
    BE_ASYNC_SCRAM_FIRST_RECV,  /**< Waiting for SCRAM server-first */
    BE_ASYNC_SCRAM_FINAL_SEND,  /**< Sending SCRAM client-final */
    BE_ASYNC_SCRAM_FINAL_RECV,  /**< Waiting for SCRAM server-final */
    BE_ASYNC_DRAIN_RECV,        /**< Draining ParameterStatus until ReadyForQuery */
    /* MySQL phases */
    BE_ASYNC_MY_GREETING_RECV,  /**< Waiting for MySQL Initial Handshake */
    BE_ASYNC_MY_RESPONSE_SEND,  /**< Sending MySQL Handshake Response */
    BE_ASYNC_MY_AUTH_RECV,      /**< Waiting for MySQL auth result */
    BE_ASYNC_MY_AUTH_SEND,      /**< Sending MySQL auth data (switch/RSA) */
    BE_ASYNC_MY_RSA_KEY_RECV,   /**< Waiting for RSA public key */
    BE_ASYNC_MY_RSA_PW_SEND,    /**< Sending RSA-encrypted password */
    /* Backend TLS phases */
    BE_ASYNC_TLS_SSLREQUEST_SENT, /**< SSLRequest sent, waiting for 'S' or 'N' */
    BE_ASYNC_TLS_HANDSHAKE,      /**< TLS handshake in progress */
    /* Terminal */
    BE_ASYNC_DONE,              /**< Complete (success or failure) */
} backend_async_phase_t;

/**
 * @brief Completion callback
 *
 * The callback receives the same backend slot originally passed to
 * `backend_async_start()`. By callback time the slot has effectively become an
 * output object: on success its file descriptor and negotiated connection state
 * are ready for pool use; on failure the slot identifies which attempt failed
 * so the pool can recycle or mark it closed.
 *
 * @param[in,out] conn Backend connection slot populated by the async state machine.
 * @param success `true` if the connection is authenticated and ready for use.
 * @param userdata User context, typically the owning pool.
 */
typedef void (*backend_async_done_cb)(struct backend_conn* conn,
                                      bool success, void* userdata);

/**
 * @brief Async connect context — one per in-flight backend connection attempt
 */
typedef struct backend_async_ctx {
    /* Connection being established */
    struct backend_conn*    conn;
    struct backend_pool*    pool;
    struct keel_reactor*     reactor;

    /* Completion callback */
    backend_async_done_cb   on_complete;
    void*                   userdata;

    /* State machine */
    backend_async_phase_t   phase;
    int                     fd;         /**< Socket fd being connected */

    /* I/O buffers */
    uint8_t                 send_buf[1024];
    size_t                  send_len;
    uint8_t                 recv_buf[4096];
    size_t                  recv_have;  /**< Valid bytes in recv_buf */

    /* SCRAM auth state (PostgreSQL) */
    char                    client_nonce_b64[32];
    char                    client_first_bare[128];
    char                    server_first[512];
    bool                    authed;

    /* PostgreSQL cancel key (captured from BackendKeyData) */
    uint32_t                pg_backend_pid;      /**< Backend PID from 'K' message */
    uint32_t                pg_cancel_secret;    /**< Backend secret from 'K' message */

    /* MySQL auth state */
    uint8_t                 my_scramble[21];     /**< Server scramble (20 bytes + NUL) */
    size_t                  my_scramble_len;
    uint32_t                my_server_caps;
    uint32_t                my_connection_id;    /**< MySQL connection ID from greeting */
    char                    my_plugin[64];       /**< Auth plugin name */
    uint8_t                 my_seq;              /**< Current MySQL sequence id */
    int                     my_auth_rounds;      /**< Auth round counter */

    /* Sockaddr for connect */
    struct sockaddr_in      addr;

    /* Backend TLS state */
    keel_tls_context_t*     tls_ctx;             /**< TLS context (NULL = no TLS) */
    bool                    tls_hs_active;        /**< Handshake in progress */
    bool                    tls_ktls_active;      /**< kTLS active on fd */
    uint8_t                 tls_hs_buf[4096];     /**< TLS hs send buffer */
} backend_async_ctx_t;

/**
 * @brief Start an async backend connection attempt
 *
 * Allocates a context, creates a socket, and queues the connect operation
 * on the reactor. All subsequent steps are driven by reactor callbacks.
 * The `conn` parameter is caller-owned storage that acts as an output object:
 * the async state machine progressively fills it with the live file descriptor
 * and negotiated backend state if the handshake succeeds.
 *
 * @param pool      Backend pool (for config: host, port, user, etc.)
 * @param[in,out] conn Backend connection slot to populate on success.
 * @param reactor   Worker's reactor for async I/O
 * @param on_complete Callback when done (success or failure)
 * @param userdata  Context for callback
 * @return 0 on success (connect queued), -1 on immediate failure
 */
int backend_async_start(struct backend_pool* pool,
                        struct backend_conn* conn,
                        struct keel_reactor* reactor,
                        backend_async_done_cb on_complete,
                        void* userdata);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_BACKEND_CONNECT_ASYNC_H */
