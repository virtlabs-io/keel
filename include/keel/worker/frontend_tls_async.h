/**
 * @file frontend_tls_async.h
 * @brief Entry points for asynchronous frontend TLS handshakes.
 *
 * This interface lets the worker accept path hand a newly accepted client
 * socket to a non-blocking TLS bootstrap sequence before the normal protocol
 * flow takes over. The goal is to keep the worker event loop responsive while
 * still centralizing all TLS-specific setup, timeout, and kTLS activation
 * logic in one place.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_FRONTEND_TLS_ASYNC_H
#define KEEL_FRONTEND_TLS_ASYNC_H

#include "keel/protocol/tls_context.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Begin a non-blocking TLS handshake on a client socket.
 *
 * Ownership of the TLS context stays inside the handshake helper; ownership of
 * the socket remains with the caller, which decides whether to continue with
 * protocol setup or close the connection after the completion callback fires.
 *
 * @param client_fd Client socket descriptor. The socket must already be
 *        configured for non-blocking I/O.
 * @param tls_config TLS configuration to apply.
 * @param timeout_ms Handshake timeout in milliseconds.
 * @param userdata Opaque caller context echoed back into `on_complete`.
 * @param on_complete Completion callback invoked with `0` on success or a
 *        negative error code on failure.
 * @return `0` when the handshake was started successfully, `-EINVAL` on invalid
 *         input, `-ENOMEM` on allocation failure, or another negative errno-
 *         style code on immediate setup failure.
 */
int keel_frontend_tls_handshake_start(
    int client_fd,
    const keel_tls_config_t* tls_config,
    size_t timeout_ms,
    void* userdata,
    void (*on_complete)(void* userdata, int result)
);

/**
 * @brief Destroy a TLS context owned by frontend handshake code.
 *
 * This is primarily an escape hatch for cleanup paths that need symmetry with
 * the start helper.
 *
 * @param ctx TLS context pointer, or `NULL`.
 * @return
 */
void keel_frontend_tls_context_destroy(keel_tls_context_t* ctx);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_FRONTEND_TLS_ASYNC_H */
