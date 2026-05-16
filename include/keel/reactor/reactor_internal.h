/**
 * @file reactor_internal.h
 * @brief Internal reactor backend contract and opaque handle layout.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This header exposes the concrete `keel_reactor_t` layout to platform-specific
 * backend implementations inside the `arch` subsystem. Public callers should use
 * only the stable API in `reactor.h`.
 *
 * Responsibilities defined here:
 * - thread-local completion metadata handoff between backend completion loops
 *   and higher-layer callbacks
 * - the function-pointer vtable each backend must populate
 * - the full reactor structure shared by the common factory and platform code
 */

#ifndef KEEL_REACTOR_INTERNAL_H
#define KEEL_REACTOR_INTERNAL_H

#include "keel/reactor/reactor.h"
#include <sys/socket.h>

/**
 * @brief Record the monotonic timestamp at which the current completion became visible.
 *
 * @param ns Completion observation timestamp in nanoseconds.
 * @return Nothing.
 *
 * @note Backends call this before invoking higher-layer callbacks so worker and
 *       stats code can attribute reactor latency precisely.
 */
void keel_reactor_set_completion_seen_ns(uint64_t ns);

/**
 * @brief Record the monotonic timestamp at which the current wait syscall woke up.
 *
 * @param ns Wakeup timestamp in nanoseconds.
 * @return Nothing.
 */
void keel_reactor_set_completion_wakeup_ns(uint64_t ns);

/**
 * @brief Record the number of CQEs/events in the current completion batch.
 *
 * @param size Completion batch size.
 * @return Nothing.
 */
void keel_reactor_set_completion_batch_size(uint32_t size);

/**
 * @brief Record the 1-based index of the completion currently being processed in its batch.
 *
 * @param index Batch index for the current completion.
 * @return Nothing.
 */
void keel_reactor_set_completion_batch_index(uint32_t index);

/* ============================================================================
 * Full Reactor Structure (Internal)
 * ============================================================================ */

/**
 * @brief Concrete reactor object shared between the common layer and a backend.
 *
 * Backends own `backend_state` and populate the function pointers during
 * initialization. The common wrapper functions in `reactor_common.c` use this
 * vtable to provide the stable, platform-agnostic API exported in `reactor.h`.
 */
struct keel_reactor {
    keel_reactor_type_t      type;
    keel_reactor_config_t    config;
    keel_reactor_stats_t     stats;
    
    /* Platform-specific state object allocated and freed by the backend. */
    void*                   backend_state;
    
    /* Backend operation hooks. Any unsupported hook may be left NULL. */
    void    (*destroy)(struct keel_reactor* r);
    int     (*register_fd)(struct keel_reactor* r, int fd);
    void    (*unregister_fd)(struct keel_reactor* r, int fd);
    int     (*accept)(struct keel_reactor* r, int listen_fd, struct sockaddr* addr,
                      socklen_t* addrlen, void* userdata, 
                      keel_reactor_callback_t callback, bool multishot);
    int     (*recv)(struct keel_reactor* r, int fd, void* buf, size_t len,
                    int flags, void* userdata, keel_reactor_callback_t callback);
    int     (*send)(struct keel_reactor* r, int fd, const void* buf, size_t len,
                    int flags, void* userdata, keel_reactor_callback_t callback);
    int     (*connect)(struct keel_reactor* r, int fd, const struct sockaddr* addr,
                       socklen_t addrlen, void* userdata, 
                       keel_reactor_callback_t callback);
    int     (*close_fd)(struct keel_reactor* r, int fd, void* userdata,
                     keel_reactor_callback_t callback);
    int     (*splice)(struct keel_reactor* r, int fd_in, int fd_out, size_t len,
                      int pipe_fd[2], void* userdata, 
                      keel_reactor_callback_t callback);
    int     (*submit_linked)(struct keel_reactor* r, keel_op_t* ops, size_t count);
    int     (*chain_send_recv)(struct keel_reactor* r,
                    int send_fd, const void* send_buf, size_t send_len,
                    int send_flags, void* send_userdata,
                    keel_reactor_callback_t on_send_done,
                    int recv_fd, void* recv_buf, size_t recv_len,
                    int recv_flags, void* recv_userdata,
                    keel_reactor_callback_t on_recv_done);
    int     (*timeout)(struct keel_reactor* r, uint32_t timeout_ms, void* userdata,
                       keel_reactor_callback_t callback);
    int     (*cancel_timeout)(struct keel_reactor* r, int timer_id);
    int     (*submit)(struct keel_reactor* r);
    int     (*wait)(struct keel_reactor* r, int timeout_ms);
    int     (*process)(struct keel_reactor* r);
    size_t  (*pending)(struct keel_reactor* r);
};

#endif /* KEEL_REACTOR_INTERNAL_H */
