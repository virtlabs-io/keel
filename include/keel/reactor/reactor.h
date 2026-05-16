/**
 * @file reactor.h
 * @brief Public API for KEEL's platform-agnostic event reactor.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * The reactor abstracts platform-specific event and async-I/O facilities behind
 * one callback-driven interface used by workers, protocol code, and backend
 * connection management.
 *
 * Supported backends:
 * - `io_uring` on Linux as the primary high-performance path
 * - `kqueue` on macOS/BSD
 * - `epoll` on Linux as the fallback path
 *
 * The API intentionally separates three phases:
 * - queue work
 * - wait for completions/readiness
 * - process completions and invoke callbacks
 */

#ifndef KEEL_REACTOR_H
#define KEEL_REACTOR_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "keel/engine/engine.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declarations */
struct keel_session;

/* ============================================================================
 * Reactor Handle
 * ============================================================================ */

typedef struct keel_reactor keel_reactor_t;

/* ============================================================================
 * Reactor Configuration
 * ============================================================================ */

/**
 * @brief Reactor configuration shared by all backends.
 *
 * Some fields are backend-specific hints and may be ignored by platforms that
 * do not support the underlying feature.
 */
typedef struct keel_reactor_config {
    keel_reactor_type_t  type;           /* Reactor type (AUTO for best) */
    uint32_t            queue_depth;    /* SQ/CQ depth (io_uring) */
    bool                sqpoll;         /* Use SQ polling (io_uring) */
    uint32_t            sqpoll_idle_ms; /* SQ poll idle timeout */
    bool                register_fds;   /* Register FDs (io_uring) */
    size_t              max_fds;        /* Max registered FDs */
    bool                enable_ktls;    /* Enable kernel TLS (if available) */
    /* Buffer ring options (io_uring Linux 5.19+ / liburing 2.2+) */
    bool                use_buf_rings;  /* Pre-register recv buffers (IOSQE_BUFFER_SELECT) */
    uint32_t            buf_ring_size;  /* Number of buffers in the ring (0 = queue_depth) */
} keel_reactor_config_t;

#define KEEL_REACTOR_CONFIG_DEFAULT { \
    .type = KEEL_REACTOR_AUTO, \
    .queue_depth = 256, \
    .sqpoll = false, \
    .sqpoll_idle_ms = 1000, \
    .register_fds = false, \
    .max_fds = 1024, \
    .enable_ktls = true, \
    .use_buf_rings = false, \
    .buf_ring_size = 0, \
}

/* ============================================================================
 * Completion Callback
 * ============================================================================ */

/**
 * @brief Completion callback type
 * @param userdata User-provided context (typically session pointer)
 * @param result Operation result (bytes transferred, or -errno on error)
 */
typedef void (*keel_reactor_callback_t)(void* userdata, int result);

/* ============================================================================
 * Reactor Lifecycle
 * ============================================================================ */

/**
 * @brief Create a new reactor instance
 * @param config Reactor configuration (NULL for defaults)
 * @return Reactor handle or NULL on error
 */
keel_reactor_t* keel_reactor_create(const keel_reactor_config_t* config);

/**
 * @brief Get the actual reactor type being used
 * @param reactor Reactor handle
 * @return Reactor type
 */
keel_reactor_type_t keel_reactor_get_type(keel_reactor_t* reactor);

/**
 * @brief Destroy a reactor and free resources
 * @param reactor Reactor handle
 */
void keel_reactor_destroy(keel_reactor_t* reactor);

/* ============================================================================
 * File Descriptor Management
 * ============================================================================ */

/**
 * @brief Register a file descriptor for faster operations
 * 
 * On io_uring, this allows using registered FD indices for less overhead.
 * On other backends, this is a no-op.
 *
 * @param reactor Reactor handle
 * @param fd File descriptor to register
 * @return Registered FD index, or fd if registration not supported
 */
int keel_reactor_register_fd(keel_reactor_t* reactor, int fd);

/**
 * @brief Unregister a file descriptor
 * @param reactor Reactor handle
 * @param fd File descriptor or registered index
 */
void keel_reactor_unregister_fd(keel_reactor_t* reactor, int fd);

/* ============================================================================
 * Asynchronous Operations
 * ============================================================================
 * These functions queue operations for later submission. They return
 * immediately. Results come through the completion callback.
 */

/**
 * @brief Queue an accept operation
 *
 * @param reactor Reactor handle
 * @param listen_fd Listening socket
 * @param addr Output: client address (can be NULL)
 * @param addrlen Address length
 * @param userdata Callback context
 * @param callback Completion callback
 * @param multishot If true, auto-rearm after each accept (io_uring 5.19+)
 * @return 0 on success, -1 on error
 */
int keel_reactor_accept(
    keel_reactor_t* reactor,
    int listen_fd,
    struct sockaddr* addr,
    socklen_t* addrlen,
    void* userdata,
    keel_reactor_callback_t callback,
    bool multishot
);

/**
 * @brief Queue a recv operation
 *
 * @param reactor Reactor handle
 * @param fd Socket file descriptor
 * @param buf Buffer to receive into
 * @param len Buffer size
 * @param flags recv() flags (e.g., MSG_PEEK)
 * @param userdata Callback context
 * @param callback Completion callback
 * @return 0 on success, -1 on error
 */
int keel_reactor_recv(
    keel_reactor_t* reactor,
    int fd,
    void* buf,
    size_t len,
    int flags,
    void* userdata,
    keel_reactor_callback_t callback
);

/**
 * @brief Queue a send operation
 *
 * @param reactor Reactor handle
 * @param fd Socket file descriptor
 * @param buf Buffer to send from
 * @param len Bytes to send
 * @param flags send() flags
 * @param userdata Callback context
 * @param callback Completion callback
 * @return 0 on success, -1 on error
 */
int keel_reactor_send(
    keel_reactor_t* reactor,
    int fd,
    const void* buf,
    size_t len,
    int flags,
    void* userdata,
    keel_reactor_callback_t callback
);

/**
 * @brief Queue a connect operation
 *
 * @param reactor Reactor handle
 * @param fd Socket file descriptor
 * @param addr Server address
 * @param addrlen Address length
 * @param userdata Callback context
 * @param callback Completion callback
 * @return 0 on success, -1 on error
 */
int keel_reactor_connect(
    keel_reactor_t* reactor,
    int fd,
    const struct sockaddr* addr,
    socklen_t addrlen,
    void* userdata,
    keel_reactor_callback_t callback
);

/**
 * @brief Queue a close operation
 *
 * @param reactor Reactor handle
 * @param fd File descriptor to close
 * @param userdata Callback context
 * @param callback Completion callback
 * @return 0 on success, -1 on error
 */
int keel_reactor_close(
    keel_reactor_t* reactor,
    int fd,
    void* userdata,
    keel_reactor_callback_t callback
);

/* ============================================================================
 * Zero-Copy Operations (Linux only, noop on other platforms)
 * ============================================================================ */

/**
 * @brief Queue a splice operation (zero-copy pipe transfer)
 *
 * Moves data from fd_in to fd_out through a pipe without userspace copy.
 * On non-Linux platforms, falls back to read/write.
 *
 * @param reactor Reactor handle
 * @param fd_in Source file descriptor
 * @param fd_out Destination file descriptor
 * @param len Bytes to transfer
 * @param pipe_fd Pipe to use as intermediate buffer
 * @param userdata Callback context
 * @param callback Completion callback
 * @return 0 on success, -1 on error
 */
int keel_reactor_splice(
    keel_reactor_t* reactor,
    int fd_in,
    int fd_out,
    size_t len,
    int pipe_fd[2],
    void* userdata,
    keel_reactor_callback_t callback
);

/**
 * @brief Queue a linked operation chain
 *
 * On io_uring, links operations so they execute atomically in sequence.
 * If any operation fails, subsequent operations are cancelled.
 *
 * Common pattern: peek header → splice body
 *
 * @param reactor Reactor handle
 * @param ops Array of operations
 * @param count Number of operations
 * @return 0 on success, -1 on error
 */
int keel_reactor_submit_linked(
    keel_reactor_t* reactor,
    keel_op_t* ops,
    size_t count
);

/**
 * @brief Queue a linked send+recv chain (io_uring optimization)
 *
 * Chains a send SQE followed by a recv SQE using IOSQE_IO_LINK.
 * The send executes in the kernel, and upon success the recv is
 * automatically armed — eliminating inline send() syscalls and
 * the return to userspace between send and recv.
 *
 * The send callback fires first (result = bytes sent or -errno).
 * If the send fails, the recv CQE fires with -ECANCELED.
 *
 * @param reactor Reactor handle
 * @param send_fd FD to send to
 * @param send_buf Buffer to send from
 * @param send_len Bytes to send
 * @param send_flags send() flags (e.g., MSG_NOSIGNAL)
 * @param send_userdata Callback context for send completion
 * @param on_send_done Send completion callback
 * @param recv_fd FD to recv from (can differ from send_fd)
 * @param recv_buf Buffer to recv into
 * @param recv_len Buffer size
 * @param recv_flags recv() flags
 * @param recv_userdata Callback context for recv completion
 * @param on_recv_done Recv completion callback
 * @return 0 on success, -1 on error
 */
int keel_reactor_chain_send_recv(
    keel_reactor_t* reactor,
    int send_fd, const void* send_buf, size_t send_len, int send_flags,
    void* send_userdata, keel_reactor_callback_t on_send_done,
    int recv_fd, void* recv_buf, size_t recv_len, int recv_flags,
    void* recv_userdata, keel_reactor_callback_t on_recv_done
);

/* ============================================================================
 * Timer Operations
 * ============================================================================ */

/**
 * @brief Queue a timeout operation
 *
 * @param reactor Reactor handle
 * @param timeout_ms Timeout in milliseconds
 * @param userdata Callback context
 * @param callback Completion callback
 * @return Timer ID (>0) on success, -1 on error
 */
int keel_reactor_timeout(
    keel_reactor_t* reactor,
    uint32_t timeout_ms,
    void* userdata,
    keel_reactor_callback_t callback
);

/**
 * @brief Cancel a pending timeout
 *
 * @param reactor Reactor handle
 * @param timer_id Timer ID from keel_reactor_timeout
 * @return 0 on success, -1 on error
 */
int keel_reactor_cancel_timeout(keel_reactor_t* reactor, int timer_id);

/* ============================================================================
 * Event Loop
 * ============================================================================ */

/**
 * @brief Submit all pending operations
 *
 * For io_uring, this calls io_uring_submit().
 * For kqueue/epoll, this is typically a no-op (operations are immediate).
 *
 * @param reactor Reactor handle
 * @return Number of operations submitted, or -1 on error
 */
int keel_reactor_submit(keel_reactor_t* reactor);

/**
 * @brief Wait for completions
 *
 * Blocks until at least one operation completes or timeout expires.
 *
 * @param reactor Reactor handle
 * @param timeout_ms Timeout in milliseconds (-1 for infinite)
 * @return Number of completions available, or -1 on error
 */
int keel_reactor_wait(keel_reactor_t* reactor, int timeout_ms);

/**
 * @brief Process pending completions
 *
 * Invokes callbacks for all completed operations.
 *
 * @param reactor Reactor handle
 * @return Number of completions processed
 */
int keel_reactor_process(keel_reactor_t* reactor);

/**
 * @brief Get number of pending operations
 * @param reactor Reactor handle
 * @return Number of pending operations
 */
size_t keel_reactor_pending(keel_reactor_t* reactor);

/**
 * @brief Return the monotonic timestamp at which the current completion was observed.
 * @return Timestamp in nanoseconds, or `0` when no callback is active.
 */
uint64_t keel_reactor_current_completion_seen_ns(void);

/**
 * @brief Return the monotonic timestamp at which the current completion batch woke from wait.
 * @return Timestamp in nanoseconds, or `0` when no callback is active.
 */
uint64_t keel_reactor_current_completion_wakeup_ns(void);

/**
 * @brief Return the size of the current completion batch.
 * @return Batch size, or `0` when no callback is active.
 */
uint32_t keel_reactor_current_completion_batch_size(void);

/**
 * @brief Return the 1-based position of the current completion within its batch.
 * @return Batch index, or `0` when no callback is active.
 */
uint32_t keel_reactor_current_completion_batch_index(void);

/* ============================================================================
 * Statistics
 * ============================================================================ */

typedef struct keel_reactor_stats {
    uint64_t    ops_submitted;
    uint64_t    ops_completed;
    uint64_t    bytes_read;
    uint64_t    bytes_written;
    uint64_t    bytes_spliced;
    uint64_t    accepts;
    uint64_t    connects;
    uint64_t    timeouts;
    uint64_t    errors;
    uint64_t    sq_overflow;
} keel_reactor_stats_t;

/**
 * @brief Copy the reactor statistics into caller storage.
 * @param reactor Reactor handle.
 * @param stats Output statistics structure.
 */
void keel_reactor_get_stats(keel_reactor_t* reactor, keel_reactor_stats_t* stats);

/**
 * @brief Reset all reactor statistics counters to zero.
 * @param reactor Reactor handle.
 */
void keel_reactor_reset_stats(keel_reactor_t* reactor);

/* ============================================================================
 * Platform Detection
 * ============================================================================ */

/**
 * @brief Check whether io_uring is available and usable.
 * @return `true` if io_uring support is compiled in and the runtime kernel is new enough.
 */
bool keel_reactor_has_iouring(void);

/**
 * @brief Check whether kqueue is available.
 * @return `true` on supported macOS/BSD targets.
 */
bool keel_reactor_has_kqueue(void);

/**
 * @brief Check whether epoll is available.
 * @return `true` on Linux targets.
 */
bool keel_reactor_has_epoll(void);

/**
 * @brief Check whether splice-style zero-copy transfer is available.
 * @return `true` on supported Linux targets.
 */
bool keel_reactor_has_splice(void);

/**
 * @brief Check whether kernel TLS is available.
 * @return `true` when runtime kernel heuristics indicate kTLS support.
 */
bool keel_reactor_has_ktls(void);

/**
 * @brief Check whether io_uring provider buffer rings are available.
 * @return `true` when build/runtime support is present.
 */
bool keel_reactor_has_buf_rings(void);

/**
 * @brief Check whether io_uring multishot recv is available.
 * @return `true` when build/runtime support is present and provider buffer rings are available.
 */
bool keel_reactor_has_recv_multishot(void);

/**
 * @brief Return the preferred backend type for the current platform.
 * @return Best available reactor type.
 */
keel_reactor_type_t keel_reactor_best_type(void);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_REACTOR_H */
