/**
 * @file io_splice.c
 * @brief Linux splice/vmsplice helpers plus portable fallback transfer paths.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file implements the low-level zero-copy helpers used by the protocol and
 * reactor layers when a message path can bypass userspace copying. Where the
 * kernel or platform cannot satisfy the request, the implementation degrades to
 * bounded user-space copies while preserving a consistent result contract.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE  /* For splice, pipe2, etc. */
#endif

#include "keel/reactor/io_splice.h"

#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include "keel/util/platform_compat.h"

#if KEEL_HAVE_SPLICE
#include <linux/version.h>
#endif

/* Fallback buffer for non-Linux or when splice unavailable */
#define SPLICE_FALLBACK_BUF_SIZE 65536

/* ============================================================================
 * Splice Pipe Operations
 * ============================================================================ */

/**
 * @brief Create and initialize a splice pipe pair.
 *
 * @param[out] pipe Pipe handle to initialize.
 * @param size Requested capacity, or `0` for system default.
 * @return `KEEL_OK` on success, otherwise a KEEL error code.
 *
 * Corner cases:
 * - pipe capacity requests are best-effort and may be ignored by the kernel
 * - non-Linux fallback still creates a regular non-blocking pipe
 */
keel_error_t keel_splice_pipe_create(keel_splice_pipe_t* pipe, size_t size)
{
    if (pipe == NULL) {
        return KEEL_ERR_INVALID_ARG;
    }

    memset(pipe, 0, sizeof(*pipe));
    pipe->pipe_fds[0] = -1;
    pipe->pipe_fds[1] = -1;

#if KEEL_HAVE_SPLICE
    /* Create non-blocking pipe */
    if (pipe2(pipe->pipe_fds, O_NONBLOCK | O_CLOEXEC) < 0) {
        return KEEL_ERR_IO;
    }

    /* Try to set the pipe size if requested */
    if (size > 0) {
        int requested = (int)size;
        if (fcntl(pipe->pipe_fds[1], F_SETPIPE_SZ, requested) < 0) {
            /* Non-fatal - use default size */
        }
    }

    /* Get actual pipe size */
    int actual = fcntl(pipe->pipe_fds[1], F_GETPIPE_SZ);
    if (actual > 0) {
        pipe->capacity = (size_t)actual;
    } else {
        pipe->capacity = 65536;  /* Default pipe size */
    }

    pipe->pending = 0;
    pipe->valid = true;
    return KEEL_OK;
#else
    /* Fallback: use regular pipe */
    int fds[2];
    if (pipe(fds) < 0) {
        return KEEL_ERR_IO;
    }

    /* Set non-blocking */
    fcntl(fds[0], F_SETFL, fcntl(fds[0], F_GETFL) | O_NONBLOCK);
    fcntl(fds[1], F_SETFL, fcntl(fds[1], F_GETFL) | O_NONBLOCK);

    pipe->pipe_fds[0] = fds[0];
    pipe->pipe_fds[1] = fds[1];
    pipe->capacity = 65536;
    pipe->pending = 0;
    pipe->valid = true;
    return KEEL_OK;
#endif
}

/**
 * @brief Destroy a splice pipe and invalidate its bookkeeping.
 *
 * @param[in,out] pipe Pipe handle to destroy.
 * @return Nothing.
 */
void keel_splice_pipe_destroy(keel_splice_pipe_t* pipe)
{
    if (pipe == NULL) {
        return;
    }

    if (pipe->pipe_fds[0] >= 0) {
        close(pipe->pipe_fds[0]);
        pipe->pipe_fds[0] = -1;
    }
    if (pipe->pipe_fds[1] >= 0) {
        close(pipe->pipe_fds[1]);
        pipe->pipe_fds[1] = -1;
    }

    pipe->valid = false;
    pipe->pending = 0;
}

/**
 * @brief Return the effective capacity of a valid splice pipe.
 *
 * @param pipe Pipe handle.
 * @return Capacity in bytes, or `0` if the pipe is invalid.
 */
size_t keel_splice_pipe_capacity(const keel_splice_pipe_t* pipe)
{
    if (pipe == NULL || !pipe->valid) {
        return 0;
    }
    return pipe->capacity;
}

/* ============================================================================
 * Peek Operations
 * ============================================================================ */

/**
 * @brief Non-destructively inspect socket data with `MSG_PEEK`.
 *
 * @param fd Socket file descriptor.
 * @param[out] buf Destination buffer.
 * @param len Maximum bytes to inspect.
 * @return Structured result containing status, byte count, and errno.
 */
keel_peek_result_t keel_peek(int fd, void* buf, size_t len)
{
    keel_peek_result_t result = { .status = KEEL_PEEK_OK, .len = 0, .error = 0 };

    if (fd < 0 || buf == NULL || len == 0) {
        result.status = KEEL_PEEK_ERROR;
        result.error = EINVAL;
        return result;
    }

    ssize_t n = recv(fd, buf, len, MSG_PEEK | MSG_DONTWAIT);
    if (n > 0) {
        result.status = KEEL_PEEK_OK;
        result.len = (size_t)n;
    } else if (n == 0) {
        result.status = KEEL_PEEK_CLOSED;
    } else {
        int err = errno;
        if (err == EAGAIN || err == EWOULDBLOCK) {
            result.status = KEEL_PEEK_WOULDBLOCK;
        } else {
            result.status = KEEL_PEEK_ERROR;
            result.error = err;
        }
    }

    return result;
}

/**
 * @brief Wait up to `timeout_ms` for readability and then perform a peek.
 *
 * @param fd Socket file descriptor.
 * @param[out] buf Destination buffer.
 * @param len Maximum bytes to inspect.
 * @param timeout_ms Poll timeout in milliseconds.
 * @return Structured peek result.
 */
keel_peek_result_t keel_peek_timed(int fd, void* buf, size_t len, int timeout_ms)
{
    keel_peek_result_t result = { .status = KEEL_PEEK_OK, .len = 0, .error = 0 };

    if (fd < 0 || buf == NULL || len == 0) {
        result.status = KEEL_PEEK_ERROR;
        result.error = EINVAL;
        return result;
    }

    /* Wait for data using epoll — O(1), preferred over poll() */
    int rc = keel_fd_wait(fd, KEEL_FD_WAIT_READ, timeout_ms);

    if (rc < 0) {
        result.status = KEEL_PEEK_ERROR;
        result.error = errno;
        return result;
    }

    if (rc == 0) {
        result.status = KEEL_PEEK_WOULDBLOCK;
        return result;
    }

    /* Data available, peek it */
    return keel_peek(fd, buf, len);
}

/* ============================================================================
 * Splice Transfer Operations
 * ============================================================================ */

/**
 * @brief Move data from one socket to another using splice or a fallback copy.
 *
 * @param src_fd Source fd.
 * @param dst_fd Destination fd.
 * @param[in,out] pipe Intermediate splice pipe.
 * @param len Requested maximum transfer size, or `0` for pipe capacity.
 * @param flags Transfer modifiers.
 * @return Transfer result with byte count, KEEL error code, errno, and EOF flag.
 *
 * Main uses:
 * - proxy fast path socket-to-socket forwarding
 */
keel_transfer_result_t keel_splice_transfer(
    int                     src_fd,
    int                     dst_fd,
    keel_splice_pipe_t*      pipe,
    size_t                  len,
    keel_transfer_flags_t    flags)
{
    keel_transfer_result_t result = {
        .bytes = 0,
        .error = KEEL_OK,
        .sys_errno = 0,
        .eof = false
    };

    if (src_fd < 0 || dst_fd < 0 || pipe == NULL || !pipe->valid) {
        result.error = KEEL_ERR_INVALID_ARG;
        return result;
    }

    size_t to_transfer = (len > 0) ? len : pipe->capacity;

#if KEEL_HAVE_SPLICE
    /* Build splice flags */
    unsigned int splice_flags = SPLICE_F_NONBLOCK;
    if (flags & KEEL_TRANSFER_MORE) {
        splice_flags |= SPLICE_F_MORE;
    }
    if (flags & KEEL_TRANSFER_MOVE) {
        splice_flags |= SPLICE_F_MOVE;
    }

    /* Step 1: splice from source socket to pipe */
    ssize_t n = splice(src_fd, NULL, pipe->pipe_fds[1], NULL,
                       to_transfer, splice_flags);

    if (n <= 0) {
        if (n == 0) {
            result.eof = true;
            return result;
        }
        int err = errno;
        if (err == EAGAIN || err == EWOULDBLOCK) {
            /* No data available, not an error */
            return result;
        }
        result.error = KEEL_ERR_IO;
        result.sys_errno = err;
        return result;
    }

    size_t in_pipe = (size_t)n;

    /* Step 2: splice from pipe to destination socket */
    ssize_t out = splice(pipe->pipe_fds[0], NULL, dst_fd, NULL,
                         in_pipe, splice_flags);

    if (out <= 0) {
        int err = errno;
        /* Data is stuck in pipe - this is problematic */
        result.error = KEEL_ERR_IO;
        result.sys_errno = err;
        pipe->pending = in_pipe;  /* Track pending data */
        return result;
    }

    result.bytes = (size_t)out;

    /* Handle partial transfer */
    if ((size_t)out < in_pipe) {
        pipe->pending = in_pipe - (size_t)out;
    } else {
        pipe->pending = 0;
    }

    return result;
#else
    /* Fallback: read/write through user-space buffer */
    static __thread uint8_t fallback_buf[SPLICE_FALLBACK_BUF_SIZE];

    size_t buf_size = (to_transfer < SPLICE_FALLBACK_BUF_SIZE) ?
                      to_transfer : SPLICE_FALLBACK_BUF_SIZE;

    ssize_t n = recv(src_fd, fallback_buf, buf_size, MSG_DONTWAIT);
    if (n <= 0) {
        if (n == 0) {
            result.eof = true;
            return result;
        }
        int err = errno;
        if (err == EAGAIN || err == EWOULDBLOCK) {
            return result;
        }
        result.error = KEEL_ERR_IO;
        result.sys_errno = err;
        return result;
    }

    ssize_t w = send(dst_fd, fallback_buf, (size_t)n, MSG_NOSIGNAL);
    if (w < 0) {
        result.error = KEEL_ERR_IO;
        result.sys_errno = errno;
        return result;
    }

    result.bytes = (size_t)w;
    return result;
#endif
}

/**
 * @brief Move bytes from a socket into a splice pipe.
 *
 * @return Structured transfer result.
 */
keel_transfer_result_t keel_splice_from_socket(
    int                     src_fd,
    keel_splice_pipe_t*      pipe,
    size_t                  len,
    keel_transfer_flags_t    flags)
{
    keel_transfer_result_t result = {
        .bytes = 0,
        .error = KEEL_OK,
        .sys_errno = 0,
        .eof = false
    };

    if (src_fd < 0 || pipe == NULL || !pipe->valid) {
        result.error = KEEL_ERR_INVALID_ARG;
        return result;
    }

    size_t to_transfer = (len > 0) ? len : pipe->capacity;

#if KEEL_HAVE_SPLICE
    unsigned int splice_flags = SPLICE_F_NONBLOCK;
    if (flags & KEEL_TRANSFER_MORE) {
        splice_flags |= SPLICE_F_MORE;
    }
    if (flags & KEEL_TRANSFER_MOVE) {
        splice_flags |= SPLICE_F_MOVE;
    }

    ssize_t n = splice(src_fd, NULL, pipe->pipe_fds[1], NULL,
                       to_transfer, splice_flags);

    if (n <= 0) {
        if (n == 0) {
            result.eof = true;
            return result;
        }
        int err = errno;
        if (err == EAGAIN || err == EWOULDBLOCK) {
            return result;
        }
        result.error = KEEL_ERR_IO;
        result.sys_errno = err;
        return result;
    }

    result.bytes = (size_t)n;
    pipe->pending += (size_t)n;
    return result;
#else
    (void)flags;
    result.error = KEEL_ERR_NOT_SUPPORTED;
    return result;
#endif
}

/**
 * @brief Drain bytes already present in a splice pipe into a destination socket.
 *
 * @return Structured transfer result.
 */
keel_transfer_result_t keel_splice_to_socket(
    keel_splice_pipe_t*      pipe,
    int                     dst_fd,
    size_t                  len,
    keel_transfer_flags_t    flags)
{
    keel_transfer_result_t result = {
        .bytes = 0,
        .error = KEEL_OK,
        .sys_errno = 0,
        .eof = false
    };

    if (pipe == NULL || !pipe->valid || dst_fd < 0) {
        result.error = KEEL_ERR_INVALID_ARG;
        return result;
    }

    if (pipe->pending == 0) {
        return result;  /* Nothing to transfer */
    }

    size_t to_transfer = (len > 0 && len < pipe->pending) ? len : pipe->pending;

#if KEEL_HAVE_SPLICE
    unsigned int splice_flags = SPLICE_F_NONBLOCK;
    if (flags & KEEL_TRANSFER_MORE) {
        splice_flags |= SPLICE_F_MORE;
    }

    ssize_t n = splice(pipe->pipe_fds[0], NULL, dst_fd, NULL,
                       to_transfer, splice_flags);

    if (n <= 0) {
        int err = errno;
        if (err == EAGAIN || err == EWOULDBLOCK) {
            return result;
        }
        result.error = KEEL_ERR_IO;
        result.sys_errno = err;
        return result;
    }

    result.bytes = (size_t)n;
    pipe->pending -= (size_t)n;
    return result;
#else
    (void)flags;
    result.error = KEEL_ERR_NOT_SUPPORTED;
    return result;
#endif
}

/**
 * @brief Push user-space bytes toward a socket using vmsplice/splice or send fallback.
 *
 * @return Structured transfer result.
 */
keel_transfer_result_t keel_splice_from_buffer(
    const void*             buf,
    size_t                  len,
    int                     dst_fd,
    keel_splice_pipe_t*      pipe,
    keel_transfer_flags_t    flags)
{
    keel_transfer_result_t result = {
        .bytes = 0,
        .error = KEEL_OK,
        .sys_errno = 0,
        .eof = false
    };

    if (buf == NULL || len == 0 || dst_fd < 0 || pipe == NULL || !pipe->valid) {
        result.error = KEEL_ERR_INVALID_ARG;
        return result;
    }

#if KEEL_HAVE_VMSPLICE
    /* Use vmsplice to put data in pipe, then splice to socket */
    struct iovec iov = {
        .iov_base = (void*)buf,
        .iov_len = len
    };

    unsigned int vmsplice_flags = SPLICE_F_NONBLOCK;
    if (flags & KEEL_TRANSFER_GIFT) {
        vmsplice_flags |= SPLICE_F_GIFT;
    }

    ssize_t n = vmsplice(pipe->pipe_fds[1], &iov, 1, vmsplice_flags);
    if (n <= 0) {
        if (n == 0) {
            return result;
        }
        int err = errno;
        if (err == EAGAIN || err == EWOULDBLOCK) {
            return result;
        }
        result.error = KEEL_ERR_IO;
        result.sys_errno = err;
        return result;
    }

    size_t in_pipe = (size_t)n;

    /* Now splice from pipe to socket */
    unsigned int splice_flags = SPLICE_F_NONBLOCK;
    if (flags & KEEL_TRANSFER_MORE) {
        splice_flags |= SPLICE_F_MORE;
    }

    ssize_t out = splice(pipe->pipe_fds[0], NULL, dst_fd, NULL,
                         in_pipe, splice_flags);

    if (out <= 0) {
        int err = errno;
        result.error = KEEL_ERR_IO;
        result.sys_errno = err;
        pipe->pending = in_pipe;
        return result;
    }

    result.bytes = (size_t)out;
    if ((size_t)out < in_pipe) {
        pipe->pending = in_pipe - (size_t)out;
    }

    return result;
#else
    /* Fallback: direct send */
    (void)pipe;
    (void)flags;

    ssize_t w = send(dst_fd, buf, len, MSG_NOSIGNAL);
    if (w < 0) {
        result.error = KEEL_ERR_IO;
        result.sys_errno = errno;
        return result;
    }

    result.bytes = (size_t)w;
    return result;
#endif
}
