/**
 * @file platform_compat.h
 * @brief Small portability shims for Linux and Darwin/BSD differences.
 *
 * KEEL is primarily Linux-oriented but still keeps a limited portability layer
 * for development and auxiliary environments. This header centralizes a few of
 * the platform gaps that otherwise tend to leak preprocessor branches across
 * the codebase: missing socket flags, clock identifiers, and signal waiting
 * primitives.
 *
 * The intent is not to emulate every Linux semantic perfectly. Instead, these
 * shims preserve the higher-level contract KEEL needs, while documenting the
 * compromises involved on non-Linux systems.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef KEEL_PLATFORM_COMPAT_H
#define KEEL_PLATFORM_COMPAT_H

#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#if defined(__linux__)
#include <sys/epoll.h>
#endif

/* ============================================================================
 * MSG_NOSIGNAL
 * ========================================================================== */
#if !defined(MSG_NOSIGNAL)
#  define MSG_NOSIGNAL 0
#endif

/* ============================================================================
 * CLOCK_MONOTONIC_COARSE
 * ========================================================================== */
#if !defined(CLOCK_MONOTONIC_COARSE)
#  define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif

/* ============================================================================
 * keel_socket_nonblock — portable non-blocking socket creation.
 *
 * Usage: replace
 *   socket(domain, SOCK_STREAM | SOCK_NONBLOCK, 0)
 * with
 *   keel_socket_nonblock(domain, SOCK_STREAM, 0)
 * ========================================================================== */
/**
 * @brief Create a non-blocking socket using the cheapest platform mechanism.
 *
 * Linux can request non-blocking behavior at socket creation time with
 * `SOCK_NONBLOCK`, which avoids a follow-up `fcntl()` call and closes a small
 * race window. Platforms without that flag fall back to `socket()` plus
 * `fcntl(F_SETFL, O_NONBLOCK)`.
 *
 * @param domain Socket domain such as `AF_INET`.
 * @param type Socket type such as `SOCK_STREAM`.
 * @param protocol Protocol number, usually `0`.
 * @return File descriptor on success, or `-1` on failure with `errno` set.
 */
static inline int keel_socket_nonblock(int domain, int type, int protocol)
{
#if defined(__linux__)
    return socket(domain, type | SOCK_NONBLOCK, protocol);
#else
    int fd = socket(domain, type, protocol);
    if (fd < 0) return -1;
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(fd);
        return -1;
    }
    return fd;
#endif
}

/* ============================================================================
 * keel_sigtimedwait — portable signal wait with timeout.
 *
 * sigtimedwait(2) is a Linux-specific POSIX extension not available on macOS.
 * The emulation polls sigpending() every 50 ms until a queued signal is found
 * or the requested timeout (tv_sec + tv_nsec) expires.
 *
 * On Linux, delegates directly to the real sigtimedwait syscall.
 * ========================================================================== */
#ifndef __linux__
#define KEEL__SIGWAIT_STEP_NS (50L * 1000000L)  /* 50 ms poll granularity */

/**
 * @brief Wait for a blocked signal with a timeout on platforms lacking
 *        `sigtimedwait(2)`.
 *
 * The non-Linux fallback polls the process pending set at a fixed interval.
 * That is less precise and less efficient than the native Linux syscall, but
 * it preserves the blocking-with-timeout contract well enough for KEEL's
 * control-plane style usage.
 *
 * @param set Signal set to wait on.
 * @param ts Relative timeout.
 * @return Signal number on success, or `-1` on timeout/error.
 */
static inline int keel_sigtimedwait(const sigset_t *set,
                                     const struct timespec *ts)
{
    long total_ns = ts->tv_sec * 1000000000L + ts->tv_nsec;
    long elapsed  = 0;
    struct timespec step = { 0, KEEL__SIGWAIT_STEP_NS };

    while (elapsed < total_ns) {
        sigset_t pending;
        sigpending(&pending);
        for (int s = 1; s < NSIG; s++) {
            if (sigismember(set, s) && sigismember(&pending, s)) {
                int caught = 0;
                sigwait(set, &caught);
                return caught;
            }
        }
        nanosleep(&step, NULL);
        elapsed += KEEL__SIGWAIT_STEP_NS;
    }
    errno = EAGAIN;
    return -1;
}
#else
/* On Linux use the real sigtimedwait(2) syscall */
/**
 * @brief Linux wrapper around the native `sigtimedwait(2)` syscall.
 *
 * @param set Signal set to wait on.
 * @param ts Relative timeout.
 * @return Signal number on success, or `-1` on timeout/error.
 */
static inline int keel_sigtimedwait(const sigset_t *set,
                                     const struct timespec *ts)
{
    return sigtimedwait(set, NULL, ts);
}
#endif /* __linux__ */

/* ============================================================================
 * keel_set_nonblocking / keel_set_nodelay
 *
 * Previously duplicated as static set_nonblocking() in engine.c, cluster.c,
 * reactor_epoll.c, and reactor_kqueue.c; and as set_nodelay() in engine.c
 * and cluster.c.  All callers should use these canonical versions.
 * ========================================================================== */

/**
 * @brief Put @p fd into non-blocking mode.
 *
 * Uses F_GETFL / F_SETFL + O_NONBLOCK.  The macOS variant checks the flag
 * first to avoid a redundant syscall when the fd is already non-blocking.
 *
 * @return 0 on success, -1 on error (errno set).
 */
static inline int keel_set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (flags & O_NONBLOCK) return 0;   /* already set */
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/**
 * @brief Disable Nagle's algorithm on @p fd (TCP_NODELAY).
 *
 * Reduces latency for short request/response patterns by sending small
 * segments immediately instead of coalescing them.
 *
 * @return 0 on success, -1 on error (errno set).
 */
static inline int keel_set_nodelay(int fd) {
    int one = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

/* ============================================================================
 * keel_fd_wait — wait for readability/writability on a single fd.
 *
 * Replaces one-shot poll(fd, 1, timeout_ms) calls used in probe threads,
 * the cluster thread, and the admin event loop.
 *
 * On Linux uses epoll_wait() — one syscall, no signal-mask overhead,
 * O(1) regardless of fd value.
 * On BSD/macOS falls back to select() which is always available.
 *
 * Flags: KEEL_FD_WAIT_READ | KEEL_FD_WAIT_WRITE (can be OR'd).
 * Returns: > 0 if the fd is ready, 0 on timeout, -1 on error (errno set).
 * ========================================================================== */
#define KEEL_FD_WAIT_READ  0x01
#define KEEL_FD_WAIT_WRITE 0x02

/**
 * @brief Wait for I/O readiness on a single file descriptor.
 *
 * Blocks until @p fd is readable and/or writable (as specified by @p flags),
 * or until the timeout elapses.  Uses epoll on Linux for O(1) dispatch,
 * select on BSD/macOS.
 *
 * This helper is the correct replacement for one-shot @c poll() calls in
 * non-reactor threads (probe threads, cluster thread, admin thread).  The
 * main worker reactor already uses io_uring; this is for auxiliary threads.
 *
 * @param fd         File descriptor to monitor.
 * @param flags      KEEL_FD_WAIT_READ and/or KEEL_FD_WAIT_WRITE.
 * @param timeout_ms Timeout in milliseconds; -1 means wait forever.
 * @return Positive if ready (epoll event count or select count),
 *         0 on timeout, -1 on error (errno set).
 */
static inline int keel_fd_wait(int fd, int flags, int timeout_ms)
{
#if defined(__linux__)
    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) return -1;

    struct epoll_event ev = { .data.fd = fd };
    if (flags & KEEL_FD_WAIT_READ)  ev.events |= EPOLLIN;
    if (flags & KEEL_FD_WAIT_WRITE) ev.events |= EPOLLOUT;
    ev.events |= EPOLLERR | EPOLLHUP;

    if (epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        int saved = errno;
        close(epfd);
        errno = saved;
        return -1;
    }

    struct epoll_event out;
    int ret = epoll_wait(epfd, &out, 1, timeout_ms);
    int saved = errno;
    close(epfd);
    errno = saved;
    return ret;
#else
    /* BSD / macOS fallback: select */
    fd_set rset, wset, eset;
    FD_ZERO(&rset); FD_ZERO(&wset); FD_ZERO(&eset);
    if (flags & KEEL_FD_WAIT_READ)  FD_SET(fd, &rset);
    if (flags & KEEL_FD_WAIT_WRITE) FD_SET(fd, &wset);
    FD_SET(fd, &eset);

    struct timeval tv, *tvp = NULL;
    if (timeout_ms >= 0) {
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        tvp = &tv;
    }
    return select(fd + 1, &rset, &wset, &eset, tvp);
#endif
}

/* ============================================================================
 * keel_epoll_create — create a long-lived epoll fd for multi-fd event loops.
 *
 * Used by the admin thread (2 listen sockets) and cluster thread (listen +
 * peer sockets).  Returns an epoll fd on Linux, -1 with ENOSYS on BSD.
 * ========================================================================== */
/**
 * @brief Create an epoll instance (Linux) for multi-fd event loops.
 *
 * Returns a valid epoll fd on Linux.  On BSD/macOS returns -1 and sets
 * errno=ENOSYS — callers must fall back to select-based loops there.
 *
 * @return epoll fd on success, -1 on error.
 */
static inline int keel_epoll_create(void)
{
#if defined(__linux__)
    return epoll_create1(EPOLL_CLOEXEC);
#else
    errno = ENOSYS;
    return -1;
#endif
}

#endif /* KEEL_PLATFORM_COMPAT_H */
