/**
 * @file reactor_common.c
 * @brief Platform-neutral reactor factory, wrappers, and capability detection.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file is the narrow waist of the reactor subsystem. It owns the public
 * wrapper API that higher layers call, probes platform/runtime capabilities,
 * selects the best backend, and delegates every operation through the backend
 * vtable stored in `keel_reactor_t`.
 *
 * Design intent:
 * - keep backend selection and fallback behavior centralized
 * - present consistent error semantics even when a backend omits a feature
 * - provide thread-local completion metadata for worker latency accounting
 */

#include "keel/reactor/reactor.h"
#include "keel/log/log.h"
#include "keel/reactor/reactor_internal.h"
#include "keel/mem/mem.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>

static __thread uint64_t g_keel_reactor_completion_seen_ns = 0;
static __thread uint64_t g_keel_reactor_completion_wakeup_ns = 0;
static __thread uint32_t g_keel_reactor_completion_batch_size = 0;
static __thread uint32_t g_keel_reactor_completion_batch_index = 0;

/**
 * @brief Store the completion-seen timestamp for the current thread.
 *
 * @param ns Monotonic timestamp in nanoseconds.
 * @return Nothing.
 */
void keel_reactor_set_completion_seen_ns(uint64_t ns)
{
    g_keel_reactor_completion_seen_ns = ns;
}

/**
 * @brief Store the wait-wakeup timestamp for the current thread.
 *
 * @param ns Monotonic timestamp in nanoseconds.
 * @return Nothing.
 */
void keel_reactor_set_completion_wakeup_ns(uint64_t ns)
{
    g_keel_reactor_completion_wakeup_ns = ns;
}

/**
 * @brief Store the current completion batch size for the current thread.
 *
 * @param size Number of completions observed in the batch.
 * @return Nothing.
 */
void keel_reactor_set_completion_batch_size(uint32_t size)
{
    g_keel_reactor_completion_batch_size = size;
}

/**
 * @brief Store the current completion's 1-based position within its batch.
 *
 * @param index Batch index.
 * @return Nothing.
 */
void keel_reactor_set_completion_batch_index(uint32_t index)
{
    g_keel_reactor_completion_batch_index = index;
}

/**
 * @brief Fetch the current thread's completion-seen timestamp.
 *
 * @return Monotonic timestamp in nanoseconds, or `0` if none is active.
 */
uint64_t keel_reactor_current_completion_seen_ns(void)
{
    return g_keel_reactor_completion_seen_ns;
}

/**
 * @brief Fetch the current thread's completion-wakeup timestamp.
 *
 * @return Monotonic timestamp in nanoseconds, or `0` if none is active.
 */
uint64_t keel_reactor_current_completion_wakeup_ns(void)
{
    return g_keel_reactor_completion_wakeup_ns;
}

/**
 * @brief Fetch the current thread's completion batch size.
 *
 * @return Batch size, or `0` if no completion is active.
 */
uint32_t keel_reactor_current_completion_batch_size(void)
{
    return g_keel_reactor_completion_batch_size;
}

/**
 * @brief Fetch the current thread's current completion batch index.
 *
 * @return 1-based batch index, or `0` if no completion is active.
 */
uint32_t keel_reactor_current_completion_batch_index(void)
{
    return g_keel_reactor_completion_batch_index;
}

/* Platform detection */
#if defined(__linux__)
    #define KEEL_PLATFORM_LINUX 1
    #include <sys/utsname.h>
#elif defined(__APPLE__) && defined(__MACH__)
    #define KEEL_PLATFORM_MACOS 1
#elif defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
    #define KEEL_PLATFORM_BSD 1
#endif

/* Forward declarations for backend initialization */
#if KEEL_PLATFORM_LINUX
#ifdef KEEL_HAS_IOURING
extern int keel_reactor_iouring_init(keel_reactor_t* reactor);
#endif
extern int keel_reactor_epoll_init(keel_reactor_t* reactor);
#endif

#if KEEL_PLATFORM_MACOS || KEEL_PLATFORM_BSD
extern int keel_reactor_kqueue_init(keel_reactor_t* reactor);
#endif

/* ============================================================================
 * Platform Detection Functions
 * ============================================================================ */

/**
 * @brief Check whether `io_uring` should be considered available.
 *
 * @return `true` when the build and runtime kernel version support the
 *         required feature level, otherwise `false`.
 *
 * Corner cases:
 * - Build-time liburing support is required.
 * - Runtime kernel probing is version-based rather than feature-probe-based.
 */
bool keel_reactor_has_iouring(void)
{
#if KEEL_PLATFORM_LINUX && defined(KEEL_HAS_IOURING)
    /* Check if io_uring is available at runtime via kernel version */
    struct utsname uts;
    if (uname(&uts) != 0) {
        return false;
    }
    
    int major, minor;
    if (sscanf(uts.release, "%d.%d", &major, &minor) != 2) {
        return false;
    }
    
    /* io_uring was added in Linux 5.1, but 5.6+ is recommended */
    return (major > 5) || (major == 5 && minor >= 6);
#else
    return false;
#endif
}

/**
 * @brief Check whether kqueue is available on the current platform.
 *
 * @return `true` on supported macOS/BSD targets, otherwise `false`.
 */
bool keel_reactor_has_kqueue(void)
{
#if KEEL_PLATFORM_MACOS || KEEL_PLATFORM_BSD
    return true;
#else
    return false;
#endif
}

/**
 * @brief Check whether epoll is available on the current platform.
 *
 * @return `true` on Linux, otherwise `false`.
 */
bool keel_reactor_has_epoll(void)
{
#if KEEL_PLATFORM_LINUX
    return true;  /* epoll is always available on Linux */
#else
    return false;
#endif
}

/**
 * @brief Check whether splice-based zero-copy transfer is expected to be available.
 *
 * @return `true` on Linux, otherwise `false`.
 */
bool keel_reactor_has_splice(void)
{
#if KEEL_PLATFORM_LINUX
    return true;
#else
    return false;
#endif
}

/**
 * @brief Check whether the runtime kernel is new enough for kTLS support.
 *
 * @return `true` when Linux kernel version heuristics indicate kTLS support,
 *         otherwise `false`.
 */
bool keel_reactor_has_ktls(void)
{
#if KEEL_PLATFORM_LINUX
    /* Check for kernel TLS support (Linux 4.13+) */
    struct utsname uts;
    if (uname(&uts) != 0) {
        return false;
    }
    
    int major, minor;
    if (sscanf(uts.release, "%d.%d", &major, &minor) != 2) {
        return false;
    }
    
    return (major > 4) || (major == 4 && minor >= 13);
#else
    return false;
#endif
}

/**
 * @brief Check whether provider buffer rings should be considered available.
 *
 * @return `true` when build-time support is present and the runtime kernel is
 *         new enough, otherwise `false`.
 */
bool keel_reactor_has_buf_rings(void)
{
#if KEEL_PLATFORM_LINUX && defined(KEEL_HAS_URING_BUF_RING)
    /* IORING_REGISTER_PBUF_RING requires Linux 5.19+ */
    struct utsname uts;
    if (uname(&uts) != 0) {
        return false;
    }

    int major, minor;
    if (sscanf(uts.release, "%d.%d", &major, &minor) != 2) {
        return false;
    }

    return (major > 5) || (major == 5 && minor >= 19);
#else
    return false;
#endif
}

/**
 * @brief Check whether io_uring multishot recv support is available.
 *
 * @return `true` when build-time support is present and the runtime kernel is
 *         Linux 6.0+, otherwise `false`.
 */
bool keel_reactor_has_recv_multishot(void)
{
#if KEEL_PLATFORM_LINUX && defined(KEEL_HAS_URING_BUF_RING) && defined(KEEL_HAS_URING_RECV_MULTISHOT)
    /* IORING_OP_RECV_MULTISHOT requires Linux 6.0+ */
    struct utsname uts;
    if (uname(&uts) != 0) {
        return false;
    }

    int major, minor;
    if (sscanf(uts.release, "%d.%d", &major, &minor) != 2) {
        return false;
    }

    return (major >= 6);
#else
    return false;
#endif
}

/**
 * @brief Pick the preferred backend type for the current platform.
 *
 * @return Preferred reactor type, or `KEEL_REACTOR_AUTO` if no backend is
 *         considered usable.
 */
keel_reactor_type_t keel_reactor_best_type(void)
{
    if (keel_reactor_has_iouring()) {
        return KEEL_REACTOR_IOURING;
    }
    if (keel_reactor_has_kqueue()) {
        return KEEL_REACTOR_KQUEUE;
    }
#if KEEL_PLATFORM_LINUX
    return KEEL_REACTOR_EPOLL;
#else
    /* No suitable reactor available */
    return KEEL_REACTOR_AUTO;
#endif
}

/* ============================================================================
 * Reactor Creation
 * ============================================================================ */

/**
 * @brief Allocate and initialize a reactor using the selected backend.
 *
 * @param config Optional caller-supplied configuration, or `NULL` for defaults.
 * @return New reactor handle on success, or `NULL` on allocation/backend init failure.
 *
 * Behavior:
 * - applies default configuration when `config` is `NULL`
 * - resolves `AUTO` to the preferred runtime backend
 * - delegates backend-specific initialization to the selected platform file
 */
keel_reactor_t* keel_reactor_create(const keel_reactor_config_t* config)
{
    keel_reactor_t* reactor = (keel_reactor_t*)keel_calloc(1, sizeof(keel_reactor_t));
    if (reactor == NULL) {
        return NULL;
    }
    
    /* Apply configuration */
    if (config != NULL) {
        reactor->config = *config;
    } else {
        keel_reactor_config_t defaults = KEEL_REACTOR_CONFIG_DEFAULT;
        reactor->config = defaults;
    }
    
    /* Determine reactor type; remember whether it was auto-selected so we can
     * fall back to epoll if the preferred backend (io_uring) is unavailable at
     * runtime (e.g. EPERM inside an unprivileged Docker container). */
    keel_reactor_type_t type = reactor->config.type;
    bool auto_selected = (type == KEEL_REACTOR_AUTO);
    if (auto_selected) {
        type = keel_reactor_best_type();
    }
    reactor->type = type;
    
    /* Initialize backend */
    int result = -1;
    
    switch (type) {
#if KEEL_PLATFORM_LINUX
#ifdef KEEL_HAS_IOURING
        case KEEL_REACTOR_IOURING:
            result = keel_reactor_iouring_init(reactor);
            break;
#endif
        case KEEL_REACTOR_EPOLL:
            result = keel_reactor_epoll_init(reactor);
            break;
#endif
#if KEEL_PLATFORM_MACOS || KEEL_PLATFORM_BSD
        case KEEL_REACTOR_KQUEUE:
            result = keel_reactor_kqueue_init(reactor);
            break;
#endif
        default:
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "No suitable reactor backend for type %d", type);
            break;
    }
    
#if KEEL_PLATFORM_LINUX && defined(KEEL_HAS_IOURING)
    /* If the auto-selected io_uring backend failed at runtime (EPERM in a
     * container, ENOSYS on a kernel without the syscall, etc.) fall back
     * transparently to the always-available epoll backend.  We only do this
     * when the type was AUTO-resolved — an explicit IOURING request from the
     * caller is treated as a hard requirement and is not silently downgraded. */
    if (result != 0 && auto_selected && type == KEEL_REACTOR_IOURING) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
            "io_uring unavailable at runtime — falling back to epoll");
        reactor->type = KEEL_REACTOR_EPOLL;
        result = keel_reactor_epoll_init(reactor);
    }
#endif
    
    if (result != 0) {
        keel_free(reactor);
        return NULL;
    }
    
    return reactor;
}

/**
 * @brief Return the concrete backend type of a reactor handle.
 *
 * @param reactor Reactor handle.
 * @return Concrete backend type, or `KEEL_REACTOR_AUTO` for `NULL` handles.
 */
keel_reactor_type_t keel_reactor_get_type(keel_reactor_t* reactor)
{
    return reactor ? reactor->type : KEEL_REACTOR_AUTO;
}

/**
 * @brief Destroy a reactor and all backend-owned resources.
 *
 * @param reactor Reactor handle, or `NULL`.
 * @return Nothing.
 */
void keel_reactor_destroy(keel_reactor_t* reactor)
{
    if (reactor == NULL) {
        return;
    }
    
    if (reactor->destroy) {
        reactor->destroy(reactor);
    }
    
    keel_free(reactor);
}

/* ============================================================================
 * Forwarding Functions
 * ============================================================================ */

/**
 * @brief Register a file descriptor when the backend supports it.
 *
 * @param reactor Reactor handle.
 * @param fd File descriptor to register.
 * @return Backend-specific registered index, or the original fd when
 *         registration is unsupported/unavailable.
 */
int keel_reactor_register_fd(keel_reactor_t* reactor, int fd)
{
    if (reactor == NULL || reactor->register_fd == NULL) {
        return fd;  /* Return original fd if not supported */
    }
    return reactor->register_fd(reactor, fd);
}

/**
 * @brief Unregister a previously registered file descriptor.
 *
 * @param reactor Reactor handle.
 * @param fd File descriptor or registration index, depending on backend semantics.
 * @return Nothing.
 */
void keel_reactor_unregister_fd(keel_reactor_t* reactor, int fd)
{
    if (reactor != NULL && reactor->unregister_fd != NULL) {
        reactor->unregister_fd(reactor, fd);
    }
}

/**
 * @brief Queue or arm an asynchronous accept operation.
 *
 * @return `0` on success, `-1` with `errno=ENOSYS` when unsupported, or `-1`
 *         for backend-specific submission failure.
 */
int keel_reactor_accept(
    keel_reactor_t* reactor,
    int listen_fd,
    struct sockaddr* addr,
    socklen_t* addrlen,
    void* userdata,
    keel_reactor_callback_t callback,
    bool multishot)
{
    if (reactor == NULL || reactor->accept == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return reactor->accept(reactor, listen_fd, addr, addrlen, 
                           userdata, callback, multishot);
}

/**
 * @brief Queue or arm an asynchronous recv operation.
 *
 * @return `0` on success, or `-1` with `errno=ENOSYS` when unsupported.
 */
int keel_reactor_recv(
    keel_reactor_t* reactor,
    int fd,
    void* buf,
    size_t len,
    int flags,
    void* userdata,
    keel_reactor_callback_t callback)
{
    if (reactor == NULL || reactor->recv == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return reactor->recv(reactor, fd, buf, len, flags, userdata, callback);
}

/**
 * @brief Queue or arm an asynchronous send operation.
 *
 * @return `0` on success, or `-1` with `errno=ENOSYS` when unsupported.
 */
int keel_reactor_send(
    keel_reactor_t* reactor,
    int fd,
    const void* buf,
    size_t len,
    int flags,
    void* userdata,
    keel_reactor_callback_t callback)
{
    if (reactor == NULL || reactor->send == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return reactor->send(reactor, fd, buf, len, flags, userdata, callback);
}

/**
 * @brief Queue or arm an asynchronous connect operation.
 *
 * @return `0` on success, or `-1` with `errno=ENOSYS` when unsupported.
 */
int keel_reactor_connect(
    keel_reactor_t* reactor,
    int fd,
    const struct sockaddr* addr,
    socklen_t addrlen,
    void* userdata,
    keel_reactor_callback_t callback)
{
    if (reactor == NULL || reactor->connect == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return reactor->connect(reactor, fd, addr, addrlen, userdata, callback);
}

/**
 * @brief Close a file descriptor asynchronously when supported, synchronously otherwise.
 *
 * @return `0` when close dispatch succeeded or the synchronous fallback ran,
 *         otherwise backend-specific failure.
 */
int keel_reactor_close(
    keel_reactor_t* reactor,
    int fd,
    void* userdata,
    keel_reactor_callback_t callback)
{
    if (reactor == NULL || reactor->close_fd == NULL) {
        /* Fallback: synchronous close */
        close(fd);
        if (callback) {
            callback(userdata, 0);
        }
        return 0;
    }
    return reactor->close_fd(reactor, fd, userdata, callback);
}

/**
 * @brief Queue a zero-copy splice transfer when supported.
 *
 * @return `0` on success, or `-1` with `errno=ENOSYS` when unsupported.
 */
int keel_reactor_splice(
    keel_reactor_t* reactor,
    int fd_in,
    int fd_out,
    size_t len,
    int pipe_fd[2],
    void* userdata,
    keel_reactor_callback_t callback)
{
    if (reactor == NULL || reactor->splice == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return reactor->splice(reactor, fd_in, fd_out, len, pipe_fd, 
                           userdata, callback);
}

/**
 * @brief Submit a backend-native linked chain of operations.
 *
 * @return `0` on success, or `-1` with `errno=ENOSYS` when unsupported.
 */
int keel_reactor_submit_linked(
    keel_reactor_t* reactor,
    keel_op_t* ops,
    size_t count)
{
    if (reactor == NULL || reactor->submit_linked == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return reactor->submit_linked(reactor, ops, count);
}

/**
 * @brief Queue a linked send-then-recv sequence when the backend supports it.
 *
 * @return `0` on success, or `-1` with `errno=ENOSYS` when unsupported.
 */
int keel_reactor_chain_send_recv(
    keel_reactor_t* reactor,
    int send_fd, const void* send_buf, size_t send_len, int send_flags,
    void* send_userdata, keel_reactor_callback_t on_send_done,
    int recv_fd, void* recv_buf, size_t recv_len, int recv_flags,
    void* recv_userdata, keel_reactor_callback_t on_recv_done)
{
    if (reactor == NULL || reactor->chain_send_recv == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return reactor->chain_send_recv(reactor,
        send_fd, send_buf, send_len, send_flags,
        send_userdata, on_send_done,
        recv_fd, recv_buf, recv_len, recv_flags,
        recv_userdata, on_recv_done);
}

/**
 * @brief Queue a timeout callback.
 *
 * @return Backend timer identifier on success, or `-1` with `errno=ENOSYS`
 *         when unsupported.
 */
int keel_reactor_timeout(
    keel_reactor_t* reactor,
    uint32_t timeout_ms,
    void* userdata,
    keel_reactor_callback_t callback)
{
    if (reactor == NULL || reactor->timeout == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return reactor->timeout(reactor, timeout_ms, userdata, callback);
}

/**
 * @brief Cancel a previously armed timeout when supported.
 *
 * @return Backend-specific status; silently returns success when cancellation
 *         is unsupported.
 */
int keel_reactor_cancel_timeout(keel_reactor_t* reactor, int timer_id)
{
    if (reactor == NULL || reactor->cancel_timeout == NULL) {
        return 0;  /* Silently succeed if not supported */
    }
    return reactor->cancel_timeout(reactor, timer_id);
}

/**
 * @brief Flush pending submissions when required by the backend.
 *
 * @return Backend-specific status, or `0` when submission is a no-op.
 */
int keel_reactor_submit(keel_reactor_t* reactor)
{
    if (reactor == NULL || reactor->submit == NULL) {
        return 0;  /* No-op if not needed */
    }
    return reactor->submit(reactor);
}

/**
 * @brief Wait for one or more backend completions/events.
 *
 * @return Backend-specific positive event count, `0` for timeout/interruption,
 *         or `-1` with `errno=ENOSYS` when unsupported.
 */
int keel_reactor_wait(keel_reactor_t* reactor, int timeout_ms)
{
    if (reactor == NULL || reactor->wait == NULL) {
        errno = ENOSYS;
        return -1;
    }
    return reactor->wait(reactor, timeout_ms);
}

/**
 * @brief Process any completions/events already made available by the backend.
 *
 * @return Number of processed completions/events.
 */
int keel_reactor_process(keel_reactor_t* reactor)
{
    if (reactor == NULL || reactor->process == NULL) {
        return 0;
    }
    return reactor->process(reactor);
}

/**
 * @brief Report the backend's count of pending operations.
 *
 * @return Pending operation count, or `0` when unavailable.
 */
size_t keel_reactor_pending(keel_reactor_t* reactor)
{
    if (reactor == NULL || reactor->pending == NULL) {
        return 0;
    }
    return reactor->pending(reactor);
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Copy the current reactor statistics snapshot into caller storage.
 *
 * @param reactor Reactor handle.
 * @param[out] stats Destination stats structure.
 * @return Nothing.
 */
void keel_reactor_get_stats(keel_reactor_t* reactor, keel_reactor_stats_t* stats)
{
    if (reactor == NULL || stats == NULL) {
        return;
    }
    *stats = reactor->stats;
}

/**
 * @brief Reset the reactor statistics counters to zero.
 *
 * @param reactor Reactor handle.
 * @return Nothing.
 */
void keel_reactor_reset_stats(keel_reactor_t* reactor)
{
    if (reactor == NULL) {
        return;
    }
    memset(&reactor->stats, 0, sizeof(reactor->stats));
}
