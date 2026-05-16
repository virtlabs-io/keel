/**
 * @file reactor_kqueue.c
 * @brief macOS/BSD kqueue backend for the reactor abstraction.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This backend maps the reactor API onto kqueue readiness notifications. Unlike
 * io_uring, kqueue does not provide native async send/recv execution, so this
 * implementation stores pending operations in userspace and performs the actual
 * syscalls when read/write readiness arrives.
 *
 * Tradeoffs:
 * - no true zero-copy splice support
 * - no native linked op execution
 * - still preserves a callback-driven API compatible with the rest of KEEL
 */

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)

#include "keel/reactor/reactor.h"
#include "keel/log/log.h"
#include "keel/reactor/reactor_internal.h"
#include "keel/mem/mem.h"
#include "keel/util/platform_compat.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <sys/time.h>

/* ============================================================================
 * Internal Structures
 * ============================================================================ */

typedef enum kqueue_op_state {
    KQUEUE_OP_PENDING,
    KQUEUE_OP_READY,
    KQUEUE_OP_COMPLETE
} kqueue_op_state_t;

typedef struct kqueue_op {
    struct kqueue_op*       next;       /* Free list or pending list */
    
    /* Operation info */
    keel_op_type_t           type;
    int                     fd;
    void*                   buf;
    size_t                  len;
    int                     flags;
    
    /* For connect */
    const struct sockaddr*  addr;
    socklen_t               addrlen;
    
    /* For accept */
    struct sockaddr*        accept_addr;
    socklen_t*              accept_addrlen;
    
    /* Callback */
    void*                   userdata;
    keel_reactor_callback_t  callback;
    
    /* State */
    kqueue_op_state_t       state;
    int                     result;
} kqueue_op_t;

typedef struct kqueue_state {
    int                     kq;         /* kqueue file descriptor */
    
    /* Event arrays */
    struct kevent*          changes;    /* Pending changes */
    size_t                  change_count;
    size_t                  change_capacity;
    
    struct kevent*          events;     /* Received events */
    size_t                  event_capacity;
    int                     event_count;    /* Events from last wait */
    
    /* Pending operations by fd (simple hash table) */
    kqueue_op_t**           read_ops;   /* Pending read/accept ops by fd */
    kqueue_op_t**           write_ops;  /* Pending write/connect ops by fd */
    size_t                  fd_capacity;
    
    /* Timer operations */
    kqueue_op_t*            timer_ops;
    int                     next_timer_id;
    
    /* Operation free list */
    kqueue_op_t*            op_free_list;
    
    /* Statistics */
    size_t                  pending_count;
} kqueue_state_t;

/* ============================================================================
 * Forward Declarations
 * ============================================================================ */

static void kqueue_destroy(keel_reactor_t* reactor);
static int kqueue_register_fd(keel_reactor_t* reactor, int fd);
static void kqueue_unregister_fd(keel_reactor_t* reactor, int fd);
static int kqueue_accept(keel_reactor_t* reactor, int listen_fd,
                         struct sockaddr* addr, socklen_t* addrlen,
                         void* userdata, keel_reactor_callback_t callback,
                         bool multishot);
static int kqueue_recv(keel_reactor_t* reactor, int fd, void* buf, size_t len,
                       int flags, void* userdata, keel_reactor_callback_t callback);
static int kqueue_send(keel_reactor_t* reactor, int fd, const void* buf,
                       size_t len, int flags, void* userdata,
                       keel_reactor_callback_t callback);
static int kqueue_connect_op(keel_reactor_t* reactor, int fd,
                          const struct sockaddr* addr, socklen_t addrlen,
                          void* userdata, keel_reactor_callback_t callback);
static int kqueue_close_fd(keel_reactor_t* reactor, int fd, void* userdata,
                        keel_reactor_callback_t callback);
static int kqueue_splice(keel_reactor_t* reactor, int fd_in, int fd_out,
                         size_t len, int pipe_fd[2], void* userdata,
                         keel_reactor_callback_t callback);
static int kqueue_submit_linked(keel_reactor_t* reactor, keel_op_t* ops,
                                size_t count);
static int kqueue_timeout(keel_reactor_t* reactor, uint32_t timeout_ms,
                          void* userdata, keel_reactor_callback_t callback);
static int kqueue_cancel_timeout(keel_reactor_t* reactor, int timer_id);
static int kqueue_submit(keel_reactor_t* reactor);
static int kqueue_wait(keel_reactor_t* reactor, int timeout_ms);
static int kqueue_process(keel_reactor_t* reactor);
static size_t kqueue_pending(keel_reactor_t* reactor);

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

/**
 * @brief Allocate or recycle one pending kqueue operation record.
 */
static kqueue_op_t* alloc_op(kqueue_state_t* state)
{
    kqueue_op_t* op;
    
    if (state->op_free_list != NULL) {
        op = state->op_free_list;
        state->op_free_list = op->next;
    } else {
        op = (kqueue_op_t*)keel_calloc(1, sizeof(kqueue_op_t));
        if (op == NULL) {
            return NULL;
        }
    }
    
    memset(op, 0, sizeof(kqueue_op_t));
    state->pending_count++;
    return op;
}

/**
 * @brief Return an operation record to the backend free list.
 */
static void free_op(kqueue_state_t* state, kqueue_op_t* op)
{
    op->next = state->op_free_list;
    state->op_free_list = op;
    state->pending_count--;
}

/**
 * @brief Grow per-fd pending-op tables to cover `fd`.
 */
static int ensure_fd_capacity(kqueue_state_t* state, int fd)
{
    if (fd < 0) {
        return -1;
    }
    
    size_t needed = (size_t)(fd + 1);
    if (needed <= state->fd_capacity) {
        return 0;
    }
    
    /* Grow capacity */
    size_t new_capacity = state->fd_capacity * 2;
    if (new_capacity < needed) {
        new_capacity = needed;
    }
    if (new_capacity < 256) {
        new_capacity = 256;
    }
    
    kqueue_op_t** new_read = (kqueue_op_t**)keel_realloc(
        state->read_ops, new_capacity * sizeof(kqueue_op_t*)
    );
    kqueue_op_t** new_write = (kqueue_op_t**)keel_realloc(
        state->write_ops, new_capacity * sizeof(kqueue_op_t*)
    );
    
    if (new_read == NULL || new_write == NULL) {
        return -1;
    }
    
    /* Zero new entries */
    for (size_t i = state->fd_capacity; i < new_capacity; i++) {
        new_read[i] = NULL;
        new_write[i] = NULL;
    }
    
    state->read_ops = new_read;
    state->write_ops = new_write;
    state->fd_capacity = new_capacity;
    
    return 0;
}

/**
 * @brief Append one change record to the pending kevent submission list.
 */
static int add_kevent(kqueue_state_t* state, int fd, int16_t filter, 
                      uint16_t flags, void* udata)
{
    if (state->change_count >= state->change_capacity) {
        size_t new_cap = state->change_capacity * 2;
        struct kevent* new_changes = (struct kevent*)keel_realloc(
            state->changes, new_cap * sizeof(struct kevent)
        );
        if (new_changes == NULL) {
            return -1;
        }
        state->changes = new_changes;
        state->change_capacity = new_cap;
    }
    
    EV_SET(&state->changes[state->change_count],
           (uintptr_t)fd, filter, flags, 0, 0, udata);
    state->change_count++;
    
    return 0;
}

/* set_nonblocking() removed: use keel_set_nonblocking() from keel/util/platform_compat.h */

/* ============================================================================
 * Initialization
 * ============================================================================ */

/**
 * @brief Initialize the kqueue backend into a reactor handle.
 */
int keel_reactor_kqueue_init(keel_reactor_t* reactor)
{
    kqueue_state_t* state = (kqueue_state_t*)keel_calloc(1, sizeof(kqueue_state_t));
    if (state == NULL) {
        return -1;
    }
    
    /* Create kqueue */
    state->kq = kqueue();
    if (state->kq < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "kqueue() failed: %s", strerror(errno));
        keel_free(state);
        return -1;
    }
    
    /* Initialize arrays */
    state->change_capacity = 64;
    state->changes = (struct kevent*)keel_calloc(
        state->change_capacity, sizeof(struct kevent)
    );
    
    state->event_capacity = 256;
    state->events = (struct kevent*)keel_calloc(
        state->event_capacity, sizeof(struct kevent)
    );
    
    state->fd_capacity = 256;
    state->read_ops = (kqueue_op_t**)keel_calloc(
        state->fd_capacity, sizeof(kqueue_op_t*)
    );
    state->write_ops = (kqueue_op_t**)keel_calloc(
        state->fd_capacity, sizeof(kqueue_op_t*)
    );
    
    if (state->changes == NULL || state->events == NULL ||
        state->read_ops == NULL || state->write_ops == NULL) {
        close(state->kq);
        keel_free(state->changes);
        keel_free(state->events);
        keel_free(state->read_ops);
        keel_free(state->write_ops);
        keel_free(state);
        return -1;
    }
    
    state->next_timer_id = 1;
    
    /* Set backend state and function pointers */
    reactor->backend_state = state;
    reactor->destroy = kqueue_destroy;
    reactor->register_fd = kqueue_register_fd;
    reactor->unregister_fd = kqueue_unregister_fd;
    reactor->accept = kqueue_accept;
    reactor->recv = kqueue_recv;
    reactor->send = kqueue_send;
    reactor->connect = kqueue_connect_op;
    reactor->close_fd = kqueue_close_fd;
    reactor->splice = kqueue_splice;
    reactor->submit_linked = kqueue_submit_linked;
    reactor->timeout = kqueue_timeout;
    reactor->cancel_timeout = kqueue_cancel_timeout;
    reactor->submit = kqueue_submit;
    reactor->wait = kqueue_wait;
    reactor->process = kqueue_process;
    reactor->pending = kqueue_pending;
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "kqueue reactor initialized");
    
    return 0;
}

/* ============================================================================
 * Destruction
 * ============================================================================ */

/**
 * @brief Tear down the kqueue backend and free all queued work items.
 */
static void kqueue_destroy(keel_reactor_t* reactor)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    if (state == NULL) {
        return;
    }
    
    /* Free pending operations */
    for (size_t i = 0; i < state->fd_capacity; i++) {
        while (state->read_ops[i] != NULL) {
            kqueue_op_t* op = state->read_ops[i];
            state->read_ops[i] = op->next;
            keel_free(op);
        }
        while (state->write_ops[i] != NULL) {
            kqueue_op_t* op = state->write_ops[i];
            state->write_ops[i] = op->next;
            keel_free(op);
        }
    }
    
    /* Free op free list */
    while (state->op_free_list != NULL) {
        kqueue_op_t* op = state->op_free_list;
        state->op_free_list = op->next;
        keel_free(op);
    }
    
    close(state->kq);
    keel_free(state->changes);
    keel_free(state->events);
    keel_free(state->read_ops);
    keel_free(state->write_ops);
    keel_free(state);
    
    reactor->backend_state = NULL;
}

/* ============================================================================
 * File Descriptor Registration
 * ============================================================================ */

/**
 * @brief kqueue backend no-op fd registration hook.
 */
static int kqueue_register_fd(keel_reactor_t* reactor, int fd)
{
    (void)reactor;
    /* kqueue doesn't require fd registration like io_uring */
    return fd;
}

/**
 * @brief Cancel any pending read/write ops for an fd and unregister kevents.
 */
static void kqueue_unregister_fd(keel_reactor_t* reactor, int fd)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    
    if (fd < 0 || (size_t)fd >= state->fd_capacity) {
        return;
    }
    
    /* Cancel any pending operations for this fd */
    while (state->read_ops[fd] != NULL) {
        kqueue_op_t* op = state->read_ops[fd];
        state->read_ops[fd] = op->next;
        if (op->callback) {
            op->callback(op->userdata, -ECANCELED);
        }
        free_op(state, op);
    }
    
    while (state->write_ops[fd] != NULL) {
        kqueue_op_t* op = state->write_ops[fd];
        state->write_ops[fd] = op->next;
        if (op->callback) {
            op->callback(op->userdata, -ECANCELED);
        }
        free_op(state, op);
    }
    
    /* Remove from kqueue */
    add_kevent(state, fd, EVFILT_READ, EV_DELETE, NULL);
    add_kevent(state, fd, EVFILT_WRITE, EV_DELETE, NULL);
}

/* ============================================================================
 * Accept
 * ============================================================================ */

/**
 * @brief Queue an accept operation and arm EVFILT_READ on the listening socket.
 */
static int kqueue_accept(
    keel_reactor_t* reactor,
    int listen_fd,
    struct sockaddr* addr,
    socklen_t* addrlen,
    void* userdata,
    keel_reactor_callback_t callback,
    bool multishot)
{
    (void)multishot;  /* kqueue doesn't support multishot */
    
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    
    if (ensure_fd_capacity(state, listen_fd) < 0) {
        errno = ENOMEM;
        return -1;
    }
    
    kqueue_op_t* op = alloc_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->type = KEEL_OP_ACCEPT;
    op->fd = listen_fd;
    op->accept_addr = addr;
    op->accept_addrlen = addrlen;
    op->userdata = userdata;
    op->callback = callback;
    op->state = KQUEUE_OP_PENDING;
    
    /* Add to read ops list */
    op->next = state->read_ops[listen_fd];
    state->read_ops[listen_fd] = op;
    
    /* Register for read events (accept triggers read) */
    add_kevent(state, listen_fd, EVFILT_READ, EV_ADD | EV_CLEAR, NULL);
    
    return 0;
}

/* ============================================================================
 * Recv
 * ============================================================================ */

/**
 * @brief Queue a recv operation and arm EVFILT_READ.
 */
static int kqueue_recv(
    keel_reactor_t* reactor,
    int fd,
    void* buf,
    size_t len,
    int flags,
    void* userdata,
    keel_reactor_callback_t callback)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    
    if (ensure_fd_capacity(state, fd) < 0) {
        errno = ENOMEM;
        return -1;
    }
    
    kqueue_op_t* op = alloc_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->type = KEEL_OP_RECV;
    op->fd = fd;
    op->buf = buf;
    op->len = len;
    op->flags = flags;
    op->userdata = userdata;
    op->callback = callback;
    op->state = KQUEUE_OP_PENDING;
    
    op->next = state->read_ops[fd];
    state->read_ops[fd] = op;
    
    KEEL_LOG_WARN(KEEL_LOG_CAT_IO, "kqueue_recv: fd=%d op_type=%d (ops_now=%d)",
                 fd, (int)op->type, state->read_ops[fd]->next ? 2 : 1);
    keel_set_nonblocking(fd);
    add_kevent(state, fd, EVFILT_READ, EV_ADD | EV_CLEAR, NULL);
    
    return 0;
}

/* ============================================================================
 * Send
 * ============================================================================ */

/**
 * @brief Queue a send operation and arm EVFILT_WRITE.
 */
static int kqueue_send(
    keel_reactor_t* reactor,
    int fd,
    const void* buf,
    size_t len,
    int flags,
    void* userdata,
    keel_reactor_callback_t callback)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    
    if (ensure_fd_capacity(state, fd) < 0) {
        errno = ENOMEM;
        return -1;
    }
    
    kqueue_op_t* op = alloc_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->type = KEEL_OP_SEND;
    op->fd = fd;
    op->buf = (void*)buf;  /* Cast away const for storage */
    op->len = len;
    op->flags = flags;
    op->userdata = userdata;
    op->callback = callback;
    op->state = KQUEUE_OP_PENDING;
    
    op->next = state->write_ops[fd];
    state->write_ops[fd] = op;
    
    keel_set_nonblocking(fd);
    add_kevent(state, fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, NULL);
    
    return 0;
}

/* ============================================================================
 * Connect
 * ============================================================================ */

/**
 * @brief Start a non-blocking connect and wait for write readiness.
 */
static int kqueue_connect_op(
    keel_reactor_t* reactor,
    int fd,
    const struct sockaddr* addr,
    socklen_t addrlen,
    void* userdata,
    keel_reactor_callback_t callback)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    
    if (ensure_fd_capacity(state, fd) < 0) {
        errno = ENOMEM;
        return -1;
    }
    
    /* Start non-blocking connect */
    keel_set_nonblocking(fd);
    int result = connect(fd, addr, addrlen);
    
    if (result == 0) {
        /* Connected immediately */
        if (callback) {
            callback(userdata, 0);
        }
        return 0;
    }
    
    if (errno != EINPROGRESS) {
        return -1;
    }
    
    /* Connection in progress, register for write event */
    kqueue_op_t* op = alloc_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->type = KEEL_OP_CONNECT;
    op->fd = fd;
    op->addr = addr;
    op->addrlen = addrlen;
    op->userdata = userdata;
    op->callback = callback;
    op->state = KQUEUE_OP_PENDING;
    
    op->next = state->write_ops[fd];
    state->write_ops[fd] = op;
    
    add_kevent(state, fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, NULL);
    
    return 0;
}

/* ============================================================================
 * Close
 * ============================================================================ */

/**
 * @brief Cancel any queued operations for an fd and close it synchronously.
 */
static int kqueue_close_fd(
    keel_reactor_t* reactor,
    int fd,
    void* userdata,
    keel_reactor_callback_t callback)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    
    /* Remove from kqueue and cancel pending ops */
    kqueue_unregister_fd(reactor, fd);
    
    /* Synchronous close */
    int result = close(fd);
    
    if (callback) {
        callback(userdata, result < 0 ? -errno : 0);
    }
    
    (void)state;
    return 0;
}

/* ============================================================================
 * Splice (Not available on macOS - fallback implementation)
 * ============================================================================ */

/**
 * @brief kqueue splice stub.
 *
 * @note macOS/BSD do not provide the Linux splice semantics KEEL expects, so
 *       this backend currently returns `ENOSYS`.
 */
static int kqueue_splice(
    keel_reactor_t* reactor,
    int fd_in,
    int fd_out,
    size_t len,
    int pipe_fd[2],
    void* userdata,
    keel_reactor_callback_t callback)
{
    (void)pipe_fd;  /* Not used on macOS */
    
    /* Fallback: Queue a read followed by a write
     * This is not zero-copy, but provides the same interface */
    
    /* For now, just return an error - proper implementation would
     * need to chain read/write operations */
    (void)reactor;
    (void)fd_in;
    (void)fd_out;
    (void)len;
    (void)userdata;
    (void)callback;
    
    errno = ENOSYS;
    return -1;
}

/* ============================================================================
 * Linked Operations
 * ============================================================================ */

/**
 * @brief kqueue linked-op stub.
 */
static int kqueue_submit_linked(
    keel_reactor_t* reactor,
    keel_op_t* ops,
    size_t count)
{
    /* kqueue doesn't support linked operations natively
     * We execute them sequentially in callbacks */
    (void)reactor;
    (void)ops;
    (void)count;
    errno = ENOSYS;
    return -1;
}

/* ============================================================================
 * Timeout
 * ============================================================================ */

/**
 * @brief Create a one-shot EVFILT_TIMER-backed timeout.
 */
static int kqueue_timeout(
    keel_reactor_t* reactor,
    uint32_t timeout_ms,
    void* userdata,
    keel_reactor_callback_t callback)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    
    int timer_id = state->next_timer_id++;
    
    /* Use EVFILT_TIMER for timeouts */
    if (state->change_count >= state->change_capacity) {
        size_t new_cap = state->change_capacity * 2;
        struct kevent* new_changes = (struct kevent*)keel_realloc(
            state->changes, new_cap * sizeof(struct kevent)
        );
        if (new_changes == NULL) {
            return -1;
        }
        state->changes = new_changes;
        state->change_capacity = new_cap;
    }
    
    /* Allocate op for callback tracking */
    kqueue_op_t* op = alloc_op(state);
    if (op == NULL) {
        return -1;
    }
    
    op->type = KEEL_OP_TIMEOUT;
    op->fd = timer_id;
    op->userdata = userdata;
    op->callback = callback;
    op->state = KQUEUE_OP_PENDING;
    
    /* Link into timer list (use fd as key) */
    op->next = state->timer_ops;
    state->timer_ops = op;
    
    EV_SET(&state->changes[state->change_count],
           (uintptr_t)timer_id, EVFILT_TIMER, EV_ADD | EV_ONESHOT,
           0, (intptr_t)timeout_ms, op);
    state->change_count++;
    
    return timer_id;
}

/**
 * @brief Remove a pending timeout from both kqueue and local bookkeeping.
 */
static int kqueue_cancel_timeout(keel_reactor_t* reactor, int timer_id)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    
    add_kevent(state, timer_id, EVFILT_TIMER, EV_DELETE, NULL);
    
    /* Find and remove from timer list */
    kqueue_op_t** prev = &state->timer_ops;
    kqueue_op_t* op = state->timer_ops;
    
    while (op != NULL) {
        if (op->fd == timer_id) {
            *prev = op->next;
            free_op(state, op);
            return 0;
        }
        prev = &op->next;
        op = op->next;
    }
    
    return 0;
}

/* ============================================================================
 * Submit / Wait / Process
 * ============================================================================ */

/**
 * @brief Submit queued kevent registration changes without waiting.
 */
static int kqueue_submit(keel_reactor_t* reactor)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    
    if (state->change_count == 0) {
        return 0;
    }
    
    /* Submit changes */
    int result = kevent(state->kq, state->changes, (int)state->change_count,
                        NULL, 0, NULL);
    
    if (result >= 0) {
        reactor->stats.ops_submitted += state->change_count;
        state->change_count = 0;
    }
    
    return result;
}

/**
 * @brief Submit pending changes and wait for readiness events.
 */
static int kqueue_wait(keel_reactor_t* reactor, int timeout_ms)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    
    struct timespec ts;
    struct timespec* ts_ptr = NULL;
    
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
        ts_ptr = &ts;
    }
    
    int result = kevent(state->kq, 
                        state->changes, (int)state->change_count,
                        state->events, (int)state->event_capacity,
                        ts_ptr);
    
    if (result >= 0) {
        state->change_count = 0;  /* Changes consumed */
        state->event_count = result;  /* Store events for process() */
    } else {
        state->event_count = 0;
    }
    
    return result;
}

/**
 * @brief Process the readiness events captured by the last `kqueue_wait()` call.
 *
 * @note This function owns the backend's requeue-on-EAGAIN logic and the
 *       subtle connect completion handling needed for macOS spurious writability.
 */
static int kqueue_process(keel_reactor_t* reactor)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    int processed = 0;
    
    /* Use events already retrieved by wait() */
    int nevents = state->event_count;
    state->event_count = 0;  /* Clear for next iteration */
    
    if (nevents <= 0) {
        return 0;
    }
    
    for (int i = 0; i < nevents; i++) {
        struct kevent* ev = &state->events[i];
        
        if (ev->filter == EVFILT_TIMER) {
            /* Timer event */
            kqueue_op_t* op = (kqueue_op_t*)ev->udata;
            if (op != NULL && op->callback != NULL) {
                op->callback(op->userdata, 0);
                reactor->stats.timeouts++;
            }
            /* Remove from timer list and free */
            kqueue_op_t** prev = &state->timer_ops;
            kqueue_op_t* cur = state->timer_ops;
            while (cur != NULL) {
                if (cur == op) {
                    *prev = cur->next;
                    free_op(state, cur);
                    break;
                }
                prev = &cur->next;
                cur = cur->next;
            }
            processed++;
            continue;
        }
        
        int fd = (int)ev->ident;
        
        if ((size_t)fd >= state->fd_capacity) {
            continue;
        }
        
        if (ev->filter == EVFILT_READ) {
            /* Process read operations */
            kqueue_op_t* op = state->read_ops[fd];
            KEEL_LOG_WARN(KEEL_LOG_CAT_IO, "kqueue EVFILT_READ fd=%d op=%p flags=0x%x",
                         fd, (void*)op, (unsigned)ev->flags);
            if (op != NULL) {
                state->read_ops[fd] = op->next;
                
                int result;
                if (op->type == KEEL_OP_ACCEPT) {
                    result = accept(fd, op->accept_addr, op->accept_addrlen);
                    if (result >= 0) {
                        reactor->stats.accepts++;
                    }
                } else {
                    result = (int)recv(fd, op->buf, op->len, op->flags);
                    if (result > 0) {
                        reactor->stats.bytes_read += (uint64_t)result;
                    }
                }
                
                if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    /* Not ready, re-queue */
                    op->next = state->read_ops[fd];
                    state->read_ops[fd] = op;
                } else {
                    if (op->callback) {
                        op->callback(op->userdata, result < 0 ? -errno : result);
                    }
                    free_op(state, op);
                    reactor->stats.ops_completed++;
                    processed++;
                }
            }
        }
        
        if (ev->filter == EVFILT_WRITE) {
            /* Process write operations */
            kqueue_op_t* op = state->write_ops[fd];
            if (op != NULL) {
                state->write_ops[fd] = op->next;

                int result;
                bool is_connect = (op->type == KEEL_OP_CONNECT);

                if (is_connect) {
                    /* Check if connect succeeded.
                     *
                     * On macOS, EVFILT_WRITE can fire before the TCP
                     * 3-way handshake has finished (spurious wake-up).
                     * In that case getsockopt(SO_ERROR) returns EINPROGRESS
                     * because there is no error YET.  Re-arm the filter
                     * and wait for the real completion event.
                     *
                     * Also note: unlike send(), getsockopt() does NOT set
                     * errno to the socket error — it stores the error in
                     * the out-parameter `err`.  So we must NOT use errno
                     * when reporting the connect result; use `err` directly.
                     */
                    int err = 0;
                    socklen_t len = sizeof(err);
                    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
                        err = errno;   /* getsockopt itself failed */
                    }
                    if (err == EINPROGRESS) {
                        /* Not done yet — put the op back and re-register */
                        op->next = state->write_ops[fd];
                        state->write_ops[fd] = op;
                        add_kevent(state, fd, EVFILT_WRITE, EV_ADD | EV_ONESHOT, NULL);
                        continue;   /* skip to next kevent */
                    }
                    /* encode: 0 = success, negative = -errno-value */
                    result = err ? -err : 0;
                    if (result == 0) {
                        reactor->stats.connects++;
                    }
                } else {
                    result = (int)send(fd, op->buf, op->len, op->flags);
                    if (result > 0) {
                        reactor->stats.bytes_written += (uint64_t)result;
                    }
                }

                if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    /* send() said not ready yet — re-queue (CONNECT never
                     * reaches here because EINPROGRESS is handled above) */
                    op->next = state->write_ops[fd];
                    state->write_ops[fd] = op;
                } else {
                    int cb_result;
                    if (is_connect) {
                        /* For CONNECT: result is already -err or 0.
                         * Do NOT use -errno here — getsockopt does not
                         * set errno to the socket error. */
                        cb_result = result;
                    } else {
                        /* For SEND: send() returns -1 on error and sets
                         * errno, or the byte count on success. */
                        cb_result = result < 0 ? -errno : result;
                    }
                    if (op->callback) {
                        op->callback(op->userdata, cb_result);
                    }
                    free_op(state, op);
                    reactor->stats.ops_completed++;
                    processed++;
                }
            }
        }
        
        if (ev->flags & EV_ERROR) {
            reactor->stats.errors++;
        }
    }
    
    return processed;
}

/**
 * @brief Return the count of outstanding queued operations and timers.
 */
static size_t kqueue_pending(keel_reactor_t* reactor)
{
    kqueue_state_t* state = (kqueue_state_t*)reactor->backend_state;
    return state->pending_count;
}

#endif /* __APPLE__ || BSD */
