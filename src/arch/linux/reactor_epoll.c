/**
 * @file reactor_epoll.c
 * @brief Linux epoll fallback backend for the reactor abstraction.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This backend provides a portability and survivability layer for Linux hosts
 * where `io_uring` is unavailable, disabled, or intentionally bypassed. It
 * uses edge-triggered epoll plus non-blocking syscalls to emulate the same
 * callback-oriented interface exposed by the higher-level reactor API.
 *
 * Important characteristics:
 * - operations are queued in userspace lists keyed by fd
 * - readiness notifications drive actual `recv`, `send`, `accept`, `connect`,
 *   and `splice` execution
 * - timeouts are represented with a shared `timerfd`
 * - linked operations and advanced kernel batching are intentionally limited
 */

#include "keel/reactor/reactor.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"
#include "keel/reactor/reactor_internal.h"
#include "keel/util/platform_compat.h"

#ifdef __linux__

#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Maximum events to process per iteration */
#define KEEL_EPOLL_MAX_EVENTS 256

/* ============================================================================
 * Internal Structures
 * ============================================================================ */

typedef struct keel_epoll_op {
    keel_op_t                base;
    struct keel_epoll_op*    next;
} keel_epoll_op_t;

typedef struct keel_epoll_fd {
    int                     fd;
    uint32_t                events;
    keel_epoll_op_t*         read_ops;
    keel_epoll_op_t*         write_ops;
} keel_epoll_fd_t;

typedef struct keel_epoll_state {
    int                     epoll_fd;
    int                     timer_fd;
    struct epoll_event      events[KEEL_EPOLL_MAX_EVENTS];
    keel_epoll_fd_t*         fd_table;
    size_t                  fd_table_size;
    uint64_t                ops_submitted;
    uint64_t                ops_completed;
} keel_epoll_state_t;

/* ============================================================================
 * Helpers
 * ============================================================================ */

/* set_nonblocking() removed: use keel_set_nonblocking() from keel/util/platform_compat.h */

/**
 * @brief Return the epoll bookkeeping entry for an fd, growing the table if needed.
 *
 * @param st Epoll backend state.
 * @param fd Target file descriptor.
 * @return Table entry on success, or `NULL` on invalid fd/allocation failure.
 */
static keel_epoll_fd_t* get_or_create_fd_entry(keel_epoll_state_t* st, int fd)
{
    if (fd < 0) return NULL;
    if ((size_t)fd >= st->fd_table_size) {
        size_t new_size = (size_t)fd + 256;
        keel_epoll_fd_t* nt = keel_calloc(new_size, sizeof(keel_epoll_fd_t));
        if (!nt) return NULL;
        if (st->fd_table) {
            memcpy(nt, st->fd_table, st->fd_table_size * sizeof(keel_epoll_fd_t));
            keel_free(st->fd_table);
        }
        for (size_t i = st->fd_table_size; i < new_size; i++) nt[i].fd = -1;
        st->fd_table = nt;
        st->fd_table_size = new_size;
    }
    if (st->fd_table[fd].fd == -1) {
        st->fd_table[fd].fd = fd;
        st->fd_table[fd].events = 0;
        st->fd_table[fd].read_ops = NULL;
        st->fd_table[fd].write_ops = NULL;
    }
    return &st->fd_table[fd];
}

/**
 * @brief Reconcile epoll interest bits with the current queued operations.
 *
 * @param st Epoll backend state.
 * @param entry Per-fd state entry.
 * @return `0` on success, `-1` on `epoll_ctl` failure.
 */
static int update_epoll_events(keel_epoll_state_t* st, keel_epoll_fd_t* entry)
{
    /* Level-triggered (no EPOLLET): epoll_wait reports the fd as ready for
     * as long as data is available.  This is critical for the async LDAP/PAM
     * auth path: the thread pool may write to an eventfd *before* the io
     * thread registers it with epoll.  With edge-triggered mode the io thread
     * would never see the notification (no 0→1 transition observed).  With
     * level-triggered mode, epoll_wait fires on the next call once the fd is
     * added, regardless of when the write occurred. */
    uint32_t wanted = 0;
    if (entry->read_ops) wanted |= EPOLLIN;
    if (entry->write_ops) wanted |= EPOLLOUT;
    if (wanted == entry->events) {
        /* Same events — skip syscall UNLESS entry->events != 0 and the fd
         * might have been closed and reopened (stale entry).  When wanted==0
         * and entry->events==0 there is genuinely nothing to do.  When both
         * are non-zero, we *could* have a stale entry where the fd was closed
         * (auto-removed from epoll) and its number re-used for a new fd that
         * also needs EPOLLIN.  We detect this by attempting EPOLL_CTL_MOD; if
         * it fails with ENOENT the fd is no longer in epoll and we fall through
         * to ADD it. */
        if (wanted == 0) return 0;
        struct epoll_event ev_check = { .events = wanted, .data.fd = entry->fd };
        if (epoll_ctl(st->epoll_fd, EPOLL_CTL_MOD, entry->fd, &ev_check) == 0)
            return 0;  /* already registered with correct events — no change */
        if (errno != ENOENT && errno != EBADF) return -1;
        /* Stale entry: fd was removed from epoll (closed). Fall through to ADD. */
        entry->events = 0;
    }

    struct epoll_event ev = { .events = wanted, .data.fd = entry->fd };
    int op = entry->events ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    if (wanted == 0) op = EPOLL_CTL_DEL;

    if (epoll_ctl(st->epoll_fd, op, entry->fd, &ev) < 0) {
        if (op == EPOLL_CTL_DEL) {
            /* DEL failures are benign: the fd may have been closed (EBADF)
             * or auto-removed from epoll when closed (ENOENT).  Either way,
             * reset events to 0 so the next incarnation of this fd number
             * uses EPOLL_CTL_ADD rather than EPOLL_CTL_MOD on a stale entry.
             * Without this reset, the alternating-TIMEOUT bug occurs: the
             * new fd that reuses the same number is never added to epoll. */
            entry->events = 0;
            return 0;
        }
        if (op == EPOLL_CTL_MOD && (errno == ENOENT || errno == EBADF)) {
            /* Stale MOD: fd was closed, try ADD */
            if (epoll_ctl(st->epoll_fd, EPOLL_CTL_ADD, entry->fd, &ev) == 0) {
                entry->events = wanted;
                return 0;
            }
        }
        return -1;
    }
    /* After DEL the fd is no longer registered; record 0 so the next
     * add uses EPOLL_CTL_ADD rather than EPOLL_CTL_MOD on a stale fd. */
    entry->events = (op == EPOLL_CTL_DEL) ? 0 : wanted;
    return 0;
}

/**
 * @brief Queue one logical reactor operation into the epoll backend.
 *
 * @param st Epoll backend state.
 * @param op Operation descriptor.
 * @return `0` on success, `-1` on unsupported type or allocation failure.
 *
 * Behavior:
 * - read-like ops are chained on the per-fd read list
 * - write-like ops are chained on the per-fd write list
 * - close executes synchronously and invokes its callback immediately
 * - timeout is represented by the shared timerfd
 */
static int submit_op(keel_epoll_state_t* st, keel_op_t* op)
{
    if (!op) return -1;
    keel_epoll_op_t* eop = keel_calloc(1, sizeof(keel_epoll_op_t));
    if (!eop) return -1;
    eop->base = *op;
    eop->next = NULL;

    keel_epoll_fd_t* entry = get_or_create_fd_entry(st, op->fd_in);
    if (!entry) { keel_free(eop); return -1; }

    switch (op->type) {
        case KEEL_OP_RECV: case KEEL_OP_ACCEPT: case KEEL_OP_PEEK:
            eop->next = entry->read_ops;
            entry->read_ops = eop;
            break;
        case KEEL_OP_SEND: case KEEL_OP_CONNECT: case KEEL_OP_SPLICE:
            eop->next = entry->write_ops;
            entry->write_ops = eop;
            break;
        case KEEL_OP_CLOSE:
            close(op->fd_in);
            if (op->callback) op->callback(op->userdata, 0);
            keel_free(eop);
            return 0;
        case KEEL_OP_TIMEOUT:
            /* op->fd_in == st->timer_fd; treat as a read event (timerfd fires EPOLLIN) */
            eop->next = entry->read_ops;
            entry->read_ops = eop;
            break;
        default:
            keel_free(eop);
            return -1;
    }
    update_epoll_events(st, entry);
    st->ops_submitted++;
    return 0;
}

/**
 * @brief Drain as many queued read-side operations as current readiness allows.
 *
 * @param st Epoll backend state.
 * @param entry Per-fd state entry.
 * @return Nothing.
 *
 * Corner cases:
 * - stops on `EAGAIN`/`EWOULDBLOCK` to preserve edge-trigger semantics
 * - converts hard syscall failure to negative errno for callbacks
 */
static void process_read(keel_epoll_state_t* st, keel_epoll_fd_t* entry)
{
    while (entry->read_ops) {
        keel_epoll_op_t* op = entry->read_ops;
        keel_op_t* b = &op->base;
        ssize_t res = -1;
        switch (b->type) {
            case KEEL_OP_RECV:
                /* Use read() rather than recv() so this path works for both
                 * regular sockets and non-socket fds such as eventfd (used by
                 * the async LDAP/PAM auth notify path).  For sockets,
                 * read(fd, buf, len) is equivalent to recv(fd, buf, len, 0). */
                res = read(b->fd_in, b->buf, b->len);
                break;
            case KEEL_OP_PEEK:
                res = recv(b->fd_in, b->buf, b->len, MSG_PEEK);
                break;
            case KEEL_OP_ACCEPT: {
                struct sockaddr_storage sa;
                socklen_t sl = sizeof(sa);
                res = accept4(b->fd_in, (struct sockaddr*)&sa, &sl,
                              SOCK_NONBLOCK | SOCK_CLOEXEC);
                break;
            }
            case KEEL_OP_TIMEOUT: {
                /* Drain the timerfd expiry count; callback receives 0 on success.
                 * Keep errno intact so the outer EAGAIN check can break the loop
                 * on a spurious wakeup; clear errno to 0 on genuine expiry. */
                uint64_t exp;
                ssize_t n = read(b->fd_in, &exp, sizeof(exp));
                if (n < 0) {
                    /* res stays -1; errno already set (EAGAIN or real error) */
                } else {
                    errno = 0;  /* success — prevent false EAGAIN detection below */
                    res = 0;
                }
                break;
            }
            default: break;
        }
        if (res < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            res = -errno;
        }
        entry->read_ops = op->next;
        if (b->callback) b->callback(b->userdata, (int)res);
        st->ops_completed++;
        keel_free(op);
    }
    update_epoll_events(st, entry);
}

/**
 * @brief Drain as many queued write-side operations as current readiness allows.
 *
 * @param st Epoll backend state.
 * @param entry Per-fd state entry.
 * @return Nothing.
 */
static void process_write(keel_epoll_state_t* st, keel_epoll_fd_t* entry)
{
    while (entry->write_ops) {
        keel_epoll_op_t* op = entry->write_ops;
        keel_op_t* b = &op->base;
        ssize_t res = -1;
        switch (b->type) {
            case KEEL_OP_SEND:
                res = send(b->fd_in, b->buf, b->len, MSG_NOSIGNAL);
                break;
            case KEEL_OP_CONNECT: {
                int err; socklen_t el = sizeof(err);
                if (getsockopt(b->fd_in, SOL_SOCKET, SO_ERROR, &err, &el) < 0)
                    res = -errno;
                else if (err) res = -err;
                else res = 0;
                break;
            }
            case KEEL_OP_SPLICE:
                res = splice(b->fd_in, NULL, b->fd_out, NULL,
                             b->len, SPLICE_F_NONBLOCK | SPLICE_F_MOVE);
                break;
            default: break;
        }
        if (res < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            res = -errno;
        }
        entry->write_ops = op->next;
        if (b->callback) b->callback(b->userdata, (int)res);
        st->ops_completed++;
        keel_free(op);
    }
    update_epoll_events(st, entry);
}

/* ============================================================================
 * reactor_internal.h interface
 * ============================================================================ */

/**
 * @brief Tear down the epoll backend and free all queued operations.
 *
 * @param r Reactor handle.
 * @return Nothing.
 */
static void ep_destroy(keel_reactor_t* r)
{
    if (!r || !r->backend_state) return;
    keel_epoll_state_t* st = (keel_epoll_state_t*)r->backend_state;
    if (st->fd_table) {
        for (size_t i = 0; i < st->fd_table_size; i++) {
            keel_epoll_fd_t* e = &st->fd_table[i];
            while (e->read_ops)  { keel_epoll_op_t* o = e->read_ops;  e->read_ops  = o->next; keel_free(o); }
            while (e->write_ops) { keel_epoll_op_t* o = e->write_ops; e->write_ops = o->next; keel_free(o); }
        }
        keel_free(st->fd_table);
    }
    if (st->timer_fd >= 0) close(st->timer_fd);
    if (st->epoll_fd >= 0) close(st->epoll_fd);
    keel_free(st);
    r->backend_state = NULL;
}

/**
 * @brief epoll backend no-op fd registration hook.
 */
static int ep_register_fd(keel_reactor_t* r, int fd) { (void)r; return fd; }
/**
 * @brief epoll backend no-op fd unregistration hook.
 */
static void ep_unregister_fd(keel_reactor_t* r, int fd) { (void)r; (void)fd; }

/**
 * @brief Queue an accept operation on the read side of the listening socket.
 */
static int ep_accept(keel_reactor_t* r, int lfd, struct sockaddr* a, socklen_t* al,
                     void* ud, keel_reactor_callback_t cb, bool ms)
{
    (void)a; (void)al; (void)ms;
    keel_op_t op = { .type = KEEL_OP_ACCEPT, .fd_in = lfd, .userdata = ud, .callback = cb };
    return submit_op(r->backend_state, &op);
}

/**
 * @brief Queue a recv or peek operation.
 */
static int ep_recv(keel_reactor_t* r, int fd, void* buf, size_t len, int flags,
                   void* ud, keel_reactor_callback_t cb)
{
    keel_op_t op = {
        .type = (flags & MSG_PEEK) ? KEEL_OP_PEEK : KEEL_OP_RECV,
        .fd_in = fd, .buf = buf, .len = len, .userdata = ud, .callback = cb
    };
    return submit_op(r->backend_state, &op);
}

/**
 * @brief Queue a send operation.
 */
static int ep_send(keel_reactor_t* r, int fd, const void* buf, size_t len,
                   int flags, void* ud, keel_reactor_callback_t cb)
{
    (void)flags;
    keel_op_t op = {
        .type = KEEL_OP_SEND, .fd_in = fd, .buf = (void*)buf,
        .len = len, .userdata = ud, .callback = cb
    };
    return submit_op(r->backend_state, &op);
}

/**
 * @brief Start a non-blocking connect and wait for writability.
 */
static int ep_connect(keel_reactor_t* r, int fd, const struct sockaddr* addr,
                      socklen_t al, void* ud, keel_reactor_callback_t cb)
{
    keel_set_nonblocking(fd);
    int rc = connect(fd, addr, al);
    if (rc == 0) { if (cb) cb(ud, 0); return 0; }
    if (errno != EINPROGRESS) return -1;
    keel_op_t op = { .type = KEEL_OP_CONNECT, .fd_in = fd, .userdata = ud, .callback = cb };
    return submit_op(r->backend_state, &op);
}

/**
 * @brief Queue a logical close operation, which executes synchronously in this backend.
 */
static int ep_close(keel_reactor_t* r, int fd, void* ud, keel_reactor_callback_t cb)
{
    keel_op_t op = { .type = KEEL_OP_CLOSE, .fd_in = fd, .userdata = ud, .callback = cb };
    return submit_op(r->backend_state, &op);
}

/**
 * @brief Queue a splice operation on the write side of the source fd.
 */
static int ep_splice(keel_reactor_t* r, int fdi, int fdo, size_t len,
                     int pfd[2], void* ud, keel_reactor_callback_t cb)
{
    (void)pfd;
    keel_op_t op = {
        .type = KEEL_OP_SPLICE, .fd_in = fdi, .fd_out = fdo,
        .len = len, .userdata = ud, .callback = cb
    };
    return submit_op(r->backend_state, &op);
}

/**
 * @brief Best-effort linked submission emulation for epoll.
 *
 * @note Operations are merely enqueued in order; there is no kernel-level
 *       transactional linkage.
 */
static int ep_submit_linked(keel_reactor_t* r, keel_op_t* ops, size_t cnt)
{
    int ok = 0;
    for (size_t i = 0; i < cnt; i++)
        if (submit_op(r->backend_state, &ops[i]) == 0) ok++;
    return ok;
}

/**
 * @brief Arm the shared timerfd and queue a timeout callback operation.
 */
static int ep_timeout(keel_reactor_t* r, uint32_t ms, void* ud, keel_reactor_callback_t cb)
{
    keel_epoll_state_t* st = r->backend_state;
    if (st->timer_fd < 0) return -1;
    struct itimerspec its = {
        .it_value = { .tv_sec = (time_t)(ms / 1000), .tv_nsec = (long)((ms % 1000) * 1000000L) },
        .it_interval = {0, 0},
    };
    if (timerfd_settime(st->timer_fd, 0, &its, NULL) < 0) return -1;
    keel_op_t op = { .type = KEEL_OP_TIMEOUT, .fd_in = st->timer_fd, .userdata = ud, .callback = cb };
    return submit_op(st, &op);
}

/**
 * @brief epoll timeout cancellation stub.
 */
static int ep_cancel_timeout(keel_reactor_t* r, int tid) { (void)r; (void)tid; return 0; }
/**
 * @brief epoll submit hook is a no-op because registration happens during queueing.
 */
static int ep_submit_fn(keel_reactor_t* r) { (void)r; return 0; }

/**
 * @brief Wait for epoll readiness and process ready operations inline.
 *
 * @param r Reactor handle.
 * @param tms epoll timeout in milliseconds.
 * @return Number of ready fds, `0` for interruption/timeout, or `-1` on error.
 */
static int ep_wait(keel_reactor_t* r, int tms)
{
    keel_epoll_state_t* st = r->backend_state;
    int nfds = epoll_wait(st->epoll_fd, st->events, KEEL_EPOLL_MAX_EVENTS, tms);
    if (nfds < 0) return (errno == EINTR) ? 0 : -1;
    for (int i = 0; i < nfds; i++) {
        int fd = st->events[i].data.fd;
        uint32_t ev = st->events[i].events;
        if ((size_t)fd >= st->fd_table_size || st->fd_table[fd].fd == -1) continue;
        keel_epoll_fd_t* e = &st->fd_table[fd];
        if (ev & (EPOLLIN | EPOLLERR | EPOLLHUP))  process_read(st, e);
        if (ev & (EPOLLOUT | EPOLLERR | EPOLLHUP)) process_write(st, e);
    }
    /* Sync private counters to reactor->stats so keel_reactor_get_stats() is accurate. */
    r->stats.ops_submitted = st->ops_submitted;
    r->stats.ops_completed = st->ops_completed;
    return nfds;
}

/**
 * @brief Process epoll work without blocking.
 */
static int ep_process(keel_reactor_t* r) { return ep_wait(r, 0); }
/**
 * @brief Pending-count stub for the epoll backend.
 */
static size_t ep_pending(keel_reactor_t* r) { (void)r; return 0; }

/* ============================================================================
 * Init (called from reactor_common.c)
 * ============================================================================ */

/**
 * @brief Initialize the Linux epoll backend into a reactor handle.
 *
 * @param reactor Reactor handle allocated by the common factory.
 * @return `0` on success, `-1` on allocation or kernel object creation failure.
 */
int keel_reactor_epoll_init(keel_reactor_t* reactor)
{
    if (!reactor) return -1;

    keel_epoll_state_t* st = keel_calloc(1, sizeof(keel_epoll_state_t));
    if (!st) return -1;

    st->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (st->epoll_fd < 0) { keel_free(st); return -1; }

    st->timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);

    st->fd_table_size = 256;
    st->fd_table = keel_calloc(st->fd_table_size, sizeof(keel_epoll_fd_t));
    if (!st->fd_table) {
        close(st->epoll_fd);
        if (st->timer_fd >= 0) close(st->timer_fd);
        keel_free(st);
        return -1;
    }
    for (size_t i = 0; i < st->fd_table_size; i++) st->fd_table[i].fd = -1;

    reactor->backend_state  = st;
    reactor->destroy        = ep_destroy;
    reactor->register_fd    = ep_register_fd;
    reactor->unregister_fd  = ep_unregister_fd;
    reactor->accept         = ep_accept;
    reactor->recv           = ep_recv;
    reactor->send           = ep_send;
    reactor->connect        = ep_connect;
    reactor->close_fd       = ep_close;
    reactor->splice         = ep_splice;
    reactor->submit_linked  = ep_submit_linked;
    reactor->timeout        = ep_timeout;
    reactor->cancel_timeout = ep_cancel_timeout;
    reactor->submit         = ep_submit_fn;
    reactor->wait           = ep_wait;
    reactor->process        = ep_process;
    reactor->pending        = ep_pending;

    return 0;
}

#endif /* __linux__ */
