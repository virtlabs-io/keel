/**
 * @file reactor_iouring.c
 * @brief Primary Linux io_uring backend for KEEL's reactor abstraction.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This backend is the preferred data-plane reactor on Linux. It translates the
 * reactor API into io_uring SQEs/CQEs, supports linked send/recv chains,
 * splice-based zero-copy forwarding, provider buffer rings, and optional
 * multishot receive/accept behavior.
 *
 * Key correctness points:
 * - pending operation records live in a fixed pool so SQE user_data pointers
 *   never become invalid
 * - multishot recv bookkeeping is keyed by fd and must be cleared when the
 *   kernel stops delivering `IORING_CQE_F_MORE`
 * - completion metadata is exported through thread-local bridge helpers so
 *   higher layers can attribute reactor scheduling latency
 */

#if defined(__linux__) && defined(KEEL_HAS_IOURING)

#include "keel/reactor/reactor.h"
#include "keel/log/log.h"
#include "keel/reactor/reactor_internal.h"
#include "keel/mem/mem.h"
#include "keel/engine/worker.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <time.h>
#include <liburing.h>

/* ============================================================================
 * Internal Structures
 * ============================================================================ */

typedef struct iouring_op {
    void*                   userdata;
    keel_reactor_callback_t  callback;
    keel_op_type_t           type;
    bool                    multishot;
    bool                    in_use;     /* Slot is currently in-flight */
    void*                   orig_buf;   /* Original caller buf for buf-ring copy-on-completion */
    int                     fd;         /* Associated fd (multishot recv tracking) */
    int                     timer_id;   /* Non-zero for KEEL_OP_TIMEOUT slots, used by cancel_timeout() */
    /*
     * ts — io_uring timeout timespec stored in the op slot rather than on the
     * caller's stack.  io_uring_prep_timeout() records a *pointer* into the SQE
     * and the kernel follows that pointer asynchronously during
     * io_uring_submit_and_wait_timeout().  If the timespec were declared as a
     * local variable in iouring_timeout(), its stack frame would be gone by the
     * time the kernel reads it, causing garbage timeout values and silent hangs.
     * Storing it here is safe because pending_ops is heap-allocated and never
     * reallocated (see the "Operation tracking" comment below).
     */
    struct __kernel_timespec ts;        /* Timeout timespec — must outlive the SQE submission */
} iouring_op_t;

typedef struct iouring_state {
    struct io_uring         ring;
    bool                    ring_initialized;
    
    /* Registered file descriptors */
    int*                    registered_fds;
    size_t                  max_registered;
    size_t                  num_registered;
    
    /* Operation tracking — fixed-size pool with free-list to avoid realloc.
     * Pointers into this array are stored as SQE user_data, so the array
     * must NEVER be reallocated (that would invalidate all in-flight ptrs). */
    iouring_op_t*           pending_ops;
    size_t                  pending_capacity;
    size_t                  pending_count;  /* In-use high-water mark for stats */
    size_t                  submitted_count;
    uint32_t*               free_stack;     /* Stack of free slot indices */
    size_t                  free_top;       /* Top of free stack */
    
    /* Timer ID counter */
    int                     next_timer_id;

#ifdef KEEL_HAS_URING_BUF_RING
    /* io_uring buffer ring (registered recv buffers, Linux 5.19+ / liburing 2.2+) */
    struct io_uring_buf_ring*  buf_ring;            /* Kernel-mapped buffer ring */
    uint8_t**                  buf_ring_bufs;       /* Per-slot buffer base addresses */
    size_t                     buf_ring_count;      /* Number of slots in the ring */
    size_t                     buf_ring_buf_size;   /* Size of each buffer (KEEL_RECV_BUF_SIZE) */
    bool                       use_buf_rings;       /* Buffer rings successfully initialised */

#ifdef KEEL_HAS_URING_RECV_MULTISHOT
    /* Per-fd tracking table for active multishot recv SQEs (Linux 6.0+ / liburing 2.3+).
     * mshot_recv_slot[fd] = pending_ops slot index of the live multishot recv for that
     * fd, or UINT32_MAX if no multishot recv is currently armed.  Sized to fd_cap at
     * init; fds >= fd_cap fall back to single-shot buf-ring recv. */
    uint32_t*                  mshot_recv_slot;     /* fd → op slot; UINT32_MAX = none */
    size_t                     mshot_recv_cap;      /* allocated size of mshot_recv_slot */
#endif /* KEEL_HAS_URING_RECV_MULTISHOT */
#endif /* KEEL_HAS_URING_BUF_RING */
} iouring_state_t;

typedef struct iouring_completion {
    iouring_op_t* op;
    int           res;
    unsigned      flags;
    uint32_t      batch_index;
} iouring_completion_t;

/* ============================================================================
 * Forward Declarations
 * ============================================================================ */

static void iouring_destroy(keel_reactor_t* reactor);
static int iouring_register_fd(keel_reactor_t* reactor, int fd);
static void iouring_unregister_fd(keel_reactor_t* reactor, int fd);
static int iouring_accept(keel_reactor_t* reactor, int listen_fd,
                          struct sockaddr* addr, socklen_t* addrlen,
                          void* userdata, keel_reactor_callback_t callback,
                          bool multishot);
static int iouring_recv(keel_reactor_t* reactor, int fd, void* buf, size_t len,
                        int flags, void* userdata, keel_reactor_callback_t callback);
static int iouring_send(keel_reactor_t* reactor, int fd, const void* buf,
                        size_t len, int flags, void* userdata,
                        keel_reactor_callback_t callback);
static int iouring_connect(keel_reactor_t* reactor, int fd,
                           const struct sockaddr* addr, socklen_t addrlen,
                           void* userdata, keel_reactor_callback_t callback);
static int iouring_close_fd(keel_reactor_t* reactor, int fd, void* userdata,
                         keel_reactor_callback_t callback);
static int iouring_splice(keel_reactor_t* reactor, int fd_in, int fd_out,
                          size_t len, int pipe_fd[2], void* userdata,
                          keel_reactor_callback_t callback);
static int iouring_submit_linked(keel_reactor_t* reactor, keel_op_t* ops,
                                 size_t count);
static int iouring_chain_send_recv(keel_reactor_t* reactor,
                        int send_fd, const void* send_buf, size_t send_len,
                        int send_flags, void* send_userdata,
                        keel_reactor_callback_t on_send_done,
                        int recv_fd, void* recv_buf, size_t recv_len,
                        int recv_flags, void* recv_userdata,
                        keel_reactor_callback_t on_recv_done);
static int iouring_timeout(keel_reactor_t* reactor, uint32_t timeout_ms,
                           void* userdata, keel_reactor_callback_t callback);
static int iouring_cancel_timeout(keel_reactor_t* reactor, int timer_id);
static int iouring_submit(keel_reactor_t* reactor);
static int iouring_wait(keel_reactor_t* reactor, int timeout_ms);
static int iouring_process(keel_reactor_t* reactor);
static size_t iouring_pending(keel_reactor_t* reactor);

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

/**
 * @brief Acquire one in-flight operation slot from the fixed pending-op pool.
 */
static iouring_op_t* alloc_pending_op(iouring_state_t* state)
{
    if (state->free_top == 0) {
        /* No free slots — pool exhausted.  The pool is sized to match
         * the io_uring queue depth so this should be very rare. */
        return NULL;
    }
    
    uint32_t idx = state->free_stack[--state->free_top];
    iouring_op_t* op = &state->pending_ops[idx];
    memset(op, 0, sizeof(*op));
    op->in_use = true;
    state->pending_count++;
    return op;
}

/**
 * @brief Return an in-flight operation slot to the pending-op free stack.
 */
static void free_pending_op(iouring_state_t* state, iouring_op_t* op)
{
    if (!op || !op->in_use) return;
    op->in_use = false;
    op->callback = NULL;
    op->userdata = NULL;
    uint32_t idx = (uint32_t)(op - state->pending_ops);
    state->free_stack[state->free_top++] = idx;
    if (state->pending_count > 0) state->pending_count--;
}

/**
 * @brief Get an SQE, flushing the ring if full.
 *
 * Under high concurrency, the SQ ring can fill up between submit cycles.
 * When io_uring_get_sqe() returns NULL (ring full), we submit what's queued
 * so the kernel can start processing, then retry.  Without this, every
 * operation (recv, send, accept) fails with EAGAIN and the proxy stalls.
 */
static struct io_uring_sqe* get_sqe_or_flush(keel_reactor_t* reactor, iouring_state_t* state)
{
    struct io_uring_sqe* sqe = io_uring_get_sqe(&state->ring);
    if (sqe == NULL) {
        reactor->stats.sq_overflow++;
        /* SQ ring full — submit what we have so far, then retry */
        io_uring_submit(&state->ring);
        sqe = io_uring_get_sqe(&state->ring);
    }
    return sqe;
}

/**
 * @brief Read the monotonic clock in nanoseconds for completion timing metadata.
 */
static inline uint64_t iouring_time_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ============================================================================
 * Initialization
 * ============================================================================ */

/**
 * @brief Initialize the io_uring backend into a reactor handle.
 *
 * @param reactor Reactor handle allocated by the common factory.
 * @return `0` on success, `-1` on kernel/liburing/allocation failure.
 *
 * Behavior:
 * - retries ring initialization with smaller queue depth under resource pressure
 * - allocates a fixed pending-op pool sized to outlive in-flight CQEs
 * - optionally initializes provider buffer rings and multishot recv tracking
 */
int keel_reactor_iouring_init(keel_reactor_t* reactor)
{
    iouring_state_t* state = (iouring_state_t*)keel_calloc(1, sizeof(iouring_state_t));
    if (state == NULL) {
        return -1;
    }
    
    /* Initialize io_uring — retry with halved depth on resource exhaustion */
    struct io_uring_params params;
    uint32_t depth = reactor->config.queue_depth;
    int result;
    while (depth >= 4) {
        memset(&params, 0, sizeof(params));
        if (reactor->config.sqpoll) {
            params.flags |= IORING_SETUP_SQPOLL;
            params.sq_thread_idle = reactor->config.sqpoll_idle_ms;
        }
        result = io_uring_queue_init_params(depth, &state->ring, &params);
        if (result == 0) break;
        if (result != -EMFILE && result != -ENOMEM && result != -ENFILE) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "io_uring_queue_init failed: %s", strerror(-result));
            keel_free(state);
            return -1;
        }
        /* Resource limit hit — retry with half the depth */
        depth /= 2;
    }
    if (result < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "io_uring_queue_init failed (depth=%u): %s",
                    depth, strerror(-result));
        keel_free(state);
        return -1;
    }
    reactor->config.queue_depth = depth;
    
    state->ring_initialized = true;
    
    /* Initialize registered FD table */
    state->max_registered = reactor->config.max_fds;
    if (reactor->config.register_fds && state->max_registered > 0) {
        state->registered_fds = (int*)keel_malloc(state->max_registered * sizeof(int));
        if (state->registered_fds != NULL) {
            for (size_t i = 0; i < state->max_registered; i++) {
                state->registered_fds[i] = -1;
            }
        }
    }
    
    /* Initialize pending operations pool — fixed-size to avoid realloc.
     * Sized to 2× the SQ depth: SQ entries are freed when the kernel
     * picks them up, but the pending_op must live until the CQE is
     * processed in iouring_process().  The CQ ring itself is 2× SQ
     * by default, so we match that to avoid pool exhaustion under
     * sustained high concurrency. */
    state->pending_capacity = reactor->config.queue_depth * 2;
    state->pending_ops = (iouring_op_t*)keel_calloc(
        state->pending_capacity,
        sizeof(iouring_op_t)
    );
    state->free_stack = (uint32_t*)keel_malloc(
        state->pending_capacity * sizeof(uint32_t)
    );
    if (state->pending_ops == NULL || state->free_stack == NULL) {
        io_uring_queue_exit(&state->ring);
        keel_free(state->registered_fds);
        keel_free(state->pending_ops);
        keel_free(state->free_stack);
        keel_free(state);
        return -1;
    }
    /* Fill free stack — all slots are initially free */
    state->free_top = state->pending_capacity;
    for (size_t i = 0; i < state->pending_capacity; i++) {
        state->free_stack[i] = (uint32_t)i;
    }
    
    state->next_timer_id = 1;
    
    /* Set backend state and function pointers */
    reactor->backend_state = state;
    reactor->destroy = iouring_destroy;
    reactor->register_fd = iouring_register_fd;
    reactor->unregister_fd = iouring_unregister_fd;
    reactor->accept = iouring_accept;
    reactor->recv = iouring_recv;
    reactor->send = iouring_send;
    reactor->connect = iouring_connect;
    reactor->close_fd = iouring_close_fd;
    reactor->splice = iouring_splice;
    reactor->submit_linked = iouring_submit_linked;
    reactor->chain_send_recv = iouring_chain_send_recv;
    reactor->timeout = iouring_timeout;
    reactor->cancel_timeout = iouring_cancel_timeout;
    reactor->submit = iouring_submit;
    reactor->wait = iouring_wait;
    reactor->process = iouring_process;
    reactor->pending = iouring_pending;

#ifdef KEEL_HAS_URING_BUF_RING
    /* ----------------------------------------------------------------
     * Optional: register a provider buffer ring so the kernel can pick
     * recv buffers automatically (IOSQE_BUFFER_SELECT). This eliminates
     * the need to pass a buffer pointer per-recv SQE.
     *
     * Requires: Linux 5.19+ / liburing 2.2+
     * ---------------------------------------------------------------- */
    if (reactor->config.use_buf_rings) {
        /* Determine ring size — default to queue_depth if not set.
         * Must be a power of two for the buf ring mask to work. */
        size_t ring_count = (reactor->config.buf_ring_size > 0)
                            ? (size_t)reactor->config.buf_ring_size
                            : (size_t)reactor->config.queue_depth;
        size_t pow2 = 1;
        while (pow2 < ring_count) pow2 <<= 1;
        ring_count = pow2;

        /* mmap the ring memory (kernel will use it in-place).
         * Size = sizeof(struct io_uring_buf) * nentries, aligned to page. */
        size_t ring_mem_size = sizeof(struct io_uring_buf) * ring_count;
        struct io_uring_buf_ring *br = (struct io_uring_buf_ring *)mmap(
            NULL, ring_mem_size,
            PROT_READ | PROT_WRITE,
            MAP_ANONYMOUS | MAP_PRIVATE,
            -1, 0);

        if (br == MAP_FAILED) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "buf ring mmap failed (errno=%d): "
                "falling back to per-recv buffers", errno);
        } else {
            /* Initialise tail to 0 */
            br->tail = 0;

            /* Register with the kernel */
            struct io_uring_buf_reg reg;
            memset(&reg, 0, sizeof(reg));
            reg.ring_addr    = (uint64_t)(uintptr_t)br;
            reg.ring_entries = (uint32_t)ring_count;
            reg.bgid         = 0;   /* buffer group id 0 */

            int reg_ret = io_uring_register_buf_ring(&state->ring, &reg, 0);
            if (reg_ret != 0) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "io_uring buf ring registration failed (err=%d): "
                    "falling back to per-recv buffers", reg_ret);
                munmap(br, ring_mem_size);
            } else {
                /* Allocate per-slot heap buffers */
                state->buf_ring_bufs = (uint8_t**)keel_calloc(ring_count,
                                                               sizeof(uint8_t*));
                if (state->buf_ring_bufs == NULL) {
                    KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                        "buf ring bufs[] alloc OOM: "
                        "falling back to per-recv buffers");
                    io_uring_unregister_buf_ring(&state->ring, 0);
                    munmap(br, ring_mem_size);
                } else {
                    state->buf_ring          = br;
                    state->buf_ring_count    = ring_count;
                    state->buf_ring_buf_size = KEEL_RECV_BUF_SIZE;

                    const unsigned ring_mask = io_uring_buf_ring_mask(
                                                   (uint32_t)ring_count);
                    bool alloc_ok = true;

                    for (size_t i = 0; i < ring_count; i++) {
                        state->buf_ring_bufs[i] =
                            (uint8_t*)keel_malloc(KEEL_RECV_BUF_SIZE);
                        if (state->buf_ring_bufs[i] == NULL) {
                            alloc_ok = false;
                            break;
                        }
                        io_uring_buf_ring_add(state->buf_ring,
                                             (void*)state->buf_ring_bufs[i],
                                             (unsigned)KEEL_RECV_BUF_SIZE,
                                             (uint16_t)i,
                                             ring_mask,
                                             (int)i);
                    }

                    if (!alloc_ok) {
                        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                            "buf ring OOM at slot alloc: "
                            "falling back to per-recv buffers");
                        io_uring_unregister_buf_ring(&state->ring, 0);
                        for (size_t i = 0; i < ring_count; i++)
                            keel_free(state->buf_ring_bufs[i]);
                        keel_free(state->buf_ring_bufs);
                        munmap(br, ring_mem_size);
                        state->buf_ring      = NULL;
                        state->buf_ring_bufs = NULL;
                        state->buf_ring_count = 0;
                    } else {
                        /* Commit all buffers to the kernel in one shot */
                        io_uring_buf_ring_advance(state->buf_ring,
                                                  (int)ring_count);
                        state->use_buf_rings = true;
                        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                            "io_uring buf ring ready: "
                            "%zu slots × %zu bytes (bgid=0)",
                            ring_count, (size_t)KEEL_RECV_BUF_SIZE);

#ifdef KEEL_HAS_URING_RECV_MULTISHOT
                        /* Allocate per-fd multishot-recv tracking table.
                         * 65536 × 4 bytes = 256 KiB — covers all fds that
                         * fit below the common RLIMIT_NOFILE ceiling. */
                        const size_t ms_cap = 65536u;
                        state->mshot_recv_slot = (uint32_t*)keel_malloc(
                                                     ms_cap * sizeof(uint32_t));
                        if (state->mshot_recv_slot != NULL) {
                            for (size_t j = 0; j < ms_cap; j++)
                                state->mshot_recv_slot[j] = UINT32_MAX;
                            state->mshot_recv_cap = ms_cap;
                            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                                "io_uring multishot recv enabled "
                                "(one SQE per fd, Linux 6.0+ / liburing 2.3+)");
                        } else {
                            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                                "mshot_recv_slot OOM: "
                                "multishot recv disabled, using single-shot");
                        }
#endif /* KEEL_HAS_URING_RECV_MULTISHOT */
                    }
                }
            }
        }
    }
#endif /* KEEL_HAS_URING_BUF_RING */

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "io_uring reactor initialized (depth=%u, sqpoll=%s)",
                reactor->config.queue_depth,
                reactor->config.sqpoll ? "yes" : "no");
    
    return 0;
}

/* ============================================================================
 * Destruction
 * ============================================================================ */

/**
 * @brief Tear down the io_uring backend and unregister auxiliary resources.
 */
static void iouring_destroy(keel_reactor_t* reactor)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    if (state == NULL) {
        return;
    }
    
    if (state->ring_initialized) {
        /* Cancel all in-flight SQEs before closing the ring.
         *
         * io_uring_queue_exit() → close(ring_fd) blocks in kernel space
         * (io_ring_ctx_wait_and_kill) until every pending SQE either
         * completes or is cancelled.  For pool-warmup TCP connect SQEs where
         * a SYN is in-flight but the backend is unreachable, the kernel blocks
         * for the full TCP retransmission timeout unless we explicitly cancel.
         *
         * Algorithm:
         *  1. Submit an ASYNC_CANCEL SQE for every live op (NULL user_data so
         *     the drain loop ignores the cancel CQEs themselves).
         *  2. Spin on io_uring_submit_and_wait_timeout() draining the CQ ring
         *     until pending_count reaches zero or a 5-second safety cap.
         *     free_pending_op() is called directly (no callbacks) because the
         *     reactor is being torn down and upper layers have already cleaned
         *     up their state.
         *  3. After all ops have completed/cancelled, io_uring_queue_exit() is
         *     instant because no in-flight refs remain.
         */
        if (state->pending_count > 0) {
            for (size_t i = 0; i < state->pending_capacity; i++) {
                iouring_op_t* op = &state->pending_ops[i];
                if (!op->in_use) continue;
                struct io_uring_sqe* sqe = io_uring_get_sqe(&state->ring);
                if (!sqe) {
                    /* SQ ring full — flush so we can get a fresh slot */
                    io_uring_submit(&state->ring);
                    sqe = io_uring_get_sqe(&state->ring);
                    if (!sqe) break;
                }
                io_uring_prep_cancel(sqe, op, 0);
                io_uring_sqe_set_data(sqe, NULL);  /* ignore cancel CQE */
            }

            /* Drain CQ ring until all tracked ops complete or cancel.
             * Safety cap: 500 × 10 ms = 5 s maximum wait. */
            int max_iters = 500;
            while (state->pending_count > 0 && max_iters-- > 0) {
                struct __kernel_timespec ts = { .tv_sec = 0,
                                                .tv_nsec = 10000000L }; /* 10 ms */
                struct io_uring_cqe* cqe;
                io_uring_submit_and_wait_timeout(&state->ring, &cqe, 1, &ts, NULL);

                unsigned head;
                int consumed = 0;
                io_uring_for_each_cqe(&state->ring, head, cqe) {
                    iouring_op_t* op = (iouring_op_t*)io_uring_cqe_get_data(cqe);
                    if (op != NULL) {
                        free_pending_op(state, op);  /* sets in_use=false, decrements count */
                    }
                    consumed++;
                }
                if (consumed > 0)
                    io_uring_cq_advance(&state->ring, (unsigned)consumed);
            }
        }

#ifdef KEEL_HAS_URING_BUF_RING
        if (state->use_buf_rings && state->buf_ring != NULL) {
#ifdef KEEL_HAS_URING_RECV_MULTISHOT
            keel_free(state->mshot_recv_slot);
            state->mshot_recv_slot = NULL;
#endif
            io_uring_unregister_buf_ring(&state->ring, 0);
            size_t ring_mem_size = sizeof(struct io_uring_buf)
                                   * state->buf_ring_count;
            if (state->buf_ring_bufs != NULL) {
                for (size_t i = 0; i < state->buf_ring_count; i++)
                    keel_free(state->buf_ring_bufs[i]);
                keel_free(state->buf_ring_bufs);
            }
            munmap(state->buf_ring, ring_mem_size);
            state->buf_ring      = NULL;
            state->buf_ring_bufs = NULL;
        }
#endif /* KEEL_HAS_URING_BUF_RING */
        io_uring_queue_exit(&state->ring);
    }
    
    keel_free(state->registered_fds);
    keel_free(state->pending_ops);
    keel_free(state->free_stack);
    keel_free(state);
    
    reactor->backend_state = NULL;
}

/* ============================================================================
 * File Descriptor Registration
 * ============================================================================ */

/**
 * @brief Register a file descriptor with the io_uring file table when enabled.
 */
static int iouring_register_fd(keel_reactor_t* reactor, int fd)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    
    if (state->registered_fds == NULL) {
        return fd;  /* Registration not enabled */
    }
    
    /* Find an empty slot */
    for (size_t i = 0; i < state->max_registered; i++) {
        if (state->registered_fds[i] == -1) {
            state->registered_fds[i] = fd;
            state->num_registered++;
            
            /* Register with io_uring */
            int result = io_uring_register_files_update(
                &state->ring, (unsigned)i, &fd, 1
            );
            if (result < 0) {
                state->registered_fds[i] = -1;
                state->num_registered--;
                return fd;  /* Fall back to regular fd */
            }
            
            return (int)i;  /* Return registered index */
        }
    }
    
    return fd;  /* No slots available */
}

/**
 * @brief Remove a registered file descriptor from the io_uring file table.
 */
static void iouring_unregister_fd(keel_reactor_t* reactor, int fd)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    
    if (state->registered_fds == NULL) {
        return;
    }
    
    /* Find and remove the fd */
    for (size_t i = 0; i < state->max_registered; i++) {
        if (state->registered_fds[i] == fd) {
            int neg_one = -1;
            io_uring_register_files_update(&state->ring, (unsigned)i, &neg_one, 1);
            state->registered_fds[i] = -1;
            state->num_registered--;
            return;
        }
    }
}

/* ============================================================================
 * Accept
 * ============================================================================ */

/**
 * @brief Queue an accept or multishot accept SQE.
 */
static int iouring_accept(
    keel_reactor_t* reactor,
    int listen_fd,
    struct sockaddr* addr,
    socklen_t* addrlen,
    void* userdata,
    keel_reactor_callback_t callback,
    bool multishot)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    
    struct io_uring_sqe* sqe = get_sqe_or_flush(reactor, state);
    if (sqe == NULL) {
        errno = EAGAIN;
        return -1;
    }
    
    /* Allocate pending op */
    iouring_op_t* op = alloc_pending_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->userdata = userdata;
    op->callback = callback;
    op->type = KEEL_OP_ACCEPT;
    op->multishot = multishot;
    
    if (multishot) {
        /* Use multishot accept (Linux 5.19+) */
        io_uring_prep_multishot_accept(sqe, listen_fd, addr, addrlen, 0);
    } else {
        io_uring_prep_accept(sqe, listen_fd, addr, addrlen, 0);
    }
    
    io_uring_sqe_set_data(sqe, op);
    
    return 0;
}

/* ============================================================================
 * Recv
 * ============================================================================ */

/**
 * @brief Queue a recv SQE, optionally using buffer rings and multishot recv.
 *
 * @note When a multishot recv is already armed for an fd, this function may
 *       only update callback/destination metadata instead of submitting a new SQE.
 */
static int iouring_recv(
    keel_reactor_t* reactor,
    int fd,
    void* buf,
    size_t len,
    int flags,
    void* userdata,
    keel_reactor_callback_t callback)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;

#if defined(KEEL_HAS_URING_BUF_RING) && defined(KEEL_HAS_URING_RECV_MULTISHOT)
    /* Fast path: a multishot recv SQE is already armed for this fd.
     * Update the op's destination fields so the next CQE delivers data to
     * the NEW buffer/callback supplied by the caller, then return without
     * submitting a new SQE.  This is safe because workers are single-threaded
     * and the update happens before the next event-loop iteration picks up
     * any pending CQE for this fd. */
    if (state->use_buf_rings
            && state->mshot_recv_slot != NULL
            && fd >= 0
            && (size_t)(unsigned)fd < state->mshot_recv_cap) {
        uint32_t slot = state->mshot_recv_slot[(size_t)(unsigned)fd];
        if (slot != UINT32_MAX && slot < (uint32_t)state->pending_capacity) {
            iouring_op_t* active = &state->pending_ops[slot];
            if (active->in_use && active->type == KEEL_OP_RECV && active->multishot) {
                active->orig_buf  = buf;
                active->userdata  = userdata;
                active->callback  = callback;
                return 0;   /* SQE still armed — no new submission */
            }
            /* Stale entry (op was freed but slot not cleared) — fall through */
            state->mshot_recv_slot[(size_t)(unsigned)fd] = UINT32_MAX;
        }
    }
#endif /* KEEL_HAS_URING_BUF_RING && KEEL_HAS_URING_RECV_MULTISHOT */

    struct io_uring_sqe* sqe = get_sqe_or_flush(reactor, state);
    if (sqe == NULL) {
        errno = EAGAIN;
        return -1;
    }
    
    iouring_op_t* op = alloc_pending_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->userdata  = userdata;
    op->callback  = callback;
    op->type      = KEEL_OP_RECV;
    op->multishot = false;
    op->fd        = -1;

#ifdef KEEL_HAS_URING_BUF_RING
    if (state->use_buf_rings) {
        /* Let the kernel pick a buffer from the registered buf ring.
         * We save the caller's destination so we can copy on CQE completion. */
        op->orig_buf = buf;
        op->fd       = fd;

#ifdef KEEL_HAS_URING_RECV_MULTISHOT
        /* Multishot recv: one SQE stays armed and delivers a CQE per message.
         * Only possible when the per-fd tracking table covers this fd. */
        if (state->mshot_recv_slot != NULL
                && (size_t)(unsigned)fd < state->mshot_recv_cap) {
            io_uring_prep_recv_multishot(sqe, fd, NULL,
                                         state->buf_ring_buf_size, 0);
            sqe->buf_group = 0;
            sqe->flags    |= IOSQE_BUFFER_SELECT;
            op->multishot  = true;
            state->mshot_recv_slot[(size_t)(unsigned)fd] =
                (uint32_t)(op - state->pending_ops);
        } else {
            /* fd >= cap: fall back to single-shot buf-ring recv */
            io_uring_prep_recv(sqe, fd, NULL, state->buf_ring_buf_size, 0);
            sqe->buf_group = 0;
            sqe->flags    |= IOSQE_BUFFER_SELECT;
        }
#else  /* !KEEL_HAS_URING_RECV_MULTISHOT */
        io_uring_prep_recv(sqe, fd, NULL, state->buf_ring_buf_size, 0);
        sqe->buf_group = 0;
        sqe->flags    |= IOSQE_BUFFER_SELECT;
#endif /* KEEL_HAS_URING_RECV_MULTISHOT */

    } else {
        op->orig_buf = NULL;
        io_uring_prep_recv(sqe, fd, buf, len, flags);
    }
#else  /* !KEEL_HAS_URING_BUF_RING */
    op->orig_buf = NULL;
    io_uring_prep_recv(sqe, fd, buf, len, flags);
#endif /* KEEL_HAS_URING_BUF_RING */

    io_uring_sqe_set_data(sqe, op);
    
    return 0;
}

/* ============================================================================
 * Send
 * ============================================================================ */

/**
 * @brief Queue a send SQE.
 */
static int iouring_send(
    keel_reactor_t* reactor,
    int fd,
    const void* buf,
    size_t len,
    int flags,
    void* userdata,
    keel_reactor_callback_t callback)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    
    struct io_uring_sqe* sqe = get_sqe_or_flush(reactor, state);
    if (sqe == NULL) {
        errno = EAGAIN;
        return -1;
    }
    
    iouring_op_t* op = alloc_pending_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->userdata = userdata;
    op->callback = callback;
    op->type = KEEL_OP_SEND;
    op->multishot = false;
    
    io_uring_prep_send(sqe, fd, buf, len, flags);
    io_uring_sqe_set_data(sqe, op);
    
    return 0;
}

/* ============================================================================
 * Connect
 * ============================================================================ */

/**
 * @brief Queue a connect SQE.
 */
static int iouring_connect(
    keel_reactor_t* reactor,
    int fd,
    const struct sockaddr* addr,
    socklen_t addrlen,
    void* userdata,
    keel_reactor_callback_t callback)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    
    struct io_uring_sqe* sqe = get_sqe_or_flush(reactor, state);
    if (sqe == NULL) {
        errno = EAGAIN;
        return -1;
    }
    
    iouring_op_t* op = alloc_pending_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->userdata = userdata;
    op->callback = callback;
    op->type = KEEL_OP_CONNECT;
    op->multishot = false;
    
    io_uring_prep_connect(sqe, fd, addr, addrlen);
    io_uring_sqe_set_data(sqe, op);
    
    return 0;
}

/* ============================================================================
 * Close
 * ============================================================================ */

/**
 * @brief Queue a close SQE.
 */
static int iouring_close_fd(
    keel_reactor_t* reactor,
    int fd,
    void* userdata,
    keel_reactor_callback_t callback)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    
    struct io_uring_sqe* sqe = get_sqe_or_flush(reactor, state);
    if (sqe == NULL) {
        errno = EAGAIN;
        return -1;
    }
    
    iouring_op_t* op = alloc_pending_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->userdata = userdata;
    op->callback = callback;
    op->type = KEEL_OP_CLOSE;
    op->multishot = false;
    
    io_uring_prep_close(sqe, fd);
    io_uring_sqe_set_data(sqe, op);
    
    return 0;
}

/* ============================================================================
 * Splice (Zero-Copy)
 * ============================================================================ */

/**
 * @brief Queue a two-stage linked splice chain.
 *
 * @note The first SQE copies from source fd into the pipe, and the second SQE
 *       drains the pipe into the destination fd. Only the second completion is
 *       surfaced to the caller.
 */
static int iouring_splice(
    keel_reactor_t* reactor,
    int fd_in,
    int fd_out,
    size_t len,
    int pipe_fd[2],
    void* userdata,
    keel_reactor_callback_t callback)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    
    /* Splice requires two operations:
     * 1. splice from fd_in to pipe_fd[1]
     * 2. splice from pipe_fd[0] to fd_out
     * We link them together for atomic execution */
    
    struct io_uring_sqe* sqe1 = get_sqe_or_flush(reactor, state);
    struct io_uring_sqe* sqe2 = get_sqe_or_flush(reactor, state);
    
    if (sqe1 == NULL || sqe2 == NULL) {
        errno = EAGAIN;
        return -1;
    }
    
    iouring_op_t* op = alloc_pending_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->userdata = userdata;
    op->callback = callback;
    op->type = KEEL_OP_SPLICE;
    op->multishot = false;
    
    /* First splice: fd_in → pipe write end */
    io_uring_prep_splice(sqe1, fd_in, -1, pipe_fd[1], -1,
                         (unsigned int)len, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
    sqe1->flags |= IOSQE_IO_LINK;  /* Link to next SQE */
    io_uring_sqe_set_data(sqe1, NULL);  /* No callback for first part */
    
    /* Second splice: pipe read end → fd_out */
    io_uring_prep_splice(sqe2, pipe_fd[0], -1, fd_out, -1,
                         (unsigned int)len, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
    io_uring_sqe_set_data(sqe2, op);  /* Callback on completion */
    
    return 0;
}

/* ============================================================================
 * Linked Operations
 * ============================================================================ */

/**
 * @brief Queue a caller-supplied linked chain of simple recv/send/peek operations.
 */
static int iouring_submit_linked(
    keel_reactor_t* reactor,
    keel_op_t* ops,
    size_t count)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    
    if (count == 0) {
        return 0;
    }
    
    for (size_t i = 0; i < count; i++) {
        keel_op_t* op = &ops[i];
        
        struct io_uring_sqe* sqe = get_sqe_or_flush(reactor, state);
        if (sqe == NULL) {
            errno = EAGAIN;
            return -1;
        }
        
        /* Set up SQE based on operation type */
        switch (op->type) {
            case KEEL_OP_RECV:
                io_uring_prep_recv(sqe, op->fd_in, op->buf, op->len, 
                                   (int)op->flags);
                break;
            case KEEL_OP_SEND:
                io_uring_prep_send(sqe, op->fd_in, op->buf, op->len,
                                   (int)op->flags);
                break;
            case KEEL_OP_PEEK:
                io_uring_prep_recv(sqe, op->fd_in, op->buf, op->len,
                                   MSG_PEEK | (int)op->flags);
                break;
            default:
                errno = EINVAL;
                return -1;
        }
        
        /* Link if not the last operation */
        if (i < count - 1) {
            sqe->flags |= IOSQE_IO_LINK;
        }
        
        io_uring_sqe_set_data(sqe, op->userdata);
    }
    
    return 0;
}

/* ============================================================================
 * Chain Send+Recv (Linked SQEs)
 *
 * Queues a send SQE linked (IOSQE_IO_LINK) to a recv SQE.  The kernel
 * executes both in sequence without returning to userspace — eliminating
 * one send() syscall + one io_uring loop iteration per query.
 *
 * Each SQE gets its own iouring_op_t from the pending pool, so both
 * completions are handled through the normal CQE processing path.
 *
 * If the send fails (result < 0), the recv SQE is cancelled automatically
 * and its CQE fires with -ECANCELED.
 * ============================================================================ */

/**
 * @brief Queue a linked send-followed-by-recv kernel sequence.
 */
static int iouring_chain_send_recv(
    keel_reactor_t* reactor,
    int send_fd, const void* send_buf, size_t send_len, int send_flags,
    void* send_userdata, keel_reactor_callback_t on_send_done,
    int recv_fd, void* recv_buf, size_t recv_len, int recv_flags,
    void* recv_userdata, keel_reactor_callback_t on_recv_done)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;

    /* Allocate both SQEs upfront to avoid partial chain on failure */
    struct io_uring_sqe* sqe_send = get_sqe_or_flush(reactor, state);
    if (sqe_send == NULL) {
        errno = EAGAIN;
        return -1;
    }
    struct io_uring_sqe* sqe_recv = get_sqe_or_flush(reactor, state);
    if (sqe_recv == NULL) {
        errno = EAGAIN;
        return -1;
    }

    /* Allocate tracking ops from the pending pool */
    iouring_op_t* send_op = alloc_pending_op(state);
    if (send_op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    iouring_op_t* recv_op = alloc_pending_op(state);
    if (recv_op == NULL) {
        free_pending_op(state, send_op);
        errno = ENOMEM;
        return -1;
    }

    /* Prep send SQE with IOSQE_IO_LINK — kernel waits for it to complete
     * before starting the recv */
    send_op->userdata  = send_userdata;
    send_op->callback  = on_send_done;
    send_op->type      = KEEL_OP_SEND;
    send_op->multishot = false;

    io_uring_prep_send(sqe_send, send_fd, send_buf, send_len, send_flags);
    sqe_send->flags |= IOSQE_IO_LINK;
    io_uring_sqe_set_data(sqe_send, send_op);

    /* Prep recv SQE — auto-starts after send completes successfully */
    recv_op->userdata  = recv_userdata;
    recv_op->callback  = on_recv_done;
    recv_op->type      = KEEL_OP_RECV;
    recv_op->multishot = false;
    recv_op->orig_buf  = NULL;
    recv_op->fd        = -1;

    io_uring_prep_recv(sqe_recv, recv_fd, recv_buf, recv_len, recv_flags);
    io_uring_sqe_set_data(sqe_recv, recv_op);

    return 0;
}

/* ============================================================================
 * Timeout
 * ============================================================================ */

/**
 * @brief Queue a timeout SQE.
 */
static int iouring_timeout(
    keel_reactor_t* reactor,
    uint32_t timeout_ms,
    void* userdata,
    keel_reactor_callback_t callback)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    
    struct io_uring_sqe* sqe = get_sqe_or_flush(reactor, state);
    if (sqe == NULL) {
        errno = EAGAIN;
        return -1;
    }
    
    iouring_op_t* op = alloc_pending_op(state);
    if (op == NULL) {
        errno = ENOMEM;
        return -1;
    }
    
    op->userdata = userdata;
    op->callback = callback;
    op->type = KEEL_OP_TIMEOUT;
    op->multishot = false;
    
    /* Store timespec in the op so the pointer remains valid until the SQE is
     * consumed by the kernel.  Using a local stack variable here is a bug:
     * the stack frame is gone before io_uring_submit_and_wait_timeout() runs,
     * and the kernel dereferences the address — reading garbage. */
    op->ts.tv_sec  = (long long)(timeout_ms / 1000);
    op->ts.tv_nsec = (long long)((timeout_ms % 1000) * 1000000LL);
    
    op->timer_id = state->next_timer_id;
    io_uring_prep_timeout(sqe, &op->ts, 0, 0);
    io_uring_sqe_set_data(sqe, op);

    return state->next_timer_id++;
}

/**
 * @brief Timeout-cancellation for io_uring.
 *
 * Issues an IORING_OP_ASYNC_CANCEL targeting the timeout SQE identified by
 * @p timer_id.  The kernel delivers two CQEs:
 *   1. The original timeout CQE with res = -ECANCELED.
 *   2. A cancel CQE with res = 0 (user_data = NULL → silently ignored).
 */
static int iouring_cancel_timeout(keel_reactor_t* reactor, int timer_id)
{
    if (timer_id <= 0)
        return 0;

    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;

    /* Scan the pending-op pool for the matching timeout slot.
     * There are at most a handful of active timers at any time, so a linear
     * scan is acceptable and avoids an extra data structure. */
    iouring_op_t* target = NULL;
    for (size_t i = 0; i < state->pending_capacity; i++) {
        iouring_op_t* op = &state->pending_ops[i];
        if (op->in_use && op->type == KEEL_OP_TIMEOUT && op->timer_id == timer_id) {
            target = op;
            break;
        }
    }

    if (!target) {
        /* Timer already fired or was never queued — nothing to cancel. */
        return 0;
    }

    struct io_uring_sqe* sqe = get_sqe_or_flush(reactor, state);
    if (!sqe) {
        errno = EAGAIN;
        return -1;
    }

    /* IORING_OP_ASYNC_CANCEL: cancel the in-flight SQE whose user_data
     * matches the op pointer.  The kernel delivers a CQE with res=-ECANCELED
     * for the original timeout SQE and a CQE with res=0 for this cancel SQE.
     * We set user_data to NULL so the completion handler ignores the cancel CQE. */
    io_uring_prep_cancel(sqe, target, 0);
    io_uring_sqe_set_data(sqe, NULL);

    /* Submit immediately so the cancel reaches the kernel before the next
     * io_uring_submit_and_wait_timeout() call.  get_sqe_or_flush() may have
     * already flushed the ring; a second submit here is a no-op if the SQ
     * is already empty, so the extra syscall is at most one io_uring_enter(). */
    io_uring_submit(&state->ring);

    return 0;
}

/* ============================================================================
 * Submit / Wait / Process
 * ============================================================================ */

/**
 * @brief Explicit submit hook for the io_uring backend.
 *
 * @note Usually a no-op because `iouring_wait()` performs submit-and-wait in a
 *       single syscall.
 */
static int iouring_submit(keel_reactor_t* reactor)
{
    /* No-op: iouring_wait() uses io_uring_submit_and_wait_timeout() which
     * flushes pending SQEs and waits for CQEs in a single io_uring_enter()
     * syscall.  This avoids the overhead of two separate syscalls per
     * event loop iteration.  The internal get_sqe_or_flush() still calls
     * io_uring_submit() directly when the SQ ring is full. */
    (void)reactor;
    return 0;
}

/**
 * @brief Process one CQE and invoke the associated callback with reactor metadata.
 *
 * @param reactor Reactor handle.
 * @param state Backend state.
 * @param comp Normalized completion payload.
 * @param batch_size Total number of completions in the batch.
 * @return Nothing.
 *
 * Responsibilities:
 * - update stats counters
 * - recycle provider buffers when buffer rings are in use
 * - clear multishot tracking when the kernel stops a multishot stream
 * - publish timing metadata for the callback duration
 */
static void iouring_handle_completion(keel_reactor_t* reactor,
                                      iouring_state_t* state,
                                      const iouring_completion_t* comp,
                                      uint32_t batch_size)
{
    iouring_op_t* op = comp->op;
    int res = comp->res;
    unsigned cqe_flags = comp->flags;
    uint32_t batch_index = comp->batch_index;

    if (op == NULL || op->callback == NULL)
        return;

    if (res >= 0) {
        switch (op->type) {
            case KEEL_OP_RECV:
                reactor->stats.bytes_read += (uint64_t)res;
                break;
            case KEEL_OP_SEND:
                reactor->stats.bytes_written += (uint64_t)res;
                break;
            case KEEL_OP_SPLICE:
                reactor->stats.bytes_spliced += (uint64_t)res;
                break;
            case KEEL_OP_ACCEPT:
                reactor->stats.accepts++;
                break;
            case KEEL_OP_CONNECT:
                reactor->stats.connects++;
                break;
            case KEEL_OP_TIMEOUT:
                reactor->stats.timeouts++;
                break;
            default:
                break;
        }
    } else {
        reactor->stats.errors++;
    }

    bool multishot_cancelled = false;
    if (op->multishot && !(cqe_flags & IORING_CQE_F_MORE)) {
        multishot_cancelled = true;
    }

#ifdef KEEL_HAS_URING_BUF_RING
    if (state->use_buf_rings
            && op->type == KEEL_OP_RECV
            && (cqe_flags & IORING_CQE_F_BUFFER)) {
        uint16_t buf_id = (uint16_t)(cqe_flags >> IORING_CQE_BUFFER_SHIFT);
        if (buf_id < (uint16_t)state->buf_ring_count) {
            uint8_t *kbuf = state->buf_ring_bufs[buf_id];
            if (op->orig_buf != NULL && res > 0) {
                memcpy(op->orig_buf, kbuf,
                       (size_t)(unsigned)res > state->buf_ring_buf_size
                           ? state->buf_ring_buf_size
                           : (size_t)(unsigned)res);
            }
            const unsigned ring_mask = io_uring_buf_ring_mask(
                                           (uint32_t)state->buf_ring_count);
            io_uring_buf_ring_add(state->buf_ring,
                                 (void*)kbuf,
                                 (unsigned)state->buf_ring_buf_size,
                                 buf_id,
                                 ring_mask,
                                 0);
            io_uring_buf_ring_advance(state->buf_ring, 1);
        }
    }
#endif /* KEEL_HAS_URING_BUF_RING */

#if defined(KEEL_HAS_URING_BUF_RING) && defined(KEEL_HAS_URING_RECV_MULTISHOT)
    if (multishot_cancelled
            && op->type == KEEL_OP_RECV
            && state->mshot_recv_slot != NULL
            && op->fd >= 0
            && (size_t)(unsigned)op->fd < state->mshot_recv_cap) {
        state->mshot_recv_slot[(size_t)(unsigned)op->fd] = UINT32_MAX;
    }
#endif /* KEEL_HAS_URING_BUF_RING && KEEL_HAS_URING_RECV_MULTISHOT */

    uint64_t wakeup_ns = keel_reactor_current_completion_wakeup_ns();
    keel_reactor_set_completion_wakeup_ns(wakeup_ns);
    keel_reactor_set_completion_batch_size(batch_size);
    keel_reactor_set_completion_batch_index(batch_index);
    keel_reactor_set_completion_seen_ns(iouring_time_ns());
    op->callback(op->userdata, res);
    keel_reactor_set_completion_seen_ns(0);
    keel_reactor_set_completion_wakeup_ns(0);
    keel_reactor_set_completion_batch_size(0);
    keel_reactor_set_completion_batch_index(0);

    if (!op->multishot || multishot_cancelled) {
        if (multishot_cancelled && op->type == KEEL_OP_ACCEPT) {
            op->multishot = false;
            keel_reactor_set_completion_seen_ns(iouring_time_ns());
            keel_reactor_set_completion_wakeup_ns(wakeup_ns);
            keel_reactor_set_completion_batch_size(batch_size);
            keel_reactor_set_completion_batch_index(batch_index);
            op->callback(op->userdata, -ECANCELED);
            keel_reactor_set_completion_seen_ns(0);
            keel_reactor_set_completion_wakeup_ns(0);
            keel_reactor_set_completion_batch_size(0);
            keel_reactor_set_completion_batch_index(0);
        }
        free_pending_op(state, op);
    }
}

/**
 * @brief Submit pending SQEs and wait for at least one completion.
 */
static int iouring_wait(keel_reactor_t* reactor, int timeout_ms)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    
    struct __kernel_timespec ts;
    struct __kernel_timespec* ts_ptr = NULL;
    
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (timeout_ms % 1000) * 1000000L;
        ts_ptr = &ts;
    }
    
    /* Track pending SQEs for stats before submission. */
    unsigned pending = io_uring_sq_ready(&state->ring);
    if (pending > 0) {
        state->submitted_count += pending;
        reactor->stats.ops_submitted += (uint64_t)pending;
    }

    /* Submit + wait in a single io_uring_enter() syscall.
     * This saves one syscall per event loop iteration compared to the
     * prior separate io_uring_submit() + io_uring_wait_cqe_timeout(). */
    struct io_uring_cqe* cqe;
    int result = io_uring_submit_and_wait_timeout(&state->ring, &cqe, 1,
                                                   ts_ptr, NULL);
    
    if (result == -ETIME || result == -EINTR) {
        keel_reactor_set_completion_wakeup_ns(0);
        return 0;  /* Timeout or signal, no completions */
    }
    if (result < 0) {
        keel_reactor_set_completion_wakeup_ns(0);
        errno = -result;
        return -1;
    }

    keel_reactor_set_completion_wakeup_ns(iouring_time_ns());
    
    return 1;  /* At least one completion available */
}

/**
 * @brief Walk the ready CQEs and dispatch each completion callback.
 */
static int iouring_process(keel_reactor_t* reactor)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    int processed = 0;
    uint32_t batch_size = io_uring_cq_ready(&state->ring);
    uint32_t batch_index = 0;

    struct io_uring_cqe* cqe;
    unsigned head;

    io_uring_for_each_cqe(&state->ring, head, cqe) {
        if (batch_index >= batch_size)
            break;
        batch_index++;

        iouring_completion_t comp;
        comp.op = (iouring_op_t*)io_uring_cqe_get_data(cqe);
        comp.res = cqe->res;
        comp.flags = cqe->flags;
        comp.batch_index = batch_index;

        if (comp.op != NULL)
            iouring_handle_completion(reactor, state, &comp, batch_size);

        reactor->stats.ops_completed++;
        processed++;
    }

    io_uring_cq_advance(&state->ring, (unsigned)processed);
    return processed;
}

/**
 * @brief Return the number of live pending-op slots currently in use.
 */
static size_t iouring_pending(keel_reactor_t* reactor)
{
    iouring_state_t* state = (iouring_state_t*)reactor->backend_state;
    return state->pending_count;
}

#endif /* __linux__ && KEEL_HAS_IOURING */
