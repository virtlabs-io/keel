/**
 * @file worker.c
 * @brief Per-worker reactor runtime and connection/session lifecycle machinery.
 *
 * This is the central execution file for KEEL's data plane. One worker owns a
 * reactor, a slab of sessions, a small set of reusable helper objects, and the
 * timers/callbacks that keep frontend and backend traffic moving.
 *
 * The file is intentionally broad because many hot-path transitions need tight
 * coordination between session state, protocol flow, backend pooling, timers,
 * and reactor re-arming. Splitting every helper into a separate module would
 * improve locality only marginally while making it harder to reason about the
 * full lifecycle of one connection.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "keel/engine/worker.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/protocol/tls_context.h"
#include "keel/engine/engine_flow.h"
#include "keel/core/stats.h"
#include "keel/log/log.h"
#include "keel/log/audit_log.h"
#include "keel/sql/sql.h"
#include "keel/engine/backend_pool.h"
#include "keel/engine/engine.h"
#include "keel_hook.h"
#include "keel/core/auth.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>
#include <sched.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include "keel/util/platform_compat.h"

/* KEEL_RECV_BUF_SIZE is defined in keel/engine/worker.h (default 65536).
 * Override at cmake time: -DKEEL_RECV_BUF_SIZE=<bytes> */

/* Debug logging - set to 0 for production to eliminate hot path overhead */
#ifndef KEEL_WORKER_DEBUG
#define KEEL_WORKER_DEBUG 0
#endif

#if KEEL_WORKER_DEBUG
#define KEEL_DEBUG_LOG(...) KEEL_LOG_TRACE(KEEL_LOG_CAT_IO, __VA_ARGS__)
#else
#define KEEL_DEBUG_LOG(...) ((void)0)
#endif

#ifdef __linux__
#include <sys/eventfd.h>
#endif

/**
 * @brief Select backend server based on query type (read/write splitting)
 * 
 * For write queries: select from RW or WO servers
 * For read queries: select from RO or RW servers, fallback to RW
 */
static const keel_backend_server_t* select_backend_server(
    keel_worker_t* worker,
    const uint8_t* query_data,
    size_t query_len,
    bool* is_read_query)
{
    /* Default: no routing, use legacy single backend */
    if (!worker->server_pool || worker->server_pool->count == 0) {
        *is_read_query = false;
        return NULL;
    }
    
    /* Check if we can route reads to RO nodes */
    size_t readable_count = worker->server_pool->ro_count + worker->server_pool->rw_count;
    if (readable_count == 0) {
        /* No readable servers — shouldn't happen, but be safe */
        *is_read_query = false;
        return NULL;
    }
    
    /* Extract SQL from Query message (skip 'Q' tag and length) */
    const char* sql = NULL;
    size_t sql_len = 0;
    
    if (query_len > 5 && query_data[0] == 'Q') {
        /* Simple Query: 'Q' + 4-byte length + SQL string */
        sql = (const char*)(query_data + 5);
        sql_len = query_len - 5;
        /* Remove null terminator from length if present */
        while (sql_len > 0 && sql[sql_len - 1] == '\0') sql_len--;
    }
    
    /* If we can't extract SQL, assume write (safe default) */
    if (!sql || sql_len == 0) {
        *is_read_query = false;
        /* Pick first healthy RW server */
        for (size_t i = 0; i < worker->server_pool->rw_count; i++) {
            size_t idx = worker->server_pool->rw_indices[i];
            if (worker->server_pool->servers[idx].healthy)
                return &worker->server_pool->servers[idx];
        }
        return NULL;
    }
    
    /* Check if query is read-only using fast path */
    keel_str_t sql_str = { .data = sql, .len = sql_len };
    bool read_only = keel_sql_is_readonly(sql_str);
    *is_read_query = read_only;
    
    if (!read_only) {
        /* Write query - select from RW (preferred) or WO servers */
        size_t start = worker->server_pool->next_write;
        /* Try RW first, then WO */
        for (size_t i = 0; i < worker->server_pool->rw_count; i++) {
            size_t ci = (start + i) % worker->server_pool->rw_count;
            size_t idx = worker->server_pool->rw_indices[ci];
            if (worker->server_pool->servers[idx].healthy) {
                ((keel_server_pool_t*)worker->server_pool)->next_write =
                    (ci + 1) % worker->server_pool->rw_count;
                return &worker->server_pool->servers[idx];
            }
        }
        for (size_t i = 0; i < worker->server_pool->wo_count; i++) {
            size_t idx = worker->server_pool->wo_indices[i];
            if (worker->server_pool->servers[idx].healthy)
                return &worker->server_pool->servers[idx];
        }
        return NULL;
    }
    
    /* Read query - round-robin across RO servers, then fallback to RW */
    size_t start = worker->server_pool->next_read;
    for (size_t i = 0; i < worker->server_pool->ro_count; i++) {
        size_t ci = (start + i) % worker->server_pool->ro_count;
        size_t idx = worker->server_pool->ro_indices[ci];
        if (worker->server_pool->servers[idx].healthy) {
            ((keel_server_pool_t*)worker->server_pool)->next_read =
                (ci + 1) % worker->server_pool->ro_count;
            return &worker->server_pool->servers[idx];
        }
    }
    
    /* Fallback to RW for reads */
    for (size_t i = 0; i < worker->server_pool->rw_count; i++) {
        size_t idx = worker->server_pool->rw_indices[i];
        if (worker->server_pool->servers[idx].healthy)
            return &worker->server_pool->servers[idx];
    }
    
    return NULL;
}

/* ============================================================================
 * Time Utilities
 * ============================================================================ */

/** @brief Return the current monotonic time in nanoseconds. */
static uint64_t get_time_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ============================================================================
 * Timer Wheel Implementation
 * ============================================================================ */

/**
 * @brief Initialize the hierarchical timer wheel used by one worker.
 *
 * @param wheel Timer wheel to initialize.
 * @param tick_ms Base tick size in milliseconds.
 * @return
 */
void keel_timer_wheel_init(keel_timer_wheel_t* wheel, uint32_t tick_ms)
{
    memset(wheel, 0, sizeof(*wheel));
    wheel->tick_ns = (uint64_t)tick_ms * 1000000ULL;
    wheel->current_tick = get_time_ns() / wheel->tick_ns;
    wheel->min_deadline = UINT64_MAX;
    wheel->min_dirty    = false;
}

/**
 * @brief Schedule one timer entry in the worker's hierarchical timer wheel.
 *
 * Timers are placed into a level/slot derived from the absolute deadline tick,
 * not just the relative delay, so subsequent wheel advancement can find and
 * cascade them deterministically.
 *
 * @param wheel Timer wheel to modify.
 * @param entry Timer entry storage owned by the caller.
 * @param delay_ms Relative delay in milliseconds.
 * @param userdata Opaque callback context.
 * @param callback Callback to invoke when the timer expires.
 * @return
 */
void keel_timer_wheel_add(
    keel_timer_wheel_t* wheel,
    keel_timer_entry_t* entry,
    uint32_t delay_ms,
    void* userdata,
    void (*callback)(void* userdata))
{
    entry->userdata = userdata;
    entry->callback = callback;
    entry->deadline = get_time_ns() + (uint64_t)delay_ms * 1000000ULL;
    
    /* Calculate which slot at which level.
     * The slot must correspond to the ABSOLUTE deadline tick so that
     * keel_timer_wheel_tick() — which scans by (current_tick & MASK) —
     * finds the entry when current_tick reaches the deadline tick. */
    uint64_t deadline_tick = entry->deadline / wheel->tick_ns;
    uint64_t ticks_from_now = deadline_tick - wheel->current_tick;
    
    int level = 0;
    while (level < KEEL_TIMER_WHEEL_LEVELS - 1 && 
           ticks_from_now >= KEEL_TIMER_WHEEL_SIZE) {
        ticks_from_now >>= KEEL_TIMER_WHEEL_BITS;
        level++;
    }
    
    size_t slot = (deadline_tick >> (level * KEEL_TIMER_WHEEL_BITS)) & KEEL_TIMER_WHEEL_MASK;
    
    /* Insert into slot */
    entry->next = wheel->slots[level][slot];
    entry->prev = NULL;
    if (wheel->slots[level][slot]) {
        wheel->slots[level][slot]->prev = entry;
    }
    wheel->slots[level][slot] = entry;
    wheel->count++;

    /* Record position for O(1) cancel */
    entry->wheel_level = (uint8_t)level;
    entry->wheel_slot  = (uint16_t)slot;

    /* Update O(1) min_deadline hint */
    if (entry->deadline < wheel->min_deadline) {
        wheel->min_deadline = entry->deadline;
    }
}

/**
 * @brief Cancel a previously scheduled timer entry.
 *
 * Stored level/slot metadata allows direct unlink in $O(1)$ time without a
 * full wheel scan.
 *
 * @param wheel Timer wheel containing the entry.
 * @param entry Timer entry to cancel.
 * @return
 */
void keel_timer_wheel_cancel(keel_timer_wheel_t* wheel, keel_timer_entry_t* entry)
{
    /* Use stored level/slot for O(1) unlink (no full-wheel scan needed) */
    if (entry->prev == NULL) {
        /* Entry is the slot head — update the head pointer directly */
        wheel->slots[entry->wheel_level][entry->wheel_slot] = entry->next;
    } else {
        entry->prev->next = entry->next;
    }
    if (entry->next) {
        entry->next->prev = entry->prev;
    }
    
    /* If we removed the current minimum, mark the hint stale */
    if (entry->deadline <= wheel->min_deadline) {
        wheel->min_dirty = true;
    }

    /* Clear pointers */
    entry->next = NULL;
    entry->prev = NULL;
    
    if (wheel->count > 0) {
        wheel->count--;
    }
}

/**
 * @brief Advance the timer wheel to the current time and fire expired timers.
 *
 * The wheel progresses tick by tick until it reaches the requested target.
 * Level-0 entries whose deadlines have passed are fired, and higher-level
 * buckets are cascaded downward as their coarse slots roll over.
 *
 * @param wheel Timer wheel to advance.
 * @param now_ns Current monotonic time in nanoseconds.
 * @return Number of timers fired during this advancement.
 */
size_t keel_timer_wheel_tick(keel_timer_wheel_t* wheel, uint64_t now_ns)
{
    size_t fired = 0;
    uint64_t target_tick = now_ns / wheel->tick_ns;
    
    while (wheel->current_tick < target_tick) {
        size_t slot = wheel->current_tick & KEEL_TIMER_WHEEL_MASK;

        /* Cascade from higher levels FIRST when at boundary, so any cascaded
         * timers whose deadlines fall in (or before) the current L0 slot are
         * placed there in time to be fired in the same iteration below.
         * Re-add uses a delay clamped at zero to avoid an unsigned underflow
         * when (upper->deadline < now_ns), which would otherwise re-insert the
         * entry at the deepest level with a near-infinite delay (effectively
         * losing the timer forever -- e.g. a long pool-warmup stall would
         * silently kill the rebalance timer). */
        if ((wheel->current_tick & KEEL_TIMER_WHEEL_MASK) == 0) {
            for (int level = 1; level < KEEL_TIMER_WHEEL_LEVELS; level++) {
                size_t upper_slot = (wheel->current_tick >> (level * KEEL_TIMER_WHEEL_BITS))
                                    & KEEL_TIMER_WHEEL_MASK;

                keel_timer_entry_t* upper = wheel->slots[level][upper_slot];
                wheel->slots[level][upper_slot] = NULL;

                while (upper != NULL) {
                    keel_timer_entry_t* next = upper->next;
                    /* Cascade-extracted entry: balance the count++ that
                     * keel_timer_wheel_add() will perform on re-insertion. */
                    if (wheel->count > 0) wheel->count--;
                    uint32_t delay_ms = (upper->deadline > now_ns)
                        ? (uint32_t)((upper->deadline - now_ns) / 1000000ULL)
                        : 0;
                    keel_timer_wheel_add(wheel, upper, delay_ms,
                                        upper->userdata, upper->callback);
                    upper = next;
                }
            }
        }

        /* Process timers in level 0 slot */
        keel_timer_entry_t* entry = wheel->slots[0][slot];
        while (entry != NULL) {
            keel_timer_entry_t* next = entry->next;
            
            if (entry->deadline <= now_ns) {
                /* Remove and fire */
                if (entry->prev) entry->prev->next = entry->next;
                if (entry->next) entry->next->prev = entry->prev;
                if (wheel->slots[0][slot] == entry) {
                    wheel->slots[0][slot] = entry->next;
                }
                
                entry->next = NULL;
                entry->prev = NULL;
                wheel->count--;
                
                /* Fired timer may have been the minimum */
                wheel->min_dirty = true;

                if (entry->callback) {
                    entry->callback(entry->userdata);
                }
                fired++;
            }
            
            entry = next;
        }

        wheel->current_tick++;
    }
    
    return fired;
}

/**
 * @brief Return the nearest known timer deadline.
 *
 * A cached minimum deadline provides the fast path; a full rescan is only
 * needed after cancellations or expirations invalidate that hint.
 *
 * @param wheel Timer wheel to inspect.
 * @return Nearest deadline in nanoseconds, or `UINT64_MAX` when empty.
 */
uint64_t keel_timer_wheel_next_deadline(keel_timer_wheel_t* wheel)
{
    if (wheel->count == 0) {
        return UINT64_MAX;
    }
    
    /* Fast path: min_deadline is still accurate */
    if (!wheel->min_dirty) {
        return wheel->min_deadline;
    }
    
    /* Slow path: rescan all slots to find the true minimum, then cache it */
    uint64_t nearest = UINT64_MAX;
    
    for (int level = 0; level < KEEL_TIMER_WHEEL_LEVELS; level++) {
        for (size_t slot = 0; slot < KEEL_TIMER_WHEEL_SIZE; slot++) {
            keel_timer_entry_t* entry = wheel->slots[level][slot];
            while (entry != NULL) {
                if (entry->deadline < nearest) {
                    nearest = entry->deadline;
                }
                entry = entry->next;
            }
        }
    }
    
    wheel->min_deadline = nearest;
    wheel->min_dirty    = false;
    return nearest;
}

/* ============================================================================
 * Pipe Pool Implementation (Linux only)
 * ============================================================================ */

#ifdef __linux__

/**
 * @brief Pre-create a small pool of Linux pipes for splice-based fast paths.
 *
 * @param pool Pipe pool to initialize.
 * @param capacity Maximum number of pipe pairs to attempt to create.
 * @return `0` on success, `-1` on allocation failure.
 */
int keel_pipe_pool_init(keel_pipe_pool_t* pool, size_t capacity)
{
    pool->pipes = (keel_pipe_t*)keel_calloc(capacity, sizeof(keel_pipe_t));
    pool->free_stack = (keel_pipe_t**)keel_calloc(capacity, sizeof(keel_pipe_t*));
    
    if (pool->pipes == NULL || pool->free_stack == NULL) {
        keel_free(pool->pipes);
        keel_free(pool->free_stack);
        return -1;
    }
    
    pool->capacity = capacity;
    pool->available = 0;
    pool->free_count = 0;
    
    /* Create pipes */
    for (size_t i = 0; i < capacity; i++) {
        int fds[2];
        if (pipe(fds) < 0) {
            /* Stop creating, but don't fail entirely */
            break;
        }
        
        pool->pipes[i].read_fd = fds[0];
        pool->pipes[i].write_fd = fds[1];
        pool->pipes[i].capacity = 65536;  /* Default pipe capacity */
        pool->pipes[i].pending = 0;
        
        pool->free_stack[pool->free_count++] = &pool->pipes[i];
        pool->available++;
    }
    
    return 0;
}

keel_pipe_t* keel_pipe_pool_acquire(keel_pipe_pool_t* pool)
{
    if (pool->free_count == 0) {
        return NULL;
    }
    
    return pool->free_stack[--pool->free_count];
}

void keel_pipe_pool_release(keel_pipe_pool_t* pool, keel_pipe_t* pipe)
{
    if (pool->free_count < pool->capacity) {
        pool->free_stack[pool->free_count++] = pipe;
    }
}

void keel_pipe_pool_destroy(keel_pipe_pool_t* pool)
{
    for (size_t i = 0; i < pool->available; i++) {
        close(pool->pipes[i].read_fd);
        close(pool->pipes[i].write_fd);
    }
    
    keel_free(pool->pipes);
    keel_free(pool->free_stack);
    memset(pool, 0, sizeof(*pool));
}

#else /* Non-Linux stub */

/**
 * @brief Initialise the pipe pool (non-Linux stub — no-op).
 *
 * @param pool      Pipe pool to initialise.
 * @param capacity  Requested capacity (ignored).
 * @return 0.
 */
int keel_pipe_pool_init(keel_pipe_pool_t* pool, size_t capacity)
{
    (void)capacity;
    memset(pool, 0, sizeof(*pool));
    return 0;
}

/** @brief Acquire a pipe from the pool (non-Linux stub — always returns NULL). */
keel_pipe_t* keel_pipe_pool_acquire(keel_pipe_pool_t* pool)
{
    (void)pool;
    return NULL;
}

/** @brief Return a pipe to the pool (non-Linux stub — no-op). */
void keel_pipe_pool_release(keel_pipe_pool_t* pool, keel_pipe_t* pipe)
{
    (void)pool;
    (void)pipe;
}

/** @brief Destroy the pipe pool and release resources (non-Linux stub — no-op). */
void keel_pipe_pool_destroy(keel_pipe_pool_t* pool)
{
    (void)pool;
}

#endif /* __linux__ */

/* ============================================================================
 * Session Slab Implementation
 * ============================================================================ */

/**
 * @brief Initialize the worker's fixed-capacity session slab.
 *
 * A slab plus free list avoids per-session wrapper allocation on the accept
 * path and gives the worker a hard cap on concurrent frontend sessions.
 *
 * @param slab Slab to initialize.
 * @param capacity Number of session slots to create.
 * @return `0` on success, `-1` on allocation failure.
 */
int keel_session_slab_init(keel_session_slab_t* slab, size_t capacity)
{
    slab->sessions = (keel_session_t*)keel_calloc(capacity, sizeof(keel_session_t));
    if (slab->sessions == NULL) {
        return -1;
    }
    
    slab->capacity = capacity;
    slab->allocated = 0;
    slab->free_list = NULL;
    slab->alloc_count = 0;
    slab->free_count = 0;
    slab->reuse_count = 0;
    
    /* Build free list */
    for (size_t i = 0; i < capacity; i++) {
        slab->sessions[i].slab_index = (uint32_t)i;
        slab->sessions[i].next_free = slab->free_list;
        slab->free_list = &slab->sessions[i];
    }
    
    return 0;
}

keel_session_t* keel_session_slab_alloc(keel_session_slab_t* slab)
{
    if (slab->free_list == NULL) {
        return NULL;
    }
    
    keel_session_t* session = slab->free_list;
    slab->free_list = session->next_free;
    session->next_free = NULL;
    
    slab->allocated++;
    slab->alloc_count++;
    
    if (session->id != 0) {
        slab->reuse_count++;
    }
    
    return session;
}

void keel_session_slab_free(keel_session_slab_t* slab, keel_session_t* session)
{
    session->next_free = slab->free_list;
    slab->free_list = session;
    slab->allocated--;
    slab->free_count++;
}

/**
 * @brief Destroy a session slab and release its backing memory.
 *
 * @param slab  Slab to destroy; leaves a zeroed struct behind.
 */
void keel_session_slab_destroy(keel_session_slab_t* slab)
{
    keel_free(slab->sessions);
    memset(slab, 0, sizeof(*slab));
}

/* ============================================================================
 * Worker Thread Main Loop
 * ============================================================================ */

/* Session idle timeout, pool prune/refill intervals and pool_refill_backoff_ms
 * are now runtime values stored in keel_worker_t (worker->idle_timeout_ms etc.).
 * They are propagated from keel_engine_config_t at worker initialisation and
 * can be tuned via the INI file.
 *
 * IMPORTANT for pool_refill_interval_ms: keep >= 100ms to avoid a busy-wait
 * (values below 100ms cause the timer-deadline calculation to reach 0 on nearly
 * every iteration, pinning the CPU at 100%). */

/* Forward declaration for pool prune callback */
static void pool_prune_timer_cb(void* userdata);
static void pool_refill_timer_cb(void* userdata);
static void rebalance_timer_cb(void* userdata);
static void on_accept_complete(void* userdata, int result);

/**
 * @brief Main loop for one worker thread.
 *
 * The loop warms backend pools, arms accept only after that warmup barrier,
 * then repeatedly submits reactor work, waits for completions, processes timer
 * callbacks, and drains inbound migration traffic. The timeout passed to the
 * reactor is derived from the nearest timer deadline so one loop coordinates
 * both I/O and timer progression.
 *
 * @param arg Worker pointer passed through `pthread_create()`.
 * @return Thread return value, always `NULL`.
 */

/**
 * @brief Callback for the wakeup eventfd async read.
 *
 * When keel_worker_stop() writes to worker->eventfd, this CQE arrives and
 * unblocks io_uring_enter() so that the warmup/main loop can see should_stop.
 * The value read is ignored; the caller already set should_stop before writing.
 */
static void on_wakeup_eventfd_complete(void* userdata, int result)
{
    (void)userdata;
    (void)result;
    /* No-op: should_stop is already set by keel_worker_stop().
     * The CQE merely interrupts keel_reactor_wait() so the event loop
     * can observe the flag on its next iteration. */
}

static void* worker_thread_func(void* arg)
{
    keel_worker_t* worker = (keel_worker_t*)arg;
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Worker %u starting on CPU %d",
                worker->id, worker->cpu_affinity);
    
#ifdef __linux__
    /* Set CPU affinity if requested */
    if (worker->cpu_affinity >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(worker->cpu_affinity, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
    }
#endif
    
    worker->state = KEEL_WORKER_RUNNING;

    /* Arm an async read on the wakeup eventfd so that keel_worker_stop()'s
     * write() immediately unblocks io_uring_enter() via a CQE.  Without this,
     * keel_reactor_wait() would block for the full timeout_ms before the
     * warmup/main loop could observe should_stop. */
#ifdef __linux__
    if (worker->eventfd >= 0) {
        worker->wakeup_buf = 0;
        int rc = keel_reactor_recv(worker->reactor, worker->eventfd,
                                   &worker->wakeup_buf, sizeof(worker->wakeup_buf),
                                   0, worker, on_wakeup_eventfd_complete);
        if (rc < 0) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_IO,
                "W%u: failed to arm wakeup eventfd recv (fd=%d): %s",
                worker->id, worker->eventfd, strerror(errno));
        }
    }
#endif
    
    /* ---- Pool warmup barrier ------------------------------------------------
     * Spin the reactor until every pool reaches min_connections ready backends.
     * This eliminates the thundering herd of pool waits: clients don't arrive
     * until backends are available.  The async connects were kicked by
     * backend_pool_async_warmup() during worker_init; we just need to drive
     * the reactor to process their completions. */
    {
        uint64_t warmup_deadline = get_time_ns() + 5000000000ULL;  /* 5 s */
        bool warmup_done = false;
        while (!warmup_done && get_time_ns() < warmup_deadline) {
            warmup_done = true;
            for (size_t i = 0; i < worker->server_pool_count; i++) {
                backend_pool_t* p = worker->server_pools[i];
                if (!p) continue;
                if (p->clean_count < p->config.min_connections) {
                    warmup_done = false;
                    break;
                }
            }
        /* Break early on shutdown request (e.g. SIGTERM) */
            if (worker->should_stop) {
                warmup_done = true;
                break;
            }
            if (warmup_done) break;
            keel_reactor_submit(worker->reactor);
            int ready = keel_reactor_wait(worker->reactor, 10);
            if (ready > 0)
                keel_reactor_process(worker->reactor);
        }
        KEEL_LOG_INFO(KEEL_LOG_CAT_POOL,
                "W%u pool warmup %s (%zu pools)",
                worker->id, warmup_done ? "complete" : "TIMED OUT",
                worker->server_pool_count);
    }

    /* Arm accept AFTER warmup so clients arrive with backends ready */
    if (keel_reactor_accept(worker->reactor, worker->listen_fd,
                           NULL, NULL, worker, on_accept_complete, false) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "Worker %u: failed to arm accept after warmup", worker->id);
    }

    /* Start pool prune timer if idle timeout is configured */
    memset(&worker->pool_prune_timer, 0, sizeof(worker->pool_prune_timer));
    keel_timer_wheel_add(&worker->timers, &worker->pool_prune_timer,
                        worker->pool_prune_interval_ms,
                        worker, pool_prune_timer_cb);
    
    /* Start pool refill timer for quick recovery of closed connections */
    memset(&worker->pool_refill_timer, 0, sizeof(worker->pool_refill_timer));
    keel_timer_wheel_add(&worker->timers, &worker->pool_refill_timer,
                        worker->pool_refill_interval_ms,
                        worker, pool_refill_timer_cb);
    
    /* Start rebalance timer for automatic load-based connection migration */
    const keel_engine_config_t *rcfg = keel_engine_get_config(worker->engine);
    if (rcfg && rcfg->rebalance_enabled && rcfg->rebalance_interval_ms > 0) {
        memset(&worker->rebalance_timer, 0, sizeof(worker->rebalance_timer));
        keel_timer_wheel_add(&worker->timers, &worker->rebalance_timer,
                            rcfg->rebalance_interval_ms,
                            worker, rebalance_timer_cb);
    }
    
    /* Main event loop */
    int loop_count = 0;
    uint64_t next_observe_ns = get_time_ns() + 10000000000ULL; /* 10s */
    uint64_t last_sq_overflow = 0;
    while (!worker->should_stop) {
        loop_count++;
        
        /* Calculate timeout until next timer */
        uint64_t now = get_time_ns();
        uint64_t next_deadline = keel_timer_wheel_next_deadline(&worker->timers);
        int timeout_ms;
        
        if (next_deadline == UINT64_MAX) {
            timeout_ms = 1000;  /* Default 1 second */
        } else if (next_deadline <= now) {
            timeout_ms = 0;
        } else {
            timeout_ms = (int)((next_deadline - now) / 1000000);
            if (timeout_ms < 1) timeout_ms = 1;   /* floor: never spin on sub-ms remainder */
            if (timeout_ms > 1000) timeout_ms = 1000;
        }
        
        if (loop_count % 2 == 1 || timeout_ms == 0) {
            KEEL_DEBUG_LOG("W%u loop=%d timeout=%d count=%u deadline=%lu now=%lu\n",
                worker->id, loop_count, timeout_ms, (unsigned)worker->timers.count,
                (unsigned long)next_deadline, (unsigned long)now);
        }
        
        /* Submit pending I/O */
        int submitted = keel_reactor_submit(worker->reactor);
        if (submitted > 0) {
            KEEL_DEBUG_LOG("W%u: submitted=%d\n", worker->id, submitted);
            worker->stats.loops++;
            if (worker->stats_ctx) {
                KEEL_STAT_INC(worker->stats_ctx, loop_iterations);
                KEEL_STAT_ADD(worker->stats_ctx, ops_submitted, submitted);
            }
        }
        
        /* Wait for I/O completions */
        int ready = keel_reactor_wait(worker->reactor, timeout_ms);

        if (ready < 0) {
            if (errno != EINTR) {
                worker->stats.errors++;
                if (worker->stats_ctx)
                    KEEL_STAT_INC(worker->stats_ctx, errors_total);
            }
            continue;
        }

        /* Process completions */
        if (ready > 0) {
            int processed = keel_reactor_process(worker->reactor);
            if (worker->stats_ctx && processed > 0)
                KEEL_STAT_ADD(worker->stats_ctx, ops_completed, processed);
            (void)processed;
        }
        
        /* Retry accept if the previous rearm failed due to SQ exhaustion.
         * When draining, suppress accept so no new clients arrive. */
        if (worker->accept_rearm_needed &&
            !atomic_load_explicit(&worker->draining, memory_order_acquire)) {
            if (keel_reactor_accept(worker->reactor, worker->listen_fd,
                                   NULL, NULL, worker, on_accept_complete, false) == 0) {
                worker->accept_rearm_needed = false;
            }
        }
        
        /* Process timers */
        now = get_time_ns();
        size_t timers_fired = keel_timer_wheel_tick(&worker->timers, now);
        worker->stats.timeouts += timers_fired;
        if (worker->stats_ctx && timers_fired > 0)
            KEEL_STAT_ADD(worker->stats_ctx, errors_timeout, timers_fired);

        /* When draining, self-stop once all sessions have completed. */
        if (atomic_load_explicit(&worker->draining, memory_order_acquire) &&
            worker->sessions.allocated == 0) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CONN,
                "Worker %u: drain complete (0 sessions), exiting", worker->id);
            worker->should_stop = true;
        }

        /* Drain inbound migration messages */
        if (worker->migration.inbox &&
            !keel_spsc_ringbuf_is_empty(worker->migration.inbox)) {
            keel_migration_drain(&worker->migration, worker);
        }

        if (worker->stats_ctx && now >= next_observe_ns) {
            size_t allocated = 0, available = 0, total = 0;
            keel_pool_stats(worker->recv_ctx_pool, &allocated, &available, &total);
            (void)available;
            (void)total;
            KEEL_STAT_GAUGE_SET(worker->stats_ctx,
                                proxy_buffer_pool_utilization_bytes,
                                (int64_t)(allocated * (size_t)KEEL_RECV_BUF_SIZE));

            uint64_t max_age_ns = 0;
            for (size_t i = 0; i < worker->sessions.capacity; i++) {
                keel_session_t* s = &worker->sessions.sessions[i];
                if (s->client_fd < 0 || s->state == KEEL_SESSION_CLOSED || s->created_at == 0)
                    continue;
                if (now > s->created_at) {
                    uint64_t age_ns = now - s->created_at;
                    if (age_ns > max_age_ns)
                        max_age_ns = age_ns;
                }
            }
            KEEL_STAT_GAUGE_SET(worker->stats_ctx,
                                proxy_connection_age_seconds,
                                (int64_t)(max_age_ns / 1000000000ULL));
            KEEL_STAT_GAUGE_SET(worker->stats_ctx, proxy_heartbeat_last_ns, (int64_t)now);

            keel_reactor_stats_t reactor_stats;
            memset(&reactor_stats, 0, sizeof(reactor_stats));
            keel_reactor_get_stats(worker->reactor, &reactor_stats);
            if (reactor_stats.sq_overflow > last_sq_overflow) {
                KEEL_STAT_ADD(worker->stats_ctx,
                              proxy_io_uring_sq_overflow_total,
                              reactor_stats.sq_overflow - last_sq_overflow);
            }
            last_sq_overflow = reactor_stats.sq_overflow;
            next_observe_ns = now + 10000000000ULL;
        }
        
        loop_count++;
        worker->stats.loops++;
    }
    
    worker->state = KEEL_WORKER_STOPPED;
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Worker %u stopped", worker->id);
    
    return NULL;
}

/* ============================================================================
 * Accept Callback
 * ============================================================================ */

/* Forward declarations */
static void on_client_recv_complete(void* userdata, int result);
static void on_backend_recv_complete(void* userdata, int result);
static void on_deferred_send_complete(void* userdata, int result);
static void on_auth_notify_complete(void* userdata, int result);

/* Backend recv context - for async backend responses */
typedef struct backend_recv_context {
    struct recv_context* client_ctx;     /* Associated client context */
} backend_recv_context_t;

/** TLS handshake buffer size — allocated lazily for TLS sessions only. */
#define KEEL_TLS_HS_BUF_SIZE 4096

/* Per-session context — metadata + pointer-based I/O attachments.
 *
 * Buffers are heap-allocated, NOT embedded:
 *   fe_buf  — frontend recv buffer (allocated on session setup)
 *   be_buf  — backend  recv buffer (allocated lazily on backend borrow)
 *   tls_hs_buf — TLS handshake send buffer (lazily on SSLRequest)
 *
 * sizeof(recv_context_t) ≈ 400 B (vs ≈ 131 KB with embedded arrays). */
typedef struct recv_context {
    keel_session_t*          session;
    keel_timer_entry_t       idle_timer;
    keel_session_flow_t      flow;       /* protocol flow state */
    backend_recv_context_t   be_ctx;     /* back-pointer for BE callback */
    bool                    closing;     /* Set when session is closing */
    bool                    be_pending;  /* Backend recv is pending */
    bool                    fe_pending;  /* Frontend recv is pending (io_uring) */
    bool                    send_pending; /* Deferred send is in-flight (io_uring) */

    /* Frontend I/O buffer — allocated on session setup */
    uint8_t*                fe_buf;
    uint32_t                fe_cap;

    /* Backend I/O buffer — allocated lazily on first backend borrow */
    uint8_t*                be_buf;
    uint32_t                be_cap;

    /* TLS state — set after client sends SSLRequest and TLS is accepted */
    keel_tls_context_t*     tls_ctx;         /* TLS context; NULL = no TLS */
    bool                    tls_hs_active;   /* Handshake in progress */
    bool                    tls_ktls_active; /* kTLS: kernel handles enc/dec */
    uint8_t*                tls_hs_buf;      /* TLS handshake send buffer (lazy) */
    size_t                  tls_hs_len;      /* Bytes pending send in tls_hs_buf */
    size_t                  tls_hs_off;      /* Bytes already sent */

    /* Async auth eventfd read buffer — 8 bytes, allocated lazily on first
     * KEEL_FLOW_WAIT_AUTH.  Freed in close_session / recv_context cleanup. */
    uint8_t*                auth_efd_buf;
} recv_context_t;

/* Forward: deferred send helper (needs recv_context_t) */
static int queue_deferred_send(keel_worker_t* worker, recv_context_t* recv_ctx);

/* ---- I/O buffer lifecycle helpers ---- */

/** Allocate the frontend recv buffer.  Called once on session setup. */
static inline int recv_ctx_alloc_fe(recv_context_t* ctx) {
    ctx->fe_buf = (uint8_t*)keel_malloc(KEEL_RECV_BUF_SIZE);
    if (!ctx->fe_buf) return -1;
    ctx->fe_cap = KEEL_RECV_BUF_SIZE;
    return 0;
}

/** Ensure the backend recv buffer is allocated (lazy — first backend borrow). */
static inline int recv_ctx_ensure_be(recv_context_t* ctx) {
    if (ctx->be_buf) return 0;
    ctx->be_buf = (uint8_t*)keel_malloc(KEEL_RECV_BUF_SIZE);
    if (!ctx->be_buf) return -1;
    ctx->be_cap = KEEL_RECV_BUF_SIZE;
    return 0;
}

/** Ensure the TLS handshake buffer is allocated (lazy — on SSLRequest). */
static inline int recv_ctx_ensure_tls_hs(recv_context_t* ctx) {
    if (ctx->tls_hs_buf) return 0;
    ctx->tls_hs_buf = (uint8_t*)keel_malloc(KEEL_TLS_HS_BUF_SIZE);
    return ctx->tls_hs_buf ? 0 : -1;
}

/** Free all heap-allocated I/O buffers.  Safe to call multiple times. */
static inline void recv_ctx_free_bufs(recv_context_t* ctx) {
    keel_free(ctx->fe_buf);   ctx->fe_buf  = NULL; ctx->fe_cap = 0;
    keel_free(ctx->be_buf);   ctx->be_buf  = NULL; ctx->be_cap = 0;
    keel_free(ctx->tls_hs_buf); ctx->tls_hs_buf = NULL;
    keel_free(ctx->auth_efd_buf); ctx->auth_efd_buf = NULL;
}

/* Forward declaration for zombie reaper callback */
static void session_idle_timeout_cb(void* userdata);

/* Forward declaration for pool wait callback */
static void pool_wait_resume_cb(void* session_ptr, void* userdata);

/* Forward declaration for deferred close completion */
static void complete_deferred_close(keel_worker_t* worker, recv_context_t* client_ctx);

enum {
    WAIT_BACKEND_KIND_NONE = 0,
    WAIT_BACKEND_KIND_QUERY = 1,
    WAIT_BACKEND_KIND_REPLAY = 2,
    WAIT_BACKEND_KIND_DISCARD = 3,
};

/* Coalesce idle timer re-arms during high traffic to reduce timer-wheel churn. */
#define IDLE_TIMER_REARM_GRACE_NS (250ULL * 1000000ULL)

static inline void refresh_session_idle_timer(keel_worker_t* worker,
                                              recv_context_t* recv_ctx,
                                              uint64_t now_ns)
{
    if (!worker || !recv_ctx || !recv_ctx->session)
        return;

    recv_ctx->session->last_activity = now_ns;

    if (worker->idle_timeout_ms == 0)
        return;

    uint64_t timeout_ns = (uint64_t)worker->idle_timeout_ms * 1000000ULL;
    uint64_t desired_deadline = now_ns + timeout_ns;
    uint64_t current_deadline = recv_ctx->idle_timer.deadline;

    if (current_deadline > now_ns &&
        current_deadline + IDLE_TIMER_REARM_GRACE_NS >= desired_deadline) {
        return;
    }

    keel_timer_wheel_cancel(&worker->timers, &recv_ctx->idle_timer);
    keel_timer_wheel_add(&worker->timers, &recv_ctx->idle_timer,
                        worker->idle_timeout_ms,
                        recv_ctx, session_idle_timeout_cb);
}

/**
 * @brief Mark the beginning of a frontend session's wait for a pool slot.
 *
 * Instrumentation is guarded both by the presence of a stats context and the
 * configured hot-path mask so the bookkeeping can be compiled in without
 * forcing every deployment to pay for it at runtime.
 *
 * @param recv_ctx Session recv context.
 * @return
 */
static inline void stats_mark_wait_pool_begin(recv_context_t* recv_ctx)
{
    if (!recv_ctx || !recv_ctx->session || !recv_ctx->session->worker ||
        !recv_ctx->session->worker->stats_ctx) {
        return;
    }
    if ((recv_ctx->session->worker->hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_POOL) == 0)
        return;
    if (recv_ctx->flow.wait_pool_start_ns != 0)
        return;

    recv_ctx->flow.wait_pool_start_ns = get_time_ns();
    KEEL_STAT_INC(recv_ctx->session->worker->stats_ctx, flow_wait_pool_events);
}

/**
 * @brief Close out pool-wait timing once a backend becomes available.
 *
 * @param recv_ctx Session recv context.
 * @return
 */
static inline void stats_mark_wait_pool_end(recv_context_t* recv_ctx)
{
    if (!recv_ctx)
        return;

    uint64_t start = recv_ctx->flow.wait_pool_start_ns;
    recv_ctx->flow.wait_pool_start_ns = 0;
    if (start == 0)
        return;

    if (recv_ctx->session && recv_ctx->session->worker && recv_ctx->session->worker->stats_ctx) {
        uint64_t now = get_time_ns();
        if (now > start)
            KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                          flow_wait_pool_ns_total,
                          now - start);
    }
}

/**
 * @brief Mark the beginning of a wait on backend-side progress.
 *
 * Different wait kinds are tracked separately so query execution, statement
 * replay, and cleanup/discard stalls can be distinguished in production
 * telemetry.
 *
 * @param recv_ctx Session recv context.
 * @param kind Wait category.
 * @return
 */
static inline void stats_mark_wait_backend_begin(recv_context_t* recv_ctx, uint8_t kind)
{
    if (!recv_ctx || !recv_ctx->session || !recv_ctx->session->worker ||
        !recv_ctx->session->worker->stats_ctx) {
        return;
    }
    if ((recv_ctx->session->worker->hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_BACKEND) == 0)
        return;
    if (recv_ctx->flow.wait_backend_start_ns != 0)
        return;

    recv_ctx->flow.wait_backend_start_ns = get_time_ns();
    recv_ctx->flow.wait_backend_kind = kind;
    recv_ctx->flow.wait_backend_query_recv_armed_ns = 0;
    KEEL_STAT_INC(recv_ctx->session->worker->stats_ctx, flow_wait_backend_events);
    switch (kind) {
    case WAIT_BACKEND_KIND_REPLAY:
        KEEL_STAT_INC(recv_ctx->session->worker->stats_ctx, flow_wait_backend_replay_events);
        break;
    case WAIT_BACKEND_KIND_DISCARD:
        KEEL_STAT_INC(recv_ctx->session->worker->stats_ctx, flow_wait_backend_discard_events);
        break;
    case WAIT_BACKEND_KIND_QUERY:
    default:
        KEEL_STAT_INC(recv_ctx->session->worker->stats_ctx, flow_wait_backend_query_events);
        break;
    }
}

/**
 * @brief Mark the moment a query wait actually arms the backend recv.
 *
 * This lets the instrumentation split total backend wait into time spent before
 * the recv was armed versus time attributable to socket/queue/parse wakeup
 * latency afterward.
 *
 * @param recv_ctx Session recv context.
 * @return
 */
static inline void stats_mark_wait_backend_query_recv_armed(recv_context_t* recv_ctx)
{
    if (!recv_ctx || !recv_ctx->session || !recv_ctx->session->worker ||
        !recv_ctx->session->worker->stats_ctx) {
        return;
    }
    if ((recv_ctx->session->worker->hotpath_instr_mask & KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT) == 0)
        return;

    if (recv_ctx->flow.wait_backend_kind != WAIT_BACKEND_KIND_QUERY)
        return;
    if (recv_ctx->flow.wait_backend_start_ns == 0)
        return;
    if (recv_ctx->flow.wait_backend_query_recv_armed_ns != 0)
        return;

    recv_ctx->flow.wait_backend_query_recv_armed_ns = get_time_ns();
}

/**
 * @brief Mark the beginning of a deferred frontend send interval.
 *
 * @param recv_ctx Session recv context.
 * @return
 */
static inline void stats_mark_query_deferred_send_begin(recv_context_t* recv_ctx)
{
    if (!recv_ctx || !recv_ctx->session || !recv_ctx->session->worker ||
        !recv_ctx->session->worker->stats_ctx) {
        return;
    }
    if ((recv_ctx->session->worker->hotpath_instr_mask & KEEL_HOT_INSTR_DEFERRED_SEND) == 0)
        return;

    if (recv_ctx->flow.wait_backend_query_send_start_ns != 0)
        return;

    recv_ctx->flow.wait_backend_query_send_start_ns = get_time_ns();
}

/**
 * @brief Accumulate timing for one completed deferred frontend send.
 *
 * @param recv_ctx Session recv context.
 * @return
 */
static inline void stats_mark_query_deferred_send_end(recv_context_t* recv_ctx)
{
    if (!recv_ctx)
        return;

    if (!recv_ctx->session || !recv_ctx->session->worker ||
        !recv_ctx->session->worker->stats_ctx ||
        (recv_ctx->session->worker->hotpath_instr_mask & KEEL_HOT_INSTR_DEFERRED_SEND) == 0) {
        recv_ctx->flow.wait_backend_query_send_start_ns = 0;
        return;
    }

    uint64_t start = recv_ctx->flow.wait_backend_query_send_start_ns;
    recv_ctx->flow.wait_backend_query_send_start_ns = 0;
    if (start == 0)
        return;

    if (recv_ctx->session && recv_ctx->session->worker && recv_ctx->session->worker->stats_ctx) {
        uint64_t now = get_time_ns();
        if (now > start) {
            KEEL_STAT_INC(recv_ctx->session->worker->stats_ctx,
                          flow_wait_backend_query_deferred_send_events);
            KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                          flow_wait_backend_query_deferred_send_ns_total,
                          now - start);
        }
    }
}

/**
 * @brief Clear deferred-send timing state without recording a completed sample.
 *
 * @param recv_ctx Session recv context.
 * @return
 */
static inline void stats_reset_query_deferred_send(recv_context_t* recv_ctx)
{
    if (!recv_ctx)
        return;
    recv_ctx->flow.wait_backend_query_send_start_ns = 0;
}

/**
 * @brief Close out backend-wait timing when the first backend bytes arrive.
 *
 * This helper records total backend wait and, when enabled, further splits the
 * query path into queue/wakeup, framing, and I/O-adjacent subintervals using
 * completion timing supplied by the reactor.
 *
 * @param recv_ctx Session recv context.
 * @param cqe_wakeup_ns Reactor timestamp for the wakeup associated with the CQE.
 * @param cqe_seen_ns Reactor timestamp for when the CQE batch was observed.
 * @param cqe_batch_size Number of CQEs in the current batch.
 * @param cqe_batch_index Index of this CQE inside the batch.
 * @param callback_entry_ns Timestamp captured at callback entry.
 * @return The wait kind that was just closed out.
 */
static inline uint8_t stats_mark_wait_backend_first_byte(recv_context_t* recv_ctx,
                                                         uint64_t cqe_wakeup_ns,
                                                         uint64_t cqe_seen_ns,
                                                         uint32_t cqe_batch_size,
                                                         uint32_t cqe_batch_index,
                                                         uint64_t callback_entry_ns)
{
    if (!recv_ctx)
        return WAIT_BACKEND_KIND_NONE;

    uint64_t start = recv_ctx->flow.wait_backend_start_ns;
    uint8_t kind = recv_ctx->flow.wait_backend_kind;
    uint64_t query_recv_armed_ns = recv_ctx->flow.wait_backend_query_recv_armed_ns;
    recv_ctx->flow.wait_backend_start_ns = 0;
    recv_ctx->flow.wait_backend_kind = WAIT_BACKEND_KIND_NONE;
    recv_ctx->flow.wait_backend_query_recv_armed_ns = 0;
    if (start == 0)
        return kind;

    if (!recv_ctx->session || !recv_ctx->session->worker ||
        !recv_ctx->session->worker->stats_ctx) {
        return kind;
    }

    uint32_t hot_mask = recv_ctx->session->worker->hotpath_instr_mask;
    bool backend_wait_enabled = (hot_mask & KEEL_HOT_INSTR_WAIT_BACKEND) != 0;
    bool query_split_enabled = (hot_mask & KEEL_HOT_INSTR_WAIT_BACKEND_QUERY_SPLIT) != 0;
    if (!backend_wait_enabled)
        return kind;

    uint64_t now = get_time_ns();
    if (now > start) {
        uint64_t delta = now - start;
        KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                      flow_wait_backend_ns_total,
                      delta);
        switch (kind) {
        case WAIT_BACKEND_KIND_REPLAY:
            KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                          flow_wait_backend_replay_ns_total, delta);
            break;
        case WAIT_BACKEND_KIND_DISCARD:
            KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                          flow_wait_backend_discard_ns_total, delta);
            break;
        case WAIT_BACKEND_KIND_QUERY:
        default:
            KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                          flow_wait_backend_query_ns_total, delta);
            if (!query_split_enabled)
                break;

            if (query_recv_armed_ns != 0 && now > query_recv_armed_ns) {
                uint64_t io_delta = now - query_recv_armed_ns;
                if (io_delta > delta)
                    io_delta = delta;
                KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                              flow_wait_backend_query_io_ns_total,
                              io_delta);
                if (callback_entry_ns > query_recv_armed_ns) {
                    uint64_t reactor_delta = callback_entry_ns - query_recv_armed_ns;
                    if (reactor_delta > io_delta)
                        reactor_delta = io_delta;
                    uint64_t ready_delta = reactor_delta;
                    if (cqe_seen_ns > query_recv_armed_ns) {
                        ready_delta = cqe_seen_ns - query_recv_armed_ns;
                        if (ready_delta > reactor_delta)
                            ready_delta = reactor_delta;
                    }
                    uint64_t ready_wakeup_delta = 0;
                    if (cqe_wakeup_ns > query_recv_armed_ns) {
                        ready_wakeup_delta = cqe_wakeup_ns - query_recv_armed_ns;
                        if (ready_wakeup_delta > ready_delta)
                            ready_wakeup_delta = ready_delta;
                    }
                    KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                  flow_wait_backend_query_io_reactor_ns_total,
                                  reactor_delta);
                    KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                  flow_wait_backend_query_io_reactor_ready_ns_total,
                                  ready_delta);
                    KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                  flow_wait_backend_query_io_reactor_ready_wakeup_ns_total,
                                  ready_wakeup_delta);
                    KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                  flow_wait_backend_query_io_reactor_ready_sched_ns_total,
                                  ready_delta - ready_wakeup_delta);
                    {
                        uint64_t sched_delta = ready_delta - ready_wakeup_delta;
                        if (cqe_batch_index <= 1) {
                            KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                          flow_wait_backend_query_io_reactor_ready_sched_head_ns_total,
                                          sched_delta);
                        } else {
                            KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                          flow_wait_backend_query_io_reactor_ready_sched_tail_ns_total,
                                          sched_delta);
                        }
                        if (cqe_batch_size > 0) {
                            KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                          flow_wait_backend_query_io_reactor_ready_sched_batch_size_sum,
                                          (uint64_t)cqe_batch_size);
                            if (cqe_batch_size == 1) {
                                KEEL_STAT_INC(recv_ctx->session->worker->stats_ctx,
                                              flow_wait_backend_query_io_reactor_ready_sched_batch_size_1_events);
                            } else if (cqe_batch_size == 2) {
                                KEEL_STAT_INC(recv_ctx->session->worker->stats_ctx,
                                              flow_wait_backend_query_io_reactor_ready_sched_batch_size_2_events);
                            } else if (cqe_batch_size == 3) {
                                KEEL_STAT_INC(recv_ctx->session->worker->stats_ctx,
                                              flow_wait_backend_query_io_reactor_ready_sched_batch_size_3_events);
                            } else {
                                KEEL_STAT_INC(recv_ctx->session->worker->stats_ctx,
                                              flow_wait_backend_query_io_reactor_ready_sched_batch_size_4p_events);
                            }
                        }
                        if (cqe_batch_index > 0) {
                            KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                          flow_wait_backend_query_io_reactor_ready_sched_batch_index_sum,
                                          (uint64_t)cqe_batch_index);
                        }
                    }
                    KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                  flow_wait_backend_query_io_reactor_dispatch_ns_total,
                                  reactor_delta - ready_delta);
                    KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                  flow_wait_backend_query_io_service_ns_total,
                                  io_delta - reactor_delta);
                } else {
                    KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                                  flow_wait_backend_query_io_service_ns_total,
                                  io_delta);
                }
                KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                              flow_wait_backend_query_exec_ns_total,
                              delta - io_delta);
            } else {
                KEEL_STAT_ADD(recv_ctx->session->worker->stats_ctx,
                              flow_wait_backend_query_exec_ns_total,
                              delta);
            }
            break;
        }
    }

    return kind;
}

/* ============================================================================
 * worker_setup_session:  core session-init path shared by accept and migration.
 *
 * Takes ownership of client_fd on success (fd is registered with the reactor).
 * Returns 0 on success, -1 on failure (client_fd is closed by caller).
 * ============================================================================ */
/**
 * @brief Create and arm a new frontend session on a worker.
 *
 * This is the shared initialization path for freshly accepted connections and
 * sessions adopted via migration. It allocates a session slot, attaches a recv
 * context, initializes the protocol flow, arms the idle timer, and finally
 * queues the first frontend recv.
 *
 * @param worker Owning worker.
 * @param client_fd Accepted client socket.
 * @return `0` on success, `-1` on any setup failure.
 */
static int worker_setup_session(keel_worker_t* worker, int client_fd)
{
    worker->stats.accepts++;

    /* Admission check — enforce max_clients limit before allocating anything.
     * keel_admission_try_frontend() is wait-free (atomic CAS); it returns
     * KEEL_ADMIT_OK or KEEL_ADMIT_REJECTED.  On rejection the fd is closed by
     * the caller (keel_worker_on_accept) without sending a protocol error. */
    if (worker->admission.max_frontends > 0) {
        if (keel_admission_try_frontend(&worker->admission) != KEEL_ADMIT_OK) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
                "W%u: connection rejected — admission limit (%u frontends)",
                worker->id, worker->admission.max_frontends);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, sessions_created); /* count rejected */
            return -1;
        }
    }

    if (worker->stats_ctx) {
        KEEL_STAT_INC(worker->stats_ctx, sessions_created);
        KEEL_STAT_GAUGE_INC(worker->stats_ctx, sessions_active);
        int64_t active = keel_gauge_get(&worker->stats_ctx->basic.sessions_active);
        KEEL_STAT_PEAK(worker->stats_ctx, sessions_peak, (uint64_t)active);
    }

    /* Allocate session */
    keel_session_t* session = keel_session_slab_alloc(&worker->sessions);
    if (session == NULL) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL, "Worker %u: session pool exhausted", worker->id);
        /* If admission was granted, undo it now that we can't honour the slot */
        if (worker->admission.max_frontends > 0)
            keel_admission_release_frontend(&worker->admission);
        return -1;
    }

    /* Initialize session */
    session->client_fd = client_fd;
    session->server_fd = -1;
    session->state = KEEL_SESSION_INIT;
    session->mode = KEEL_MODE_STARTUP;
    session->worker = worker;
    session->created_at = get_time_ns();
    session->last_activity = session->created_at;
    session->id = ++worker->stats.sessions_created;
    session->backend_conn = NULL;
    session->in_transaction = false;

    /* Start trace span (if tracing is enabled and this session is sampled) */
    keel_tracer_t *tracer = keel_engine_get_tracer(worker->engine);
    KEEL_TRACE_SESSION_START(tracer, session, "session");

    /* Set thread-local trace context for structured log correlation */
    if (session->trace_sampled) {
        char tid[33], sid[17];
        snprintf(tid, sizeof(tid), "%016lx%016lx",
                 (unsigned long)session->trace_ctx.trace_id.hi,
                 (unsigned long)session->trace_ctx.trace_id.lo);
        snprintf(sid, sizeof(sid), "%016lx",
                 (unsigned long)session->trace_span.span_id);
        keel_log_set_trace_context(tid, sid);
    }

    /* Initialize residual buffers */
    keel_residual_init(&session->client_residual);
    keel_residual_init(&session->server_residual);

    /* Set socket non-blocking + TCP_NODELAY + TCP_QUICKACK */
    int flags = fcntl(client_fd, F_GETFL, 0);
    if (flags >= 0) fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
    int opt = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
#ifdef TCP_QUICKACK
    setsockopt(client_fd, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt));
#endif

    /* SO_LINGER=0: send RST (not FIN) when we close the client socket.
     * This eliminates TIME_WAIT on both ends, which is critical on macOS
     * (only ~16K ephemeral ports, 30s TIME_WAIT) when clients reconnect
     * per-transaction (e.g. pgbench -C).  All response data has already
     * been flushed before we call close(), so no in-flight data is lost. */
    {
        struct linger ling = { .l_onoff = 1, .l_linger = 0 };
        setsockopt(client_fd, SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
    }

    /* Allocate recv context from pre-allocated pool — O(1), no syscall */
    recv_context_t* recv_ctx = (recv_context_t*)keel_pool_alloc(worker->recv_ctx_pool);
    if (!recv_ctx) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL, "Worker %u: recv context pool exhausted", worker->id);
        keel_session_slab_free(&worker->sessions, session);
        return -1;
    }
    recv_ctx->session = session;
    session->userdata = recv_ctx;  /* Store for pool wait callback */

    /* Allocate frontend I/O buffer (heap-backed, not embedded in pool slot) */
    if (recv_ctx_alloc_fe(recv_ctx) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL, "Worker %u: fe_buf alloc failed", worker->id);
        keel_pool_free(worker->recv_ctx_pool, recv_ctx);
        keel_session_slab_free(&worker->sessions, session);
        return -1;
    }

    /* Initialize protocol flow — use new vtable-driven flow */
    const char* proto_name = worker->backend_protocol ? worker->backend_protocol : "postgres";
    const keel_proto_flow_vtable_t* flow_vt = keel_proto_flow_get(proto_name);
    if (!flow_vt) flow_vt = &keel_proto_flow_postgres; /* fallback */
    if (keel_session_flow_init(&recv_ctx->flow, flow_vt, session) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "Worker %u: flow init failed", worker->id);
        recv_ctx_free_bufs(recv_ctx);
        keel_pool_free(worker->recv_ctx_pool, recv_ctx);
        keel_session_slab_free(&worker->sessions, session);
        return -1;
    }

    /* Expose plugin context through session for any code that only has
     * a session* pointer (e.g. hooks, admin console). */
    session->plugin_state = recv_ctx->flow.ctx;

    /* Register zombie reaper timer */
    keel_timer_wheel_add(&worker->timers, &recv_ctx->idle_timer,
                        worker->idle_timeout_ms,
                        recv_ctx, session_idle_timeout_cb);

    /* Server-speaks-first: MySQL requires the proxy to send an initial
     * handshake greeting before the client sends anything.
     * Use non-blocking send — freshly accepted socket, tiny payload. */
    if (flow_vt->generate_greeting) {
        uint8_t greet_buf[256];
        ssize_t gl = flow_vt->generate_greeting(recv_ctx->flow.ctx,
                                                 greet_buf, sizeof(greet_buf));
        if (gl > 0) {
            ssize_t sent = keel_try_send_nb(client_fd, greet_buf, (size_t)gl);
            if (sent < 0 || (size_t)sent < (size_t)gl) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                    "Worker %u: failed to send greeting for session %lu",
                    worker->id, (unsigned long)session->id);
                keel_session_flow_destroy(&recv_ctx->flow);
                recv_ctx_free_bufs(recv_ctx);
                keel_pool_free(worker->recv_ctx_pool, recv_ctx);
                keel_session_slab_free(&worker->sessions, session);
                return -1;
            }
        }
    }

    /* Queue recv operation */
    recv_ctx->fe_pending = true;
    int rc = keel_reactor_recv(worker->reactor, client_fd,
                              recv_ctx->fe_buf, recv_ctx->fe_cap,
                              0, recv_ctx, on_client_recv_complete);
    if (rc < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: failed to queue recv for session %lu",
                    worker->id, (unsigned long)session->id);
        keel_session_flow_destroy(&recv_ctx->flow);
        recv_ctx_free_bufs(recv_ctx);
        keel_pool_free(worker->recv_ctx_pool, recv_ctx);
        keel_session_slab_free(&worker->sessions, session);
        return -1;
    }

    /* Audit: record TCP-level connection acceptance.
     * Username/database are not known yet (pre-auth); pass NULL.
     * Client address is resolved via getpeername() for a best-effort IP string. */
    if (worker->audit_log) {
        char peer_addr[64] = "";
        uint16_t peer_port = 0;
        struct sockaddr_storage ss;
        socklen_t sslen = sizeof(ss);
        if (getpeername(client_fd, (struct sockaddr*)&ss, &sslen) == 0) {
            if (ss.ss_family == AF_INET) {
                inet_ntop(AF_INET,
                    &((struct sockaddr_in*)&ss)->sin_addr,
                    peer_addr, sizeof(peer_addr));
                peer_port = ntohs(((struct sockaddr_in*)&ss)->sin_port);
            } else if (ss.ss_family == AF_INET6) {
                inet_ntop(AF_INET6,
                    &((struct sockaddr_in6*)&ss)->sin6_addr,
                    peer_addr, sizeof(peer_addr));
                peer_port = ntohs(((struct sockaddr_in6*)&ss)->sin6_port);
            }
        }
        keel_audit_emit_connect((keel_audit_log_t*)worker->audit_log,
                                KEEL_AUDIT_CONNECT,
                                peer_addr[0] ? peer_addr : NULL, peer_port,
                                NULL, NULL);
    }

    return 0;
}

/**
 * @brief Reactor callback: accept() completed on the listening socket.
 *
 * On success, sets up the new client session.  Re-arms the accept for the
 * next connection (single-shot semantics).  Handles ECANCELED during drain.
 */
static void on_accept_complete(void* userdata, int result)
{
    keel_worker_t* worker = (keel_worker_t*)userdata;
    KEEL_DEBUG_LOG("W%u: on_accept_complete result=%d\n", worker->id, result);

    if (result < 0) {
        if (result == -ECANCELED) {
            /* Accept was cancelled — rearm via main loop */
            worker->accept_rearm_needed = true;
        } else {
            worker->stats.errors++;
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, errors_total);
        }
        /* Rearm accept even on error (single-shot is consumed),
         * unless the worker is draining. */
        if (!atomic_load_explicit(&worker->draining, memory_order_acquire)) {
            if (keel_reactor_accept(worker->reactor, worker->listen_fd,
                                   NULL, NULL, worker, on_accept_complete, false) < 0) {
                worker->accept_rearm_needed = true;
            }
        }
        return;
    }

    int client_fd = result;

    /* In drain mode (engine-level or worker-level), reject new connections
     * with a SQL error. */
    if ((worker->engine && keel_engine_is_draining(worker->engine)) ||
        atomic_load_explicit(&worker->draining, memory_order_acquire)) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_CONN,
            "Worker %u: rejecting new connection during drain (fd=%d)",
            worker->id, client_fd);
        /* Best-effort: send a PostgreSQL FATAL error so the client
         * sees "server is shutting down" rather than a raw TCP close.
         * For MySQL protocol the greeting handshake hasn't started, so
         * a clean close is the only safe option. */
        const char* proto = worker->backend_protocol ? worker->backend_protocol : "postgres";
        if (proto[0] == 'p') {
            /* Build PG ErrorResponse: FATAL 57P03 (cannot_connect_now) */
            uint8_t buf[128];
            size_t pos = 0;
            buf[pos++] = 'E';         /* message type */
            pos += 4;                 /* length placeholder (bytes 1-4) */
            #define PG_DRAIN_FIELD(tag, val) do { \
                buf[pos++] = (tag); \
                size_t vlen = strlen(val) + 1; \
                memcpy(&buf[pos], (val), vlen); \
                pos += vlen; \
            } while (0)
            PG_DRAIN_FIELD('S', "FATAL");
            PG_DRAIN_FIELD('V', "FATAL");
            PG_DRAIN_FIELD('C', "57P03");
            PG_DRAIN_FIELD('M', "the database system is shutting down");
            PG_DRAIN_FIELD('D', "server is draining");
            #undef PG_DRAIN_FIELD
            buf[pos++] = '\0';        /* terminator */
            /* Patch length (includes self 4 bytes, excludes 'E') */
            uint32_t len = (uint32_t)(pos - 1);
            buf[1] = (uint8_t)(len >> 24);
            buf[2] = (uint8_t)(len >> 16);
            buf[3] = (uint8_t)(len >> 8);
            buf[4] = (uint8_t)(len);
            int fl = fcntl(client_fd, F_GETFL, 0);
            if (fl >= 0) fcntl(client_fd, F_SETFL, fl | O_NONBLOCK);
            keel_try_send_nb(client_fd, buf, pos);
        }
        close(client_fd);
        /* Don't rearm accept — we're shutting down */
        return;
    }

    if (worker_setup_session(worker, client_fd) < 0) {
        close(client_fd);
    }

    /* Single-shot accept: rearm immediately so this worker competes
     * for the next incoming connection — unless we're draining. */
    if (!atomic_load_explicit(&worker->draining, memory_order_acquire)) {
        if (keel_reactor_accept(worker->reactor, worker->listen_fd,
                               NULL, NULL, worker, on_accept_complete, false) < 0) {
            worker->accept_rearm_needed = true;
        }
    }
}

/* ============================================================================
 * Pool Guardrail: cleanup-and-return helper
 * ============================================================================
 *
 * Implements spec §5: the Core must not return a backend_fd to the shared
 * pool until it has successfully completed a ROLLBACK / DISCARD ALL (PG)
 * or a COM_RESET_CONNECTION / COM_QUERY ROLLBACK (MySQL) on that fd.
 *
 * Preconditions assumed by caller:
 *   - be_conn->fd >= 0
 *   - be_conn->state == BACKEND_CONN_ACTIVE
 *
 * The connection was in a transaction when the client disconnected.
 * Rather than attempting a non-blocking ROLLBACK+DISCARD ALL with an
 * unreliable MSG_DONTWAIT drain (which can leave stale responses in the
 * TCP buffer and corrupt the next borrower), we simply CLOSE the backend
 * connection.  The pool will create a replacement asynchronously.
 */
/**
 * @brief Close the backend connection and update pool state after a session error.
 *
 * Rather than attempting a non-blocking ROLLBACK/DISCARD ALL (which can
 * leave stale data in the TCP buffer), the backend socket is closed
 * immediately so the pool creates a fresh replacement asynchronously.
 *
 * @param worker   Owning worker.
 * @param session  Session being cleaned up.
 * @param flow     Protocol flow (unused).
 * @param be_conn  Backend connection to close.
 * @param reason   Cleanup reason code (unused).
 */
static void backend_cleanup_and_return(keel_worker_t*        worker,
                                        keel_session_t*       session,
                                        keel_session_flow_t*  flow,
                                        backend_conn_t*       be_conn,
                                        keel_cleanup_reason_t reason)
{
    (void)flow;
    (void)reason;

    /* Close backend — don't try to reuse a connection that was mid-transaction
     * when the FE disconnected.  Non-blocking cleanup is race-prone. */
    if (be_conn->fd >= 0) { close(be_conn->fd); be_conn->fd = -1; }
    atomic_store(&be_conn->state, BACKEND_CONN_CLOSED);
    be_conn->pinned_session     = NULL;
    be_conn->in_transaction     = false;
    be_conn->current_state_hash = 0;
    be_conn->stmt_set_hash      = 0;
    be_conn->hard_pinned        = false;
    if (be_conn->pool) be_conn->pool->active_count--;
    session->backend_conn = NULL;
    session->server_fd    = -1;

    if (worker->stats_ctx)
        KEEL_STAT_INC(worker->stats_ctx, pool_returns);
}

/* ============================================================================
 * Client Recv Callback
 * ============================================================================ */

/**
 * @brief Close a frontend session and release or destroy its associated backend.
 *
 * The close path has to coordinate with outstanding reactor operations. If any
 * frontend recv, backend recv, or deferred send is still in flight, cleanup is
 * converted into a deferred close so the final resource release happens only
 * after those callbacks unwind safely.
 *
 * Backend handling is conservative: idle reusable connections go back to the
 * pool, backends with open transactions are cleaned or closed, and anything
 * suspicious is destroyed rather than risk contaminating the next borrower.
 *
 * @param worker Owning worker.
 * @param session Session to close.
 * @param recv_ctx Recv context associated with the session.
 * @return
 */
static void close_session(keel_worker_t* worker, keel_session_t* session, 
                          recv_context_t* recv_ctx)
{
    /* If there's a pending backend/send operation, we can't free recv_ctx yet.
     * Mark it as closing and let the callback complete the cleanup. */
    if (recv_ctx && (recv_ctx->be_pending || recv_ctx->fe_pending || recv_ctx->send_pending)) {
        recv_ctx->closing = true;
        
        /* Close the client fd — this will cause any pending io_uring recv
         * to complete with an error, triggering the callback which will
         * see the closing flag and do the final cleanup. */
        if (session->client_fd >= 0) {
            close(session->client_fd);
            session->client_fd = -1;
        }
        
        /* Directly close and release the backend pool slot.
         * We can't use backend_pool_release_session here because it searches
         * by pinned_session, which is NOT set for non-pinned connections. */
        if (session->backend_conn) {
            backend_conn_t* be_conn = session->backend_conn;
            /* Note: server_fd == be_conn->fd, close via be_conn */
            if (be_conn->fd >= 0) {
                close(be_conn->fd);
                be_conn->fd = -1;
            }
            atomic_store(&be_conn->state, BACKEND_CONN_CLOSED);
            be_conn->pinned_session = NULL;
            be_conn->in_transaction = false;
            be_conn->current_state_hash = 0;
            be_conn->hard_pinned = false;
            if (be_conn->pool) {
                be_conn->pool->active_count--;
            }
            session->backend_conn = NULL;
            session->server_fd = -1;
        } else if (session->server_fd >= 0) {
            close(session->server_fd);
            session->server_fd = -1;
        }
        
        /* Cancel the timer but don't free - backend callback will do it */
        keel_timer_wheel_cancel(&worker->timers, &recv_ctx->idle_timer);
        
        KEEL_DEBUG_LOG("W%u: session %lu marked for deferred close (BE=%d FE=%d)\n",
                     worker->id, (unsigned long)session->id,
                     recv_ctx->be_pending, recv_ctx->fe_pending);
        return;
    }

    /* No pending backend operation - can safely clean up now */
    if (recv_ctx) {
        keel_timer_wheel_cancel(&worker->timers, &recv_ctx->idle_timer);
        keel_session_flow_destroy(&recv_ctx->flow);
    }

    if (session->client_fd >= 0) {
        close(session->client_fd);
        session->client_fd = -1;
    }
    
    /* Handle pooled backend connection.
     *
     * If the backend is idle (not mid-transaction, fd valid), return it to
     * the pool so it can be reused immediately by a waiting session.  This
     * avoids the overhead of closing + reconnecting (TCP + SCRAM ~5 ms)
     * which, under heavy short-lived client load (pgbench -C), caused the
     * pool to drain to zero and stall for seconds while the refill timer
     * recreated connections.
     *
     * Only destroy the backend when it's unsafe to reuse:
     *   - in_transaction (uncommitted state)
     *   - fd already closed
     *   - hard-pinned (prepared statements / SET) */
    if (session->backend_conn) {
        backend_conn_t* be_conn = session->backend_conn;
        
        bool can_return = (be_conn->fd >= 0) &&
                          !be_conn->in_transaction &&
                          !be_conn->hard_pinned &&
                          atomic_load(&be_conn->state) == BACKEND_CONN_ACTIVE;
        
        if (can_return && be_conn->pool) {
            /* Safe to return — backend is idle after last query completed */
            session->backend_conn = NULL;
            session->server_fd = -1;
            backend_pool_return(be_conn->pool, be_conn, false);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, pool_returns);
        } else if (be_conn->in_transaction &&
                   !be_conn->hard_pinned &&
                   be_conn->fd >= 0 &&
                   atomic_load(&be_conn->state) == BACKEND_CONN_ACTIVE &&
                   be_conn->pool != NULL &&
                   recv_ctx != NULL &&
                   recv_ctx->flow.flow != NULL &&
                   recv_ctx->flow.flow->build_cleanup != NULL) {
            /* Spec §5 guardrail: backend holds an open transaction.
             * Send ROLLBACK / DISCARD ALL (PG) or COM_RESET_CONNECTION
             * (MySQL) before releasing.  On success the connection is
             * returned to the pool for reuse; on failure it is closed. */
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, proxy_orphaned_transactions_total);
            backend_cleanup_and_return(worker, session, &recv_ctx->flow,
                                       be_conn, KEEL_CLEANUP_FE_DISCONNECT);
        } else {
            /* Unsafe — close the backend fd and mark slot as CLOSED */
            if (be_conn->fd >= 0) {
                close(be_conn->fd);
                be_conn->fd = -1;
            }
            atomic_store(&be_conn->state, BACKEND_CONN_CLOSED);
            be_conn->pinned_session = NULL;
            be_conn->in_transaction = false;
            be_conn->current_state_hash = 0;
            be_conn->hard_pinned = false;
            if (be_conn->pool) {
                be_conn->pool->active_count--;
            }
            session->backend_conn = NULL;
            session->server_fd = -1;
        }
    } else if (session->server_fd >= 0) {
        close(session->server_fd);
        session->server_fd = -1;
    }
    
    /* plugin_state borrow is cleared; the flow vtable owns the context and
     * frees it in keel_session_flow_destroy() called above or below. */
    session->plugin_state = NULL;
    session->fast_forward_mode = 0;
    keel_residual_clear(&session->client_residual);
    keel_residual_clear(&session->server_residual);

    session->state_profile = NULL;
    session->pin_reason = 0;
    session->hard_pinned = false;
    session->state_hash = 0;
    session->in_transaction = false;

    /* Record session duration latency */
    if (worker->stats_ctx && session->created_at) {
        uint64_t dur = (uint64_t)keel_stats_now_ns() - session->created_at;
        KEEL_STAT_LATENCY(worker->stats_ctx, session_duration_ns, dur);
    }

    /* End trace span (if sampled) and submit for export */
    {
        keel_tracer_t *tracer = keel_engine_get_tracer(worker->engine);
        KEEL_TRACE_SESSION_END(tracer, session);
    }

    keel_log_clear_trace_context();

    /* Audit: DISCONNECT — emit once per authenticated session at teardown. */
    if (worker->audit_log && (session->flags & KEEL_SESSION_FLAG_AUTHENTICATED)) {
        keel_audit_emit_connect(
            (keel_audit_log_t*)worker->audit_log,
            KEEL_AUDIT_DISCONNECT,
            NULL, 0,
            session->username[0] ? session->username : NULL,
            session->database[0] ? session->database : NULL);
    }

    /* Release admission slot so the next pending connection can be accepted */
    if (worker->admission.max_frontends > 0)
        keel_admission_release_frontend(&worker->admission);

    keel_session_slab_free(&worker->sessions, session);
    worker->stats.sessions_closed++;
    if (worker->stats_ctx) {
        KEEL_STAT_INC(worker->stats_ctx, sessions_closed);
        KEEL_STAT_GAUGE_DEC(worker->stats_ctx, sessions_active);
    }
    recv_ctx_free_bufs(recv_ctx);
    keel_pool_free(worker->recv_ctx_pool, recv_ctx);
}

/**
 * @brief Zombie reaper callback — fired by timer wheel when a session has
 *        been idle longer than worker->idle_timeout_ms.
 *
 * If the session holds a backend connection inside an open transaction,
 * the plugin's build_cleanup() is called first (ROLLBACK; DISCARD ALL for PG,
 * COM_RESET_CONNECTION for MySQL), then the backend is returned to the pool.
 * Either way the frontend is closed afterwards.
 */
static void session_idle_timeout_cb(void* userdata)
{
    recv_context_t* recv_ctx = (recv_context_t*)userdata;
    keel_session_t* session = recv_ctx->session;
    keel_worker_t* worker = session->worker;

    uint64_t now = get_time_ns();
    if (worker->idle_timeout_ms > 0) {
        uint64_t timeout_ns = (uint64_t)worker->idle_timeout_ms * 1000000ULL;
        uint64_t last_activity = session->last_activity;
        if (now > last_activity && (now - last_activity) < timeout_ns) {
            uint64_t remaining_ns = timeout_ns - (now - last_activity);
            uint32_t remaining_ms = (uint32_t)(remaining_ns / 1000000ULL);
            if (remaining_ms < 1)
                remaining_ms = 1;
            keel_timer_wheel_add(&worker->timers, &recv_ctx->idle_timer,
                                remaining_ms,
                                recv_ctx, session_idle_timeout_cb);
            return;
        }
    }

    KEEL_LOG_WARN(KEEL_LOG_CAT_CONN, "Worker %u: zombie reaper — session %lu idle for >%u ms, closing",
                worker->id, (unsigned long)session->id, worker->idle_timeout_ms);

    /* If we hold a backend that's in a transaction, send cleanup first.
     * Use the plugin's build_cleanup (ROLLBACK; DISCARD ALL for PG,
     * COM_RESET_CONNECTION for MySQL) rather than a hardcoded PG query.
     * After cleanup, close_session will find !in_transaction and return
     * the backend to the pool instead of hard-closing it. */
    if (session->backend_conn && session->in_transaction) {
        if (worker->stats_ctx)
            KEEL_STAT_INC(worker->stats_ctx, proxy_orphaned_transactions_total);
        backend_conn_t* be_conn = session->backend_conn;
        if (recv_ctx != NULL &&
            recv_ctx->flow.flow != NULL &&
            recv_ctx->flow.flow->build_cleanup != NULL &&
            be_conn->fd >= 0) {
            uint8_t cleanup_buf[256];
            ssize_t cn = recv_ctx->flow.flow->build_cleanup(
                    recv_ctx->flow.ctx, KEEL_CLEANUP_TIMEOUT,
                    cleanup_buf, sizeof(cleanup_buf));
            if (cn > 0)
                (void)send(be_conn->fd, cleanup_buf, (size_t)cn,
                           MSG_NOSIGNAL | MSG_DONTWAIT);
        } else {
            /* Fallback: hardcoded PG ROLLBACK for legacy/no-vtable path */
            if (be_conn->fd >= 0) {
                const char rollback_q[] = "Q\0\0\0\x0eROLLBACK;\0";
                (void)send(be_conn->fd, rollback_q, sizeof(rollback_q) - 1,
                           MSG_NOSIGNAL | MSG_DONTWAIT);
            }
        }
        /* Drain response (ReadyForQuery / OK) — non-blocking */
        char drain[256];
        (void)recv(be_conn->fd, drain, sizeof(drain), MSG_DONTWAIT);
        session->in_transaction = false;
    }

    close_session(worker, session, recv_ctx);
}

/**
 * @brief Pool wait callback — resumes a session that was waiting for a backend
 *
 * Called by backend_pool_return() when a backend becomes available
 * for a queued session. Borrows a backend and resumes flow processing.
 */
static void pool_wait_resume_cb(void* session_ptr, void* userdata)
{
    keel_session_t* session = (keel_session_t*)session_ptr;
    keel_worker_t* worker = session->worker;
    recv_context_t* recv_ctx = session->userdata;

    /* Session is no longer queued in WAIT_POOL once callback fires. */
    stats_mark_wait_pool_end(recv_ctx);

    /* NULL userdata signals a wait-queue timeout — send error to client
     * and close the session instead of trying to borrow a backend.
     * Use non-blocking send — socket has been idle. */
    if (!userdata) {
        if (session->client_fd >= 0 && recv_ctx) {
            const keel_proto_flow_vtable_t* flow = recv_ctx->flow.flow;
            if (flow && flow->generate_error) {
                /* Build combined error + optional RFQ into one buffer */
                uint8_t sendbuf[512];
                size_t sendlen = 0;
                {
                    uint8_t errbuf[256];
                    ssize_t el = flow->generate_error(
                            recv_ctx->flow.ctx, "57014",
                            "backend connection pool timeout",
                            errbuf, sizeof(errbuf));
                    if (el > 0) { memcpy(sendbuf, errbuf, (size_t)el); sendlen += (size_t)el; }
                }
                if (flow->name && strcmp(flow->name, "postgres") == 0) {
                    uint8_t z[] = {'Z',0,0,0,5,'I'};
                    memcpy(sendbuf + sendlen, z, sizeof(z)); sendlen += sizeof(z);
                }
                if (sendlen > 0)
                    keel_try_send_nb(session->client_fd, sendbuf, sendlen);
            }
        }
        if (recv_ctx)
            close_session(worker, session, recv_ctx);
        return;
    }
    
    /* Check if client is still connected before borrowing a backend.
     * When a session is queued for pool wait, the FE recv is NOT armed,
     * so we won't get notified of client disconnect. Use a non-blocking
     * recv(MSG_PEEK) to check if the client fd is still alive. */
    if (session->client_fd >= 0) {
        char peek;
        ssize_t pr = recv(session->client_fd, &peek, 1, MSG_PEEK | MSG_DONTWAIT);
        if (pr == 0) {
            /* Client disconnected — clean up without borrowing */
            recv_context_t* recv_ctx = session->userdata;
            close_session(worker, session, recv_ctx);
            return;
        }
        /* pr < 0 with EAGAIN/EWOULDBLOCK is normal (no data yet, still connected) */
        if (pr < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            /* Client fd error — clean up */
            recv_context_t* recv_ctx = session->userdata;
            close_session(worker, session, recv_ctx);
            return;
        }
    } else {
        /* Client fd already closed — session is dead */
        recv_context_t* recv_ctx = session->userdata;
        if (recv_ctx) {
            close_session(worker, session, recv_ctx);
        }
        return;
    }
    
    /* Use the pool pointer from userdata — this is the pool the session
     * was queued on (could be primary or a replica pool).
     * userdata is guaranteed non-NULL here (timeout case handled above). */
    backend_pool_t* pool = (backend_pool_t*)userdata;

    if (!recv_ctx) return;

    /* Use borrow_with_stmts rather than the plain borrow so that the
     * session gets either:
     *   (a) a backend with an EXACT stmt_set_hash match  → no replay needed
     *   (b) a STMT-CLEAN backend (stmt_set_hash == 0)   → safe to replay on
     *
     * Using plain borrow(pool, 0) here could grab a backend from idle_list
     * that has prepared statements from a DIFFERENT session.  All sysbench
     * (and many real) clients share the same statement names, so replay
     * would send Parse("sbstmt1") onto a backend that already has "sbstmt1"
     * → ErrorResponse → backend stuck in error-recovery (needs Sync) →
     * keel waiting for RFQ → permanent deadlock. */
    uint64_t stmt_hash = 0;
    const keel_proto_flow_vtable_t* flow = recv_ctx->flow.flow;
    if (flow && flow->get_stmt_replay && recv_ctx->flow.ctx) {
        flow->get_stmt_replay(recv_ctx->flow.ctx,
                              NULL, NULL, NULL, &stmt_hash);
    }
    bool needs_replay = false;
    backend_conn_t* be_conn = backend_pool_borrow_with_stmts(
            pool, 0, stmt_hash, &needs_replay);

    if (!be_conn) {
        /* Opportunistic reclaim: a backend may have completed DISCARD ALL
         * since the last refill tick. Drain CLEANING now and retry once
         * before re-queuing this waiter. */
        backend_pool_drain_cleaning(pool);

        bool needs_replay_retry = false;
        be_conn = backend_pool_borrow_with_stmts(
                pool, 0, stmt_hash, &needs_replay_retry);
        if (be_conn) {
            needs_replay = needs_replay_retry;
        }
    }

    if (!be_conn) {
        /* No suitable backend available — re-queue the session.
         * A returning backend (or the refill timer) will wake it. */
        if (worker->stats_ctx)
            KEEL_STAT_INC(worker->stats_ctx, pool_wait_resume_requeues);
        KEEL_LOG_WARN(KEEL_LOG_CAT_POOL,
            "W%u: pool_wait_resume: no backend for stmt_hash=0x%016llx "
            "(active=%zu clean=%zu wait=%zu) re-queuing",
            worker->id, (unsigned long long)stmt_hash,
            pool->active_count, pool->clean_count,
            pool->wait_queue_size);
        if (recv_ctx->flow.pending_msg) {
            backend_pool_queue_wait(pool, session, pool);
        } else {
            /* No pending message — session is stale, close it */
            close_session(worker, session, recv_ctx);
        }
        return;
    }

    if (worker->stats_ctx)
        KEEL_STAT_INC(worker->stats_ctx, pool_wait_resume_success);
    
    keel_flow_result_t fr = keel_engine_flow_resume_from_pool(
        &recv_ctx->flow, session, be_conn);
    
    if (fr == KEEL_FLOW_WAIT_BACKEND || fr == KEEL_FLOW_WAIT_STMT_REPLAY) {
        /* Need to receive backend response.
         *
         * RACE FIX: When resume_from_pool sends a message to the backend
         * (DISCARD ALL, Parse replay, or the client's pending message),
         * the backend may respond before we submit the reactor recv SQE.
         * With io_uring, if the CQE fires while no recv is pending the
         * completion is silently lost and no new event fires until fresh
         * data arrives — but the response is already sitting in the socket
         * buffer, so keel waits forever (deadlock).
         *
         * Solution: mirror the hot-path in on_client_recv_complete: try an
         * immediate non-blocking recv() first.  If data is already waiting
         * (the backend responded very quickly) we process it immediately.
         * Only if EAGAIN do we submit a reactor_recv SQE to wait. */
        backend_recv_context_t* be_ctx = &recv_ctx->be_ctx;
        be_ctx->client_ctx = recv_ctx;

        /* Lazy-alloc backend buffer on first borrow */
        if (recv_ctx_ensure_be(recv_ctx) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL,
                "W%u: be_buf alloc failed on pool resume", worker->id);
            close_session(worker, session, recv_ctx);
            return;
        }

        uint8_t wait_kind = WAIT_BACKEND_KIND_QUERY;
        if (fr == KEEL_FLOW_WAIT_STMT_REPLAY)
            wait_kind = recv_ctx->flow.stmt_replay_needs_discard
                        ? WAIT_BACKEND_KIND_DISCARD
                        : WAIT_BACKEND_KIND_REPLAY;
        stats_mark_wait_backend_begin(recv_ctx, wait_kind);

        ssize_t imm = recv(session->server_fd, recv_ctx->be_buf,
                           recv_ctx->be_cap, MSG_DONTWAIT);
        KEEL_DEBUG_LOG("W%u: pool_resume imm_recv fd=%d imm=%zd\n",
            worker->id, session->server_fd, imm);
        if (imm > 0) {
            /* Data was already available — process immediately. */
            on_backend_recv_complete(be_ctx, (int)imm);
            return;
        }
        if (imm < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            /* Nothing yet — arm reactor_recv; will fire when data arrives. */
            if (wait_kind == WAIT_BACKEND_KIND_QUERY)
                stats_mark_wait_backend_query_recv_armed(recv_ctx);
            recv_ctx->be_pending = true;
            KEEL_DEBUG_LOG("W%u: pool_resume EAGAIN → reactor_recv fd=%d\n",
                         worker->id, session->server_fd);
            int rc = keel_reactor_recv(worker->reactor, session->server_fd,
                                       recv_ctx->be_buf, recv_ctx->be_cap,
                                       0, be_ctx, on_backend_recv_complete);
            if (rc < 0) {
                recv_ctx->be_pending = false;
                KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                    "W%u: failed to queue BE recv after pool resume", worker->id);
                close_session(worker, session, recv_ctx);
            }
            return;
        }
        /* Backend closed or error */
        KEEL_LOG_WARN(KEEL_LOG_CAT_CONN,
            "W%u: pool_resume BE recv error: imm=%zd errno=%d",
            worker->id, imm, errno);
        close_session(worker, session, recv_ctx);
    } else if (fr == KEEL_FLOW_OK) {
        /* Re-arm frontend recv */
        recv_ctx->fe_pending = true;
        int rc = keel_reactor_recv(worker->reactor, session->client_fd,
                                   recv_ctx->fe_buf, recv_ctx->fe_cap,
                                   0, recv_ctx, on_client_recv_complete);
        if (rc < 0) {
            close_session(worker, session, recv_ctx);
        }
    } else if (fr == KEEL_FLOW_SEND_PENDING) {
        if (queue_deferred_send(worker, recv_ctx) < 0)
            close_session(worker, session, recv_ctx);
    } else if (fr == KEEL_FLOW_ERROR || fr == KEEL_FLOW_CLOSED) {
        close_session(worker, session, recv_ctx);
    }
}

/**
 * @brief Pool prune timer callback — prunes idle backend connections
 *
 * Called periodically to close backend connections that have been idle
 * longer than pool_idle_timeout_ms, while keeping at least min_connections.
 */
static void pool_prune_timer_cb(void* userdata)
{
    keel_worker_t* worker = (keel_worker_t*)userdata;
    
    /* Prune all server pools */
    for (size_t i = 0; i < worker->server_pool_count; i++) {
        if (worker->server_pools[i]) {
            size_t closed = backend_pool_prune_idle(worker->server_pools[i]);
            if (closed > 0) {
                if (worker->stats_ctx)
                    KEEL_STAT_ADD(worker->stats_ctx, pool_destroys, closed);
                KEEL_DEBUG_LOG("W%u: pruned %zu idle backend connections (pool %zu)\n",
                             worker->id, closed, i);
            }
        }
    }
    
    /* Re-arm the timer */
    keel_timer_wheel_add(&worker->timers, &worker->pool_prune_timer,
                        worker->pool_prune_interval_ms,
                        worker, pool_prune_timer_cb);
}

/**
 * @brief Pool refill timer callback — reconnects closed backend connections
 *
 * Called periodically (every 100ms) to restore backend connections that were
 * closed due to client disconnects. Now that refill uses async connect,
 * we can kick off multiple reconnects per timer firing without blocking.
 * Each call to refill_one queues an async connect on the reactor and
 * returns immediately.
 */
static void pool_refill_timer_cb(void* userdata)
{
    keel_worker_t* worker = (keel_worker_t*)userdata;
    bool urgent_pool_work = false;
    
    /* Kick off up to 32 async reconnects per timer tick.
     * Each refill_one returns instantly (just queues an io_uring connect).
     * Increased from 8 to handle burst connection init (e.g. 500-thread sysbench). */
    int refilled = 0;
    for (size_t si = 0; si < worker->server_pool_count; si++) {
        if (worker->server_pools[si]) {
            for (int i = 0; i < 32; i++) {
                if (backend_pool_refill_one(worker->server_pools[si]) == 0)
                    break;
                refilled++;
            }
        }
    }

    if (refilled > 0 && worker->stats_ctx)
        KEEL_STAT_ADD(worker->stats_ctx, pool_creates, refilled);
    
    /* Drain connections stuck in CLEANING state (DISCARD ALL in-flight).
     * This reclaims backends whose cleanup response arrived since the last tick. */
    for (size_t si = 0; si < worker->server_pool_count; si++) {
        if (worker->server_pools[si]) {
            backend_pool_drain_cleaning(worker->server_pools[si]);
            if (worker->server_pools[si]->wait_queue_size > 0 ||
                worker->server_pools[si]->cleaning_count > 0) {
                urgent_pool_work = true;
            }
        }
    }

    /* Expire sessions that have been waiting too long for a backend connection.
     * This prevents clients from hanging forever when all backends are down. */
    for (size_t si = 0; si < worker->server_pool_count; si++) {
        if (worker->server_pools[si])
            backend_pool_expire_waiters(worker->server_pools[si]);
    }

    /* Prune idle and aged connections.
     * idle_timeout_ms: close connections that have been idle too long.
     * max_connection_age_ms: close connections that have existed too long.
     * Both respect min_connections — they won't shrink below the minimum. */
    for (size_t si = 0; si < worker->server_pool_count; si++) {
        if (worker->server_pools[si]) {
            size_t pruned = backend_pool_prune_idle(worker->server_pools[si]);
            pruned += backend_pool_prune_aged(worker->server_pools[si]);
            if (pruned > 0 && worker->stats_ctx)
                KEEL_STAT_ADD(worker->stats_ctx, pool_destroys, pruned);
        }
    }

    /* Re-arm policy:
     *   - fast cadence when we kicked refills, or when there are waiters /
     *     CLEANING slots that need prompt reclaim and wakeup
     *   - slow cadence only when pools are quiescent with no urgent work
     *
     * Backing off while waiters/cleaning exist can add large handoff delays
     * (timer-driven reclaim), so keep the fast interval in that case. */
    uint32_t interval = (refilled > 0 || urgent_pool_work)
                        ? worker->pool_refill_interval_ms
                        : worker->pool_refill_backoff_ms;
    keel_timer_wheel_add(&worker->timers, &worker->pool_refill_timer,
                        interval, worker, pool_refill_timer_cb);
}

/* ============================================================================
 * Connection Rebalance Timer
 * ============================================================================
 * Periodically checks for worker load imbalance and migrates idle sessions
 * from overloaded workers to less-loaded ones.
 *
 * Algorithm:
 *   1. Compute average session count across all running workers.
 *   2. If this worker's count > average * threshold / 100, it's "hot".
 *   3. Walk sessions looking for idle, migratable ones.
 *   4. Migrate up to rebalance_max_per_tick sessions per tick.
 *   5. Re-arm the timer.
 */
/**
 * @brief Timer callback: attempt to rebalance sessions across workers.
 *
 * Checks per-worker session counts, migrates up to rebalance_max_per_tick
 * idle sessions from this worker to less-loaded workers, then re-arms the
 * timer.
 *
 * @param userdata  Pointer to the owning keel_worker_t.
 */
static void rebalance_timer_cb(void* userdata)
{
    keel_worker_t* worker = (keel_worker_t*)userdata;
    const keel_engine_config_t* cfg = keel_engine_get_config(worker->engine);

    if (!cfg || !cfg->rebalance_enabled) goto rearm;

    if (worker->stats_ctx)
        KEEL_STAT_INC(worker->stats_ctx, rebalance_checks);

    /* --- Step 1: Compute per-worker session counts --- */
    uint32_t num_workers = keel_engine_get_num_workers(worker->engine);
    if (num_workers <= 1) goto rearm;

    size_t my_count = worker->sessions.allocated;
    size_t total_count = 0;
    size_t min_count = SIZE_MAX;

    for (uint32_t i = 0; i < num_workers; i++) {
        const keel_worker_t* w = keel_engine_get_worker(worker->engine, i);
        if (!w || w->state != KEEL_WORKER_RUNNING) continue;
        size_t c = w->sessions.allocated;
        total_count += c;
        if (c < min_count) min_count = c;
    }

    /* Average sessions per worker */
    size_t avg = total_count / num_workers;

    /* --- Step 2: Check threshold --- */
    /* threshold_pct is e.g. 125 meaning 1.25x.  We check:
     *   my_count * 100 > avg * threshold_pct
     * This avoids floating-point arithmetic on the hot path. */
    uint32_t threshold = cfg->rebalance_threshold_pct;
    if (threshold == 0) threshold = 125;

    if (my_count * 100 <= avg * threshold) {
        /* Not imbalanced enough — skip */
        if (worker->stats_ctx)
            KEEL_STAT_INC(worker->stats_ctx, rebalance_skipped);
        goto rearm;
    }

    /* Must have at least 2 more sessions than the minimum to be worth moving */
    if (my_count <= min_count + 1) {
        if (worker->stats_ctx)
            KEEL_STAT_INC(worker->stats_ctx, rebalance_skipped);
        goto rearm;
    }

    /* --- Step 3: Migrate sessions --- */
    uint32_t max_per_tick = cfg->rebalance_max_per_tick;
    if (max_per_tick == 0) max_per_tick = 4;

    /* Target: bring this worker down to at most avg + 1 */
    size_t target = (avg > 0) ? (avg + 1) : 1;
    uint32_t migrated = 0;

    for (size_t i = 0; i < worker->sessions.capacity && migrated < max_per_tick; i++) {
        if (worker->sessions.allocated <= target) break;

        keel_session_t* s = &worker->sessions.sessions[i];
        if (!keel_migration_can_migrate(s)) continue;

        if (keel_worker_migrate_session(worker, s) == 0) {
            migrated++;
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, rebalance_migrations);

            KEEL_LOG_DEBUG(KEEL_LOG_CAT_CONN,
                "W%u: rebalance: migrated session %lu (my=%zu avg=%zu min=%zu)",
                worker->id, (unsigned long)s->id,
                worker->sessions.allocated, avg, min_count);
        }
    }

    if (migrated > 0) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CONN,
            "W%u: rebalance: migrated %u sessions (was %zu, avg %zu, min %zu)",
            worker->id, migrated, my_count, avg, min_count);
    }

rearm:
    ; /* empty statement after label for C standard compliance */
    uint32_t interval = (cfg && cfg->rebalance_interval_ms > 0)
                        ? cfg->rebalance_interval_ms : 5000;
    keel_timer_wheel_add(&worker->timers, &worker->rebalance_timer,
                        interval, worker, rebalance_timer_cb);
}

/* ============================================================================
 * Deferred Send Completion (io_uring)
 * ============================================================================
 * When engine_flow returns KEEL_FLOW_SEND_PENDING, the worker queues the
 * unsent data via keel_reactor_send (io_uring).  This callback fires when
 * the send completes and resumes the flow according to pending_send_resume.
 */

/**
 * @brief Queue the deferred send stored in session flow via io_uring.
 * @return 0 on success, -1 on error (caller should close session).
 */
static int queue_deferred_send(keel_worker_t* worker,
                               recv_context_t* recv_ctx)
{
    keel_session_flow_t* sf = &recv_ctx->flow;
    if (sf->pending_send_off >= sf->pending_send_len) {
        errno = EINVAL;
        return -1;
    }

    if (sf->pending_send_resume == KEEL_FLOW_WAIT_BACKEND) {
        stats_mark_query_deferred_send_begin(recv_ctx);
    }

    size_t remaining = sf->pending_send_len - sf->pending_send_off;
    recv_ctx->send_pending = true;
    int rc = keel_reactor_send(worker->reactor,
                              sf->pending_send_fd,
                              sf->pending_send_buf + sf->pending_send_off,
                              remaining,
                              MSG_NOSIGNAL,
                              recv_ctx,
                              on_deferred_send_complete);
    if (rc < 0) {
        recv_ctx->send_pending = false;
        KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
            "Worker %u: failed to queue deferred send", worker->id);
        return -1;
    }
    return 0;
}

/**
 * @brief Resume the session after a deferred send completes.
 *
 * Handles short sends (re-queues), then follows the flow's
 * pending_send_resume action: re-arm FE recv, arm BE recv, or
 * replay client_residual data.
 */
static void on_deferred_send_complete(void* userdata, int result)
{
    recv_context_t* recv_ctx = (recv_context_t*)userdata;
    keel_session_t* session = recv_ctx->session;
    keel_worker_t* worker = session->worker;
    keel_session_flow_t* sf = &recv_ctx->flow;

    recv_ctx->send_pending = false;

    /* Check if session was marked for deferred close */
    if (recv_ctx->closing) {
        sf->pending_send_len = 0;
        sf->pending_send_off = 0;
        if (!recv_ctx->be_pending && !recv_ctx->fe_pending) {
            complete_deferred_close(worker, recv_ctx);
        }
        return;
    }

    if (result <= 0) {
        /* Send error or peer closed */
        KEEL_LOG_WARN(KEEL_LOG_CAT_IO,
            "Worker %u: deferred send failed: %d", worker->id, result);
        sf->pending_send_len = 0;
        sf->pending_send_off = 0;
        stats_reset_query_deferred_send(recv_ctx);
        close_session(worker, session, recv_ctx);
        return;
    }

    /* Handle short send — re-queue remainder */
    size_t remaining = sf->pending_send_len - sf->pending_send_off;
    if ((size_t)result < remaining) {
        sf->pending_send_off += (size_t)result;
        if (queue_deferred_send(worker, recv_ctx) < 0) {
            sf->pending_send_len = 0;
            sf->pending_send_off = 0;
            close_session(worker, session, recv_ctx);
        }
        return;
    }

    /* Send complete — free buffer and follow resume action */
    keel_flow_result_t resume = sf->pending_send_resume;
    sf->pending_send_len = 0;
    sf->pending_send_off = 0;

    switch (resume) {
    case KEEL_FLOW_WAIT_STMT_REPLAY:  /* fallthrough: wait for ParseComplete(s) */
    case KEEL_FLOW_WAIT_BACKEND:
        /* Arm backend recv.  If there's un-processed BE data in
         * server_residual, it will be prepended on the next recv. */
        {
            backend_recv_context_t* be_ctx = &recv_ctx->be_ctx;
            be_ctx->client_ctx = recv_ctx;
            if (recv_ctx_ensure_be(recv_ctx) < 0) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL,
                    "W%u: be_buf alloc failed after deferred send", worker->id);
                close_session(worker, session, recv_ctx);
                return;
            }
                uint8_t wait_kind = (resume == KEEL_FLOW_WAIT_STMT_REPLAY)
                                          ? (recv_ctx->flow.stmt_replay_needs_discard
                                              ? WAIT_BACKEND_KIND_DISCARD
                                              : WAIT_BACKEND_KIND_REPLAY)
                                          : WAIT_BACKEND_KIND_QUERY;
                if (wait_kind == WAIT_BACKEND_KIND_QUERY)
                    stats_mark_query_deferred_send_end(recv_ctx);
                else
                    stats_reset_query_deferred_send(recv_ctx);
                stats_mark_wait_backend_begin(recv_ctx, wait_kind);
                if (wait_kind == WAIT_BACKEND_KIND_QUERY)
                    stats_mark_wait_backend_query_recv_armed(recv_ctx);
            recv_ctx->be_pending = true;
            int rc = keel_reactor_recv(worker->reactor, session->server_fd,
                                      recv_ctx->be_buf, recv_ctx->be_cap,
                                      0, be_ctx, on_backend_recv_complete);
            if (rc < 0) {
                recv_ctx->be_pending = false;
                KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                    "Worker %u: failed to queue BE recv after deferred send",
                    worker->id);
                close_session(worker, session, recv_ctx);
            }
        }
        return;

    case KEEL_FLOW_OK:
    default:
        stats_reset_query_deferred_send(recv_ctx);

#if KEEL_HAVE_SPLICE
        /* Special case: if the deferred send was from the splice bypass
         * recv callback (partial DataRow batch send), re-enter the splice
         * loop instead of rearming FE recv. */
        if (resume == KEEL_FLOW_SPLICE_BYPASS) {
            backend_recv_context_t* be_ctx2 = &recv_ctx->be_ctx;
            be_ctx2->client_ctx = recv_ctx;
            worker_splice_bypass_loop(worker, session, recv_ctx, be_ctx2);
            return;
        }
#endif

        /* Re-arm FE recv.  If there's saved FE data in client_residual
         * (e.g., coalesced MySQL commands or extended proto pipeline),
         * replay it before arming a new recv. */
        if (!keel_residual_empty(&session->client_residual)) {
            size_t rlen = keel_residual_len(&session->client_residual);
            if (rlen > 0) {
                size_t copy_len = rlen < recv_ctx->fe_cap
                                ? rlen : recv_ctx->fe_cap;
                size_t consumed = keel_residual_consume(
                    &session->client_residual,
                    recv_ctx->fe_buf, copy_len);
                on_client_recv_complete(recv_ctx, (int)consumed);
                return;
            }
        }
        recv_ctx->fe_pending = true;
        {
            int rc = keel_reactor_recv(worker->reactor, session->client_fd,
                                      recv_ctx->fe_buf, recv_ctx->fe_cap,
                                      0, recv_ctx, on_client_recv_complete);
            if (rc < 0) {
                recv_ctx->fe_pending = false;
                KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                    "Worker %u: failed to queue FE recv after deferred send",
                    worker->id);
                close_session(worker, session, recv_ctx);
            }
        }
        return;
    }
}

/* ============================================================================
 * Linked Send Completion Callback (io_uring IOSQE_IO_LINK)
 *
 * Called when the send SQE in a linked send→recv chain completes.
 * The recv SQE fires separately via on_backend_recv_complete (or
 * on_client_recv_complete).  If the send fails or is short, the linked
 * recv gets -ECANCELED automatically.
 * ============================================================================ */
/**
 * @brief Reactor callback: linked send SQE (io_uring IOSQE_IO_LINK) completed.
 *
 * Called when the send half of a linked send→recv pair finishes.  A failed
 * send automatically cancels the linked recv (-ECANCELED), which then
 * triggers the normal error path.
 */
static void on_linked_send_complete(void* userdata, int result)
{
    recv_context_t* recv_ctx = (recv_context_t*)userdata;
    keel_session_t* session = recv_ctx->session;
    keel_worker_t* worker   = session->worker;

    if (result <= 0) {
        /* Send failed — linked recv will get -ECANCELED and close_session. */
        KEEL_LOG_WARN(KEEL_LOG_CAT_IO,
            "W%u: linked send failed: result=%d fd=%d",
            worker->id, result, recv_ctx->flow.linked_send_fd);
        return;  /* recv callback handles cleanup */
    }

    if ((size_t)result < recv_ctx->flow.linked_send_len) {
        /* Short send — extremely unlikely for small payloads but handle
         * gracefully.  The linked recv already started so it'll get
         * whatever response the backend sends to a partial message,
         * which will be a protocol error → session closes. */
        KEEL_LOG_WARN(KEEL_LOG_CAT_IO,
            "W%u: linked send short: sent=%d of %zu fd=%d",
            worker->id, result, recv_ctx->flow.linked_send_len,
            recv_ctx->flow.linked_send_fd);
    }

    /* Stats: count bytes sent */
    worker->stats.bytes_sent += (uint64_t)result;
    recv_ctx->flow.linked_send_len = 0;  /* consumed */
}

/**
 * @brief Process one completed frontend recv.
 *
 * This callback is the main ingress path from client traffic into the worker.
 * Depending on session state it may:
 *
 * - advance an in-progress TLS handshake,
 * - decrypt userspace-TLS records,
 * - hand plaintext into the protocol flow engine,
 * - arm backend waits, pool waits, or deferred sends,
 * - or close the session on EOF/error.
 *
 * The callback therefore acts as the central dispatcher from raw frontend I/O
 * events into the higher-level flow state machine.
 *
 * @param userdata Recv context for the frontend session.
 * @param result Reactor recv completion result.
 * @return
 */
static void on_client_recv_complete(void* userdata, int result)
{
    recv_context_t* recv_ctx = (recv_context_t*)userdata;
    keel_session_t* session = recv_ctx->session;
    keel_worker_t* worker = session->worker;
    
    /* Clear the pending flag since the io_uring recv has completed */
    recv_ctx->fe_pending = false;
    
    /* Check if session was marked for deferred close (e.g. by zombie reaper) */
    if (recv_ctx->closing) {
        if (!recv_ctx->be_pending && !recv_ctx->send_pending) {
            complete_deferred_close(worker, recv_ctx);
        }
        return;
    }
    
    KEEL_DEBUG_LOG("W%u: on_client_recv_complete result=%d\n", worker->id, result);
    
    if (result <= 0) {
        if (result == 0) {
            KEEL_DEBUG_LOG("Worker %u: client closed connection %lu\n",
                        worker->id, (unsigned long)session->id);
        } else {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONN, "Worker %u: recv error on session %lu: %s",
                        worker->id, (unsigned long)session->id, strerror(-result));
        }
        close_session(worker, session, recv_ctx);
        return;
    }
    
    /* Update stats + reset zombie reaper */
    worker->stats.bytes_recv += (uint64_t)result;
    if (worker->stats_ctx)
        KEEL_STAT_ADD(worker->stats_ctx, bytes_recv, result);
    refresh_session_idle_timer(worker, recv_ctx, get_time_ns());

    /* -----------------------------------------------------------------------
     * TLS handshake in-progress: drive the handshake state machine
     * ----------------------------------------------------------------------- */
    if (recv_ctx->tls_hs_active) {
        /* Feed received bytes into TLS handshake input BIO */
        keel_tls_feed_handshake_data(recv_ctx->tls_ctx,
                                     recv_ctx->fe_buf, (size_t)result);

        /* Step the handshake — may produce output bytes to send */
        keel_tls_hs_result_t hs = keel_tls_handshake_step(recv_ctx->tls_ctx, NULL);

        if (hs == KEEL_TLS_HS_WANT_WRITE || hs == KEEL_TLS_HS_WANT_READ) {
            /* Drain any handshake data that must be sent to client */
            ssize_t n = keel_tls_get_handshake_data(recv_ctx->tls_ctx,
                                                    recv_ctx->tls_hs_buf,
                                                    KEEL_TLS_HS_BUF_SIZE);
            if (n > 0) {
                ssize_t s = keel_try_send_nb(session->client_fd,
                                             recv_ctx->tls_hs_buf, (size_t)n);
                if (s < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                        "Worker %u: TLS handshake send failed: %s",
                        worker->id, strerror(errno));
                    close_session(worker, session, recv_ctx);
                    return;
                }
                /* Handle partial send without blocking the reactor.  The
                 * helper already drained until EAGAIN; queueing handshake
                 * fragments is not implemented here, so fail closed instead
                 * of spinning on a client socket. */
                if ((size_t)s < (size_t)n) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                        "Worker %u: TLS handshake partial send (%zd of %zd); closing",
                        worker->id, s, n);
                    close_session(worker, session, recv_ctx);
                    return;
                }
            }
            /* Continue waiting for more handshake data from client */
            goto queue_fe_recv;
        }

        if (hs == KEEL_TLS_HS_COMPLETE) {
            recv_ctx->tls_hs_active = false;
            session->flags |= KEEL_SESSION_FLAG_SSL;

            /* Extract mTLS peer certificate info into session */
            if (recv_ctx->tls_ctx) {
                keel_tls_peer_info_t peer_info = {0};
                if (keel_tls_context_peer_info(recv_ctx->tls_ctx, &peer_info) == KEEL_OK
                    && peer_info.has_cert) {
                    session->tls_peer_has_cert = true;
                    strncpy(session->tls_peer_subject, peer_info.subject,
                            sizeof(session->tls_peer_subject) - 1);
                    session->tls_peer_subject[sizeof(session->tls_peer_subject) - 1] = '\0';
                    strncpy(session->tls_peer_issuer, peer_info.issuer,
                            sizeof(session->tls_peer_issuer) - 1);
                    session->tls_peer_issuer[sizeof(session->tls_peer_issuer) - 1] = '\0';
                    KEEL_DEBUG_LOG("W%u: mTLS peer cert subject=%s issuer=%s\n",
                        worker->id, session->tls_peer_subject, session->tls_peer_issuer);
                }
            }

            KEEL_DEBUG_LOG("W%u: TLS handshake complete on fd=%d\n",
                worker->id, session->client_fd);

            /* Try to activate kernel TLS for zero-copy splice */
            if (recv_ctx->tls_ctx) {
                keel_error_t ke = keel_tls_context_activate_ktls(
                    recv_ctx->tls_ctx, session->client_fd);
                if (ke == KEEL_OK) {
                    recv_ctx->tls_ktls_active = true;
                    KEEL_DEBUG_LOG("W%u: kTLS active on fd=%d\n",
                        worker->id, session->client_fd);
                } else {
                    KEEL_DEBUG_LOG("W%u: kTLS unavailable (err=%d), using userspace TLS\n",
                        worker->id, (int)ke);
                }
            }
            /* Re-arm recv — client will now send real StartupMessage */
            goto queue_fe_recv;
        }

        /* KEEL_TLS_HS_ERROR */
        KEEL_LOG_WARN(KEEL_LOG_CAT_TLS,
            "Worker %u: TLS handshake failed on session %lu",
            worker->id, (unsigned long)session->id);
        close_session(worker, session, recv_ctx);
        return;
    }

    /* -----------------------------------------------------------------------
     * Userspace TLS (no kTLS): decrypt received data before protocol pass
     * ----------------------------------------------------------------------- */
    if (recv_ctx->tls_ctx && !recv_ctx->tls_ktls_active) {
        keel_tls_feed_encrypted(recv_ctx->tls_ctx, recv_ctx->fe_buf, (size_t)result);
        ssize_t plain_len = keel_tls_read_decrypted(recv_ctx->tls_ctx,
                                                    recv_ctx->fe_buf,
                                                    recv_ctx->fe_cap);
        if (plain_len <= 0) {
            /* Encrypted but no complete record yet — wait for more */
            goto queue_fe_recv;
        }
        result = (int)plain_len;
    }

    /* Drive the protocol flow engine */
    keel_flow_result_t fr = keel_engine_flow_on_fe_data(
        &recv_ctx->flow, session, recv_ctx->fe_buf, (size_t)result);

    switch (fr) {
    case KEEL_FLOW_OK:
        /* Re-arm frontend recv */
        goto queue_fe_recv;

    case KEEL_FLOW_WAIT_STMT_REPLAY:  /* fallthrough: sent Parse replay, wait for ParseComplete(s) */
    case KEEL_FLOW_WAIT_BACKEND: {
        /* Queue backend recv — data was forwarded to backend by engine_flow */
        KEEL_DEBUG_LOG("W%u: FE WAIT_BACKEND server_fd=%d fr=%d\n",
                    worker->id, session->server_fd, (int)fr);
        if (session->server_fd < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "Worker %u: WAIT_BACKEND but no server_fd", worker->id);
            close_session(worker, session, recv_ctx);
            return;
        }
        backend_recv_context_t* be_ctx = &recv_ctx->be_ctx;
        be_ctx->client_ctx = recv_ctx;

        /* Lazy-alloc backend buffer on first borrow */
        if (recv_ctx_ensure_be(recv_ctx) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL,
                "W%u: be_buf alloc failed on WAIT_BACKEND", worker->id);
            close_session(worker, session, recv_ctx);
            return;
        }

          uint8_t wait_kind = (fr == KEEL_FLOW_WAIT_STMT_REPLAY)
                                     ? (recv_ctx->flow.stmt_replay_needs_discard
                                         ? WAIT_BACKEND_KIND_DISCARD
                                         : WAIT_BACKEND_KIND_REPLAY)
                                     : WAIT_BACKEND_KIND_QUERY;
          stats_mark_wait_backend_begin(recv_ctx, wait_kind);
        
        /* Arm io_uring recv for backend response.
         * Previous code did a speculative recv(MSG_DONTWAIT) here, but for
         * point_select and most queries the backend hasn't responded yet
         * so it always returned EAGAIN — wasting ~2μs on a useless syscall. */
        KEEL_DEBUG_LOG("W%u: FE WAIT_BACKEND arm recv fd=%d\n",
                    worker->id, session->server_fd);
        if (wait_kind == WAIT_BACKEND_KIND_QUERY)
            stats_mark_wait_backend_query_recv_armed(recv_ctx);
        recv_ctx->be_pending = true;
        int rc = keel_reactor_recv(worker->reactor, session->server_fd,
                                   recv_ctx->be_buf, recv_ctx->be_cap,
                                   0, be_ctx, on_backend_recv_complete);
        if (rc < 0) {
            recv_ctx->be_pending = false;
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: failed to queue BE recv", worker->id);
            close_session(worker, session, recv_ctx);
        }
        return;
    }

    case KEEL_FLOW_CLOSED:
        close_session(worker, session, recv_ctx);
        return;

    case KEEL_FLOW_WAIT_POOL:
        /* Session is queued waiting for a backend connection from pool.
         * Don't re-arm frontend recv — the pool callback will resume processing
         * when a backend becomes available. */
        stats_mark_wait_pool_begin(recv_ctx);
        KEEL_DEBUG_LOG("W%u: session %lu queued for pool\n", 
                    worker->id, (unsigned long)session->id);
        return;

    case KEEL_FLOW_WAIT_AUTH: {
        /* Async LDAP/PAM bind in flight.
         * Arm a reactor read on the eventfd so we are woken when the pool
         * thread completes the bind.  The eventfd fd is stored in
         * recv_ctx->flow.auth_notify_fd (set by engine_flow). */
        int nfd = recv_ctx->flow.auth_notify_fd;
        if (nfd < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "W%u: WAIT_AUTH but auth_notify_fd is -1", worker->id);
            close_session(worker, session, recv_ctx);
            return;
        }
        KEEL_DEBUG_LOG("W%u: WAIT_AUTH arm eventfd=%d session %lu\n",
                    worker->id, nfd, (unsigned long)session->id);
        /* Allocate a tiny buffer — eventfd always delivers exactly 8 bytes */
        if (!recv_ctx->auth_efd_buf) {
            recv_ctx->auth_efd_buf = keel_malloc(8);
            if (!recv_ctx->auth_efd_buf) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                    "W%u: WAIT_AUTH buf alloc failed", worker->id);
                close_session(worker, session, recv_ctx);
                return;
            }
        }
        int rc = keel_reactor_recv(worker->reactor, nfd,
                                   recv_ctx->auth_efd_buf, 8,
                                   0, recv_ctx, on_auth_notify_complete);
        if (rc < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "W%u: failed to arm auth eventfd recv", worker->id);
            close_session(worker, session, recv_ctx);
        }
        return;
    }

    case KEEL_FLOW_SEND_PENDING:
        /* Engine couldn't complete a send inline — flush via io_uring */
        if (queue_deferred_send(worker, recv_ctx) < 0)
            close_session(worker, session, recv_ctx);
        return;

    case KEEL_FLOW_LINKED_SEND: {
        /* io_uring linked send+recv: engine stored send payload instead of
         * calling send() inline.  Chain send(BE) → recv(BE) as linked SQEs
         * so the kernel does both without returning to userspace. */
        keel_session_flow_t* sf = &recv_ctx->flow;
        KEEL_DEBUG_LOG("W%u: LINKED_SEND fd=%d len=%zu resume=%d\n",
                    worker->id, sf->linked_send_fd, sf->linked_send_len,
                    (int)sf->linked_send_resume);
        if (session->server_fd < 0 || sf->linked_send_len == 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
                "W%u: LINKED_SEND but no server_fd or no payload", worker->id);
            close_session(worker, session, recv_ctx);
            return;
        }
        backend_recv_context_t* be_ctx = &recv_ctx->be_ctx;
        be_ctx->client_ctx = recv_ctx;

        /* Lazy-alloc backend buffer on first borrow */
        if (recv_ctx_ensure_be(recv_ctx) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_POOL,
                "W%u: be_buf alloc failed on LINKED_SEND", worker->id);
            close_session(worker, session, recv_ctx);
            return;
        }
        stats_mark_wait_backend_begin(recv_ctx, WAIT_BACKEND_KIND_QUERY);
        stats_mark_wait_backend_query_recv_armed(recv_ctx);
        recv_ctx->be_pending = true;
        int rc = keel_reactor_chain_send_recv(worker->reactor,
                sf->linked_send_fd, sf->linked_send_buf,
                sf->linked_send_len, MSG_NOSIGNAL,
                recv_ctx, on_linked_send_complete,
                session->server_fd, recv_ctx->be_buf,
                recv_ctx->be_cap, 0,
                be_ctx, on_backend_recv_complete);
        if (rc < 0) {
            recv_ctx->be_pending = false;
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "W%u: failed to queue linked send+recv", worker->id);
            /* Fallback: try inline send + separate recv */
            size_t send_len = sf->linked_send_len;
            ssize_t s = keel_try_send_nb(sf->linked_send_fd,
                                          sf->linked_send_buf,
                                          send_len);
            sf->linked_send_len = 0;
            if (s < 0 || (size_t)s < send_len) {
                close_session(worker, session, recv_ctx);
                return;
            }
            recv_ctx->be_pending = true;
            rc = keel_reactor_recv(worker->reactor, session->server_fd,
                                   recv_ctx->be_buf, recv_ctx->be_cap,
                                   0, be_ctx, on_backend_recv_complete);
            if (rc < 0) {
                recv_ctx->be_pending = false;
                close_session(worker, session, recv_ctx);
            }
        }
        return;
    }

    case KEEL_FLOW_ERROR:
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "Worker %u: flow error on session %lu",
                    worker->id, (unsigned long)session->id);
        close_session(worker, session, recv_ctx);
        return;

    case KEEL_FLOW_TLS_HANDSHAKE: {
        /* Engine sent 'S', now create TLS context and start handshake */
        keel_tls_context_t* tls_ctx = NULL;
        keel_error_t ke = keel_tls_context_create(
            &worker->tls_config, true /* is_server */, &tls_ctx);
        if (ke != KEEL_OK) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                "Worker %u: failed to create TLS context (err=%d)",
                worker->id, (int)ke);
            close_session(worker, session, recv_ctx);
            return;
        }
        recv_ctx->tls_ctx = tls_ctx;
        recv_ctx->tls_hs_active = true;
        recv_ctx->tls_ktls_active = false;

        /* Lazily allocate TLS handshake send buffer */
        if (recv_ctx_ensure_tls_hs(recv_ctx) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                "Worker %u: TLS hs buffer alloc failed", worker->id);
            close_session(worker, session, recv_ctx);
            return;
        }

        /* Run first handshake step — produces ServerHello bytes to send */
        keel_tls_hs_result_t hs = keel_tls_handshake_step(recv_ctx->tls_ctx, NULL);
        if (hs == KEEL_TLS_HS_WANT_WRITE || hs == KEEL_TLS_HS_WANT_READ) {
            ssize_t n = keel_tls_get_handshake_data(recv_ctx->tls_ctx,
                                                    recv_ctx->tls_hs_buf,
                                                    KEEL_TLS_HS_BUF_SIZE);
            if (n > 0) {
                ssize_t s = keel_try_send_nb(session->client_fd,
                                             recv_ctx->tls_hs_buf, (size_t)n);
                if (s < 0) {
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                        "Worker %u: first TLS handshake send failed: %s",
                        worker->id, strerror(errno));
                    close_session(worker, session, recv_ctx);
                    return;
                }
            }
        } else if (hs == KEEL_TLS_HS_ERROR) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                "Worker %u: TLS handshake error on first step", worker->id);
            close_session(worker, session, recv_ctx);
            return;
        }
        /* Wait for client to send ClientHello */
        goto queue_fe_recv;
    }

    default:
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN, "Worker %u: unexpected flow result %d on session %lu",
                    worker->id, fr, (unsigned long)session->id);
        close_session(worker, session, recv_ctx);
        return;
    }

queue_fe_recv:
    {
        recv_ctx->fe_pending = true;
        int rc = keel_reactor_recv(worker->reactor, session->client_fd,
                                  recv_ctx->fe_buf, recv_ctx->fe_cap,
                                  0, recv_ctx, on_client_recv_complete);
        KEEL_DEBUG_LOG("W%u: queue_fe_recv fd=%d rc=%d\n",
                    worker->id, session->client_fd, rc);
        if (rc < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: failed to requeue recv for session %lu",
                        worker->id, (unsigned long)session->id);
            close_session(worker, session, recv_ctx);
        }
    }
}

/* ==========================================================================
 * Backend Response Callback (Async)
 * ============================================================================ */
/**
 * @brief Finish tearing down a session after deferred close conditions are met.
 *
 * Deferred close is used when a reactor operation still references the session.
 * Final cleanup therefore happens only once it is safe to destroy flow state,
 * TLS state, residual buffers, and return both the session slot and recv
 * context to their pools.
 *
 * @param worker Owning worker.
 * @param client_ctx Recv context for the session being destroyed.
 * @return
 */
static void complete_deferred_close(keel_worker_t* worker, recv_context_t* client_ctx)
{
    keel_session_t* session = client_ctx->session;
    
    KEEL_DEBUG_LOG("W%u: completing deferred close for session %lu\n",
                 worker->id, (unsigned long)session->id);
    
    /* Now we can safely destroy flow and free everything */
    keel_session_flow_destroy(&client_ctx->flow);

    /* Destroy TLS context if any */
    if (client_ctx->tls_ctx) {
        keel_tls_context_destroy(client_ctx->tls_ctx);
        client_ctx->tls_ctx = NULL;
    }
    
    session->plugin_state = NULL;
    session->fast_forward_mode = 0;
    keel_residual_clear(&session->client_residual);
    keel_residual_clear(&session->server_residual);

    session->state_profile = NULL;
    session->pin_reason = 0;
    session->hard_pinned = false;
    session->state_hash = 0;
    session->in_transaction = false;

    /* Record session duration latency */
    if (worker->stats_ctx && session->created_at) {
        uint64_t dur = (uint64_t)keel_stats_now_ns() - session->created_at;
        KEEL_STAT_LATENCY(worker->stats_ctx, session_duration_ns, dur);
    }

    /* End trace span (if sampled) and submit for export */
    {
        keel_tracer_t *tracer = keel_engine_get_tracer(worker->engine);
        KEEL_TRACE_SESSION_END(tracer, session);
    }

    keel_log_clear_trace_context();

    /* Audit: DISCONNECT — emit once per authenticated session at teardown. */
    if (worker->audit_log && (session->flags & KEEL_SESSION_FLAG_AUTHENTICATED)) {
        keel_audit_emit_connect(
            (keel_audit_log_t*)worker->audit_log,
            KEEL_AUDIT_DISCONNECT,
            NULL, 0,
            session->username[0] ? session->username : NULL,
            session->database[0] ? session->database : NULL);
    }

    keel_session_slab_free(&worker->sessions, session);
    worker->stats.sessions_closed++;
    if (worker->stats_ctx) {
        KEEL_STAT_INC(worker->stats_ctx, sessions_closed);
        KEEL_STAT_GAUGE_DEC(worker->stats_ctx, sessions_active);
    }
    recv_ctx_free_bufs(client_ctx);
    keel_pool_free(worker->recv_ctx_pool, client_ctx);
}

/* ============================================================================
 * Zero-Copy Splice Bypass (fast_network_path)
 * ============================================================================
 *
 * When the engine detects a result set being streamed (DataRow frames) and
 * fast_network_path is enabled, it returns KEEL_FLOW_SPLICE_BYPASS.  The
 * worker then enters a tight synchronous loop:
 *
 *   1. keel_peek(server_fd, hdr, 5) — MSG_PEEK the next PG message header
 *   2. If type == 'D' (DataRow): splice(server_fd → pipe → client_fd) for
 *      the exact message length — zero userspace copy
 *   3. If type != 'D': exit splice mode, rearm normal recv so the terminal
 *      frame (ReadyForQuery, ErrorResponse, etc.) goes through the engine
 *   4. If WOULDBLOCK: rearm recv; when data arrives, re-enter the loop
 *
 * This path handles the 99% case (DataRow frames in a result set) entirely
 * in kernel space via splice(2), touching userspace only for the 5-byte peek.
 */
#if KEEL_HAVE_SPLICE
#include "keel/reactor/io_splice.h"

/* Forward — splice-mode recv callback re-enters the bypass loop */
static void on_splice_bypass_recv(void* userdata, int result);

/**
 * @brief Synchronous peek+splice loop for zero-copy BE→FE forwarding.
 *
 * Called when the engine signals KEEL_FLOW_SPLICE_BYPASS.  Peeks at backend
 * message headers and splices DataRow frames directly to the client socket.
 * Returns to normal recv+engine_flow processing when a non-DataRow frame
 * is detected or the socket would block.
 */
static void worker_splice_bypass_loop(
    keel_worker_t*           worker,
    keel_session_t*          session,
    recv_context_t*          client_ctx,
    backend_recv_context_t*  be_ctx)
{
    keel_pipe_t* pipe = session->s2c_pipe;
    if (!pipe || pipe->read_fd < 0 || pipe->write_fd < 0) {
        /* No pipe available — fall back to normal recv path */
        goto fallback_recv;
    }

    keel_splice_pipe_t sp = {
        .pipe_fds = { pipe->read_fd, pipe->write_fd },
        .capacity = pipe->capacity,
        .pending  = pipe->pending,
        .valid    = true,
    };

    for (;;) {
        /* 1. Peek at the next PG message header (1 type + 4 length) */
        uint8_t hdr[5];
        keel_peek_result_t peek = keel_peek(session->server_fd, hdr, 5);

        if (peek.status == KEEL_PEEK_WOULDBLOCK) {
            /* No data ready — rearm recv with splice-bypass callback.
             * When data arrives we re-enter this loop. */
            goto splice_wait;
        }
        if (peek.status == KEEL_PEEK_CLOSED) {
            /* Backend closed — let normal path handle cleanup */
            goto fallback_recv;
        }
        if (peek.status != KEEL_PEEK_OK || peek.len < 5) {
            /* Incomplete header or error — fall back to normal recv.
             * The engine will handle partial framing. */
            goto fallback_recv;
        }

        /* 2. Parse wire header: 1 byte type, 4 bytes length (big-endian,
         *    includes self but not the type byte).  Total frame = 1 + msg_len. */
        uint32_t msg_len = ((uint32_t)hdr[1] << 24) |
                           ((uint32_t)hdr[2] << 16) |
                           ((uint32_t)hdr[3] << 8)  |
                           (uint32_t)hdr[4];
        size_t frame_len = 1 + (size_t)msg_len;

        /* 3. Delegate the "is this a spliceable data frame?" decision to the
         *    protocol vtable's is_data_frame classifier.  This keeps the core
         *    database-agnostic: Postgres DataRow ('D'), MySQL text-protocol
         *    result rows, and any future protocol are handled identically.
         *
         *    If the vtable entry is absent (legacy or test protocol), fall back
         *    to a conservative false so the frame goes through the full engine
         *    path rather than being silently dropped or mis-spliced. */
        const keel_proto_flow_vtable_t* flow_vtab = client_ctx->flow.flow;
        if (!flow_vtab ||
            !flow_vtab->is_data_frame ||
            !flow_vtab->is_data_frame(client_ctx->flow.ctx, hdr, 5)) {
            /* Exit splice bypass — clear fast_forward so engine re-evaluates */
            session->fast_forward_mode = 0;
            KEEL_LOG_TRACE(KEEL_LOG_CAT_IO,
                "[SPLICE-EXIT] type=0x%02x flen=%zu — not a data frame, switching to engine path",
                (unsigned)hdr[0], frame_len);
            goto fallback_recv;
        }

        /* 4. Splice the entire DataRow frame: BE socket → pipe → FE socket.
         *    keel_splice_transfer() does two splice() calls internally. */
        keel_transfer_result_t res = keel_splice_transfer(
            session->server_fd, session->client_fd, &sp,
            frame_len, KEEL_TRANSFER_NONBLOCK | KEEL_TRANSFER_MOVE);

        if (res.error != KEEL_OK || res.bytes == 0) {
            /* splice failed (maybe pipe full, or EAGAIN on partial) —
             * fall back to normal recv so the engine can handle it safely */
            KEEL_LOG_TRACE(KEEL_LOG_CAT_IO,
                "[SPLICE-FAIL] err=%d bytes=%zu — falling back to recv",
                (int)res.error, res.bytes);
            pipe->pending = sp.pending;
            goto fallback_recv;
        }

        /* Account for transferred bytes */
        worker->stats.bytes_spliced += res.bytes;
        worker->stats.bytes_sent   += res.bytes;
        if (worker->stats_ctx) {
            KEEL_STAT_ADD(worker->stats_ctx, bytes_spliced, res.bytes);
            KEEL_STAT_ADD(worker->stats_ctx, bytes_sent,    res.bytes);
        }

        if (res.bytes < frame_len) {
            /* Partial splice — unlikely but possible.  The remaining bytes
             * of this frame are still in the socket buffer.  Fall back to
             * normal recv so the engine can re-assemble the frame properly.
             *
             * NOTE: We already consumed res.bytes from the socket and sent
             * them to the client.  The engine's framing parser will see the
             * partial frame via server_residual.  To avoid double-sending,
             * we must store how many bytes were already sent.  The simplest
             * safe path: break out and let the engine re-read. */
            session->fast_forward_mode = 0;
            pipe->pending = sp.pending;
            goto fallback_recv;
        }

        /* Full frame spliced — loop and try the next message header */
        KEEL_LOG_TRACE(KEEL_LOG_CAT_IO,
            "[SPLICE-OK] DataRow %zu bytes spliced (zero-copy)", frame_len);
    }
    /* UNREACHABLE */

splice_wait:
    /* Backend socket empty — rearm recv with splice-bypass callback.
     * When the kernel delivers new data, on_splice_bypass_recv re-enters
     * this loop.  The recv buffer is used only as a trigger; the actual
     * data handling goes through peek+splice. */
    pipe->pending = sp.pending;
    client_ctx->be_pending = true;
    {
        int rc = keel_reactor_recv(worker->reactor, session->server_fd,
                                  client_ctx->be_buf, client_ctx->be_cap,
                                  0, be_ctx, on_splice_bypass_recv);
        if (rc < 0) {
            client_ctx->be_pending = false;
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "Worker %u: failed to rearm splice bypass recv", worker->id);
            close_session(worker, session, client_ctx);
        }
    }
    return;

fallback_recv:
    /* Exit splice bypass — rearm normal backend recv so the engine
     * processes the terminal frame through the full protocol path. */
    pipe->pending = sp.pending;
    client_ctx->be_pending = true;
    {
        int rc = keel_reactor_recv(worker->reactor, session->server_fd,
                                  client_ctx->be_buf, client_ctx->be_cap,
                                  0, be_ctx, on_backend_recv_complete);
        if (rc < 0) {
            client_ctx->be_pending = false;
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "Worker %u: failed to rearm backend recv after splice exit",
                worker->id);
            close_session(worker, session, client_ctx);
        }
    }
    return;
}

/**
 * @brief Splice-bypass recv callback — re-enters the peek+splice loop.
 *
 * When the splice bypass loop drains the socket and falls back to waiting,
 * io_uring delivers the next recv completion here.  Instead of calling the
 * engine flow, we first try to continue splicing.  If the received data
 * starts with a non-DataRow frame, we route it through the normal engine.
 */
static void on_splice_bypass_recv(void* userdata, int result)
{
    backend_recv_context_t* be_ctx = (backend_recv_context_t*)userdata;
    recv_context_t* client_ctx = be_ctx->client_ctx;
    keel_session_t* session = client_ctx->session;
    keel_worker_t* worker = session->worker;

    client_ctx->be_pending = false;

    /* Check deferred close */
    if (client_ctx->closing) {
        if (!client_ctx->fe_pending && !client_ctx->send_pending)
            complete_deferred_close(worker, client_ctx);
        return;
    }

    refresh_session_idle_timer(worker, client_ctx, get_time_ns());

    if (result <= 0) {
        /* Error or EOF — route through normal path for proper cleanup */
        on_backend_recv_complete(userdata, result);
        return;
    }

    if (worker->stats_ctx)
        KEEL_STAT_ADD(worker->stats_ctx, bytes_backend_recv, result);

    /* The recv consumed data into the buffer.  We have two choices:
     *
     * (a) If the first byte is 'D' (DataRow), send this buffer to the
     *     client directly (one userspace copy for this batch), then
     *     re-enter the peek+splice loop for subsequent frames.
     *
     * (b) If the first byte is NOT 'D', a terminal frame arrived.
     *     Route through the full engine path.
     *
     * Option (a) avoids the overhead of re-parsing through the engine
     * and keeps us in splice mode for the next data. */
    uint8_t* data = client_ctx->be_buf;
    size_t   len  = (size_t)result;

    /* Prepend any server residual (split messages across recv boundaries) */
    uint8_t* combined_buf = NULL;
    if (!keel_residual_empty(&session->server_residual)) {
        size_t rlen = keel_residual_len(&session->server_residual);
        combined_buf = keel_malloc(rlen + len);
        if (!combined_buf) {
            close_session(worker, session, client_ctx);
            return;
        }
        keel_residual_consume(&session->server_residual, combined_buf, rlen);
        memcpy(combined_buf + rlen, data, len);
        data = combined_buf;
        len  = rlen + len;
    }

    if (len >= 1 && data[0] == 'D') {
        /* DataRow batch — send directly to client, stay in splice mode.
         * This is one userspace copy (the recv already landed in buffer),
         * but we'll re-enter splice for all subsequent data. */
        ssize_t sent = keel_try_send_nb(session->client_fd, data, len);
        keel_free(combined_buf);
        if (sent < 0) {
            close_session(worker, session, client_ctx);
            return;
        }
        worker->stats.bytes_sent += (uint64_t)sent;
        if (worker->stats_ctx)
            KEEL_STAT_ADD(worker->stats_ctx, bytes_sent, sent);

        if ((size_t)sent < len) {
            /* Partial send — save remainder and defer.
             * Use the flow's pending_send mechanism so the deferred-send
             * completion handler knows to re-enter splice bypass. */
            keel_session_flow_t* sf = &client_ctx->flow;
            size_t remain = len - (size_t)sent;
            /* defer_send in engine_flow.c heap-copies the buffer, but here
             * we already have data in client_ctx->be_buf or combined_buf which
             * may go out of scope.  Heap-copy the unsent remainder. */
            if (sf->pending_send_cap < remain) {
                keel_free(sf->pending_send_buf);
                sf->pending_send_buf = keel_malloc(remain);
                sf->pending_send_cap = remain;
            }
            if (!sf->pending_send_buf) {
                close_session(worker, session, client_ctx);
                return;
            }
            memcpy(sf->pending_send_buf, data + sent, remain);
            sf->pending_send_len    = remain;
            sf->pending_send_off    = 0;
            sf->pending_send_fd     = session->client_fd;
            sf->pending_send_resume = KEEL_FLOW_SPLICE_BYPASS;
            if (queue_deferred_send(worker, client_ctx) < 0)
                close_session(worker, session, client_ctx);
            return;
        }

        /* Re-enter splice bypass loop */
        worker_splice_bypass_loop(worker, session, client_ctx, be_ctx);
    } else {
        /* Non-DataRow frame — exit splice mode, process via engine.
         * This handles terminal frames (ReadyForQuery, ErrorResponse, etc.) */
        session->fast_forward_mode = 0;
        keel_flow_result_t fr = keel_engine_flow_on_be_data(
            &client_ctx->flow, session, data, len);
        keel_free(combined_buf);

        /* Dispatch the flow result — same logic as on_backend_recv_complete */
        switch (fr) {
        case KEEL_FLOW_OK:
            /* Query complete — replay client residual or rearm FE recv */
            if (!keel_residual_empty(&session->client_residual)) {
                size_t rlen2 = keel_residual_len(&session->client_residual);
                if (rlen2 > 0) {
                    size_t copy_len = rlen2 < client_ctx->be_cap
                                      ? rlen2 : client_ctx->be_cap;
                    keel_residual_consume(&session->client_residual,
                                         client_ctx->be_buf, copy_len);
                    on_client_recv_complete(client_ctx, (int)copy_len);
                    return;
                }
            }
            client_ctx->fe_pending = true;
            {
                int rc = keel_reactor_recv(worker->reactor, session->client_fd,
                                          client_ctx->be_buf, client_ctx->be_cap,
                                          0, client_ctx, on_client_recv_complete);
                if (rc < 0) {
                    client_ctx->fe_pending = false;
                    close_session(worker, session, client_ctx);
                }
            }
            return;

        case KEEL_FLOW_SPLICE_BYPASS:
            /* Engine re-entered splice mode (unusual but possible) */
            worker_splice_bypass_loop(worker, session, client_ctx, be_ctx);
            return;

        case KEEL_FLOW_WAIT_BACKEND:
        case KEEL_FLOW_WAIT_STMT_REPLAY:
        case KEEL_FLOW_WAIT_COMMIT_CHECK:
            client_ctx->be_pending = true;
            {
                int rc = keel_reactor_recv(worker->reactor, session->server_fd,
                                          client_ctx->be_buf, client_ctx->be_cap,
                                          0, be_ctx, on_backend_recv_complete);
                if (rc < 0) {
                    client_ctx->be_pending = false;
                    close_session(worker, session, client_ctx);
                }
            }
            return;

        case KEEL_FLOW_SEND_PENDING:
            if (queue_deferred_send(worker, client_ctx) < 0)
                close_session(worker, session, client_ctx);
            return;

        default:
            close_session(worker, session, client_ctx);
            return;
        }
    }
}
#endif /* KEEL_HAVE_SPLICE */

/**
 * @brief Process a completed async auth eventfd read.
 *
 * Called by the reactor when the LDAP/PAM pool thread writes to the
 * session's auth_notify_fd.  Invokes keel_engine_flow_resume_auth() which
 * drains the eventfd, closes it, then re-invokes the protocol handler with
 * an empty payload to pick up the auth result.  The resulting flow is then
 * dispatched normally.
 *
 * @param userdata  recv_context_t* for the waiting session.
 * @param result    Bytes read (should be 8) or ≤0 on error.
 */
static void on_auth_notify_complete(void* userdata, int result)
{
    recv_context_t* recv_ctx = (recv_context_t*)userdata;
    keel_session_t* session  = recv_ctx->session;
    keel_worker_t*  worker   = session->worker;

    KEEL_DEBUG_LOG("W%u: on_auth_notify_complete result=%d session %lu\n",
                worker->id, result, (unsigned long)session->id);

    if (result <= 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
            "W%u: auth eventfd read failed (result=%d), closing session",
            worker->id, result);
        close_session(worker, session, recv_ctx);
        return;
    }

    /* keel_engine_flow_resume_auth reads the eventfd (already drained by
     * reactor into auth_efd_buf), closes the fd, and re-invokes on_fe_msg. */
    keel_flow_result_t fr = keel_engine_flow_resume_auth(
        &recv_ctx->flow, session);

    /* Dispatch the resulting flow exactly as on_client_recv_complete would */
    switch (fr) {
    case KEEL_FLOW_OK:
        /* Auth complete, or another round needed — re-arm frontend recv */
        {
            int rc = keel_reactor_recv(worker->reactor, session->client_fd,
                                       recv_ctx->fe_buf, recv_ctx->fe_cap,
                                       0, recv_ctx, on_client_recv_complete);
            if (rc < 0) close_session(worker, session, recv_ctx);
        }
        return;

    case KEEL_FLOW_CLOSED:
        close_session(worker, session, recv_ctx);
        return;

    case KEEL_FLOW_SEND_PENDING:
        if (queue_deferred_send(worker, recv_ctx) < 0)
            close_session(worker, session, recv_ctx);
        return;

    case KEEL_FLOW_WAIT_BACKEND:
    case KEEL_FLOW_WAIT_STMT_REPLAY: {
        /* Should not normally happen from auth path, but handle safely */
        if (session->server_fd < 0) {
            close_session(worker, session, recv_ctx);
            return;
        }
        backend_recv_context_t* be_ctx = &recv_ctx->be_ctx;
        be_ctx->client_ctx = recv_ctx;
        if (recv_ctx_ensure_be(recv_ctx) < 0) {
            close_session(worker, session, recv_ctx);
            return;
        }
        recv_ctx->be_pending = true;
        int rc = keel_reactor_recv(worker->reactor, session->server_fd,
                                   recv_ctx->be_buf, recv_ctx->be_cap,
                                   0, be_ctx, on_backend_recv_complete);
        if (rc < 0) {
            recv_ctx->be_pending = false;
            close_session(worker, session, recv_ctx);
        }
        return;
    }

    default:
        KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
            "W%u: unexpected flow result %d after auth resume", worker->id, (int)fr);
        close_session(worker, session, recv_ctx);
        return;
    }
}

/**
 * @brief Process one completed backend recv.
 *
 * This callback is the worker's main ingress path for server responses. It
 * closes out backend-wait instrumentation, repairs partial-message boundaries
 * with `server_residual`, feeds the combined bytes into the flow engine, and
 * then dispatches the resulting action: resume FE reads, continue waiting on
 * the backend, enter splice bypass, flush deferred sends, or close the session.
 *
 * It also contains the worker's backend-failure policy: a dead backend marks
 * the pool slot closed immediately, and commit-doubt handling gets a chance to
 * resolve uncertain transaction outcome before the session is finally closed.
 *
 * @param userdata Backend recv context pointing back to the frontend context.
 * @param result Reactor recv completion result.
 * @return
 */
static void on_backend_recv_complete(void* userdata, int result)
{
    backend_recv_context_t* be_ctx = (backend_recv_context_t*)userdata;
    recv_context_t* client_ctx = be_ctx->client_ctx;
    keel_session_t* session = client_ctx->session;
    keel_worker_t* worker = session->worker;
    uint64_t callback_entry_ns = get_time_ns();
    uint64_t cqe_wakeup_ns = keel_reactor_current_completion_wakeup_ns();
    uint64_t cqe_seen_ns = keel_reactor_current_completion_seen_ns();
    uint32_t cqe_batch_size = keel_reactor_current_completion_batch_size();
    uint32_t cqe_batch_index = keel_reactor_current_completion_batch_index();
    
    /* Clear the pending flag since we're now processing */
    client_ctx->be_pending = false;
    
    /* Check if session was marked for deferred close */
    if (client_ctx->closing) {
        if (!client_ctx->fe_pending && !client_ctx->send_pending) {
            complete_deferred_close(worker, client_ctx);
        }
        return;
    }
    
    /* Reset the zombie reaper — backend activity means session is alive */
    refresh_session_idle_timer(worker, client_ctx, get_time_ns());
    
    KEEL_DEBUG_LOG("W%u: on_backend_recv_complete fd=%d result=%d closing=%d\n",
                 worker->id, session->server_fd, result, (int)client_ctx->closing);
    
    if (result <= 0) {
        client_ctx->flow.wait_backend_start_ns = 0;
        client_ctx->flow.wait_backend_kind = WAIT_BACKEND_KIND_NONE;
        client_ctx->flow.wait_backend_query_recv_armed_ns = 0;
        client_ctx->flow.wait_backend_query_send_start_ns = 0;
        /* Differentiate backend errors for stats and logging:
         *   result == 0       → EOF, backend closed (fatal)
         *   result < 0 EAGAIN → transient (rare with io_uring, still fatal here)
         *   result < 0 other  → fatal I/O error (ECONNRESET, EPIPE, etc.)
         */
        const char* be_host = "?";
        uint16_t    be_port = 0;
        if (session->backend_conn && session->backend_conn->pool) {
            be_host = session->backend_conn->pool->config.host;
            be_port = session->backend_conn->pool->config.port;
        }
        if (result == 0) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONN, "Worker %u: backend closed connection (EOF) [%s:%u]",
                         worker->id, be_host, be_port);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, backend_error_fatal);
        } else if (result == -EAGAIN || result == -EWOULDBLOCK) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONN, "Worker %u: backend recv EAGAIN (transient) [%s:%u]",
                         worker->id, be_host, be_port);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, backend_error_transient);
        } else {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CONN, "Worker %u: backend recv error: %d (%s) [%s:%u]",
                         worker->id, result, strerror(-result), be_host, be_port);
            if (worker->stats_ctx)
                KEEL_STAT_INC(worker->stats_ctx, backend_error_fatal);
        }
        /* Mark pooled connection as broken so it gets discarded */
        if (session->backend_conn) {
            backend_conn_t* be_conn = session->backend_conn;
            atomic_store(&be_conn->state, BACKEND_CONN_CLOSED);
            if (be_conn->fd >= 0) {
                close(be_conn->fd);
                be_conn->fd = -1;
            }
            be_conn->pinned_session = NULL;
            be_conn->in_transaction = false;
            be_conn->hard_pinned = false;
            if (be_conn->pool) {
                be_conn->pool->active_count--;
            }
            session->backend_conn = NULL;
            session->server_fd = -1;
        } else if (session->server_fd >= 0) {
            close(session->server_fd);
            session->server_fd = -1;
        }

        /* Replication tracking: if a COMMIT was in-flight when the backend
         * died, try to resolve the outcome via txid_status() on a new conn.
         * keel_engine_flow_handle_commit_doubt() returns WAIT_COMMIT_CHECK
         * if it successfully borrows a check connection, or FLOW_CLOSED/ERROR
         * if the pool is unavailable (in which case it sends an error to the
         * client and we fall through to close_session). */
        keel_session_flow_t* sf = &client_ctx->flow;
        if (sf->txn_tracking && sf->commit_in_flight) {
            keel_flow_result_t cdr =
                keel_engine_flow_handle_commit_doubt(sf, session, worker);
            if (cdr == KEEL_FLOW_WAIT_COMMIT_CHECK) {
                /* Arm recv on the borrowed check connection */
                client_ctx->be_pending = true;
                backend_recv_context_t* be_ctx2 = &client_ctx->be_ctx;
                int rc = keel_reactor_recv(worker->reactor, session->server_fd,
                                          client_ctx->be_buf, client_ctx->be_cap,
                                          0, be_ctx2, on_backend_recv_complete);
                if (rc < 0) {
                    client_ctx->be_pending = false;
                    KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                        "Worker %u: failed to arm commit-doubt check recv",
                        worker->id);
                    close_session(worker, session, client_ctx);
                }
                return;
            }
            /* CLOSED or ERROR: error already sent to client, just close */
            close_session(worker, session, client_ctx);
            return;
        }

        /* Send an ErrorResponse to the client so it sees a proper database
         * error (SQLSTATE 08006) instead of "server closed the connection
         * unexpectedly" (bare EOF). */
        {
            const keel_proto_flow_vtable_t* flow = client_ctx->flow.flow;
            if (flow && flow->generate_error) {
                uint8_t sendbuf[512];
                size_t sendlen = 0;
                uint8_t errbuf[256];
                ssize_t el = flow->generate_error(
                        client_ctx->flow.ctx, "08006",
                        "connection to server was lost",
                        errbuf, sizeof(errbuf));
                if (el > 0) {
                    memcpy(sendbuf, errbuf, (size_t)el);
                    sendlen += (size_t)el;
                }
                if (flow->name && strcmp(flow->name, "postgres") == 0) {
                    uint8_t z[] = {'Z', 0, 0, 0, 5, 'I'};
                    memcpy(sendbuf + sendlen, z, sizeof(z));
                    sendlen += sizeof(z);
                }
                if (sendlen > 0)
                    keel_try_send_nb(session->client_fd, sendbuf, sendlen);
            }
        }
        close_session(worker, session, client_ctx);
        return;
    }

    uint8_t wait_kind = stats_mark_wait_backend_first_byte(client_ctx,
                                                           cqe_wakeup_ns,
                                                           cqe_seen_ns,
                                                           cqe_batch_size,
                                                           cqe_batch_index,
                                                           callback_entry_ns);
    uint64_t query_framing_start_ns = 0;
    if (wait_kind == WAIT_BACKEND_KIND_QUERY)
        query_framing_start_ns = get_time_ns();
    
    worker->stats.bytes_sent += (uint64_t)result;
    if (worker->stats_ctx)
        KEEL_STAT_ADD(worker->stats_ctx, bytes_backend_recv, result);
    
    /* Prepend any saved server residual from a previous partial message.
     * When a backend recv splits mid-message, on_be_data saves the trailing
     * bytes to server_residual.  We must reassemble them with the new recv
     * data so the framing parser sees a contiguous byte stream. */
    uint8_t* be_data = client_ctx->be_buf;
    size_t   be_len  = (size_t)result;
    uint8_t* combined_buf = NULL;

    if (!keel_residual_empty(&session->server_residual)) {
        size_t rlen = keel_residual_len(&session->server_residual);
        combined_buf = keel_malloc(rlen + be_len);
        if (!combined_buf) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "Worker %u: alloc failed for server residual prepend (%zu bytes)",
                worker->id, rlen + be_len);
            close_session(worker, session, client_ctx);
            return;
        }
        keel_residual_consume(&session->server_residual, combined_buf, rlen);
        memcpy(combined_buf + rlen, be_data, be_len);
        be_data = combined_buf;
        be_len  = rlen + be_len;
    }

    /* Drive the protocol flow engine for backend data */
    keel_flow_result_t fr = keel_engine_flow_on_be_data(
        &client_ctx->flow, session, be_data, be_len);

    if (query_framing_start_ns != 0 && worker->stats_ctx) {
        uint64_t now = get_time_ns();
        if (now > query_framing_start_ns) {
            KEEL_STAT_ADD(worker->stats_ctx,
                          flow_wait_backend_query_framing_ns_total,
                          now - query_framing_start_ns);
        }
    }

    keel_free(combined_buf);  /* NULL-safe */

    switch (fr) {
    case KEEL_FLOW_OK:
        /* Query complete — go back to listening for client */
        goto queue_fe_recv;

    case KEEL_FLOW_LINKED_FE_RESPONSE: {
        /* Engine deferred the FE response send so we can chain
         * send(client_fd)+recv(client_fd) via linked io_uring SQEs.
         * This eliminates the inline send() syscall. */
        keel_session_flow_t* sf = &client_ctx->flow;
        if (sf->linked_send_len == 0 || sf->linked_send_fd < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "W%u: LINKED_FE_RESPONSE but no payload", worker->id);
            close_session(worker, session, client_ctx);
            return;
        }
        const void* save_buf = sf->linked_send_buf;
        size_t save_len = sf->linked_send_len;
        int save_fd = sf->linked_send_fd;
        client_ctx->fe_pending = true;
        int rc = keel_reactor_chain_send_recv(worker->reactor,
                save_fd, save_buf, save_len, MSG_NOSIGNAL,
                client_ctx, on_linked_send_complete,
                session->client_fd, client_ctx->be_buf,
                client_ctx->be_cap, 0,
                client_ctx, on_client_recv_complete);
        sf->linked_send_len = 0;
        if (rc < 0) {
            client_ctx->fe_pending = false;
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO,
                "W%u: linked FE send+recv failed, falling back to inline",
                worker->id);
            /* Fallback: inline send + separate recv */
            ssize_t s = keel_try_send_nb(save_fd, save_buf, save_len);
            if (s < 0) {
                close_session(worker, session, client_ctx);
                return;
            }
            goto queue_fe_recv;
        }
        return;
    }

    case KEEL_FLOW_WAIT_STMT_REPLAY:  /* fallthrough: waiting for ParseComplete(s) from replay */
    case KEEL_FLOW_WAIT_COMMIT_CHECK: /* fallthrough: waiting for txid_status() response */
    case KEEL_FLOW_WAIT_BACKEND:
        /* More data expected from backend */
        {
            client_ctx->be_pending = true;  /* Mark backend recv as pending */
            int rc = keel_reactor_recv(worker->reactor, session->server_fd,
                                      client_ctx->be_buf, client_ctx->be_cap,
                                      0, be_ctx, on_backend_recv_complete);
            if (rc < 0) {
                client_ctx->be_pending = false;
                KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: failed to queue backend recv", worker->id);
                close_session(worker, session, client_ctx);
            }
        }
        return;

    case KEEL_FLOW_SEND_PENDING:
        /* Engine couldn't complete a send inline — flush via io_uring */
        if (queue_deferred_send(worker, client_ctx) < 0)
            close_session(worker, session, client_ctx);
        return;

#if KEEL_HAVE_SPLICE
    case KEEL_FLOW_SPLICE_BYPASS:
        /* Zero-copy path: peek at BE headers, splice DataRow frames directly
         * to the client socket through kernel pipes.  worker_splice_bypass_loop
         * handles the synchronous loop and rearms recv when needed. */
        KEEL_LOG_TRACE(KEEL_LOG_CAT_IO,
            "Worker %u: entering splice bypass for session %lu",
            worker->id, (unsigned long)session->id);
        worker_splice_bypass_loop(worker, session, client_ctx, be_ctx);
        return;
#endif

    case KEEL_FLOW_CLOSED:
    case KEEL_FLOW_ERROR:
    default:
        close_session(worker, session, client_ctx);
        return;
    }

queue_fe_recv:
    {
        /* FIX: Replay any residual FE data saved from coalesced TCP segments.
         * When a MySQL client sends multiple commands in one TCP segment
         * (e.g., SET NAMES + DROP TABLE + CREATE TABLE), on_fe_data processes
         * the first query and saves the rest in client_residual.  After the
         * backend responds, we replay the saved data here before re-arming
         * the FE recv, ensuring no commands are lost. */
        if (!keel_residual_empty(&session->client_residual)) {
            size_t rlen = keel_residual_len(&session->client_residual);
            if (rlen > 0) {
                size_t copy_len = rlen < client_ctx->fe_cap ? rlen : client_ctx->fe_cap;
                size_t consumed = keel_residual_consume(&session->client_residual,
                                                       client_ctx->fe_buf, copy_len);
                /* Process the residual data as if we just received it */
                on_client_recv_complete(client_ctx, (int)consumed);
                return;
            }
        }
        client_ctx->fe_pending = true;
        int rc = keel_reactor_recv(worker->reactor, session->client_fd,
                                  client_ctx->fe_buf, client_ctx->fe_cap,
                                  0, client_ctx, on_client_recv_complete);
        if (rc < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_IO, "Worker %u: failed to requeue recv for session %lu",
                        worker->id, (unsigned long)session->id);
            close_session(worker, session, client_ctx);
        }
    }
}

/* ============================================================================
 * Worker Lifecycle
 * ============================================================================ */

/**
 * @brief Initialize one worker and its owned runtime objects.
 *
 * Initialization wires together the reactor, backend pools, session slab,
 * recv-context pool, timers, optional migration channel, and stats context.
 * Most resource-heavy objects are created here so the running worker loop can
 * stay focused on event processing rather than late lazy initialization.
 *
 * @param worker Worker structure to initialize.
 * @param engine Owning engine.
 * @param id Worker identifier.
 * @param listen_fd Shared listening socket.
 * @return `0` on success, `-1` on failure.
 */
int keel_worker_init(
    keel_worker_t* worker,
    struct keel_engine* engine,
    uint32_t id,
    int listen_fd)
{
    memset(worker, 0, sizeof(*worker));
    
    worker->id = id;
    worker->engine = engine;
    worker->listen_fd = listen_fd;
    worker->state = KEEL_WORKER_INIT;
    worker->cpu_affinity = -1;  /* No affinity by default */
    
    /* Copy backend configuration from engine */
    const keel_engine_config_t* cfg = keel_engine_get_config(engine);
    if (cfg) {
        worker->backend_host = cfg->backend_host;
        worker->backend_port = cfg->backend_port;
        worker->backend_user = cfg->backend_user;
        worker->backend_password = cfg->backend_password;
        worker->backend_database = cfg->backend_database;
        worker->backend_protocol = cfg->default_protocol;
        
        /* Copy server pool pointer for read/write splitting */
        worker->server_pool = (keel_server_pool_t*)&cfg->server_pool;

        /* Prepared-statement pooling strategy */
        worker->ps_mode = cfg->ps_mode;

        /* Runtime mode tier */
        worker->runtime_mode = cfg->runtime_mode;

        /* Replication uncertainty tracking */
        worker->txn_tracking = cfg->txn_tracking;

        /* Zero-copy fast network path */
        worker->fast_network_path = cfg->fast_network_path;
        worker->result_cache = cfg->result_cache;

        /* Create the query result cache if result_cache = on.
         * One cache per worker (shared-nothing model). */
        if (worker->result_cache) {
            keel_error_t cache_err = keel_query_cache_create(
                &worker->query_cache,
                3000,   /* default TTL: 3 s */
                64      /* max size: 64 MiB */
            );
            if (cache_err != KEEL_OK) {
                KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                    "W%u: query cache init failed (err=%d) — caching disabled",
                    worker->id, (int)cache_err);
                worker->query_cache = NULL;
            }
        }

        /* Sticky-primary TTL */
        worker->sticky_primary_ttl_ms = cfg->sticky_primary_ttl_ms;

        /* TLS configuration (frontend + backend) */
        worker->tls_config         = cfg->tls_config;
        worker->backend_tls_config = cfg->backend_tls_config;

        /* Shard router for hash-based routing and scatter-merge */
        worker->router = cfg->router;
        worker->scatter_merge_max_mem_bytes = cfg->scatter_merge_max_mem_bytes;
        worker->scatter_merge_spill_dir     = cfg->scatter_merge_spill_dir;

        /* Propagate operational timing from engine config.
         * worker_thread_func reads these at startup to arm timers so they
         * must be set before keel_engine_run() is called.
         * Each field falls back to a hard minimum if the config value is 0. */
        worker->idle_timeout_ms         = cfg->idle_timeout_ms        > 0
                                          ? cfg->idle_timeout_ms        : 300000U;
        worker->pool_prune_interval_ms  = cfg->pool_prune_interval_ms > 0
                                          ? cfg->pool_prune_interval_ms : 30000U;
        worker->pool_refill_interval_ms = cfg->pool_refill_interval_ms >= 100
                                          ? cfg->pool_refill_interval_ms : 100U;
        worker->pool_refill_backoff_ms  = cfg->pool_refill_backoff_ms > 0
                                          ? cfg->pool_refill_backoff_ms  : 5000U;
        worker->hotpath_instr_mask      = cfg->hotpath_instr_mask;

        /* Create backend connection pools for multiplexing.
         *
         * min_pool_size / max_pool_size are PER SERVER values from the config.
         * We divide by num_workers so the aggregate across all workers never
         * exceeds the configured per-server limit.
         *
         * Example: min=20, max=60, 4 workers → each worker gets 5 min, 15 max
         *          to EACH server (primary and each replica).
         *          Total to primary across all workers: 4 × 15 = 60 = max_pool_size ✓
         */
        uint32_t nw = cfg->num_workers > 0 ? cfg->num_workers : 4;
        size_t per_worker_min = cfg->pool_min_size / nw;
        size_t per_worker_max = cfg->pool_max_size / nw;
        if (per_worker_min < 1) per_worker_min = 1;
        if (per_worker_max < per_worker_min) per_worker_max = per_worker_min;

        /* max_waiting must be large enough to hold all queued frontend
         * sessions while they wait for a backend connection.  With transaction
         * pooling the queue drains rapidly (a query round-trip is ~1 ms), so
         * a deep queue is cheap and prevents spurious "pool exhausted" errors.
         * Default: 10 000 or 100× the per-worker pool size, whichever is larger.
         * Override: set pool_max_waiting in the INI (0 = auto). */
        size_t max_waiting;
        if (cfg->pool_max_waiting > 0) {
            /* Distribute the global cap evenly across workers */
            uint32_t nw_div = nw > 0 ? nw : 1;
            max_waiting = cfg->pool_max_waiting / nw_div;
            if (max_waiting < 1) max_waiting = 1;
        } else {
            max_waiting = per_worker_max * 100;
            if (max_waiting < 10000) max_waiting = 10000;
        }

        /* Admission controller — limit concurrent frontend connections.
         * max_frontends = 0 means unlimited (the default). */
        keel_admission_init(&worker->admission,
                            cfg->max_clients_per_worker,  /* max_frontends (0=unlimited) */
                            (uint32_t)per_worker_max,     /* max_backends */
                            (uint32_t)max_waiting);       /* max_waiting */

        backend_pool_config_t primary_cfg = {
            .host = cfg->backend_host,
            .port = cfg->backend_port,
            .user = cfg->backend_user ? cfg->backend_user : "postgres",
            .password = cfg->backend_password,
            .database = cfg->backend_database ? cfg->backend_database : "postgres",
            .protocol = cfg->default_protocol,
            .min_connections = per_worker_min,
            .max_connections = per_worker_max,
            .max_waiting = max_waiting,
            .idle_timeout_ms = cfg->pool_idle_timeout_ms,
            .max_connection_age_ms = cfg->pool_max_connection_age_ms,
            .wait_timeout_ms = cfg->connect_timeout_ms > 0 ? cfg->connect_timeout_ms : 10000,
            .tls_config = worker->backend_tls_config,
        };

        /* Create one pool per configured server (indexed by server_pool slot) */
        if (cfg->server_pool.count > 0) {
            worker->server_pools = keel_calloc(cfg->server_pool.count,
                                               sizeof(backend_pool_t*));
            worker->server_pool_count = cfg->server_pool.count;

            for (size_t i = 0; i < cfg->server_pool.count; i++) {
                const keel_backend_server_t* srv = &cfg->server_pool.servers[i];
                backend_pool_config_t pool_cfg = {
                    .host = srv->host,
                    .port = srv->port,
                    .user = srv->user ? srv->user : cfg->backend_user,
                    .password = srv->password ? srv->password : cfg->backend_password,
                    .database = srv->database ? srv->database : cfg->backend_database,
                    .protocol = cfg->default_protocol,
                    .min_connections = per_worker_min,
                    .max_connections = per_worker_max,
                    .max_waiting = max_waiting,
                    .idle_timeout_ms = cfg->pool_idle_timeout_ms,
                    .max_connection_age_ms = cfg->pool_max_connection_age_ms,
                    .wait_timeout_ms = cfg->connect_timeout_ms > 0 ? cfg->connect_timeout_ms : 10000,
                    .tls_config = worker->backend_tls_config,
                };
                backend_pool_t* p = backend_pool_create(&pool_cfg);
                if (p) {
                    backend_pool_set_wait_callback(p, pool_wait_resume_cb);
                }
                worker->server_pools[i] = p;
            }
        } else {
            /* Legacy single-backend fallback */
            worker->server_pools = keel_calloc(1, sizeof(backend_pool_t*));
            worker->server_pool_count = 1;
            worker->server_pools[0] = backend_pool_create(&primary_cfg);
            if (worker->server_pools[0]) {
                backend_pool_set_wait_callback(worker->server_pools[0], pool_wait_resume_cb);
            }
        }
    } else {
        /* Fallback defaults (should never happen with proper config) */
        worker->backend_host = "127.0.0.1";
        worker->backend_port = 5432;
        worker->backend_user = "postgres";
        worker->backend_password = NULL;
        worker->backend_database = "postgres";
        worker->backend_protocol = "postgres";
        worker->server_pool = NULL;
        worker->hotpath_instr_mask = KEEL_HOT_INSTR_ALL;
    }
    
    /* Create reactor with queue depth scaled to max expected connections.
     * Each active frontend session holds ~2 SQEs (a recv + sometimes a send).
     * With the default depth of 256, a burst of >128 simultaneous connections
     * overflows the SQE ring — io_uring_get_sqe() returns NULL, the accept
     * re-arm fails, and the worker stalls completely.
     *
     * Scale queue depth based on configured max_conns_per_worker so that
     * high-connection-count deployments don't run out of SQEs.  Minimum
     * depth 512 — each session needs ~3 SQEs (client recv, backend recv,
     * deferred send) so 512 entries supports ~170 concurrent sessions.
     * The io_uring init path will halve the depth automatically if the
     * kernel rejects the requested size (e.g., memlock limit). */
    size_t session_cap = cfg ? cfg->session_pool_size : 1024;
    if (session_cap < 512) session_cap = 512;
    uint32_t uring_depth = (uint32_t)session_cap;
    /* Round up to next power of 2 for io_uring efficiency */
    uring_depth--;
    uring_depth |= uring_depth >> 1;
    uring_depth |= uring_depth >> 2;
    uring_depth |= uring_depth >> 4;
    uring_depth |= uring_depth >> 8;
    uring_depth |= uring_depth >> 16;
    uring_depth++;
    if (uring_depth < 512) uring_depth = 512;
    if (uring_depth > 65536) uring_depth = 65536; /* Kernel sanity cap */

    keel_reactor_config_t rconfig = KEEL_REACTOR_CONFIG_DEFAULT;
    rconfig.queue_depth = uring_depth;
    rconfig.register_fds = true;
    if (cfg != NULL) {
        rconfig.use_buf_rings = cfg->use_buf_rings;
        rconfig.buf_ring_size = cfg->buf_ring_size;
        rconfig.sqpoll = cfg->sqpoll;
        rconfig.sqpoll_idle_ms = cfg->sqpoll_idle_ms;
    }
    worker->reactor = keel_reactor_create(&rconfig);
    if (worker->reactor == NULL) {
        return -1;
    }
    
    /* Wire reactor into pools so they can do async connect */
    for (size_t i = 0; i < worker->server_pool_count; i++) {
        if (worker->server_pools[i]) {
            worker->server_pools[i]->reactor = worker->reactor;
            backend_pool_async_warmup(worker->server_pools[i]);
        }
    }
    
    /* Initialize session slab — must be large enough for the expected
     * concurrent frontend connections per worker.  Uses session_pool_size
     * which is now driven by max_conns_per_worker from the config. */
    size_t slab_size = session_cap;
    if (keel_session_slab_init(&worker->sessions, slab_size) < 0) {
        keel_reactor_destroy(worker->reactor);
        return -1;
    }

    /* Create recv context pool — one recv_context_t per active session.
     * Pre-allocated with O(1) free-list alloc/free, zero syscalls on the
     * hot path.  I/O buffers are heap-allocated separately so the pool slot
     * is only ~400 B of metadata.  initial_count=256 → ~100 KB upfront. */
    {
        keel_pool_config_t rcfg = {
            .object_size   = sizeof(recv_context_t),
            .object_align  = 64,   /* Cache-line aligned */
            .initial_count = 256,
            .max_count     = slab_size,
            .zero_on_alloc = true,
        };
        worker->recv_ctx_pool = keel_pool_create(&rcfg);
        if (!worker->recv_ctx_pool) {
            keel_session_slab_destroy(&worker->sessions);
            keel_reactor_destroy(worker->reactor);
            return -1;
        }
    }

    /* Initialize pipe pool (Linux only) — use config value, not hardcoded */
    {
        size_t pool_sz = cfg ? cfg->pipe_pool_size : 16;
        if (pool_sz > 256) pool_sz = 256;  /* Sanity cap */
        keel_pipe_pool_init(&worker->pipes, pool_sz);
    }
    
    /* Initialize timer wheel (10ms resolution) */
    keel_timer_wheel_init(&worker->timers, 10);
    
    /* Attach per-worker stats context from the engine's collector */
    {
        keel_stats_collector_t *sc = keel_engine_get_stats_collector(engine);
        worker->stats_ctx = sc ? keel_stats_collector_get_ctx(sc, id) : NULL;

        /* Initialize function-level instrumentation with the configured mask */
        if (worker->stats_ctx) {
            keel_instr_ctx_init(&worker->stats_ctx->instr, cfg->instr_mask);
        }
    }

    /* Wire stats context into pools so they can track backends_cleaning */
    for (size_t i = 0; i < worker->server_pool_count; i++) {
        if (worker->server_pools[i])
            worker->server_pools[i]->stats_ctx = worker->stats_ctx;
    }
    
#ifdef __linux__
    /* Create eventfd for wakeup */
    worker->eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
#endif

    /* Initialise migration channel for inbound session transfers */
    if (keel_migration_init(&worker->migration, worker->id, worker->eventfd) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONN,
            "Worker %u: failed to initialise migration channel", worker->id);
        /* Non-fatal: migration is an optional optimisation.
         * sock_recv == -1 means drain() will be a no-op. */
    }

    /* Cache hook registry pointer — avoids repeated engine dereferences in
     * the query hot path (engine_flow.c fires hooks 3-4x per query). */
    worker->hooks = keel_engine_get_hook_registry(engine);
    worker->hook_mask = keel_hook_registry_active_mask(worker->hooks);

    /* Cache audit log pointer — engine-global, not owned by the worker.
     * NULL when audit logging is disabled; checked at emit sites. */
    worker->audit_log = (struct keel_audit_log*)keel_engine_get_audit_log(engine);

    /* ---- Per-worker authentication manager ----
     *
     * Build the auth manager from the engine config.  Each worker gets its
     * own manager instance (shared-nothing model) so there is no locking on
     * the auth hot path.  The manager is NULL in trust mode.
     */
    worker->auth_manager = NULL;
    if (cfg && cfg->auth_method != KEEL_AUTH_TRUST) {
        keel_auth_manager_config_t amcfg = {
            .default_method   = cfg->auth_method,
            .allow_clear_password = true,
            .scram_iterations = 4096,
        };
        worker->auth_manager = keel_auth_manager_create(&amcfg);
        if (!worker->auth_manager) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "Worker %u: failed to create auth manager — falling back to trust",
                worker->id);
        } else {
            keel_error_t rerr = KEEL_OK;
            switch (cfg->auth_method) {
            case KEEL_AUTH_SCRAM_SHA_256:
                rerr = keel_auth_manager_register(worker->auth_manager,
                                                  keel_auth_scram_sha256_ops(), NULL);
                if (rerr == KEEL_OK && cfg->auth_userlist_file)
                    keel_auth_load_userlist(worker->auth_manager, cfg->auth_userlist_file);
                break;
            case KEEL_AUTH_MD5:
                rerr = keel_auth_manager_register(worker->auth_manager,
                                                  keel_auth_md5_ops(), NULL);
                if (rerr == KEEL_OK && cfg->auth_userlist_file)
                    keel_auth_load_userlist(worker->auth_manager, cfg->auth_userlist_file);
                break;
            case KEEL_AUTH_CERTIFICATE:
                rerr = keel_auth_manager_register(worker->auth_manager,
                                                  keel_auth_cert_ops(), NULL);
                break;
            case KEEL_AUTH_LDAP: {
                keel_auth_ldap_config_t lcfg = {
                    .url           = cfg->auth_ldap_url,
                    .base_dn       = cfg->auth_ldap_base_dn,
                    .bind_dn       = cfg->auth_ldap_bind_dn,
                    .bind_password = cfg->auth_ldap_bind_password,
                    .dn_suffix     = cfg->auth_ldap_dn_suffix,
                    .search_filter = cfg->auth_ldap_search_filter,
                    .start_tls     = cfg->auth_ldap_start_tls,
                    .timeout_s     = cfg->auth_ldap_timeout_s > 0
                                     ? cfg->auth_ldap_timeout_s : 5,
                };
                rerr = keel_auth_manager_register(worker->auth_manager,
                                                  keel_auth_ldap_ops(), &lcfg);
                break;
            }
            case KEEL_AUTH_PAM: {
                keel_auth_pam_config_t pcfg = {
                    .service_name = cfg->auth_pam_service_name
                                    ? cfg->auth_pam_service_name : "keel",
                };
                rerr = keel_auth_manager_register(worker->auth_manager,
                                                  keel_auth_pam_ops(), &pcfg);
                break;
            }
            case KEEL_AUTH_QUERY: {
                keel_auth_query_config_t qcfg = {
                    .query       = cfg->auth_query,
                    .conn_string = cfg->auth_query_conn_string,
                };
                rerr = keel_auth_manager_register(worker->auth_manager,
                                                  keel_auth_query_ops(), &qcfg);
                break;
            }
            default:
                KEEL_LOG_WARN(KEEL_LOG_CAT_AUTH,
                    "Worker %u: unrecognised auth method %d — using trust",
                    worker->id, (int)cfg->auth_method);
                keel_auth_manager_destroy(worker->auth_manager);
                worker->auth_manager = NULL;
                break;
            }
            if (rerr != KEEL_OK && worker->auth_manager) {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                    "Worker %u: auth provider registration failed (err=%d) — falling back to trust",
                    worker->id, (int)rerr);
                keel_auth_manager_destroy(worker->auth_manager);
                worker->auth_manager = NULL;
            }
        }
    }

    return 0;
}

/**
 * @brief Launch a worker thread.
 *
 * @param worker Worker to start.
 * @return `0` on success, `-1` if thread creation failed.
 */
int keel_worker_start(keel_worker_t* worker)
{
    worker->state = KEEL_WORKER_STARTING;
    worker->should_stop = false;
    
    /* Accept is now armed inside worker_thread_func AFTER pool warmup
     * completes.  This eliminates the thundering herd of pool waits by
     * ensuring backends are ready before clients arrive. */
    
    /* Start thread */
    if (pthread_create(&worker->thread, NULL, worker_thread_func, worker) != 0) {
        return -1;
    }
    
    return 0;
}

/**
 * @brief Request that a worker exit its main loop.
 *
 * @param worker Worker to stop.
 * @return
 */
void keel_worker_stop(keel_worker_t* worker)
{
    worker->should_stop = true;
    worker->state = KEEL_WORKER_STOPPING;
    
#ifdef __linux__
    /* Wake up the worker if it's blocked. 8-byte eventfd add can never
     * block: the kernel buffer is always able to accept it (sem-mode is
     * disabled here). */
    if (worker->eventfd >= 0) {
        uint64_t val = 1;
        ssize_t r = write(worker->eventfd, &val, sizeof(val)); /* NOLINT(keel-blocking) */
        (void)r;
    }
#endif
}

void keel_worker_drain(keel_worker_t* worker)
{
    atomic_store_explicit(&worker->draining, true, memory_order_release);

#ifdef __linux__
    if (worker->eventfd >= 0) {
        uint64_t val = 1;
        /* 8-byte eventfd add never blocks; see keel_worker_stop above. */
        ssize_t r = write(worker->eventfd, &val, sizeof(val)); /* NOLINT(keel-blocking) */
        (void)r;
    }
#endif
}

size_t keel_worker_active_sessions(const keel_worker_t* worker)
{
    return worker->sessions.allocated;
}

int keel_worker_join(keel_worker_t* worker)
{
    return pthread_join(worker->thread, NULL);
}

/**
 * @brief Destroy worker-owned runtime resources after the thread has stopped.
 *
 * @param worker Worker to clean up.
 * @return
 */
void keel_worker_cleanup(keel_worker_t* worker)
{
    /* Destroy sessions (and close connections) */
    for (size_t i = 0; i < worker->sessions.capacity; i++) {
        keel_session_t* session = &worker->sessions.sessions[i];
        if (session->client_fd >= 0) {
            close(session->client_fd);
            session->client_fd = -1;
        }
        if (session->server_fd >= 0) {
            close(session->server_fd);
            session->server_fd = -1;
        }
        /* Destroy flow state and TLS context via the recv_context */
        recv_context_t* recv_ctx = (recv_context_t*)session->userdata;
        if (recv_ctx) {
            keel_session_flow_destroy(&recv_ctx->flow);
            if (recv_ctx->tls_ctx) {
                keel_tls_context_destroy(recv_ctx->tls_ctx);
                recv_ctx->tls_ctx = NULL;
            }
            recv_ctx_free_bufs(recv_ctx);
            session->userdata = NULL;
        }
        session->plugin_state = NULL;
        keel_residual_clear(&session->client_residual);
        keel_residual_clear(&session->server_residual);
    }
    
    /* Destroy backend pools */
    if (worker->server_pools) {
        for (size_t i = 0; i < worker->server_pool_count; i++) {
            if (worker->server_pools[i]) {
                backend_pool_destroy(worker->server_pools[i]);
            }
        }
        keel_free(worker->server_pools);
        worker->server_pools = NULL;
        worker->server_pool_count = 0;
    }
    
    /* Destroy recv context pool before session slab */
    if (worker->recv_ctx_pool) {
        keel_pool_destroy(worker->recv_ctx_pool);
        worker->recv_ctx_pool = NULL;
    }

    /* Destroy the query result cache */
    if (worker->query_cache) {
        keel_query_cache_destroy(worker->query_cache);
        worker->query_cache = NULL;
    }

    keel_session_slab_destroy(&worker->sessions);
    keel_pipe_pool_destroy(&worker->pipes);
    keel_reactor_destroy(worker->reactor);
    
    /* Destroy migration channel */
    keel_migration_destroy(&worker->migration);

    /* Destroy auth manager */
    if (worker->auth_manager) {
        keel_auth_manager_destroy(worker->auth_manager);
        worker->auth_manager = NULL;
    }

#ifdef __linux__
    if (worker->eventfd >= 0) {
        close(worker->eventfd);
    }
#endif
}

/* ============================================================================
 * Worker Pool
 * ============================================================================ */

/**
 * @brief Accept a pre-validated client FD into this worker.
 *
 * Called from keel_migration_drain() on the destination worker thread when
 * receiving a migrated session.  Equivalent to the happy path of
 * on_accept_complete(), but without rearming the listen socket.
 */
void keel_worker_on_accept(keel_worker_t* worker, int client_fd)
{
    if (worker_setup_session(worker, client_fd) < 0) {
        close(client_fd);
    }
}

/**
 * @brief Migrate an idle session to the least-loaded worker.
 *
 * Eligibility is checked via keel_migration_can_migrate().  On success the
 * source side closes its copy of the client FD and frees the session slot;
 * the destination worker will adopt the session from its migration inbox on
 * its next reactor loop iteration.
 *
 * @return 0 on success, -1 if migration was not possible.
 */
int keel_worker_migrate_session(keel_worker_t* worker, keel_session_t* session)
{
    if (!worker || !session) return -1;
    if (!keel_migration_can_migrate(session)) return -1;

    /* Find the least-loaded target worker */
    uint32_t tgt_idx = keel_migration_find_target(worker->engine, worker->id);
    if (tgt_idx == UINT32_MAX) return -1;

    keel_worker_t* dst = keel_engine_get_worker_mut(worker->engine, tgt_idx);
    if (!dst) return -1;

    /* Send via SCM_RIGHTS + ring buffer — ownership of client_fd transfers */
    if (keel_migration_send(session, &dst->migration) < 0) return -1;

    /* --- Source-side cleanup ---
     *
     * The destination now owns a dup'd copy of client_fd (via SCM_RIGHTS).
     * We need to cancel the pending fe_recv at the source and release all
     * resources.  Since there is no keel_reactor_cancel_recv(), we use the
     * same deferred-close pattern as close_session(): mark the recv_context
     * as closing, close our fd copy (which cancels the io_uring recv), and
     * let on_client_recv_complete() do the final freeing.
     *
     * Important: we do NOT call keel_session_flow_destroy() here to avoid
     * a double-free — on_client_recv_complete() will call complete_deferred_close()
     * which calls it after the reactor operation is fully cancelled. */
    recv_context_t* recv_ctx = (recv_context_t*)session->userdata;
    if (recv_ctx) {
        /* Cancel the idle timer */
        keel_timer_wheel_cancel(&worker->timers, &recv_ctx->idle_timer);

        /* Mark as closing so the imminent error callback is a no-op */
        recv_ctx->closing = true;
    }

    /* Close source fd copy — destination has its own via SCM_RIGHTS */
    if (session->client_fd >= 0) {
        close(session->client_fd);
        session->client_fd = -1;
    }

    /* Update stats — session is "closed" on this worker */
    worker->stats.sessions_closed++;
    if (worker->stats_ctx) {
        KEEL_STAT_INC(worker->stats_ctx, sessions_closed);
        KEEL_STAT_GAUGE_DEC(worker->stats_ctx, sessions_active);
    }

    /* Track migration counter */
    worker->migration.sent++;
    if (worker->stats_ctx)
        KEEL_STAT_INC(worker->stats_ctx, migrations_sent);

    KEEL_LOG_DEBUG(KEEL_LOG_CAT_CONN,
        "W%u: migrated session %lu → W%u",
        worker->id, (unsigned long)session->id, tgt_idx);

    return 0;
}

/**
 * @brief Initialize a pool of workers sharing one listening socket.
 *
 * @param pool Worker-pool wrapper to initialize.
 * @param count Number of workers to create.
 * @param engine Owning engine.
 * @param listen_fd Shared listening socket.
 * @return `0` on success, `-1` on failure.
 */
int keel_worker_pool_init(
    keel_worker_pool_t* pool,
    size_t count,
    struct keel_engine* engine,
    int listen_fd)
{
    pool->workers = (keel_worker_t*)keel_calloc(count, sizeof(keel_worker_t));
    if (pool->workers == NULL) {
        return -1;
    }
    
    pool->count = count;
    pool->running = 0;
    pthread_mutex_init(&pool->lock, NULL);
    
    for (size_t i = 0; i < count; i++) {
        if (keel_worker_init(&pool->workers[i], engine, (uint32_t)i, listen_fd) < 0) {
            /* Clean up previously initialized workers */
            for (size_t j = 0; j < i; j++) {
                keel_worker_cleanup(&pool->workers[j]);
            }
            keel_free(pool->workers);
            return -1;
        }
        
        /* Set CPU affinity */
        pool->workers[i].cpu_affinity = (int)i;
    }
    
    return 0;
}

int keel_worker_pool_start(keel_worker_pool_t* pool)
{
    pthread_mutex_lock(&pool->lock);
    
    for (size_t i = 0; i < pool->count; i++) {
        if (keel_worker_start(&pool->workers[i]) == 0) {
            pool->running++;
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    
    return pool->running == pool->count ? 0 : -1;
}

void keel_worker_pool_stop(keel_worker_pool_t* pool)
{
    pthread_mutex_lock(&pool->lock);
    
    for (size_t i = 0; i < pool->count; i++) {
        keel_worker_stop(&pool->workers[i]);
    }
    
    pthread_mutex_unlock(&pool->lock);
}

/**
 * @brief Join all worker threads in the pool.
 *
 * Blocks until every worker's pthread has exited, then marks the pool as
 * not running.
 *
 * @param pool  Worker pool to join.
 */
void keel_worker_pool_join(keel_worker_pool_t* pool)
{
    for (size_t i = 0; i < pool->count; i++) {
        keel_worker_join(&pool->workers[i]);
    }
    
    pthread_mutex_lock(&pool->lock);
    pool->running = 0;
    pthread_mutex_unlock(&pool->lock);
}

/**
 * @brief Clean up all workers and release pool resources.
 *
 * Calls keel_worker_cleanup() for each worker, frees the workers array,
 * and destroys the pool mutex.
 *
 * @param pool  Worker pool to destroy.
 */
void keel_worker_pool_destroy(keel_worker_pool_t* pool)
{
    for (size_t i = 0; i < pool->count; i++) {
        keel_worker_cleanup(&pool->workers[i]);
    }
    
    keel_free(pool->workers);
    pthread_mutex_destroy(&pool->lock);
    memset(pool, 0, sizeof(*pool));
}

/**
 * @brief Aggregate reactor statistics across all workers in the pool.
 *
 * @param pool         Pool to query.
 * @param[out] stats   Receives summed reactor stats.
 */
void keel_worker_pool_get_stats(keel_worker_pool_t* pool, keel_reactor_stats_t* stats)
{
    memset(stats, 0, sizeof(*stats));
    
    pthread_mutex_lock(&pool->lock);
    
    for (size_t i = 0; i < pool->count; i++) {
        keel_reactor_stats_t worker_stats;
        keel_reactor_get_stats(pool->workers[i].reactor, &worker_stats);
        
        stats->ops_submitted += worker_stats.ops_submitted;
        stats->ops_completed += worker_stats.ops_completed;
        stats->bytes_read += worker_stats.bytes_read;
        stats->bytes_written += worker_stats.bytes_written;
        stats->bytes_spliced += worker_stats.bytes_spliced;
        stats->accepts += worker_stats.accepts;
        stats->connects += worker_stats.connects;
        stats->timeouts += worker_stats.timeouts;
        stats->errors += worker_stats.errors;
    }
    
    pthread_mutex_unlock(&pool->lock);
}
