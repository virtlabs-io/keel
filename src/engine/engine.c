/**
 * @file engine.c
 * @brief Engine lifecycle, worker-pool startup, and connection accounting.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file implements KEEL's top-level control plane. It is intentionally
 * separate from the hot data path in `engine_flow.c`: the engine owns process
 * lifetime, worker creation, signal handling, drain/shutdown behavior, and
 * cross-worker connection accounting, while the workers and flow layer own the
 * per-message protocol state machines.
 *
 * Architectural responsibilities implemented here:
 *   - Resolve a concrete runtime configuration from defaults plus user input.
 *   - Choose the best available reactor backend for the host platform.
 *   - Materialize and start the shared-nothing worker pool.
 *   - Own the listening socket and decide when accepts are globally allowed.
 *   - Run the main-thread control loop for signals and periodic maintenance.
 *   - Coordinate graceful drain and last-resort force-close semantics.
 *
 * Control-plane algorithm and interface model:
 *   - Startup is two-phase. `keel_engine_create()` is pure construction and
 *     capability resolution; it never starts threads or mutates external file
 *     descriptors. `keel_engine_start()` takes ownership of an already-created
 *     listening socket, applies required socket flags, initializes the worker
 *     pool, and then launches the workers.
 *   - Reactor selection is capability-driven rather than configuration-heavy.
 *     On Linux the engine prefers `io_uring` only when the kernel is new
 *     enough for the feature set KEEL relies on; otherwise it falls back to
 *     `epoll`. BSD/macOS use `kqueue`. The goal is to preserve predictable
 *     behavior on older kernels instead of hard-failing on missing features.
 *   - The main loop is signal-driven through `sigwait()` or
 *     `sigtimedwait()`. This keeps the control plane simple and avoids mixing
 *     asynchronous signal handlers with complex mutable state. Periodic work
 *     such as system-stat sampling and maintenance callbacks is piggybacked on
 *     the timed wait path instead of introducing a separate timer thread.
 *   - Drain is implemented by closing the listening socket first, then waiting
 *     for the active-session count to converge to zero. This is cheaper and
 *     less error-prone than trying to inject per-worker "stop accepting"
 *     messages while the shared listening fd remains open. If the timeout
 *     expires, the engine force-closes remaining sessions except those still
 *     resolving commit-in-doubt state, because killing those sessions can hide
 *     whether a COMMIT actually reached the backend.
 *
 * Design tradeoffs:
 *   - The engine uses a small amount of polling during drain (`nanosleep`
 *     with periodic progress logs) rather than a more elaborate condition-
 *     variable scheme. Drain is a rare control-plane event, so simplicity and
 *     debuggability matter more than micro-optimizing the wait loop.
 *   - Connection counters are atomic and intentionally approximate enough for
 *     management decisions. They are used for visibility, drain decisions, and
 *     worker selection heuristics, not for transactional correctness.
 *   - The engine deliberately does not understand protocol semantics beyond
 *     lifecycle boundaries. That separation keeps shutdown/orchestration code
 *     readable and prevents control-plane logic from duplicating the protocol
 *     and routing decisions already centralized elsewhere.
 */

#include "keel/engine/engine.h"
#include "keel/reactor/reactor.h"
#include "keel/engine/worker.h"
#include "keel/session/session.h"
#include "keel/protocol/protocol_flow.h"
#include "keel/core/stats.h"
#include "keel/mem/mem.h"
#include "keel_error.h"
#include "keel/log/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdatomic.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <signal.h>
#include "keel/util/platform_compat.h"

#ifdef __linux__
#include <sys/utsname.h>
#include <sys/sysinfo.h>
#include <sched.h>
#endif

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

/* keel_sigtimedwait — portable sigtimedwait(2) — provided by platform_compat.h */

/* ============================================================================
 * Engine Structure — defined in the private header shared with engine_flow.c
 * ============================================================================ */

#include "engine_private.h"

/* ============================================================================
 * Platform Detection
 * ============================================================================ */

/**
 * @brief Detect the number of CPUs available to size the worker pool.
 *
 * The engine resolves `num_workers == 0` to the host CPU count so default
 * deployments scale with available cores. Platform-specific APIs are used when
 * available, with a conservative fallback of one worker.
 *
 * @return Number of CPUs visible to the process, or `1` on unsupported platforms.
 */
static int get_cpu_count(void) {
#ifdef __linux__
    return get_nprocs();
#elif defined(__APPLE__)
    int count;
    size_t size = sizeof(count);
    if (sysctlbyname("hw.ncpu", &count, &size, NULL, 0) == 0) {
        return count;
    }
    return 1;
#else
    return 1;
#endif
}

/**
 * @brief Pick the most capable reactor backend supported by the host platform.
 *
 * On Linux, the helper prefers `io_uring` only when the kernel release is new
 * enough to support the features KEEL relies on; otherwise it falls back to
 * `epoll`. BSD-style systems choose `kqueue`.
 *
 * @return Preferred reactor type for the current platform.
 */
static keel_reactor_type_t detect_best_reactor(void) {
#ifdef __linux__
    /* Check kernel version for io_uring support */
    struct utsname uname_data;
    if (uname(&uname_data) == 0) {
        int major = 0, minor = 0;
        if (sscanf(uname_data.release, "%d.%d", &major, &minor) >= 2) {
            if (major > 5 || (major == 5 && minor >= 6)) {
                return KEEL_REACTOR_IOURING;
            }
        }
    }
    return KEEL_REACTOR_EPOLL;
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
    return KEEL_REACTOR_KQUEUE;
#else
    /* Fallback - should not happen on supported platforms */
    return KEEL_REACTOR_EPOLL;
#endif
}

/* ============================================================================
 * Socket Helpers
 * ============================================================================ */

/**
 * @brief Enable non-blocking I/O on a file descriptor.
 *
 * @param fd File descriptor to modify.
 * @return `0` on success or `-1` if the descriptor flags could not be read or updated.
 */
/* set_nonblocking() removed: use keel_set_nonblocking() from keel/util/platform_compat.h */

/**
 * @brief Enable `SO_REUSEADDR` on a socket.
 *
 * @param fd Socket file descriptor.
 * @return `0` on success or `-1` if `setsockopt()` fails.
 */
static int set_reuseaddr(int fd) {
    int opt = 1;
    return setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
}

/**
 * @brief Disable Nagle's algorithm on a TCP socket.
 *
 * Low-latency request/response traffic benefits from sending small protocol
 * frames immediately rather than waiting for coalescing.
 *
 * @param fd Socket file descriptor.
 * @return `0` on success or `-1` if `setsockopt()` fails.
 */
/* set_nodelay() removed: use keel_set_nodelay() from keel/util/platform_compat.h */

/* ============================================================================
 * Engine Lifecycle
 * ============================================================================ */

/**
 * @brief Allocate and initialize the top-level engine object.
 *
 * This routine copies or synthesizes the runtime configuration, resolves the
 * worker count, creates the optional statistics collector, and chooses the
 * concrete reactor backend the workers will use once started. It does not yet
 * create worker threads or take ownership of a listening socket.
 *
 * @param config Engine configuration, or `NULL` to use `KEEL_ENGINE_CONFIG_DEFAULT`.
 * @return New engine handle, or `NULL` if allocation fails.
 */
keel_engine_t* keel_engine_create(const keel_engine_config_t* config) {
    keel_engine_t* engine = keel_calloc(1, sizeof(keel_engine_t));
    if (!engine) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "engine: failed to allocate engine structure");
        return NULL;
    }
    
    /* Apply configuration */
    if (config) {
        engine->config = *config;
    } else {
        /* Use defaults */
        keel_engine_config_t defaults = KEEL_ENGINE_CONFIG_DEFAULT;
        engine->config = defaults;
    }
    
    /* Determine number of workers */
    if (engine->config.num_workers == 0) {
        engine->num_workers = (uint32_t)get_cpu_count();
    } else {
        engine->num_workers = engine->config.num_workers;
    }
    /* Write resolved count back so workers can read it */
    engine->config.num_workers = engine->num_workers;

    /* Store hook registry (may be NULL — no hooks for this group) */
    engine->hook_registry = engine->config.hook_registry;
    
    /* Create stats collector */
    engine->stats_collector = keel_stats_collector_create(
        engine->config.stats_level, engine->num_workers);
    if (!engine->stats_collector) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_STATS,
                     "engine: stats collector alloc failed, running with stats OFF");
    }
    
    /* Detect reactor type */
    if (engine->config.reactor_type == KEEL_REACTOR_AUTO) {
        engine->reactor_type = detect_best_reactor();
    } else {
        engine->reactor_type = engine->config.reactor_type;
    }
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: using %s reactor with %u workers",
             engine->reactor_type == KEEL_REACTOR_IOURING ? "io_uring" :
             engine->reactor_type == KEEL_REACTOR_KQUEUE ? "kqueue" : "epoll",
             engine->num_workers);
    
/* Initialize state */
    engine->listen_fd = -1;
    engine->running = false;
    engine->stopping = false;
    engine->draining = false;
    engine->lifecycle_state = KEEL_ENGINE_STATE_CREATED;
    engine->drain_start_ns = 0;
    engine->drain_timeout_ms = 30000; /* 30 second default */
    engine->pool_initialized = false;
    
    return engine;
}

/**
 * @brief Start the engine by initializing and launching the worker pool.
 *
 * The caller provides an already-created listening socket. The engine applies
 * required socket flags, initializes worker-local state, and then starts all
 * worker threads. If startup fails after partial initialization, the function
 * destroys the worker pool before returning an error.
 *
 * @param engine Engine handle returned by `keel_engine_create()`.
 * @param listen_fd Listening socket that workers will accept on.
 * @return `0` on success or `-1` if the engine is invalid, already running, or
 *         worker startup fails.
 */
int keel_engine_start(keel_engine_t* engine, int listen_fd) {
    if (!engine) {
        return -1;
    }
    
    if (engine->running) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "engine: already running");
        return -1;
    }
    
    /* Store listen socket */
    engine->listen_fd = listen_fd;
    
    /* Set socket options */
    if (keel_set_nonblocking(listen_fd) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "engine: failed to set non-blocking: %s", strerror(errno));
        return -1;
    }
    
    if (set_reuseaddr(listen_fd) < 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "engine: failed to set SO_REUSEADDR: %s", strerror(errno));
    }
    
    /* Initialize worker pool */
    int ret = keel_worker_pool_init(
        &engine->worker_pool,
        engine->num_workers,
        engine,
        listen_fd
    );
    
    if (ret < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "engine: failed to initialize worker pool");
        return -1;
    }
    engine->pool_initialized = true;
    
    /* Start worker threads */
    if (keel_worker_pool_start(&engine->worker_pool) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "engine: failed to start worker threads");
        goto cleanup;
    }
    
    engine->running = true;
    engine->lifecycle_state = KEEL_ENGINE_STATE_ACTIVE;
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: started with %u workers", engine->num_workers);
    
    return 0;

cleanup:
    if (engine->pool_initialized) {
        keel_worker_pool_destroy(&engine->worker_pool);
        engine->pool_initialized = false;
    }
    return -1;
}

/**
 * @brief Run the main-thread engine control loop until shutdown is requested.
 *
 * The loop blocks `SIGINT`, `SIGTERM`, and `SIGUSR1` in the main thread and
 * then waits synchronously for them via `sigwait()` or `sigtimedwait()`. This
 * avoids asynchronous signal-handler reentrancy and gives the engine one
 * serialized place to trigger maintenance work, stats sampling, and orderly
 * shutdown transitions.
 *
 * When `stats_interval_ms` is configured, timed waits double as the engine's
 * periodic scheduler. Timeout wakeups sample system metrics and invoke the
 * registered periodic callback without creating an extra maintenance thread.
 * `SIGUSR1` uses the same path so manual stats dumps and timer-driven dumps are
 * implemented consistently.
 *
 * @param engine Running engine handle.
 * @return `0` after the shutdown signal path completes, or `-1` if the engine
 *         is `NULL` or was never started.
 */
int keel_engine_run(keel_engine_t* engine) {
    if (!engine || !engine->running) {
        return -1;
    }
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: entering main loop");
    
    /* Block SIGINT/SIGTERM/SIGUSR1 in main thread so sigwait can catch them */
    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGINT);
    sigaddset(&sigset, SIGTERM);
    sigaddset(&sigset, SIGUSR1);
    pthread_sigmask(SIG_BLOCK, &sigset, NULL);
    
    /* If periodic stats are configured, use sigtimedwait for periodic wake */
    uint32_t interval_ms = engine->config.stats_interval_ms;
    
    while (!engine->stopping) {
        int sig;
        
        if (interval_ms > 0) {
            struct timespec ts = {
                .tv_sec  = interval_ms / 1000,
                .tv_nsec = (long)(interval_ms % 1000) * 1000000L,
            };
            sig = keel_sigtimedwait(&sigset, &ts);
            if (sig < 0) {
                if (errno == EAGAIN) {
                    /* Timeout — fire the periodic callback */
                    if (engine->stats_collector &&
                        engine->config.stats_level >= 3)
                        keel_stats_sample_system(engine->stats_collector);
                    if (engine->periodic_cb)
                        engine->periodic_cb(engine->periodic_ctx);
                    continue;
                }
                continue;  /* EINTR */
            }
        } else {
            if (sigwait(&sigset, &sig) != 0)
                continue;
        }
        
        if (sig == SIGUSR1) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_STATS,
                         "engine: SIGUSR1 received, stats dump requested");
            if (engine->stats_collector &&
                engine->config.stats_level >= 3)
                keel_stats_sample_system(engine->stats_collector);
            if (engine->periodic_cb)
                engine->periodic_cb(engine->periodic_ctx);
            continue;
        }
        
        /* SIGINT / SIGTERM */
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                     "engine: received signal %d, initiating shutdown", sig);
        engine->stopping = true;
    }
    
    return 0;
}

/**
 * @brief Read a monotonic nanosecond timestamp for drain accounting.
 *
 * @return Current monotonic time in nanoseconds.
 */
static uint64_t engine_get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/**
 * @brief Enter drain mode and wait for active sessions to finish naturally.
 *
 * Draining closes the listening socket so workers stop accepting new clients,
 * then polls the engine's active-connection count until it reaches zero or the
 * configured timeout expires. When the timeout is reached, the engine force
 * closes remaining sessions except those still resolving commit-in-doubt state,
 * because killing those sessions could lose a committed transaction outcome.
 *
 * @param engine Running engine handle.
 * @return `0` on success or `-1` if the engine is not running.
 */
int keel_engine_drain(keel_engine_t* engine) {
    if (!engine || !engine->running) {
        return -1;
    }

    if (engine->draining) {
        return 0; /* Already draining */
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: entering drain mode (timeout=%ums)",
                 engine->drain_timeout_ms);
    engine->draining = true;
    engine->lifecycle_state = KEEL_ENGINE_STATE_DRAINING;
    engine->drain_start_ns = engine_get_time_ns();

    /* Close listen socket to stop accepting new connections */
    if (engine->listen_fd >= 0) {
        close(engine->listen_fd);
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: listen socket closed (no new connections)");
        engine->listen_fd = -1;
    }

    /* Wait for active connections to finish or timeout */
    uint64_t timeout_ns = (uint64_t)engine->drain_timeout_ms * 1000000ULL;
    uint64_t last_log_ns = engine->drain_start_ns;

    while (__atomic_load_n(&engine->active_connections, __ATOMIC_RELAXED) > 0) {
        uint64_t now = engine_get_time_ns();
        uint64_t elapsed = now - engine->drain_start_ns;

        if (elapsed >= timeout_ns) {
            uint64_t remaining = __atomic_load_n(&engine->active_connections, __ATOMIC_RELAXED);
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "engine: drain timeout after %ums, %llu connections still active — force-closing",
                engine->drain_timeout_ms, (unsigned long long)remaining);
            /* Force-close all remaining sessions */
            uint64_t killed = keel_engine_force_close_all(engine);
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "engine: force-closed %llu sessions after drain timeout",
                (unsigned long long)killed);
            break;
        }

        /* Log progress every 5 seconds */
        if (now - last_log_ns >= 5000000000ULL) {
            uint64_t remaining = __atomic_load_n(&engine->active_connections, __ATOMIC_RELAXED);
            double elapsed_s = (double)elapsed / 1.0e9;
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                "engine: draining — %llu active connections remaining (%.1fs elapsed)",
                (unsigned long long)remaining, elapsed_s);
            last_log_ns = now;
        }

        /* Sleep briefly to avoid busy-waiting */
        struct timespec sleep_ts = { .tv_sec = 0, .tv_nsec = 100000000 }; /* 100ms */
        nanosleep(&sleep_ts, NULL); /* NOLINT(keel-blocking): lifecycle drain loop, not worker reactor */
    }

    uint64_t final_active = __atomic_load_n(&engine->active_connections, __ATOMIC_RELAXED);
    if (final_active == 0) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: all connections drained cleanly");
    }

    return 0;
}

/**
 * @brief Override the maximum time drain mode waits before escalating.
 *
 * @param engine Engine handle.
 * @param timeout_ms Maximum drain wait in milliseconds; `0` means force-close immediately.
 * @return
 */
void keel_engine_set_drain_timeout(keel_engine_t* engine, uint32_t timeout_ms) {
    if (engine) {
        engine->drain_timeout_ms = timeout_ms;
    }
}

/**
 * @brief Report whether the engine is currently in drain mode.
 *
 * @param engine Engine handle.
 * @return `true` if drain mode is active, otherwise `false`.
 */
bool keel_engine_is_draining(keel_engine_t* engine) {
    return engine ? engine->draining : false;
}

/**
 * @brief Return the current lifecycle state for the engine.
 *
 * @param engine Engine handle.
 * @return Current lifecycle state, or `KEEL_ENGINE_STATE_STOPPED` when `engine` is `NULL`.
 */
keel_engine_state_t keel_engine_get_state(keel_engine_t* engine) {
    if (!engine) {
        return KEEL_ENGINE_STATE_STOPPED;
    }
    return (keel_engine_state_t)engine->lifecycle_state;
}

/**
 * @brief Force-close all sessions still tracked by worker slabs.
 *
 * This is the final shutdown fallback used after drain timeout. The walk is
 * intentionally conservative: sessions marked as commit-in-doubt are skipped
 * so their independent recovery path can determine whether the transaction
 * committed before the backend disappeared.
 *
 * @param engine Engine handle.
 * @return Number of sessions that were force-closed.
 */
uint64_t keel_engine_force_close_all(keel_engine_t* engine) {
    if (!engine || !engine->pool_initialized) {
        return 0;
    }

    uint64_t killed = 0;
    keel_worker_pool_t* pool = &engine->worker_pool;

    for (size_t wi = 0; wi < pool->count; wi++) {
        keel_worker_t* w = &pool->workers[wi];
        keel_session_slab_t* slab = &w->sessions;

        for (size_t si = 0; si < slab->capacity; si++) {
            keel_session_t* s = &slab->sessions[si];

            /* Only close sessions that have an open client_fd
             * (allocated sessions; free-list entries have fd == -1). */
            if (s->client_fd >= 0) {
                /* Protect sessions with active commit-in-doubt recovery:
                 * forcibly closing them would lose committed transactions.
                 * These sessions will complete CID independently. */
                if (s->commit_in_doubt) {
                    KEEL_LOG_WARN(KEEL_LOG_CAT_CONN,
                        "engine: skipping force-close for session %lu (commit-in-doubt active)",
                        (unsigned long)s->id);
                    continue;
                }

                KEEL_LOG_WARN(KEEL_LOG_CAT_CONN,
                    "engine: force-closing session %lu on worker %u (client_fd=%d, server_fd=%d)",
                    (unsigned long)s->id, w->id, s->client_fd, s->server_fd);

                if (s->client_fd >= 0) {
                    close(s->client_fd);
                    s->client_fd = -1;
                }
                if (s->server_fd >= 0) {
                    close(s->server_fd);
                    s->server_fd = -1;
                }
                __atomic_sub_fetch(&engine->active_connections, 1, __ATOMIC_RELAXED);
                killed++;
            }
        }
    }

    return killed;
}

/**
 * @brief Stop the engine and join all worker threads.
 *
 * The function is idempotent for already-stopped engines. It marks the engine
 * as stopping, signals the worker pool, waits for worker exit, and updates the
 * lifecycle state accordingly.
 *
 * @param engine Engine handle.
 * @return
 */
void keel_engine_stop(keel_engine_t* engine) {
    if (!engine) {
        return;
    }
    
    if (!engine->running) {
        return;
    }
    
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: stopping");
    engine->stopping = true;
    engine->lifecycle_state = KEEL_ENGINE_STATE_STOPPING;
    
    /* Signal workers to stop */
    if (engine->pool_initialized) {
        keel_worker_pool_stop(&engine->worker_pool);
        keel_worker_pool_join(&engine->worker_pool);
    }
    
    engine->running = false;
    engine->lifecycle_state = KEEL_ENGINE_STATE_STOPPED;
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "engine: stopped");
}

/**
 * @brief Rolling restart of all worker threads.
 *
 * Drains existing workers (stop accepting, let sessions complete), then spawns
 * a fresh worker pool with the current engine configuration. New workers begin
 * accepting on the shared listen socket before old workers have fully exited,
 * minimising the service gap to the time it takes the new pool to warm up.
 *
 * @param engine Running engine handle.
 * @param drain_timeout_ms Maximum time (ms) to wait for old worker sessions to
 *                         drain. Zero uses the engine's default.
 * @return `0` on success, `-1` on error.
 */
int keel_engine_restart_workers(keel_engine_t* engine, uint32_t drain_timeout_ms) {
    if (!engine || !engine->running || !engine->pool_initialized) {
        return -1;
    }

    uint32_t timeout_ms = drain_timeout_ms > 0 ? drain_timeout_ms
                                                : engine->drain_timeout_ms;
    keel_worker_pool_t *pool = &engine->worker_pool;
    size_t n = pool->count;

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "engine: restarting %zu workers (drain timeout %u ms)",
                  n, timeout_ms);

    /* Phase 1 — signal old workers to drain (stop accepting, keep processing). */
    for (size_t i = 0; i < n; i++) {
        keel_worker_drain(&pool->workers[i]);
    }

    /* Phase 2 — save old workers array and allocate new one. */
    keel_worker_t *old_workers = pool->workers;
    size_t old_count = pool->count;

    keel_worker_t *new_workers = keel_calloc(n, sizeof(keel_worker_t));
    if (!new_workers) {
        /* Rollback: un-drain old workers (accept will re-arm on next loop). */
        for (size_t i = 0; i < n; i++) {
            atomic_store_explicit(&old_workers[i].draining, false, memory_order_release);
        }
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "engine: worker restart failed (allocation)");
        return -1;
    }

    /* Init new workers (they'll get current engine config). */
    for (size_t i = 0; i < n; i++) {
        if (keel_worker_init(&new_workers[i], engine, (uint32_t)i,
                             engine->listen_fd) < 0) {
            for (size_t j = 0; j < i; j++) keel_worker_cleanup(&new_workers[j]);
            keel_free(new_workers);
            for (size_t j = 0; j < old_count; j++)
                atomic_store_explicit(&old_workers[j].draining, false, memory_order_release);
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                           "engine: worker restart failed (init worker %zu)", i);
            return -1;
        }
        new_workers[i].cpu_affinity = (int)i;
    }

    /* Phase 3 — swap the pool pointer so engine accessors see the new workers,
     * then start the new workers (they arm accept on listen_fd). */
    pthread_mutex_lock(&pool->lock);
    pool->workers = new_workers;
    pool->count   = n;
    pool->running = 0;
    pthread_mutex_unlock(&pool->lock);

    for (size_t i = 0; i < n; i++) {
        if (keel_worker_start(&new_workers[i]) == 0) {
            pthread_mutex_lock(&pool->lock);
            pool->running++;
            pthread_mutex_unlock(&pool->lock);
        }
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "engine: %zu new workers started, draining old workers",
                  pool->running);

    /* Phase 4 — wait for old workers to drain (sessions reach 0). */
    uint64_t deadline_ns = engine_get_time_ns() + (uint64_t)timeout_ms * 1000000ULL;
    size_t still_active;
    do {
        still_active = 0;
        for (size_t i = 0; i < old_count; i++) {
            if (old_workers[i].state != KEEL_WORKER_STOPPED)
                still_active++;
        }
        if (still_active == 0) break;

        struct timespec ts = { .tv_sec = 0, .tv_nsec = 100000000 }; /* 100ms */
        nanosleep(&ts, NULL); /* NOLINT(keel-blocking): graceful restart control loop, not worker reactor */
    } while (engine_get_time_ns() < deadline_ns);

    /* Phase 5 — force-stop any stragglers. */
    for (size_t i = 0; i < old_count; i++) {
        if (old_workers[i].state != KEEL_WORKER_STOPPED) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "engine: force-stopping old worker %u (%zu sessions remaining)",
                old_workers[i].id, old_workers[i].sessions.allocated);
            keel_worker_stop(&old_workers[i]);
        }
    }

    /* Phase 6 — join and cleanup. */
    for (size_t i = 0; i < old_count; i++) {
        keel_worker_join(&old_workers[i]);
        keel_worker_cleanup(&old_workers[i]);
    }
    keel_free(old_workers);

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "engine: worker restart complete (%zu workers active)",
                  pool->running);
    return 0;
}

/**
 * @brief Destroy the engine and release all remaining owned resources.
 *
 * If the caller forgot to stop the engine explicitly, this function stops it
 * first, then destroys the worker pool, closes the listening socket, tears
 * down the stats collector, and finally frees the engine object itself.
 *
 * @param engine Engine handle, or `NULL`.
 * @return
 */
void keel_engine_destroy(keel_engine_t* engine) {
    if (!engine) {
        return;
    }
    
    /* Ensure stopped */
    if (engine->running) {
        keel_engine_stop(engine);
    }
    
    /* Destroy worker pool */
    if (engine->pool_initialized) {
        keel_worker_pool_destroy(&engine->worker_pool);
        engine->pool_initialized = false;
    }
    
    /* Close listen socket if we still have it */
    if (engine->listen_fd >= 0) {
        close(engine->listen_fd);
        engine->listen_fd = -1;
    }
    
    /* Destroy stats collector */
    if (engine->stats_collector) {
        keel_stats_collector_destroy(engine->stats_collector);
        engine->stats_collector = NULL;
    }
    
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE, "engine: destroyed (total connections: %llu)", 
              (unsigned long long)engine->total_connections);
    
    /* Free heap-allocated strings for any dynamically-added servers
     * (servers with dynamic=true were added via ADD SERVER at runtime). */
    for (size_t i = 0; i < engine->config.server_pool.count; i++) {
        keel_backend_server_t *srv = &engine->config.server_pool.servers[i];
        if (!srv->dynamic) continue;
        keel_free((void*)srv->host);
        keel_free((void*)srv->user);
        keel_free((void*)srv->password);
        keel_free((void*)srv->database);
        keel_free((void*)srv->probe_user);
        keel_free((void*)srv->probe_password);
        keel_free((void*)srv->probe_auth);
    }

    keel_free(engine);
}

/**
 * @brief Report the reactor implementation chosen for this engine.
 *
 * @param engine Engine handle.
 * @return Active reactor type, or `KEEL_REACTOR_AUTO` when `engine` is `NULL`.
 */
keel_reactor_type_t keel_engine_get_reactor_type(keel_engine_t* engine) {
    if (!engine) {
        return KEEL_REACTOR_AUTO;
    }
    return engine->reactor_type;
}

/* ============================================================================
 * Connection Accept Handler
 * ============================================================================
 * Called by worker when a new connection is accepted.
 */

/**
 * @brief Initialize session state for a newly accepted client connection.
 *
 * Workers call this immediately after `accept()`. The engine normalizes socket
 * flags, allocates a session object from the worker slab, initializes protocol
 * state, and increments connection counters. Protocol flow initialization and
 * receive scheduling are deferred to worker-side flow setup.
 *
 * @param engine Engine handle.
 * @param worker Owning worker for the accepted connection.
 * @param client_fd Accepted client socket.
 * @param addr Peer socket address, unused by the current implementation.
 * @param addrlen Length of `addr`, unused by the current implementation.
 * @return
 */
void keel_engine_on_accept(keel_engine_t* engine, keel_worker_t* worker, 
                          int client_fd, struct sockaddr* addr, socklen_t addrlen) {
    (void)addr;
    (void)addrlen;
    
    if (!engine || !worker || client_fd < 0) {
        if (client_fd >= 0) {
            close(client_fd);
        }
        return;
    }
    
    /* Set socket options */
    keel_set_nonblocking(client_fd);
    keel_set_nodelay(client_fd);
    
    /* Allocate session from worker's slab */
    keel_session_t* session = keel_session_slab_alloc(&worker->sessions);
    if (!session) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "engine: session allocation failed, closing connection");
        close(client_fd);
        return;
    }
    
    /* Initialize session */
    if (keel_session_init(session, client_fd) < 0) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "engine: session init failed");
        keel_session_slab_free(&worker->sessions, session);
        close(client_fd);
        return;
    }
    
    session->worker = worker;
    session->mode = KEEL_MODE_STARTUP;
    
    /* Update stats */
    __atomic_add_fetch(&engine->total_connections, 1, __ATOMIC_RELAXED);
    __atomic_add_fetch(&engine->active_connections, 1, __ATOMIC_RELAXED);
    
    /* Note: recv scheduling and plugin context init are done by the worker
     * via keel_session_flow_init(). */
}

/* ============================================================================
 * Connection Close Handler
 * ============================================================================ */

/**
 * @brief Close and recycle all resources owned by a session.
 *
 * This helper closes any remaining client or backend sockets, runs the normal
 * session cleanup path, returns the session to its worker slab, and decrements
 * the active-connection counter.
 *
 * @param engine Engine handle.
 * @param session Session being closed.
 * @return
 */
void keel_engine_on_close(keel_engine_t* engine, keel_session_t* session) {
    if (!engine || !session) {
        return;
    }
    
    keel_worker_t* worker = session->worker;
    
    /* Close file descriptors */
    if (session->client_fd >= 0) {
        close(session->client_fd);
        session->client_fd = -1;
    }
    
    if (session->server_fd >= 0) {
        close(session->server_fd);
        session->server_fd = -1;
    }
    
    /* Cleanup session (clears residual buffers, protocol context, etc.) */
    keel_session_cleanup(session);
    
    /* Return to slab */
    if (worker) {
        keel_session_slab_free(&worker->sessions, session);
    }
    
    /* Update stats */
    __atomic_sub_fetch(&engine->active_connections, 1, __ATOMIC_RELAXED);
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Read the cumulative number of accepted client connections.
 *
 * @param engine Engine handle.
 * @return Total accepted connections, or `0` if `engine` is `NULL`.
 */
uint64_t keel_engine_get_total_connections(keel_engine_t* engine) {
    if (!engine) {
        return 0;
    }
    return __atomic_load_n(&engine->total_connections, __ATOMIC_RELAXED);
}

/**
 * @brief Read the current number of active client sessions.
 *
 * @param engine Engine handle.
 * @return Active connection count, or `0` if `engine` is `NULL`.
 */
uint64_t keel_engine_get_active_connections(keel_engine_t* engine) {
    if (!engine) {
        return 0;
    }
    return __atomic_load_n(&engine->active_connections, __ATOMIC_RELAXED);
}

/**
 * @brief Decrement the active-connection counter manually.
 *
 * This is used by management paths that terminate sessions outside the normal
 * close callback flow, such as administrative kill operations.
 *
 * @param engine Engine handle.
 * @return
 */
void keel_engine_dec_connections(keel_engine_t* engine) {
    if (!engine) return;
    __atomic_sub_fetch(&engine->active_connections, 1, __ATOMIC_RELAXED);
}

/**
 * @brief Return the engine configuration as an immutable pointer.
 *
 * @param engine Engine handle.
 * @return Pointer to the stored configuration, or `NULL` if `engine` is `NULL`.
 */
const keel_engine_config_t* keel_engine_get_config(keel_engine_t* engine) {
    if (!engine) {
        return NULL;
    }
    return &engine->config;
}

/**
 * @brief Return the engine configuration as a mutable pointer.
 *
 * Callers should use this only for narrowly scoped runtime adjustments that
 * are known to be safe after startup.
 *
 * @param engine Engine handle.
 * @return Mutable configuration pointer, or `NULL` if `engine` is `NULL`.
 */
keel_engine_config_t* keel_engine_get_config_mut(keel_engine_t* engine) {
    if (!engine) {
        return NULL;
    }
    return &engine->config;
}

/**
 * @brief Return the engine-wide statistics collector, if enabled.
 *
 * @param engine Engine handle.
 * @return Stats collector pointer, or `NULL` if stats are disabled or `engine` is `NULL`.
 */
keel_stats_collector_t* keel_engine_get_stats_collector(keel_engine_t* engine) {
    if (!engine) {
        return NULL;
    }
    return engine->stats_collector;
}

/**
 * @brief Install a callback fired from the main-thread control loop.
 *
 * The callback runs when the engine receives `SIGUSR1` and on periodic timer
 * wakeups when `stats_interval_ms` is configured. It is typically used for
 * stats dumps, housekeeping, or coordinated maintenance work.
 *
 * @param engine Engine handle.
 * @param cb Callback function, or `NULL` to clear it.
 * @param ctx Opaque context passed to `cb`.
 * @return
 */
void keel_engine_set_periodic_callback(keel_engine_t* engine,
                                       void (*cb)(void *ctx), void *ctx) {
    if (!engine) return;
    engine->periodic_cb  = cb;
    engine->periodic_ctx = ctx;
}

/**
 * @brief Return the resolved number of worker threads.
 *
 * @param engine Engine handle.
 * @return Number of workers, or `0` if `engine` is `NULL`.
 */
uint32_t keel_engine_get_num_workers(keel_engine_t* engine) {
    return engine ? engine->num_workers : 0;
}

/**
 * @brief Return a read-only pointer to a worker by index.
 *
 * @param engine Engine handle.
 * @param idx Worker index in `[0, num_workers)`.
 * @return Worker pointer, or `NULL` if the index is out of range.
 */
const keel_worker_t* keel_engine_get_worker(keel_engine_t* engine, uint32_t idx) {
    if (!engine || idx >= engine->num_workers) return NULL;
    if (!engine->worker_pool.workers) return NULL;
    return &engine->worker_pool.workers[idx];
}

/**
 * @brief Return a mutable pointer to a worker by index.
 *
 * @param engine Engine handle.
 * @param idx Worker index in `[0, num_workers)`.
 * @return Mutable worker pointer, or `NULL` if the index is out of range.
 */
keel_worker_t* keel_engine_get_worker_mut(keel_engine_t* engine, uint32_t idx) {
    if (!engine || idx >= engine->num_workers) return NULL;
    if (!engine->worker_pool.workers) return NULL;
    return &engine->worker_pool.workers[idx];
}

/**
 * @brief Return the engine's configured server-pool topology.
 *
 * @param engine Engine handle.
 * @return Pointer to the mutable server pool, or `NULL` if `engine` is `NULL`.
 */
keel_server_pool_t* keel_engine_get_server_pool(keel_engine_t* engine) {
    if (!engine) return NULL;
    return &engine->config.server_pool;
}

/**
 * @brief Return the hook registry associated with the engine's worker group.
 *
 * @param engine Engine handle.
 * @return Hook registry pointer, or `NULL` when hooks are disabled or `engine` is `NULL`.
 */
keel_hook_registry_t* keel_engine_get_hook_registry(keel_engine_t* engine) {
    if (!engine) return NULL;
    return engine->hook_registry;
}

/**
 * @brief Attach a distributed-tracing provider to the engine.
 *
 * Stores @p tracer in the engine so that the query-processing pipeline can
 * emit spans for each request.  Passing `NULL` disables tracing without
 * affecting other engine state.  Not thread-safe; call before starting
 * worker threads.
 *
 * @param engine  Engine handle. `NULL` is a safe no-op.
 * @param tracer  Tracer implementation, or `NULL` to disable tracing.
 */
void keel_engine_set_tracer(keel_engine_t* engine, struct keel_tracer* tracer) {
    if (engine) engine->tracer = tracer;
}

/**
 * @brief Retrieve the tracing provider currently attached to the engine.
 *
 * @param engine Engine handle.
 * @return The `keel_tracer` pointer set by `keel_engine_set_tracer()`, or
 *         `NULL` if tracing is disabled or @p engine is `NULL`.
 */
struct keel_tracer* keel_engine_get_tracer(keel_engine_t* engine) {
    if (!engine) return NULL;
    return engine->tracer;
}

void keel_engine_set_audit_log(keel_engine_t* engine, struct keel_audit_log* al) {
    if (engine) engine->audit_log = (keel_audit_log_t*)al;
}

struct keel_audit_log* keel_engine_get_audit_log(keel_engine_t* engine) {
    if (!engine) return NULL;
    return (struct keel_audit_log*)engine->audit_log;
}
