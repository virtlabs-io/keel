/**
 * @file engine_private.h
 * @brief Private engine struct definition shared between engine.c and engine_flow.c.
 *
 * NOT part of the public API.  Only translation units inside src/engine/ that
 * genuinely need to dereference `keel_engine_t` fields should include this.
 */
#ifndef KEEL_ENGINE_PRIVATE_H
#define KEEL_ENGINE_PRIVATE_H

#include "keel/engine/engine.h"
#include "keel/engine/worker.h"
#include "keel/core/stats.h"
#include "keel/reactor/reactor.h"
#include "keel/log/audit_log.h"
#include <stdbool.h>
#include <stdatomic.h>

struct keel_engine {
    keel_engine_config_t config;

    /* Worker pool (inline, not pointer) */
    keel_worker_pool_t   worker_pool;
    uint32_t            num_workers;
    bool                pool_initialized;

    /* Listen socket */
    int                 listen_fd;

    /* State */
    _Atomic bool        running;
    _Atomic bool        stopping;
    _Atomic bool        draining;
    _Atomic int         lifecycle_state;
    uint64_t            drain_start_ns;
    uint32_t            drain_timeout_ms;

    /* Statistics */
    uint64_t            total_connections;
    uint64_t            active_connections;

    /* Instrumentation framework */
    keel_stats_collector_t* stats_collector;

    /* Reactor type actually in use */
    keel_reactor_type_t  reactor_type;

    /* Periodic callback */
    void (*periodic_cb)(void *ctx);
    void *periodic_ctx;

    /* Hook registry */
    keel_hook_registry_t* hook_registry;

    /* Distributed tracing (NULL when disabled) */
    struct keel_tracer* tracer;

    /* Audit logging (NULL when disabled) */
    keel_audit_log_t*   audit_log;
};

#endif /* KEEL_ENGINE_PRIVATE_H */
