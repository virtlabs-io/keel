/**
 * @file hook.c
 * @brief Registry-backed hook execution, plugin loading, and per-point statistics.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Each worker group owns a keel_hook_registry_t — hooks registered in one
 * group never fire in another.
 *
 * Legacy global API (keel_hook_init / keel_hook_shutdown) keeps a single
 * internal registry for backward compatibility with tests.
 *
 * Internal implementation strategy:
 *   - Each hook point owns a singly linked chain sorted by ascending priority.
 *     Registration is rare compared with firing, so insertion cost is allowed
 *     to be O(n) in exchange for a minimal runtime representation.
 *   - Firing is a straight-line traversal of the selected chain. The first hook
 *     that returns `false` aborts the chain and preserves the earliest failure
 *     reason in `ctx->error_msg`.
 *   - Per-point counters track fire count, abort count, and cumulative time to
 *     make hook overhead measurable without instrumenting every callback site
 *     externally.
 *   - Native plugin loading is intentionally simple: `dlopen()`, validate a
 *     versioned descriptor, register hooks into the target registry, and keep
 *     the shared object handle around until registry destruction.
 *
 * Tradeoffs:
 *   - There is no complex concurrent mutation story here because registries are
 *     expected to be assembled during startup and then treated as mostly stable.
 *     That keeps hook firing cheap and predictable in the query path.
 *   - The legacy global registry is retained only for compatibility and tests;
 *     production code should prefer explicit registries to avoid hidden global
 *     state.
 */

#include "keel_hook.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"
#include "keel/util/util.h"

#include <dlfcn.h>
#include <string.h>
#include <time.h>

/* ============================================================================
 * Internal types
 * ============================================================================ */

struct keel_hook_handle {
    keel_hook_type_t     type;
    keel_hook_point_t    point;
    char                 name[64];
    int                  priority;
    void*                data;

    /* Type-specific */
    union {
        struct {
            keel_hook_fn  fn;
        } native;
        struct {
            char  file[256];
            char  func[64];
        } lua;
        struct {
            char  module[256];
            char  func[64];
            void* cached_module;  /* PyObject* cached on first successful import */
            bool  import_failed;  /* true = log once, skip silently on every call */
        } python;
    };

    struct keel_hook_handle* next;  /* linked list */
};

typedef struct hook_chain {
    keel_hook_handle_t* head;       /* sorted by priority */
    uint32_t            count;
    _Atomic uint64_t    fire_count;
    _Atomic uint64_t    abort_count;
    _Atomic uint64_t    total_ns;
} hook_chain_t;

/* Plugin tracking */
typedef struct loaded_plugin {
    void*               dl_handle;
    const keel_hook_plugin_info_t* info;
    struct loaded_plugin* next;
} loaded_plugin_t;

/* ============================================================================
 * Registry — the per-group data structure
 * ============================================================================ */

struct keel_hook_registry {
    hook_chain_t     chains[KEEL_HOOK_POINT_COUNT];
    loaded_plugin_t* plugins;
};

/* Legacy global registry (for keel_hook_init / keel_hook_shutdown) */
static keel_hook_registry_t* g_legacy_registry = NULL;

/* Forward declarations for Lua/Python bridges */
extern bool keel_lua_call_hook(const char* file, const char* func,
                               keel_hook_ctx_t* ctx);

/**
 * @brief Invoke a Python hook function from the hook dispatch loop.
 *
 * Implemented in `src/hook/python_bridge.c`.  Imports @p module (or reuses
 * the cached handle in @p p_cached_module) and calls `module.func(ctx)`.
 * Sets `*p_import_failed` on the first unrecoverable import error so
 * subsequent calls are skipped without retrying the import.\n *
 * @param module          Python module name to import.
 * @param func            Callable attribute name within the module.
 * @param ctx             Hook context passed to the Python function.
 * @param p_cached_module In/out cache slot for the imported module object.
 * @param p_import_failed Flag set to `true` if the import fails permanently.
 * @return `true` to continue the hook chain, `false` to short-circuit it.
 */
extern bool keel_python_call_hook(const char* module, const char* func,
                                  keel_hook_ctx_t* ctx,
                                  void** p_cached_module,
                                  bool*  p_import_failed);

/**
 * @brief Release a Python module reference obtained by `keel_python_call_hook`.
 *
 * Wraps `Py_XDECREF()` so that the hook registry can drop cached module
 * handles without depending on Python headers directly.
 *
 * @param mod Python module object pointer (cast to `void*`). `NULL` is safe.
 */
extern void keel_python_decref_module(void* mod);

/* ============================================================================
 * Insertion (sorted by priority)
 * ============================================================================ */

/**
 * @brief Insert a hook into a point-local chain while preserving priority order.
 *
 * Registration is a cold path, so the chain is kept as a simple linked list
 * sorted at insertion time rather than a tree or heap. That makes firing a
 * cache-friendly forward walk with no extra ordering work.
 *
 * @param chain Hook chain for one hook point.
 * @param h Registration handle to insert.
 * @return
 */
static void chain_insert(hook_chain_t* chain, keel_hook_handle_t* h) {
    keel_hook_handle_t** pp = &chain->head;
    while (*pp && (*pp)->priority <= h->priority) {
        pp = &(*pp)->next;
    }
    h->next = *pp;
    *pp = h;
    chain->count++;
}

/**
 * @brief Remove a hook from a point-local chain.
 *
 * @param chain Hook chain for one hook point.
 * @param h Registration handle to remove.
 * @return
 */
static void chain_remove(hook_chain_t* chain, keel_hook_handle_t* h) {
    keel_hook_handle_t** pp = &chain->head;
    while (*pp) {
        if (*pp == h) {
            *pp = h->next;
            chain->count--;
            return;
        }
        pp = &(*pp)->next;
    }
}

/* ============================================================================
 * Registry Lifecycle
 * ============================================================================ */

/**
 * @brief Allocate and initialize an empty hook registry.
 *
 * @return New registry handle, or `NULL` if allocation fails.
 */
keel_hook_registry_t* keel_hook_registry_create(void) {
    keel_hook_registry_t* reg = keel_calloc(1, sizeof(*reg));
    if (!reg) return NULL;

    memset(reg->chains, 0, sizeof(reg->chains));
    reg->plugins = NULL;

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Hook registry created (%p)", (void*)reg);
    return reg;
}

/**
 * @brief Destroy a registry, release hooks, and unload plugins.
 *
 * Python cached-module references are explicitly decremented before handle
 * destruction so the interpreter still owns valid objects during cleanup.
 *
 * @param reg Registry to destroy.
 * @return
 */
void keel_hook_registry_destroy(keel_hook_registry_t* reg) {
    if (!reg) return;

    /* Unregister all hooks */
    for (int p = 0; p < KEEL_HOOK_POINT_COUNT; p++) {
        keel_hook_handle_t* h = reg->chains[p].head;
        while (h) {
            keel_hook_handle_t* next = h->next;
            /* Release cached Python module reference before freeing the handle.
             * keel_hook_registry_destroy is always called before
             * keel_python_shutdown, so the interpreter is still valid here. */
            if (h->type == KEEL_HOOK_TYPE_PYTHON && h->python.cached_module) {
                keel_python_decref_module(h->python.cached_module);
                h->python.cached_module = NULL;
            }
            keel_free(h);
            h = next;
        }
        reg->chains[p].head = NULL;
        reg->chains[p].count = 0;
    }

    /* Unload all plugins */
    loaded_plugin_t* pl = reg->plugins;
    while (pl) {
        loaded_plugin_t* next = pl->next;
        if (pl->info && pl->info->shutdown)
            pl->info->shutdown();
        if (pl->dl_handle)
            dlclose(pl->dl_handle);
        keel_free(pl);
        pl = next;
    }
    reg->plugins = NULL;

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Hook registry destroyed (%p)", (void*)reg);
    keel_free(reg);
}

/**
 * @brief Compute a bitmask describing which hook points are populated.
 *
 * The engine caches this mask so it can skip expensive context-building work
 * entirely when no hooks exist for a point.
 *
 * @param reg Hook registry.
 * @return Bitmask with one bit per populated hook point.
 */
uint32_t keel_hook_registry_active_mask(const keel_hook_registry_t* reg) {
    if (!reg) return 0;
    uint32_t mask = 0;
    for (int p = 0; p < KEEL_HOOK_POINT_COUNT; p++) {
        if (reg->chains[p].head)
            mask |= (1u << p);
    }
    return mask;
}

/* ============================================================================
 * Legacy global lifecycle (for tests)
 * ============================================================================ */

/**
 * @brief Initialize the legacy global hook registry used by tests.
 *
 * @return `KEEL_OK` on success or an error code on allocation failure.
 */
keel_error_t keel_hook_init(void) {
    if (g_legacy_registry) return KEEL_OK;

    g_legacy_registry = keel_hook_registry_create();
    if (!g_legacy_registry) return KEEL_ERR_NOMEM;

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Hook system initialized (legacy global)");
    return KEEL_OK;
}

/**
 * @brief Destroy the legacy global hook registry.
 *
 * @return
 */
void keel_hook_shutdown(void) {
    if (!g_legacy_registry) return;

    keel_hook_registry_destroy(g_legacy_registry);
    g_legacy_registry = NULL;

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Hook system shutdown (legacy global)");
}

/* ============================================================================
 * Registration
 * ============================================================================ */

/**
 * @brief Register a native C hook into a registry.
 *
 * @return Hook handle on success, or `NULL` on validation or allocation failure.
 */
keel_hook_handle_t* keel_hook_register(
    keel_hook_registry_t* reg,
    keel_hook_point_t point,
    const char*       name,
    keel_hook_fn      fn,
    int               priority,
    void*             data)
{
    if (!reg) reg = g_legacy_registry;
    if (!reg || point >= KEEL_HOOK_POINT_COUNT || !fn)
        return NULL;

    keel_hook_handle_t* h = keel_calloc(1, sizeof(*h));
    if (!h) return NULL;

    h->type = KEEL_HOOK_TYPE_NATIVE;
    h->point = point;
    h->priority = priority;
    h->data = data;
    h->native.fn = fn;
    if (name) {
        strncpy(h->name, name, sizeof(h->name) - 1);
    }

    chain_insert(&reg->chains[point], h);

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "Hook registered: '%s' at %s (priority %d)",
                  h->name, keel_hook_point_name(point), priority);
    return h;
}

/**
 * @brief Register a Lua-backed hook.
 *
 * @return Hook handle on success, or `NULL` on validation or allocation failure.
 */
keel_hook_handle_t* keel_hook_register_lua(
    keel_hook_registry_t* reg,
    keel_hook_point_t point,
    const char*       name,
    const char*       lua_file,
    const char*       lua_func,
    int               priority)
{
    if (!reg) reg = g_legacy_registry;
    if (!reg || point >= KEEL_HOOK_POINT_COUNT)
        return NULL;

    keel_hook_handle_t* h = keel_calloc(1, sizeof(*h));
    if (!h) return NULL;

    h->type = KEEL_HOOK_TYPE_LUA;
    h->point = point;
    h->priority = priority;
    if (name) strncpy(h->name, name, sizeof(h->name) - 1);
    if (lua_file) strncpy(h->lua.file, lua_file, sizeof(h->lua.file) - 1);
    if (lua_func) strncpy(h->lua.func, lua_func, sizeof(h->lua.func) - 1);

    chain_insert(&reg->chains[point], h);

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "Lua hook registered: '%s' at %s (%s:%s)",
                  h->name, keel_hook_point_name(point),
                  h->lua.file, h->lua.func);
    return h;
}

/**
 * @brief Register a Python-backed hook and pre-import its module when possible.
 *
 * Pre-importing shifts path/import failures into startup time and warms the
 * CPython module cache so the first live query does not pay import latency.
 *
 * @return Hook handle on success, or `NULL` on validation or allocation failure.
 */
keel_hook_handle_t* keel_hook_register_python(
    keel_hook_registry_t* reg,
    keel_hook_point_t point,
    const char*       name,
    const char*       py_module,
    const char*       py_func,
    int               priority)
{
    if (!reg) reg = g_legacy_registry;
    if (!reg || point >= KEEL_HOOK_POINT_COUNT)
        return NULL;

    keel_hook_handle_t* h = keel_calloc(1, sizeof(*h));
    if (!h) return NULL;

    h->type = KEEL_HOOK_TYPE_PYTHON;
    h->point = point;
    h->priority = priority;
    if (name) strncpy(h->name, name, sizeof(h->name) - 1);
    if (py_module) strncpy(h->python.module, py_module, sizeof(h->python.module) - 1);
    if (py_func) strncpy(h->python.func, py_func, sizeof(h->python.func) - 1);

    chain_insert(&reg->chains[point], h);

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "Python hook registered: '%s' at %s (%s.%s)",
                  h->name, keel_hook_point_name(point),
                  h->python.module, h->python.func);

    /* Pre-import the module now so that:
     *   1. Path errors are detected immediately (not on first client request)
     *   2. sys.modules cache is warm — no import overhead on first real call
     * We pass ctx=NULL which makes keel_python_call_hook perform the import
     * and cache the result, then return without invoking the hook function. */
    keel_python_call_hook(h->python.module, h->python.func, /*ctx=*/NULL,
                          &h->python.cached_module, &h->python.import_failed);
    if (h->python.import_failed) {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                      "Python hook '%s' is DISABLED — fix the script path and restart",
                      h->name);
    }

    return h;
}

/**
 * @brief Unregister and free one hook handle.
 *
 * @param reg Registry containing the hook.
 * @param handle Handle to remove.
 * @return
 */
void keel_hook_unregister(keel_hook_registry_t* reg, keel_hook_handle_t* handle) {
    if (!reg) reg = g_legacy_registry;
    if (!handle || !reg) return;
    chain_remove(&reg->chains[handle->point], handle);
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Hook unregistered: '%s'", handle->name);
    keel_free(handle);
}

/* ============================================================================
 * Plugin Loading
 * ============================================================================ */

/**
 * @brief Load a native hook plugin and register its exported hooks.
 *
 * The loader validates the plugin's API version before registration so that an
 * incompatible shared object cannot silently corrupt the registry layout.
 *
 * @param reg Target registry.
 * @param path Shared-library path.
 * @return `KEEL_OK` on success or an error code on load/validation failure.
 */
keel_error_t keel_hook_load_plugin(keel_hook_registry_t* reg, const char* path) {
    if (!reg) reg = g_legacy_registry;
    if (!reg || !path) return KEEL_ERR_INVALID_ARG;

    void* dl = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (!dl) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Failed to load plugin '%s': %s",
                       path, dlerror());
        return KEEL_ERR_NOT_FOUND;
    }

    /* Look up the init function */
    typedef const keel_hook_plugin_info_t* (*init_fn_t)(void);
    init_fn_t init_fn = (init_fn_t)dlsym(dl, "keel_hook_plugin_init");
    if (!init_fn) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "Plugin '%s' missing keel_hook_plugin_init(): %s",
                       path, dlerror());
        dlclose(dl);
        return KEEL_ERR_NOT_FOUND;
    }

    const keel_hook_plugin_info_t* info = init_fn();
    if (!info || info->api_version != KEEL_HOOK_PLUGIN_API_VERSION) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "Plugin '%s' API version mismatch (got %u, want %u)",
                       path, info ? info->api_version : 0,
                       KEEL_HOOK_PLUGIN_API_VERSION);
        dlclose(dl);
        return KEEL_ERR_NOT_FOUND;
    }

    /* Track the plugin */
    loaded_plugin_t* pl = keel_calloc(1, sizeof(*pl));
    if (!pl) { dlclose(dl); return KEEL_ERR_NOMEM; }

    pl->dl_handle = dl;
    pl->info = info;
    pl->next = reg->plugins;
    reg->plugins = pl;

    /* Register the plugin's hooks into this registry */
    for (size_t i = 0; i < info->hook_count; i++) {
        const keel_hook_plugin_def_t* def = &info->hooks[i];
        keel_hook_register(reg, def->point, def->name, def->fn, def->priority, NULL);
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "Plugin loaded: '%s' v%s (%zu hooks)",
                  info->name, info->version, info->hook_count);
    return KEEL_OK;
}

/* ============================================================================
 * Hook Firing
 * ============================================================================ */

/**
 * @brief Read a monotonic timestamp used for hook timing statistics.
 *
 * @return Current monotonic time in nanoseconds.
 */
static uint64_t now_ns(void) { return (uint64_t)keel_time_now(); }

/**
 * @brief Execute all hooks registered for one hook point.
 *
 * The context parameter is both the hook input object and the output channel
 * back to the engine. Hooks may update route hints, pin requests, or abort
 * reasons in-place. The chain short-circuits on the first `false` result so
 * later hooks do not overwrite an earlier policy decision.
 *
 * @param reg Registry to fire from.
 * @param point Hook point being executed.
 * @param[in,out] ctx Mutable context shared across the chain.
 * @return `true` if all hooks passed, `false` if a hook aborted processing.
 */
bool keel_hook_fire(keel_hook_registry_t* reg, keel_hook_point_t point,
                    keel_hook_ctx_t* ctx) {
    if (!reg) reg = g_legacy_registry;
    if (!reg || point >= KEEL_HOOK_POINT_COUNT)
        return true;

    hook_chain_t* chain = &reg->chains[point];
    if (!chain->head) return true;  /* No hooks — fast path */

    uint64_t t0 = now_ns();
    chain->fire_count++;

    for (keel_hook_handle_t* h = chain->head; h; h = h->next) {
        ctx->user_data = h->data;
        bool result = true;

        switch (h->type) {
        case KEEL_HOOK_TYPE_NATIVE:
        case KEEL_HOOK_TYPE_PLUGIN:
            result = h->native.fn(ctx);
            break;

        case KEEL_HOOK_TYPE_LUA:
            result = keel_lua_call_hook(h->lua.file, h->lua.func, ctx);
            break;

        case KEEL_HOOK_TYPE_PYTHON:
            result = keel_python_call_hook(h->python.module,
                                           h->python.func, ctx,
                                           &h->python.cached_module,
                                           &h->python.import_failed);
            break;
        }

        if (!result) {
            chain->abort_count++;
            uint64_t elapsed = now_ns() - t0;
            chain->total_ns += elapsed;

            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                          "Hook '%s' aborted at %s: %s",
                          h->name, keel_hook_point_name(point),
                          ctx->error_msg[0] ? ctx->error_msg : "(no reason)");
            return false;
        }
    }

    uint64_t elapsed = now_ns() - t0;
    chain->total_ns += elapsed;
    return true;
}

/* ============================================================================
 * Statistics
 * ============================================================================ */

/**
 * @brief Return cumulative statistics for one hook point.
 *
 * @param reg Hook registry.
 * @param point Hook point.
 * @return Statistics snapshot for the selected point.
 */
keel_hook_stats_t keel_hook_get_stats(keel_hook_registry_t* reg,
                                      keel_hook_point_t point) {
    keel_hook_stats_t s = {0};
    if (!reg) reg = g_legacy_registry;
    if (reg && point < KEEL_HOOK_POINT_COUNT) {
        s.fire_count  = reg->chains[point].fire_count;
        s.abort_count = reg->chains[point].abort_count;
        s.total_ns    = reg->chains[point].total_ns;
        s.hook_count  = reg->chains[point].count;
    }
    return s;
}
