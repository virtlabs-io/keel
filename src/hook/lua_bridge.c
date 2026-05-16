/**
 * @file lua_bridge.c
 * @brief Lua hook bridge, per-thread interpreter management, and context marshalling.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Uses Lua 5.4 C API to run hook scripts.  If Lua is not linked,
 * the weak symbol keel_lua_call_hook remains NULL and hooks are skipped.
 *
 * Thread-safety: each worker thread gets its own lua_State via
 * _Thread_local storage.  Scripts are loaded lazily and cached per-thread.
 *
 * Build with: -DKEEL_ENABLE_LUA=ON and link -llua -lm
 *
 * Bridge strategy:
 *   - Every worker thread lazily creates its own interpreter state. This avoids
 *     cross-thread locking inside Lua and lets scripts keep module globals
 *     without sharing mutable VM state across workers.
 *   - Scripts are loaded once per thread and tracked through the
 *     `__keel_loaded` cache table so the steady-state hook path only performs a
 *     global-function lookup plus context table marshalling.
 *   - The bridge uses a table-based ABI instead of exposing raw C structures to
 *     Lua. That is slower than direct userdata access but much easier to keep
 *     stable as the engine evolves and far safer for user scripts.
 */

#include "keel_hook.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel/sql/query_tree.h"

#include <string.h>

#ifdef KEEL_ENABLE_LUA

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <pthread.h>

/* ============================================================================
 * Thread-local Lua States
 *
 * Each worker thread lazily creates its own lua_State on the first hook call.
 * All states are tracked in g_all_states[] so keel_lua_shutdown() can close them.
 * ============================================================================ */

#define MAX_LUA_STATES 128

static pthread_mutex_t  g_states_lock = PTHREAD_MUTEX_INITIALIZER;
static lua_State*       g_all_states[MAX_LUA_STATES];
static size_t           g_num_states = 0;
static bool             g_lua_enabled = false;   /* set by keel_lua_init() */

static _Thread_local lua_State* tl_lua = NULL;

/* ============================================================================
 * keel.log(msg) — callable from Lua scripts
 * ============================================================================ */

/**
 * @brief C callback implementing <tt>keel.log(msg)</tt> for Lua scripts.
 *
 * @param L Lua state; expects one string argument on the stack.
 * @return 0 (no return values pushed to Lua).
 */
static int keel_log_from_lua(lua_State* L) {
    const char* msg = luaL_checkstring(L, 1);
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Lua: %s", msg);
    return 0;  /* no return values */
}

/* ============================================================================
 * Register the 'keel' global table into a Lua state
 * ============================================================================ */

/**
 * @brief Register the <tt>keel</tt> global table and its constants into a Lua state.
 *
 * Adds routing constants (ROUTE_PRIMARY, ROUTE_REPLICA, ROUTE_ANY) and the
 * @c keel.log() function to the provided interpreter.
 *
 * @param L Lua state to initialise.
 */
static void register_keel_globals(lua_State* L) {
    lua_newtable(L);

    /* keel.ROUTE_PRIMARY / ROUTE_REPLICA / ROUTE_ANY */
    lua_pushinteger(L, KEEL_HOOK_ROUTE_PRIMARY);
    lua_setfield(L, -2, "ROUTE_PRIMARY");
    lua_pushinteger(L, KEEL_HOOK_ROUTE_REPLICA);
    lua_setfield(L, -2, "ROUTE_REPLICA");
    lua_pushinteger(L, KEEL_HOOK_ROUTE_ANY);
    lua_setfield(L, -2, "ROUTE_ANY");

    /* keel.log(msg) */
    lua_pushcfunction(L, keel_log_from_lua);
    lua_setfield(L, -2, "log");

    lua_setglobal(L, "keel");
}

/* ============================================================================
 * Per-thread Lua state — lazy initialization
 * ============================================================================ */

/**
 * @brief Lazily create or retrieve the calling thread's Lua interpreter.
 *
 * @return Thread-local Lua state, or `NULL` if Lua support is disabled or allocation fails.
 */
static lua_State* get_thread_lua(void) {
    if (tl_lua) return tl_lua;
    if (!g_lua_enabled) return NULL;

    lua_State* L = luaL_newstate();
    if (!L) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "Lua: failed to create per-thread state (out of memory)");
        return NULL;
    }

    luaL_openlibs(L);
    register_keel_globals(L);

    /* Create __keel_loaded table for script caching */
    lua_newtable(L);
    lua_setglobal(L, "__keel_loaded");

    /* Track for shutdown */
    pthread_mutex_lock(&g_states_lock);
    if (g_num_states < MAX_LUA_STATES) {
        g_all_states[g_num_states++] = L;
    } else {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                      "Lua: too many thread-local states (%d max), "
                      "this state will leak on shutdown", MAX_LUA_STATES);
    }
    pthread_mutex_unlock(&g_states_lock);

    tl_lua = L;

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "Lua: per-thread state created (Lua %s)", LUA_VERSION);
    return L;
}

/* ============================================================================
 * Ensure a script file is loaded (once per thread)
 * ============================================================================ */

/**
 * @brief Ensure a Lua script file has been loaded into one thread-local state.
 *
 * @param L Thread-local Lua state.
 * @param file Path to the script file.
 * @return `true` if the script is ready for use.
 */
static bool ensure_script_loaded(lua_State* L, const char* file) {
    lua_getglobal(L, "__keel_loaded");
    lua_getfield(L, -1, file);
    bool loaded = lua_toboolean(L, -1);
    lua_pop(L, 1);  /* pop the boolean */

    if (!loaded) {
        int err = luaL_dofile(L, file);
        if (err) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                           "Lua: failed to load '%s': %s",
                           file, lua_tostring(L, -1));
            lua_pop(L, 2);  /* pop error + __keel_loaded */
            return false;
        }

        /* Mark as loaded */
        lua_pushboolean(L, 1);
        lua_setfield(L, -2, file);
    }

    lua_pop(L, 1);  /* pop __keel_loaded */
    return true;
}

/* ============================================================================
 * Push / Read Helper — keel_hook_ctx_t ↔ Lua table
 * ============================================================================ */

/**
 * @brief Marshal a hook context into a Lua table.
 *
 * @param L Lua state.
 * @param ctx Hook context to expose to the script.
 * @return
 */
static void push_ctx_to_lua(lua_State* L, keel_hook_ctx_t* ctx) {
    lua_newtable(L);

    /* Read-only session info */
    lua_pushinteger(L, (lua_Integer)ctx->session_id);
    lua_setfield(L, -2, "session_id");

    lua_pushstring(L, ctx->username ? ctx->username : "");
    lua_setfield(L, -2, "username");

    lua_pushstring(L, ctx->database ? ctx->database : "");
    lua_setfield(L, -2, "database");

    lua_pushinteger(L, ctx->client_fd);
    lua_setfield(L, -2, "client_fd");

    lua_pushinteger(L, ctx->server_fd);
    lua_setfield(L, -2, "server_fd");

    lua_pushboolean(L, ctx->in_transaction);
    lua_setfield(L, -2, "in_transaction");

    lua_pushinteger(L, (lua_Integer)ctx->query_count);
    lua_setfield(L, -2, "query_count");

    /* Query data */
    if (ctx->sql_text && ctx->sql_text_len > 0) {
        lua_pushlstring(L, ctx->sql_text, ctx->sql_text_len);
    } else {
        lua_pushstring(L, "");
    }
    lua_setfield(L, -2, "sql_text");

    /* Classification */
    lua_pushinteger(L, (lua_Integer)ctx->query_type);
    lua_setfield(L, -2, "query_type");

    lua_pushinteger(L, (lua_Integer)ctx->query_flags);
    lua_setfield(L, -2, "query_flags");

    lua_pushinteger(L, (lua_Integer)ctx->effect_flags);
    lua_setfield(L, -2, "effect_flags");

    lua_pushboolean(L, ctx->needs_primary);
    lua_setfield(L, -2, "needs_primary");

    /* Routing */
    lua_pushinteger(L, (lua_Integer)ctx->route_hint);
    lua_setfield(L, -2, "route_hint");

    lua_pushboolean(L, ctx->splice_eligible);
    lua_setfield(L, -2, "splice_eligible");

    /* Payload info (for BEFORE_SEND hooks) */
    lua_pushinteger(L, (lua_Integer)ctx->be_payload_len);
    lua_setfield(L, -2, "be_payload_len");

    lua_pushinteger(L, (lua_Integer)ctx->raw_query_len);
    lua_setfield(L, -2, "raw_query_len");

    /* Hook point identifier */
    lua_pushinteger(L, (lua_Integer)ctx->hook_point);
    lua_setfield(L, -2, "hook_point");

    /* Constants for route_hint */
    lua_pushinteger(L, KEEL_HOOK_ROUTE_PRIMARY);
    lua_setfield(L, -2, "ROUTE_PRIMARY");

    lua_pushinteger(L, KEEL_HOOK_ROUTE_REPLICA);
    lua_setfield(L, -2, "ROUTE_REPLICA");

    lua_pushinteger(L, KEEL_HOOK_ROUTE_ANY);
    lua_setfield(L, -2, "ROUTE_ANY");

    /* ---- Parsed query tree (available from AFTER_QUERY_PARSE onward) ---- */
    if (ctx->query_tree) {
        const keel_qt_query_t* qt = ctx->query_tree;
        lua_newtable(L);

        lua_pushinteger(L, (lua_Integer)qt->operation);
        lua_setfield(L, -2, "operation");

        lua_pushinteger(L, (lua_Integer)qt->flags);
        lua_setfield(L, -2, "flags");

        lua_pushinteger(L, (lua_Integer)qt->table_count);
        lua_setfield(L, -2, "table_count");

        /* tables[] — linked list iterated into a 1-based Lua array */
        lua_newtable(L);
        {
            int idx = 1;
            const keel_qt_table_ref_t* tr = qt->tables;
            while (tr) {
                lua_newtable(L);
                lua_pushlstring(L, tr->table.data ? tr->table.data : "",
                                tr->table.data ? tr->table.len : 0);
                lua_setfield(L, -2, "name");
                lua_pushlstring(L, tr->schema.data ? tr->schema.data : "",
                                tr->schema.data ? tr->schema.len : 0);
                lua_setfield(L, -2, "schema");
                lua_pushlstring(L, tr->alias.data ? tr->alias.data : "",
                                tr->alias.data ? tr->alias.len : 0);
                lua_setfield(L, -2, "alias");
                lua_rawseti(L, -2, idx++);
                tr = tr->next;
            }
        }
        lua_setfield(L, -2, "tables");

        lua_pushinteger(L, (lua_Integer)qt->column_count);
        lua_setfield(L, -2, "column_count");

        /* columns[] — linked list iterated into a 1-based Lua array */
        lua_newtable(L);
        {
            int idx = 1;
            const keel_qt_column_ref_t* cr = qt->columns;
            while (cr) {
                lua_newtable(L);
                lua_pushlstring(L, cr->column.data ? cr->column.data : "",
                                cr->column.data ? cr->column.len : 0);
                lua_setfield(L, -2, "name");
                lua_pushlstring(L, cr->table.data ? cr->table.data : "",
                                cr->table.data ? cr->table.len : 0);
                lua_setfield(L, -2, "table_ref");
                lua_rawseti(L, -2, idx++);
                cr = cr->next;
            }
        }
        lua_setfield(L, -2, "columns");

        /* target_table for DML */
        if (qt->target_table) {
            lua_pushlstring(L, qt->target_table->table.data ? qt->target_table->table.data : "",
                            qt->target_table->table.data ? qt->target_table->table.len : 0);
        } else {
            lua_pushnil(L);
        }
        lua_setfield(L, -2, "target_table");

        /* stmt_name for prepared statements */
        if (qt->stmt_name.data && qt->stmt_name.len > 0) {
            lua_pushlstring(L, qt->stmt_name.data, qt->stmt_name.len);
        } else {
            lua_pushstring(L, "");
        }
        lua_setfield(L, -2, "stmt_name");

        lua_setfield(L, -2, "query_tree");
    } else {
        lua_pushnil(L);
        lua_setfield(L, -2, "query_tree");
    }

    /* ---- Shard context (AFTER_ROUTE / BEFORE_SCATTER / AFTER_SCATTER) ---- */
    if (ctx->ext && (ctx->hook_point == KEEL_HOOK_AFTER_ROUTE ||
                     ctx->hook_point == KEEL_HOOK_BEFORE_SCATTER ||
                     ctx->hook_point == KEEL_HOOK_AFTER_SCATTER)) {
        const keel_hook_shard_ctx_t* sc = (const keel_hook_shard_ctx_t*)ctx->ext;
        lua_newtable(L);

        lua_pushinteger(L, (lua_Integer)sc->dispatch_kind);
        lua_setfield(L, -2, "dispatch_kind");

        lua_pushinteger(L, (lua_Integer)sc->shard_count);
        lua_setfield(L, -2, "shard_count");

        /* shards[] */
        lua_newtable(L);
        size_t sc_count = sc->shard_count < KEEL_HOOK_MAX_SHARDS
                        ? sc->shard_count : KEEL_HOOK_MAX_SHARDS;
        for (size_t i = 0; i < sc_count; i++) {
            const keel_hook_shard_info_t* si = &sc->shards[i];
            lua_newtable(L);
            lua_pushinteger(L, (lua_Integer)si->shard_index);
            lua_setfield(L, -2, "shard_index");
            lua_pushboolean(L, si->is_write);
            lua_setfield(L, -2, "is_write");
            lua_pushboolean(L, si->is_healthy);
            lua_setfield(L, -2, "is_healthy");
            lua_pushstring(L, si->host ? si->host : "");
            lua_setfield(L, -2, "host");
            lua_pushinteger(L, si->port);
            lua_setfield(L, -2, "port");
            lua_rawseti(L, -2, (lua_Integer)(i + 1));
        }
        lua_setfield(L, -2, "shards");

        lua_pushboolean(L, sc->requires_merge);
        lua_setfield(L, -2, "requires_merge");

        lua_pushinteger(L, (lua_Integer)sc->norder_keys);
        lua_setfield(L, -2, "norder_keys");

        lua_pushinteger(L, (lua_Integer)sc->limit_count);
        lua_setfield(L, -2, "limit_count");

        lua_pushinteger(L, (lua_Integer)sc->limit_offset);
        lua_setfield(L, -2, "limit_offset");

        lua_pushinteger(L, (lua_Integer)sc->nagg_specs);
        lua_setfield(L, -2, "nagg_specs");

        lua_pushinteger(L, (lua_Integer)sc->ngroup_key_cols);
        lua_setfield(L, -2, "ngroup_key_cols");

        lua_pushboolean(L, sc->twopc_required);
        lua_setfield(L, -2, "twopc_required");

        lua_pushinteger(L, (lua_Integer)sc->scatter_rows_merged);
        lua_setfield(L, -2, "scatter_rows_merged");

        lua_pushboolean(L, sc->scatter_spilled);
        lua_setfield(L, -2, "scatter_spilled");

        lua_pushinteger(L, (lua_Integer)sc->scatter_elapsed_us);
        lua_setfield(L, -2, "scatter_elapsed_us");

        /* [mutable] veto fields */
        lua_pushboolean(L, sc->veto_execution);
        lua_setfield(L, -2, "veto_execution");

        lua_pushstring(L, sc->veto_reason);
        lua_setfield(L, -2, "veto_reason");

        lua_setfield(L, -2, "shard_ctx");
    } else {
        lua_pushnil(L);
        lua_setfield(L, -2, "shard_ctx");
    }

    /* ---- Health context (ON_HEALTH_CHANGE) ---- */
    if (ctx->ext && ctx->hook_point == KEEL_HOOK_ON_HEALTH_CHANGE) {
        const keel_hook_health_ctx_t* hc = (const keel_hook_health_ctx_t*)ctx->ext;
        lua_newtable(L);

        lua_pushstring(L, hc->host ? hc->host : "");
        lua_setfield(L, -2, "host");

        lua_pushinteger(L, hc->port);
        lua_setfield(L, -2, "port");

        lua_pushinteger(L, (lua_Integer)hc->shard_index);
        lua_setfield(L, -2, "shard_index");

        lua_pushboolean(L, hc->is_primary);
        lua_setfield(L, -2, "is_primary");

        lua_pushinteger(L, hc->prev_health);
        lua_setfield(L, -2, "prev_health");

        lua_pushinteger(L, hc->curr_health);
        lua_setfield(L, -2, "curr_health");

        lua_pushstring(L, hc->prev_health_str ? hc->prev_health_str : "");
        lua_setfield(L, -2, "prev_health_str");

        lua_pushstring(L, hc->curr_health_str ? hc->curr_health_str : "");
        lua_setfield(L, -2, "curr_health_str");

        lua_pushinteger(L, (lua_Integer)hc->probe_latency_us);
        lua_setfield(L, -2, "probe_latency_us");

        lua_pushstring(L, hc->error_detail ? hc->error_detail : "");
        lua_setfield(L, -2, "error_detail");

        /* [mutable] */
        lua_pushboolean(L, hc->suppress_log);
        lua_setfield(L, -2, "suppress_log");

        lua_setfield(L, -2, "health_ctx");
    } else {
        lua_pushnil(L);
        lua_setfield(L, -2, "health_ctx");
    }
}

/**
 * @brief Read back mutable hook-context fields from a Lua table.
 *
 * This is effectively an output-parameter decode step: the Lua script updates
 * table fields and the bridge copies only the fields the engine treats as
 * mutable outputs.
 *
 * @param L Lua state with the context table on top of the stack.
 * @param[in,out] ctx Hook context receiving script-modified outputs.
 * @return
 */
static void read_ctx_from_lua(lua_State* L, keel_hook_ctx_t* ctx) {
    /* Read mutable fields back from Lua table (at top of stack) */

    lua_getfield(L, -1, "route_hint");
    if (lua_isinteger(L, -1))
        ctx->route_hint = (keel_hook_route_t)lua_tointeger(L, -1);
    lua_pop(L, 1);

    lua_getfield(L, -1, "needs_primary");
    if (lua_isboolean(L, -1))
        ctx->needs_primary = lua_toboolean(L, -1);
    lua_pop(L, 1);

    lua_getfield(L, -1, "splice_eligible");
    if (lua_isboolean(L, -1))
        ctx->splice_eligible = lua_toboolean(L, -1);
    lua_pop(L, 1);

    lua_getfield(L, -1, "effect_flags");
    if (lua_isinteger(L, -1))
        ctx->effect_flags = (uint32_t)lua_tointeger(L, -1);
    lua_pop(L, 1);

    lua_getfield(L, -1, "error_msg");
    if (lua_isstring(L, -1)) {
        const char* msg = lua_tostring(L, -1);
        strncpy(ctx->error_msg, msg, sizeof(ctx->error_msg) - 1);
    }
    lua_pop(L, 1);

    /* Read back mutable shard_ctx fields (veto_execution / veto_reason) */
    if (ctx->ext && (ctx->hook_point == KEEL_HOOK_AFTER_ROUTE ||
                     ctx->hook_point == KEEL_HOOK_BEFORE_SCATTER ||
                     ctx->hook_point == KEEL_HOOK_AFTER_SCATTER)) {
        keel_hook_shard_ctx_t* sc = (keel_hook_shard_ctx_t*)ctx->ext;
        lua_getfield(L, -1, "shard_ctx");
        if (lua_istable(L, -1)) {
            lua_getfield(L, -1, "veto_execution");
            if (lua_isboolean(L, -1))
                sc->veto_execution = lua_toboolean(L, -1);
            lua_pop(L, 1);

            lua_getfield(L, -1, "veto_reason");
            if (lua_isstring(L, -1)) {
                const char* r = lua_tostring(L, -1);
                strncpy(sc->veto_reason, r, sizeof(sc->veto_reason) - 1);
            }
            lua_pop(L, 1);
        }
        lua_pop(L, 1); /* pop shard_ctx */
    }

    /* Read back mutable health_ctx fields (suppress_log) */
    if (ctx->ext && ctx->hook_point == KEEL_HOOK_ON_HEALTH_CHANGE) {
        keel_hook_health_ctx_t* hc = (keel_hook_health_ctx_t*)ctx->ext;
        lua_getfield(L, -1, "health_ctx");
        if (lua_istable(L, -1)) {
            lua_getfield(L, -1, "suppress_log");
            if (lua_isboolean(L, -1))
                hc->suppress_log = lua_toboolean(L, -1);
            lua_pop(L, 1);
        }
        lua_pop(L, 1); /* pop health_ctx */
    }
}

/* ============================================================================
 * Lua Call — per-thread state, cached script loading
 * ============================================================================ */

/**
 * @brief Invoke one Lua hook function.
 *
 * The bridge loads the script lazily, resolves the named function, marshals
 * the context to a Lua table, and then interprets the result as either a
 * `(bool, table)` pair or a single boolean. The context pointer is an in/out
 * parameter because the script may write routing and policy outputs back into
 * the returned or mutated table.
 *
 * @param file Script path.
 * @param func Function name inside the script.
 * @param[in,out] ctx Hook context passed to and updated by the script.
 * @return `true` to continue query processing, `false` to abort.
 */
bool keel_lua_call_hook(const char* file, const char* func,
                        keel_hook_ctx_t* ctx) {
    lua_State* L = get_thread_lua();
    if (!L) return true;  /* Lua not available — pass through */

    /* Load script (once per thread) */
    if (!ensure_script_loaded(L, file)) {
        return true;  /* Don't abort on load failure */
    }

    /* Get the function */
    lua_getglobal(L, func);
    if (!lua_isfunction(L, -1)) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "Lua: '%s' is not a function in '%s'",
                       func, file);
        lua_pop(L, 1);
        return true;
    }

    /* Push context table */
    push_ctx_to_lua(L, ctx);

    /* Call: 1 argument (ctx table), 2 returns (bool, ctx table) */
    int err = lua_pcall(L, 1, 2, 0);
    if (err) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "Lua: hook %s.%s() error: %s",
                       file, func, lua_tostring(L, -1));
        lua_pop(L, 1);
        return true;
    }

    /* Read result */
    bool result = true;
    if (lua_isboolean(L, -2)) {
        result = lua_toboolean(L, -2);
    }

    /* Read back modified context */
    if (lua_istable(L, -1)) {
        read_ctx_from_lua(L, ctx);
    }
    lua_pop(L, 2);

    return result;
}

/* ============================================================================
 * Lua Subsystem Lifecycle
 * ============================================================================ */

/**
 * @brief Enable Lua hook support for subsequent per-thread lazy initialization.
 *
 * @return `KEEL_OK`.
 */
keel_error_t keel_lua_init(void) {
    g_lua_enabled = true;
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                  "Lua: hook support enabled (Lua %s, per-thread states)",
                  LUA_VERSION);
    return KEEL_OK;
}

/**
 * @brief Shutdown the Lua bridge and close all tracked thread-local states.
 *
 * @return
 */
void keel_lua_shutdown(void) {
    g_lua_enabled = false;

    pthread_mutex_lock(&g_states_lock);
    for (size_t i = 0; i < g_num_states; i++) {
        if (g_all_states[i]) {
            lua_close(g_all_states[i]);
            g_all_states[i] = NULL;
        }
    }
    size_t closed = g_num_states;
    g_num_states = 0;
    pthread_mutex_unlock(&g_states_lock);

    /* Reset thread-local pointer for the calling thread */
    tl_lua = NULL;

    if (closed > 0) {
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                      "Lua: shutdown (%zu thread state(s) closed)", closed);
    }
}

/** @brief Return whether Lua hook support is compiled in.
 *  @return `true` when Lua is available.
 */
bool keel_lua_available(void) { return true; }

#else /* !KEEL_ENABLE_LUA */

/* Stubs when Lua is not compiled in */
/** @brief No-op stub: Lua not compiled in; always returns `true` (pass-through). */
bool keel_lua_call_hook(const char* file, const char* func,
                        keel_hook_ctx_t* ctx) {
    (void)file; (void)func; (void)ctx;
    return true;
}

/** @brief Stub: returns `false` when Lua is not compiled in. */
bool keel_lua_available(void) { return false; }
/** @brief Stub: no-op init when Lua is not compiled in; returns `KEEL_OK`. */
keel_error_t keel_lua_init(void)  { return KEEL_OK; }
/** @brief Stub: no-op shutdown when Lua is not compiled in. */
void         keel_lua_shutdown(void) {}

#endif /* KEEL_ENABLE_LUA */
