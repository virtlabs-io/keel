/**
 * @file python_bridge.c
 * @brief Python hook bridge, CPython lifecycle control, and context marshalling.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Uses the Python/C API to run hook callables.  If Python is not linked,
 * the weak symbol keel_python_call_hook remains NULL and hooks are skipped.
 *
 * Build with: -DKEEL_ENABLE_PYTHON=ON and link against libpython3.x
 *
 * Design notes:
 *   - CPython is process-global, so unlike the Lua bridge this file does not
 *     create one interpreter per worker. Instead it relies on the GIL and
 *     caches imported modules to reduce per-hook overhead.
 *   - The bridge converts `keel_hook_ctx_t` into a Python dict rather than a
 *     custom extension type. That is slower than a bespoke object API but much
 *     easier to keep source-compatible for user scripts.
 *   - Cached-module and import-failure flags are explicit output parameters of
 *     the bridge API. They let the caller retain per-hook import state without
 *     pushing that caching policy into a global registry.
 */

#include "keel_hook.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel/sql/query_tree.h"

#include <string.h>

#ifdef KEEL_ENABLE_PYTHON

/* Must define PY_SSIZE_T_CLEAN before Python.h */
#define PY_SSIZE_T_CLEAN
#include <Python.h>

/* ============================================================================
 * Module State
 * ============================================================================ */

static bool g_python_initialized = false;
static PyThreadState* g_main_thread_state = NULL;  /* saved by init, restored by shutdown */

/* ============================================================================
 * Context ↔ Python dict
 * ============================================================================ */

/**
 * @brief Convert a hook context into a Python dictionary.
 *
 * @param ctx Hook context to expose to Python.
 * @return New Python dict object, or `NULL` on allocation failure.
 */
static PyObject* ctx_to_pydict(keel_hook_ctx_t* ctx) {
    PyObject* d = PyDict_New();
    if (!d) return NULL;

    PyDict_SetItemString(d, "session_id",
        PyLong_FromUnsignedLongLong(ctx->session_id));
    PyDict_SetItemString(d, "username",
        PyUnicode_FromString(ctx->username ? ctx->username : ""));
    PyDict_SetItemString(d, "database",
        PyUnicode_FromString(ctx->database ? ctx->database : ""));
    PyDict_SetItemString(d, "client_fd",
        PyLong_FromLong(ctx->client_fd));
    PyDict_SetItemString(d, "server_fd",
        PyLong_FromLong(ctx->server_fd));
    PyDict_SetItemString(d, "in_transaction",
        PyBool_FromLong(ctx->in_transaction));
    PyDict_SetItemString(d, "query_count",
        PyLong_FromUnsignedLong(ctx->query_count));

    if (ctx->sql_text && ctx->sql_text_len > 0)
        PyDict_SetItemString(d, "sql_text",
            PyUnicode_FromStringAndSize(ctx->sql_text, (Py_ssize_t)ctx->sql_text_len));
    else
        PyDict_SetItemString(d, "sql_text", PyUnicode_FromString(""));

    PyDict_SetItemString(d, "query_type",
        PyLong_FromUnsignedLong(ctx->query_type));
    PyDict_SetItemString(d, "query_flags",
        PyLong_FromUnsignedLong(ctx->query_flags));
    PyDict_SetItemString(d, "effect_flags",
        PyLong_FromUnsignedLong(ctx->effect_flags));
    PyDict_SetItemString(d, "needs_primary",
        PyBool_FromLong(ctx->needs_primary));
    PyDict_SetItemString(d, "route_hint",
        PyLong_FromLong((long)ctx->route_hint));
    PyDict_SetItemString(d, "splice_eligible",
        PyBool_FromLong(ctx->splice_eligible));
    PyDict_SetItemString(d, "be_payload_len",
        PyLong_FromSize_t(ctx->be_payload_len));
    PyDict_SetItemString(d, "raw_query_len",
        PyLong_FromSize_t(ctx->raw_query_len));
    PyDict_SetItemString(d, "hook_point",
        PyLong_FromLong((long)ctx->hook_point));

    /* Constants */
    PyDict_SetItemString(d, "ROUTE_PRIMARY",
        PyLong_FromLong(KEEL_HOOK_ROUTE_PRIMARY));
    PyDict_SetItemString(d, "ROUTE_REPLICA",
        PyLong_FromLong(KEEL_HOOK_ROUTE_REPLICA));
    PyDict_SetItemString(d, "ROUTE_ANY",
        PyLong_FromLong(KEEL_HOOK_ROUTE_ANY));

    /* ---- Parsed query tree ---- */
    if (ctx->query_tree) {
        const keel_qt_query_t* qt = ctx->query_tree;
        PyObject* qtd = PyDict_New();
        if (qtd) {
            PyDict_SetItemString(qtd, "operation",
                PyLong_FromLong((long)qt->operation));
            PyDict_SetItemString(qtd, "flags",
                PyLong_FromUnsignedLong(qt->flags));
            PyDict_SetItemString(qtd, "table_count",
                PyLong_FromSize_t(qt->table_count));

            /* tables list */
            PyObject* tlist = PyList_New(0);
            if (tlist) {
                const keel_qt_table_ref_t* tr = qt->tables;
                while (tr) {
                    PyObject* td = PyDict_New();
                    if (td) {
                        PyDict_SetItemString(td, "name",
                            PyUnicode_FromStringAndSize(
                                tr->table.data ? tr->table.data : "",
                                (Py_ssize_t)(tr->table.data ? tr->table.len : 0)));
                        PyDict_SetItemString(td, "schema",
                            PyUnicode_FromStringAndSize(
                                tr->schema.data ? tr->schema.data : "",
                                (Py_ssize_t)(tr->schema.data ? tr->schema.len : 0)));
                        PyDict_SetItemString(td, "alias",
                            PyUnicode_FromStringAndSize(
                                tr->alias.data ? tr->alias.data : "",
                                (Py_ssize_t)(tr->alias.data ? tr->alias.len : 0)));
                        PyList_Append(tlist, td);
                        Py_DECREF(td);
                    }
                    tr = tr->next;
                }
                PyDict_SetItemString(qtd, "tables", tlist);
                Py_DECREF(tlist);
            }

            PyDict_SetItemString(qtd, "column_count",
                PyLong_FromSize_t(qt->column_count));

            /* columns list */
            PyObject* clist = PyList_New(0);
            if (clist) {
                const keel_qt_column_ref_t* cr = qt->columns;
                while (cr) {
                    PyObject* cd = PyDict_New();
                    if (cd) {
                        PyDict_SetItemString(cd, "name",
                            PyUnicode_FromStringAndSize(
                                cr->column.data ? cr->column.data : "",
                                (Py_ssize_t)(cr->column.data ? cr->column.len : 0)));
                        PyDict_SetItemString(cd, "table_ref",
                            PyUnicode_FromStringAndSize(
                                cr->table.data ? cr->table.data : "",
                                (Py_ssize_t)(cr->table.data ? cr->table.len : 0)));
                        PyList_Append(clist, cd);
                        Py_DECREF(cd);
                    }
                    cr = cr->next;
                }
                PyDict_SetItemString(qtd, "columns", clist);
                Py_DECREF(clist);
            }

            /* target_table */
            if (qt->target_table && qt->target_table->table.data) {
                PyDict_SetItemString(qtd, "target_table",
                    PyUnicode_FromStringAndSize(qt->target_table->table.data,
                                                (Py_ssize_t)qt->target_table->table.len));
            } else {
                PyDict_SetItemString(qtd, "target_table", Py_None);
                Py_INCREF(Py_None);
            }

            /* stmt_name */
            PyDict_SetItemString(qtd, "stmt_name",
                PyUnicode_FromStringAndSize(
                    qt->stmt_name.data ? qt->stmt_name.data : "",
                    (Py_ssize_t)(qt->stmt_name.data ? qt->stmt_name.len : 0)));

            PyDict_SetItemString(d, "query_tree", qtd);
            Py_DECREF(qtd);
        }
    } else {
        PyDict_SetItemString(d, "query_tree", Py_None);
        Py_INCREF(Py_None);
    }

    /* ---- Shard context (AFTER_ROUTE / BEFORE_SCATTER / AFTER_SCATTER) ---- */
    if (ctx->ext && (ctx->hook_point == KEEL_HOOK_AFTER_ROUTE ||
                     ctx->hook_point == KEEL_HOOK_BEFORE_SCATTER ||
                     ctx->hook_point == KEEL_HOOK_AFTER_SCATTER)) {
        const keel_hook_shard_ctx_t* sc = (const keel_hook_shard_ctx_t*)ctx->ext;
        PyObject* sd = PyDict_New();
        if (sd) {
            PyDict_SetItemString(sd, "dispatch_kind",
                PyLong_FromLong((long)sc->dispatch_kind));
            PyDict_SetItemString(sd, "shard_count",
                PyLong_FromSize_t(sc->shard_count));

            /* shards list */
            PyObject* slist = PyList_New(0);
            if (slist) {
                size_t sc_count = sc->shard_count < KEEL_HOOK_MAX_SHARDS
                                ? sc->shard_count : KEEL_HOOK_MAX_SHARDS;
                for (size_t i = 0; i < sc_count; i++) {
                    const keel_hook_shard_info_t* si = &sc->shards[i];
                    PyObject* sid = PyDict_New();
                    if (sid) {
                        PyDict_SetItemString(sid, "shard_index",
                            PyLong_FromSize_t(si->shard_index));
                        PyDict_SetItemString(sid, "is_write",
                            PyBool_FromLong(si->is_write));
                        PyDict_SetItemString(sid, "is_healthy",
                            PyBool_FromLong(si->is_healthy));
                        PyDict_SetItemString(sid, "host",
                            PyUnicode_FromString(si->host ? si->host : ""));
                        PyDict_SetItemString(sid, "port",
                            PyLong_FromLong(si->port));
                        PyList_Append(slist, sid);
                        Py_DECREF(sid);
                    }
                }
                PyDict_SetItemString(sd, "shards", slist);
                Py_DECREF(slist);
            }

            PyDict_SetItemString(sd, "requires_merge",
                PyBool_FromLong(sc->requires_merge));
            PyDict_SetItemString(sd, "norder_keys",
                PyLong_FromUnsignedLong(sc->norder_keys));
            PyDict_SetItemString(sd, "limit_count",
                PyLong_FromUnsignedLongLong(sc->limit_count));
            PyDict_SetItemString(sd, "limit_offset",
                PyLong_FromUnsignedLongLong(sc->limit_offset));
            PyDict_SetItemString(sd, "nagg_specs",
                PyLong_FromUnsignedLong(sc->nagg_specs));
            PyDict_SetItemString(sd, "ngroup_key_cols",
                PyLong_FromUnsignedLong(sc->ngroup_key_cols));
            PyDict_SetItemString(sd, "twopc_required",
                PyBool_FromLong(sc->twopc_required));
            PyDict_SetItemString(sd, "scatter_rows_merged",
                PyLong_FromUnsignedLongLong(sc->scatter_rows_merged));
            PyDict_SetItemString(sd, "scatter_spilled",
                PyBool_FromLong(sc->scatter_spilled));
            PyDict_SetItemString(sd, "scatter_elapsed_us",
                PyLong_FromUnsignedLongLong(sc->scatter_elapsed_us));
            PyDict_SetItemString(sd, "veto_execution",
                PyBool_FromLong(sc->veto_execution));
            PyDict_SetItemString(sd, "veto_reason",
                PyUnicode_FromString(sc->veto_reason));

            PyDict_SetItemString(d, "shard_ctx", sd);
            Py_DECREF(sd);
        }
    } else {
        PyDict_SetItemString(d, "shard_ctx", Py_None);
        Py_INCREF(Py_None);
    }

    /* ---- Health context (ON_HEALTH_CHANGE) ---- */
    if (ctx->ext && ctx->hook_point == KEEL_HOOK_ON_HEALTH_CHANGE) {
        const keel_hook_health_ctx_t* hc = (const keel_hook_health_ctx_t*)ctx->ext;
        PyObject* hd = PyDict_New();
        if (hd) {
            PyDict_SetItemString(hd, "host",
                PyUnicode_FromString(hc->host ? hc->host : ""));
            PyDict_SetItemString(hd, "port",
                PyLong_FromLong(hc->port));
            PyDict_SetItemString(hd, "shard_index",
                PyLong_FromSize_t(hc->shard_index));
            PyDict_SetItemString(hd, "is_primary",
                PyBool_FromLong(hc->is_primary));
            PyDict_SetItemString(hd, "prev_health",
                PyLong_FromLong(hc->prev_health));
            PyDict_SetItemString(hd, "curr_health",
                PyLong_FromLong(hc->curr_health));
            PyDict_SetItemString(hd, "prev_health_str",
                PyUnicode_FromString(hc->prev_health_str ? hc->prev_health_str : ""));
            PyDict_SetItemString(hd, "curr_health_str",
                PyUnicode_FromString(hc->curr_health_str ? hc->curr_health_str : ""));
            PyDict_SetItemString(hd, "probe_latency_us",
                PyLong_FromUnsignedLongLong(hc->probe_latency_us));
            PyDict_SetItemString(hd, "error_detail",
                PyUnicode_FromString(hc->error_detail ? hc->error_detail : ""));
            PyDict_SetItemString(hd, "suppress_log",
                PyBool_FromLong(hc->suppress_log));

            PyDict_SetItemString(d, "health_ctx", hd);
            Py_DECREF(hd);
        }
    } else {
        PyDict_SetItemString(d, "health_ctx", Py_None);
        Py_INCREF(Py_None);
    }

    return d;
}

/**
 * @brief Copy mutable output fields from a Python dict back into the hook context.
 *
 * @param d Python dictionary returned or mutated by the hook.
 * @param[in,out] ctx Hook context receiving Python-produced outputs.
 * @return
 */
static void pydict_to_ctx(PyObject* d, keel_hook_ctx_t* ctx) {
    PyObject* val;

    val = PyDict_GetItemString(d, "route_hint");
    if (val && PyLong_Check(val))
        ctx->route_hint = (keel_hook_route_t)PyLong_AsLong(val);

    val = PyDict_GetItemString(d, "needs_primary");
    if (val)
        ctx->needs_primary = PyObject_IsTrue(val);

    val = PyDict_GetItemString(d, "splice_eligible");
    if (val)
        ctx->splice_eligible = PyObject_IsTrue(val);

    val = PyDict_GetItemString(d, "effect_flags");
    if (val && PyLong_Check(val))
        ctx->effect_flags = (uint32_t)PyLong_AsUnsignedLong(val);

    val = PyDict_GetItemString(d, "error_msg");
    if (val && PyUnicode_Check(val)) {
        const char* msg = PyUnicode_AsUTF8(val);
        if (msg)
            strncpy(ctx->error_msg, msg, sizeof(ctx->error_msg) - 1);
    }

    /* Read back mutable shard_ctx fields */
    if (ctx->ext && (ctx->hook_point == KEEL_HOOK_AFTER_ROUTE ||
                     ctx->hook_point == KEEL_HOOK_BEFORE_SCATTER ||
                     ctx->hook_point == KEEL_HOOK_AFTER_SCATTER)) {
        keel_hook_shard_ctx_t* sc = (keel_hook_shard_ctx_t*)ctx->ext;
        val = PyDict_GetItemString(d, "shard_ctx");
        if (val && PyDict_Check(val)) {
            PyObject* ve = PyDict_GetItemString(val, "veto_execution");
            if (ve)
                sc->veto_execution = PyObject_IsTrue(ve);
            PyObject* vr = PyDict_GetItemString(val, "veto_reason");
            if (vr && PyUnicode_Check(vr)) {
                const char* r = PyUnicode_AsUTF8(vr);
                if (r) strncpy(sc->veto_reason, r, sizeof(sc->veto_reason) - 1);
            }
        }
    }

    /* Read back mutable health_ctx fields */
    if (ctx->ext && ctx->hook_point == KEEL_HOOK_ON_HEALTH_CHANGE) {
        keel_hook_health_ctx_t* hc = (keel_hook_health_ctx_t*)ctx->ext;
        val = PyDict_GetItemString(d, "health_ctx");
        if (val && PyDict_Check(val)) {
            PyObject* sl = PyDict_GetItemString(val, "suppress_log");
            if (sl)
                hc->suppress_log = PyObject_IsTrue(sl);
        }
    }
}

/* ============================================================================
 * Python Call
 * ============================================================================ */

/**
 * @brief Invoke one Python hook function with module caching.
 *
 * `p_cached_module` and `p_import_failed` are output-style state channels owned
 * by the caller. The bridge updates them so repeated hook invocations avoid
 * redundant imports and repeated logging for known-broken modules. The hook
 * context itself is also an in/out parameter because Python code may alter
 * routing and policy fields before control returns to the engine.
 *
 * @param module_name Python module name.
 * @param func_name Python callable name inside the module.
 * @param[in,out] ctx Hook context to expose and update. `NULL` means import-only warmup.
 * @param[in,out] p_cached_module Caller-owned cached module slot.
 * @param[in,out] p_import_failed Caller-owned sticky failure flag.
 * @return `true` to continue processing, `false` to abort.
 */
bool keel_python_call_hook(const char* module_name, const char* func_name,
                           keel_hook_ctx_t* ctx,
                           void** p_cached_module,
                           bool*  p_import_failed) {
    if (!g_python_initialized) return true;

    /* Short-circuit without acquiring the GIL when this hook is known-broken.
     * This prevents log spam and avoids Python API overhead on every query. */
    if (*p_import_failed) return true;

    /* Acquire the GIL — required for any Python API call from worker threads */
    PyGILState_STATE gstate = PyGILState_Ensure();

    bool continue_processing = true;
    PyObject* mod  = NULL;
    PyObject* func = NULL;
    PyObject* ctx_dict = NULL;
    PyObject* args = NULL;
    PyObject* result = NULL;

    /* -----------------------------------------------------------------------
     * Module import / cache
     * ---------------------------------------------------------------------- */
    if (*p_cached_module != NULL) {
        /* Fast path: module already imported and cached */
        mod = (PyObject*)*p_cached_module;
        Py_INCREF(mod);  /* balance Py_XDECREF(mod) at done: */
    } else {
        mod = PyImport_ImportModule(module_name);
        if (!mod) {
            /* Capture the exception text so we can log it properly without
             * calling PyErr_Print() which writes directly to stderr and
             * bypasses the log mutex (causing interleaved output). */
            char exc_buf[512] = "unknown Python error";
            if (PyErr_Occurred()) {
                PyObject *etype = NULL, *evalue = NULL, *etb = NULL;
                PyErr_Fetch(&etype, &evalue, &etb);
                PyErr_NormalizeException(&etype, &evalue, &etb);
                if (evalue) {
                    PyObject* str = PyObject_Str(evalue);
                    if (str) {
                        const char* s = PyUnicode_AsUTF8(str);
                        if (s) snprintf(exc_buf, sizeof(exc_buf), "%s", s);
                        Py_DECREF(str);
                    }
                }
                Py_XDECREF(etype);
                Py_XDECREF(evalue);
                Py_XDECREF(etb);
            }
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                           "Python: cannot import '%s': %s  "
                           "(hook disabled — fix script path and restart)",
                           module_name, exc_buf);
            *p_import_failed = true;
            goto done;
        }
        /* Cache the module for all future calls (keep the +1 ref from import) */
        Py_INCREF(mod);          /* +1 for the cache */
        *p_cached_module = mod;  /* cache holds this reference */
        /* mod itself still holds the original +1 from ImportModule, balanced by
         * the Py_XDECREF(mod) at done: */
    }

    /* -----------------------------------------------------------------------
     * ctx == NULL means "pre-import / validation only" — skip hook invocation
     * ---------------------------------------------------------------------- */
    if (ctx == NULL) goto done;

    func = PyObject_GetAttrString(mod, func_name);
    if (!func || !PyCallable_Check(func)) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "Python: '%s.%s' is not callable",
                       module_name, func_name);
        goto done;
    }

    /* Build context dict */
    ctx_dict = ctx_to_pydict(ctx);
    if (!ctx_dict) goto done;

    /* Call: result = func(ctx_dict) */
    args = PyTuple_Pack(1, ctx_dict);
    result = PyObject_CallObject(func, args);

    if (!result) {
        /* Capture the exception rather than calling PyErr_Print() */
        char exc_buf[512] = "unknown exception";
        if (PyErr_Occurred()) {
            PyObject *etype = NULL, *evalue = NULL, *etb = NULL;
            PyErr_Fetch(&etype, &evalue, &etb);
            PyErr_NormalizeException(&etype, &evalue, &etb);
            if (evalue) {
                PyObject* str = PyObject_Str(evalue);
                if (str) {
                    const char* s = PyUnicode_AsUTF8(str);
                    if (s) snprintf(exc_buf, sizeof(exc_buf), "%s", s);
                    Py_DECREF(str);
                }
            }
            Py_XDECREF(etype);
            Py_XDECREF(evalue);
            Py_XDECREF(etb);
        }
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                       "Python: hook %s.%s() raised exception: %s",
                       module_name, func_name, exc_buf);
    } else if (PyTuple_Check(result) && PyTuple_Size(result) >= 2) {
        /* Expecting (bool, dict) */
        continue_processing = PyObject_IsTrue(PyTuple_GetItem(result, 0));
        PyObject* ret_dict = PyTuple_GetItem(result, 1);
        if (PyDict_Check(ret_dict))
            pydict_to_ctx(ret_dict, ctx);
    } else {
        /* Single bool return */
        continue_processing = PyObject_IsTrue(result);
        /* Read back from the ctx_dict (hooks may have modified it) */
        pydict_to_ctx(ctx_dict, ctx);
    }

done:
    Py_XDECREF(result);
    Py_XDECREF(args);
    Py_XDECREF(ctx_dict);
    Py_XDECREF(func);
    Py_XDECREF(mod);

    PyGILState_Release(gstate);
    return continue_processing;
}

/* Release a cached module reference. Called by keel_hook_registry_destroy
 * before keel_python_shutdown so the interpreter is still valid. */
/**
 * @brief Release one cached Python module reference held by a hook handle.
 *
 * @param mod Cached module pointer to decref.
 * @return
 */
void keel_python_decref_module(void* mod) {
    if (!mod || !g_python_initialized) return;
    PyGILState_STATE gstate = PyGILState_Ensure();
    Py_XDECREF((PyObject*)mod);
    PyGILState_Release(gstate);
}

/* ============================================================================
 * Python Subsystem Lifecycle
 * ============================================================================ */

/**
 * @brief Prepend a directory to Python's `sys.path`.
 *
 * @param dir Directory to prepend.
 * @return
 */
void keel_python_add_script_dir(const char* dir) {
    if (!dir || !dir[0] || !g_python_initialized) return;

    PyGILState_STATE gstate = PyGILState_Ensure();

    /* Escape any single quotes in the path */
    char escaped[512];
    size_t j = 0;
    for (size_t i = 0; dir[i] && j < sizeof(escaped) - 2; i++) {
        if (dir[i] == '\'') {
            if (j + 2 < sizeof(escaped)) {
                escaped[j++] = '\\';
                escaped[j++] = '\'';
            }
        } else {
            escaped[j++] = dir[i];
        }
    }
    escaped[j] = '\0';

    char cmd[768];
    snprintf(cmd, sizeof(cmd),
             "import sys\n"
             "d = '%s'\n"
             "if d not in sys.path: sys.path.insert(0, d)\n",
             escaped);
    PyRun_SimpleString(cmd);
    KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE,
                   "Python: added '%s' to sys.path", dir);

    PyGILState_Release(gstate);
}

/**
 * @brief Initialize the embedded CPython interpreter.
 *
 * @return `KEEL_OK` on success or an error code if initialization fails.
 */
keel_error_t keel_python_init(void) {
    if (g_python_initialized) return KEEL_OK;

    Py_Initialize();
    if (!Py_IsInitialized()) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Python: Py_Initialize() failed");
        return KEEL_ERR_NOT_INITIALIZED;
    }

    /* Add current directory to sys.path for user scripts */
    PyRun_SimpleString("import sys; sys.path.insert(0, '.')");

    g_python_initialized = true;
    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Python: interpreter initialized (%s)",
                  Py_GetVersion());

    /*
     * Release the GIL so that worker threads can acquire it via
     * PyGILState_Ensure().  Py_Initialize() leaves the GIL held by
     * the calling (main) thread; we must release it before any other
     * thread attempts to use the Python API.
     */
    g_main_thread_state = PyEval_SaveThread();

    return KEEL_OK;
}

/**
 * @brief Shutdown the embedded CPython interpreter.
 *
 * @return
 */
void keel_python_shutdown(void) {
    if (g_python_initialized) {
        /* Re-acquire the GIL on the main thread before finalizing */
        if (g_main_thread_state)
            PyEval_RestoreThread(g_main_thread_state);
        g_main_thread_state = NULL;
        Py_Finalize();
        g_python_initialized = false;
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Python: interpreter shutdown");
    }
}

/** @brief Return whether Python hook support is compiled in.
 *  @return `true` when Python is available.
 */
bool keel_python_available(void) { return true; }

#else /* !KEEL_ENABLE_PYTHON */

/* Stubs when Python is not compiled in */
/** @brief No-op stub: Python not compiled in; always returns `true` (pass-through). */
bool keel_python_call_hook(const char* module, const char* func,
                           keel_hook_ctx_t* ctx,
                           void** p_cached_module,
                           bool*  p_import_failed) {
    (void)module; (void)func; (void)ctx;
    (void)p_cached_module; (void)p_import_failed;
    return true;
}

/** @brief Stub: returns `false` when Python is not compiled in. */
bool keel_python_available(void) { return false; }
/** @brief Stub: no-op when Python is not compiled in. */
void keel_python_add_script_dir(const char* dir) { (void)dir; }
/** @brief Stub: no-op when Python is not compiled in. */
void keel_python_decref_module(void* mod) { (void)mod; }
/** @brief Stub: no-op init when Python is not compiled in; returns `KEEL_OK`. */
keel_error_t keel_python_init(void)  { return KEEL_OK; }
/** @brief Stub: no-op shutdown when Python is not compiled in. */
void         keel_python_shutdown(void) {}

#endif /* KEEL_ENABLE_PYTHON */
