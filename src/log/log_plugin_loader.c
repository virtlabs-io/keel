/**
 * @file log_plugin_loader.c
 * @brief Dynamic loading and unloading of log sink plugins.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * Loads a log plugin from a shared library at runtime. The library
 * must export a single constructor function:
 *
 *   keel_log_plugin_t* keel_log_plugin_create(void);
 *
 * The loader calls dlopen(), looks up the symbol, invokes it,
 * and returns the resulting plugin instance.
 *
 * The implementation keeps a tiny side table from plugin pointer to `dlopen`
 * handle so `keel_log_plugin_unload()` can later `dlclose()` the same object.
 * This avoids forcing every plugin implementation to expose loader-specific
 * state inside the public plugin struct.
 */

#include "keel/log/log_plugin.h"
#include "keel/log/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>

/* ============================================================================
 * Loader bookkeeping — we need to track the dlopen handle so we can
 * dlclose() it later. We stash it in a small wrapper.
 * ============================================================================ */

typedef struct {
    void*               dl_handle;   /**< dlopen() handle        */
    keel_log_plugin_t*   plugin;      /**< The plugin instance    */
} loaded_plugin_t;

/* We keep a small static table so that keel_log_plugin_unload() can
 * find the dl_handle that corresponds to a given plugin pointer.    */
#define MAX_LOADED_PLUGINS 8
static loaded_plugin_t s_loaded[MAX_LOADED_PLUGINS];
static size_t          s_loaded_count = 0;

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Load a log plugin from a shared library and call its constructor.
 *
 * @param path Shared-library path.
 * @return Plugin instance, or `NULL` on validation or loading failure.
 */
keel_log_plugin_t* keel_log_plugin_load(const char* path)
{
    if (!path || path[0] == '\0') {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG, "log_plugin_load: path is NULL or empty");
        return NULL;
    }

    /* Open the shared library */
    void* handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG, "log_plugin_load: dlopen(%s) failed: %s",
                path, dlerror());
        return NULL;
    }

    /* Look up the constructor symbol */
    typedef keel_log_plugin_t* (*create_fn_t)(void);
    create_fn_t create_fn = (create_fn_t)(uintptr_t)dlsym(handle, "keel_log_plugin_create");
    if (!create_fn) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "log_plugin_load: symbol 'keel_log_plugin_create' not found "
                "in %s: %s",
                path, dlerror());
        dlclose(handle);
        return NULL;
    }

    /* Invoke the constructor */
    keel_log_plugin_t* plugin = create_fn();
    if (!plugin) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CONFIG,
                "log_plugin_load: keel_log_plugin_create() returned NULL "
                "from %s",
                path);
        dlclose(handle);
        return NULL;
    }

    /* Track the handle */
    if (s_loaded_count < MAX_LOADED_PLUGINS) {
        s_loaded[s_loaded_count].dl_handle = handle;
        s_loaded[s_loaded_count].plugin    = plugin;
        s_loaded_count++;
    } else {
        KEEL_LOG_WARN(KEEL_LOG_CAT_CONFIG,
                "log_plugin_load: too many loaded plugins (max %d)",
                MAX_LOADED_PLUGINS);
        /* Still return the plugin — we just can't dlclose later */
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CONFIG,
                 "Loaded log plugin '%s' from %s", plugin->name, path);

    return plugin;
}

/**
 * @brief Unload a dynamically loaded log plugin.
 *
 * @param plugin Plugin instance to unload.
 * @return
 */
void keel_log_plugin_unload(keel_log_plugin_t* plugin)
{
    if (!plugin) return;

    /* Close and destroy the plugin itself */
    if (plugin->close)   plugin->close(plugin);
    if (plugin->destroy) plugin->destroy(plugin);

    /* Find and dlclose the corresponding handle */
    for (size_t i = 0; i < s_loaded_count; i++) {
        if (s_loaded[i].plugin == plugin) {
            if (s_loaded[i].dl_handle) {
                dlclose(s_loaded[i].dl_handle);
            }
            /* Compact the table */
            s_loaded[i] = s_loaded[s_loaded_count - 1];
            s_loaded_count--;
            return;
        }
    }
}
