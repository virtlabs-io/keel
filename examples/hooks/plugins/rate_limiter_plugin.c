/**
 * @file rate_limiter_plugin.c
 * @brief Native .so hook plugin example — per-user query rate limiter
 *
 * Demonstrates a stateful native plugin that tracks per-user query
 * counts and aborts queries when a user exceeds the limit.
 *
 * Build:
 *   gcc -shared -fPIC -o rate_limiter_plugin.so rate_limiter_plugin.c \
 *       -I../../include
 *
 * Configuration (inside a worker group):
 *   [worker_group.<group>.hooks]
 *   hook.plugin.rate_limiter = examples/hooks/plugins/rate_limiter_plugin.so
 *
 * Environment:
 *   KEEL_RATE_LIMIT=1000    — max queries per window (default: 1000)
 *   KEEL_RATE_WINDOW=60     — window in seconds (default: 60)
 */

#include "keel_hook.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>

/* -------------------------------------------------------------------------- */
/* Configuration                                                               */
/* -------------------------------------------------------------------------- */

/**
 * Maximum number of distinct users tracked concurrently.
 *
 * Uses a fixed-size array rather than a hash table to keep the example
 * simple and dependency-free.  For production use with many users, replace
 * with a proper hash map.
 */
#define MAX_TRACKED_USERS 256

/**
 * Maximum length of a tracked username (including NUL terminator).
 */
#define USER_NAME_MAX     64

/**
 * Default query limit per time window.  Overridden by KEEL_RATE_LIMIT.
 */
static uint32_t g_max_queries = 1000;

/**
 * Default time window in seconds.  Overridden by KEEL_RATE_WINDOW.
 */
static uint32_t g_window_sec  = 60;

/* -------------------------------------------------------------------------- */
/* Per-user tracking                                                           */
/* -------------------------------------------------------------------------- */

/**
 * Per-user rate tracking entry.
 *
 * Stores the username, an atomic query counter (safe for concurrent
 * fetch_add from multiple worker threads), and the start of the current
 * time window.  When the window expires, the counter resets.
 */
typedef struct {
    char         name[USER_NAME_MAX];
    _Atomic uint32_t count;
    time_t       window_start;
} user_entry_t;

/**
 * Fixed-size user tracking table.
 *
 * Linear scan for lookup — O(n) with n ≤ MAX_TRACKED_USERS.  Acceptable
 * for a demonstration plugin.  A production implementation would use a
 * concurrent hash map or per-worker sharded counters.
 */
static user_entry_t g_users[MAX_TRACKED_USERS];
static int          g_user_count = 0;

/**
 * Find an existing user entry or create a new one.
 *
 * Linear scan through the tracking table.  If the user is not found and
 * there is capacity, a new entry is initialised with a zeroed counter and
 * the current timestamp as the window start.
 *
 * @param name  NUL-terminated username string.
 * @return      Pointer to the user entry, or NULL if the table is full.
 */
static user_entry_t* find_or_create_user(const char* name) {
    /* Linear scan — fine for small user counts */
    for (int i = 0; i < g_user_count; i++) {
        if (strcmp(g_users[i].name, name) == 0)
            return &g_users[i];
    }
    if (g_user_count >= MAX_TRACKED_USERS)
        return NULL;

    user_entry_t* u = &g_users[g_user_count++];
    strncpy(u->name, name, USER_NAME_MAX - 1);
    u->name[USER_NAME_MAX - 1] = '\0';
    atomic_store(&u->count, 0);
    u->window_start = time(NULL);
    return u;
}

/* -------------------------------------------------------------------------- */
/* Hook callback                                                               */
/* -------------------------------------------------------------------------- */

/**
 * Hook callback: enforce per-user query rate limit (AFTER_QUERY_READ).
 *
 * Runs at the earliest hook point (priority 10) so over-limit users are
 * rejected before any parsing or routing work is done.
 *
 * Algorithm:
 *   1. Look up or create the user's tracking entry.
 *   2. If the current time window has expired, atomically reset the counter.
 *   3. Atomically increment the counter.
 *   4. If the counter exceeds g_max_queries, write an error message into
 *      ctx->error_msg and return false to abort the query.
 *
 * The atomic counter allows safe concurrent access from multiple Keel
 * worker threads without locks.  The window reset is not perfectly atomic
 * (a brief race between the time check and the store), but the worst case
 * is one extra query getting through — acceptable for rate limiting.
 *
 * @param ctx  Hook context.  Fields used: username.  On abort, error_msg
 *             is populated with a descriptive message.
 * @return     true to continue, false to abort the query.
 */
static bool hook_rate_limit(keel_hook_ctx_t* ctx) {
    const char* user = ctx->username ? ctx->username : "unknown";

    user_entry_t* u = find_or_create_user(user);
    if (!u) return true;  /* out of slots — let it through */

    time_t now = time(NULL);

    /* Reset window */
    if ((now - u->window_start) >= (time_t)g_window_sec) {
        atomic_store(&u->count, 0);
        u->window_start = now;
    }

    uint32_t current = atomic_fetch_add(&u->count, 1) + 1;

    if (current > g_max_queries) {
        snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                 "Rate limit exceeded: user '%s' sent %u queries in %us (limit %u)",
                 user, current, g_window_sec, g_max_queries);
        return false;  /* abort */
    }

    return true;  /* continue */
}

/* -------------------------------------------------------------------------- */
/* Plugin definition                                                           */
/* -------------------------------------------------------------------------- */

/**
 * Reset the user tracking table on plugin unload.
 */
static void plugin_shutdown(void) {
    g_user_count = 0;
}

/**
 * Hook definition — single hook at AFTER_QUERY_READ with high priority.
 *
 * Priority 10 ensures the rate limiter runs before security and policy
 * hooks (50+), rejecting excess traffic as early as possible.
 */
static const keel_hook_plugin_def_t s_hooks[] = {
    {
        .point    = KEEL_HOOK_AFTER_QUERY_READ,
        .name     = "rate-limiter",
        .fn       = hook_rate_limit,
        .priority = 10,   /* run very early */
    },
};

/**
 * Plugin metadata.
 */
static const keel_hook_plugin_info_t s_plugin_info = {
    .api_version = KEEL_HOOK_PLUGIN_API_VERSION,
    .name        = "rate_limiter_plugin",
    .version     = "1.0.0",
    .description = "Per-user query rate limiter",
    .hooks       = s_hooks,
    .hook_count  = sizeof(s_hooks) / sizeof(s_hooks[0]),
    .shutdown    = plugin_shutdown,
};

/**
 * Plugin entry point.
 *
 * Reads configuration from environment variables:
 *   KEEL_RATE_LIMIT  — max queries per window (default 1000)
 *   KEEL_RATE_WINDOW — window duration in seconds (default 60)
 *
 * @return  Pointer to the static plugin info struct.
 */
const keel_hook_plugin_info_t* keel_hook_plugin_init(void) {
    const char* env;

    env = getenv("KEEL_RATE_LIMIT");
    if (env) g_max_queries = (uint32_t)atoi(env);

    env = getenv("KEEL_RATE_WINDOW");
    if (env) g_window_sec = (uint32_t)atoi(env);

    return &s_plugin_info;
}
