/**
 * @file throttle.c
 * @brief Query-rate throttling using per-rule token buckets.
 *
 * Loads [throttle.N] INI sections, builds per-rule token buckets, and
 * provides an evaluation function suitable for use in a KEEL_HOOK_BEFORE_ROUTE
 * native hook.
 */

#define _POSIX_C_SOURCE 200809L
#include "keel/core/throttle.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <regex.h>

/* ============================================================================
 * Monotonic clock helper
 * ============================================================================ */

static uint64_t throttle_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ============================================================================
 * Token Bucket
 * ============================================================================ */

void keel_token_bucket_init(keel_token_bucket_t *tb,
                             double rate_rps,
                             double burst)
{
    if (!tb) return;
    pthread_mutex_init(&tb->mutex, NULL);
    tb->rate_rps       = rate_rps > 0 ? rate_rps : 1.0;
    tb->burst          = burst > 0    ? burst     : 1.0;
    tb->tokens         = tb->burst;   /* start full */
    tb->last_refill_ns = throttle_now_ns();
}

/**
 * @brief Try to consume one token from a token bucket at the given timestamp.
 *
 * @param tb     Token bucket instance to operate on.
 * @param now_ns Current monotonic timestamp in nanoseconds (from
 *               `clock_gettime(CLOCK_MONOTONIC, ...)`).  Used to refill the
 *               bucket proportionally to the elapsed time since the last
 *               successful operation.
 * @return `true` when a token was available and successfully consumed (request
 *         is permitted); `false` when the bucket is empty (request should be
 *         throttled).
 *
 * Notes:
 * - A `NULL` bucket pointer is treated as an unconstrained bucket: the
 *   function returns `true` (permit) without touching any state.
 * - Internally guarded by a per-bucket mutex so it is safe to call from
 *   multiple threads.
 * - The bucket refills tokens at `rate_rps` per second up to `burst` tokens.
 *   If `now_ns` is not monotonically increasing relative to the last refill
 *   timestamp, the refill step is skipped but the function still operates
 *   correctly on the current token count.
 */
bool keel_token_bucket_consume(keel_token_bucket_t *tb, uint64_t now_ns)
{
    if (!tb) return true;   /* null guard: pass through */

    pthread_mutex_lock(&tb->mutex);

    /* Refill proportionally to elapsed time */
    if (now_ns > tb->last_refill_ns) {
        double elapsed_s = (double)(now_ns - tb->last_refill_ns) / 1e9;
        tb->tokens += elapsed_s * tb->rate_rps;
        if (tb->tokens > tb->burst)
            tb->tokens = tb->burst;
        tb->last_refill_ns = now_ns;
    }

    bool granted = (tb->tokens >= 1.0);
    if (granted) tb->tokens -= 1.0;

    pthread_mutex_unlock(&tb->mutex);
    return granted;
}

/**
 * @brief Destroy a token bucket, releasing its internal mutex.
 *
 * @param tb Token bucket to destroy.  Passing `NULL` is safe.
 * @return Nothing.
 *
 * Notes:
 * - After this call the token bucket struct must not be used again.
 * - The struct storage itself is not freed; it is the caller's responsibility
 *   to manage the lifetime of the embedding `keel_throttle_rule_t`.
 */
void keel_token_bucket_destroy(keel_token_bucket_t *tb)
{
    if (!tb) return;
    pthread_mutex_destroy(&tb->mutex);
}

/* ============================================================================
 * Per-client bucket map
 * ============================================================================ */

void keel_client_bucket_map_init(keel_client_bucket_map_t *m,
                                  double rate_rps, double burst)
{
    if (!m) return;
    pthread_mutex_init(&m->mutex, NULL);
    m->rate_rps = rate_rps;
    m->burst    = burst;
    for (int i = 0; i < KEEL_THROTTLE_CLIENT_MAP_SLOTS; i++)
        m->slots[i].fd = -1;
}

void keel_client_bucket_map_destroy(keel_client_bucket_map_t *m)
{
    if (!m) return;
    for (int i = 0; i < KEEL_THROTTLE_CLIENT_MAP_SLOTS; i++) {
        if (m->slots[i].fd != -1)
            keel_token_bucket_destroy(&m->slots[i].bucket);
    }
    pthread_mutex_destroy(&m->mutex);
}

/**
 * @brief Find or create a per-client token bucket and consume one token.
 *
 * Uses open-addressing with linear probing.  When the table is full, the
 * slot with the fewest tokens (i.e. the least active client) is evicted.
 */
bool keel_client_bucket_map_consume(keel_client_bucket_map_t *m,
                                     int client_fd, uint64_t now_ns)
{
    if (!m || client_fd < 0) return true;

    pthread_mutex_lock(&m->mutex);

    unsigned int mask  = KEEL_THROTTLE_CLIENT_MAP_SLOTS - 1;
    unsigned int start = (unsigned int)client_fd & mask;
    unsigned int idx   = start;
    int          empty = -1;

    /* Linear probe: look for existing entry or first empty slot */
    for (unsigned int probe = 0; probe < KEEL_THROTTLE_CLIENT_MAP_SLOTS; probe++) {
        int slot = (start + probe) & mask;
        if (m->slots[slot].fd == client_fd) {
            /* Found: consume */
            bool granted = keel_token_bucket_consume(&m->slots[slot].bucket, now_ns);
            pthread_mutex_unlock(&m->mutex);
            return granted;
        }
        if (m->slots[slot].fd == -1 && empty == -1)
            empty = slot;
    }

    /* Not found — insert into empty slot (or evict fullest bucket if table full) */
    if (empty == -1) {
        /* Evict the slot with the most tokens (least recently constrained) */
        double max_tokens = -1.0;
        for (unsigned int probe = 0; probe < KEEL_THROTTLE_CLIENT_MAP_SLOTS; probe++) {
            int slot = (start + probe) & mask;
            /* Lock already held; access bucket tokens directly */
            if (m->slots[slot].bucket.tokens > max_tokens) {
                max_tokens = m->slots[slot].bucket.tokens;
                empty = slot;
            }
        }
        keel_token_bucket_destroy(&m->slots[empty].bucket);
        m->slots[empty].fd = -1;
    }

    /* Initialise bucket for new client */
    idx = (unsigned int)empty;
    m->slots[idx].fd = client_fd;
    keel_token_bucket_init(&m->slots[idx].bucket, m->rate_rps, m->burst);
    /* Consume the first token immediately */
    bool granted = keel_token_bucket_consume(&m->slots[idx].bucket, now_ns);
    pthread_mutex_unlock(&m->mutex);
    return granted;
}

/* ============================================================================
 * Rule lifecycle
 * ============================================================================ */

static void rule_free(keel_throttle_rule_t *r)
{
    if (!r) return;
    if (r->regex_valid) {
        regfree(&r->regex_storage);
        r->regex_valid = false;
    }
    keel_free(r->name);
    keel_free(r->match_regex);
    keel_free(r->match_user);
    keel_free(r->match_db);
    keel_free(r->error_msg);
    if (r->per_client)
        keel_client_bucket_map_destroy(&r->client_map);
    else
        keel_token_bucket_destroy(&r->bucket);
}

/* ============================================================================
 * Collection lifecycle
 * ============================================================================ */

keel_throttle_rules_t *keel_throttle_rules_create(void)
{
    keel_throttle_rules_t *tr = keel_calloc(1, sizeof(*tr));
    if (!tr) return NULL;
    tr->refcnt = 1;
    return tr;
}

/**
 * @brief Free all resources owned by a throttle rule set.
 *
 * Internal function called when the reference count drops to zero.
 *
 * @param tr Rule set to destroy.  Passing `NULL` is safe.
 * @return Nothing.
 *
 * Notes:
 * - Calls `rule_free()` for every rule, then releases the rules array and the
 *   container itself.
 * - Not thread-safe with respect to concurrent `ref`/`unref`; callers must
 *   ensure no other threads hold references.
 */
static void throttle_rules_destroy(keel_throttle_rules_t *tr)
{
    if (!tr) return;
    for (size_t i = 0; i < tr->count; i++)
        rule_free(&tr->rules[i]);
    keel_free(tr->rules);
    keel_free(tr);
}

/**
 * @brief Increment the reference count of a throttle rule set.
 *
 * @param tr Rule set whose reference count should be incremented.
 *           Passing `NULL` is safe.
 * @return Nothing.
 *
 * Notes:
 * - Must be paired with a corresponding `keel_throttle_rules_unref()` to
 *   avoid a memory leak.
 * - Not thread-safe relative to concurrent `unref()` calls on the same object.
 *   The typical usage pattern is: acquire a reference on the caller's thread
 *   while holding an external lock, then release it from the same thread when
 *   done.
 */
void keel_throttle_rules_ref(keel_throttle_rules_t *tr)
{
    if (!tr) return;
    tr->refcnt++;
}

/**
 * @brief Decrement the reference count of a throttle rule set, destroying it
 *        when the count reaches zero.
 *
 * @param tr Rule set to release.  Passing `NULL` is safe.
 * @return Nothing.
 *
 * Notes:
 * - When the reference count reaches zero, all rules and their compiled
 *   regular expressions are freed.
 * - Must not be called after the reference count would go below zero; this
 *   would indicate a programming error (over-release).
 */
void keel_throttle_rules_unref(keel_throttle_rules_t *tr)
{
    if (!tr) return;
    if (--tr->refcnt <= 0)
        throttle_rules_destroy(tr);
}

/* ============================================================================
 * INI loading
 * ============================================================================ */

typedef struct load_ctx {
    keel_throttle_rules_t *tr;
    const keel_config_t   *config;
    bool                   error;
} load_ctx_t;

/**
 * @brief INI section callback: parse one `[throttle.NAME]` section into a rule.
 *
 * @param section  Name of the INI section being processed (e.g.
 *                 `"throttle.api"`).
 * @param ud       Pointer to the `load_ctx_t` accumulator carrying the target
 *                 rule set, the config handle, and an error flag.
 * @return Nothing.  Sets `ctx->error = true` on allocation failure so the
 *         caller can detect and abort after iteration finishes.
 *
 * Fields read from the INI section:
 * - `enabled`     — bool, default `true`.
 * - `rate_rps`    — double, queries per second, default `100.0`.
 * - `burst`       — double, bucket capacity in tokens, defaults to `rate_rps`.
 * - `match_regex` — POSIX ERE string; rule is disabled if regex fails to compile.
 * - `match_user`  — exact user-name matcher; `NULL` means any user.
 * - `match_db`    — exact database-name matcher; `NULL` means any database.
 * - `error_msg`   — custom error string returned to throttled clients.
 *
 * Notes:
 * - Each call grows `tr->rules` by one via `keel_realloc`.
 * - A rule whose regex fails to compile is retained in the array but marked
 *   `enabled = false` so it has no runtime effect.
 */
static void load_one_rule(const char *section, void *ud)
{
    load_ctx_t *ctx = (load_ctx_t *)ud;
    if (ctx->error) return;

    keel_throttle_rules_t *tr = ctx->tr;

    /* Grow the rules array */
    size_t idx = tr->count;
    keel_throttle_rule_t *tmp =
        keel_realloc(tr->rules, (idx + 1) * sizeof(*tmp));
    if (!tmp) { ctx->error = true; return; }
    tr->rules = tmp;
    keel_throttle_rule_t *r = &tr->rules[idx];
    memset(r, 0, sizeof(*r));

    r->name = keel_strdup(section);
    if (!r->name) { ctx->error = true; return; }

    /* enabled (default true) */
    const char *en = keel_config_get_string(
        ctx->config, section, "enabled", "true");
    r->enabled = !(strcmp(en, "false") == 0 || strcmp(en, "0") == 0);

    /* rate_rps */
    double rate = keel_config_get_float(ctx->config, section, "rate_rps", 100.0);
    /* burst */
    double burst = keel_config_get_float(ctx->config, section, "burst", rate);

    /* match_regex */
    const char *regex_str = keel_config_get_string(
        ctx->config, section, "match_regex", NULL);
    if (regex_str) {
        r->match_regex = keel_strdup(regex_str);
        if (!r->match_regex) { ctx->error = true; return; }
        char errbuf[128];
        int rc = regcomp(&r->regex_storage, regex_str, REG_EXTENDED | REG_NOSUB);
        if (rc != 0) {
            regerror(rc, &r->regex_storage, errbuf, sizeof(errbuf));
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "throttle [%s]: invalid match_regex '%s': %s (rule disabled)",
                section, regex_str, errbuf);
            r->enabled = false;
        } else {
            r->regex_valid = true;
        }
    }

    /* match_user */
    const char *mu = keel_config_get_string(ctx->config, section, "match_user", NULL);
    if (mu) {
        r->match_user = keel_strdup(mu);
        if (!r->match_user) { ctx->error = true; return; }
    }

    /* match_db */
    const char *md = keel_config_get_string(ctx->config, section, "match_db", NULL);
    if (md) {
        r->match_db = keel_strdup(md);
        if (!r->match_db) { ctx->error = true; return; }
    }

    /* error_msg */
    const char *em = keel_config_get_string(ctx->config, section, "error_msg", NULL);
    if (em) {
        r->error_msg = keel_strdup(em);
        if (!r->error_msg) { ctx->error = true; return; }
    }

    /* Initialise token bucket (global or per-client) */
    const char *pc = keel_config_get_string(ctx->config, section, "per_client", "false");
    r->per_client = !(strcmp(pc, "false") == 0 || strcmp(pc, "0") == 0);

    if (r->per_client)
        keel_client_bucket_map_init(&r->client_map, rate, burst);
    else
        keel_token_bucket_init(&r->bucket, rate, burst);

    tr->count++;

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "throttle: loaded [%s] rate=%.1f burst=%.1f%s%s%s%s per_client=%s enabled=%s",
        section, rate, burst,
        r->match_regex ? " regex=" : "",
        r->match_regex ? r->match_regex : "",
        r->match_user  ? " user=" : "",
        r->match_user  ? r->match_user : "",
        r->per_client ? "yes" : "no",
        r->enabled ? "yes" : "no");
}

/**
 * @brief Load all `[throttle.N]` INI sections into a new rule set.
 *
 * @param config  Parsed INI configuration.  May be `NULL`, in which case an
 *                empty rule set is returned.
 * @param[out] out  Receives a pointer to the newly created rule set on
 *                  success.  Set to `NULL` on error.
 * @return `KEEL_OK` on success, `KEEL_ERR_INVALID_ARG` when `out` is `NULL`,
 *         or `KEEL_ERR_NOMEM` on allocation failure.
 *
 * Notes:
 * - The returned rule set has reference count 1; the caller owns it and must
 *   eventually call `keel_throttle_rules_unref()` to release it.
 * - Section names must use the prefix `"throttle."`.  Any other sections are
 *   ignored.
 * - Errors in individual rule sections (e.g. invalid regex) disable the rule
 *   but do not abort the load of remaining rules.
 */
keel_error_t keel_throttle_rules_load(const keel_config_t *config,
                                       keel_throttle_rules_t **out)
{
    if (!out) return KEEL_ERR_INVALID_ARG;
    *out = NULL;

    keel_throttle_rules_t *tr = keel_throttle_rules_create();
    if (!tr) return KEEL_ERR_NOMEM;

    load_ctx_t ctx = { .tr = tr, .config = config, .error = false };
    keel_config_iter_sections_prefix(config, "throttle.", load_one_rule, &ctx);

    if (ctx.error) {
        keel_throttle_rules_unref(tr);
        return KEEL_ERR_NOMEM;
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "throttle: loaded %zu rule(s)", tr->count);

    *out = tr;
    return KEEL_OK;
}

/**
 * @brief Atomically swap the active rule set in a slot, managing reference counts.
 *
 * @param slot       Pointer to the slot variable that holds the current rule set.
 *                   Must not be `NULL`.
 * @param new_rules  The new rule set to install, or `NULL` to clear the slot.
 * @return Nothing.
 *
 * Behavior:
 * - The old rule set is unref'd after the pointer is replaced.
 * - The new rule set is ref'd so the slot holds one reference to it.
 * - Safe to call with `new_rules == NULL` (clears the slot).
 *
 * Thread safety:
 * - This function is not itself atomic; callers must serialize concurrent
 *   slot updates with an external lock.  The ref/unref operations are safe
 *   after the pointer swap because the old rule set is not destroyed until
 *   all references are released.
 */
void keel_throttle_rules_replace(keel_throttle_rules_t **slot,
                                  keel_throttle_rules_t  *new_rules)
{
    if (!slot) return;
    keel_throttle_rules_t *old = *slot;
    *slot = new_rules;
    if (old) keel_throttle_rules_unref(old);
    if (new_rules) keel_throttle_rules_ref(new_rules);
}

/* ============================================================================
 * Evaluation
 * ============================================================================ */

bool keel_throttle_check(keel_throttle_rules_t *tr,
                          const char *sql,
                          const char *username,
                          const char *database,
                          int         client_fd,
                          uint64_t    now_ns,
                          const char **error_msg)
{
    if (!tr || tr->count == 0) return false;

    if (now_ns == 0) now_ns = throttle_now_ns();

    for (size_t i = 0; i < tr->count; i++) {
        keel_throttle_rule_t *r = &tr->rules[i];
        if (!r->enabled) continue;

        /* Check matchers — all must match */
        if (r->match_user && username &&
            strcmp(r->match_user, username) != 0) continue;
        if (r->match_user && !username) continue;

        if (r->match_db && database &&
            strcmp(r->match_db, database) != 0) continue;
        if (r->match_db && !database) continue;

        if (r->regex_valid && sql) {
            if (regexec(&r->regex_storage, sql, 0, NULL, 0) != 0) continue;
        }

        /* Rule matched — try to consume a token (per-client or global) */
        bool granted;
        if (r->per_client)
            granted = keel_client_bucket_map_consume(&r->client_map, client_fd, now_ns);
        else
            granted = keel_token_bucket_consume(&r->bucket, now_ns);

        if (!granted) {
            atomic_fetch_add(&tr->queries_throttled, 1);
            if (error_msg) {
                *error_msg = r->error_msg
                    ? r->error_msg
                    : "too many requests — rate limit exceeded";
            }
            return true;  /* throttled */
        }

        return false;  /* first match: token granted, not throttled */
    }

    return false;  /* no rule matched */
}

/* ============================================================================
 * Hook integration
 * ============================================================================ */

typedef struct throttle_hook_ctx {
    keel_throttle_rules_t *rules;
} throttle_hook_ctx_t;

/**
 * @brief `KEEL_HOOK_BEFORE_ROUTE` callback that enforces the active throttle rules.
 *
 * @param ctx  Hook execution context carrying the SQL text, username, database,
 *             and a writable `error_msg` buffer.
 * @return `true` when the query is permitted (no matching rule throttled it),
 *         `false` when the query should be rejected.  On rejection, `ctx->error_msg`
 *         is populated with the rule's error message or a generic rate-limit string.
 *
 * Notes:
 * - Retrieves the `keel_throttle_rules_t *` from `ctx->user_data`.
 * - Passes timestamp `0` to `keel_throttle_check()`, which resolves it to the
 *   current monotonic clock reading.
 * - Priority 10 ensures this hook fires before the query-rules hook (priority 0).
 */
static bool throttle_hook_cb(keel_hook_ctx_t *ctx)
{
    throttle_hook_ctx_t *tc = (throttle_hook_ctx_t *)ctx->user_data;
    if (!tc || !tc->rules) return true;

    const char *sql      = ctx->sql_text;
    const char *username = ctx->username;
    const char *database = ctx->database;
    const char *err_msg  = NULL;

    bool throttled = keel_throttle_check(
        tc->rules, sql, username, database, ctx->client_fd, 0, &err_msg);

    if (throttled) {
        snprintf(ctx->error_msg, sizeof(ctx->error_msg), "%s",
            err_msg ? err_msg : "too many requests");
        return false;
    }
    return true;
}

/**
 * @brief Register the throttle hook into a hook registry.
 *
 * @param registry  Target hook registry.  Must not be `NULL`.
 * @param rules     Rule set to associate with the hook.  Must not be `NULL`.
 * @return `KEEL_OK` on success, `KEEL_ERR_INVALID_ARG` when either argument
 *         is `NULL`, or `KEEL_ERR_NOMEM` on allocation failure.
 *
 * Behavior:
 * - Allocates a `throttle_hook_ctx_t` wrapper that holds a reference to
 *   `rules`.  The wrapper is owned by the hook registry and freed with it.
 * - Registers the hook at `KEEL_HOOK_BEFORE_ROUTE` with priority 10, which
 *   causes it to fire before the query-rules hook (priority 0) so throttling
 *   is applied before any routing decisions are made.
 *
 * Notes:
 * - The hook registration does not take ownership of `rules`; `rules` must
 *   remain valid for the lifetime of the registry, or until the hook is
 *   unregistered.
 * - Calling this function multiple times on the same registry creates multiple
 *   independent hook entries.
 */
keel_error_t keel_throttle_rules_register_hook(keel_hook_registry_t  *registry,
                                                keel_throttle_rules_t *rules)
{
    if (!registry || !rules) return KEEL_ERR_INVALID_ARG;

    throttle_hook_ctx_t *tc = keel_calloc(1, sizeof(*tc));
    if (!tc) return KEEL_ERR_NOMEM;
    tc->rules = rules;
    keel_throttle_rules_ref(rules);

    keel_hook_handle_t *h = keel_hook_register(
        registry,
        KEEL_HOOK_BEFORE_ROUTE,
        "throttle",
        throttle_hook_cb,
        10,   /* priority: fires before query_rules (priority 0) */
        tc
    );
    return h ? KEEL_OK : KEEL_ERR_NOMEM;
}

/* ============================================================================
 * Introspection
 * ============================================================================ */

uint64_t keel_throttle_total_rejected(const keel_throttle_rules_t *tr)
{
    if (!tr) return 0;
    return atomic_load(&tr->queries_throttled);
}
