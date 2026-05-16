/**
 * @file query_rules.c
 * @brief Declarative query routing/rewriting/blocking rules.
 *
 * Loads `[query_rule.N]` INI sections, compiles POSIX extended regular
 * expressions, and fires as a KEEL_HOOK_BEFORE_ROUTE native hook.
 *
 * Each rule is evaluated in priority (section-number) order. The first rule
 * whose matchers all pass wins. If no rule matches the query proceeds with
 * the engine's default routing decision.
 */

#define _POSIX_C_SOURCE 200809L
#include <regex.h>

#include "keel/core/query_rules.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

/* ============================================================================
 * Internal helpers
 * ============================================================================ */

static void rule_free(keel_query_rule_t* r) {
    if (!r) return;
    if (r->regex_valid) {
        regfree((regex_t*)&r->regex_storage);
        r->regex_valid = false;
    }
    keel_free(r->match_regex);
    keel_free(r->match_user);
    keel_free(r->match_db);
    keel_free(r->error_msg);
    keel_free(r->rewrite_to);
    keel_free(r->rewrite_packet);
    keel_free(r->inject_search_path);
    keel_free(r->name);
    r->match_regex  = NULL;
    r->match_user   = NULL;
    r->match_db     = NULL;
    r->error_msg    = NULL;
    r->rewrite_to   = NULL;
    r->rewrite_packet     = NULL;
    r->rewrite_packet_len = 0;
    r->inject_search_path = NULL;
    r->name         = NULL;
}

/* ============================================================================
 * Lifecycle
 * ============================================================================ */

keel_query_rules_t* keel_query_rules_create(void) {
    keel_query_rules_t* rl = keel_calloc(1, sizeof(*rl));
    if (!rl) return NULL;
    rl->refcnt = 1;
    return rl;
}

/**
 * @brief Destroy a query rule set and all rules it contains.
 *
 * @param rl  Rule set to destroy.  Passing `NULL` is safe.
 * @return Nothing.
 *
 * Notes:
 * - Frees all heap strings and compiled regex state for every rule, then
 *   releases the rules array and the container struct.
 * - This is the low-level destructor; prefer `keel_query_rules_unref()` for
 *   reference-counted teardown.
 */
void keel_query_rules_destroy(keel_query_rules_t* rl) {
    if (!rl) return;
    for (size_t i = 0; i < rl->count; i++)
        rule_free(&rl->rules[i]);
    keel_free(rl->rules);
    keel_free(rl);
}

/**
 * @brief Increment the reference count of a query rule set.
 *
 * @param rl  Rule set whose reference count should be incremented.
 *            Passing `NULL` is safe.
 * @return Nothing.
 *
 * Notes:
 * - Must be paired with a corresponding `keel_query_rules_unref()` to avoid
 *   a memory leak.
 */
void keel_query_rules_ref(keel_query_rules_t* rl) {
    if (rl) rl->refcnt++;
}

/**
 * @brief Decrement the reference count and destroy the rule set when it
 *        reaches zero.
 *
 * @param rl  Rule set to release.  Passing `NULL` is safe.
 * @return Nothing.
 */
void keel_query_rules_unref(keel_query_rules_t* rl) {
    if (!rl) return;
    if (--rl->refcnt == 0)
        keel_query_rules_destroy(rl);
}

/* ============================================================================
 * Config loading
 * ============================================================================ */

/**
 * Context for the section-iteration callback used during config loading.
 */
typedef struct {
    const keel_config_t* config;
    keel_query_rule_t*   rules;      /**< Growing array (realloc'd) */
    size_t               count;
    size_t               capacity;
    bool                 error;
} load_ctx_t;

/**
 * @brief INI section callback: parse one `[query_rule.N]` section into a rule.
 *
 * @param section  Name of the INI section being processed
 *                 (e.g. `"query_rule.0"`).
 * @param ptr      Pointer to the `load_ctx_t` accumulator.
 * @return Nothing.  Sets `ctx->error = true` on allocation failure.
 *
 * Fields read from the INI section:
 * - `enabled`     — bool, default `true`.
 * - `match_regex` — POSIX ERE; rule disabled on compile error.
 * - `match_user`  — exact user-name matcher; `NULL` means any user.
 * - `match_db`    — exact database matcher; `NULL` means any database.
 * - `action`      — `"route"` (default), `"block"`, or `"rewrite"`.
 * - `route_to`    — `"any"` (default), `"primary"`, or `"replica"`
 *                   (only meaningful when `action = route`).
 * - `error_msg`   — custom error string (only for `action = block`).
 * - `rewrite_to`  — replacement SQL string (only for `action = rewrite`).
 *
 * Notes:
 * - The rule `priority` is parsed from the numeric suffix after
 *   `"query_rule."` and is used to sort rules into evaluation order.
 * - Array growth uses a doubling strategy starting at capacity 8.
 */
static void load_one_rule(const char* section, void* ptr) {
    load_ctx_t* ctx = (load_ctx_t*)ptr;

    /* Grow the array when needed. */
    if (ctx->count == ctx->capacity) {
        size_t new_cap = ctx->capacity ? ctx->capacity * 2 : 8;
        keel_query_rule_t* nr = keel_realloc(ctx->rules,
                                              new_cap * sizeof(*nr));
        if (!nr) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                "query_rules: OOM expanding rule array");
            ctx->error = true;
            return;
        }
        /* Zero-init the new slots. */
        memset(nr + ctx->capacity, 0,
               (new_cap - ctx->capacity) * sizeof(*nr));
        ctx->rules    = nr;
        ctx->capacity = new_cap;
    }

    keel_query_rule_t* r = &ctx->rules[ctx->count];
    memset(r, 0, sizeof(*r));

    /* Extract priority from suffix after "query_rule." (11 chars) */
    const char* suffix = section + 11;   /* "query_rule." is 11 chars */
    r->priority = (uint32_t)atoi(suffix);
    r->enabled  = keel_config_get_bool(ctx->config, section, "enabled", true);
    r->name     = keel_strdup(section);  /* e.g. "query_rule.0" */

    /* ---- Matchers ---- */
    const char* regex_str = keel_config_get_string(
        ctx->config, section, "match_regex", NULL);
    if (regex_str && regex_str[0]) {
        r->match_regex = keel_strdup(regex_str);
        if (!r->match_regex) { ctx->error = true; return; }
        int re = regcomp((regex_t*)&r->regex_storage,
                         regex_str, REG_EXTENDED | REG_NOSUB);
        if (re != 0) {
            char errbuf[256];
            regerror(re, (regex_t*)&r->regex_storage, errbuf, sizeof(errbuf));
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "query_rules [%s]: invalid match_regex '%s': %s (rule disabled)",
                section, regex_str, errbuf);
            r->enabled = false;
        } else {
            r->regex_valid = true;
        }
    }

    const char* mu = keel_config_get_string(
        ctx->config, section, "match_user", NULL);
    if (mu && mu[0]) {
        r->match_user = keel_strdup(mu);
        if (!r->match_user) { ctx->error = true; return; }
    }

    const char* md = keel_config_get_string(
        ctx->config, section, "match_db", NULL);
    if (md && md[0]) {
        r->match_db = keel_strdup(md);
        if (!r->match_db) { ctx->error = true; return; }
    }

    /* ---- Action ---- */
    const char* action_str = keel_config_get_string(
        ctx->config, section, "action", "route");

    if (strcmp(action_str, "block") == 0) {
        r->action = KEEL_QR_ACTION_BLOCK;
        const char* em = keel_config_get_string(
            ctx->config, section, "error_msg",
            "Query blocked by rule");
        r->error_msg = keel_strdup(em ? em : "Query blocked by rule");
        if (!r->error_msg) { ctx->error = true; return; }

    } else if (strcmp(action_str, "rewrite") == 0) {
        r->action = KEEL_QR_ACTION_REWRITE;
        const char* rt = keel_config_get_string(
            ctx->config, section, "rewrite_to", NULL);
        if (!rt || !rt[0]) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE,
                "query_rules [%s]: action=rewrite but no rewrite_to "
                "(rule disabled)", section);
            r->enabled = false;
        } else {
            r->rewrite_to = keel_strdup(rt);
            if (!r->rewrite_to) { ctx->error = true; return; }

            /* Pre-build the PostgreSQL Simple Query ('Q') wire message so the
             * engine can substitute it without any per-request allocation.
             * Format: 'Q'(1) + int32 msg_len(4) + sql(n) + NUL(1)
             * msg_len = 4 (length field itself) + sql_len + 1 (NUL) */
            size_t sql_len = strlen(r->rewrite_to);
            size_t pkt_len = 1 + 4 + sql_len + 1;
            r->rewrite_packet = keel_malloc(pkt_len);
            if (!r->rewrite_packet) { ctx->error = true; return; }
            uint32_t msg_len = (uint32_t)(4 + sql_len + 1);
            r->rewrite_packet[0] = 'Q';
            r->rewrite_packet[1] = (uint8_t)(msg_len >> 24);
            r->rewrite_packet[2] = (uint8_t)(msg_len >> 16);
            r->rewrite_packet[3] = (uint8_t)(msg_len >>  8);
            r->rewrite_packet[4] = (uint8_t)(msg_len);
            memcpy(r->rewrite_packet + 5, r->rewrite_to, sql_len);
            r->rewrite_packet[5 + sql_len] = '\0';
            r->rewrite_packet_len = pkt_len;
        }

    } else {
        /* Default: route */
        r->action = KEEL_QR_ACTION_ROUTE;
        const char* rt_str = keel_config_get_string(
            ctx->config, section, "route_to", "any");
        if (strcmp(rt_str, "primary") == 0 || strcmp(rt_str, "write") == 0)
            r->route_to = KEEL_QR_ROUTE_PRIMARY;
        else if (strcmp(rt_str, "replica") == 0 || strcmp(rt_str, "read") == 0)
            r->route_to = KEEL_QR_ROUTE_REPLICA;
        else
            r->route_to = KEEL_QR_ROUTE_ANY;
    }

    /* ---- Optional SQL rewrite policy knobs ---- */
    {
        int64_t toms = keel_config_get_int(ctx->config, section,
                                           "statement_timeout", 0);
        if (toms > 0)
            r->statement_timeout_ms = (uint32_t)toms;

        r->force_read_only = keel_config_get_bool(ctx->config, section,
                                                  "force_read_only", false);

        const char* sp = keel_config_get_string(ctx->config, section,
                                                "search_path", NULL);
        if (sp && sp[0]) {
            r->inject_search_path = keel_strdup(sp);
            if (!r->inject_search_path) { ctx->error = true; return; }
        }
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "query_rules: loaded [%s] action=%s%s%s%s%s",
        section,
        keel_qr_action_name(r->action),
        r->match_regex ? " regex=yes" : "",
        r->match_user  ? " user=" : "", r->match_user  ? r->match_user  : "",
        r->enabled     ? "" : " (disabled)");

    ctx->count++;
}

/**
 * Comparison function for sorting rules by priority (section number).
 */
static int rule_cmp(const void* a, const void* b) {
    const keel_query_rule_t* ra = (const keel_query_rule_t*)a;
    const keel_query_rule_t* rb = (const keel_query_rule_t*)b;
    if (ra->priority < rb->priority) return -1;
    if (ra->priority > rb->priority) return  1;
    return 0;
}

/**
 * @brief Load all `[query_rule.N]` INI sections into a new rule set.
 *
 * @param config  Parsed INI configuration.  May be `NULL`, in which case an
 *                empty rule set is created and returned.
 * @param[out] out  Receives the newly created rule set on success.  Set to
 *                  `NULL` on error.
 * @return `KEEL_OK` on success, `KEEL_ERR_INVALID_ARG` when `out` is `NULL`,
 *         or `KEEL_ERR_NOMEM` on allocation failure.
 *
 * Notes:
 * - The returned rule set has reference count 1; the caller must eventually
 *   call `keel_query_rules_unref()` or `keel_query_rules_destroy()`.
 * - Rules are sorted by their numeric priority (section suffix) after all
 *   sections have been loaded, so evaluation order matches section numbering
 *   regardless of the order the INI parser visits sections.
 * - Errors in individual sections do not abort loading; the rule is disabled
 *   (see `load_one_rule`) and remaining sections are still processed.  An
 *   allocation failure in any section causes the entire load to fail.
 */
keel_error_t keel_query_rules_load(const keel_config_t* config,
                                   keel_query_rules_t** out)
{
    if (!out) return KEEL_ERR_INVALID_ARG;

    keel_query_rules_t* rl = keel_query_rules_create();
    if (!rl) return KEEL_ERR_NOMEM;

    if (!config) {
        *out = rl;
        return KEEL_OK;
    }

    load_ctx_t ctx = {
        .config   = config,
        .rules    = NULL,
        .count    = 0,
        .capacity = 0,
        .error    = false,
    };

    keel_config_iter_sections_prefix(config, "query_rule.",
                                     load_one_rule, &ctx);

    if (ctx.error) {
        /* Free whatever was partially allocated */
        for (size_t i = 0; i < ctx.count; i++)
            rule_free(&ctx.rules[i]);
        keel_free(ctx.rules);
        keel_query_rules_destroy(rl);
        return KEEL_ERR_NOMEM;
    }

    /* Sort by priority so rule evaluation order matches section numbers */
    if (ctx.count > 1)
        qsort(ctx.rules, ctx.count, sizeof(*ctx.rules), rule_cmp);

    rl->rules = ctx.rules;
    rl->count = ctx.count;

    KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
        "query_rules: loaded %zu rule(s)", ctx.count);

    *out = rl;
    return KEEL_OK;
}

/* ============================================================================
 * Hot-reload
 * ============================================================================ */

void keel_query_rules_replace(keel_query_rules_t** slot,
                              keel_query_rules_t*  new_rules)
{
    if (!slot) return;
    keel_query_rules_t* old = *slot;
    *slot = new_rules;
    if (old) keel_query_rules_unref(old);
}

/* ============================================================================
 * Hook callback
 * ============================================================================ */

bool keel_query_rules_hook(keel_hook_ctx_t* ctx) {
    if (!ctx) return true;

    /* user_data is keel_query_rules_t* */
    const keel_query_rules_t* rl =
        (const keel_query_rules_t*)ctx->user_data;
    if (!rl || rl->count == 0) return true;

    const char* sql  = ctx->sql_text;
    size_t      slen = ctx->sql_text_len;
    /* Skip past leading whitespace for matching */
    while (slen > 0 && (*sql == ' ' || *sql == '\t' || *sql == '\n')) {
        sql++; slen--;
    }

    for (size_t i = 0; i < rl->count; i++) {
        const keel_query_rule_t* r = &rl->rules[i];
        if (!r->enabled) continue;

        /* ---- Evaluate matchers ---- */

        /* match_user */
        if (r->match_user) {
            if (!ctx->username) continue;
            if (strcmp(ctx->username, r->match_user) != 0) continue;
        }

        /* match_db */
        if (r->match_db) {
            if (!ctx->database) continue;
            if (strcmp(ctx->database, r->match_db) != 0) continue;
        }

        /* match_regex */
        if (r->regex_valid) {
            if (!sql || slen == 0) continue;
            /* regexec needs a NUL-terminated string. sql_text is guaranteed
             * NUL-terminated by the hook context contract. */
            if (regexec((const regex_t*)&r->regex_storage,
                        sql, 0, NULL, 0) != 0) continue;
        }

        /* ---- Rule matched — apply action ---- */
        switch (r->action) {

        case KEEL_QR_ACTION_ROUTE:
            switch (r->route_to) {
            case KEEL_QR_ROUTE_PRIMARY:
                ctx->route_hint    = KEEL_HOOK_ROUTE_WRITE;
                ctx->needs_primary = true;
                break;
            case KEEL_QR_ROUTE_REPLICA:
                ctx->route_hint    = KEEL_HOOK_ROUTE_READ;
                ctx->needs_primary = false;
                break;
            case KEEL_QR_ROUTE_ANY:
                ctx->route_hint    = KEEL_HOOK_ROUTE_ANY;
                ctx->needs_primary = false;
                break;
            }
            /* Fall through to apply optional rewrite policy knobs */
            goto apply_policy_knobs;

        case KEEL_QR_ACTION_BLOCK:
            if (r->error_msg)
                snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                         "%s", r->error_msg);
            else
                snprintf(ctx->error_msg, sizeof(ctx->error_msg),
                         "Query blocked by rule [%s]",
                         r->name ? r->name : "?");
            return false;  /* abort query */

        case KEEL_QR_ACTION_REWRITE:
            /* Signal the engine to substitute the SQL by writing the rewrite
             * fields into the hook context.  The engine reads these after the
             * full hook chain returns and replaces act.sql_view + act.be_payload
             * before dispatching to the backend. */
            ctx->rewrite_sql             = r->rewrite_to;
            ctx->rewrite_sql_len         = strlen(r->rewrite_to);
            ctx->rewrite_be_payload      = r->rewrite_packet;
            ctx->rewrite_be_payload_len  = r->rewrite_packet_len;
            KEEL_LOG_DEBUG(KEEL_LOG_CAT_CORE,
                "query_rules [%s]: rewriting SQL to '%.*s'",
                r->name ? r->name : "?",
                (int)(ctx->rewrite_sql_len < 120 ? ctx->rewrite_sql_len : 120),
                ctx->rewrite_sql);
            /* Fall through to apply optional rewrite policy knobs */
            goto apply_policy_knobs;
        }

apply_policy_knobs:
        /* Apply optional per-rule SQL rewrite policy knobs to the hook context.
         * engine_flow.c reads these after the hook chain and calls
         * keel_sql_rewrite() when any knob is set. */
        if (r->statement_timeout_ms > 0) {
            ctx->sql_rewrite_add_timeout = true;
            ctx->sql_rewrite_timeout_ms  = (int)r->statement_timeout_ms;
        }
        if (r->force_read_only)
            ctx->sql_rewrite_add_read_only = true;
        if (r->inject_search_path)
            ctx->sql_rewrite_search_path = r->inject_search_path;
        return true;
    }

    return true;  /* no rule matched — pass through */
}

/* ============================================================================
 * Introspection
 * ============================================================================ */

const char* keel_qr_action_name(keel_qr_action_t action) {
    switch (action) {
    case KEEL_QR_ACTION_ROUTE:   return "route";
    case KEEL_QR_ACTION_BLOCK:   return "block";
    case KEEL_QR_ACTION_REWRITE: return "rewrite";
    default:                     return "unknown";
    }
}

/**
 * @brief Return the canonical lowercase name for a query route target.
 *
 * @param route  Route enumeration value.
 * @return A non-NULL string such as `"primary"`, `"replica"`, or `"any"`.
 *         Returns `"unknown"` for unrecognized values.
 */
const char* keel_qr_route_name(keel_qr_route_t route) {
    switch (route) {
    case KEEL_QR_ROUTE_PRIMARY: return "primary";
    case KEEL_QR_ROUTE_REPLICA: return "replica";
    case KEEL_QR_ROUTE_ANY:     return "any";
    default:                    return "unknown";
    }
}
