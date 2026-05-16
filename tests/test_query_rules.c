/**
 * @file test_query_rules.c
 * @brief Unit tests for declarative query routing/blocking/rewriting rules.
 *
 * Tests cover:
 * §1 — Rule loading from INI config (route, block, rewrite, disabled)
 * §2 — Matcher evaluation (regex, user, db, combined AND semantics)
 * §3 — Hook callback: route action updates route_hint and needs_primary
 * §4 — Hook callback: block action returns false + populates error_msg
 * §5 — Hook callback: first-match-wins ordering
 * §6 — Empty rule list is a no-op
 * §7 — Rules with no matchers match all queries (catch-all)
 * §8 — Invalid regex disables rule but does not abort loading
 * §9 — keel_qr_action_name / keel_qr_route_name introspection
 */

#include "test_utils.h"

#define _POSIX_C_SOURCE 200809L

#include "keel/core/query_rules.h"
#include "keel/core/ini.h"
#include "keel_hook.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

/* ============================================================================
 * Test fixture helpers
 * ============================================================================ */

/** Write a temporary INI file and return its path (caller must unlink+free). */
static char* write_tmp_ini(const char* content) {
    char* path = strdup("/tmp/keel_test_qr_XXXXXX.ini");
    if (!path) return NULL;
    int fd = mkstemps(path, 4);
    if (fd < 0) { free(path); return NULL; }
    size_t len = strlen(content);
    if (write(fd, content, len) != (ssize_t)len) {
        close(fd); unlink(path); free(path); return NULL;
    }
    close(fd);
    return path;
}

/** Load query rules from an inline INI string. */
static keel_query_rules_t* rules_from_ini(const char* ini_content) {
    char* path = write_tmp_ini(ini_content);
    if (!path) return NULL;
    keel_config_t* cfg = keel_config_load(path);
    unlink(path);
    free(path);
    if (!cfg) return NULL;
    keel_query_rules_t* rl = NULL;
    keel_query_rules_load(cfg, &rl);
    keel_config_free(cfg);
    return rl;
}

/** Build a minimal hook context. */
static keel_hook_ctx_t make_ctx(const char* sql,
                                const char* username,
                                const char* db) {
    keel_hook_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.sql_text     = sql;
    ctx.sql_text_len = sql ? strlen(sql) : 0;
    ctx.username     = username;
    ctx.database     = db;
    ctx.route_hint   = KEEL_HOOK_ROUTE_ANY;
    ctx.needs_primary = false;
    return ctx;
}

/* ============================================================================
 * §1 — Rule loading
 * ============================================================================ */

static void test_load_route_rule(void) {
    TEST_BEGIN("load: route rule parsed correctly");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_regex = ^SELECT\n"
        "action      = route\n"
        "route_to    = replica\n"
    );
    TEST_ASSERT_NOT_NULL(rl);
    TEST_ASSERT_EQ((int)rl->count, 1);
    TEST_ASSERT_EQ((int)rl->rules[0].action, (int)KEEL_QR_ACTION_ROUTE);
    TEST_ASSERT_EQ((int)rl->rules[0].route_to, (int)KEEL_QR_ROUTE_REPLICA);
    TEST_ASSERT_NOT_NULL(rl->rules[0].match_regex);
    TEST_ASSERT(rl->rules[0].regex_valid);
    TEST_ASSERT(rl->rules[0].enabled);
    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_load_block_rule(void) {
    TEST_BEGIN("load: block rule with custom error message");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_regex = ^DROP\n"
        "action      = block\n"
        "error_msg   = DDL not allowed\n"
    );
    TEST_ASSERT_NOT_NULL(rl);
    TEST_ASSERT_EQ((int)rl->count, 1);
    TEST_ASSERT_EQ((int)rl->rules[0].action, (int)KEEL_QR_ACTION_BLOCK);
    TEST_ASSERT_NOT_NULL(rl->rules[0].error_msg);
    TEST_ASSERT_STR_EQ(rl->rules[0].error_msg, "DDL not allowed");
    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_load_rewrite_rule(void) {
    TEST_BEGIN("load: rewrite rule stored correctly");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_regex = ^/\\* analytics\n"
        "action      = rewrite\n"
        "rewrite_to  = SELECT 1\n"
    );
    TEST_ASSERT_NOT_NULL(rl);
    TEST_ASSERT_EQ((int)rl->count, 1);
    TEST_ASSERT_EQ((int)rl->rules[0].action, (int)KEEL_QR_ACTION_REWRITE);
    TEST_ASSERT_NOT_NULL(rl->rules[0].rewrite_to);
    TEST_ASSERT_STR_EQ(rl->rules[0].rewrite_to, "SELECT 1");
    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_load_multiple_rules(void) {
    TEST_BEGIN("load: multiple rules sorted by section number");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.2]\n"
        "action = route\n"
        "route_to = primary\n"
        "[query_rule.0]\n"
        "action = block\n"
        "error_msg = rule0\n"
        "[query_rule.1]\n"
        "action = route\n"
        "route_to = replica\n"
    );
    TEST_ASSERT_NOT_NULL(rl);
    TEST_ASSERT_EQ((int)rl->count, 3);
    /* Sorted by priority (section number) */
    TEST_ASSERT_EQ((int)rl->rules[0].priority, 0);
    TEST_ASSERT_EQ((int)rl->rules[1].priority, 1);
    TEST_ASSERT_EQ((int)rl->rules[2].priority, 2);
    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_load_empty_config(void) {
    TEST_BEGIN("load: no query_rule sections → empty list");

    keel_query_rules_t* rl = rules_from_ini("[keel]\nlog_level = 3\n");
    TEST_ASSERT_NOT_NULL(rl);
    TEST_ASSERT_EQ((int)rl->count, 0);
    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_load_invalid_regex_disables(void) {
    TEST_BEGIN("load: invalid regex disables rule without aborting");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_regex = [invalid(\n"
        "action      = block\n"
    );
    TEST_ASSERT_NOT_NULL(rl);
    /* Rule is loaded but disabled */
    TEST_ASSERT_EQ((int)rl->count, 1);
    TEST_ASSERT(!rl->rules[0].enabled);
    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_load_disabled_flag(void) {
    TEST_BEGIN("load: enabled = false disables rule");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "action  = block\n"
        "enabled = false\n"
    );
    TEST_ASSERT_NOT_NULL(rl);
    TEST_ASSERT_EQ((int)rl->count, 1);
    TEST_ASSERT(!rl->rules[0].enabled);
    keel_query_rules_destroy(rl);
    TEST_END();
}

/* ============================================================================
 * §3 — Hook callback: route action
 * ============================================================================ */

static void test_hook_route_to_replica(void) {
    TEST_BEGIN("hook: SELECT routed to replica");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_regex = ^SELECT\n"
        "action      = route\n"
        "route_to    = replica\n"
    );
    TEST_ASSERT_NOT_NULL(rl);

    keel_hook_ctx_t ctx = make_ctx("SELECT 1", NULL, NULL);
    ctx.user_data = rl;
    bool ok = keel_query_rules_hook(&ctx);

    TEST_ASSERT(ok);
    TEST_ASSERT_EQ((int)ctx.route_hint, (int)KEEL_HOOK_ROUTE_READ);
    TEST_ASSERT(!ctx.needs_primary);
    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_hook_route_to_primary(void) {
    TEST_BEGIN("hook: INSERT routed to primary");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_regex = ^INSERT\n"
        "action      = route\n"
        "route_to    = primary\n"
    );
    TEST_ASSERT_NOT_NULL(rl);

    keel_hook_ctx_t ctx = make_ctx("INSERT INTO t VALUES (1)", NULL, NULL);
    ctx.user_data = rl;
    bool ok = keel_query_rules_hook(&ctx);

    TEST_ASSERT(ok);
    TEST_ASSERT_EQ((int)ctx.route_hint, (int)KEEL_HOOK_ROUTE_WRITE);
    TEST_ASSERT(ctx.needs_primary);
    keel_query_rules_destroy(rl);
    TEST_END();
}

/* ============================================================================
 * §4 — Hook callback: block action
 * ============================================================================ */

static void test_hook_block_returns_false(void) {
    TEST_BEGIN("hook: block rule returns false and sets error_msg");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_regex = ^DROP\n"
        "action      = block\n"
        "error_msg   = DDL not allowed\n"
    );
    TEST_ASSERT_NOT_NULL(rl);

    keel_hook_ctx_t ctx = make_ctx("DROP TABLE foo", NULL, NULL);
    ctx.user_data = rl;
    bool ok = keel_query_rules_hook(&ctx);

    TEST_ASSERT(!ok);
    TEST_ASSERT(strlen(ctx.error_msg) > 0);
    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_hook_block_no_match_passes(void) {
    TEST_BEGIN("hook: non-matching query passes through block rule");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_regex = ^DROP\n"
        "action      = block\n"
    );
    TEST_ASSERT_NOT_NULL(rl);

    keel_hook_ctx_t ctx = make_ctx("SELECT 1", NULL, NULL);
    ctx.user_data = rl;
    bool ok = keel_query_rules_hook(&ctx);

    TEST_ASSERT(ok);
    keel_query_rules_destroy(rl);
    TEST_END();
}

/* ============================================================================
 * §2 — Matcher: user and db filters
 * ============================================================================ */

static void test_hook_match_user(void) {
    TEST_BEGIN("hook: match_user filters by username");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_user = readonly_user\n"
        "action     = route\n"
        "route_to   = replica\n"
    );
    TEST_ASSERT_NOT_NULL(rl);

    /* Matching user — should route to replica */
    keel_hook_ctx_t ctx1 = make_ctx("SELECT 1", "readonly_user", NULL);
    ctx1.user_data = rl;
    keel_query_rules_hook(&ctx1);
    TEST_ASSERT_EQ((int)ctx1.route_hint, (int)KEEL_HOOK_ROUTE_READ);

    /* Non-matching user — route_hint stays ANY */
    keel_hook_ctx_t ctx2 = make_ctx("SELECT 1", "admin", NULL);
    ctx2.user_data = rl;
    keel_query_rules_hook(&ctx2);
    TEST_ASSERT_EQ((int)ctx2.route_hint, (int)KEEL_HOOK_ROUTE_ANY);

    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_hook_match_db(void) {
    TEST_BEGIN("hook: match_db filters by database");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_db  = analytics\n"
        "action    = route\n"
        "route_to  = replica\n"
    );
    TEST_ASSERT_NOT_NULL(rl);

    keel_hook_ctx_t ctx1 = make_ctx("SELECT 1", NULL, "analytics");
    ctx1.user_data = rl;
    keel_query_rules_hook(&ctx1);
    TEST_ASSERT_EQ((int)ctx1.route_hint, (int)KEEL_HOOK_ROUTE_READ);

    keel_hook_ctx_t ctx2 = make_ctx("SELECT 1", NULL, "production");
    ctx2.user_data = rl;
    keel_query_rules_hook(&ctx2);
    TEST_ASSERT_EQ((int)ctx2.route_hint, (int)KEEL_HOOK_ROUTE_ANY);

    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_hook_combined_matchers(void) {
    TEST_BEGIN("hook: combined match_user + match_db requires both");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_user = alice\n"
        "match_db   = reporting\n"
        "action     = route\n"
        "route_to   = replica\n"
    );
    TEST_ASSERT_NOT_NULL(rl);

    /* Both match */
    keel_hook_ctx_t ctx1 = make_ctx("SELECT 1", "alice", "reporting");
    ctx1.user_data = rl;
    keel_query_rules_hook(&ctx1);
    TEST_ASSERT_EQ((int)ctx1.route_hint, (int)KEEL_HOOK_ROUTE_READ);

    /* Only user matches */
    keel_hook_ctx_t ctx2 = make_ctx("SELECT 1", "alice", "production");
    ctx2.user_data = rl;
    keel_query_rules_hook(&ctx2);
    TEST_ASSERT_EQ((int)ctx2.route_hint, (int)KEEL_HOOK_ROUTE_ANY);

    /* Only db matches */
    keel_hook_ctx_t ctx3 = make_ctx("SELECT 1", "bob", "reporting");
    ctx3.user_data = rl;
    keel_query_rules_hook(&ctx3);
    TEST_ASSERT_EQ((int)ctx3.route_hint, (int)KEEL_HOOK_ROUTE_ANY);

    keel_query_rules_destroy(rl);
    TEST_END();
}

/* ============================================================================
 * §5 — First-match-wins ordering
 * ============================================================================ */

static void test_hook_first_match_wins(void) {
    TEST_BEGIN("hook: first matching rule wins, later rules skipped");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "match_regex = ^SELECT\n"
        "action      = route\n"
        "route_to    = replica\n"
        "[query_rule.1]\n"
        "match_regex = ^SELECT\n"
        "action      = block\n"
        "error_msg   = should not reach\n"
    );
    TEST_ASSERT_NOT_NULL(rl);
    TEST_ASSERT_EQ((int)rl->count, 2);

    keel_hook_ctx_t ctx = make_ctx("SELECT 1", NULL, NULL);
    ctx.user_data = rl;
    bool ok = keel_query_rules_hook(&ctx);

    /* First rule (route) matches → second rule (block) should not fire */
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ((int)ctx.route_hint, (int)KEEL_HOOK_ROUTE_READ);

    keel_query_rules_destroy(rl);
    TEST_END();
}

/* ============================================================================
 * §6 — Empty rule list
 * ============================================================================ */

static void test_hook_empty_rules_passthrough(void) {
    TEST_BEGIN("hook: empty rule list is a no-op");

    keel_query_rules_t* rl = keel_query_rules_create();
    TEST_ASSERT_NOT_NULL(rl);

    keel_hook_ctx_t ctx = make_ctx("DROP TABLE foo", NULL, NULL);
    ctx.user_data = rl;
    bool ok = keel_query_rules_hook(&ctx);

    TEST_ASSERT(ok);
    TEST_ASSERT_EQ((int)ctx.route_hint, (int)KEEL_HOOK_ROUTE_ANY);

    keel_query_rules_destroy(rl);
    TEST_END();
}

static void test_hook_null_rules_passthrough(void) {
    TEST_BEGIN("hook: NULL user_data is a no-op");

    keel_hook_ctx_t ctx = make_ctx("DROP TABLE foo", NULL, NULL);
    ctx.user_data = NULL;
    bool ok = keel_query_rules_hook(&ctx);
    TEST_ASSERT(ok);
    TEST_END();
}

/* ============================================================================
 * §7 — Catch-all rule (no matchers)
 * ============================================================================ */

static void test_hook_catchall_rule(void) {
    TEST_BEGIN("hook: rule with no matchers matches every query");

    keel_query_rules_t* rl = rules_from_ini(
        "[query_rule.0]\n"
        "action   = route\n"
        "route_to = primary\n"
    );
    TEST_ASSERT_NOT_NULL(rl);

    /* Any query — even an empty one — should match */
    keel_hook_ctx_t ctx1 = make_ctx("SELECT 42", "anyone", "anydb");
    ctx1.user_data = rl;
    keel_query_rules_hook(&ctx1);
    TEST_ASSERT_EQ((int)ctx1.route_hint, (int)KEEL_HOOK_ROUTE_WRITE);

    keel_hook_ctx_t ctx2 = make_ctx("DELETE FROM t", "admin", "prod");
    ctx2.user_data = rl;
    keel_query_rules_hook(&ctx2);
    TEST_ASSERT_EQ((int)ctx2.route_hint, (int)KEEL_HOOK_ROUTE_WRITE);

    keel_query_rules_destroy(rl);
    TEST_END();
}

/* ============================================================================
 * §9 — Introspection helpers
 * ============================================================================ */

static void test_action_names(void) {
    TEST_BEGIN("introspection: keel_qr_action_name returns correct strings");

    TEST_ASSERT_STR_EQ(keel_qr_action_name(KEEL_QR_ACTION_ROUTE),   "route");
    TEST_ASSERT_STR_EQ(keel_qr_action_name(KEEL_QR_ACTION_BLOCK),   "block");
    TEST_ASSERT_STR_EQ(keel_qr_action_name(KEEL_QR_ACTION_REWRITE), "rewrite");
    TEST_END();
}

static void test_route_names(void) {
    TEST_BEGIN("introspection: keel_qr_route_name returns correct strings");

    TEST_ASSERT_STR_EQ(keel_qr_route_name(KEEL_QR_ROUTE_PRIMARY), "primary");
    TEST_ASSERT_STR_EQ(keel_qr_route_name(KEEL_QR_ROUTE_REPLICA), "replica");
    TEST_ASSERT_STR_EQ(keel_qr_route_name(KEEL_QR_ROUTE_ANY),     "any");
    TEST_END();
}

/* ============================================================================
 * §ref/unref lifecycle
 * ============================================================================ */

static void test_refcount(void) {
    TEST_BEGIN("lifecycle: ref/unref increments and decrements refcount");

    keel_query_rules_t* rl = keel_query_rules_create();
    TEST_ASSERT_NOT_NULL(rl);
    TEST_ASSERT_EQ((int)rl->refcnt, 1);

    keel_query_rules_ref(rl);
    TEST_ASSERT_EQ((int)rl->refcnt, 2);

    keel_query_rules_unref(rl);
    TEST_ASSERT_EQ((int)rl->refcnt, 1);

    keel_query_rules_unref(rl);  /* frees — valgrind would catch double-free */
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    /* §1 — Loading */
    test_load_route_rule();
    test_load_block_rule();
    test_load_rewrite_rule();
    test_load_multiple_rules();
    test_load_empty_config();
    test_load_invalid_regex_disables();
    test_load_disabled_flag();

    /* §3 — Route hook */
    test_hook_route_to_replica();
    test_hook_route_to_primary();

    /* §4 — Block hook */
    test_hook_block_returns_false();
    test_hook_block_no_match_passes();

    /* §2 — Matcher filters */
    test_hook_match_user();
    test_hook_match_db();
    test_hook_combined_matchers();

    /* §5 — Ordering */
    test_hook_first_match_wins();

    /* §6 — Empty */
    test_hook_empty_rules_passthrough();
    test_hook_null_rules_passthrough();

    /* §7 — Catch-all */
    test_hook_catchall_rule();

    /* §9 — Introspection */
    test_action_names();
    test_route_names();

    /* lifecycle */
    test_refcount();

    return test_summary();
}
