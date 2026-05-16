/**
 * @file test_throttle.c
 * @brief Unit tests for per-rule token-bucket query throttling.
 *
 * Tests cover:
 * §1 — Token bucket: init / consume / drain / refill / destroy
 * §2 — Throttle rules collection: create / ref / unref
 * §3 — Rule matching: regex, user, db, combinations
 * §4 — keel_throttle_check() semantics
 * §5 — Stats: queries_throttled atomic counter
 * §6 — Null / empty guards
 */

#include "test_utils.h"

#include "keel/core/throttle.h"
#include "keel/mem/mem.h"

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>

/* ============================================================================
 * Time helpers
 * ============================================================================ */

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* ============================================================================
 * §1 — Token bucket
 * ============================================================================ */

static void test_bucket_init(void)
{
    TEST_BEGIN("throttle bucket: init starts full");
    keel_token_bucket_t tb;
    keel_token_bucket_init(&tb, 100.0, 50.0);
    /* Full = burst capacity tokens */
    TEST_ASSERT(tb.tokens >= 50.0 - 0.001);
    keel_token_bucket_destroy(&tb);
    TEST_END();
}

static void test_bucket_consume_single(void)
{
    TEST_BEGIN("throttle bucket: first consume succeeds when bucket is full");
    keel_token_bucket_t tb;
    keel_token_bucket_init(&tb, 1000.0, 10.0);
    bool ok = keel_token_bucket_consume(&tb, now_ns());
    TEST_ASSERT(ok);
    keel_token_bucket_destroy(&tb);
    TEST_END();
}

static void test_bucket_drain(void)
{
    TEST_BEGIN("throttle bucket: drained bucket rejects requests");
    keel_token_bucket_t tb;
    /* 100 burst, low rate so no meaningful refill in this test */
    keel_token_bucket_init(&tb, 0.001, 5.0);

    uint64_t t = now_ns();
    /* Drain all tokens */
    for (int i = 0; i < 5; i++)
        keel_token_bucket_consume(&tb, t);

    /* 6th consume should fail */
    bool ok = keel_token_bucket_consume(&tb, t);
    TEST_ASSERT(!ok);

    keel_token_bucket_destroy(&tb);
    TEST_END();
}

static void test_bucket_refill(void)
{
    TEST_BEGIN("throttle bucket: tokens refill after elapsed time");
    keel_token_bucket_t tb;
    /* rate=10000 RPS, burst=1 */
    keel_token_bucket_init(&tb, 10000.0, 1.0);

    /* Drain it */
    uint64_t t = now_ns();
    keel_token_bucket_consume(&tb, t);

    /* Advance by 1 second (1e9 ns) — should refill 10000 tokens (capped at 1) */
    bool ok = keel_token_bucket_consume(&tb, t + 1000000000ULL);
    TEST_ASSERT(ok);

    keel_token_bucket_destroy(&tb);
    TEST_END();
}

static void test_bucket_burst_cap(void)
{
    TEST_BEGIN("throttle bucket: tokens never exceed burst");
    keel_token_bucket_t tb;
    keel_token_bucket_init(&tb, 100.0, 5.0);

    /* Advance by 1 hour — tokens should clamp to burst (5) */
    uint64_t t = now_ns() + 3600ULL * 1000000000ULL;
    keel_token_bucket_consume(&tb, t); /* forces refill */
    TEST_ASSERT(tb.tokens <= 5.0 + 0.001);  /* check clamping after consume */

    keel_token_bucket_destroy(&tb);
    TEST_END();
}

static void test_bucket_zero_rate_clamped(void)
{
    TEST_BEGIN("throttle bucket: zero rate clamped to 1");
    keel_token_bucket_t tb;
    keel_token_bucket_init(&tb, 0.0, 0.0);
    TEST_ASSERT(tb.rate_rps >= 1.0);
    TEST_ASSERT(tb.burst >= 1.0);
    keel_token_bucket_destroy(&tb);
    TEST_END();
}

static void test_bucket_negative_rate_clamped(void)
{
    TEST_BEGIN("throttle bucket: negative rate clamped to 1");
    keel_token_bucket_t tb;
    keel_token_bucket_init(&tb, -50.0, -10.0);
    TEST_ASSERT(tb.rate_rps >= 1.0);
    keel_token_bucket_destroy(&tb);
    TEST_END();
}

static void test_bucket_high_rate_high_throughput(void)
{
    TEST_BEGIN("throttle bucket: high rate allows many consecutive consumes");
    keel_token_bucket_t tb;
    keel_token_bucket_init(&tb, 1e9, 1000.0);
    uint64_t t = now_ns();
    int granted = 0;
    for (int i = 0; i < 1000; i++) {
        if (keel_token_bucket_consume(&tb, t)) granted++;
    }
    TEST_ASSERT_EQ(granted, 1000);
    keel_token_bucket_destroy(&tb);
    TEST_END();
}

static void test_bucket_destroy_idempotent(void)
{
    TEST_BEGIN("throttle bucket: destroy is safe on zero-init struct");
    keel_token_bucket_t tb;
    memset(&tb, 0, sizeof(tb));
    /* Should not crash */
    keel_token_bucket_destroy(&tb);
    TEST_END();
}

static void test_bucket_null_safe(void)
{
    TEST_BEGIN("throttle bucket: NULL safe");
    keel_token_bucket_init(NULL, 100.0, 100.0);
    bool ok = keel_token_bucket_consume(NULL, 0);
    TEST_ASSERT(ok);   /* NULL bucket → pass-through = true */
    keel_token_bucket_destroy(NULL);
    TEST_END();
}

/* ============================================================================
 * §2 — Throttle rules collection
 * ============================================================================ */

static void test_rules_create_destroy(void)
{
    TEST_BEGIN("throttle rules: create and unref");
    keel_throttle_rules_t *tr = keel_throttle_rules_create();
    TEST_ASSERT_NOT_NULL(tr);
    TEST_ASSERT_EQ((int)tr->count, 0);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_rules_refcount(void)
{
    TEST_BEGIN("throttle rules: ref/unref increments/decrements safely");
    keel_throttle_rules_t *tr = keel_throttle_rules_create();
    keel_throttle_rules_ref(tr);     /* refcnt = 2 */
    keel_throttle_rules_unref(tr);   /* refcnt = 1 */
    keel_throttle_rules_unref(tr);   /* refcnt = 0 → freed */
    /* No crash means success */
    TEST_ASSERT(true);
    TEST_END();
}

static void test_rules_unref_null(void)
{
    TEST_BEGIN("throttle rules: unref NULL is safe");
    keel_throttle_rules_unref(NULL);
    keel_throttle_rules_ref(NULL);
    TEST_ASSERT(true);
    TEST_END();
}

/* ============================================================================
 * §3 — Rule matching
 * ============================================================================ */

/*
 * Build a minimal in-memory rule and verify keel_throttle_check() behaviour.
 * We construct rules directly (without INI config) to test the matcher logic.
 */

static keel_throttle_rules_t *make_rules_with_one(
    const char *match_regex,
    const char *match_user,
    const char *match_db,
    double rate, double burst)
{
    keel_throttle_rules_t *tr = keel_throttle_rules_create();
    if (!tr) return NULL;

    tr->rules = keel_calloc(1, sizeof(*tr->rules));
    if (!tr->rules) { keel_throttle_rules_unref(tr); return NULL; }

    keel_throttle_rule_t *r = &tr->rules[0];
    r->name    = keel_strdup("test.0");
    r->enabled = true;

    if (match_user)  r->match_user  = keel_strdup(match_user);
    if (match_db)    r->match_db    = keel_strdup(match_db);
    if (match_regex) {
        r->match_regex = keel_strdup(match_regex);
        if (regcomp(&r->regex_storage, match_regex, REG_EXTENDED | REG_NOSUB) == 0)
            r->regex_valid = true;
    }

    keel_token_bucket_init(&r->bucket, rate, burst);
    tr->count = 1;
    return tr;
}

static void test_match_no_constraints_matches_all(void)
{
    TEST_BEGIN("throttle match: rule with no constraints matches everything");
    keel_throttle_rules_t *tr = make_rules_with_one(NULL, NULL, NULL, 1e9, 1e6);
    const char *err = NULL;
    bool throttled = keel_throttle_check(tr, "SELECT 1", "u", "d", -1, now_ns(), &err);
    TEST_ASSERT(!throttled);   /* first request: tokens available */
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_match_user_match(void)
{
    TEST_BEGIN("throttle match: user match evaluates rule");
    keel_throttle_rules_t *tr = make_rules_with_one(NULL, "alice", NULL, 0.001, 1.0);
    uint64_t t = now_ns();
    /* Drain the bucket */
    keel_throttle_check(tr, "SELECT 1", "alice", "db", -1, t, NULL);
    /* Second should be throttled */
    bool throttled = keel_throttle_check(tr, "SELECT 1", "alice", "db", -1, t, NULL);
    TEST_ASSERT(throttled);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_match_user_no_match_skips(void)
{
    TEST_BEGIN("throttle match: different user skips rule");
    keel_throttle_rules_t *tr = make_rules_with_one(NULL, "alice", NULL, 0.001, 1.0);
    uint64_t t = now_ns();
    /* Even after "alice" drains it, "bob" is unaffected */
    keel_throttle_check(tr, "SELECT 1", "alice", "db", -1, t, NULL);
    bool throttled = keel_throttle_check(tr, "SELECT 1", "bob", "db", -1, t, NULL);
    TEST_ASSERT(!throttled);  /* bob skips the rule — no match */
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_match_db_match(void)
{
    TEST_BEGIN("throttle match: db match evaluates rule");
    keel_throttle_rules_t *tr = make_rules_with_one(NULL, NULL, "reports", 0.001, 1.0);
    uint64_t t = now_ns();
    keel_throttle_check(tr, "SELECT 1", "u", "reports", -1, t, NULL);
    bool throttled = keel_throttle_check(tr, "SELECT 1", "u", "reports", -1, t, NULL);
    TEST_ASSERT(throttled);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_match_db_no_match_skips(void)
{
    TEST_BEGIN("throttle match: different db skips rule");
    keel_throttle_rules_t *tr = make_rules_with_one(NULL, NULL, "reports", 0.001, 1.0);
    uint64_t t = now_ns();
    bool throttled = keel_throttle_check(tr, "SELECT 1", "u", "other_db", -1, t, NULL);
    TEST_ASSERT(!throttled);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_match_regex_match(void)
{
    TEST_BEGIN("throttle match: regex matches DROP TABLE");
    keel_throttle_rules_t *tr =
        make_rules_with_one("^DROP\\s+TABLE", NULL, NULL, 0.001, 1.0);
    uint64_t t = now_ns();
    /* First DROP — token consumed */
    keel_throttle_check(tr, "DROP TABLE foo", NULL, NULL, -1, t, NULL);
    /* Second DROP — throttled */
    bool throttled = keel_throttle_check(tr, "DROP TABLE bar", NULL, NULL, -1, t, NULL);
    TEST_ASSERT(throttled);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_match_regex_no_match_skips(void)
{
    TEST_BEGIN("throttle match: regex mismatch skips rule");
    keel_throttle_rules_t *tr =
        make_rules_with_one("^DROP\\s+TABLE", NULL, NULL, 0.001, 1.0);
    uint64_t t = now_ns();
    bool throttled = keel_throttle_check(tr, "SELECT * FROM t", NULL, NULL, -1, t, NULL);
    TEST_ASSERT(!throttled);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_match_combined_all_must_match(void)
{
    TEST_BEGIN("throttle match: all predicates must match");
    keel_throttle_rules_t *tr =
        make_rules_with_one("SELECT", "alice", "reports", 0.001, 1.0);
    uint64_t t = now_ns();
    /* alice/reports: match → consumes token */
    keel_throttle_check(tr, "SELECT 1", "alice", "reports", -1, t, NULL);
    /* alice/other_db: db mismatch → not throttled */
    bool t1 = keel_throttle_check(tr, "SELECT 1", "alice", "other_db", -1, t, NULL);
    /* bob/reports: user mismatch → not throttled */
    bool t2 = keel_throttle_check(tr, "SELECT 1", "bob", "reports", -1, t, NULL);
    TEST_ASSERT(!t1);
    TEST_ASSERT(!t2);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_disabled_rule_skipped(void)
{
    TEST_BEGIN("throttle match: disabled rule is skipped");
    keel_throttle_rules_t *tr = make_rules_with_one(NULL, NULL, NULL, 0.001, 1.0);
    tr->rules[0].enabled = false;
    uint64_t t = now_ns();
    bool throttled = keel_throttle_check(tr, "SELECT 1", "u", "d", -1, t, NULL);
    TEST_ASSERT(!throttled);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

/* ============================================================================
 * §4 — keel_throttle_check semantics
 * ============================================================================ */

static void test_check_empty_rules_pass(void)
{
    TEST_BEGIN("throttle check: empty rules collection passes all");
    keel_throttle_rules_t *tr = keel_throttle_rules_create();
    bool throttled = keel_throttle_check(tr, "SELECT 1", "u", "d", -1, 0, NULL);
    TEST_ASSERT(!throttled);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_check_null_rules_pass(void)
{
    TEST_BEGIN("throttle check: NULL rules collection passes all");
    bool throttled = keel_throttle_check(NULL, "SELECT 1", "u", "d", -1, 0, NULL);
    TEST_ASSERT(!throttled);
    TEST_END();
}

static void test_check_error_msg_set_on_throttle(void)
{
    TEST_BEGIN("throttle check: error_msg populated on throttle");
    keel_throttle_rules_t *tr = make_rules_with_one(NULL, NULL, NULL, 0.001, 1.0);
    /* Set custom error message */
    keel_free(tr->rules[0].error_msg);
    tr->rules[0].error_msg = keel_strdup("custom throttle message");

    uint64_t t = now_ns();
    keel_throttle_check(tr, "SELECT 1", "u", "d", -1, t, NULL);  /* drain */
    const char *err = NULL;
    bool throttled = keel_throttle_check(tr, "SELECT 1", "u", "d", -1, t, &err);
    TEST_ASSERT(throttled);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT(strstr(err, "custom throttle message") != NULL);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_check_default_error_msg(void)
{
    TEST_BEGIN("throttle check: default error_msg when rule has none");
    keel_throttle_rules_t *tr = make_rules_with_one(NULL, NULL, NULL, 0.001, 1.0);
    /* Ensure no custom msg */
    keel_free(tr->rules[0].error_msg);
    tr->rules[0].error_msg = NULL;

    uint64_t t = now_ns();
    keel_throttle_check(tr, "SELECT 1", "u", "d", -1, t, NULL);
    const char *err = NULL;
    bool throttled = keel_throttle_check(tr, "SELECT 1", "u", "d", -1, t, &err);
    TEST_ASSERT(throttled);
    TEST_ASSERT_NOT_NULL(err);
    TEST_ASSERT(strlen(err) > 0);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_check_first_match_wins(void)
{
    TEST_BEGIN("throttle check: first matching rule wins; others not evaluated");
    /* Two rules: rule[0] pass-through (high rate), rule[1] strict (rate 0) */
    keel_throttle_rules_t *tr = keel_throttle_rules_create();
    tr->rules = keel_calloc(2, sizeof(*tr->rules));
    tr->count = 2;

    /* rule 0: match "alice", high rate */
    tr->rules[0].name    = keel_strdup("r0");
    tr->rules[0].enabled = true;
    tr->rules[0].match_user = keel_strdup("alice");
    keel_token_bucket_init(&tr->rules[0].bucket, 1e9, 1e6);

    /* rule 1: no constraints, extremely strict (impossible rate) */
    tr->rules[1].name    = keel_strdup("r1");
    tr->rules[1].enabled = true;
    keel_token_bucket_init(&tr->rules[1].bucket, 0.001, 1.0);
    /* Pre-drain rule1 bucket */
    uint64_t t = now_ns();
    keel_token_bucket_consume(&tr->rules[1].bucket, t);

    /* alice hits rule0 first → not throttled (tokens available) */
    bool throttled = keel_throttle_check(tr, "SELECT 1", "alice", "d", -1, t, NULL);
    TEST_ASSERT(!throttled);

    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_check_null_sql_safe(void)
{
    TEST_BEGIN("throttle check: NULL sql is safe");
    keel_throttle_rules_t *tr = make_rules_with_one(NULL, NULL, NULL, 1e9, 1e6);
    bool throttled = keel_throttle_check(tr, NULL, "u", "d", -1, now_ns(), NULL);
    TEST_ASSERT(!throttled);   /* no regex constraint → always matches */
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_check_null_sql_with_regex_skips(void)
{
    TEST_BEGIN("throttle check: NULL sql skips regex rule");
    keel_throttle_rules_t *tr = make_rules_with_one("SELECT", NULL, NULL, 1e9, 1e6);
    bool throttled = keel_throttle_check(tr, NULL, "u", "d", -1, now_ns(), NULL);
    TEST_ASSERT(!throttled);   /* regex can't match NULL → rule skipped */
    keel_throttle_rules_unref(tr);
    TEST_END();
}

/* ============================================================================
 * §5 — Stats
 * ============================================================================ */

static void test_stats_queries_throttled_increments(void)
{
    TEST_BEGIN("throttle stats: queries_throttled counter increments");
    keel_throttle_rules_t *tr = make_rules_with_one(NULL, NULL, NULL, 0.001, 1.0);
    uint64_t t = now_ns();
    keel_throttle_check(tr, "SELECT 1", "u", "d", -1, t, NULL);   /* consume token */
    keel_throttle_check(tr, "SELECT 1", "u", "d", -1, t, NULL);   /* throttled #1 */
    keel_throttle_check(tr, "SELECT 1", "u", "d", -1, t, NULL);   /* throttled #2 */
    TEST_ASSERT_EQ((int)keel_throttle_total_rejected(tr), 2);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_stats_zero_initially(void)
{
    TEST_BEGIN("throttle stats: counter starts at zero");
    keel_throttle_rules_t *tr = keel_throttle_rules_create();
    TEST_ASSERT_EQ((int)keel_throttle_total_rejected(tr), 0);
    keel_throttle_rules_unref(tr);
    TEST_END();
}

static void test_stats_null_safe(void)
{
    TEST_BEGIN("throttle stats: null safe returns 0");
    uint64_t n = keel_throttle_total_rejected(NULL);
    TEST_ASSERT_EQ((int)n, 0);
    TEST_END();
}

/* ============================================================================
 * §6 — Null and edge-case guards
 * ============================================================================ */

static void test_replace_null_safe(void)
{
    TEST_BEGIN("throttle replace: null-safe");
    keel_throttle_rules_replace(NULL, NULL);
    keel_throttle_rules_t *tr = keel_throttle_rules_create();
    keel_throttle_rules_replace(&tr, NULL);
    /* tr should now be NULL and the old object freed */
    TEST_ASSERT(tr == NULL);
    TEST_END();
}

static void test_replace_swaps_and_refs(void)
{
    TEST_BEGIN("throttle replace: swaps rule sets");
    keel_throttle_rules_t *old_tr = keel_throttle_rules_create();
    keel_throttle_rules_t *new_tr = keel_throttle_rules_create();
    keel_throttle_rules_t *slot = old_tr;
    keel_throttle_rules_replace(&slot, new_tr);
    TEST_ASSERT(slot == new_tr);
    keel_throttle_rules_unref(new_tr); /* for the extra ref from replace */
    keel_throttle_rules_unref(slot);
    TEST_END();
}

static void test_per_client_isolation(void)
{
    TEST_BEGIN("per-client: separate buckets isolate clients");

    /* Rule with burst=1 and very low rate (won't refill during test) */
    keel_throttle_rules_t *tr = keel_throttle_rules_create();
    keel_throttle_rule_t *r = keel_realloc(tr->rules, sizeof(*r));
    tr->rules = r;
    memset(r, 0, sizeof(*r));
    r->name       = keel_strdup("test-per-client");
    r->enabled    = true;
    r->per_client = true;
    keel_client_bucket_map_init(&r->client_map, 0.001, 1.0);
    tr->count = 1;

    uint64_t t = now_ns();

    /* Client fd=10: first query granted, second throttled */
    bool g1 = !keel_throttle_check(tr, "SELECT 1", NULL, NULL, 10, t, NULL);
    bool g2 = !keel_throttle_check(tr, "SELECT 1", NULL, NULL, 10, t, NULL);
    /* Client fd=20: first query still granted (own bucket) */
    bool g3 = !keel_throttle_check(tr, "SELECT 1", NULL, NULL, 20, t, NULL);

    TEST_ASSERT(g1);   /* fd=10 first query granted */
    TEST_ASSERT(!g2);  /* fd=10 second query throttled */
    TEST_ASSERT(g3);   /* fd=20 first query granted independently */

    keel_throttle_rules_unref(tr);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    /* §1 Token bucket */
    test_bucket_init();
    test_bucket_consume_single();
    test_bucket_drain();
    test_bucket_refill();
    test_bucket_burst_cap();
    test_bucket_zero_rate_clamped();
    test_bucket_negative_rate_clamped();
    test_bucket_high_rate_high_throughput();
    test_bucket_destroy_idempotent();
    test_bucket_null_safe();

    /* §2 Rules collection lifecycle */
    test_rules_create_destroy();
    test_rules_refcount();
    test_rules_unref_null();

    /* §3 Rule matching */
    test_match_no_constraints_matches_all();
    test_match_user_match();
    test_match_user_no_match_skips();
    test_match_db_match();
    test_match_db_no_match_skips();
    test_match_regex_match();
    test_match_regex_no_match_skips();
    test_match_combined_all_must_match();
    test_disabled_rule_skipped();

    /* §4 keel_throttle_check */
    test_check_empty_rules_pass();
    test_check_null_rules_pass();
    test_check_error_msg_set_on_throttle();
    test_check_default_error_msg();
    test_check_first_match_wins();
    test_check_null_sql_safe();
    test_check_null_sql_with_regex_skips();

    /* §5 Stats */
    test_stats_queries_throttled_increments();
    test_stats_zero_initially();
    test_stats_null_safe();

    /* §6 Guards */
    test_replace_null_safe();
    test_replace_swaps_and_refs();

    /* §7 Per-client throttling */
    test_per_client_isolation();

    return test_summary();
}
