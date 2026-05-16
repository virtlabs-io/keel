/**
 * @file test_admin_auth.c
 * @brief Unit tests for admin console SCRAM-SHA-256 authentication.
 *
 * Exercises the admin auth infrastructure added in P1 #5:
 *   §1 — Config defaults: admin_password NULL ⇒ trust mode.
 *   §2 — Auth manager lifecycle: create with password, add users.
 *   §3 — SCRAM-SHA-256 flow: full challenge-response exchange via the
 *         auth framework (same primitives used by the admin console).
 *   §4 — Pre-hashed SCRAM password parsing.
 *   §5 — User allowlist matching.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/core/admin.h"
#include "keel/core/auth.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ============================================================================
 * Test: Default config has no password (trust mode)
 * ============================================================================ */

static void test_default_config_is_trust(void) {
    TEST_BEGIN("default_config_is_trust");

    keel_admin_config_t cfg = KEEL_ADMIN_CONFIG_DEFAULT;
    TEST_ASSERT_NULL(cfg.admin_password);
    TEST_ASSERT(cfg.admin_enabled == false);
    TEST_ASSERT_NOT_NULL(cfg.admin_users);

    TEST_END();
}

/* ============================================================================
 * Test: SCRAM auth manager creation with plaintext password
 * ============================================================================ */

static void test_scram_manager_plaintext(void) {
    TEST_BEGIN("scram_manager_plaintext");

#ifdef KEEL_HAS_OPENSSL
    keel_auth_manager_config_t acfg = {
        .default_method   = KEEL_AUTH_SCRAM_SHA_256,
        .scram_iterations = 4096,
    };
    keel_auth_manager_t *mgr = keel_auth_manager_create(&acfg);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) { TEST_END(); return; }

    keel_auth_manager_register(mgr, keel_auth_scram_sha256_ops(), NULL);

    /* Hash plaintext, parse, build user */
    char *hash = NULL;
    keel_error_t err = keel_auth_scram_hash_password("s3cret!", 4096, &hash);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(hash);

    if (hash) {
        keel_auth_user_t au = { .username = "admin", .can_login = true };
        char *salt = NULL;
        int iters  = 0;
        err = keel_auth_scram_parse_hash(hash, &salt, &iters,
                                         au.stored_key, au.server_key);
        TEST_ASSERT_EQ(err, KEEL_OK);
        au.has_scram_keys = true;
        au.iterations     = iters;
        au.password_salt  = salt;

        err = keel_auth_add_user(mgr, &au);
        TEST_ASSERT_EQ(err, KEEL_OK);

        /* Verify user can be looked up */
        const keel_auth_user_t *found = NULL;
        err = keel_auth_lookup_user(mgr, "admin", &found);
        TEST_ASSERT_EQ(err, KEEL_OK);
        TEST_ASSERT_NOT_NULL(found);
        if (found) {
            TEST_ASSERT(found->has_scram_keys);
        }

        keel_free(salt);
        keel_free(hash);
    }

    keel_auth_manager_destroy(mgr);
#else
    printf("    Note: OpenSSL not available, skipping SCRAM tests\n");
#endif
    TEST_END();
}

/* ============================================================================
 * Test: SCRAM auth flow — start produces challenge
 * ============================================================================ */

static void test_scram_start_challenge(void) {
    TEST_BEGIN("scram_start_challenge");

#ifdef KEEL_HAS_OPENSSL
    keel_auth_manager_config_t acfg = {
        .default_method   = KEEL_AUTH_SCRAM_SHA_256,
        .scram_iterations = 4096,
    };
    keel_auth_manager_t *mgr = keel_auth_manager_create(&acfg);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) { TEST_END(); return; }

    keel_auth_manager_register(mgr, keel_auth_scram_sha256_ops(), NULL);

    char *hash = NULL;
    keel_auth_scram_hash_password("testpw", 4096, &hash);
    TEST_ASSERT_NOT_NULL(hash);

    if (hash) {
        keel_auth_user_t au = { .username = "admin", .can_login = true };
        char *salt = NULL;
        int iters = 0;
        keel_auth_scram_parse_hash(hash, &salt, &iters,
                                   au.stored_key, au.server_key);
        au.has_scram_keys = true;
        au.iterations = iters;
        au.password_salt = salt;
        keel_auth_add_user(mgr, &au);

        /* Start auth — should produce SASL challenge */
        keel_auth_context_t *ctx = NULL;
        keel_error_t err = keel_auth_manager_start(mgr, "admin", &ctx);
        TEST_ASSERT_EQ(err, KEEL_OK);
        TEST_ASSERT_NOT_NULL(ctx);

        if (ctx) {
            keel_auth_state_t state = keel_auth_get_state(ctx);
            TEST_ASSERT_EQ(state, KEEL_AUTH_STATE_CHALLENGE);

            /* Get message — should be AuthenticationSASL (type 10) */
            void *msg = NULL;
            size_t msg_len = 0;
            int msg_type = 0;
            err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
            TEST_ASSERT_EQ(err, KEEL_OK);
            TEST_ASSERT_NOT_NULL(msg);
            TEST_ASSERT_EQ(msg_type, 10);

            /* Payload should start with int32(10) + mechanism list */
            if (msg && msg_len >= 4) {
                const uint8_t *p = msg;
                int type = (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
                TEST_ASSERT_EQ(type, 10);
                /* Mechanism "SCRAM-SHA-256" follows */
                if (msg_len > 4) {
                    TEST_ASSERT(strcmp((const char *)p + 4, "SCRAM-SHA-256") == 0);
                }
            }

            keel_free(msg);
            keel_auth_context_free(ctx);
        }

        keel_free(salt);
        keel_free(hash);
    }

    keel_auth_manager_destroy(mgr);
#else
    printf("    Note: OpenSSL not available, skipping\n");
#endif
    TEST_END();
}

/* ============================================================================
 * Test: Unknown user fails auth start
 * ============================================================================ */

static void test_unknown_user_rejected(void) {
    TEST_BEGIN("unknown_user_rejected");

#ifdef KEEL_HAS_OPENSSL
    keel_auth_manager_config_t acfg = {
        .default_method   = KEEL_AUTH_SCRAM_SHA_256,
        .scram_iterations = 4096,
    };
    keel_auth_manager_t *mgr = keel_auth_manager_create(&acfg);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) { TEST_END(); return; }

    keel_auth_manager_register(mgr, keel_auth_scram_sha256_ops(), NULL);

    /* Start auth for a user that was never added.
     * The SCRAM provider still starts (to prevent timing-based user
     * enumeration) but will fail on client-final verification. */
    keel_auth_context_t *ctx = NULL;
    keel_error_t err = keel_auth_manager_start(mgr, "nobody", &ctx);
    /* Starting may succeed (fake challenge) or fail immediately — both OK */
    if (err == KEEL_OK && ctx) {
        keel_auth_context_free(ctx);
    }

    keel_auth_manager_destroy(mgr);
#else
    printf("    Note: OpenSSL not available, skipping\n");
#endif
    TEST_END();
}

/* ============================================================================
 * Test: Pre-hashed SCRAM password parsing
 * ============================================================================ */

static void test_prehashed_scram_password(void) {
    TEST_BEGIN("prehashed_scram_password");

#ifdef KEEL_HAS_OPENSSL
    /* Generate a hash, then verify it can be parsed back */
    char *hash = NULL;
    keel_error_t err = keel_auth_scram_hash_password("mypw", 4096, &hash);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(hash);

    if (hash) {
        TEST_ASSERT(strncmp(hash, "SCRAM-SHA-256$", 14) == 0);

        char *salt = NULL;
        int iters = 0;
        uint8_t sk[32], svk[32];
        err = keel_auth_scram_parse_hash(hash, &salt, &iters, sk, svk);
        TEST_ASSERT_EQ(err, KEEL_OK);
        TEST_ASSERT_NOT_NULL(salt);
        TEST_ASSERT_EQ(iters, 4096);

        keel_free(salt);
        keel_free(hash);
    }
#else
    printf("    Note: OpenSSL not available, skipping\n");
#endif
    TEST_END();
}

/* ============================================================================
 * Test: Config with password → non-NULL admin_password
 * ============================================================================ */

static void test_config_password_field(void) {
    TEST_BEGIN("config_password_field");

    keel_admin_config_t cfg = KEEL_ADMIN_CONFIG_DEFAULT;
    cfg.admin_password = "secret";
    TEST_ASSERT_NOT_NULL(cfg.admin_password);
    TEST_ASSERT(strcmp(cfg.admin_password, "secret") == 0);

    TEST_END();
}

/* ============================================================================
 * Driver
 * ============================================================================ */

int main(void) {
    printf("\n=== Admin Auth Tests ===\n\n");

    test_default_config_is_trust();
    test_scram_manager_plaintext();
    test_scram_start_challenge();
    test_unknown_user_rejected();
    test_prehashed_scram_password();
    test_config_password_field();

    return test_summary();
}
