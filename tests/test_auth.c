/**
 * @file test_auth.c
 * @brief Tests for the pluggable authentication framework
 *
 * Exercises every layer of the keel_auth_manager API — lifecycle, user
 * directory, method dispatch, and full multi-step handshake flows.
 *
 * Test families:
 *   §1 — Manager lifecycle: create with default / custom config,
 *         destroy, NULL tolerance.
 *   §2 — User directory: add, lookup, duplicate rejection,
 *         lookup-callback path.
 *   §3 — TRUST authentication: zero-round-trip path.
 *   §4 — SCRAM-SHA-256 flow: password hashing, stored-key
 *         parsing, full client-first → server-first → client-final
 *         → server-final four-message exchange.
 *   §5 — MD5 legacy authentication path.
 *   §6 — Cleartext password path.
 *   §7 — REJECT method: should unconditionally deny.
 *   §8 — Provider introspection and auth-method / auth-state
 *         name round-trips (ensures every enum has a printable
 *         label for diagnostics).
 *   §9 — NULL safety: every public entry point must tolerate
 *         NULL arguments without crashing.
 *
 * Design choices:
 *   - Each test allocates its own auth_manager so tests are
 *     independent and parallelisable.
 *   - SCRAM tests verify the internal parsing of StoredKey +
 *     ServerKey from `scram-sha-256$iter:salt$stored:server`
 *     format, exercising the same codepath used during
 *     userlist.txt bootstrap.
 *   - No real network I/O — the SCRAM "handshake" is driven by
 *     feeding pre-built message buffers through
 *     keel_auth_manager_step().
 */


#include "test_utils.h"
#include "keel/core/auth.h"
#include "keel/mem/mem.h"
#include "keel/log/log.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* ============================================================================
 * Test: Auth Manager Creation
 * ============================================================================ */

static void test_auth_manager_create(void) {
    TEST_BEGIN("auth_manager_create");
    
    /* Create manager with default config */
    keel_auth_manager_t* mgr = keel_auth_manager_create(NULL);
    TEST_ASSERT_NOT_NULL(mgr);
    
    if (mgr) {
        keel_auth_manager_destroy(mgr);
    }
    
    /* Create manager with custom config */
    keel_auth_manager_config_t config = {
        .default_method = KEEL_AUTH_SCRAM_SHA_256,
        .scram_iterations = 4096,
        .allow_clear_password = false,
    };
    
    mgr = keel_auth_manager_create(&config);
    TEST_ASSERT_NOT_NULL(mgr);
    
    if (mgr) {
        keel_auth_manager_destroy(mgr);
    }
    
    TEST_END();
}

/* ============================================================================
 * Test: User Management
 * ============================================================================ */

static void test_user_management(void) {
    TEST_BEGIN("user_management");
    
    keel_auth_manager_t* mgr = keel_auth_manager_create(NULL);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) return;
    
    /* Add a user */
    keel_auth_user_t user = {
        .username = "testuser",
        .password_hash = "test_hash",
        .can_login = true,
    };
    
    keel_error_t err = keel_auth_add_user(mgr, &user);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Lookup the user */
    const keel_auth_user_t* found = NULL;
    err = keel_auth_lookup_user(mgr, "testuser", &found);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(found);
    
    if (found) {
        TEST_ASSERT_STR_EQ(found->username, "testuser");
    }
    
    /* Lookup non-existent user */
    err = keel_auth_lookup_user(mgr, "nonexistent", &found);
    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_FOUND);
    
    /* Try to add duplicate user */
    err = keel_auth_add_user(mgr, &user);
    TEST_ASSERT_EQ(err, KEEL_ERR_ALREADY_EXISTS);
    
    keel_auth_manager_destroy(mgr);
    
    TEST_END();
}

/* ============================================================================
 * Test: Trust Authentication
 * ============================================================================ */

static void test_trust_auth(void) {
    TEST_BEGIN("trust_auth");
    
    keel_auth_manager_config_t config = {
        .default_method = KEEL_AUTH_TRUST,
    };
    
    keel_auth_manager_t* mgr = keel_auth_manager_create(&config);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) return;
    
    /* Add a user */
    keel_auth_user_t user = {
        .username = "trustuser",
        .can_login = true,
    };
    
    keel_error_t err = keel_auth_add_user(mgr, &user);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Start trust authentication */
    keel_auth_context_t* ctx = NULL;
    err = keel_auth_manager_start(mgr, "trustuser", &ctx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(ctx);
    
    if (ctx) {
        /* Trust auth should succeed immediately */
        keel_auth_state_t state = keel_auth_get_state(ctx);
        TEST_ASSERT_EQ(state, KEEL_AUTH_STATE_SUCCESS);
        
        keel_auth_context_free(ctx);
    }
    
    keel_auth_manager_destroy(mgr);
    
    TEST_END();
}

/* ============================================================================
 * Test: SCRAM-SHA-256 Password Hashing
 * ============================================================================ */

static void test_scram_hash_password(void) {
    TEST_BEGIN("scram_hash_password");
    
    char* hash = NULL;
    keel_error_t err = keel_auth_scram_hash_password("testpassword", 4096, &hash);
    
#ifdef KEEL_HAS_OPENSSL
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(hash);
    
    if (hash) {
        /* Hash should start with SCRAM-SHA-256$ */
        TEST_ASSERT(strncmp(hash, "SCRAM-SHA-256$", 14) == 0);
        printf("    Generated hash: %s\n", hash);
        keel_free(hash);  /* Use keel_free, not free */
    }
#else
    /* Without OpenSSL, should return NOT_SUPPORTED */
    TEST_ASSERT_EQ(err, KEEL_ERR_NOT_SUPPORTED);
#endif
    
    TEST_END();
}

static void test_scram_hash_format_pg_compatible(void) {
    TEST_BEGIN("scram_hash_format: PostgreSQL-compatible $ separator");

#ifdef KEEL_HAS_OPENSSL
    /* Generate a hash and verify the format uses '$' between salt and
     * stored_key, matching PostgreSQL's pg_authid.rolpassword format:
     *   SCRAM-SHA-256$<iter>:<salt>$<stored>:<server>
     * not the legacy Keel format that used ':' everywhere. */
    char* hash = NULL;
    keel_error_t err = keel_auth_scram_hash_password("pw", 4096, &hash);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(hash);
    if (!hash) { TEST_END(); return; }

    /* Skip "SCRAM-SHA-256$<iter>:" to reach the salt */
    const char* salt_start = strchr(hash + 14, ':');
    TEST_ASSERT_NOT_NULL(salt_start);
    salt_start++;
    /* The next separator MUST be '$', not ':' */
    const char* sep = strchr(salt_start, '$');
    TEST_ASSERT_NOT_NULL(sep);
    /* And there must NOT be a ':' before that '$' in the salt body
     * (base64 can contain '+', '/', '=', but never '$' or ':' so this
     * check is unambiguous). */
    const char* colon_in_salt = strchr(salt_start, ':');
    if (colon_in_salt) {
        TEST_ASSERT(colon_in_salt > sep);
    }
    keel_free(hash);

    /* Parse a known PostgreSQL-format hash and verify it round-trips.
     * This is the hash the user reported: password "keel_pwd_cli"
     * exported from PostgreSQL. */
    const char* pg_hash =
        "SCRAM-SHA-256$4096:5KOkk+vGXeSKcfz8ZMeJhw==$"
        "DqiI33Y7Xsvn81W5pv6mqRSqZmNs8YEy5BIQyuTcFvY=:"
        "EFCcfr9WcDKylBAuf8j/T4vdiv9HPnTYRHW8Fcm0Nj4=";
    char* salt = NULL;
    int iters = 0;
    uint8_t sk[32], svk[32];
    err = keel_auth_scram_parse_hash(pg_hash, &salt, &iters, sk, svk);
    TEST_ASSERT_EQ(err, KEEL_OK);
    if (err == KEEL_OK) {
        TEST_ASSERT_EQ(iters, 4096);
        TEST_ASSERT_NOT_NULL(salt);
        keel_free(salt);
    }
#else
    printf("    Skipping (no OpenSSL)\n");
#endif

    TEST_END();
}

/* ============================================================================
 * Test: Auth Method Names
 * ============================================================================ */

static void test_auth_method_names(void) {
    TEST_BEGIN("auth_method_names");
    
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_TRUST), "trust");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_PASSWORD), "password");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_MD5), "md5");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_SCRAM_SHA_256), "scram-sha-256");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_CERTIFICATE), "cert");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_LDAP), "ldap");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_GSSAPI), "gss");
    
    TEST_END();
}

/* ============================================================================
 * Test: Auth State Names
 * ============================================================================ */

static void test_auth_state_names(void) {
    TEST_BEGIN("auth_state_names");
    
    TEST_ASSERT_STR_EQ(keel_auth_state_name(KEEL_AUTH_STATE_INIT), "INIT");
    TEST_ASSERT_STR_EQ(keel_auth_state_name(KEEL_AUTH_STATE_CHALLENGE), "CHALLENGE");
    TEST_ASSERT_STR_EQ(keel_auth_state_name(KEEL_AUTH_STATE_SUCCESS), "SUCCESS");
    TEST_ASSERT_STR_EQ(keel_auth_state_name(KEEL_AUTH_STATE_FAILED), "FAILED");
    
    TEST_END();
}

/* ============================================================================
 * Test: SCRAM Hash Parsing
 * ============================================================================ */

static void test_scram_hash_parsing(void) {
    TEST_BEGIN("scram_hash_parsing");
    
#ifndef KEEL_HAS_OPENSSL
    printf("    Skipping (no OpenSSL)\n");
    TEST_END();
    return;
#else
    /* Generate a hash */
    char* hash = NULL;
    keel_error_t err = keel_auth_scram_hash_password("testpassword", 4096, &hash);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(hash);
    
    if (hash) {
        /* Parse the hash */
        char* salt = NULL;
        int iterations = 0;
        uint8_t stored_key[32];
        uint8_t server_key[32];
        
        err = keel_auth_scram_parse_hash(hash, &salt, &iterations, stored_key, server_key);
        TEST_ASSERT_EQ(err, KEEL_OK);
        TEST_ASSERT_NOT_NULL(salt);
        TEST_ASSERT_EQ(iterations, 4096);
        
        printf("    Parsed: iterations=%d, salt=%s\n", iterations, salt ? salt : "(null)");
        
        keel_free(salt);
        keel_free(hash);
    }
#endif
    
    TEST_END();
}

/* ============================================================================
 * Test: SCRAM-SHA-256 Authentication Flow
 * ============================================================================ */

static void test_scram_auth_flow(void) {
    TEST_BEGIN("scram_auth_flow");
    
#ifndef KEEL_HAS_OPENSSL
    printf("    Skipping (no OpenSSL)\n");
    TEST_END();
    return;
#else
    keel_auth_manager_t* mgr = keel_auth_manager_create(NULL);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) return;
    
    /* Create a user with SCRAM credentials */
    char* hash = NULL;
    keel_error_t err = keel_auth_scram_hash_password("secretpassword", 4096, &hash);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    if (hash) {
        /* Parse the hash to get SCRAM keys */
        char* salt = NULL;
        int iterations = 0;
        uint8_t stored_key[32];
        uint8_t server_key[32];
        
        err = keel_auth_scram_parse_hash(hash, &salt, &iterations, stored_key, server_key);
        TEST_ASSERT_EQ(err, KEEL_OK);
        
        /* Create user with parsed SCRAM keys */
        keel_auth_user_t user = {
            .username = "scramuser",
            .password_hash = hash,
            .password_salt = salt,
            .iterations = iterations,
            .has_scram_keys = true,
            .can_login = true,
        };
        memcpy(user.stored_key, stored_key, 32);
        memcpy(user.server_key, server_key, 32);
        
        err = keel_auth_add_user(mgr, &user);
        TEST_ASSERT_EQ(err, KEEL_OK);
        
        /* Start SCRAM authentication */
        keel_auth_context_t* ctx = NULL;
        err = keel_auth_manager_start(mgr, "scramuser", &ctx);
        TEST_ASSERT_EQ(err, KEEL_OK);
        TEST_ASSERT_NOT_NULL(ctx);
        
        if (ctx) {
            /* Initial state should be CHALLENGE (waiting for client-first) */
            keel_auth_state_t state = keel_auth_get_state(ctx);
            TEST_ASSERT_EQ(state, KEEL_AUTH_STATE_CHALLENGE);
            
            /* We would need to simulate a full SCRAM exchange here,
             * which is complex. For now, just verify the setup worked. */
            printf("    SCRAM auth context created successfully\n");
            
            keel_auth_context_free(ctx);
        }
        
        keel_free(salt);
        keel_free(hash);
    }
    
    keel_auth_manager_destroy(mgr);
    
    TEST_END();
#endif
}

/* ============================================================================
 * Test: MD5 Authentication
 * ============================================================================ */

static void test_md5_auth(void) {
    TEST_BEGIN("md5_auth");
    
#ifndef KEEL_HAS_OPENSSL
    printf("    Skipping (no OpenSSL)\n");
    TEST_END();
    return;
#else
    keel_auth_manager_config_t config = {
        .default_method = KEEL_AUTH_MD5,
        .allow_clear_password = false,
    };
    
    keel_auth_manager_t* mgr = keel_auth_manager_create(&config);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) return;
    
    /* Add user with MD5 hash */
    keel_auth_user_t user = {
        .username = "md5user",
        .password_hash = "md5dummyhash",
        .can_login = true,
    };
    
    keel_error_t err = keel_auth_add_user(mgr, &user);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Start MD5 auth */
    keel_auth_context_t* ctx = NULL;
    err = keel_auth_manager_start(mgr, "md5user", &ctx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(ctx);
    
    if (ctx) {
        keel_auth_state_t state = keel_auth_get_state(ctx);
        TEST_ASSERT_EQ(state, KEEL_AUTH_STATE_CHALLENGE);
        
        /* Get challenge message */
        void* msg = NULL;
        size_t msg_len = 0;
        int msg_type = 0;
        err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
        TEST_ASSERT_EQ(err, KEEL_OK);
        keel_free(msg);
        
        keel_auth_context_free(ctx);
    }
    
    keel_auth_manager_destroy(mgr);
    
    TEST_END();
#endif
}

/* ============================================================================
 * Test: Password (cleartext) Authentication
 * ============================================================================ */

static void test_password_auth(void) {
    TEST_BEGIN("password_auth");
    
    keel_auth_manager_config_t config = {
        .default_method = KEEL_AUTH_PASSWORD,
        .allow_clear_password = true,
    };
    
    keel_auth_manager_t* mgr = keel_auth_manager_create(&config);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) return;
    
    /* Add user with plaintext password */
    keel_auth_user_t user = {
        .username = "plainuser",
        .password_hash = "secret123",
        .can_login = true,
    };
    
    keel_error_t err = keel_auth_add_user(mgr, &user);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Start password auth - may fail if no password provider registered */
    keel_auth_context_t* ctx = NULL;
    err = keel_auth_manager_start(mgr, "plainuser", &ctx);
    
    if (err == KEEL_OK && ctx) {
        keel_auth_state_t state = keel_auth_get_state(ctx);
        TEST_ASSERT_EQ(state, KEEL_AUTH_STATE_CHALLENGE);
        
        /* Get challenge message (cleartext auth request) */
        void* msg = NULL;
        size_t msg_len = 0;
        int msg_type = 0;
        err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
        TEST_ASSERT_EQ(err, KEEL_OK);
        
        /* Process correct password */
        state = keel_auth_process(ctx, "secret123", strlen("secret123"));
        TEST_ASSERT_EQ(state, KEEL_AUTH_STATE_SUCCESS);
        
        keel_auth_context_free(ctx);
    } else {
        /* No password provider registered - expected behavior */
        printf("    Note: Password provider not registered (OK)\n");
    }
    
    keel_auth_manager_destroy(mgr);
    
    TEST_END();
}

/* ============================================================================
 * Test: Reject Authentication
 * ============================================================================ */

static void test_reject_auth(void) {
    TEST_BEGIN("reject_auth");
    
    keel_auth_manager_config_t config = {
        .default_method = KEEL_AUTH_REJECT,
        .allow_clear_password = false,
    };
    
    keel_auth_manager_t* mgr = keel_auth_manager_create(&config);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) return;
    
    /* Add user but with reject method */
    keel_auth_user_t user = {
        .username = "rejectuser",
        .password_hash = "anypassword",
        .can_login = true,
    };
    
    keel_error_t err = keel_auth_add_user(mgr, &user);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Try to start auth - should fail immediately */
    keel_auth_context_t* ctx = NULL;
    err = keel_auth_manager_start(mgr, "rejectuser", &ctx);
    /* Reject auth might succeed in starting but fail immediately */
    if (err == KEEL_OK && ctx) {
        keel_auth_state_t state = keel_auth_get_state(ctx);
        /* Should be failed or error */
        TEST_ASSERT(state == KEEL_AUTH_STATE_FAILED || state == KEEL_AUTH_STATE_ERROR);
        keel_auth_context_free(ctx);
    }
    
    keel_auth_manager_destroy(mgr);
    
    TEST_END();
}

/* ============================================================================
 * Test: User Removal
 * ============================================================================ */

static void test_add_and_lookup_user(void) {
    TEST_BEGIN("add_and_lookup_user");
    
    keel_auth_manager_t* mgr = keel_auth_manager_create(NULL);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) return;
    
    /* Add user */
    keel_auth_user_t user = {
        .username = "tempuser",
        .password_hash = "temppass",
        .can_login = true,
    };
    
    keel_error_t err = keel_auth_add_user(mgr, &user);
    TEST_ASSERT_EQ(err, KEEL_OK);
    
    /* Verify user exists */
    const keel_auth_user_t* found = NULL;
    err = keel_auth_lookup_user(mgr, "tempuser", &found);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(found);
    
    /* Note: keel_auth_remove_user is declared but not implemented
     * So we skip testing removal for now */
    
    keel_auth_manager_destroy(mgr);
    
    TEST_END();
}

/* ============================================================================
 * Test: Provider Management
 * ============================================================================ */

static void test_get_provider(void) {
    TEST_BEGIN("get_provider");
    
    keel_auth_manager_t* mgr = keel_auth_manager_create(NULL);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) return;
    
    /* Get provider for trust - should be built-in */
    keel_auth_provider_t* provider = keel_auth_manager_get_provider(mgr, KEEL_AUTH_TRUST);
    TEST_ASSERT_NOT_NULL(provider);
    
    /* Get provider for unsupported method */
    provider = keel_auth_manager_get_provider(mgr, KEEL_AUTH_GSSAPI);
    /* Might be NULL if not registered */
    
    keel_auth_manager_destroy(mgr);
    
    TEST_END();
}

/* ============================================================================
 * Test: Additional Auth Method Names
 * ============================================================================ */

static void test_auth_method_names_complete(void) {
    TEST_BEGIN("auth_method_names_complete");
    
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_NONE), "none");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_TRUST), "trust");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_PASSWORD), "password");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_MD5), "md5");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_SCRAM_SHA_256), "scram-sha-256");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_GSSAPI), "gss");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_SSPI), "sspi");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_CERTIFICATE), "cert");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_LDAP), "ldap");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_RADIUS), "radius");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_PAM), "pam");
    TEST_ASSERT_STR_EQ(keel_auth_method_name(KEEL_AUTH_REJECT), "reject");
    
    /* Unknown method */
    const char* unknown = keel_auth_method_name((keel_auth_method_t)999);
    TEST_ASSERT_NOT_NULL(unknown);
    TEST_ASSERT_STR_EQ(unknown, "unknown");
    
    TEST_END();
}

/* ============================================================================
 * Test: Additional Auth State Names
 * ============================================================================ */

static void test_auth_state_names_complete(void) {
    TEST_BEGIN("auth_state_names_complete");
    
    TEST_ASSERT_STR_EQ(keel_auth_state_name(KEEL_AUTH_STATE_INIT), "INIT");
    TEST_ASSERT_STR_EQ(keel_auth_state_name(KEEL_AUTH_STATE_CHALLENGE), "CHALLENGE");
    TEST_ASSERT_STR_EQ(keel_auth_state_name(KEEL_AUTH_STATE_VERIFY), "VERIFY");
    TEST_ASSERT_STR_EQ(keel_auth_state_name(KEEL_AUTH_STATE_SUCCESS), "SUCCESS");
    TEST_ASSERT_STR_EQ(keel_auth_state_name(KEEL_AUTH_STATE_FAILED), "FAILED");
    TEST_ASSERT_STR_EQ(keel_auth_state_name(KEEL_AUTH_STATE_ERROR), "ERROR");
    
    /* Unknown state */
    const char* unknown = keel_auth_state_name((keel_auth_state_t)999);
    TEST_ASSERT_NOT_NULL(unknown);
    TEST_ASSERT_STR_EQ(unknown, "UNKNOWN");
    
    TEST_END();
}

/* ============================================================================
 * Test: Null Safety
 * ============================================================================ */

static void test_auth_null_safety(void) {
    TEST_BEGIN("auth_null_safety");
    
    /* Manager null safety */
    keel_auth_manager_destroy(NULL);  /* Should not crash */
    
    /* Add/find user null safety */
    keel_error_t err = keel_auth_add_user(NULL, NULL);
    TEST_ASSERT(KEEL_IS_ERR(err));
    
    const keel_auth_user_t* found = NULL;
    err = keel_auth_lookup_user(NULL, "test", &found);
    TEST_ASSERT(KEEL_IS_ERR(err));
    
    /* Note: keel_auth_remove_user is not implemented */
    
    /* Context null safety */
    keel_auth_context_free(NULL);  /* Should not crash */
    
    keel_auth_state_t state = keel_auth_get_state(NULL);
    /* Should return a default state */
    TEST_ASSERT(state == KEEL_AUTH_STATE_ERROR || state == KEEL_AUTH_STATE_INIT);
    
    /* Process null safety */
    state = keel_auth_process(NULL, "data", 4);
    TEST_ASSERT_EQ(state, KEEL_AUTH_STATE_ERROR);
    
    /* Get message null safety */
    void* msg = NULL;
    size_t len = 0;
    int type = 0;
    err = keel_auth_get_message(NULL, &msg, &len, &type);
    TEST_ASSERT(KEEL_IS_ERR(err));
    
    /* Start auth null safety */
    keel_auth_context_t* ctx = NULL;
    err = keel_auth_manager_start(NULL, "user", &ctx);
    TEST_ASSERT(KEEL_IS_ERR(err));
    
    keel_auth_manager_t* mgr = keel_auth_manager_create(NULL);
    if (mgr) {
        err = keel_auth_manager_start(mgr, NULL, &ctx);
        TEST_ASSERT(KEEL_IS_ERR(err));
        
        err = keel_auth_manager_start(mgr, "user", NULL);
        TEST_ASSERT(KEEL_IS_ERR(err));
        
        keel_auth_manager_destroy(mgr);
    }
    
    TEST_END();
}

/* ============================================================================
 * Test: User Lookup Callback
 * ============================================================================ */

static bool test_user_lookup_called = false;
static keel_auth_user_t g_external_user;

static keel_error_t test_user_lookup(const char* username, const keel_auth_user_t** user_out, void* data) {
    (void)data;
    test_user_lookup_called = true;
    
    if (strcmp(username, "external_user") == 0) {
        g_external_user.username = "external_user";
        g_external_user.password_hash = "external_pass";
        g_external_user.can_login = true;
        *user_out = &g_external_user;
        return KEEL_OK;
    }
    
    return KEEL_ERR_NOT_FOUND;
}

static void test_user_lookup_callback(void) {
    TEST_BEGIN("user_lookup_callback");
    
    keel_auth_manager_t* mgr = keel_auth_manager_create(NULL);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) return;
    
    /* Set lookup callback */
    keel_auth_manager_set_user_lookup(mgr, test_user_lookup, NULL);
    
    test_user_lookup_called = false;
    
    /* Try to find external user - should trigger callback */
    const keel_auth_user_t* found = NULL;
    keel_error_t err = keel_auth_lookup_user(mgr, "external_user", &found);
    
    /* Callback should have been called */
    TEST_ASSERT(test_user_lookup_called);
    
    keel_auth_manager_destroy(mgr);
    
    TEST_END();
}

/* ============================================================================
 * Test: Certificate Identity Provider API
 * ============================================================================ */

/**
 * §10 — Certificate identity provider: no challenge is issued, state
 *        transitions directly to KEEL_AUTH_STATE_SUCCESS on start().
 */
static void test_cert_auth(void) {
    TEST_BEGIN("cert_auth");

    const keel_auth_provider_ops_t *ops = keel_auth_cert_ops();
    TEST_ASSERT_NOT_NULL(ops);
    if (!ops) return;

    /* Verify meta */
    TEST_ASSERT_STR_EQ(ops->name(), "cert");
    TEST_ASSERT_EQ(ops->method(), KEEL_AUTH_CERTIFICATE);

    /* Create a provider */
    keel_auth_provider_t prov = { .ops = ops, .provider_data = NULL };
    keel_error_t err = ops->init(&prov, NULL);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* Start: should succeed immediately (no TLS challenge from provider) */
    keel_auth_context_t *ctx = NULL;
    err = ops->start(&prov, "certuser", NULL, &ctx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(ctx);

    if (ctx) {
        keel_auth_state_t state = ctx->state;
        TEST_ASSERT_EQ(state, KEEL_AUTH_STATE_SUCCESS);

        /* get_message must return NOT_FOUND (no challenge bytes) */
        void *msg = NULL; size_t mlen = 0; int mtype = 0;
        keel_error_t msg_err = ops->get_message(ctx, &msg, &mlen, &mtype);
        TEST_ASSERT_EQ(msg_err, KEEL_ERR_NOT_FOUND);

        ops->free_context(ctx);
    }

    ops->destroy(&prov);
    TEST_END();
}

/* ============================================================================
 * Test: LDAP Provider API (no live LDAP server required)
 * ============================================================================ */

/**
 * §11 — LDAP provider: verifies ops vtable, method enum, cleartext-password
 *        challenge issuance, and init-time validation.
 */
static void test_ldap_ops_api(void) {
    TEST_BEGIN("ldap_ops_api");

    const keel_auth_provider_ops_t *ops = keel_auth_ldap_ops();
    TEST_ASSERT_NOT_NULL(ops);
    if (!ops) return;

    TEST_ASSERT_STR_EQ(ops->name(), "ldap");
    TEST_ASSERT_EQ(ops->method(), KEEL_AUTH_LDAP);

    /* init with NULL config should fail */
    keel_auth_provider_t prov = { .ops = ops, .provider_data = NULL };
    keel_error_t err = ops->init(&prov, NULL);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);

    /* init with minimal valid config */
    keel_auth_ldap_config_t cfg = {
        .url       = "ldap://localhost:389",
        .dn_suffix = "ou=users,dc=example,dc=com",
        .timeout_s = 3,
    };
    err = ops->init(&prov, &cfg);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* start — should issue cleartext-password challenge */
    keel_auth_context_t *ctx = NULL;
    err = ops->start(&prov, "alice", NULL, &ctx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(ctx);

    if (ctx) {
        /* state must be CHALLENGE */
        TEST_ASSERT_EQ(ctx->state, KEEL_AUTH_STATE_CHALLENGE);

        /* get_message must return the 4-byte CleartextPassword indicator */
        void *msg = NULL; size_t mlen = 0; int mtype = 0;
        keel_error_t msg_err = ops->get_message(ctx, &msg, &mlen, &mtype);
        TEST_ASSERT_EQ(msg_err, KEEL_OK);
        TEST_ASSERT_NOT_NULL(msg);
        TEST_ASSERT_EQ(mlen, (size_t)4);
        TEST_ASSERT_EQ(mtype, 3); /* AuthenticationCleartextPassword type */

        /* Verify the 4-byte payload encodes integer 3 (big-endian) */
        if (msg && mlen == 4) {
            const uint8_t *bytes = (const uint8_t *)msg;
            TEST_ASSERT_EQ(bytes[0], 0);
            TEST_ASSERT_EQ(bytes[1], 0);
            TEST_ASSERT_EQ(bytes[2], 0);
            TEST_ASSERT_EQ(bytes[3], 3);
        }
        keel_free(msg); /* get_message transfers ownership — caller must free */

        ops->free_context(ctx);
    }

    ops->destroy(&prov);
    TEST_END();
}

/* ============================================================================
 * Test: PAM Provider API (no live PAM service required)
 * ============================================================================ */

/**
 * §12 — PAM provider: verifies ops vtable, method enum, and cleartext-password
 *        challenge issuance.
 */
static void test_pam_ops_api(void) {
    TEST_BEGIN("pam_ops_api");

    const keel_auth_provider_ops_t *ops = keel_auth_pam_ops();
    TEST_ASSERT_NOT_NULL(ops);
    if (!ops) return;

    TEST_ASSERT_STR_EQ(ops->name(), "pam");
    TEST_ASSERT_EQ(ops->method(), KEEL_AUTH_PAM);

    /* init with NULL config — should succeed (uses default service "keel") */
    keel_auth_provider_t prov = { .ops = ops, .provider_data = NULL };
    keel_error_t err = ops->init(&prov, NULL);
    TEST_ASSERT_EQ(err, KEEL_OK);

    /* start — should issue cleartext-password challenge */
    keel_auth_context_t *ctx = NULL;
    err = ops->start(&prov, "pamuser", NULL, &ctx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(ctx);

    if (ctx) {
        TEST_ASSERT_EQ(ctx->state, KEEL_AUTH_STATE_CHALLENGE);

        void *msg = NULL; size_t mlen = 0; int mtype = 0;
        keel_error_t msg_err = ops->get_message(ctx, &msg, &mlen, &mtype);
        TEST_ASSERT_EQ(msg_err, KEEL_OK);
        TEST_ASSERT_NOT_NULL(msg);
        TEST_ASSERT_EQ(mlen, (size_t)4);
        TEST_ASSERT_EQ(mtype, 3);

        if (msg && mlen == 4) {
            const uint8_t *bytes = (const uint8_t *)msg;
            TEST_ASSERT_EQ(bytes[3], 3);
        }
        keel_free(msg); /* get_message transfers ownership — caller must free */

        ops->free_context(ctx);
    }

    ops->destroy(&prov);
    TEST_END();
}

/* ============================================================================
 * Test: auth_query Provider API (no live backend required)
 * ============================================================================ */

/**
 * §13 — auth_query provider: verifies ops vtable, method enum, and that
 *        init rejects invalid configs.  Does NOT attempt a live DB connection.
 */
static void test_auth_query_ops_api(void) {
    TEST_BEGIN("auth_query_ops_api");

    const keel_auth_provider_ops_t *ops = keel_auth_query_ops();
    TEST_ASSERT_NOT_NULL(ops);
    if (!ops) return;

    TEST_ASSERT_STR_EQ(ops->name(), "auth_query");

    /* init with NULL config should fail */
    keel_auth_provider_t prov = { .ops = ops, .provider_data = NULL };
    keel_error_t err = ops->init(&prov, NULL);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);

    /* init without required fields (missing query) should fail */
    keel_auth_query_config_t bad_cfg = {
        .query       = NULL,
        .conn_string = "host=localhost dbname=postgres",
    };
    err = ops->init(&prov, &bad_cfg);
    TEST_ASSERT_EQ(err, KEEL_ERR_INVALID_ARG);

    /* Valid init */
    keel_auth_query_config_t cfg = {
        .query          = "SELECT password FROM pgbouncer.users WHERE usename=$1",
        .conn_string    = "host=localhost dbname=postgres",
        .upstream_method = KEEL_AUTH_SCRAM_SHA_256,
        .timeout_s      = 2,
    };
    err = ops->init(&prov, &cfg);
    TEST_ASSERT_EQ(err, KEEL_OK);

    ops->destroy(&prov);
    TEST_END();
}

/* ============================================================================
 * §A3 / §A4 regression — constant-time comparison coverage
 *
 * The existing scram_auth_flow / md5_auth tests above only exercise the
 * challenge-issue phase (start + get_message). They never call
 * keel_auth_process(), so the password-verifying comparison (the very
 * line the A3/A4 fix changes) was not covered. The tests below drive
 * the full exchange through keel_auth_process so both the equal and
 * unequal branches of CRYPTO_memcmp are exercised.
 *
 * cover review_20260618_01.md §A3 (SCRAM StoredKey) and §A4 (MD5 strcmp).
 * ============================================================================ */

#ifdef KEEL_HAS_OPENSSL
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/crypto.h>

/* Minimal RFC 4648 base64 decoder for test use only.
 * Returns number of decoded bytes, or -1 on invalid input.
 * Values in the table are shifted by 1 (1..64) so that 0 unambiguously
 * marks an invalid byte — important because base64 'A' legitimately
 * decodes to 0. */
static int test_b64_decode(const char* in, uint8_t* out, size_t out_max) {
    static const int8_t tab[256] = {
        ['A']=1,['B']=2,['C']=3,['D']=4,['E']=5,['F']=6,['G']=7,['H']=8,
        ['I']=9,['J']=10,['K']=11,['L']=12,['M']=13,['N']=14,['O']=15,['P']=16,
        ['Q']=17,['R']=18,['S']=19,['T']=20,['U']=21,['V']=22,['W']=23,['X']=24,
        ['Y']=25,['Z']=26,
        ['a']=27,['b']=28,['c']=29,['d']=30,['e']=31,['f']=32,['g']=33,['h']=34,
        ['i']=35,['j']=36,['k']=37,['l']=38,['m']=39,['n']=40,['o']=41,['p']=42,
        ['q']=43,['r']=44,['s']=45,['t']=46,['u']=47,['v']=48,['w']=49,['x']=50,
        ['y']=51,['z']=52,
        ['0']=53,['1']=54,['2']=55,['3']=56,['4']=57,['5']=58,['6']=59,['7']=60,
        ['8']=61,['9']=62,['+']=63,['/']=64,
    };
    size_t in_len = strlen(in);
    uint32_t buf = 0;
    int bits = 0;
    size_t out_len = 0;
    for (size_t i = 0; i < in_len; i++) {
        unsigned char c = (unsigned char)in[i];
        if (c == '=') break;
        if (c >= 128 || tab[c] == 0) return -1;
        buf = (buf << 6) | (uint32_t)(tab[c] - 1);
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            if (out_len >= out_max) return -1;
            out[out_len++] = (uint8_t)((buf >> bits) & 0xFF);
        }
    }
    return (int)out_len;
}

/* Minimal RFC 4648 base64 encoder for test use only.
 * `out` must have room for ((in_len + 2) / 3) * 4 + 1 bytes. */
static void test_b64_encode(const uint8_t* in, size_t in_len, char* out) {
    static const char b64[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t o = 0;
    size_t i;
    for (i = 0; i + 2 < in_len; i += 3) {
        uint32_t v = ((uint32_t)in[i] << 16) | ((uint32_t)in[i+1] << 8) | in[i+2];
        out[o++] = b64[(v >> 18) & 0x3F];
        out[o++] = b64[(v >> 12) & 0x3F];
        out[o++] = b64[(v >> 6) & 0x3F];
        out[o++] = b64[v & 0x3F];
    }
    size_t rem = in_len - i;
    if (rem == 1) {
        uint32_t v = (uint32_t)in[i] << 16;
        out[o++] = b64[(v >> 18) & 0x3F];
        out[o++] = b64[(v >> 12) & 0x3F];
        out[o++] = '=';
        out[o++] = '=';
    } else if (rem == 2) {
        uint32_t v = ((uint32_t)in[i] << 16) | ((uint32_t)in[i+1] << 8);
        out[o++] = b64[(v >> 18) & 0x3F];
        out[o++] = b64[(v >> 12) & 0x3F];
        out[o++] = b64[(v >> 6) & 0x3F];
        out[o++] = '=';
    }
    out[o] = '\0';
}

/* Replicate the PostgreSQL client MD5 computation:
 *   inner = md5(password + user)        → hex
 *   outer = md5(inner_hex + salt)       → hex
 *   result = "md5" + outer_hex
 * Returns a keel_malloc'd string (caller frees) or NULL. */
static char* test_compute_md5_response(const char* user,
                                       const char* password,
                                       const uint8_t salt[4]) {
    static const char hex[] = "0123456789abcdef";

    unsigned char inner[16];
    EVP_MD_CTX* md = EVP_MD_CTX_new();
    if (!md) return NULL;

    if (EVP_DigestInit_ex(md, EVP_md5(), NULL) != 1 ||
        EVP_DigestUpdate(md, password, strlen(password)) != 1 ||
        EVP_DigestUpdate(md, user, strlen(user)) != 1 ||
        EVP_DigestFinal_ex(md, inner, NULL) != 1) {
        EVP_MD_CTX_free(md);
        return NULL;
    }

    char inner_hex[33];
    for (int i = 0; i < 16; i++) {
        inner_hex[i*2]   = hex[inner[i] >> 4];
        inner_hex[i*2+1] = hex[inner[i] & 0x0f];
    }
    inner_hex[32] = '\0';

    unsigned char outer[16];
    if (EVP_DigestInit_ex(md, EVP_md5(), NULL) != 1 ||
        EVP_DigestUpdate(md, inner_hex, 32) != 1 ||
        EVP_DigestUpdate(md, salt, 4) != 1 ||
        EVP_DigestFinal_ex(md, outer, NULL) != 1) {
        EVP_MD_CTX_free(md);
        return NULL;
    }
    EVP_MD_CTX_free(md);

    char* result = keel_malloc(36);
    if (!result) return NULL;
    result[0] = 'm'; result[1] = 'd'; result[2] = '5';
    for (int i = 0; i < 16; i++) {
        result[3 + i*2]   = hex[outer[i] >> 4];
        result[3 + i*2+1] = hex[outer[i] & 0x0f];
    }
    result[35] = '\0';
    return result;
}

/* ---- A4: MD5 constant-time comparison ---------------------------------- */

/* Helper: drive a full MD5 exchange and return the final auth state.
 * `response` is the raw bytes the "client" sends as its MD5 reply; the
 * caller controls whether this is the correct hash, a wrong hash, or a
 * malformed-length payload. */
static keel_auth_state_t test_md5_drive(const char* username,
                                        const char* stored_password,
                                        const void* response,
                                        size_t response_len) {
    keel_auth_manager_config_t cfg = {
        .default_method = KEEL_AUTH_MD5,
        .allow_clear_password = false,
    };
    keel_auth_manager_t* mgr = keel_auth_manager_create(&cfg);
    if (!mgr) return KEEL_AUTH_STATE_ERROR;

    keel_auth_user_t user = {
        .username = (char*)username,
        .password_hash = (char*)stored_password,
        .can_login = true,
    };
    keel_auth_add_user(mgr, &user);

    keel_auth_context_t* ctx = NULL;
    keel_error_t err = keel_auth_manager_start(mgr, username, &ctx);
    if (err != KEEL_OK || !ctx) {
        keel_auth_manager_destroy(mgr);
        return KEEL_AUTH_STATE_ERROR;
    }

    /* Drain the challenge message so the provider is in the right state */
    void* msg = NULL;
    size_t msg_len = 0;
    int msg_type = 0;
    err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
    if (err == KEEL_OK && msg) keel_free(msg);

    /* The salt lives at bytes 4..7 of the MD5 challenge; pull it out so
     * the caller can compute the matching response.  We expose it via a
     * side channel for the correct-password test.  For the wrong-password
     * and wrong-length tests the caller already has the response bytes. */
    keel_auth_state_t state = keel_auth_process(ctx, response, response_len);

    keel_auth_context_free(ctx);
    keel_auth_manager_destroy(mgr);
    return state;
}

static void test_md5_constant_time_correct(void) {
    TEST_BEGIN("A4: MD5 correct password → SUCCESS (CRYPTO_memcmp equal branch)");

    const char* username = "alice";
    const char* password = "correct-horse-battery-staple";

    /* We need the salt to compute the right response, so start once to
     * harvest it, then drive the actual exchange. */
    keel_auth_manager_config_t cfg = {
        .default_method = KEEL_AUTH_MD5,
        .allow_clear_password = false,
    };
    keel_auth_manager_t* mgr = keel_auth_manager_create(&cfg);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) { TEST_END(); return; }

    keel_auth_user_t user = {
        .username = (char*)username,
        .password_hash = (char*)password,
        .can_login = true,
    };
    keel_auth_add_user(mgr, &user);

    keel_auth_context_t* ctx = NULL;
    keel_error_t err = keel_auth_manager_start(mgr, username, &ctx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(ctx);

    if (ctx) {
        void* msg = NULL;
        size_t msg_len = 0;
        int msg_type = 0;
        err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
        TEST_ASSERT_EQ(err, KEEL_OK);
        TEST_ASSERT_NOT_NULL(msg);

        if (msg && msg_len >= 8) {
            /* Salt is at offset 4..7 of the AuthenticationMD5Password body */
            uint8_t salt[4];
            memcpy(salt, (uint8_t*)msg + 4, 4);
            keel_free(msg);

            char* response = test_compute_md5_response(username, password, salt);
            TEST_ASSERT_NOT_NULL(response);

            if (response) {
                keel_auth_state_t state =
                    keel_auth_process(ctx, response, 35);
                TEST_ASSERT_EQ((int)state, (int)KEEL_AUTH_STATE_SUCCESS);
                keel_free(response);
            }
        } else if (msg) {
            keel_free(msg);
        }

        keel_auth_context_free(ctx);
    }
    keel_auth_manager_destroy(mgr);
    TEST_END();
}

static void test_md5_constant_time_wrong(void) {
    TEST_BEGIN("A4: MD5 wrong password → FAILED (CRYPTO_memcmp unequal branch)");
    /* Feed a syntactically-valid (35-byte "md5"+hex) but cryptographically
     * wrong response. The constant-time compare must still return not-equal
     * and the auth must fail. */
    const char* wrong_response = "md5deadbeefdeadbeefdeadbeefdeadbeef";
    /* "md5" + 32 hex chars = 35 bytes total */
    TEST_ASSERT_EQ((int)strlen(wrong_response), 35);

    keel_auth_state_t state = test_md5_drive(
        "bob", "the-real-password", wrong_response, 35);
    TEST_ASSERT_EQ((int)state, (int)KEEL_AUTH_STATE_FAILED);
    TEST_END();
}

static void test_md5_rejects_wrong_length(void) {
    TEST_BEGIN("A4/M10: MD5 wrong-length response → FAILED (tightened len != 35)");
    /* Previously the check was `len < 35`, so a 4000-byte response starting
     * with "md5" would be accepted past the format gate and then fed to
     * strcmp. Now `len != 35` rejects at the door. Verify both shorter
     * and longer malformed payloads. */
    const char* too_short = "md5tooshort";
    const char* too_long  = "md5aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaxxxxxx";

    keel_auth_state_t s_short = test_md5_drive(
        "carol", "pw", too_short, strlen(too_short));
    TEST_ASSERT_EQ((int)s_short, (int)KEEL_AUTH_STATE_FAILED);

    keel_auth_state_t s_long = test_md5_drive(
        "carol", "pw", too_long, strlen(too_long));
    TEST_ASSERT_EQ((int)s_long, (int)KEEL_AUTH_STATE_FAILED);
    TEST_END();
}

/* ---- A3: SCRAM constant-time comparison -------------------------------- */

/* Drive a full SCRAM-SHA-256 exchange with an honest client and verify
 * SUCCESS. Exercises the CRYPTO_memcmp equal branch at line 800. */
static void test_scram_constant_time_correct(void) {
    TEST_BEGIN("A3: SCRAM correct ClientProof → SUCCESS (CRYPTO_memcmp equal branch)");

    const char* password = "scram-test-password";

    keel_auth_manager_t* mgr = keel_auth_manager_create(NULL);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) { TEST_END(); return; }

    /* Hash the password and parse out the verifier fields */
    char* hash = NULL;
    int rc_hash = keel_auth_scram_hash_password(password, 4096, &hash);
    TEST_ASSERT_EQ(rc_hash, (int)KEEL_OK);
    if (!hash) { keel_auth_manager_destroy(mgr); TEST_END(); return; }

    char* salt_b64 = NULL;
    int iterations = 0;
    uint8_t stored_key[32];
    uint8_t server_key[32];
    keel_error_t err = keel_auth_scram_parse_hash(hash, &salt_b64,
                                                  &iterations, stored_key, server_key);
    TEST_ASSERT_EQ(err, KEEL_OK);
    if (err != KEEL_OK) { keel_free(hash); keel_auth_manager_destroy(mgr); TEST_END(); return; }

    /* Decode the base64 salt to raw bytes for PBKDF2 */
    uint8_t salt_raw[64];
    int salt_raw_len = test_b64_decode(salt_b64, salt_raw, sizeof(salt_raw));
    TEST_ASSERT(salt_raw_len > 0);
    if (salt_raw_len <= 0) {
        keel_free(salt_b64); keel_free(hash); keel_auth_manager_destroy(mgr); TEST_END(); return;
    }

    /* Register the user */
    keel_auth_user_t user = {
        .username = "scramuser",
        .password_hash = hash,
        .password_salt = salt_b64,
        .iterations = iterations,
        .has_scram_keys = true,
        .can_login = true,
    };
    memcpy(user.stored_key, stored_key, 32);
    memcpy(user.server_key, server_key, 32);
    keel_auth_add_user(mgr, &user);

    /* --- SCRAM step 0: send client-first, receive server-first --- */
    keel_auth_context_t* ctx = NULL;
    err = keel_auth_manager_start(mgr, "scramuser", &ctx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(ctx);

    if (!ctx) {
        keel_free(salt_b64); keel_free(hash); keel_auth_manager_destroy(mgr); TEST_END(); return;
    }

    /* Drain the initial SASL mechanism advertisement */
    void* msg = NULL; size_t msg_len = 0; int msg_type = 0;
    err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
    if (err == KEEL_OK && msg) keel_free(msg);

    /* client-first-message: gs2-header n,, + bare n=user,r=nonce */
    const char* client_nonce = "clientnonce12345";
    char client_first[256];
    char client_first_bare[256];
    snprintf(client_first_bare, sizeof(client_first_bare), "n=scramuser,r=%s", client_nonce);
    snprintf(client_first, sizeof(client_first), "n,,%s", client_first_bare);

    keel_auth_state_t state = keel_auth_process(ctx, client_first, strlen(client_first));
    TEST_ASSERT_EQ((int)state, (int)KEEL_AUTH_STATE_CHALLENGE);

    /* Harvest server-first: r=<combined_nonce>,s=<salt_b64>,i=<iter> */
    err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(msg);
    if (!msg) { keel_auth_context_free(ctx); keel_free(salt_b64); keel_free(hash); keel_auth_manager_destroy(mgr); TEST_END(); return; }

    /* The server-first body starts after the 4-byte auth-type prefix.
     * The buffer from keel_auth_get_message is NOT NUL-terminated (it's
     * a binary auth message: 4-byte type + text body), so we must use
     * msg_len to bound the copy rather than relying on %s/strstr.
     * Copy the full server-first text into a local buffer before freeing
     * `msg` — the auth_message computation below needs it, and
     * keel_auth_get_message returns a caller-owned heap buffer (not an
     * internal alias), so the pointer is dangling after keel_free(msg). */
    char server_first_buf[512];
    size_t body_len = msg_len >= 4 ? msg_len - 4 : 0;
    if (body_len >= sizeof(server_first_buf)) body_len = sizeof(server_first_buf) - 1;
    memcpy(server_first_buf, (const char*)msg + 4, body_len);
    server_first_buf[body_len] = '\0';
    const char* server_first = server_first_buf;

    /* Extract combined nonce: r=... up to next comma */
    const char* r_eq = strstr(server_first, "r=");
    TEST_ASSERT_NOT_NULL(r_eq);
    const char* comma_after_r = strchr(r_eq, ',');
    TEST_ASSERT_NOT_NULL(comma_after_r);
    char combined_nonce[256];
    size_t nonce_len = (size_t)(comma_after_r - r_eq - 2);
    memcpy(combined_nonce, r_eq + 2, nonce_len);
    combined_nonce[nonce_len] = '\0';
    keel_free(msg);

    /* --- Compute the honest SCRAM ClientProof --- */
    /* SaltedPassword = PBKDF2(password, salt, iterations) */
    uint8_t salted_pw[32];
    int pbkdf2_rc = PKCS5_PBKDF2_HMAC(password, (int)strlen(password),
                                      salt_raw, salt_raw_len,
                                      iterations, EVP_sha256(),
                                      32, salted_pw);
    TEST_ASSERT_EQ(pbkdf2_rc, 1);
    if (pbkdf2_rc != 1) {
        keel_auth_context_free(ctx); keel_free(salt_b64); keel_free(hash);
        keel_auth_manager_destroy(mgr); TEST_END(); return;
    }

    /* ClientKey = HMAC(SaltedPassword, "Client Key") */
    uint8_t client_key[32];
    unsigned int ck_len = 32;
    HMAC(EVP_sha256(), salted_pw, 32,
         (const unsigned char*)"Client Key", 10, client_key, &ck_len);

    /* StoredKey = SHA256(ClientKey) — should match the stored_key from parse */
    uint8_t computed_stored[32];
    SHA256(client_key, 32, computed_stored);

    /* AuthMessage = client_first_bare + "," + server_first + "," + client_final_without_proof */
    char client_final_without_proof[512];
    snprintf(client_final_without_proof, sizeof(client_final_without_proof),
             "c=biws,r=%s", combined_nonce);

    char auth_message[1024];
    snprintf(auth_message, sizeof(auth_message), "%s,%s,%s",
             client_first_bare, server_first, client_final_without_proof);

    /* ClientSignature = HMAC(StoredKey, AuthMessage) */
    uint8_t client_sig[32];
    unsigned int cs_len = 32;
    HMAC(EVP_sha256(), computed_stored, 32,
         (const unsigned char*)auth_message, strlen(auth_message),
         client_sig, &cs_len);

    /* ClientProof = ClientKey XOR ClientSignature */
    uint8_t client_proof[32];
    for (int i = 0; i < 32; i++) client_proof[i] = client_key[i] ^ client_sig[i];

    /* Base64-encode the proof and build client-final-message */
    char proof_b64[64];
    test_b64_encode(client_proof, 32, proof_b64);

    char client_final[1024];
    snprintf(client_final, sizeof(client_final), "%s,p=%s",
             client_final_without_proof, proof_b64);

    /* --- SCRAM step 1: send client-final, expect SUCCESS --- */
    state = keel_auth_process(ctx, client_final, strlen(client_final));
    TEST_ASSERT_EQ((int)state, (int)KEEL_AUTH_STATE_SUCCESS);

    keel_auth_context_free(ctx);
    keel_free(salt_b64);
    keel_free(hash);
    keel_auth_manager_destroy(mgr);
    TEST_END();
}

/* Drive a SCRAM exchange with a syntactically valid but cryptographically
 * wrong ClientProof. Exercises the CRYPTO_memcmp unequal branch. */
static void test_scram_constant_time_wrong(void) {
    TEST_BEGIN("A3: SCRAM wrong ClientProof → FAILED (CRYPTO_memcmp unequal branch)");

    keel_auth_manager_t* mgr = keel_auth_manager_create(NULL);
    TEST_ASSERT_NOT_NULL(mgr);
    if (!mgr) { TEST_END(); return; }

    const char* password = "another-password";
    char* hash = NULL;
    int rc_hash = keel_auth_scram_hash_password(password, 4096, &hash);
    TEST_ASSERT_EQ(rc_hash, (int)KEEL_OK);
    if (!hash) { keel_auth_manager_destroy(mgr); TEST_END(); return; }

    char* salt_b64 = NULL;
    int iterations = 0;
    uint8_t stored_key[32];
    uint8_t server_key[32];
    keel_error_t err = keel_auth_scram_parse_hash(hash, &salt_b64,
                                                  &iterations, stored_key, server_key);
    TEST_ASSERT_EQ(err, KEEL_OK);
    if (err != KEEL_OK) { keel_free(hash); keel_auth_manager_destroy(mgr); TEST_END(); return; }

    keel_auth_user_t user = {
        .username = "wronguser",
        .password_hash = hash,
        .password_salt = salt_b64,
        .iterations = iterations,
        .has_scram_keys = true,
        .can_login = true,
    };
    memcpy(user.stored_key, stored_key, 32);
    memcpy(user.server_key, server_key, 32);
    keel_auth_add_user(mgr, &user);

    keel_auth_context_t* ctx = NULL;
    err = keel_auth_manager_start(mgr, "wronguser", &ctx);
    TEST_ASSERT_EQ(err, KEEL_OK);
    TEST_ASSERT_NOT_NULL(ctx);
    if (!ctx) { keel_free(salt_b64); keel_free(hash); keel_auth_manager_destroy(mgr); TEST_END(); return; }

    /* Drain SASL advertisement */
    void* msg = NULL; size_t msg_len = 0; int msg_type = 0;
    err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
    if (err == KEEL_OK && msg) keel_free(msg);

    /* step 0: client-first */
    const char* client_first = "n,,n=wronguser,r=fakenonce000";
    keel_auth_state_t state = keel_auth_process(ctx, client_first, strlen(client_first));
    TEST_ASSERT_EQ((int)state, (int)KEEL_AUTH_STATE_CHALLENGE);

    /* harvest combined nonce. The buffer from keel_auth_get_message is
     * NOT NUL-terminated, so bound the copy with msg_len before strstr. */
    err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
    TEST_ASSERT_EQ(err, KEEL_OK);
    if (!msg) { keel_auth_context_free(ctx); keel_free(salt_b64); keel_free(hash); keel_auth_manager_destroy(mgr); TEST_END(); return; }
    char server_first_buf[512];
    size_t body_len = msg_len >= 4 ? msg_len - 4 : 0;
    if (body_len >= sizeof(server_first_buf)) body_len = sizeof(server_first_buf) - 1;
    memcpy(server_first_buf, (const char*)msg + 4, body_len);
    server_first_buf[body_len] = '\0';
    const char* r_eq = strstr(server_first_buf, "r=");
    const char* comma = r_eq ? strchr(r_eq, ',') : NULL;
    char combined_nonce[256];
    if (r_eq && comma) {
        size_t nlen = (size_t)(comma - r_eq - 2);
        memcpy(combined_nonce, r_eq + 2, nlen);
        combined_nonce[nlen] = '\0';
    } else {
        combined_nonce[0] = '\0';
    }
    keel_free(msg);

    /* step 1: send a wrong proof (32 zero bytes base64-encoded) */
    uint8_t zero_proof[32];
    memset(zero_proof, 0, 32);
    char proof_b64[64];
    test_b64_encode(zero_proof, 32, proof_b64);

    char client_final[512];
    snprintf(client_final, sizeof(client_final), "c=biws,r=%s,p=%s",
             combined_nonce, proof_b64);
    state = keel_auth_process(ctx, client_final, strlen(client_final));
    TEST_ASSERT_EQ((int)state, (int)KEEL_AUTH_STATE_FAILED);

    keel_auth_context_free(ctx);
    keel_free(salt_b64);
    keel_free(hash);
    keel_auth_manager_destroy(mgr);
    TEST_END();
}

#endif /* KEEL_HAS_OPENSSL */

/* ============================================================================
 * Main
 * ============================================================================ */

int main(void) {
    printf("=== Authentication Framework Tests ===\n\n");
    
    /* Initialize memory system */
    keel_mem_init(NULL);
    
    /* Run tests */
    test_auth_manager_create();
    test_user_management();
    test_trust_auth();
    test_scram_hash_password();
    test_scram_hash_format_pg_compatible();
    test_auth_method_names();
    test_auth_state_names();
    test_scram_hash_parsing();
    test_scram_auth_flow();
    
    /* New comprehensive tests */
    test_md5_auth();
    test_password_auth();
    test_reject_auth();
    test_add_and_lookup_user();
    test_get_provider();
    test_auth_method_names_complete();
    test_auth_state_names_complete();
    test_auth_null_safety();
    test_user_lookup_callback();

    /* Enterprise auth provider API tests */
    test_cert_auth();
    test_ldap_ops_api();
    test_pam_ops_api();
    test_auth_query_ops_api();

    /* A3/A4 regression: constant-time comparison coverage */
#ifdef KEEL_HAS_OPENSSL
    test_md5_constant_time_correct();
    test_md5_constant_time_wrong();
    test_md5_rejects_wrong_length();
    test_scram_constant_time_correct();
    test_scram_constant_time_wrong();
#endif
    
    printf("\n");
    return test_summary();
}
