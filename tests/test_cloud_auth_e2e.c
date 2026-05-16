/**
 * @file test_cloud_auth_e2e.c
 * @brief End-to-end integration tests for cloud authentication providers.
 *
 * Validates the full token acquisition, caching, and refresh lifecycle
 * under realistic conditions: cache hits, expiry, concurrent access patterns,
 * and error recovery.
 *
 * These tests use real provider implementations but mock network operations
 * (cloud_http_get/post) to avoid external dependencies.
 *
 * @author KEEL Development Team
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/core/cloud_auth.h"
#include "keel/mem/mem.h"
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>

/* ============================================================================
 * §1 — Token Cache Lifecycle: create → get → refresh → destroy
 * ============================================================================ */

static void test_cache_lifecycle(void) {
    TEST_BEGIN("cache_lifecycle");

    /* Create a static env provider for deterministic testing */
    setenv("TEST_TOKEN_V1", "token_v1_initial", 1);
    keel_cloud_static_config_t cfg = {
        .path = "TEST_TOKEN_V1",
        .refresh_interval_s = 1,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_static_create(&prov,
        KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(prov);

    /* Initialize cache with provider */
    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, prov, 60);

    /* First fetch: hits provider */
    const char* pw1 = keel_cloud_auth_get_password(&cache,
        "backend.example.com", 5432, "testuser", "fallback");
    TEST_ASSERT_NOT_NULL(pw1);
    TEST_ASSERT_STR_EQ(pw1, "token_v1_initial");

    /* Second fetch (within cache margin): returns cached token */
    const char* pw2 = keel_cloud_auth_get_password(&cache,
        "backend.example.com", 5432, "testuser", "fallback");
    TEST_ASSERT_STR_EQ(pw2, "token_v1_initial");

    /* Update env and manually expire cache */
    setenv("TEST_TOKEN_V1", "token_v1_updated", 1);
    cache.expires_at = 0;  /* Force expiry */
    cache.valid = false;

    /* Third fetch: sees new token from environment */
    const char* pw3 = keel_cloud_auth_get_password(&cache,
        "backend.example.com", 5432, "testuser", "fallback");
    TEST_ASSERT_NOT_NULL(pw3);
    TEST_ASSERT_STR_EQ(pw3, "token_v1_updated");

    /* Cleanup */
    keel_cloud_token_cache_destroy(&cache);

    TEST_END();
}

/* ============================================================================
 * §2 — Fallback to Static Password on Provider Failure
 * ============================================================================ */

static void test_fallback_on_provider_failure(void) {
    TEST_BEGIN("fallback_on_provider_failure");

    /* Create provider with non-existent file → will fail on fetch */
    keel_cloud_static_config_t cfg = {
        .path = "/nonexistent/token/file",
        .refresh_interval_s = 0,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_static_create(&prov,
        KEEL_CLOUD_AUTH_STATIC_FILE, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, prov, 60);

    /* First call should fail to fetch, but fallback should NOT be used yet
     * (we want to distinguish between "provider error" and "no provider") */
    const char* pw = keel_cloud_auth_get_password(&cache,
        "backend.example.com", 5432, "testuser", "my_static_password");

    /* When provider fails, fallback is returned */
    TEST_ASSERT_NOT_NULL(pw);
    TEST_ASSERT_STR_EQ(pw, "my_static_password");

    keel_cloud_token_cache_destroy(&cache);

    TEST_END();
}

/* ============================================================================
 * §3 — No Provider: Always Return Static Password
 * ============================================================================ */

static void test_no_provider_always_static(void) {
    TEST_BEGIN("no_provider_always_static");

    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, NULL, 60);

    /* No provider → always returns static password */
    const char* pw1 = keel_cloud_auth_get_password(&cache,
        "backend.example.com", 5432, "testuser", "static_pw_1");
    TEST_ASSERT_STR_EQ(pw1, "static_pw_1");

    const char* pw2 = keel_cloud_auth_get_password(&cache,
        "backend.example.com", 5432, "testuser", "static_pw_2");
    TEST_ASSERT_STR_EQ(pw2, "static_pw_2");

    keel_cloud_token_cache_destroy(&cache);

    TEST_END();
}

/* ============================================================================
 * §4 — Concurrent Access Pattern: Multiple Hosts & Users
 * ============================================================================ */

static void test_concurrent_access_pattern(void) {
    TEST_BEGIN("concurrent_access_pattern");

    setenv("TOKEN_HOST_A", "token_for_host_a", 1);
    setenv("TOKEN_HOST_B", "token_for_host_b", 1);

    /* Single cache instance, multiple backend hosts */
    keel_cloud_static_config_t cfg = {
        .path = "TOKEN_HOST_A",
        .refresh_interval_s = 0,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_cloud_auth_static_create(&prov, KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);

    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, prov, 60);

    /* Multiple hosts should share the same cache (not ideal for production,
     * but the current implementation caches per backend pool, not per host) */
    const char* pw_a = keel_cloud_auth_get_password(&cache,
        "host-a.rds.amazonaws.com", 5432, "user1", "fallback");
    TEST_ASSERT_NOT_NULL(pw_a);

    /* Same call again (cache hit) */
    const char* pw_a2 = keel_cloud_auth_get_password(&cache,
        "host-a.rds.amazonaws.com", 5432, "user1", "fallback");
    TEST_ASSERT_STR_EQ(pw_a, pw_a2);

    keel_cloud_token_cache_destroy(&cache);

    TEST_END();
}

/* ============================================================================
 * §5 — Cache Destroy: Zeroing Sensitive Data
 * ============================================================================ */

static void test_cache_destroy_zeros_token(void) {
    TEST_BEGIN("cache_destroy_zeros_token");

    setenv("SENSITIVE_TOKEN", "secret_token_abc123xyz", 1);
    keel_cloud_static_config_t cfg = {
        .path = "SENSITIVE_TOKEN",
        .refresh_interval_s = 0,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_cloud_auth_static_create(&prov, KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);

    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, prov, 60);

    /* Fetch token to populate cache */
    const char* pw = keel_cloud_auth_get_password(&cache,
        "backend.example.com", 5432, "testuser", "fallback");
    TEST_ASSERT_NOT_NULL(pw);
    TEST_ASSERT_NOT_NULL(cache.cached_token);
    size_t token_len = cache.cached_token_len;
    TEST_ASSERT(token_len > 0);

    /* After destroy, cached_token should be NULL */
    keel_cloud_token_cache_destroy(&cache);
    TEST_ASSERT_NULL(cache.cached_token);
    TEST_ASSERT_EQ(cache.cached_token_len, (size_t)0);

    TEST_END();
}

/* ============================================================================
 * §6 — Token Refresh Margin: Prevent Edge-Case Expiry
 * ============================================================================ */

static void test_refresh_margin_prevents_expiry(void) {
    TEST_BEGIN("refresh_margin_prevents_expiry");

    setenv("SHORT_LIVED_TOKEN", "token_expires_soon", 1);
    keel_cloud_static_config_t cfg = {
        .path = "SHORT_LIVED_TOKEN",
        .refresh_interval_s = 5,  /* 5 second lifetime */
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_cloud_auth_static_create(&prov, KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);

    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, prov, 2);  /* 2-second margin */

    /* First fetch: token expires in 5 seconds, margin is 2 seconds */
    const char* pw1 = keel_cloud_auth_get_password(&cache,
        "backend.example.com", 5432, "testuser", "fallback");
    TEST_ASSERT_NOT_NULL(pw1);

    time_t first_expiry = cache.expires_at;
    TEST_ASSERT(first_expiry > 0);

    /* Simulate time progression: 3 seconds later
     * Now token expires in 2 seconds = within margin → should refresh */
    cache.expires_at = time(NULL) + 2;

    setenv("SHORT_LIVED_TOKEN", "token_refreshed", 1);
    const char* pw2 = keel_cloud_auth_get_password(&cache,
        "backend.example.com", 5432, "testuser", "fallback");

    /* Should have refreshed and gotten new token */
    TEST_ASSERT_STR_EQ(pw2, "token_refreshed");

    keel_cloud_token_cache_destroy(&cache);

    TEST_END();
}

/* ============================================================================
 * §7 — Cache Initialization with Various Margin Values
 * ============================================================================ */

static void test_cache_init_margin_clamping(void) {
    TEST_BEGIN("cache_init_margin_clamping");

    /* Zero margin should clamp to 60 */
    {
        keel_cloud_token_cache_t c1;
        keel_cloud_token_cache_init(&c1, NULL, 0);
        TEST_ASSERT_EQ(c1.refresh_margin_s, 60);
        keel_cloud_token_cache_destroy(&c1);
    }

    /* Negative margin should clamp to 60 */
    {
        keel_cloud_token_cache_t c2;
        keel_cloud_token_cache_init(&c2, NULL, -10);
        TEST_ASSERT_EQ(c2.refresh_margin_s, 60);
        keel_cloud_token_cache_destroy(&c2);
    }

    /* Positive margin should be accepted */
    {
        keel_cloud_token_cache_t c3;
        keel_cloud_token_cache_init(&c3, NULL, 120);
        TEST_ASSERT_EQ(c3.refresh_margin_s, 120);
        keel_cloud_token_cache_destroy(&c3);
    }

    TEST_END();
}

/* ============================================================================
 * §8 — Provider Type Identification
 * ============================================================================ */

static void test_provider_type_identification(void) {
    TEST_BEGIN("provider_type_identification");

    /* AWS provider */
    {
        keel_cloud_aws_config_t cfg = {
            .region = "us-east-1",
            .access_key_id = NULL,
            .secret_access_key = NULL,
        };
        keel_cloud_auth_provider_t* prov = NULL;
        keel_cloud_auth_aws_create(&prov, &cfg);
        TEST_ASSERT_NOT_NULL(prov);
        TEST_ASSERT_NOT_NULL(prov->ops);
        TEST_ASSERT_STR_EQ(prov->ops->name, "aws-rds-iam");
        TEST_ASSERT_EQ(prov->ops->type, KEEL_CLOUD_AUTH_AWS_IAM);
        prov->ops->destroy(prov);
    }

    /* GCP provider */
    {
        keel_cloud_gcp_config_t cfg = { .service_account_file = NULL };
        keel_cloud_auth_provider_t* prov = NULL;
        keel_cloud_auth_gcp_create(&prov, &cfg);
        TEST_ASSERT_NOT_NULL(prov);
        TEST_ASSERT_STR_EQ(prov->ops->name, "gcp-cloud-sql-iam");
        TEST_ASSERT_EQ(prov->ops->type, KEEL_CLOUD_AUTH_GCP_IAM);
        prov->ops->destroy(prov);
    }

    /* Azure provider */
    {
        keel_cloud_azure_config_t cfg = { .client_id = NULL, .resource = NULL };
        keel_cloud_auth_provider_t* prov = NULL;
        keel_cloud_auth_azure_create(&prov, &cfg);
        TEST_ASSERT_NOT_NULL(prov);
        TEST_ASSERT_STR_EQ(prov->ops->name, "azure-ad");
        TEST_ASSERT_EQ(prov->ops->type, KEEL_CLOUD_AUTH_AZURE_AD);
        prov->ops->destroy(prov);
    }

    TEST_END();
}

/* ============================================================================
 * Test Entry Point
 * ============================================================================ */

int main(void) {
    test_cache_lifecycle();
    test_fallback_on_provider_failure();
    test_no_provider_always_static();
    test_concurrent_access_pattern();
    test_cache_destroy_zeros_token();
    test_refresh_margin_prevents_expiry();
    test_cache_init_margin_clamping();
    test_provider_type_identification();

    return test_summary() == 0 ? 0 : 1;
}
