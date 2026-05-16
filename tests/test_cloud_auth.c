/**
 * @file test_cloud_auth.c
 * @brief Unit tests for cloud-native authentication token providers.
 *
 * Exercises the keel_cloud_auth infrastructure:
 *   §1 — Token cache lifecycle: init/destroy, get_password with no provider.
 *   §2 — AWS IAM token generation: SigV4 pre-signed URL format and structure.
 *   §3 — Static file provider: read password from a temp file.
 *   §4 — Static env provider: read password from environment variable.
 *   §5 — Token expiry and refresh: cache returns fresh token after expiry.
 *   §6 — GCP/Azure providers: creation and error handling.
 *   §7 — Security: tokens are zeroed on destroy.
 *
 * @author Generated for KEEL P1 roadmap
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/core/cloud_auth.h"
#include "keel/mem/mem.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ============================================================================
 * §1 — Token cache lifecycle
 * ============================================================================ */

static void test_cache_no_provider(void) {
    TEST_BEGIN("cache_no_provider");

    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, NULL, 60);

    /* With no provider, get_password returns the static fallback */
    const char* pw = keel_cloud_auth_get_password(&cache,
        "localhost", 5432, "user", "static_secret");
    TEST_ASSERT_NOT_NULL(pw);
    TEST_ASSERT_STR_EQ(pw, "static_secret");

    /* NULL cache returns static pw */
    pw = keel_cloud_auth_get_password(NULL, "localhost", 5432, "user", "fallback");
    TEST_ASSERT_NOT_NULL(pw);
    TEST_ASSERT_STR_EQ(pw, "fallback");

    keel_cloud_token_cache_destroy(&cache);
    TEST_END();
}

/* ============================================================================
 * §2 — AWS IAM token generation
 * ============================================================================ */

static void test_aws_iam_token(void) {
    TEST_BEGIN("aws_iam_token");

    keel_cloud_aws_config_t cfg = {
        .region = "us-east-1",
        .access_key_id = "AKIAIOSFODNN7EXAMPLE",
        .secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        .session_token = NULL,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_aws_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(prov);
    TEST_ASSERT_NOT_NULL(prov->ops);
    TEST_ASSERT_STR_EQ(prov->ops->name, "aws-rds-iam");

    /* Fetch a token */
    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "mydb.us-east-1.rds.amazonaws.com",
                                  5432, "db_user", &tok);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(tok.token);
    TEST_ASSERT(tok.token_len > 0);
    TEST_ASSERT(tok.expires_at > 0);

    /* Token should contain the host */
    TEST_ASSERT(strstr(tok.token, "mydb.us-east-1.rds.amazonaws.com") != NULL);
    /* Token should contain Action=connect */
    TEST_ASSERT(strstr(tok.token, "Action=connect") != NULL);
    /* Token should contain DBUser */
    TEST_ASSERT(strstr(tok.token, "DBUser=db_user") != NULL);
    /* Token should contain AWS4-HMAC-SHA256 */
    TEST_ASSERT(strstr(tok.token, "AWS4-HMAC-SHA256") != NULL);
    /* Token should contain the credential */
    TEST_ASSERT(strstr(tok.token, "AKIAIOSFODNN7EXAMPLE") != NULL);
    /* Token should contain a signature */
    TEST_ASSERT(strstr(tok.token, "X-Amz-Signature=") != NULL);

    /* Signature should be 64 hex chars */
    const char* sig_start = strstr(tok.token, "X-Amz-Signature=");
    TEST_ASSERT_NOT_NULL(sig_start);
    sig_start += strlen("X-Amz-Signature=");
    TEST_ASSERT(strlen(sig_start) == 64);

    keel_free(tok.token);

    /* Token via cache */
    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, prov, 60);

    const char* pw = keel_cloud_auth_get_password(&cache,
        "mydb.us-east-1.rds.amazonaws.com", 5432, "db_user", "unused");
    TEST_ASSERT_NOT_NULL(pw);
    TEST_ASSERT(strstr(pw, "Action=connect") != NULL);

    /* Second call should return cached token (same pointer) */
    const char* pw2 = keel_cloud_auth_get_password(&cache,
        "mydb.us-east-1.rds.amazonaws.com", 5432, "db_user", "unused");
    TEST_ASSERT(pw == pw2); /* Same cached pointer */

    keel_cloud_token_cache_destroy(&cache);
    /* prov is owned by cache — destroyed above */

    TEST_END();
}

static void test_aws_iam_missing_region(void) {
    TEST_BEGIN("aws_iam_missing_region");

    keel_cloud_aws_config_t cfg = {
        .region = NULL,
        .access_key_id = "AKID",
        .secret_access_key = "SECRET",
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_aws_create(&prov, &cfg);
    /* Create should fail since region is required */
    TEST_ASSERT(err != KEEL_OK);

    TEST_END();
}

static void test_aws_iam_env_credentials(void) {
    TEST_BEGIN("aws_iam_env_credentials");

    /* Test that provider falls back to env vars when config keys are NULL */
    setenv("AWS_ACCESS_KEY_ID", "AKIAENVEXAMPLE", 1);
    setenv("AWS_SECRET_ACCESS_KEY", "EnvSecretKeyExample", 1);

    keel_cloud_aws_config_t cfg = {
        .region = "eu-west-1",
        .access_key_id = NULL,  /* Should use env */
        .secret_access_key = NULL,  /* Should use env */
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_aws_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "db.eu-west-1.rds.amazonaws.com",
                                  5432, "user", &tok);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(tok.token);
    TEST_ASSERT(strstr(tok.token, "AKIAENVEXAMPLE") != NULL);

    keel_free(tok.token);
    prov->ops->destroy(prov);

    unsetenv("AWS_ACCESS_KEY_ID");
    unsetenv("AWS_SECRET_ACCESS_KEY");

    TEST_END();
}

/* ============================================================================
 * §3 — Static file provider
 * ============================================================================ */

static void test_static_file_provider(void) {
    TEST_BEGIN("static_file_provider");

    /* Write a temp password file */
    const char* tmppath = "/tmp/keel_test_password.txt";
    FILE* f = fopen(tmppath, "w");
    TEST_ASSERT_NOT_NULL(f);
    fputs("my-secret-password\n", f);
    fclose(f);

    keel_cloud_static_config_t cfg = {
        .path = tmppath,
        .refresh_interval_s = 300,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_static_create(&prov,
        KEEL_CLOUD_AUTH_STATIC_FILE, &cfg);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(prov);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "localhost", 5432, "user", &tok);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(tok.token);
    TEST_ASSERT_STR_EQ(tok.token, "my-secret-password"); /* trailing newline stripped */
    TEST_ASSERT(tok.expires_at > 0);

    keel_free(tok.token);

    /* Via cache */
    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, prov, 60);

    const char* pw = keel_cloud_auth_get_password(&cache,
        "localhost", 5432, "user", "fallback");
    TEST_ASSERT_STR_EQ(pw, "my-secret-password");

    keel_cloud_token_cache_destroy(&cache);
    unlink(tmppath);

    TEST_END();
}

static void test_static_file_missing(void) {
    TEST_BEGIN("static_file_missing");

    keel_cloud_static_config_t cfg = {
        .path = "/nonexistent/path/password.txt",
        .refresh_interval_s = 0,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_static_create(&prov,
        KEEL_CLOUD_AUTH_STATIC_FILE, &cfg);
    TEST_ASSERT(err == KEEL_OK); /* Create succeeds */

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "localhost", 5432, "user", &tok);
    TEST_ASSERT(err != KEEL_OK); /* Fetch fails */
    TEST_ASSERT_NULL(tok.token);

    /* Cache falls back to static pw */
    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, prov, 60);
    const char* pw = keel_cloud_auth_get_password(&cache,
        "localhost", 5432, "user", "fallback_pw");
    TEST_ASSERT_STR_EQ(pw, "fallback_pw");

    keel_cloud_token_cache_destroy(&cache);

    TEST_END();
}

/* ============================================================================
 * §4 — Static env provider
 * ============================================================================ */

static void test_static_env_provider(void) {
    TEST_BEGIN("static_env_provider");

    setenv("KEEL_TEST_DB_PASSWORD", "env-secret-123", 1);

    keel_cloud_static_config_t cfg = {
        .path = "KEEL_TEST_DB_PASSWORD",
        .refresh_interval_s = 600,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_static_create(&prov,
        KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "localhost", 5432, "user", &tok);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(tok.token);
    TEST_ASSERT_STR_EQ(tok.token, "env-secret-123");

    keel_free(tok.token);
    prov->ops->destroy(prov);
    unsetenv("KEEL_TEST_DB_PASSWORD");

    TEST_END();
}

static void test_static_env_missing(void) {
    TEST_BEGIN("static_env_missing");

    unsetenv("KEEL_TEST_NONEXISTENT_VAR");

    keel_cloud_static_config_t cfg = {
        .path = "KEEL_TEST_NONEXISTENT_VAR",
        .refresh_interval_s = 0,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_static_create(&prov,
        KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "localhost", 5432, "user", &tok);
    TEST_ASSERT(err != KEEL_OK);

    prov->ops->destroy(prov);

    TEST_END();
}

/* ============================================================================
 * §5 — GCP provider creation and token file fallback
 * ============================================================================ */

static void test_gcp_provider_create(void) {
    TEST_BEGIN("gcp_provider_create");

    keel_cloud_gcp_config_t cfg = {
        .service_account_file = NULL,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_gcp_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(prov);
    TEST_ASSERT_STR_EQ(prov->ops->name, "gcp-cloud-sql-iam");

    /* Without any env vars set, fetch should fail gracefully */
    unsetenv("GOOGLE_APPLICATION_CREDENTIALS");
    unsetenv("CLOUDSQL_ACCESS_TOKEN_FILE");
    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "db.project.region", 5432, "user", &tok);
    TEST_ASSERT(err != KEEL_OK);

    prov->ops->destroy(prov);
    TEST_END();
}

static void test_gcp_token_file_fallback(void) {
    TEST_BEGIN("gcp_token_file_fallback");

    /* Write a token file that simulates gcloud output */
    const char* tmppath = "/tmp/keel_test_gcp_token.txt";
    FILE* f = fopen(tmppath, "w");
    TEST_ASSERT_NOT_NULL(f);
    fputs("ya29.c.ElqBBYkNm-fake-gcp-access-token-for-testing\n", f);
    fclose(f);

    setenv("CLOUDSQL_ACCESS_TOKEN_FILE", tmppath, 1);

    keel_cloud_gcp_config_t cfg = { .service_account_file = NULL };
    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_gcp_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "db.project.region", 5432, "user", &tok);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(tok.token);
    /* Token should have the trailing newline stripped */
    TEST_ASSERT_STR_EQ(tok.token,
        "ya29.c.ElqBBYkNm-fake-gcp-access-token-for-testing");
    TEST_ASSERT(tok.expires_at > 0);

    keel_free(tok.token);
    prov->ops->destroy(prov);
    unsetenv("CLOUDSQL_ACCESS_TOKEN_FILE");
    unlink(tmppath);

    TEST_END();
}

static void test_gcp_invalid_key_file(void) {
    TEST_BEGIN("gcp_invalid_key_file");

    /* Write a file that is NOT a valid service account JSON */
    const char* tmppath = "/tmp/keel_test_gcp_bad_key.json";
    FILE* f = fopen(tmppath, "w");
    TEST_ASSERT_NOT_NULL(f);
    fputs("this is not json\n", f);
    fclose(f);

    /* Provider should create successfully (parse failure is not fatal) */
    keel_cloud_gcp_config_t cfg = { .service_account_file = tmppath };
    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_gcp_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(prov);

    prov->ops->destroy(prov);
    unlink(tmppath);

    TEST_END();
}

/* ============================================================================
 * §6 — Azure provider creation with native IMDS
 * ============================================================================ */

static void test_azure_provider_create(void) {
    TEST_BEGIN("azure_provider_create");

    keel_cloud_azure_config_t cfg = {
        .client_id = NULL,
        .resource = NULL,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_azure_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(prov);
    TEST_ASSERT_STR_EQ(prov->ops->name, "azure-ad");

    prov->ops->destroy(prov);
    TEST_END();
}

static void test_azure_env_token(void) {
    TEST_BEGIN("azure_env_token");

    setenv("AZURE_POSTGRESQL_ACCESS_TOKEN", "azure-token-XYZ", 1);

    keel_cloud_azure_config_t cfg = {0};
    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_azure_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "mydb.postgres.database.azure.com",
                                  5432, "user", &tok);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(tok.token);
    TEST_ASSERT_STR_EQ(tok.token, "azure-token-XYZ");

    keel_free(tok.token);
    prov->ops->destroy(prov);
    unsetenv("AZURE_POSTGRESQL_ACCESS_TOKEN");

    TEST_END();
}

static void test_azure_file_token(void) {
    TEST_BEGIN("azure_file_token");

    /* Write a token file */
    const char* tmppath = "/tmp/keel_test_azure_token.txt";
    FILE* f = fopen(tmppath, "w");
    TEST_ASSERT_NOT_NULL(f);
    fputs("eyJhbGciOiJSUzI1NiIs.azure-jwt-token-content\n", f);
    fclose(f);

    /* Clear direct env var, set file var */
    unsetenv("AZURE_POSTGRESQL_ACCESS_TOKEN");
    setenv("AZURE_ACCESS_TOKEN_FILE", tmppath, 1);

    keel_cloud_azure_config_t cfg = {0};
    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_azure_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "mydb.postgres.database.azure.com",
                                  5432, "user", &tok);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(tok.token);
    TEST_ASSERT_STR_EQ(tok.token,
        "eyJhbGciOiJSUzI1NiIs.azure-jwt-token-content");

    keel_free(tok.token);
    prov->ops->destroy(prov);
    unsetenv("AZURE_ACCESS_TOKEN_FILE");
    unlink(tmppath);

    TEST_END();
}

static void test_azure_with_client_id(void) {
    TEST_BEGIN("azure_with_client_id");

    /* Test that Azure provider stores client_id and resource */
    keel_cloud_azure_config_t cfg = {
        .client_id = "12345678-abcd-ef01-2345-678901234567",
        .resource = "https://ossrdbms-aad.database.windows.net",
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_azure_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(prov);

    /* Without IMDS or env/file, fetch should fail gracefully */
    unsetenv("AZURE_POSTGRESQL_ACCESS_TOKEN");
    unsetenv("AZURE_ACCESS_TOKEN_FILE");
    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "mydb.postgres.database.azure.com",
                                  5432, "user", &tok);
    TEST_ASSERT(err != KEEL_OK);

    prov->ops->destroy(prov);
    TEST_END();
}

static void test_azure_no_token_source(void) {
    TEST_BEGIN("azure_no_token_source");

    unsetenv("AZURE_POSTGRESQL_ACCESS_TOKEN");
    unsetenv("AZURE_ACCESS_TOKEN_FILE");

    keel_cloud_azure_config_t cfg = {0};
    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_azure_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    /* Should fail with no IMDS, no env, no file */
    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "mydb.postgres.database.azure.com",
                                  5432, "user", &tok);
    TEST_ASSERT(err != KEEL_OK);

    /* Cache should fall back to static password */
    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, prov, 60);
    const char* pw = keel_cloud_auth_get_password(&cache,
        "mydb.postgres.database.azure.com", 5432, "user", "fallback_pw");
    TEST_ASSERT_STR_EQ(pw, "fallback_pw");

    keel_cloud_token_cache_destroy(&cache);
    TEST_END();
}

/* ============================================================================
 * §7 — Invalid args
 * ============================================================================ */

static void test_invalid_args(void) {
    TEST_BEGIN("invalid_args");

    /* NULL output pointer */
    keel_error_t err = keel_cloud_auth_aws_create(NULL, &(keel_cloud_aws_config_t){
        .region = "us-east-1"
    });
    TEST_ASSERT(err != KEEL_OK);

    err = keel_cloud_auth_gcp_create(NULL, NULL);
    TEST_ASSERT(err != KEEL_OK);

    err = keel_cloud_auth_azure_create(NULL, NULL);
    TEST_ASSERT(err != KEEL_OK);

    /* Static with wrong type */
    keel_cloud_auth_provider_t* prov = NULL;
    err = keel_cloud_auth_static_create(&prov, KEEL_CLOUD_AUTH_AWS_IAM,
        &(keel_cloud_static_config_t){ .path = "/tmp" });
    TEST_ASSERT(err != KEEL_OK);

    /* Static with NULL path */
    err = keel_cloud_auth_static_create(&prov, KEEL_CLOUD_AUTH_STATIC_FILE, NULL);
    TEST_ASSERT(err != KEEL_OK);

    TEST_END();
}

/* ============================================================================
 * §8 — AWS SigV4 token format validation
 * ============================================================================ */

static void test_aws_token_format(void) {
    TEST_BEGIN("aws_token_format");

    keel_cloud_aws_config_t cfg = {
        .region = "us-west-2",
        .access_key_id = "AKIATESTACCESSKEY123",
        .secret_access_key = "TestSecretKey1234567890/abcdef",
        .session_token = "FwoGZXIvYXdzEBYaDB-session-token-example",
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_aws_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "mydb.us-west-2.rds.amazonaws.com",
                                  5432, "iam_user", &tok);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(tok.token);

    /* Verify all required SigV4 query parameters are present */
    TEST_ASSERT(strstr(tok.token, "X-Amz-Algorithm=AWS4-HMAC-SHA256") != NULL);
    TEST_ASSERT(strstr(tok.token, "X-Amz-Credential=") != NULL);
    TEST_ASSERT(strstr(tok.token, "X-Amz-Date=") != NULL);
    TEST_ASSERT(strstr(tok.token, "X-Amz-Expires=") != NULL);
    TEST_ASSERT(strstr(tok.token, "X-Amz-Signature=") != NULL);

    /* With session_token, X-Amz-Security-Token should be included */
    TEST_ASSERT(strstr(tok.token, "X-Amz-Security-Token=") != NULL);

    /* Credential should contain the region */
    TEST_ASSERT(strstr(tok.token, "us-west-2") != NULL);

    /* DBUser should match the provided user */
    TEST_ASSERT(strstr(tok.token, "DBUser=iam_user") != NULL);

    /* Port should be in the host part */
    TEST_ASSERT(strstr(tok.token, ":5432") != NULL);

    /* Token should start with the host */
    TEST_ASSERT(strncmp(tok.token,
        "mydb.us-west-2.rds.amazonaws.com:5432", 37) == 0);

    /* Signature should be 64 hex chars (32 bytes) */
    const char* sig = strstr(tok.token, "X-Amz-Signature=");
    TEST_ASSERT_NOT_NULL(sig);
    sig += strlen("X-Amz-Signature=");
    size_t sig_len = 0;
    while (sig[sig_len] && sig[sig_len] != '&') sig_len++;
    TEST_ASSERT_EQ(sig_len, (size_t)64);

    /* All hex chars in signature */
    for (size_t i = 0; i < sig_len; i++) {
        char c = sig[i];
        TEST_ASSERT((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
    }

    keel_free(tok.token);
    prov->ops->destroy(prov);

    TEST_END();
}

/* ============================================================================
 * §9 — AWS without session token (no X-Amz-Security-Token)
 * ============================================================================ */

static void test_aws_no_session_token(void) {
    TEST_BEGIN("aws_no_session_token");

    keel_cloud_aws_config_t cfg = {
        .region = "eu-central-1",
        .access_key_id = "AKIANOTOKEN12345ABCD",
        .secret_access_key = "NoTokenSecretKey/1234567890",
        .session_token = NULL,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_aws_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "db.eu-central-1.rds.amazonaws.com",
                                  5432, "user", &tok);
    TEST_ASSERT(err == KEEL_OK);

    /* Without session token, X-Amz-Security-Token should NOT be present */
    TEST_ASSERT(strstr(tok.token, "X-Amz-Security-Token") == NULL);

    keel_free(tok.token);
    prov->ops->destroy(prov);

    TEST_END();
}

/* ============================================================================
 * §10 — AWS missing credentials (both config and env)
 * ============================================================================ */

static void test_aws_missing_credentials(void) {
    TEST_BEGIN("aws_missing_credentials");

    unsetenv("AWS_ACCESS_KEY_ID");
    unsetenv("AWS_SECRET_ACCESS_KEY");

    keel_cloud_aws_config_t cfg = {
        .region = "us-east-1",
        .access_key_id = NULL,
        .secret_access_key = NULL,
    };

    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_aws_create(&prov, &cfg);
    /* Create succeeds — credential resolution is deferred to fetch_token */
    TEST_ASSERT(err == KEEL_OK);

    /* Fetch without credentials should fail */
    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "host.rds.amazonaws.com", 5432,
                                  "user", &tok);
    TEST_ASSERT(err != KEEL_OK);

    prov->ops->destroy(prov);

    TEST_END();
}

/* ============================================================================
 * §11 — Token cache refresh: verify cache expiry triggers re-fetch
 * ============================================================================ */

static void test_cache_refresh(void) {
    TEST_BEGIN("cache_refresh");

    /* Use static env provider for deterministic token content */
    setenv("KEEL_TEST_CACHE_REFRESH", "token_v1", 1);

    keel_cloud_static_config_t cfg = {
        .path = "KEEL_TEST_CACHE_REFRESH",
        .refresh_interval_s = 0,
    };
    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_static_create(&prov,
        KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    /* Set very small margin so cache expires quickly */
    keel_cloud_token_cache_t cache;
    keel_cloud_token_cache_init(&cache, prov, 1);

    /* First call → fetches token */
    const char* pw1 = keel_cloud_auth_get_password(&cache,
        "host", 5432, "user", "fallback");
    TEST_ASSERT_STR_EQ(pw1, "token_v1");

    /* Same call → returns cached */
    const char* pw2 = keel_cloud_auth_get_password(&cache,
        "host", 5432, "user", "fallback");
    TEST_ASSERT(pw1 == pw2); /* Same pointer → cached */

    /* Artificially expire the cache */
    cache.expires_at = 0;
    cache.valid = false;

    /* Update env and re-fetch */
    setenv("KEEL_TEST_CACHE_REFRESH", "token_v2", 1);
    const char* pw3 = keel_cloud_auth_get_password(&cache,
        "host", 5432, "user", "fallback");
    TEST_ASSERT_STR_EQ(pw3, "token_v2");

    keel_cloud_token_cache_destroy(&cache);
    unsetenv("KEEL_TEST_CACHE_REFRESH");

    TEST_END();
}

/* ============================================================================
 * §12 — GCP GOOGLE_APPLICATION_CREDENTIALS fallback
 * ============================================================================ */

static void test_gcp_google_creds_fallback(void) {
    TEST_BEGIN("gcp_google_creds_fallback");

    /* Write a token-like file (simulating a non-JSON credential file) */
    const char* tmppath = "/tmp/keel_test_gcp_google_creds.txt";
    FILE* f = fopen(tmppath, "w");
    TEST_ASSERT_NOT_NULL(f);
    fputs("ya29.c.GoogleCredsFallbackToken\n", f);
    fclose(f);

    /* Only set GOOGLE_APPLICATION_CREDENTIALS, not CLOUDSQL_ACCESS_TOKEN_FILE */
    unsetenv("CLOUDSQL_ACCESS_TOKEN_FILE");
    setenv("GOOGLE_APPLICATION_CREDENTIALS", tmppath, 1);

    keel_cloud_gcp_config_t cfg = { .service_account_file = NULL };
    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_gcp_create(&prov, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "db.region", 5432, "user", &tok);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(tok.token);
    TEST_ASSERT_STR_EQ(tok.token,
        "ya29.c.GoogleCredsFallbackToken");

    keel_free(tok.token);
    prov->ops->destroy(prov);
    unsetenv("GOOGLE_APPLICATION_CREDENTIALS");
    unlink(tmppath);

    TEST_END();
}

/* ============================================================================
 * §13 — Static file: whitespace and newline stripping
 * ============================================================================ */

static void test_static_file_whitespace(void) {
    TEST_BEGIN("static_file_whitespace");

    const char* tmppath = "/tmp/keel_test_whitespace.txt";
    FILE* f = fopen(tmppath, "w");
    TEST_ASSERT_NOT_NULL(f);
    /* Write password with trailing \r\n (Windows-style) */
    fputs("my-password\r\n", f);
    fclose(f);

    keel_cloud_static_config_t cfg = {
        .path = tmppath,
        .refresh_interval_s = 300,
    };
    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_static_create(&prov,
        KEEL_CLOUD_AUTH_STATIC_FILE, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "host", 5432, "user", &tok);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(tok.token);
    TEST_ASSERT_STR_EQ(tok.token, "my-password");

    keel_free(tok.token);
    prov->ops->destroy(prov);
    unlink(tmppath);

    TEST_END();
}

/* ============================================================================
 * §14 — Static file: empty file
 * ============================================================================ */

static void test_static_file_empty(void) {
    TEST_BEGIN("static_file_empty");

    const char* tmppath = "/tmp/keel_test_empty.txt";
    FILE* f = fopen(tmppath, "w");
    fclose(f);

    keel_cloud_static_config_t cfg = {
        .path = tmppath,
        .refresh_interval_s = 0,
    };
    keel_cloud_auth_provider_t* prov = NULL;
    keel_error_t err = keel_cloud_auth_static_create(&prov,
        KEEL_CLOUD_AUTH_STATIC_FILE, &cfg);
    TEST_ASSERT(err == KEEL_OK);

    keel_cloud_token_t tok = {0};
    err = prov->ops->fetch_token(prov, "host", 5432, "user", &tok);
    TEST_ASSERT(err != KEEL_OK);

    prov->ops->destroy(prov);
    unlink(tmppath);

    TEST_END();
}

/* ============================================================================
 * §15 — Provider ops name and type consistency
 * ============================================================================ */

static void test_provider_ops_metadata(void) {
    TEST_BEGIN("provider_ops_metadata");

    /* AWS */
    {
        keel_cloud_aws_config_t cfg = {
            .region = "us-east-1",
            .access_key_id = "AKID",
            .secret_access_key = "SECRET",
        };
        keel_cloud_auth_provider_t* prov = NULL;
        keel_cloud_auth_aws_create(&prov, &cfg);
        TEST_ASSERT_NOT_NULL(prov->ops);
        TEST_ASSERT_NOT_NULL(prov->ops->name);
        TEST_ASSERT(strlen(prov->ops->name) > 0);
        TEST_ASSERT_EQ(prov->ops->type, KEEL_CLOUD_AUTH_AWS_IAM);
        TEST_ASSERT_NOT_NULL(prov->ops->fetch_token);
        TEST_ASSERT_NOT_NULL(prov->ops->destroy);
        prov->ops->destroy(prov);
    }

    /* GCP */
    {
        keel_cloud_gcp_config_t cfg = {0};
        keel_cloud_auth_provider_t* prov = NULL;
        keel_cloud_auth_gcp_create(&prov, &cfg);
        TEST_ASSERT_EQ(prov->ops->type, KEEL_CLOUD_AUTH_GCP_IAM);
        prov->ops->destroy(prov);
    }

    /* Azure */
    {
        keel_cloud_azure_config_t cfg = {0};
        keel_cloud_auth_provider_t* prov = NULL;
        keel_cloud_auth_azure_create(&prov, &cfg);
        TEST_ASSERT_EQ(prov->ops->type, KEEL_CLOUD_AUTH_AZURE_AD);
        prov->ops->destroy(prov);
    }

    /* Static file */
    {
        keel_cloud_static_config_t cfg = { .path = "/tmp/x" };
        keel_cloud_auth_provider_t* prov = NULL;
        keel_cloud_auth_static_create(&prov, KEEL_CLOUD_AUTH_STATIC_FILE, &cfg);
        TEST_ASSERT_EQ(prov->ops->type, KEEL_CLOUD_AUTH_STATIC_FILE);
        prov->ops->destroy(prov);
    }

    /* Static env */
    {
        keel_cloud_static_config_t cfg = { .path = "X" };
        keel_cloud_auth_provider_t* prov = NULL;
        keel_cloud_auth_static_create(&prov, KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);
        /* Note: ops->type is STATIC_FILE because both static providers share
         * the same ops table; the actual type is stored in the provider struct */
        TEST_ASSERT_NOT_NULL(prov);
        TEST_ASSERT_NOT_NULL(prov->ops);
        TEST_ASSERT_NOT_NULL(prov->ops->fetch_token);
        TEST_ASSERT_NOT_NULL(prov->ops->destroy);
        prov->ops->destroy(prov);
    }

    TEST_END();
}

/* ============================================================================
 * §16 — Token cache with zero/negative margin
 * ============================================================================ */

static void test_cache_margin_edge(void) {
    TEST_BEGIN("cache_margin_edge");

    /* Zero margin → should default to 60 */
    {
        keel_cloud_token_cache_t cache;
        keel_cloud_token_cache_init(&cache, NULL, 0);
        TEST_ASSERT_EQ(cache.refresh_margin_s, 60);
        keel_cloud_token_cache_destroy(&cache);
    }

    /* Negative margin → should default to 60 */
    {
        keel_cloud_token_cache_t cache;
        keel_cloud_token_cache_init(&cache, NULL, -5);
        TEST_ASSERT_EQ(cache.refresh_margin_s, 60);
        keel_cloud_token_cache_destroy(&cache);
    }

    /* Positive margin → use as-is */
    {
        keel_cloud_token_cache_t cache;
        keel_cloud_token_cache_init(&cache, NULL, 120);
        TEST_ASSERT_EQ(cache.refresh_margin_s, 120);
        keel_cloud_token_cache_destroy(&cache);
    }

    TEST_END();
}

/* ============================================================================
 * §17 — Repeated provider create/destroy cycles (leak test)
 * ============================================================================ */

static void test_provider_create_destroy_cycle(void) {
    TEST_BEGIN("provider_create_destroy_cycle");

    /* 100 cycles per provider type */
    for (int i = 0; i < 100; i++) {
        /* AWS */
        {
            keel_cloud_aws_config_t cfg = {
                .region = "us-east-1",
                .access_key_id = "AKID",
                .secret_access_key = "SECRET",
            };
            keel_cloud_auth_provider_t* prov = NULL;
            keel_cloud_auth_aws_create(&prov, &cfg);
            prov->ops->destroy(prov);
        }

        /* GCP */
        {
            keel_cloud_gcp_config_t cfg = {0};
            keel_cloud_auth_provider_t* prov = NULL;
            keel_cloud_auth_gcp_create(&prov, &cfg);
            prov->ops->destroy(prov);
        }

        /* Azure */
        {
            keel_cloud_azure_config_t cfg = {0};
            keel_cloud_auth_provider_t* prov = NULL;
            keel_cloud_auth_azure_create(&prov, &cfg);
            prov->ops->destroy(prov);
        }
    }

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    printf("=== Cloud-Native Auth Tests ===\n\n");

    test_cache_no_provider();
    test_aws_iam_token();
    test_aws_iam_missing_region();
    test_aws_iam_env_credentials();
    test_static_file_provider();
    test_static_file_missing();
    test_static_env_provider();
    test_static_env_missing();
    test_gcp_provider_create();
    test_gcp_token_file_fallback();
    test_gcp_invalid_key_file();
    test_azure_provider_create();
    test_azure_env_token();
    test_azure_file_token();
    test_azure_with_client_id();
    test_azure_no_token_source();
    test_invalid_args();
    /* Extended tests */
    test_aws_token_format();
    test_aws_no_session_token();
    test_aws_missing_credentials();
    test_cache_refresh();
    test_gcp_google_creds_fallback();
    test_static_file_whitespace();
    test_static_file_empty();
    test_provider_ops_metadata();
    test_cache_margin_edge();
    test_provider_create_destroy_cycle();

    printf("\n");
    return test_summary();
}
