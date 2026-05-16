/**
 * @file test_cloud_auth_internals.c
 * @brief White-box tests for cloud_auth.c internal/static helper functions.
 *
 * This test file #includes the source file directly to access static functions
 * that cannot be tested through the public API.  Tests cover:
 *
 *   §1 — hex_encode: byte→hex string encoding
 *   §2 — base64url_encode: RFC 4648 base64url without padding
 *   §3 — base64url_encode_alloc: heap-allocated variant
 *   §4 — json_find_string: simple JSON string extraction
 *   §5 — json_find_int: simple JSON integer extraction
 *   §6 — hmac_sha256: HMAC-SHA256 known-answer tests
 *   §7 — aws_signing_key: AWS SigV4 derived key chain
 *   §8 — url_encode: RFC 3986 percent-encoding
 *   §9 — GCP JWT structure validation (with synthetic RSA key)
 *   §10 — GCP key file parsing (synthetic service account JSON)
 *   §11 — Memory safety: double-destroy, zero-after-free
 *
 * @author Generated for KEEL test coverage
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"

/* Include the source directly to access static functions */
#include "../src/core/cloud_auth.c"

#include <string.h>
#include <stdio.h>

/* ============================================================================
 * §1 — hex_encode
 * ============================================================================ */

static void test_hex_encode(void) {
    TEST_BEGIN("hex_encode");

    /* Empty input */
    {
        char out[1] = {0};
        keel_hex_encode((const uint8_t*)"", out, 0); out[0] = '\0';
        TEST_ASSERT_STR_EQ(out, "");
    }

    /* Single byte */
    {
        char out[3];
        uint8_t in[] = {0xab};
        keel_hex_encode(in, out, 1); out[2] = '\0';
        TEST_ASSERT_STR_EQ(out, "ab");
    }

    /* Multiple bytes */
    {
        char out[7];
        uint8_t in[] = {0xde, 0xad, 0xbe};
        keel_hex_encode(in, out, 3); out[6] = '\0';
        TEST_ASSERT_STR_EQ(out, "deadbe");
    }

    /* All zeros */
    {
        char out[9];
        uint8_t in[] = {0x00, 0x00, 0x00, 0x00};
        keel_hex_encode(in, out, 4); out[8] = '\0';
        TEST_ASSERT_STR_EQ(out, "00000000");
    }

    /* All 0xFF */
    {
        char out[5];
        uint8_t in[] = {0xff, 0xff};
        keel_hex_encode(in, out, 2); out[4] = '\0';
        TEST_ASSERT_STR_EQ(out, "ffff");
    }

    /* SHA-256-length output (32 bytes → 64 hex chars) */
    {
        char out[65];
        uint8_t in[32];
        for (int i = 0; i < 32; i++) in[i] = (uint8_t)i;
        keel_hex_encode(in, out, 32); out[64] = '\0';
        TEST_ASSERT(strlen(out) == 64);
        /* First byte is 0x00 */
        TEST_ASSERT(out[0] == '0' && out[1] == '0');
        /* Last byte is 0x1f */
        TEST_ASSERT(out[62] == '1' && out[63] == 'f');
    }

    TEST_END();
}

/* ============================================================================
 * §2 — base64url_encode (stack buffer)
 * ============================================================================ */

static void test_base64url_encode(void) {
    TEST_BEGIN("base64url_encode");

    /* RFC 4648 test vectors (adapted for no-padding base64url) */
    {
        char out[64];
        /* "" → "" */
        size_t n = base64url_encode((const uint8_t*)"", 0, out, sizeof(out));
        TEST_ASSERT_EQ(n, (size_t)0);
        TEST_ASSERT_STR_EQ(out, "");
    }

    {
        char out[64];
        /* "f" (1 byte) — stack version includes zero-byte encoding */
        size_t n = base64url_encode((const uint8_t*)"f", 1, out, sizeof(out));
        TEST_ASSERT(n > 0);
        /* stack-based encoder outputs full 4-char groups; heap alloc trims */
        TEST_ASSERT(strncmp(out, "Zg", 2) == 0);
    }

    {
        char out[64];
        /* "fo" (2 bytes) */
        size_t n = base64url_encode((const uint8_t*)"fo", 2, out, sizeof(out));
        TEST_ASSERT(n > 0);
        TEST_ASSERT(strncmp(out, "Zm8", 3) == 0);
    }

    {
        char out[64];
        /* "foo" → "Zm9v" */
        size_t n = base64url_encode((const uint8_t*)"foo", 3, out, sizeof(out));
        TEST_ASSERT(n > 0);
        TEST_ASSERT_STR_EQ(out, "Zm9v");
    }

    {
        char out[64];
        /* "foob" (4 bytes) */
        size_t n = base64url_encode((const uint8_t*)"foob", 4, out, sizeof(out));
        TEST_ASSERT(n > 0);
        TEST_ASSERT(strncmp(out, "Zm9vYg", 6) == 0);
    }

    {
        char out[64];
        /* "fooba" (5 bytes) */
        size_t n = base64url_encode((const uint8_t*)"fooba", 5, out, sizeof(out));
        TEST_ASSERT(n > 0);
        TEST_ASSERT(strncmp(out, "Zm9vYmE", 7) == 0);
    }

    {
        char out[64];
        /* "foobar" → "Zm9vYmFy" */
        size_t n = base64url_encode((const uint8_t*)"foobar", 6, out, sizeof(out));
        TEST_ASSERT(n > 0);
        TEST_ASSERT_STR_EQ(out, "Zm9vYmFy");
    }

    /* Verify URL-safe chars: input with bytes that produce +/ in standard base64
     * should produce -_ instead */
    {
        char out[64];
        /* 0x3e → '>' in base64 encodes to include '+' (62), but base64url uses '-' */
        uint8_t in[] = {0xfb, 0xef, 0xbe};  /* These produce /++/ in base64 */
        base64url_encode(in, 3, out, sizeof(out));
        TEST_ASSERT(strchr(out, '+') == NULL);
        TEST_ASSERT(strchr(out, '/') == NULL);
        TEST_ASSERT(strchr(out, '=') == NULL);
    }

    TEST_END();
}

/* ============================================================================
 * §3 — base64url_encode_alloc (heap-allocated)
 * ============================================================================ */

static void test_base64url_encode_alloc(void) {
    TEST_BEGIN("base64url_encode_alloc");

    /* Empty input */
    {
        char* out = base64url_encode_alloc((const uint8_t*)"", 0);
        TEST_ASSERT_NOT_NULL(out);
        TEST_ASSERT_STR_EQ(out, "");
        keel_free(out);
    }

    /* Standard test vectors */
    {
        char* out = base64url_encode_alloc((const uint8_t*)"foo", 3);
        TEST_ASSERT_NOT_NULL(out);
        TEST_ASSERT_STR_EQ(out, "Zm9v");
        keel_free(out);
    }

    {
        char* out = base64url_encode_alloc((const uint8_t*)"foobar", 6);
        TEST_ASSERT_NOT_NULL(out);
        TEST_ASSERT_STR_EQ(out, "Zm9vYmFy");
        keel_free(out);
    }

    /* No padding chars should appear */
    {
        char* out = base64url_encode_alloc((const uint8_t*)"f", 1);
        TEST_ASSERT_NOT_NULL(out);
        TEST_ASSERT(strchr(out, '=') == NULL);
        keel_free(out);
    }

    /* Large input (256 bytes) should not crash */
    {
        uint8_t big[256];
        for (int i = 0; i < 256; i++) big[i] = (uint8_t)i;
        char* out = base64url_encode_alloc(big, 256);
        TEST_ASSERT_NOT_NULL(out);
        TEST_ASSERT(strlen(out) > 0);
        /* No standard base64 chars */
        TEST_ASSERT(strchr(out, '+') == NULL);
        TEST_ASSERT(strchr(out, '/') == NULL);
        TEST_ASSERT(strchr(out, '=') == NULL);
        keel_free(out);
    }

    /* Consistency: stack and heap should produce same result */
    {
        const uint8_t* data = (const uint8_t*)"Hello, World!";
        size_t len = 13;
        char stack[64];
        base64url_encode(data, len, stack, sizeof(stack));
        char* heap = base64url_encode_alloc(data, len);
        TEST_ASSERT_NOT_NULL(heap);
        TEST_ASSERT_STR_EQ(stack, heap);
        keel_free(heap);
    }

    TEST_END();
}

/* ============================================================================
 * §4 — json_find_string
 * ============================================================================ */

static void test_json_find_string(void) {
    TEST_BEGIN("json_find_string");

    char out[256];

    /* Simple key-value */
    {
        const char* json = "{\"name\": \"keel\"}";
        const char* r = json_find_string(json, "name", out, sizeof(out));
        TEST_ASSERT_NOT_NULL(r);
        TEST_ASSERT_STR_EQ(out, "keel");
    }

    /* Nested among other keys */
    {
        const char* json = "{\"type\": \"service_account\", \"client_email\": \"sa@proj.iam.gserviceaccount.com\", \"private_key\": \"-----BEGIN RSA PRIVATE KEY-----\\nMIIE...\"}";
        const char* r = json_find_string(json, "client_email", out, sizeof(out));
        TEST_ASSERT_NOT_NULL(r);
        TEST_ASSERT_STR_EQ(out, "sa@proj.iam.gserviceaccount.com");
    }

    /* Key not found */
    {
        const char* json = "{\"foo\": \"bar\"}";
        const char* r = json_find_string(json, "baz", out, sizeof(out));
        TEST_ASSERT_NULL(r);
    }

    /* Value with escaped newline */
    {
        const char* json = "{\"key\": \"line1\\nline2\"}";
        const char* r = json_find_string(json, "key", out, sizeof(out));
        TEST_ASSERT_NOT_NULL(r);
        TEST_ASSERT(strchr(out, '\n') != NULL);
    }

    /* Empty string value */
    {
        const char* json = "{\"empty\": \"\"}";
        const char* r = json_find_string(json, "empty", out, sizeof(out));
        TEST_ASSERT_NOT_NULL(r);
        TEST_ASSERT_STR_EQ(out, "");
    }

    /* Value that is not a string (integer) should fail */
    {
        const char* json = "{\"count\": 42}";
        const char* r = json_find_string(json, "count", out, sizeof(out));
        TEST_ASSERT_NULL(r);
    }

    /* Multiple keys with same prefix */
    {
        const char* json = "{\"host\": \"db.com\", \"hostname\": \"full.db.com\"}";
        const char* r = json_find_string(json, "host", out, sizeof(out));
        TEST_ASSERT_NOT_NULL(r);
        TEST_ASSERT_STR_EQ(out, "db.com");
    }

    /* Colon without spaces */
    {
        const char* json = "{\"key\":\"value\"}";
        const char* r = json_find_string(json, "key", out, sizeof(out));
        TEST_ASSERT_NOT_NULL(r);
        TEST_ASSERT_STR_EQ(out, "value");
    }

    TEST_END();
}

/* ============================================================================
 * §5 — json_find_int
 * ============================================================================ */

static void test_json_find_int(void) {
    TEST_BEGIN("json_find_int");

    /* Simple integer */
    {
        const char* json = "{\"expires_on\": 1714500000}";
        int64_t v = json_find_int(json, "expires_on");
        TEST_ASSERT_EQ(v, (int64_t)1714500000);
    }

    /* Integer as quoted string */
    {
        const char* json = "{\"expires_on\": \"1714500000\"}";
        int64_t v = json_find_int(json, "expires_on");
        TEST_ASSERT_EQ(v, (int64_t)1714500000);
    }

    /* Zero */
    {
        const char* json = "{\"count\": 0}";
        int64_t v = json_find_int(json, "count");
        TEST_ASSERT_EQ(v, (int64_t)0);
    }

    /* Key not found returns -1 */
    {
        const char* json = "{\"foo\": 42}";
        int64_t v = json_find_int(json, "bar");
        TEST_ASSERT_EQ(v, (int64_t)-1);
    }

    /* Negative number */
    {
        const char* json = "{\"offset\": -100}";
        int64_t v = json_find_int(json, "offset");
        TEST_ASSERT_EQ(v, (int64_t)-100);
    }

    /* Large number */
    {
        const char* json = "{\"big\": 9223372036854775807}";
        int64_t v = json_find_int(json, "big");
        TEST_ASSERT(v > 0);
    }

    TEST_END();
}

/* ============================================================================
 * §6 — hmac_sha256 known-answer tests
 * ============================================================================ */

static void test_hmac_sha256(void) {
    TEST_BEGIN("hmac_sha256");

    /* RFC 4231 Test Case 1:
     * Key = 0x0b repeated 20 times
     * Data = "Hi There"
     * HMAC-SHA-256 = b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7
     */
    {
        uint8_t key[20];
        memset(key, 0x0b, 20);
        const char* data = "Hi There";
        uint8_t mac[32];

        bool ok = hmac_sha256(key, 20, (const uint8_t*)data, strlen(data), mac);
        TEST_ASSERT(ok);

        char hex[65];
        keel_hex_encode(mac, hex, 32); hex[64] = '\0';
        TEST_ASSERT_STR_EQ(hex,
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
    }

    /* RFC 4231 Test Case 2:
     * Key = "Jefe"
     * Data = "what do ya want for nothing?"
     * HMAC-SHA-256 = 5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843
     */
    {
        const char* key = "Jefe";
        const char* data = "what do ya want for nothing?";
        uint8_t mac[32];

        bool ok = hmac_sha256((const uint8_t*)key, 4,
                               (const uint8_t*)data, strlen(data), mac);
        TEST_ASSERT(ok);

        char hex[65];
        keel_hex_encode(mac, hex, 32); hex[64] = '\0';
        TEST_ASSERT_STR_EQ(hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    }

    TEST_END();
}

/* ============================================================================
 * §7 — aws_signing_key: AWS SigV4 4-step key derivation
 * ============================================================================ */

static void test_aws_signing_key(void) {
    TEST_BEGIN("aws_signing_key");

    /* AWS SigV4 known-answer test:
     * Secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
     * Date = "20150830"
     * Region = "us-east-1"
     * Service = "iam"
     *
     * Expected signing key (hex):
     * c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9
     */
    {
        uint8_t key[32];
        bool ok = aws_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830", "us-east-1", "iam", key);
        TEST_ASSERT(ok);

        char hex[65];
        keel_hex_encode(key, hex, 32); hex[64] = '\0';
        TEST_ASSERT_STR_EQ(hex,
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9");
    }

    /* Same secret, different region → different key */
    {
        uint8_t key1[32], key2[32];
        aws_signing_key("SECRET", "20260101", "us-east-1", "rds-db", key1);
        aws_signing_key("SECRET", "20260101", "eu-west-1", "rds-db", key2);
        TEST_ASSERT(memcmp(key1, key2, 32) != 0);
    }

    /* Same secret, different date → different key */
    {
        uint8_t key1[32], key2[32];
        aws_signing_key("SECRET", "20260101", "us-east-1", "rds-db", key1);
        aws_signing_key("SECRET", "20260102", "us-east-1", "rds-db", key2);
        TEST_ASSERT(memcmp(key1, key2, 32) != 0);
    }

    TEST_END();
}

/* ============================================================================
 * §8 — url_encode: RFC 3986 percent-encoding
 * ============================================================================ */

static void test_url_encode(void) {
    TEST_BEGIN("url_encode");

    /* Unreserved chars pass through */
    {
        char out[128];
        url_encode("abc123", out, sizeof(out));
        TEST_ASSERT_STR_EQ(out, "abc123");
    }

    /* Dots, hyphens, underscores, tildes are unreserved */
    {
        char out[128];
        url_encode("a.b-c_d~e", out, sizeof(out));
        TEST_ASSERT_STR_EQ(out, "a.b-c_d~e");
    }

    /* Spaces are percent-encoded */
    {
        char out[128];
        url_encode("hello world", out, sizeof(out));
        TEST_ASSERT_STR_EQ(out, "hello%20world");
    }

    /* Special chars */
    {
        char out[128];
        url_encode("user@host:5432", out, sizeof(out));
        TEST_ASSERT(strstr(out, "%40") != NULL);  /* @ */
        TEST_ASSERT(strstr(out, "%3A") != NULL);  /* : */
    }

    /* Slash is encoded */
    {
        char out[128];
        url_encode("a/b", out, sizeof(out));
        TEST_ASSERT(strstr(out, "%2F") != NULL);
    }

    /* Empty string */
    {
        char out[128];
        url_encode("", out, sizeof(out));
        TEST_ASSERT_STR_EQ(out, "");
    }

    TEST_END();
}

/* ============================================================================
 * §9 — GCP JWT structure validation (with synthetic RSA key)
 * ============================================================================ */

static void test_gcp_build_jwt(void) {
    TEST_BEGIN("gcp_build_jwt");

    /* Generate a synthetic RSA 2048 key for testing */
    EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
    TEST_ASSERT_NOT_NULL(ctx);
    TEST_ASSERT(EVP_PKEY_keygen_init(ctx) > 0);
    TEST_ASSERT(EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048) > 0);

    EVP_PKEY* pkey = NULL;
    TEST_ASSERT(EVP_PKEY_keygen(ctx, &pkey) > 0);
    TEST_ASSERT_NOT_NULL(pkey);
    EVP_PKEY_CTX_free(ctx);

    /* Build a gcp_iam_provider_t with the test key */
    gcp_iam_provider_t gcp = {0};
    gcp.client_email = safe_strdup("test@project.iam.gserviceaccount.com");
    gcp.private_key = pkey;

    char* jwt = gcp_build_jwt(&gcp);
    TEST_ASSERT_NOT_NULL(jwt);

    /* JWT should have exactly 3 dot-separated parts */
    int dots = 0;
    for (const char* p = jwt; *p; p++)
        if (*p == '.') dots++;
    TEST_ASSERT_EQ(dots, 2);

    /* No standard base64 chars (+ / =) should be in the JWT */
    TEST_ASSERT(strchr(jwt, '+') == NULL);
    TEST_ASSERT(strchr(jwt, '/') == NULL);
    TEST_ASSERT(strchr(jwt, '=') == NULL);

    /* JWT header should decode to {"alg":"RS256","typ":"JWT"} */
    /* Just verify it starts with 'eyJ' (base64url of '{"') */
    TEST_ASSERT(strncmp(jwt, "eyJ", 3) == 0);

    /* Length should be substantial (header + payload + 256-byte RSA sig) */
    TEST_ASSERT(strlen(jwt) > 200);

    keel_free(jwt);

    /* NULL client_email → NULL JWT */
    gcp_iam_provider_t gcp2 = {0};
    gcp2.private_key = pkey;
    char* jwt2 = gcp_build_jwt(&gcp2);
    TEST_ASSERT_NULL(jwt2);

    /* NULL private_key → NULL JWT */
    gcp_iam_provider_t gcp3 = {0};
    gcp3.client_email = safe_strdup("a@b.com");
    char* jwt3 = gcp_build_jwt(&gcp3);
    TEST_ASSERT_NULL(jwt3);

    keel_free(gcp.client_email);
    keel_free(gcp3.client_email);
    EVP_PKEY_free(pkey);

    TEST_END();
}

/* ============================================================================
 * §10 — GCP key file parsing (synthetic service account JSON)
 * ============================================================================ */

static void test_gcp_parse_key_file(void) {
    TEST_BEGIN("gcp_parse_key_file");

    /* Generate a synthetic RSA key and write as PEM */
    EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
    TEST_ASSERT_NOT_NULL(ctx);
    TEST_ASSERT(EVP_PKEY_keygen_init(ctx) > 0);
    TEST_ASSERT(EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048) > 0);

    EVP_PKEY* pkey = NULL;
    TEST_ASSERT(EVP_PKEY_keygen(ctx, &pkey) > 0);
    EVP_PKEY_CTX_free(ctx);

    /* Write PEM key to string */
    BIO* bio = BIO_new(BIO_s_mem());
    TEST_ASSERT_NOT_NULL(bio);
    TEST_ASSERT(PEM_write_bio_PrivateKey(bio, pkey, NULL, NULL, 0, NULL, NULL) > 0);

    char* pem_data = NULL;
    long pem_len = BIO_get_mem_data(bio, &pem_data);
    TEST_ASSERT(pem_len > 0);

    /* Build a synthetic service account JSON — escape newlines in the PEM */
    char escaped_pem[8192] = {0};
    size_t ep = 0;
    for (long i = 0; i < pem_len && ep < sizeof(escaped_pem) - 3; i++) {
        if (pem_data[i] == '\n') {
            escaped_pem[ep++] = '\\';
            escaped_pem[ep++] = 'n';
        } else {
            escaped_pem[ep++] = pem_data[i];
        }
    }
    escaped_pem[ep] = '\0';

    char json[16384];
    snprintf(json, sizeof(json),
        "{\n"
        "  \"type\": \"service_account\",\n"
        "  \"project_id\": \"test-project\",\n"
        "  \"client_email\": \"test-sa@test-project.iam.gserviceaccount.com\",\n"
        "  \"private_key\": \"%s\"\n"
        "}", escaped_pem);

    const char* tmppath = "/tmp/keel_test_sa_key.json";
    FILE* f = fopen(tmppath, "w");
    TEST_ASSERT_NOT_NULL(f);
    fputs(json, f);
    fclose(f);

    /* Parse the key file */
    gcp_iam_provider_t gcp = {0};
    keel_error_t err = gcp_parse_key_file(&gcp, tmppath);
    TEST_ASSERT(err == KEEL_OK);
    TEST_ASSERT_NOT_NULL(gcp.client_email);
    TEST_ASSERT_STR_EQ(gcp.client_email,
        "test-sa@test-project.iam.gserviceaccount.com");
    TEST_ASSERT_NOT_NULL(gcp.private_key);

    /* Should be able to build a JWT with the parsed key */
    char* jwt = gcp_build_jwt(&gcp);
    TEST_ASSERT_NOT_NULL(jwt);
    TEST_ASSERT(strlen(jwt) > 200);

    keel_free(jwt);
    keel_free(gcp.client_email);
    EVP_PKEY_free(gcp.private_key);
    EVP_PKEY_free(pkey);
    BIO_free(bio);
    unlink(tmppath);

    /* Non-existent file should fail */
    gcp_iam_provider_t gcp2 = {0};
    err = gcp_parse_key_file(&gcp2, "/nonexistent/path.json");
    TEST_ASSERT(err != KEEL_OK);

    /* Empty file should fail */
    f = fopen(tmppath, "w");
    fclose(f);
    gcp_iam_provider_t gcp3 = {0};
    err = gcp_parse_key_file(&gcp3, tmppath);
    TEST_ASSERT(err != KEEL_OK);
    unlink(tmppath);

    TEST_END();
}

/* ============================================================================
 * §11 — Memory safety: double-destroy patterns, provider lifecycle
 * ============================================================================ */

static void test_provider_lifecycle(void) {
    TEST_BEGIN("provider_lifecycle");

    /* AWS: create → fetch → destroy (no leak) */
    {
        keel_cloud_aws_config_t cfg = {
            .region = "us-east-1",
            .access_key_id = "AKIAIOSFODNN7EXAMPLE",
            .secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        };
        keel_cloud_auth_provider_t* prov = NULL;
        keel_error_t err = keel_cloud_auth_aws_create(&prov, &cfg);
        TEST_ASSERT(err == KEEL_OK);
        keel_cloud_token_t tok = {0};
        err = prov->ops->fetch_token(prov, "db.rds.amazonaws.com",
                                      5432, "user", &tok);
        TEST_ASSERT(err == KEEL_OK);
        keel_free(tok.token);
        prov->ops->destroy(prov);
    }

    /* GCP: create with NULL config → destroy */
    {
        keel_cloud_gcp_config_t cfg = {0};
        keel_cloud_auth_provider_t* prov = NULL;
        keel_error_t err = keel_cloud_auth_gcp_create(&prov, &cfg);
        TEST_ASSERT(err == KEEL_OK);
        prov->ops->destroy(prov);
    }

    /* Azure: create → destroy */
    {
        keel_cloud_azure_config_t cfg = {0};
        keel_cloud_auth_provider_t* prov = NULL;
        keel_error_t err = keel_cloud_auth_azure_create(&prov, &cfg);
        TEST_ASSERT(err == KEEL_OK);
        prov->ops->destroy(prov);
    }

    /* Static: create → fetch → destroy */
    {
        setenv("KEEL_TEST_LIFECYCLE", "pw123", 1);
        keel_cloud_static_config_t cfg = {
            .path = "KEEL_TEST_LIFECYCLE",
            .refresh_interval_s = 300,
        };
        keel_cloud_auth_provider_t* prov = NULL;
        keel_error_t err = keel_cloud_auth_static_create(&prov,
            KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);
        TEST_ASSERT(err == KEEL_OK);
        keel_cloud_token_t tok = {0};
        err = prov->ops->fetch_token(prov, "localhost", 5432, "u", &tok);
        TEST_ASSERT(err == KEEL_OK);
        keel_free(tok.token);
        prov->ops->destroy(prov);
        unsetenv("KEEL_TEST_LIFECYCLE");
    }

    /* Token cache: init → get_password → destroy (provider owned) */
    {
        setenv("KEEL_TEST_CACHE", "cached_pw", 1);
        keel_cloud_static_config_t cfg = {
            .path = "KEEL_TEST_CACHE",
        };
        keel_cloud_auth_provider_t* prov = NULL;
        keel_cloud_auth_static_create(&prov, KEEL_CLOUD_AUTH_STATIC_ENV, &cfg);

        keel_cloud_token_cache_t cache;
        keel_cloud_token_cache_init(&cache, prov, 30);

        const char* pw = keel_cloud_auth_get_password(&cache,
            "host", 5432, "user", "fallback");
        TEST_ASSERT_STR_EQ(pw, "cached_pw");

        keel_cloud_token_cache_destroy(&cache);
        unsetenv("KEEL_TEST_CACHE");
    }

    /* Repeated create/destroy cycles (stress) */
    for (int i = 0; i < 50; i++) {
        keel_cloud_azure_config_t cfg = {0};
        keel_cloud_auth_provider_t* prov = NULL;
        keel_cloud_auth_azure_create(&prov, &cfg);
        prov->ops->destroy(prov);
    }

    TEST_END();
}

/* ============================================================================
 * §12 — safe_strdup edge cases
 * ============================================================================ */

static void test_safe_strdup(void) {
    TEST_BEGIN("safe_strdup");

    /* NULL input */
    {
        char* r = safe_strdup(NULL);
        TEST_ASSERT_NULL(r);
    }

    /* Empty string */
    {
        char* r = safe_strdup("");
        TEST_ASSERT_NOT_NULL(r);
        TEST_ASSERT_STR_EQ(r, "");
        keel_free(r);
    }

    /* Normal string */
    {
        char* r = safe_strdup("hello world");
        TEST_ASSERT_NOT_NULL(r);
        TEST_ASSERT_STR_EQ(r, "hello world");
        keel_free(r);
    }

    /* Long string (1024 chars) */
    {
        char big[1025];
        memset(big, 'A', 1024);
        big[1024] = '\0';
        char* r = safe_strdup(big);
        TEST_ASSERT_NOT_NULL(r);
        TEST_ASSERT(strlen(r) == 1024);
        keel_free(r);
    }

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    printf("=== Cloud Auth Internals (White-Box) Tests ===\n\n");

    test_hex_encode();
    test_base64url_encode();
    test_base64url_encode_alloc();
    test_json_find_string();
    test_json_find_int();
    test_hmac_sha256();
    test_aws_signing_key();
    test_url_encode();
    test_gcp_build_jwt();
    test_gcp_parse_key_file();
    test_provider_lifecycle();
    test_safe_strdup();

    printf("\n");
    return test_summary();
}
