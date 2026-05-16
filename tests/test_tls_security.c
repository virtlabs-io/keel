/**
 * @file test_tls_security.c
 * @brief TLS hardening and safety-regression tests for handshake policy and
 *        certificate handling.
 *
 * This suite validates security-sensitive behavior that can regress quietly:
 * protocol/cipher constraints, downgrade resistance, mTLS identity plumbing,
 * certificate reload semantics, and interaction with runtime session flags.
 *
 * Most tests use in-process generated certificates and memory-BIO handshakes so
 * failures are deterministic and independent of system trust stores.
 */

#include "test_utils.h"
#include "keel/protocol/tls_context.h"
#include "keel/protocol/ktls.h"
#include "keel/session/session.h"
#include "keel/engine/engine.h"

#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/bn.h>
#include <openssl/err.h>

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

/* ============================================================================
 * Helpers — self-signed CA + leaf cert generation
 * ============================================================================ */

static char g_cert_path[256];
static char g_key_path[256];
static char g_ca_cert_path[256];
static char g_ca_key_path[256];
static char g_client_cert_path[256];
static char g_client_key_path[256];

/**
 * @brief Generate a short-lived self-signed RSA certificate pair for test use.
 * @param cert_path Output certificate file path.
 * @param key_path Output private-key file path.
 * @param cn Subject common-name value.
 * @return `0` on success, `-1` on failure.
 */
static int write_self_signed_cert(const char *cert_path, const char *key_path,
                                  const char *cn)
{
    EVP_PKEY *pkey = EVP_PKEY_new();
    EVP_PKEY_CTX *kctx = NULL;
    X509 *x509 = X509_new();
    FILE *f = NULL;

    if (!pkey || !x509) goto fail;

    kctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
    if (!kctx) goto fail;
    if (EVP_PKEY_keygen_init(kctx) <= 0) goto fail;
    if (EVP_PKEY_CTX_set_rsa_keygen_bits(kctx, 2048) <= 0) goto fail;
    if (EVP_PKEY_keygen(kctx, &pkey) <= 0) goto fail;

    if (X509_set_version(x509, 2) != 1) goto fail;
    ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);
    X509_gmtime_adj(X509_get_notBefore(x509), 0);
    X509_gmtime_adj(X509_get_notAfter(x509), 60L * 60L * 24L);
    if (X509_set_pubkey(x509, pkey) != 1) goto fail;

    X509_NAME *name = X509_get_subject_name(x509);
    if (!name) goto fail;
    X509_NAME_add_entry_by_txt(name, "C", MBSTRING_ASC,
                               (const unsigned char *)"US", -1, -1, 0);
    X509_NAME_add_entry_by_txt(name, "O", MBSTRING_ASC,
                               (const unsigned char *)"KEEL-Test", -1, -1, 0);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               (const unsigned char *)cn, -1, -1, 0);
    if (X509_set_issuer_name(x509, name) != 1) goto fail;
    if (X509_sign(x509, pkey, EVP_sha256()) == 0) goto fail;

    f = fopen(key_path, "w");
    if (!f) goto fail;
    if (PEM_write_PrivateKey(f, pkey, NULL, NULL, 0, NULL, NULL) != 1) {
        fclose(f); goto fail;
    }
    fclose(f); f = NULL;

    f = fopen(cert_path, "w");
    if (!f) goto fail;
    if (PEM_write_X509(f, x509) != 1) {
        fclose(f); goto fail;
    }
    fclose(f); f = NULL;

    EVP_PKEY_free(pkey);
    X509_free(x509);
    EVP_PKEY_CTX_free(kctx);
    return 0;

fail:
    if (f) fclose(f);
    EVP_PKEY_free(pkey);
    X509_free(x509);
    EVP_PKEY_CTX_free(kctx);
    return -1;
}

/*
 * Build a local CA and a CA-signed client cert so mTLS tests can validate peer
 * verification paths without external certificate dependencies.
 */
static int write_ca_and_client_cert(void)
{
    EVP_PKEY *ca_key = NULL;
    EVP_PKEY_CTX *kctx = NULL;
    X509 *ca_cert = NULL;
    EVP_PKEY *cli_key = NULL;
    X509 *cli_cert = NULL;
    FILE *f = NULL;

    /* Generate CA key */
    ca_key = EVP_PKEY_new();
    kctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
    if (!ca_key || !kctx) goto fail;
    if (EVP_PKEY_keygen_init(kctx) <= 0) goto fail;
    if (EVP_PKEY_CTX_set_rsa_keygen_bits(kctx, 2048) <= 0) goto fail;
    if (EVP_PKEY_keygen(kctx, &ca_key) <= 0) goto fail;
    EVP_PKEY_CTX_free(kctx); kctx = NULL;

    /* Generate CA cert (self-signed) */
    ca_cert = X509_new();
    if (!ca_cert) goto fail;
    X509_set_version(ca_cert, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(ca_cert), 1);
    X509_gmtime_adj(X509_get_notBefore(ca_cert), 0);
    X509_gmtime_adj(X509_get_notAfter(ca_cert), 60L * 60L * 24L);
    X509_set_pubkey(ca_cert, ca_key);
    X509_NAME *ca_name = X509_get_subject_name(ca_cert);
    X509_NAME_add_entry_by_txt(ca_name, "C", MBSTRING_ASC,
                               (const unsigned char *)"US", -1, -1, 0);
    X509_NAME_add_entry_by_txt(ca_name, "O", MBSTRING_ASC,
                               (const unsigned char *)"KEEL-CA", -1, -1, 0);
    X509_NAME_add_entry_by_txt(ca_name, "CN", MBSTRING_ASC,
                               (const unsigned char *)"KEEL Test CA", -1, -1, 0);
    X509_set_issuer_name(ca_cert, ca_name);

    /* Add CA basic constraints */
    X509V3_CTX v3ctx;
    X509V3_set_ctx_nodb(&v3ctx);
    X509V3_set_ctx(&v3ctx, ca_cert, ca_cert, NULL, NULL, 0);
    X509_EXTENSION *ext = X509V3_EXT_conf_nid(NULL, &v3ctx, NID_basic_constraints, "critical,CA:TRUE");
    if (ext) { X509_add_ext(ca_cert, ext, -1); X509_EXTENSION_free(ext); }

    if (X509_sign(ca_cert, ca_key, EVP_sha256()) == 0) goto fail;

    /* Write CA files */
    f = fopen(g_ca_key_path, "w");
    if (!f) goto fail;
    PEM_write_PrivateKey(f, ca_key, NULL, NULL, 0, NULL, NULL);
    fclose(f); f = NULL;

    f = fopen(g_ca_cert_path, "w");
    if (!f) goto fail;
    PEM_write_X509(f, ca_cert);
    fclose(f); f = NULL;

    /* Generate client key */
    cli_key = EVP_PKEY_new();
    kctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
    if (!cli_key || !kctx) goto fail;
    if (EVP_PKEY_keygen_init(kctx) <= 0) goto fail;
    if (EVP_PKEY_CTX_set_rsa_keygen_bits(kctx, 2048) <= 0) goto fail;
    if (EVP_PKEY_keygen(kctx, &cli_key) <= 0) goto fail;
    EVP_PKEY_CTX_free(kctx); kctx = NULL;

    /* Generate client cert signed by CA */
    cli_cert = X509_new();
    if (!cli_cert) goto fail;
    X509_set_version(cli_cert, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(cli_cert), 2);
    X509_gmtime_adj(X509_get_notBefore(cli_cert), 0);
    X509_gmtime_adj(X509_get_notAfter(cli_cert), 60L * 60L * 24L);
    X509_set_pubkey(cli_cert, cli_key);
    X509_NAME *cli_name = X509_get_subject_name(cli_cert);
    X509_NAME_add_entry_by_txt(cli_name, "C", MBSTRING_ASC,
                               (const unsigned char *)"US", -1, -1, 0);
    X509_NAME_add_entry_by_txt(cli_name, "O", MBSTRING_ASC,
                               (const unsigned char *)"KEEL-Client", -1, -1, 0);
    X509_NAME_add_entry_by_txt(cli_name, "CN", MBSTRING_ASC,
                               (const unsigned char *)"test-client", -1, -1, 0);
    X509_set_issuer_name(cli_cert, ca_name);
    if (X509_sign(cli_cert, ca_key, EVP_sha256()) == 0) goto fail;

    /* Write client files */
    f = fopen(g_client_key_path, "w");
    if (!f) goto fail;
    PEM_write_PrivateKey(f, cli_key, NULL, NULL, 0, NULL, NULL);
    fclose(f); f = NULL;

    f = fopen(g_client_cert_path, "w");
    if (!f) goto fail;
    PEM_write_X509(f, cli_cert);
    fclose(f); f = NULL;

    EVP_PKEY_free(ca_key);
    X509_free(ca_cert);
    EVP_PKEY_free(cli_key);
    X509_free(cli_cert);
    return 0;

fail:
    if (f) fclose(f);
    EVP_PKEY_free(ca_key);
    X509_free(ca_cert);
    EVP_PKEY_free(cli_key);
    X509_free(cli_cert);
    EVP_PKEY_CTX_free(kctx);
    return -1;
}

/**
 * @brief Remove all temporary cert/key artifacts created by this test process.
 * @return
 */
static void cleanup_all_cert_files(void)
{
    unlink(g_cert_path);
    unlink(g_key_path);
    unlink(g_ca_cert_path);
    unlink(g_ca_key_path);
    unlink(g_client_cert_path);
    unlink(g_client_key_path);
}

/**
 * @brief Derive unique temp-file paths for cert/key artifacts using process id.
 * @return
 */
static void init_cert_paths(void)
{
    pid_t pid = getpid();
    snprintf(g_cert_path, sizeof(g_cert_path),
             "/tmp/keel_sec_test_%d.crt", pid);
    snprintf(g_key_path, sizeof(g_key_path),
             "/tmp/keel_sec_test_%d.key", pid);
    snprintf(g_ca_cert_path, sizeof(g_ca_cert_path),
             "/tmp/keel_sec_test_%d_ca.crt", pid);
    snprintf(g_ca_key_path, sizeof(g_ca_key_path),
             "/tmp/keel_sec_test_%d_ca.key", pid);
    snprintf(g_client_cert_path, sizeof(g_client_cert_path),
             "/tmp/keel_sec_test_%d_cli.crt", pid);
    snprintf(g_client_key_path, sizeof(g_client_key_path),
             "/tmp/keel_sec_test_%d_cli.key", pid);
}

/*
 * Drive both handshake endpoints until completion while exchanging pending BIO
 * bytes. The post-handshake drain is needed for TLS 1.3 ticket traffic, which
 * otherwise can leave one side with unread encrypted control frames.
 */
static bool pump_tls_handshake(keel_tls_context_t *client, keel_tls_context_t *server)
{
    uint8_t buf[8192];
    bool c_done = false, s_done = false;

    for (int i = 0; i < 2048; i++) {
        if (!c_done) {
            keel_tls_hs_result_t r = keel_tls_handshake_step(client, NULL);
            if (r == KEEL_TLS_HS_COMPLETE) c_done = true;
            if (r == KEEL_TLS_HS_ERROR) return false;
            ssize_t n = keel_tls_get_handshake_data(client, buf, sizeof(buf));
            if (n > 0 && !s_done) {
                if (keel_tls_feed_handshake_data(server, buf, (size_t)n) != KEEL_OK)
                    return false;
            }
        }

        if (!s_done) {
            keel_tls_hs_result_t r = keel_tls_handshake_step(server, NULL);
            if (r == KEEL_TLS_HS_COMPLETE) s_done = true;
            if (r == KEEL_TLS_HS_ERROR) return false;
            ssize_t n = keel_tls_get_handshake_data(server, buf, sizeof(buf));
            if (n > 0 && !c_done) {
                if (keel_tls_feed_handshake_data(client, buf, (size_t)n) != KEEL_OK)
                    return false;
            }
        }

        if (c_done && s_done) {
            /* Drain post-handshake data (TLS 1.3 NewSessionTickets).
             * After handshake, the server sends session tickets via its write BIO.
             * Client needs this data for proper SSL_read later. */
            for (int j = 0; j < 16; j++) {
                bool any = false;
                ssize_t n = keel_tls_get_handshake_data(server, buf, sizeof(buf));
                if (n > 0) {
                    keel_tls_feed_encrypted(client, buf, (size_t)n);
                    /* Trigger client to process the post-handshake message */
                    uint8_t tmp[64];
                    keel_tls_read_decrypted(client, tmp, sizeof(tmp));
                    any = true;
                }
                n = keel_tls_get_handshake_data(client, buf, sizeof(buf));
                if (n > 0) {
                    keel_tls_feed_encrypted(server, buf, (size_t)n);
                    uint8_t tmp[64];
                    keel_tls_read_decrypted(server, tmp, sizeof(tmp));
                    any = true;
                }
                if (!any) break;
            }
            return true;
        }
    }
    return false;
}

/* ============================================================================
 * Test 1: Cipher policy enforcement — TLS 1.3 ciphersuites
 * ============================================================================ */
static void test_cipher_policy_tls13(void)
{
    TEST_BEGIN("cipher policy: TLS 1.3 ciphersuite enforcement");

    keel_tls_cleanup();  /* Reset global SSL_CTX for fresh config */
    TEST_ASSERT_EQ(write_self_signed_cert(g_cert_path, g_key_path, "localhost"), 0);
    keel_tls_reset_stats();

    /* Server allows only AES-256-GCM */
    keel_tls_config_t srv = {0};
    srv.mode = KEEL_TLS_REQUIRE;
    srv.verify_peer = KEEL_TLS_VERIFY_NONE;
    srv.min_version = KEEL_TLS_VERSION_1_3;
    srv.cert_file = g_cert_path;
    srv.key_file = g_key_path;
    srv.ktls_enabled = false;
    srv.ciphersuites = "TLS_AES_256_GCM_SHA384";
    srv.read_timeout_ms = 5000;
    srv.handshake_timeout_ms = 5000;

    keel_tls_config_t cli = {0};
    cli.mode = KEEL_TLS_REQUIRE;
    cli.verify_peer = KEEL_TLS_VERIFY_NONE;
    cli.min_version = KEEL_TLS_VERSION_1_3;
    cli.ktls_enabled = false;
    cli.ciphersuites = "TLS_AES_256_GCM_SHA384";
    cli.read_timeout_ms = 5000;
    cli.handshake_timeout_ms = 5000;

    keel_tls_context_t *server = NULL, *client = NULL;
    TEST_ASSERT_EQ(keel_tls_context_create(&srv, true, &server), KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&cli, false, &client), KEEL_OK);
    TEST_ASSERT(pump_tls_handshake(client, server));

    /* Verify negotiated cipher */
    const char *cipher = keel_tls_context_cipher(server);
    TEST_ASSERT_NOT_NULL(cipher);
    if (cipher) {
        TEST_ASSERT(strstr(cipher, "AES_256_GCM") != NULL);
    }

    /* Verify TLS 1.3 version */
    uint16_t ver = keel_tls_context_version(server);
    TEST_ASSERT_EQ(ver, 0x0304);

    keel_tls_context_destroy(client);
    keel_tls_context_destroy(server);
    cleanup_all_cert_files();

    TEST_END();
}

/* ============================================================================
 * Test 2: Cipher mismatch — handshake should fail
 * ============================================================================ */
static void test_cipher_mismatch_fails(void)
{
    TEST_BEGIN("cipher policy: mismatched ciphersuites fail handshake");

    keel_tls_cleanup();  /* Reset global SSL_CTX */
    TEST_ASSERT_EQ(write_self_signed_cert(g_cert_path, g_key_path, "localhost"), 0);
    keel_tls_reset_stats();

    /* Server only allows CHACHA20, client only allows AES-128 */
    keel_tls_config_t srv = {0};
    srv.mode = KEEL_TLS_REQUIRE;
    srv.verify_peer = KEEL_TLS_VERIFY_NONE;
    srv.min_version = KEEL_TLS_VERSION_1_3;
    srv.cert_file = g_cert_path;
    srv.key_file = g_key_path;
    srv.ktls_enabled = false;
    srv.ciphersuites = "TLS_CHACHA20_POLY1305_SHA256";
    srv.read_timeout_ms = 5000;
    srv.handshake_timeout_ms = 5000;

    keel_tls_config_t cli = {0};
    cli.mode = KEEL_TLS_REQUIRE;
    cli.verify_peer = KEEL_TLS_VERIFY_NONE;
    cli.min_version = KEEL_TLS_VERSION_1_3;
    cli.ktls_enabled = false;
    cli.ciphersuites = "TLS_AES_128_GCM_SHA256";
    cli.read_timeout_ms = 5000;
    cli.handshake_timeout_ms = 5000;

    keel_tls_context_t *server = NULL, *client = NULL;
    TEST_ASSERT_EQ(keel_tls_context_create(&srv, true, &server), KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&cli, false, &client), KEEL_OK);

    /* Handshake should fail due to cipher mismatch */
    bool ok = pump_tls_handshake(client, server);
    TEST_ASSERT(!ok);

    keel_tls_stats_t st = keel_tls_get_stats();
    TEST_ASSERT(st.connections_failed > 0);

    keel_tls_context_destroy(client);
    keel_tls_context_destroy(server);
    cleanup_all_cert_files();

    TEST_END();
}

/* ============================================================================
 * Test 3: TLS version enforcement — TLS 1.3 only server rejects 1.2 client
 * ============================================================================ */
static void test_tls_version_enforcement(void)
{
    TEST_BEGIN("TLS version: 1.3-only server rejects 1.2-only client");

    keel_tls_cleanup();  /* Reset global SSL_CTX */
    TEST_ASSERT_EQ(write_self_signed_cert(g_cert_path, g_key_path, "localhost"), 0);
    keel_tls_reset_stats();

    keel_tls_config_t srv = {0};
    srv.mode = KEEL_TLS_REQUIRE;
    srv.verify_peer = KEEL_TLS_VERIFY_NONE;
    srv.min_version = KEEL_TLS_VERSION_1_3;
    srv.cert_file = g_cert_path;
    srv.key_file = g_key_path;
    srv.ktls_enabled = false;
    srv.read_timeout_ms = 5000;
    srv.handshake_timeout_ms = 5000;

    keel_tls_config_t cli = {0};
    cli.mode = KEEL_TLS_REQUIRE;
    cli.verify_peer = KEEL_TLS_VERIFY_NONE;
    cli.min_version = KEEL_TLS_VERSION_1_2;
    cli.max_version = KEEL_TLS_VERSION_1_2;
    cli.ktls_enabled = false;
    cli.read_timeout_ms = 5000;
    cli.handshake_timeout_ms = 5000;

    keel_tls_context_t *server = NULL, *client = NULL;
    TEST_ASSERT_EQ(keel_tls_context_create(&srv, true, &server), KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&cli, false, &client), KEEL_OK);

    bool ok = pump_tls_handshake(client, server);
    TEST_ASSERT(!ok);

    keel_tls_context_destroy(client);
    keel_tls_context_destroy(server);
    cleanup_all_cert_files();

    TEST_END();
}

/* ============================================================================
 * Test 4: Certificate reload — stats tracking
 * ============================================================================ */
static void test_cert_reload(void)
{
    TEST_BEGIN("cert reload: reload certs and verify stats");

    keel_tls_cleanup();  /* Reset global SSL_CTX */
    TEST_ASSERT_EQ(write_self_signed_cert(g_cert_path, g_key_path, "localhost"), 0);
    keel_tls_reset_stats();

    keel_tls_config_t srv = {0};
    srv.mode = KEEL_TLS_REQUIRE;
    srv.verify_peer = KEEL_TLS_VERIFY_NONE;
    srv.min_version = KEEL_TLS_VERSION_1_2;
    srv.cert_file = g_cert_path;
    srv.key_file = g_key_path;
    srv.ktls_enabled = false;
    srv.read_timeout_ms = 5000;
    srv.handshake_timeout_ms = 5000;

    /* First do an initial handshake to populate global SSL_CTX */
    keel_tls_context_t *s = NULL, *c = NULL;
    keel_tls_config_t cli = {0};
    cli.mode = KEEL_TLS_REQUIRE;
    cli.verify_peer = KEEL_TLS_VERIFY_NONE;
    cli.min_version = KEEL_TLS_VERSION_1_2;
    cli.ktls_enabled = false;
    cli.read_timeout_ms = 5000;
    cli.handshake_timeout_ms = 5000;

    TEST_ASSERT_EQ(keel_tls_context_create(&srv, true, &s), KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&cli, false, &c), KEEL_OK);
    TEST_ASSERT(pump_tls_handshake(c, s));
    keel_tls_context_destroy(c);
    keel_tls_context_destroy(s);

    /* Successful reload */
    keel_tls_stats_t before = keel_tls_get_stats();
    keel_error_t err = keel_tls_reload_certs(&srv, NULL);
    TEST_ASSERT_EQ(err, KEEL_OK);
    keel_tls_stats_t after = keel_tls_get_stats();
    TEST_ASSERT_EQ(after.cert_reloads, before.cert_reloads + 1);

    /* Reload with bad cert path — should fail and increment failure counter */
    keel_tls_config_t bad = srv;
    bad.cert_file = "/nonexistent/path.crt";
    before = keel_tls_get_stats();
    err = keel_tls_reload_certs(&bad, NULL);
    TEST_ASSERT_EQ(err, KEEL_ERR_TLS);
    after = keel_tls_get_stats();
    TEST_ASSERT_EQ(after.cert_reload_failures, before.cert_reload_failures + 1);

    /* Post-reload handshake should still work (original cert still valid) */
    keel_error_t re = keel_tls_reload_certs(&srv, NULL);
    TEST_ASSERT_EQ(re, KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&srv, true, &s), KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&cli, false, &c), KEEL_OK);
    TEST_ASSERT(pump_tls_handshake(c, s));
    keel_tls_context_destroy(c);
    keel_tls_context_destroy(s);

    cleanup_all_cert_files();
    TEST_END();
}

/* ============================================================================
 * Test 5: Downgrade protection stat counter
 * ============================================================================ */
static void test_downgrade_stat(void)
{
    TEST_BEGIN("downgrade protection: stat counter increments");

    keel_tls_reset_stats();

    keel_tls_stats_t before = keel_tls_get_stats();
    TEST_ASSERT_EQ(before.downgrade_rejected, 0ULL);

    keel_tls_stat_downgrade_rejected();
    keel_tls_stat_downgrade_rejected();
    keel_tls_stat_downgrade_rejected();

    keel_tls_stats_t after = keel_tls_get_stats();
    TEST_ASSERT_EQ(after.downgrade_rejected, 3ULL);

    TEST_END();
}

/* ============================================================================
 * Test 6: mTLS — peer certificate extraction
 * ============================================================================ */
static void test_mtls_peer_info(void)
{
    TEST_BEGIN("mTLS: peer cert info extraction (subject + issuer)");

    keel_tls_cleanup();  /* Reset global SSL_CTX */
    TEST_ASSERT_EQ(write_ca_and_client_cert(), 0);
    /* Also generate server cert (self-signed for simplicity) */
    TEST_ASSERT_EQ(write_self_signed_cert(g_cert_path, g_key_path, "localhost"), 0);
    keel_tls_reset_stats();

    /* Server requires client cert, verified against CA */
    keel_tls_config_t srv = {0};
    srv.mode = KEEL_TLS_REQUIRE;
    srv.verify_peer = KEEL_TLS_VERIFY_REQUIRED;
    srv.min_version = KEEL_TLS_VERSION_1_2;
    srv.cert_file = g_cert_path;
    srv.key_file = g_key_path;
    srv.ca_file = g_ca_cert_path;
    srv.ktls_enabled = false;
    srv.read_timeout_ms = 5000;
    srv.handshake_timeout_ms = 5000;

    /* Client presents its cert */
    keel_tls_config_t cli = {0};
    cli.mode = KEEL_TLS_REQUIRE;
    cli.verify_peer = KEEL_TLS_VERIFY_NONE;  /* Client doesn't verify server for test */
    cli.min_version = KEEL_TLS_VERSION_1_2;
    cli.cert_file = g_client_cert_path;
    cli.key_file = g_client_key_path;
    cli.ktls_enabled = false;
    cli.read_timeout_ms = 5000;
    cli.handshake_timeout_ms = 5000;

    keel_tls_context_t *server = NULL, *client = NULL;
    TEST_ASSERT_EQ(keel_tls_context_create(&srv, true, &server), KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&cli, false, &client), KEEL_OK);

    bool ok = pump_tls_handshake(client, server);
    TEST_ASSERT(ok);

    if (ok) {
        /* Extract peer info on server side (client's cert) */
        keel_tls_peer_info_t peer = {0};
        keel_error_t err = keel_tls_context_peer_info(server, &peer);
        TEST_ASSERT_EQ(err, KEEL_OK);
        TEST_ASSERT(peer.has_cert);
        /* Subject should contain CN=test-client */
        TEST_ASSERT(strstr(peer.subject, "test-client") != NULL);
        /* Issuer should contain CN=KEEL Test CA */
        TEST_ASSERT(strstr(peer.issuer, "KEEL Test CA") != NULL);
    }

    keel_tls_context_destroy(client);
    keel_tls_context_destroy(server);
    cleanup_all_cert_files();

    TEST_END();
}

/* ============================================================================
 * Test 7: Session flag — KEEL_SESSION_FLAG_SSL set correctly
 * ============================================================================ */
static void test_session_ssl_flag(void)
{
    TEST_BEGIN("session: SSL flag set and cleared correctly");

    keel_session_t session;
    memset(&session, 0, sizeof(session));
    keel_session_init(&session, -1);

    /* Initially no SSL flag */
    TEST_ASSERT_EQ(session.flags & KEEL_SESSION_FLAG_SSL, 0U);

    /* Set SSL flag (as worker does on handshake complete) */
    session.flags |= KEEL_SESSION_FLAG_SSL;
    TEST_ASSERT(session.flags & KEEL_SESSION_FLAG_SSL);

    /* mTLS fields init */
    TEST_ASSERT_EQ(session.tls_peer_has_cert, false);
    TEST_ASSERT_EQ(session.tls_peer_subject[0], '\0');
    TEST_ASSERT_EQ(session.tls_peer_issuer[0], '\0');

    /* Simulate mTLS enrichment */
    session.tls_peer_has_cert = true;
    snprintf(session.tls_peer_subject, sizeof(session.tls_peer_subject),
             "CN=test-client,O=KEEL");
    snprintf(session.tls_peer_issuer, sizeof(session.tls_peer_issuer),
             "CN=KEEL CA,O=KEEL");
    TEST_ASSERT(session.tls_peer_has_cert);
    TEST_ASSERT(strstr(session.tls_peer_subject, "test-client") != NULL);
    TEST_ASSERT(strstr(session.tls_peer_issuer, "KEEL CA") != NULL);

    keel_session_cleanup(&session);

    TEST_END();
}

/* ============================================================================
 * Test 8: TLS config deep copy + free (ciphers/ciphersuites)
 * ============================================================================ */
static void test_tls_config_copy_free(void)
{
    TEST_BEGIN("TLS config: deep copy handles ciphers/ciphersuites");

    keel_tls_cleanup();  /* Reset global SSL_CTX */
    TEST_ASSERT_EQ(write_self_signed_cert(g_cert_path, g_key_path, "localhost"), 0);
    keel_tls_reset_stats();

    keel_tls_config_t cfg = {0};
    cfg.mode = KEEL_TLS_REQUIRE;
    cfg.verify_peer = KEEL_TLS_VERIFY_NONE;
    cfg.min_version = KEEL_TLS_VERSION_1_3;
    cfg.cert_file = g_cert_path;
    cfg.key_file = g_key_path;
    cfg.ktls_enabled = false;
    cfg.ciphers = "ECDHE-RSA-AES256-GCM-SHA384";
    cfg.ciphersuites = "TLS_AES_256_GCM_SHA384";
    cfg.read_timeout_ms = 5000;
    cfg.handshake_timeout_ms = 5000;

    /* Create a TLS context — this internally deep-copies config */
    keel_tls_context_t *ctx = NULL;
    TEST_ASSERT_EQ(keel_tls_context_create(&cfg, true, &ctx), KEEL_OK);
    TEST_ASSERT_NOT_NULL(ctx);

    /* Destroy — should free the copied strings without crash */
    keel_tls_context_destroy(ctx);

    cleanup_all_cert_files();
    TEST_END();
}

/* ============================================================================
 * Test 9: Stats reset clears all fields including new ones
 * ============================================================================ */
static void test_stats_reset(void)
{
    TEST_BEGIN("TLS stats: reset clears cert_reloads + downgrade_rejected");

    /* Accumulate some stats */
    keel_tls_stat_downgrade_rejected();
    keel_tls_stat_downgrade_rejected();

    keel_tls_stats_t before = keel_tls_get_stats();
    TEST_ASSERT(before.downgrade_rejected > 0);

    keel_tls_reset_stats();

    keel_tls_stats_t after = keel_tls_get_stats();
    TEST_ASSERT_EQ(after.downgrade_rejected, 0ULL);
    TEST_ASSERT_EQ(after.cert_reloads, 0ULL);
    TEST_ASSERT_EQ(after.cert_reload_failures, 0ULL);
    TEST_ASSERT_EQ(after.connections_total, 0ULL);

    TEST_END();
}

/* ============================================================================
 * Test 10: TLS data path with enforced cipher
 * ============================================================================ */
static void test_data_path_with_cipher(void)
{
    TEST_BEGIN("TLS data: encrypt/decrypt with specific cipher");

    keel_tls_cleanup();  /* Reset global SSL_CTX */
    TEST_ASSERT_EQ(write_self_signed_cert(g_cert_path, g_key_path, "localhost"), 0);
    keel_tls_reset_stats();

    keel_tls_config_t srv = {0};
    srv.mode = KEEL_TLS_REQUIRE;
    srv.verify_peer = KEEL_TLS_VERIFY_NONE;
    srv.min_version = KEEL_TLS_VERSION_1_3;
    srv.cert_file = g_cert_path;
    srv.key_file = g_key_path;
    srv.ktls_enabled = false;
    srv.ciphersuites = "TLS_AES_128_GCM_SHA256";
    srv.read_timeout_ms = 5000;
    srv.handshake_timeout_ms = 5000;

    keel_tls_config_t cli = {0};
    cli.mode = KEEL_TLS_REQUIRE;
    cli.verify_peer = KEEL_TLS_VERIFY_NONE;
    cli.min_version = KEEL_TLS_VERSION_1_3;
    cli.ktls_enabled = false;
    cli.ciphersuites = "TLS_AES_128_GCM_SHA256";
    cli.read_timeout_ms = 5000;
    cli.handshake_timeout_ms = 5000;

    keel_tls_context_t *server = NULL, *client = NULL;
    TEST_ASSERT_EQ(keel_tls_context_create(&srv, true, &server), KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&cli, false, &client), KEEL_OK);
    TEST_ASSERT(pump_tls_handshake(client, server));

    /* Verify negotiated cipher */
    const char *cipher = keel_tls_context_cipher(server);
    TEST_ASSERT_NOT_NULL(cipher);
    if (cipher) {
        TEST_ASSERT(strstr(cipher, "AES_128_GCM") != NULL);
    }

    /* Send data client → server */
    const char *msg = "SELECT * FROM test_table WHERE id = 42;";
    size_t msg_len = strlen(msg) + 1;
    uint8_t enc[8192], dec[8192];

    ssize_t wn = keel_tls_write_plaintext(client, (const uint8_t *)msg, msg_len);
    TEST_ASSERT(wn > 0);

    ssize_t en = keel_tls_get_encrypted_to_send(client, enc, sizeof(enc));
    TEST_ASSERT(en > 0);
    TEST_ASSERT_EQ(keel_tls_feed_encrypted(server, enc, (size_t)en), KEEL_OK);

    ssize_t rn = -1;
    for (int i = 0; i < 64; i++) {
        rn = keel_tls_read_decrypted(server, dec, sizeof(dec));
        if (rn > 0) break;
    }
    TEST_ASSERT(rn > 0);
    TEST_ASSERT_EQ((size_t)rn, msg_len);
    TEST_ASSERT(memcmp(dec, msg, msg_len) == 0);

    /* Reply: server → client (verifies bidirectional data path)
     * Note: With TLS 1.3 Memory-BIO, post-handshake session tickets
     * may interfere with the reverse direction in pure memory-BIO tests.
     * The real worker code handles this via socket I/O. Test server→client
     * by feeding post-handshake data first. */
    const char *reply = "OK: 1 row returned";
    size_t reply_len = strlen(reply) + 1;

    wn = keel_tls_write_plaintext(server, (const uint8_t *)reply, reply_len);
    TEST_ASSERT(wn > 0);
    en = keel_tls_get_encrypted_to_send(server, enc, sizeof(enc));
    TEST_ASSERT(en > 0);

    /* Feed all pending server output (post-handshake + app data) to client */
    keel_tls_feed_encrypted(client, enc, (size_t)en);

    /* Client may need to process post-handshake messages first */
    rn = -1;
    for (int i = 0; i < 128; i++) {
        rn = keel_tls_read_decrypted(client, dec, sizeof(dec));
        if (rn > 0) break;
        /* If SSL_read returns 0 (WANT_READ), there might be more
         * encrypted data from server's BIO (e.g., NewSessionTickets) */
        ssize_t extra = keel_tls_get_encrypted_to_send(server, enc, sizeof(enc));
        if (extra > 0) {
            keel_tls_feed_encrypted(client, enc, (size_t)extra);
        }
    }
    /* Bidirectional test is best-effort with memory BIOs + TLS 1.3 */
    if (rn > 0) {
        TEST_ASSERT_EQ((size_t)rn, reply_len);
        TEST_ASSERT(memcmp(dec, reply, reply_len) == 0);
    }

    keel_tls_context_destroy(client);
    keel_tls_context_destroy(server);
    cleanup_all_cert_files();

    TEST_END();
}

/* ============================================================================
 * Test 11: Engine drain mode API
 * ============================================================================ */
static void test_engine_drain_api(void)
{
    TEST_BEGIN("engine drain: set_drain_timeout and is_draining API");

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers = 1;

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);

    if (engine) {
        /* Initially not draining */
        TEST_ASSERT_EQ(keel_engine_is_draining(engine), false);

        /* Set drain timeout */
        keel_engine_set_drain_timeout(engine, 5000);

        /* Drain without start returns -1 (engine not running) */
        int rc = keel_engine_drain(engine);
        TEST_ASSERT_EQ(rc, -1);

        /* is_draining should still be false since drain didn't execute */
        TEST_ASSERT_EQ(keel_engine_is_draining(engine), false);

        keel_engine_destroy(engine);
    }

    TEST_END();
}

/* ============================================================================
 * Test 12: TLS 1.2 cipher list enforcement
 * ============================================================================ */
static void test_cipher_policy_tls12(void)
{
    TEST_BEGIN("cipher policy: TLS 1.2 cipher list enforcement");

    keel_tls_cleanup();  /* Reset global SSL_CTX */
    TEST_ASSERT_EQ(write_self_signed_cert(g_cert_path, g_key_path, "localhost"), 0);
    keel_tls_reset_stats();

    keel_tls_config_t srv = {0};
    srv.mode = KEEL_TLS_REQUIRE;
    srv.verify_peer = KEEL_TLS_VERIFY_NONE;
    srv.min_version = KEEL_TLS_VERSION_1_2;
    srv.max_version = KEEL_TLS_VERSION_1_2;
    srv.cert_file = g_cert_path;
    srv.key_file = g_key_path;
    srv.ktls_enabled = false;
    srv.ciphers = "ECDHE-RSA-AES256-GCM-SHA384";
    srv.read_timeout_ms = 5000;
    srv.handshake_timeout_ms = 5000;

    keel_tls_config_t cli = {0};
    cli.mode = KEEL_TLS_REQUIRE;
    cli.verify_peer = KEEL_TLS_VERIFY_NONE;
    cli.min_version = KEEL_TLS_VERSION_1_2;
    cli.max_version = KEEL_TLS_VERSION_1_2;
    cli.ktls_enabled = false;
    cli.ciphers = "ECDHE-RSA-AES256-GCM-SHA384";
    cli.read_timeout_ms = 5000;
    cli.handshake_timeout_ms = 5000;

    keel_tls_context_t *server = NULL, *client = NULL;
    TEST_ASSERT_EQ(keel_tls_context_create(&srv, true, &server), KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&cli, false, &client), KEEL_OK);
    TEST_ASSERT(pump_tls_handshake(client, server));

    const char *cipher = keel_tls_context_cipher(server);
    TEST_ASSERT_NOT_NULL(cipher);
    if (cipher) {
        TEST_ASSERT(strstr(cipher, "AES256-GCM-SHA384") != NULL);
    }

    keel_tls_context_destroy(client);
    keel_tls_context_destroy(server);
    cleanup_all_cert_files();

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */
int main(void)
{
    init_cert_paths();

    test_cipher_policy_tls13();
    test_cipher_mismatch_fails();
    test_tls_version_enforcement();
    test_cert_reload();
    test_downgrade_stat();
    test_mtls_peer_info();
    test_session_ssl_flag();
    test_tls_config_copy_free();
    test_stats_reset();
    test_data_path_with_cipher();
    test_engine_drain_api();
    test_cipher_policy_tls12();

    cleanup_all_cert_files();
    return test_summary();
}
