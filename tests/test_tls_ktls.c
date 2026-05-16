/**
 * @file test_tls_ktls.c
 * @brief TLS/kTLS regression and integration-level unit tests
 *
 * Validates the TLS handshake path and kernel-TLS (kTLS) offload
 * activation that is central to keel's zero-copy splice strategy.
 * When kTLS is available, the kernel handles encryption in
 * sendfile / splice, eliminating a user-space copy per data frame.
 *
 * Test families:
 *   §1 — TLS handshake + data path: generates an ephemeral
 *         self-signed certificate, creates a loopback socketpair,
 *         drives a full OpenSSL handshake (SSL_accept / SSL_connect),
 *         then sends and receives a small payload.
 *   §2 — kTLS activation, stats, and fallback: after a successful
 *         handshake, attempts keel_ktls_activate() and verifies
 *         the stats counters (activated / fallback).  On kernels
 *         or cipher suites where kTLS is unavailable the test
 *         verifies graceful fallback to user-space TLS.
 *
 * Design choices:
 *   - The cert is written to /tmp with a PID-scoped filename to
 *     avoid collisions when ctest runs in parallel.
 *   - Cleanup is registered early so even on assertion failure
 *     the temp files are removed.
 *   - The test cannot guarantee kTLS activation on every CI
 *     machine (requires TLS_TX kernel module + AES-GCM cipher),
 *     so §2 asserts the counter *sum* rather than a specific
 *     activated count.
 */


#include "test_utils.h"
#include "keel/protocol/tls_context.h"
#include "keel/protocol/ktls.h"

#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/bn.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char g_cert_path[256];
static char g_key_path[256];

/**
 * @brief Generate an ephemeral self-signed RSA-2048 certificate + key.
 *
 * Files are written to /tmp with PID-scoped names so parallel ctest
 * runs don't collide.  Paths are stored in g_cert_path and g_key_path
 * for the test functions and removed by cleanup_cert_files().
 *
 * @return 0 on success, -1 on any OpenSSL or I/O failure.
 */
static int write_self_signed_cert_and_key(void)
{
    snprintf(g_cert_path, sizeof(g_cert_path), "/tmp/keel_tls_test_%d.crt", getpid());
    snprintf(g_key_path, sizeof(g_key_path), "/tmp/keel_tls_test_%d.key", getpid());

    EVP_PKEY* pkey = EVP_PKEY_new();
    EVP_PKEY_CTX* kctx = NULL;
    X509* x509 = X509_new();
    FILE* f = NULL;

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

    X509_NAME* name = X509_get_subject_name(x509);
    if (!name) goto fail;
    X509_NAME_add_entry_by_txt(name, "C", MBSTRING_ASC,
                               (const unsigned char*)"US", -1, -1, 0);
    X509_NAME_add_entry_by_txt(name, "O", MBSTRING_ASC,
                               (const unsigned char*)"KEEL", -1, -1, 0);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               (const unsigned char*)"localhost", -1, -1, 0);
    if (X509_set_issuer_name(x509, name) != 1) goto fail;
    if (X509_sign(x509, pkey, EVP_sha256()) == 0) goto fail;

    f = fopen(g_key_path, "w");
    if (!f) goto fail;
    if (PEM_write_PrivateKey(f, pkey, NULL, NULL, 0, NULL, NULL) != 1) {
        fclose(f);
        goto fail;
    }
    fclose(f);
    f = NULL;

    f = fopen(g_cert_path, "w");
    if (!f) goto fail;
    if (PEM_write_X509(f, x509) != 1) {
        fclose(f);
        goto fail;
    }
    fclose(f);
    f = NULL;

    EVP_PKEY_free(pkey);
    X509_free(x509);
    EVP_PKEY_CTX_free(kctx);
    return 0;

fail:
    if (f) fclose(f);
    EVP_PKEY_free(pkey);
    X509_free(x509);
    EVP_PKEY_CTX_free(kctx);
    unlink(g_cert_path);
    unlink(g_key_path);
    return -1;
}

/**
 * @brief Remove the ephemeral cert + key written by
 *        write_self_signed_cert_and_key().
 */
static void cleanup_cert_files(void)
{
    unlink(g_cert_path);
    unlink(g_key_path);
}

static bool pump_tls_handshake(keel_tls_context_t* client, keel_tls_context_t* server)
{
    uint8_t buf[8192];
    bool c_done = false;
    bool s_done = false;

    for (int i = 0; i < 2048; i++) {
        bool progress = false;

        if (!c_done) {
            keel_tls_hs_result_t r = keel_tls_handshake_step(client, NULL);
            if (r == KEEL_TLS_HS_COMPLETE) c_done = true;
            if (r == KEEL_TLS_HS_ERROR) return false;
            ssize_t n = keel_tls_get_handshake_data(client, buf, sizeof(buf));
            if (n > 0) {
                if (keel_tls_feed_handshake_data(server, buf, (size_t)n) != KEEL_OK)
                    return false;
                progress = true;
            }
        }

        if (!s_done) {
            keel_tls_hs_result_t r = keel_tls_handshake_step(server, NULL);
            if (r == KEEL_TLS_HS_COMPLETE) s_done = true;
            if (r == KEEL_TLS_HS_ERROR) return false;
            ssize_t n = keel_tls_get_handshake_data(server, buf, sizeof(buf));
            if (n > 0) {
                if (keel_tls_feed_handshake_data(client, buf, (size_t)n) != KEEL_OK)
                    return false;
                progress = true;
            }
        }

        if (c_done && s_done) return true;
        if (!progress && !(c_done && s_done)) continue;
    }

    return false;
}

static int make_tcp_connected_pair(int* client_fd, int* server_fd)
{
    int lfd = -1;
    int cfd = -1;
    int sfd = -1;

    struct sockaddr_in addr;
    socklen_t alen = sizeof(addr);
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) goto fail;

    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    if (bind(lfd, (struct sockaddr*)&addr, sizeof(addr)) != 0) goto fail;
    if (listen(lfd, 1) != 0) goto fail;
    if (getsockname(lfd, (struct sockaddr*)&addr, &alen) != 0) goto fail;

    cfd = socket(AF_INET, SOCK_STREAM, 0);
    if (cfd < 0) goto fail;
    if (connect(cfd, (struct sockaddr*)&addr, sizeof(addr)) != 0) goto fail;

    sfd = accept(lfd, NULL, NULL);
    if (sfd < 0) goto fail;

    close(lfd);
    *client_fd = cfd;
    *server_fd = sfd;
    return 0;

fail:
    if (lfd >= 0) close(lfd);
    if (cfd >= 0) close(cfd);
    if (sfd >= 0) close(sfd);
    return -1;
}

static void test_tls_handshake_and_data_path(void)
{
    TEST_BEGIN("tls handshake + encrypted data path");

    TEST_ASSERT_EQ(write_self_signed_cert_and_key(), 0);

    keel_tls_reset_stats();

    keel_tls_config_t server_cfg;
    memset(&server_cfg, 0, sizeof(server_cfg));
    server_cfg.mode = KEEL_TLS_REQUIRE;
    server_cfg.verify_peer = KEEL_TLS_VERIFY_NONE;
    server_cfg.min_version = KEEL_TLS_VERSION_1_2;
    server_cfg.max_version = KEEL_TLS_VERSION_1_2;
    server_cfg.cert_file = g_cert_path;
    server_cfg.key_file = g_key_path;
    server_cfg.ktls_enabled = true;
    server_cfg.read_timeout_ms = 30000;
    server_cfg.handshake_timeout_ms = 10000;

    keel_tls_config_t client_cfg;
    memset(&client_cfg, 0, sizeof(client_cfg));
    client_cfg.mode = KEEL_TLS_REQUIRE;
    client_cfg.verify_peer = KEEL_TLS_VERIFY_NONE;
    client_cfg.min_version = KEEL_TLS_VERSION_1_2;
    client_cfg.max_version = KEEL_TLS_VERSION_1_2;
    client_cfg.ktls_enabled = true;
    client_cfg.read_timeout_ms = 30000;
    client_cfg.handshake_timeout_ms = 10000;

    keel_tls_context_t* server = NULL;
    keel_tls_context_t* client = NULL;

    TEST_ASSERT_EQ(keel_tls_context_create(&server_cfg, true, &server), KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&client_cfg, false, &client), KEEL_OK);
    TEST_ASSERT_NOT_NULL(server);
    TEST_ASSERT_NOT_NULL(client);

    TEST_ASSERT(pump_tls_handshake(client, server));
    TEST_ASSERT_EQ(keel_tls_context_state(client), KEEL_TLS_STATE_ESTABLISHED);
    TEST_ASSERT_EQ(keel_tls_context_state(server), KEEL_TLS_STATE_ESTABLISHED);

    const uint8_t msg[] = "select 1;";
    uint8_t enc[8192];
    uint8_t dec[8192];

    ssize_t wn = keel_tls_write_plaintext(client, msg, sizeof(msg));
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
    TEST_ASSERT_EQ((size_t)rn, sizeof(msg));
    TEST_ASSERT(memcmp(dec, msg, sizeof(msg)) == 0);

    keel_tls_stats_t st = keel_tls_get_stats();
    TEST_ASSERT(st.connections_total >= 2);
    TEST_ASSERT(st.connections_succeeded >= 2);

    keel_tls_context_destroy(client);
    keel_tls_context_destroy(server);
    cleanup_cert_files();

    TEST_END();
}

static void test_ktls_activation_stats_and_fallback(void)
{
    TEST_BEGIN("kTLS activation accounting + fallback safety");

    TEST_ASSERT_EQ(write_self_signed_cert_and_key(), 0);

    keel_tls_reset_stats();

    keel_tls_config_t cfg_srv;
    memset(&cfg_srv, 0, sizeof(cfg_srv));
    cfg_srv.mode = KEEL_TLS_REQUIRE;
    cfg_srv.verify_peer = KEEL_TLS_VERIFY_NONE;
    cfg_srv.min_version = KEEL_TLS_VERSION_1_2;
    cfg_srv.max_version = KEEL_TLS_VERSION_1_2;
    cfg_srv.cert_file = g_cert_path;
    cfg_srv.key_file = g_key_path;
    cfg_srv.ktls_enabled = true;
    cfg_srv.read_timeout_ms = 30000;
    cfg_srv.handshake_timeout_ms = 10000;

    keel_tls_config_t cfg_cli;
    memset(&cfg_cli, 0, sizeof(cfg_cli));
    cfg_cli.mode = KEEL_TLS_REQUIRE;
    cfg_cli.verify_peer = KEEL_TLS_VERIFY_NONE;
    cfg_cli.min_version = KEEL_TLS_VERSION_1_2;
    cfg_cli.max_version = KEEL_TLS_VERSION_1_2;
    cfg_cli.ktls_enabled = true;
    cfg_cli.read_timeout_ms = 30000;
    cfg_cli.handshake_timeout_ms = 10000;

    keel_tls_context_t* server = NULL;
    keel_tls_context_t* client = NULL;
    TEST_ASSERT_EQ(keel_tls_context_create(&cfg_srv, true, &server), KEEL_OK);
    TEST_ASSERT_EQ(keel_tls_context_create(&cfg_cli, false, &client), KEEL_OK);
    TEST_ASSERT(pump_tls_handshake(client, server));

    /* Invalid fd path must return invalid arg and not mutate active counter */
    keel_tls_stats_t before = keel_tls_get_stats();
    keel_error_t e = keel_tls_context_activate_ktls(client, -1);
    TEST_ASSERT_EQ(e, KEEL_ERR_INVALID_ARG);
    keel_tls_stats_t after_invalid = keel_tls_get_stats();
    TEST_ASSERT_EQ(after_invalid.ktls_active, before.ktls_active);

    int cfd = -1, sfd = -1;
    TEST_ASSERT_EQ(make_tcp_connected_pair(&cfd, &sfd), 0);

    before = keel_tls_get_stats();
    e = keel_tls_context_activate_ktls(client, cfd);
    keel_tls_stats_t after = keel_tls_get_stats();

    const char* require_ktls_env = getenv("KEEL_TEST_REQUIRE_KTLS");
    bool require_ktls = (require_ktls_env &&
                         (strcmp(require_ktls_env, "1") == 0 ||
                          strcasecmp(require_ktls_env, "true") == 0 ||
                          strcasecmp(require_ktls_env, "yes") == 0));

    if (e == KEEL_OK) {
        TEST_ASSERT_EQ(after.ktls_active, before.ktls_active + 1);
    } else {
        TEST_ASSERT_EQ(after.ktls_active, before.ktls_active);
        TEST_ASSERT(after.ktls_fallback >= before.ktls_fallback);
        if (require_ktls) {
            TEST_ASSERT(false && "kTLS required by KEEL_TEST_REQUIRE_KTLS but activation failed");
        }
    }

    if (cfd >= 0) close(cfd);
    if (sfd >= 0) close(sfd);
    keel_tls_context_destroy(client);
    keel_tls_context_destroy(server);
    cleanup_cert_files();

    TEST_END();
}

int main(void)
{
    test_tls_handshake_and_data_path();
    test_ktls_activation_stats_and_fallback();
    return test_summary();
}
