/**
 * @file test_tls_auto.c
 * @brief Unit tests for built-in CA and certificate auto-generation.
 *
 * Exercises the tls_auto subsystem added in P2 #10:
 *   §1 — First-run generation: CA + server + client certs written.
 *   §2 — Idempotent re-run: existing CA reused, server/client regenerated.
 *   §3 — certs_exist() correctness: true after gen, false for empty dir.
 *   §4 — NULL / invalid config handling.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#include "test_utils.h"
#include "keel/protocol/tls_auto.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ftw.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

static char g_tmpdir[256];

/** nftw callback to remove files and directories */
static int rm_cb(const char* path, const struct stat* sb,
                 int typeflag, struct FTW* ftwbuf) {
    (void)sb; (void)typeflag; (void)ftwbuf;
    return remove(path);
}

static void cleanup_tmpdir(void) {
    if (g_tmpdir[0])
        nftw(g_tmpdir, rm_cb, 64, FTW_DEPTH | FTW_PHYS);
}

static void make_tmpdir(void) {
    snprintf(g_tmpdir, sizeof(g_tmpdir), "/tmp/keel_tls_auto_test_XXXXXX");
    if (!mkdtemp(g_tmpdir)) {
        perror("mkdtemp");
        exit(1);
    }
    atexit(cleanup_tmpdir);
}

static bool file_exists(const char* path) {
    struct stat st;
    return stat(path, &st) == 0 && S_ISREG(st.st_mode);
}

static bool file_mode_is(const char* path, mode_t expected) {
    struct stat st;
    if (stat(path, &st) != 0) return false;
    return (st.st_mode & 0777) == expected;
}

/* ============================================================================
 * Test: First-run generation creates all expected files
 * ============================================================================ */

static void test_generate_first_run(void) {
    TEST_BEGIN("generate_first_run");

    char outdir[512];
    snprintf(outdir, sizeof(outdir), "%s/first", g_tmpdir);

    keel_tls_auto_config_t cfg = {
        .output_dir    = outdir,
        .key_bits      = 2048,
        .validity_days = 30,
        .cn            = "Test CA",
        .server_cn     = "test-server",
        .server_san    = "localhost,127.0.0.1",
    };

    keel_tls_auto_result_t result = {0};
    int rc = keel_tls_auto_generate(&cfg, &result);
    TEST_ASSERT_EQ(rc, 0);

    /* All path buffers populated */
    TEST_ASSERT(strlen(result.ca_cert) > 0);
    TEST_ASSERT(strlen(result.ca_key) > 0);
    TEST_ASSERT(strlen(result.server_cert) > 0);
    TEST_ASSERT(strlen(result.server_key) > 0);
    TEST_ASSERT(strlen(result.client_cert) > 0);
    TEST_ASSERT(strlen(result.client_key) > 0);

    /* Files exist on disk */
    TEST_ASSERT(file_exists(result.ca_cert));
    TEST_ASSERT(file_exists(result.ca_key));
    TEST_ASSERT(file_exists(result.server_cert));
    TEST_ASSERT(file_exists(result.server_key));
    TEST_ASSERT(file_exists(result.client_cert));
    TEST_ASSERT(file_exists(result.client_key));

    /* Private keys have 0600 permissions */
    TEST_ASSERT(file_mode_is(result.ca_key, 0600));
    TEST_ASSERT(file_mode_is(result.server_key, 0600));
    TEST_ASSERT(file_mode_is(result.client_key, 0600));

    /* Certificates have 0644 permissions */
    TEST_ASSERT(file_mode_is(result.ca_cert, 0644));
    TEST_ASSERT(file_mode_is(result.server_cert, 0644));
    TEST_ASSERT(file_mode_is(result.client_cert, 0644));

    TEST_END();
}

/* ============================================================================
 * Test: Second run reuses existing CA (idempotent)
 * ============================================================================ */

static void test_generate_idempotent(void) {
    TEST_BEGIN("generate_idempotent");

    char outdir[512];
    snprintf(outdir, sizeof(outdir), "%s/idem", g_tmpdir);

    keel_tls_auto_config_t cfg = {
        .output_dir    = outdir,
        .key_bits      = 2048,
        .validity_days = 30,
        .cn            = "Idem CA",
        .server_cn     = "idem-server",
        .server_san    = "localhost",
    };

    /* First generation */
    keel_tls_auto_result_t r1 = {0};
    int rc1 = keel_tls_auto_generate(&cfg, &r1);
    TEST_ASSERT_EQ(rc1, 0);

    /* Record CA cert mtime */
    struct stat st1;
    TEST_ASSERT_EQ(stat(r1.ca_cert, &st1), 0);

    /* Tiny sleep so mtime would differ if the file were rewritten */
    usleep(50000);

    /* Second generation */
    keel_tls_auto_result_t r2 = {0};
    int rc2 = keel_tls_auto_generate(&cfg, &r2);
    TEST_ASSERT_EQ(rc2, 0);

    /* CA cert path is the same */
    TEST_ASSERT_STR_EQ(r1.ca_cert, r2.ca_cert);

    /* CA cert was NOT regenerated (mtime unchanged) */
    struct stat st2;
    TEST_ASSERT_EQ(stat(r2.ca_cert, &st2), 0);
    TEST_ASSERT_EQ(st1.st_mtim.tv_sec, st2.st_mtim.tv_sec);

    TEST_END();
}

/* ============================================================================
 * Test: certs_exist() returns correct values
 * ============================================================================ */

static void test_certs_exist(void) {
    TEST_BEGIN("certs_exist");

    char empty_dir[512];
    snprintf(empty_dir, sizeof(empty_dir), "%s/empty", g_tmpdir);
    mkdir(empty_dir, 0755);
    TEST_ASSERT(keel_tls_auto_certs_exist(empty_dir) == false);

    /* Generate into a fresh directory */
    char gendir[512];
    snprintf(gendir, sizeof(gendir), "%s/exist", g_tmpdir);

    keel_tls_auto_config_t cfg = {
        .output_dir    = gendir,
        .key_bits      = 2048,
        .validity_days = 30,
        .cn            = "Exist CA",
        .server_cn     = "exist-server",
        .server_san    = "localhost",
    };
    keel_tls_auto_result_t result = {0};
    int rc = keel_tls_auto_generate(&cfg, &result);
    TEST_ASSERT_EQ(rc, 0);

    TEST_ASSERT(keel_tls_auto_certs_exist(gendir) == true);

    TEST_END();
}

/* ============================================================================
 * Test: NULL config returns error
 * ============================================================================ */

static void test_null_config(void) {
    TEST_BEGIN("null_config");

    keel_tls_auto_result_t result = {0};

    /* NULL config pointer → error */
    int rc = keel_tls_auto_generate(NULL, &result);
    TEST_ASSERT_EQ(rc, -1);

    /* NULL result pointer → error */
    keel_tls_auto_config_t cfg = {
        .output_dir    = "/tmp/keel_test_null",
        .key_bits      = 2048,
        .validity_days = 30,
    };
    rc = keel_tls_auto_generate(&cfg, NULL);
    TEST_ASSERT_EQ(rc, -1);

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void) {
    printf("=== TLS Auto-Generate Tests ===\n");

    make_tmpdir();

    test_null_config();
    test_generate_first_run();
    test_generate_idempotent();
    test_certs_exist();

    return test_summary();
}
