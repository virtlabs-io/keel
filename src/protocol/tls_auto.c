/**
 * @file tls_auto.c
 * @brief Built-in CA and certificate auto-generation using OpenSSL 3.x EVP API.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This module produces a minimal PKI suitable for encrypted dev / test / small
 * production deployments where an external CA workflow would be overkill.
 *
 * Key security properties:
 *   - RSA keys default to 2048 bits; configurable.
 *   - Leaf certs signed by the generated CA; not self-signed.
 *   - Private keys written with mode 0600.
 *   - Existing CA is reused so key rollover remains explicit.
 *   - No shell-outs; pure OpenSSL in-process generation.
 */

#include "keel/protocol/tls_auto.h"
#include "keel/log/log.h"

#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/pem.h>
#include <openssl/bn.h>
#include <openssl/err.h>

#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>

/* ============================================================================
 * Helpers
 * ============================================================================ */

/**
 * @brief Generate an RSA EVP_PKEY.
 */
static EVP_PKEY* gen_rsa_key(int bits) {
    EVP_PKEY_CTX* kctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
    if (!kctx) return NULL;

    EVP_PKEY* pkey = NULL;
    if (EVP_PKEY_keygen_init(kctx) <= 0 ||
        EVP_PKEY_CTX_set_rsa_keygen_bits(kctx, bits) <= 0 ||
        EVP_PKEY_keygen(kctx, &pkey) <= 0) {
        EVP_PKEY_CTX_free(kctx);
        return NULL;
    }
    EVP_PKEY_CTX_free(kctx);
    return pkey;
}

/**
 * @brief Set a random 128-bit serial number on an X509 certificate.
 */
static int set_random_serial(X509* cert) {
    BIGNUM* bn = BN_new();
    if (!bn) return -1;
    if (!BN_rand(bn, 128, BN_RAND_TOP_ANY, BN_RAND_BOTTOM_ANY)) {
        BN_free(bn);
        return -1;
    }
    ASN1_INTEGER* serial = X509_get_serialNumber(cert);
    BN_to_ASN1_INTEGER(bn, serial);
    BN_free(bn);
    return 0;
}

/**
 * @brief Add a subject alternative name (SAN) extension to a certificate.
 *
 * @param cert   Certificate to modify.
 * @param issuer Issuing certificate (for context copy).
 * @param san    Comma-separated SAN entries, e.g. "DNS:localhost,IP:127.0.0.1".
 */
static int add_san_ext(X509* cert, X509* issuer, const char* san) {
    X509V3_CTX x509v3_ctx;
    X509V3_set_ctx_nodb(&x509v3_ctx);
    X509V3_set_ctx(&x509v3_ctx, issuer, cert, NULL, NULL, 0);

    X509_EXTENSION* ext = X509V3_EXT_conf_nid(
        NULL, &x509v3_ctx, NID_subject_alt_name, san);
    if (!ext) return -1;

    X509_add_ext(cert, ext, -1);
    X509_EXTENSION_free(ext);
    return 0;
}

/**
 * @brief Add Basic Constraints extension.
 */
static int add_basic_constraints(X509* cert, X509* issuer, const char* value) {
    X509V3_CTX x509v3_ctx;
    X509V3_set_ctx_nodb(&x509v3_ctx);
    X509V3_set_ctx(&x509v3_ctx, issuer, cert, NULL, NULL, 0);

    X509_EXTENSION* ext = X509V3_EXT_conf_nid(
        NULL, &x509v3_ctx, NID_basic_constraints, value);
    if (!ext) return -1;

    X509_add_ext(cert, ext, -1);
    X509_EXTENSION_free(ext);
    return 0;
}

/**
 * @brief Add Key Usage extension.
 */
static int add_key_usage(X509* cert, X509* issuer, const char* value) {
    X509V3_CTX x509v3_ctx;
    X509V3_set_ctx_nodb(&x509v3_ctx);
    X509V3_set_ctx(&x509v3_ctx, issuer, cert, NULL, NULL, 0);

    X509_EXTENSION* ext = X509V3_EXT_conf_nid(
        NULL, &x509v3_ctx, NID_key_usage, value);
    if (!ext) return -1;

    X509_add_ext(cert, ext, -1);
    X509_EXTENSION_free(ext);
    return 0;
}

/**
 * @brief Write a PEM-encoded private key to a file with mode 0600.
 */
static int write_key_pem(const char* path, EVP_PKEY* pkey) {
    FILE* f = fopen(path, "w");
    if (!f) return -1;
    /* Set restrictive permissions before writing key material */
    if (fchmod(fileno(f), 0600) < 0) {
        fclose(f);
        return -1;
    }
    int rc = PEM_write_PrivateKey(f, pkey, NULL, NULL, 0, NULL, NULL);
    fclose(f);
    return rc <= 0 ? -1 : 0;
}

/**
 * @brief Write a PEM-encoded certificate to a file with mode 0644.
 */
static int write_cert_pem(const char* path, X509* cert) {
    FILE* f = fopen(path, "w");
    if (!f) return -1;
    fchmod(fileno(f), 0644);
    int rc = PEM_write_X509(f, cert);
    fclose(f);
    return rc <= 0 ? -1 : 0;
}

/**
 * @brief Load an existing PEM private key from disk.
 */
static EVP_PKEY* load_key_pem(const char* path) {
    FILE* f = fopen(path, "r");
    if (!f) return NULL;
    EVP_PKEY* pkey = PEM_read_PrivateKey(f, NULL, NULL, NULL);
    fclose(f);
    return pkey;
}

/**
 * @brief Load an existing PEM certificate from disk.
 */
static X509* load_cert_pem(const char* path) {
    FILE* f = fopen(path, "r");
    if (!f) return NULL;
    X509* cert = PEM_read_X509(f, NULL, NULL, NULL);
    fclose(f);
    return cert;
}

/**
 * @brief Ensure a directory exists (create with 0755 if needed).
 */
static int ensure_dir(const char* dir) {
    struct stat st;
    if (stat(dir, &st) == 0) {
        return S_ISDIR(st.st_mode) ? 0 : -1;
    }
    return mkdir(dir, 0755);
}

/* ============================================================================
 * CA Generation
 * ============================================================================ */

/**
 * @brief Create a self-signed CA certificate.
 */
static int generate_ca(EVP_PKEY** out_key, X509** out_cert,
                       int key_bits, int validity_days, const char* cn) {
    EVP_PKEY* ca_key = gen_rsa_key(key_bits);
    if (!ca_key) return -1;

    X509* ca_cert = X509_new();
    if (!ca_cert) { EVP_PKEY_free(ca_key); return -1; }

    X509_set_version(ca_cert, 2);  /* X509 v3 */
    set_random_serial(ca_cert);

    X509_gmtime_adj(X509_getm_notBefore(ca_cert), 0);
    X509_gmtime_adj(X509_getm_notAfter(ca_cert), (long)validity_days * 86400);

    X509_NAME* name = X509_get_subject_name(ca_cert);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               (const unsigned char*)cn, -1, -1, 0);
    X509_NAME_add_entry_by_txt(name, "O", MBSTRING_ASC,
                               (const unsigned char*)"KEEL Auto", -1, -1, 0);
    X509_set_issuer_name(ca_cert, name);

    X509_set_pubkey(ca_cert, ca_key);

    /* CA extensions */
    add_basic_constraints(ca_cert, ca_cert, "critical,CA:TRUE");
    add_key_usage(ca_cert, ca_cert,
                  "critical,keyCertSign,cRLSign");

    if (X509_sign(ca_cert, ca_key, EVP_sha256()) <= 0) {
        X509_free(ca_cert);
        EVP_PKEY_free(ca_key);
        return -1;
    }

    *out_key = ca_key;
    *out_cert = ca_cert;
    return 0;
}

/* ============================================================================
 * Leaf Certificate Generation
 * ============================================================================ */

/**
 * @brief Create a leaf certificate signed by the CA.
 *
 * @param ca_key    CA private key for signing.
 * @param ca_cert   CA certificate (issuer).
 * @param out_key   [out] Generated leaf key.
 * @param out_cert  [out] Generated leaf certificate.
 * @param key_bits  RSA key size.
 * @param days      Validity period in days.
 * @param cn        Common Name for the leaf.
 * @param san       SAN string (e.g. "DNS:localhost,IP:127.0.0.1") or NULL.
 * @param is_server true for server cert (digitalSignature, keyEncipherment),
 *                  false for client cert (digitalSignature, keyAgreement).
 * @return `0` on success.
 */
static int generate_leaf(EVP_PKEY* ca_key, X509* ca_cert,
                         EVP_PKEY** out_key, X509** out_cert,
                         int key_bits, int days, const char* cn,
                         const char* san, bool is_server) {
    EVP_PKEY* leaf_key = gen_rsa_key(key_bits);
    if (!leaf_key) return -1;

    X509* leaf_cert = X509_new();
    if (!leaf_cert) { EVP_PKEY_free(leaf_key); return -1; }

    X509_set_version(leaf_cert, 2);
    set_random_serial(leaf_cert);

    X509_gmtime_adj(X509_getm_notBefore(leaf_cert), 0);
    X509_gmtime_adj(X509_getm_notAfter(leaf_cert), (long)days * 86400);

    /* Subject */
    X509_NAME* subj = X509_get_subject_name(leaf_cert);
    X509_NAME_add_entry_by_txt(subj, "CN", MBSTRING_ASC,
                               (const unsigned char*)cn, -1, -1, 0);
    X509_NAME_add_entry_by_txt(subj, "O", MBSTRING_ASC,
                               (const unsigned char*)"KEEL Auto", -1, -1, 0);

    /* Issuer = CA subject */
    X509_set_issuer_name(leaf_cert, X509_get_subject_name(ca_cert));
    X509_set_pubkey(leaf_cert, leaf_key);

    /* Extensions */
    add_basic_constraints(leaf_cert, ca_cert, "critical,CA:FALSE");
    if (is_server) {
        add_key_usage(leaf_cert, ca_cert,
                      "critical,digitalSignature,keyEncipherment");
    } else {
        add_key_usage(leaf_cert, ca_cert,
                      "critical,digitalSignature,keyAgreement");
    }

    /* SAN */
    if (san && san[0]) {
        add_san_ext(leaf_cert, ca_cert, san);
    }

    if (X509_sign(leaf_cert, ca_key, EVP_sha256()) <= 0) {
        X509_free(leaf_cert);
        EVP_PKEY_free(leaf_key);
        return -1;
    }

    *out_key = leaf_key;
    *out_cert = leaf_cert;
    return 0;
}

/* ============================================================================
 * Build SAN string from config
 * ============================================================================ */

/**
 * @brief Convert comma-separated hostnames/IPs to X509v3 SAN format.
 *
 * Input:  "localhost,127.0.0.1,::1,myhost.local"
 * Output: "DNS:localhost,IP:127.0.0.1,IP:::1,DNS:myhost.local"
 */
static int build_san_string(const char* entries, char* out, size_t out_len) {
    if (!entries || !entries[0]) {
        snprintf(out, out_len, "DNS:localhost,IP:127.0.0.1");
        return 0;
    }

    size_t pos = 0;
    const char* p = entries;

    while (*p && pos < out_len - 1) {
        /* Skip whitespace */
        while (*p == ' ' || *p == '\t') p++;
        if (!*p) break;

        /* Find end of this entry */
        const char* end = p;
        while (*end && *end != ',') end++;
        size_t elen = (size_t)(end - p);

        /* Trim trailing whitespace */
        while (elen > 0 && (p[elen-1] == ' ' || p[elen-1] == '\t'))
            elen--;

        if (elen == 0) { if (*end == ',') { p = end + 1; continue; } else break; }

        /* Separator */
        if (pos > 0) {
            if (pos + 1 >= out_len) break;
            out[pos++] = ',';
        }

        /* Determine DNS vs IP: if it starts with a digit or ':' → IP */
        bool is_ip = (p[0] >= '0' && p[0] <= '9') || p[0] == ':';
        const char* prefix = is_ip ? "IP:" : "DNS:";
        size_t plen = strlen(prefix);

        if (pos + plen + elen >= out_len) break;
        memcpy(out + pos, prefix, plen); pos += plen;
        memcpy(out + pos, p, elen);       pos += elen;

        p = (*end == ',') ? end + 1 : end;
    }

    out[pos] = '\0';
    return 0;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Check whether all three required certificate files exist in @p dir.
 *
 * Verifies the presence of `ca.crt`, `server.crt`, and `server.key` using
 * `stat()`.  Does not validate file contents or permissions.
 *
 * @param dir  Directory path to check (NULL returns false).
 * @return true if all three files are accessible; false otherwise.
 */
bool keel_tls_auto_certs_exist(const char* dir) {
    if (!dir) return false;

    char path[512];
    struct stat st;

    snprintf(path, sizeof(path), "%s/ca.crt", dir);
    if (stat(path, &st) != 0) return false;

    snprintf(path, sizeof(path), "%s/server.crt", dir);
    if (stat(path, &st) != 0) return false;

    snprintf(path, sizeof(path), "%s/server.key", dir);
    if (stat(path, &st) != 0) return false;

    return true;
}

/**
 * @brief Generate a self-signed CA and server/client certificate set.
 *
 * Creates the output directory if needed, then:
 *  - Generates a CA key and self-signed CA certificate.
 *  - Generates a server key and certificate signed by the CA (with SAN).
 *  - Generates a client key and certificate signed by the CA.
 *  - Writes a PEM-bundle file (`ca-bundle.pem`) for client trust stores.
 *
 * If the CA files already exist, they are reloaded rather than regenerated,
 * so existing client trust stores remain valid after a server cert renewal.
 *
 * Defaults (if not specified in @p cfg):
 *  - output_dir: "/tmp/keel-certs"
 *  - key_bits: 2048
 *  - validity_days: 365
 *  - cn: "KEEL Auto CA"
 *  - server_cn: "localhost"
 *  - server_san: "localhost,127.0.0.1"
 *
 * @param cfg  Generation parameters (NULL fields use defaults above).
 * @param out  Output paths and result flags.
 * @return 0 on success, -1 on any OpenSSL or file-system error.
 */
int keel_tls_auto_generate(const keel_tls_auto_config_t* cfg,
                           keel_tls_auto_result_t* out) {
    if (!cfg || !out) return -1;

    const char* dir   = cfg->output_dir  ? cfg->output_dir  : "/tmp/keel-certs";
    int key_bits      = cfg->key_bits     > 0 ? cfg->key_bits     : 2048;
    int validity_days = cfg->validity_days > 0 ? cfg->validity_days : 365;
    const char* cn    = cfg->cn          ? cfg->cn           : "KEEL Auto CA";
    const char* scn   = cfg->server_cn   ? cfg->server_cn    : "localhost";
    const char* san   = cfg->server_san  ? cfg->server_san   : "localhost,127.0.0.1";

    memset(out, 0, sizeof(*out));

    if (ensure_dir(dir) < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                       "tls_auto: cannot create directory %s: %s",
                       dir, strerror(errno));
        return -1;
    }

    /* Build output paths */
    snprintf(out->ca_cert,     sizeof(out->ca_cert),     "%s/ca.crt",      dir);
    snprintf(out->ca_key,      sizeof(out->ca_key),      "%s/ca.key",      dir);
    snprintf(out->server_cert, sizeof(out->server_cert), "%s/server.crt",  dir);
    snprintf(out->server_key,  sizeof(out->server_key),  "%s/server.key",  dir);
    snprintf(out->client_cert, sizeof(out->client_cert), "%s/client.crt",  dir);
    snprintf(out->client_key,  sizeof(out->client_key),  "%s/client.key",  dir);
    snprintf(out->ca_bundle,   sizeof(out->ca_bundle),   "%s/ca-bundle.pem", dir);

    /* ---- CA: load existing or generate ---- */
    EVP_PKEY* ca_key  = NULL;
    X509*     ca_cert = NULL;

    if (access(out->ca_cert, R_OK) == 0 && access(out->ca_key, R_OK) == 0) {
        ca_key  = load_key_pem(out->ca_key);
        ca_cert = load_cert_pem(out->ca_cert);
        if (ca_key && ca_cert) {
            KEEL_LOG_INFO(KEEL_LOG_CAT_TLS,
                          "tls_auto: reusing existing CA from %s", dir);
        } else {
            EVP_PKEY_free(ca_key);
            X509_free(ca_cert);
            ca_key = NULL;
            ca_cert = NULL;
        }
    }

    if (!ca_key || !ca_cert) {
        if (generate_ca(&ca_key, &ca_cert, key_bits, validity_days * 10, cn) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                           "tls_auto: CA generation failed");
            return -1;
        }
        if (write_key_pem(out->ca_key, ca_key) < 0 ||
            write_cert_pem(out->ca_cert, ca_cert) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                           "tls_auto: failed to write CA files to %s", dir);
            EVP_PKEY_free(ca_key);
            X509_free(ca_cert);
            return -1;
        }
        /* Write ca-bundle.pem (identical to ca.crt for a single-level CA) */
        write_cert_pem(out->ca_bundle, ca_cert);

        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS,
                      "tls_auto: generated new CA in %s", dir);
    }

    /* ---- Server certificate ---- */
    if (access(out->server_cert, R_OK) != 0 || access(out->server_key, R_OK) != 0) {
        char san_str[1024];
        build_san_string(san, san_str, sizeof(san_str));

        EVP_PKEY* srv_key  = NULL;
        X509*     srv_cert = NULL;
        if (generate_leaf(ca_key, ca_cert, &srv_key, &srv_cert,
                          key_bits, validity_days, scn, san_str, true) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                           "tls_auto: server cert generation failed");
            EVP_PKEY_free(ca_key); X509_free(ca_cert);
            return -1;
        }
        if (write_key_pem(out->server_key, srv_key) < 0 ||
            write_cert_pem(out->server_cert, srv_cert) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                           "tls_auto: failed to write server cert to %s", dir);
            EVP_PKEY_free(srv_key); X509_free(srv_cert);
            EVP_PKEY_free(ca_key); X509_free(ca_cert);
            return -1;
        }
        EVP_PKEY_free(srv_key);
        X509_free(srv_cert);
        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS,
                      "tls_auto: generated server cert (CN=%s, SAN=%s)", scn, san_str);
    }

    /* ---- Client certificate ---- */
    if (access(out->client_cert, R_OK) != 0 || access(out->client_key, R_OK) != 0) {
        EVP_PKEY* cli_key  = NULL;
        X509*     cli_cert = NULL;
        if (generate_leaf(ca_key, ca_cert, &cli_key, &cli_cert,
                          key_bits, validity_days, "KEEL Client", NULL, false) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                           "tls_auto: client cert generation failed");
            EVP_PKEY_free(ca_key); X509_free(ca_cert);
            return -1;
        }
        if (write_key_pem(out->client_key, cli_key) < 0 ||
            write_cert_pem(out->client_cert, cli_cert) < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_TLS,
                           "tls_auto: failed to write client cert to %s", dir);
            EVP_PKEY_free(cli_key); X509_free(cli_cert);
            EVP_PKEY_free(ca_key); X509_free(ca_cert);
            return -1;
        }
        EVP_PKEY_free(cli_key);
        X509_free(cli_cert);
        KEEL_LOG_INFO(KEEL_LOG_CAT_TLS,
                      "tls_auto: generated client cert");
    }

    EVP_PKEY_free(ca_key);
    X509_free(ca_cert);
    return 0;
}
