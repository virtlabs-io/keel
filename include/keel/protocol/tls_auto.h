/**
 * @file tls_auto.h
 * @brief Built-in CA and self-signed certificate generation for zero-config TLS.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * When `tls_mode = auto` or the `tls_auto_generate` flag is set, KEEL can
 * generate a local CA and leaf certificates on demand so operators get encrypted
 * connections out of the box without an external PKI.
 *
 * Generated artifacts:
 *   - CA key + self-signed CA certificate  (ca.key, ca.crt)
 *   - Server key + CA-signed certificate   (server.key, server.crt)
 *   - Client key + CA-signed certificate   (client.key, client.crt)
 *   - CA bundle (ca-bundle.pem)
 *
 * All PEM files are written with 0600 permissions (keys) or 0644 (certs).
 */

#ifndef KEEL_TLS_AUTO_H
#define KEEL_TLS_AUTO_H

#include <stdint.h>
#include <stdbool.h>

/**
 * @brief Configuration for auto-generated certificates.
 */
typedef struct keel_tls_auto_config {
    const char* output_dir;        /**< Directory for PEM output (created if needed) */
    int         key_bits;          /**< RSA key size (default 2048) */
    int         validity_days;     /**< Certificate validity in days (default 365) */
    const char* cn;                /**< Common Name for the CA (default "KEEL Auto CA") */
    const char* server_cn;         /**< Server cert CN  (default "localhost") */
    const char* server_san;        /**< Server SAN entries, comma-separated
                                        (default "localhost,127.0.0.1") */
} keel_tls_auto_config_t;

/**
 * @brief Paths to generated certificate files, filled by keel_tls_auto_generate().
 */
typedef struct keel_tls_auto_result {
    char ca_cert[512];             /**< Path to CA certificate */
    char ca_key[512];              /**< Path to CA private key */
    char server_cert[512];         /**< Path to server certificate */
    char server_key[512];          /**< Path to server private key */
    char client_cert[512];         /**< Path to client certificate */
    char client_key[512];          /**< Path to client private key */
    char ca_bundle[512];           /**< Path to CA bundle */
} keel_tls_auto_result_t;

/**
 * @brief Generate a self-signed CA and leaf certificates.
 *
 * If the CA files already exist in @p cfg->output_dir they are reused and only
 * missing leaf certificates are regenerated.
 *
 * @param cfg   Generation parameters.
 * @param out   [out] Filled with paths to generated files on success.
 * @return `0` on success, `-1` on error.
 */
int keel_tls_auto_generate(const keel_tls_auto_config_t* cfg,
                           keel_tls_auto_result_t* out);

/**
 * @brief Check whether auto-generated certificates already exist.
 *
 * @param dir  Certificate directory to check.
 * @return `true` if ca.crt, server.crt, server.key all exist.
 */
bool keel_tls_auto_certs_exist(const char* dir);

#endif /* KEEL_TLS_AUTO_H */
