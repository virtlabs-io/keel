/**
 * @file pg_backend_auth.c
 * @brief Pure PostgreSQL backend-auth packet builders and SCRAM helpers.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * These routines intentionally avoid all I/O so they can be reused by any caller
 * that needs PostgreSQL backend-auth logic: asynchronous backend connection setup,
 * probes, and unit tests. Keeping the wire construction and SCRAM derivation pure
 * also makes the code much easier to reason about than if message building were
 * interleaved with transport state management.
 */

#include "keel/protocol/pg_backend_auth.h"
#include "keel/log/log.h"

#include <stdlib.h>
#include <string.h>

#include <openssl/sha.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

/* ============================================================================
 * Pure Functions — Base64
 * ============================================================================ */

/**
 * @brief Encode binary data to Base64.
 * @param in   Input buffer to encode.
 * @param len  Number of bytes in @p in.
 * @param out  Output buffer for the null-terminated Base64 string.
 * @param omax Capacity of @p out in bytes.
 * @return Number of Base64 characters written (excluding the null terminator),
 *         or 0 if @p omax is too small.
 */
size_t pg_b64_encode(const uint8_t* in, size_t len, char* out, size_t omax)
{
    static const char t[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t o = 0;
    for (size_t i = 0; i < len; i += 3) {
        if (o + 4 >= omax) return 0;
        uint32_t v = (uint32_t)in[i] << 16;
        if (i+1 < len) v |= (uint32_t)in[i+1] << 8;
        if (i+2 < len) v |= in[i+2];
        out[o++] = t[(v>>18)&63]; out[o++] = t[(v>>12)&63];
        out[o++] = (i+1 < len) ? t[(v>>6)&63] : '=';
        out[o++] = (i+2 < len) ? t[v&63]      : '=';
    }
    out[o] = '\0';
    return o;
}

/**
 * @brief Decode a Base64 string to binary data.
 * @param in   Base64 input string.
 * @param len  Length of @p in in characters.
 * @param out  Output buffer for decoded bytes.
 * @param omax Capacity of @p out in bytes.
 * @return Number of bytes written to @p out.
 */
size_t pg_b64_decode(const char* in, size_t len, uint8_t* out, size_t omax)
{
    static const int8_t d[] = {
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
        52,53,54,55,56,57,58,59,60,61,-1,-1,-1, 0,-1,-1,
        -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
        15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
        -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50,51
    };
    size_t o = 0;
    for (size_t i = 0; i + 3 < len; i += 4) {
        /* Cast to unsigned char so the < 128 guard is meaningful on platforms
         * where plain char is signed (where a byte >= 0x80 would appear < 128
         * as a signed value, yielding a negative d[] index). */
        int a = ((unsigned char)in[i  ] < 128u) ? d[(unsigned char)in[i  ]] : -1;
        int b = ((unsigned char)in[i+1] < 128u) ? d[(unsigned char)in[i+1]] : -1;
        int c = ((unsigned char)in[i+2] < 128u) ? d[(unsigned char)in[i+2]] : -1;
        int e = ((unsigned char)in[i+3] < 128u) ? d[(unsigned char)in[i+3]] : -1;
        if (a < 0 || b < 0) break;
        if (o < omax) out[o++] = (uint8_t)((a<<2)|(b>>4));
        if (c >= 0 && in[i+2] != '=' && o < omax) out[o++] = (uint8_t)((b<<4)|(c>>2));
        if (e >= 0 && in[i+3] != '=' && o < omax) out[o++] = (uint8_t)((c<<6)|e);
    }
    return o;
}

/* ============================================================================
 * Pure Functions — Message Building
 * ============================================================================ */

/**
 * @brief Build a PostgreSQL startup message (protocol 3.0).
 * @param user     PostgreSQL user name.
 * @param database Target database name.
 * @param out      Output buffer to write the message into.
 * @param out_max  Capacity of @p out in bytes.
 * @return Number of bytes written, or -1 on error.
 */
ssize_t pg_build_startup_message(const char* user, const char* database,
                                 uint8_t* out, size_t out_max)
{
    size_t pos = 4;                         /* skip length header */
    if (pos + 4 > out_max) return -1;
    out[pos++] = 0; out[pos++] = 3;        /* protocol 3.0 */
    out[pos++] = 0; out[pos++] = 0;

    memcpy(out + pos, "user", 5);           pos += 5;
    size_t ul = strlen(user) + 1;
    if (pos + ul > out_max) return -1;
    memcpy(out + pos, user, ul);            pos += ul;

    memcpy(out + pos, "database", 9);       pos += 9;
    size_t dl = strlen(database) + 1;
    if (pos + dl > out_max) return -1;
    memcpy(out + pos, database, dl);        pos += dl;

    if (pos + 1 > out_max) return -1;
    out[pos++] = 0;                         /* terminator */
    pg_wr32be(out, (uint32_t)pos);          /* write length */

    return (ssize_t)pos;
}

/**
 * @brief Build a SASL initial response packet ('p' message type).
 * @param mechanism SASL mechanism string (e.g., "SCRAM-SHA-256").
 * @param data      Initial client response data.
 * @param dlen      Length of @p data in bytes.
 * @param out       Output buffer for the wire packet.
 * @param omax      Capacity of @p out in bytes.
 * @return Number of bytes written, or -1 if the buffer is too small.
 */
ssize_t pg_sasl_initial_response(const char* mechanism,
                                 const char* data, size_t dlen,
                                 uint8_t* out, size_t omax)
{
    size_t mlen = strlen(mechanism);
    size_t total = 1 + 4 + mlen + 1 + 4 + dlen;
    if (total > omax) return -1;
    out[0] = 'p';
    pg_wr32be(out+1, (uint32_t)(total - 1));
    memcpy(out+5, mechanism, mlen+1);
    pg_wr32be(out+5+mlen+1, (uint32_t)dlen);
    memcpy(out+5+mlen+1+4, data, dlen);
    return (ssize_t)total;
}

/**
 * @brief Build a SASL continuation response packet ('p' message type).
 * @param data  Client-final message data.
 * @param dlen  Length of @p data in bytes.
 * @param out   Output buffer for the wire packet.
 * @param omax  Capacity of @p out in bytes.
 * @return Number of bytes written, or -1 if the buffer is too small.
 */
ssize_t pg_sasl_response(const uint8_t* data, size_t dlen,
                         uint8_t* out, size_t omax)
{
    size_t total = 1 + 4 + dlen;
    if (total > omax) return -1;
    out[0] = 'p';
    pg_wr32be(out+1, (uint32_t)(total - 1));
    memcpy(out+5, data, dlen);
    return (ssize_t)total;
}

/**
 * @brief Build a cleartext password message ('p' message type).
 * @param password  Null-terminated cleartext password.
 * @param out       Output buffer for the wire packet.
 * @param out_max   Capacity of @p out in bytes.
 * @return Number of bytes written, or -1 if the buffer is too small.
 */
ssize_t pg_build_password_message(const char* password,
                                  uint8_t* out, size_t out_max)
{
    size_t plen = strlen(password) + 1;
    size_t total = 5 + plen;
    if (total > out_max) return -1;
    out[0] = 'p';
    pg_wr32be(out + 1, (uint32_t)(4 + plen));
    memcpy(out + 5, password, plen);
    return (ssize_t)total;
}

/* ============================================================================
 * Pure Functions — SCRAM-SHA-256
 * ============================================================================ */

/**
 * @brief Build the SCRAM-SHA-256 client-first message wrapped in a SASL
 *        initial response packet.
 * @param user     PostgreSQL user name (used in the SCRAM bare message).
 * @param scram    SCRAM context; receives the generated client nonce and
 *                 client-first-bare string for use in subsequent steps.
 * @param out      Output buffer for the wire packet.
 * @param out_max  Capacity of @p out in bytes.
 * @return Number of bytes written, or -1 on error.
 */
ssize_t pg_scram_build_client_first(const char* user,
                                    pg_scram_ctx_t* scram,
                                    uint8_t* out, size_t out_max)
{
    char client_nonce_raw[18];
    RAND_bytes((uint8_t*)client_nonce_raw, sizeof(client_nonce_raw));
    pg_b64_encode((const uint8_t*)client_nonce_raw, sizeof(client_nonce_raw),
                  scram->client_nonce_b64, sizeof(scram->client_nonce_b64));

    snprintf(scram->client_first_bare, sizeof(scram->client_first_bare),
             "n=%s,r=%s", user, scram->client_nonce_b64);

    char client_first[256];
    int cf_len = snprintf(client_first, sizeof(client_first),
                          "n,,%s", scram->client_first_bare);

    return pg_sasl_initial_response("SCRAM-SHA-256",
        client_first, (size_t)cf_len, out, out_max);
}

/**
 * @brief Build the SCRAM-SHA-256 client-final message.
 *
 * Parses the server-first message, performs PBKDF2 key derivation, computes
 * the client proof, and writes a SASL response packet containing the
 * client-final-message.
 *
 * @param server_first      Server-first message bytes (not null-terminated).
 * @param server_first_len  Length of @p server_first in bytes.
 * @param password          Cleartext password used for key derivation.
 * @param scram             SCRAM context populated by pg_scram_build_client_first().
 * @param out               Output buffer for the wire packet.
 * @param out_max           Capacity of @p out in bytes.
 * @return Number of bytes written, or -1 on error.
 */
ssize_t pg_scram_build_client_final(const char* server_first,
                                    size_t server_first_len,
                                    const char* password,
                                    pg_scram_ctx_t* scram,
                                    uint8_t* out, size_t out_max)
{
    /* Store server_first for auth_msg */
    if (server_first_len >= sizeof(scram->server_first)) return -1;
    memcpy(scram->server_first, server_first, server_first_len);
    scram->server_first[server_first_len] = '\0';

    /* Parse: r=<nonce>, s=<salt>, i=<iterations> */
    char sf_copy[256];
    if (server_first_len >= sizeof(sf_copy)) return -1;
    memcpy(sf_copy, server_first, server_first_len);
    sf_copy[server_first_len] = '\0';

    char* sr = strstr(sf_copy, "r=");
    char* ss = strstr(sf_copy, "s=");
    char* si = strstr(sf_copy, "i=");
    if (!sr || !ss || !si) return -1;

    char* combined_nonce = sr + 2;
    char* cn_end = strchr(combined_nonce, ',');
    if (cn_end) *cn_end = '\0';

    char* salt_b64 = ss + 2;
    char* salt_end = strchr(salt_b64, ',');
    if (salt_end) *salt_end = '\0';

    int iterations = atoi(si + 2);
    if (iterations < 1) return -1;

    uint8_t salt[64];
    size_t salt_len = pg_b64_decode(salt_b64, strlen(salt_b64), salt, sizeof(salt));
    if (salt_len == 0) return -1;

    /* Restore commas for auth_msg */
    if (cn_end) *cn_end = ',';
    if (salt_end) *salt_end = ',';

    /* Key derivation */
    uint8_t salted_password[32];
    if (!PKCS5_PBKDF2_HMAC(password, (int)strlen(password),
                            salt, (int)salt_len, iterations,
                            EVP_sha256(), 32, salted_password))
        return -1;

    uint8_t client_key[32];
    unsigned int ck_len = 32;
    HMAC(EVP_sha256(), salted_password, 32,
         (const uint8_t*)"Client Key", 10, client_key, &ck_len);

    uint8_t stored_key[32];
    SHA256(client_key, 32, stored_key);

    char cf_no_proof[256];
    snprintf(cf_no_proof, sizeof(cf_no_proof), "c=biws,r=%s", combined_nonce);

    char auth_msg[512];
    int am_len = snprintf(auth_msg, sizeof(auth_msg), "%s,%s,%s",
                          scram->client_first_bare, sf_copy, cf_no_proof);

    uint8_t client_sig[32];
    unsigned int cs_len = 32;
    HMAC(EVP_sha256(), stored_key, 32,
         (const uint8_t*)auth_msg, (size_t)am_len, client_sig, &cs_len);

    uint8_t client_proof[32];
    for (int i = 0; i < 32; i++)
        client_proof[i] = client_key[i] ^ client_sig[i];

    char proof_b64[64];
    pg_b64_encode(client_proof, 32, proof_b64, sizeof(proof_b64));

    /* Build client-final */
    char client_final[512];
    int cfin_len = snprintf(client_final, sizeof(client_final),
                            "%s,p=%s", cf_no_proof, proof_b64);

    return pg_sasl_response((const uint8_t*)client_final,
                            (size_t)cfin_len, out, out_max);
}
