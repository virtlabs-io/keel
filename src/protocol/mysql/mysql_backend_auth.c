/**
 * @file mysql_backend_auth.c
 * @brief Pure MySQL/MariaDB backend-auth packet builders and crypto helpers.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The MySQL-family auth path includes several moving pieces: initial greeting
 * parsing, capability negotiation, plugin-specific scramble generation, and in
 * some cases RSA-based password exchange. This file keeps that logic transport-
 * independent so the asynchronous connector and any test harnesses can share one
 * implementation without duplicating packet rules.
 */

#include "keel/protocol/mysql_backend_auth.h"
#include "keel/log/log.h"

#include <stdlib.h>
#include <string.h>

#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/bio.h>

/* ============================================================================
 * Pure Functions — Scramble Computation
 * ============================================================================ */

/**
 * @brief Compute a mysql_native_password scramble (SHA1-based).
 *
 * Implements: XOR(SHA1(password), SHA1(SHA1(SHA1(password)) ++ scramble))
 *
 * @param password     Null-terminated plaintext password.
 * @param scramble     Server-provided scramble bytes.
 * @param scramble_len Length of @p scramble in bytes.
 * @param out          Output buffer; must be at least SHA_DIGEST_LENGTH (20) bytes.
 */
void my_scramble_native(const char* password,
                        const uint8_t* scramble, size_t scramble_len,
                        uint8_t* out)
{
    uint8_t stage1[SHA_DIGEST_LENGTH];
    SHA1((const uint8_t*)password, strlen(password), stage1);

    uint8_t stage2[SHA_DIGEST_LENGTH];
    SHA1(stage1, SHA_DIGEST_LENGTH, stage2);

    EVP_MD_CTX* md = EVP_MD_CTX_new();
    uint8_t digest[SHA_DIGEST_LENGTH];
    unsigned int dlen = SHA_DIGEST_LENGTH;
    EVP_DigestInit_ex(md, EVP_sha1(), NULL);
    EVP_DigestUpdate(md, scramble, scramble_len);
    EVP_DigestUpdate(md, stage2, SHA_DIGEST_LENGTH);
    EVP_DigestFinal_ex(md, digest, &dlen);
    EVP_MD_CTX_free(md);

    for (int i = 0; i < SHA_DIGEST_LENGTH; i++)
        out[i] = stage1[i] ^ digest[i];
}

/**
 * @brief Compute a caching_sha2_password scramble (SHA-256-based).
 *
 * Implements: XOR(SHA256(password), SHA256(SHA256(SHA256(password)) ++ scramble))
 *
 * @param password     Null-terminated plaintext password.
 * @param scramble     Server-provided scramble bytes.
 * @param scramble_len Length of @p scramble in bytes.
 * @param out          Output buffer; must be at least SHA256_DIGEST_LENGTH (32) bytes.
 */
void my_scramble_caching_sha2(const char* password,
                              const uint8_t* scramble, size_t scramble_len,
                              uint8_t* out)
{
    uint8_t stage1[SHA256_DIGEST_LENGTH];
    SHA256((const uint8_t*)password, strlen(password), stage1);

    uint8_t stage2[SHA256_DIGEST_LENGTH];
    SHA256(stage1, SHA256_DIGEST_LENGTH, stage2);

    EVP_MD_CTX* md = EVP_MD_CTX_new();
    uint8_t digest[SHA256_DIGEST_LENGTH];
    unsigned int dlen = SHA256_DIGEST_LENGTH;
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);
    EVP_DigestUpdate(md, stage2, SHA256_DIGEST_LENGTH);
    EVP_DigestUpdate(md, scramble, scramble_len);
    EVP_DigestFinal_ex(md, digest, &dlen);
    EVP_MD_CTX_free(md);

    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
        out[i] = stage1[i] ^ digest[i];
}

/* ============================================================================
 * Pure Functions — Packet Parsing
 * ============================================================================ */

/**
 * @brief Parse a MySQL initial handshake packet (Protocol::Handshake v10).
 * @param data  Raw packet bytes, including the 4-byte packet header.
 * @param len   Total length of @p data in bytes.
 * @param out   Structure populated with server capabilities, scramble bytes,
 *              and authentication plugin name on success.
 * @return 0 on success, -1 if the packet is malformed or unsupported.
 */
int my_parse_greeting(const uint8_t* data, size_t len,
                      my_handshake_info_t* out)
{
    memset(out, 0, sizeof(*out));

    if (len < MY_HDR_SIZE + 1) return -1;

    uint32_t payload_len = my_rdle24(data);
    out->seq_id = data[3];
    const uint8_t* payload = data + MY_HDR_SIZE;
    size_t plen = payload_len;

    if (plen < 1 || payload[0] != 10) {
        if (payload[0] == MY_ERR_MARKER && plen >= 3) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_AUTH,
                "mysql_auth: server sent error %u during handshake",
                my_rdle16(payload + 1));
        }
        return -1;
    }

    size_t pos = 1;
    size_t vlen = strnlen((const char*)(payload + pos), plen - pos);
    pos += vlen + 1;

    if (pos + 4 > plen) return -1;
    pos += 4; /* connection ID */

    if (pos + 8 > plen) return -1;
    memcpy(out->scramble, payload + pos, 8);
    pos += 8;

    if (pos + 1 > plen) return -1;
    pos += 1; /* filler */

    if (pos + 2 > plen) return -1;
    out->server_caps = my_rdle16(payload + pos);
    pos += 2;

    if (pos >= plen) {
        out->scramble_len = 8;
        strcpy(out->plugin, "mysql_native_password");
        return 0;
    }

    pos += 1; /* character set */
    if (pos + 2 > plen) return -1;
    pos += 2; /* status flags */
    if (pos + 2 > plen) return -1;
    out->server_caps |= ((uint32_t)my_rdle16(payload + pos)) << 16;
    pos += 2;

    uint8_t auth_data_len = 0;
    if (pos < plen) auth_data_len = payload[pos];
    pos += 1;

    if (pos + 10 > plen) return -1;
    pos += 10; /* reserved */

    if (out->server_caps & MY_CAP_SECURE_CONNECTION) {
        size_t part2_len = (auth_data_len > 8) ? (size_t)(auth_data_len - 8) : 13;
        if (pos + part2_len > plen) part2_len = plen - pos;
        size_t copy_len = (part2_len > 12) ? 12 : part2_len;
        memcpy(out->scramble + 8, payload + pos, copy_len);
        out->scramble_len = 8 + copy_len;
        pos += part2_len;
    } else {
        out->scramble_len = 8;
    }

    out->plugin[0] = '\0';
    if (out->server_caps & MY_CAP_PLUGIN_AUTH) {
        if (pos < plen) {
            size_t nlen = strnlen((const char*)(payload + pos), plen - pos);
            if (nlen < sizeof(out->plugin)) {
                memcpy(out->plugin, payload + pos, nlen);
                out->plugin[nlen] = '\0';
            }
        }
    }
    if (out->plugin[0] == '\0')
        strcpy(out->plugin, "mysql_native_password");

    return 0;
}

/**
 * @brief Parse a MySQL authentication result packet (OK / ERR / AuthSwitch /
 *        AuthMoreData).
 * @param data  Raw packet bytes, including the 4-byte packet header.
 * @param len   Total length of @p data in bytes.
 * @param out   Structure populated with the result type and any associated
 *              error code, error message, or auth-switch details.
 * @return 0 on success, -1 if the packet is too short to be valid.
 */
int my_parse_auth_result(const uint8_t* data, size_t len,
                         my_auth_result_t* out)
{
    memset(out, 0, sizeof(*out));

    if (len < MY_HDR_SIZE + 1) return -1;

    uint32_t pkt_len = my_rdle24(data);
    const uint8_t* payload = data + MY_HDR_SIZE;
    size_t plen = pkt_len;

    if (plen == 0) return -1;

    uint8_t marker = payload[0];

    if (marker == MY_OK_MARKER) {
        out->type = MY_AUTH_OK;
        return 0;
    }

    if (marker == MY_ERR_MARKER) {
        out->type = MY_AUTH_ERR;
        if (plen >= 3) {
            out->err_code = my_rdle16(payload + 1);
            const char* errmsg = "";
            size_t errmsg_len = 0;
            if (plen > 9) {
                errmsg = (const char*)(payload + 9);
                errmsg_len = plen - 9;
            } else if (plen > 3) {
                errmsg = (const char*)(payload + 3);
                errmsg_len = plen - 3;
            }
            size_t cl = errmsg_len < sizeof(out->err_msg) - 1
                        ? errmsg_len : sizeof(out->err_msg) - 1;
            memcpy(out->err_msg, errmsg, cl);
            out->err_msg[cl] = '\0';
        }
        return 0;
    }

    if (marker == MY_AUTH_SWITCH_MARKER && plen > 1) {
        out->type = MY_AUTH_SWITCH;
        size_t pos = 1;
        const char* new_plugin = (const char*)(payload + pos);
        size_t nlen = strnlen(new_plugin, plen - pos);
        pos += nlen + 1;

        if (nlen < sizeof(out->switch_plugin)) {
            memcpy(out->switch_plugin, new_plugin, nlen);
            out->switch_plugin[nlen] = '\0';
        }

        const uint8_t* new_scramble = payload + pos;
        size_t new_scramble_len = plen - pos;
        if (new_scramble_len > 0 && new_scramble[new_scramble_len - 1] == 0)
            new_scramble_len--;
        if (new_scramble_len > sizeof(out->switch_scramble))
            new_scramble_len = sizeof(out->switch_scramble);
        memcpy(out->switch_scramble, new_scramble, new_scramble_len);
        out->switch_scramble_len = new_scramble_len;
        return 0;
    }

    if (marker == MY_AUTH_MORE_DATA_MARKER && plen >= 2) {
        out->type = MY_AUTH_MORE_DATA;
        out->more_data_status = payload[1];
        return 0;
    }

    out->type = MY_AUTH_UNKNOWN;
    return 0;
}

/* ============================================================================
 * Pure Functions — Packet Building
 * ============================================================================ */

/**
 * @brief Build a MySQL HandshakeResponse41 packet.
 *
 * Selects the appropriate scramble algorithm based on @p hs->plugin and
 * negotiates capabilities against the server's advertised set.
 *
 * @param hs       Handshake info returned by my_parse_greeting().
 * @param user     Null-terminated MySQL user name.
 * @param database Null-terminated database name, or NULL/empty for none.
 * @param password Null-terminated plaintext password, or NULL/empty for none.
 * @param out      Output buffer for the wire packet.
 * @param out_max  Capacity of @p out in bytes.
 * @return Number of bytes written, or -1 on error.
 */
ssize_t my_build_handshake_response(const my_handshake_info_t* hs,
                                    const char* user,
                                    const char* database,
                                    const char* password,
                                    uint8_t* out, size_t out_max)
{
    uint8_t payload[1024];
    size_t pos = 0;

    uint8_t auth_data[32];
    size_t auth_data_len = 0;

    if (password && password[0]) {
        if (strcmp(hs->plugin, "caching_sha2_password") == 0) {
            my_scramble_caching_sha2(password, hs->scramble,
                                     hs->scramble_len, auth_data);
            auth_data_len = 32;
        } else {
            my_scramble_native(password, hs->scramble,
                               hs->scramble_len, auth_data);
            auth_data_len = 20;
        }
    }

    uint32_t caps = MY_CAP_LONG_PASSWORD | MY_CAP_FOUND_ROWS |
                    MY_CAP_LONG_FLAG | MY_CAP_IGNORE_SPACE |
                    MY_CAP_PROTOCOL_41 | MY_CAP_TRANSACTIONS |
                    MY_CAP_SECURE_CONNECTION | MY_CAP_MULTI_STATEMENTS |
                    MY_CAP_MULTI_RESULTS | MY_CAP_PS_MULTI_RESULTS |
                    MY_CAP_PLUGIN_AUTH;
    if (database && database[0])
        caps |= MY_CAP_CONNECT_WITH_DB;
    caps &= hs->server_caps;
    caps |= MY_CAP_PROTOCOL_41 | MY_CAP_SECURE_CONNECTION;

    payload[pos++] = (uint8_t)(caps);
    payload[pos++] = (uint8_t)(caps >> 8);
    payload[pos++] = (uint8_t)(caps >> 16);
    payload[pos++] = (uint8_t)(caps >> 24);

    payload[pos++] = 0xFF; payload[pos++] = 0xFF;
    payload[pos++] = 0xFF; payload[pos++] = 0x00;

    payload[pos++] = 0x2d; /* utf8mb4 */

    memset(payload + pos, 0, 23); pos += 23;

    size_t ulen = strlen(user);
    if (pos + ulen + 1 > sizeof(payload)) return -1;
    memcpy(payload + pos, user, ulen); pos += ulen;
    payload[pos++] = 0;

    payload[pos++] = (uint8_t)auth_data_len;
    if (auth_data_len > 0) {
        memcpy(payload + pos, auth_data, auth_data_len);
        pos += auth_data_len;
    }

    if (caps & MY_CAP_CONNECT_WITH_DB) {
        size_t dlen = strlen(database);
        if (pos + dlen + 1 > sizeof(payload)) return -1;
        memcpy(payload + pos, database, dlen); pos += dlen;
        payload[pos++] = 0;
    }

    if (caps & MY_CAP_PLUGIN_AUTH) {
        size_t pnlen = strlen(hs->plugin);
        if (pos + pnlen + 1 > sizeof(payload)) return -1;
        memcpy(payload + pos, hs->plugin, pnlen); pos += pnlen;
        payload[pos++] = 0;
    }

    size_t total = MY_HDR_SIZE + pos;
    if (total > out_max) return -1;

    my_wrle24(out, (uint32_t)pos);
    out[3] = hs->seq_id + 1;
    memcpy(out + MY_HDR_SIZE, payload, pos);

    return (ssize_t)total;
}

/**
 * @brief Build a MySQL AuthSwitchResponse packet for the given plugin.
 *
 * Supports "caching_sha2_password" and "mysql_native_password".
 * Returns -1 for any other plugin name.
 *
 * @param plugin       Null-terminated authentication plugin name.
 * @param scramble     Server-provided scramble bytes from the switch request.
 * @param scramble_len Length of @p scramble in bytes.
 * @param password     Null-terminated plaintext password.
 * @param seq_id       Packet sequence ID to use in the header.
 * @param out          Output buffer for the wire packet.
 * @param out_max      Capacity of @p out in bytes.
 * @return Number of bytes written, or -1 on error.
 */
ssize_t my_build_auth_switch_response(const char* plugin,
                                      const uint8_t* scramble,
                                      size_t scramble_len,
                                      const char* password,
                                      uint8_t seq_id,
                                      uint8_t* out, size_t out_max)
{
    uint8_t auth_data[32];
    size_t auth_data_len = 0;

    if (strcmp(plugin, "caching_sha2_password") == 0) {
        my_scramble_caching_sha2(password, scramble, scramble_len, auth_data);
        auth_data_len = 32;
    } else if (strcmp(plugin, "mysql_native_password") == 0) {
        my_scramble_native(password, scramble, scramble_len, auth_data);
        auth_data_len = 20;
    } else {
        return -1;
    }

    size_t total = MY_HDR_SIZE + auth_data_len;
    if (total > out_max) return -1;

    my_wrle24(out, (uint32_t)auth_data_len);
    out[3] = seq_id;
    memcpy(out + MY_HDR_SIZE, auth_data, auth_data_len);

    return (ssize_t)total;
}

/**
 * @brief Build a MySQL RSA public-key request packet (payload byte 0x02).
 *
 * Used during caching_sha2_password full-auth when TLS is not available.
 *
 * @param seq_id   Packet sequence ID to use in the header.
 * @param out      Output buffer for the wire packet.
 * @param out_max  Capacity of @p out in bytes.
 * @return Number of bytes written, or -1 if the buffer is too small.
 */
ssize_t my_build_rsa_key_request(uint8_t seq_id, uint8_t* out, size_t out_max)
{
    size_t total = MY_HDR_SIZE + 1;
    if (total > out_max) return -1;

    my_wrle24(out, 1);
    out[3] = seq_id;
    out[MY_HDR_SIZE] = 0x02;

    return (ssize_t)total;
}

/**
 * @brief Build a MySQL RSA-encrypted password packet.
 *
 * XOR-obfuscates the password with the server scramble, then encrypts the
 * result with the server's RSA public key using OAEP padding.
 *
 * @param pem_key       Server RSA public key in PEM format.
 * @param pem_key_len   Length of @p pem_key in bytes.
 * @param password      Null-terminated plaintext password.
 * @param scramble      Server-provided scramble bytes.
 * @param scramble_len  Length of @p scramble in bytes.
 * @param seq_id        Packet sequence ID to use in the header.
 * @param out           Output buffer for the wire packet.
 * @param out_max       Capacity of @p out in bytes.
 * @return Number of bytes written, or -1 on error.
 */
ssize_t my_build_rsa_encrypted_password(const char* pem_key, size_t pem_key_len,
                                        const char* password,
                                        const uint8_t* scramble,
                                        size_t scramble_len,
                                        uint8_t seq_id,
                                        uint8_t* out, size_t out_max)
{
    BIO* bio = BIO_new_mem_buf(pem_key, (int)pem_key_len);
    if (!bio) return -1;

    EVP_PKEY* pkey = PEM_read_bio_PUBKEY(bio, NULL, NULL, NULL);
    BIO_free(bio);
    if (!pkey) return -1;

    size_t pwlen = strlen(password) + 1;
    uint8_t xor_buf[256];
    if (pwlen > sizeof(xor_buf)) {
        EVP_PKEY_free(pkey);
        return -1;
    }
    memcpy(xor_buf, password, pwlen);
    for (size_t i = 0; i < pwlen; i++)
        xor_buf[i] ^= scramble[i % scramble_len];

    EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new(pkey, NULL);
    EVP_PKEY_free(pkey);
    if (!pctx) return -1;

    if (EVP_PKEY_encrypt_init(pctx) <= 0 ||
        EVP_PKEY_CTX_set_rsa_padding(pctx, RSA_PKCS1_OAEP_PADDING) <= 0) {
        EVP_PKEY_CTX_free(pctx);
        return -1;
    }

    size_t enc_len = 0;
    if (EVP_PKEY_encrypt(pctx, NULL, &enc_len, xor_buf, pwlen) <= 0) {
        EVP_PKEY_CTX_free(pctx);
        return -1;
    }

    if (MY_HDR_SIZE + enc_len > out_max) {
        EVP_PKEY_CTX_free(pctx);
        return -1;
    }

    if (EVP_PKEY_encrypt(pctx, out + MY_HDR_SIZE, &enc_len, xor_buf, pwlen) <= 0) {
        EVP_PKEY_CTX_free(pctx);
        return -1;
    }
    EVP_PKEY_CTX_free(pctx);

    my_wrle24(out, (uint32_t)enc_len);
    out[3] = seq_id;

    return (ssize_t)(MY_HDR_SIZE + enc_len);
}


