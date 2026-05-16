/**
 * @file mysql_backend_auth.h
 * @brief Pure MySQL/MariaDB backend-auth helpers.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * This API packages the wire-format and crypto details needed to authenticate to
 * MySQL-family backends without embedding transport concerns into the auth logic.
 * The same functions are reused by probes, async backend connection code, and tests.
 */

#ifndef KEEL_MYSQL_BACKEND_AUTH_H
#define KEEL_MYSQL_BACKEND_AUTH_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>  /* ssize_t */

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Constants
 * ============================================================================ */

#define MY_HDR_SIZE              4   /* 3-byte LE length + 1-byte seq_id */
#define MY_OK_MARKER             0x00
#define MY_ERR_MARKER            0xFF
#define MY_AUTH_SWITCH_MARKER    0xFE
#define MY_AUTH_MORE_DATA_MARKER 0x01

#define MY_CAP_LONG_PASSWORD     (1U <<  0)
#define MY_CAP_FOUND_ROWS        (1U <<  1)
#define MY_CAP_LONG_FLAG         (1U <<  2)
#define MY_CAP_CONNECT_WITH_DB   (1U <<  3)
#define MY_CAP_IGNORE_SPACE      (1U <<  8)
#define MY_CAP_PROTOCOL_41       (1U <<  9)
#define MY_CAP_TRANSACTIONS      (1U << 13)
#define MY_CAP_SECURE_CONNECTION (1U << 15)
#define MY_CAP_MULTI_STATEMENTS  (1U << 16)
#define MY_CAP_MULTI_RESULTS     (1U << 17)
#define MY_CAP_PS_MULTI_RESULTS  (1U << 18)
#define MY_CAP_PLUGIN_AUTH       (1U << 19)

/* ============================================================================
 * Types
 * ============================================================================ */

/**
 * @brief Parsed MySQL handshake greeting (server → client)
 */
typedef struct my_handshake_info {
    uint8_t     scramble[21];       /**< Auth scramble data (up to 20 bytes + NUL) */
    size_t      scramble_len;       /**< Scramble length (8 or 20) */
    uint32_t    server_caps;        /**< Server capability flags */
    char        plugin[64];         /**< Auth plugin name */
    uint8_t     seq_id;             /**< Last sequence ID */
} my_handshake_info_t;

/**
 * @brief Result of parsing an auth response packet
 */
typedef enum my_auth_result_type {
    MY_AUTH_OK = 0,                 /**< OK packet — authentication succeeded */
    MY_AUTH_ERR,                    /**< ERR packet — authentication failed */
    MY_AUTH_SWITCH,                 /**< AuthSwitchRequest */
    MY_AUTH_MORE_DATA,              /**< AuthMoreData (caching_sha2 continuation) */
    MY_AUTH_UNKNOWN,                /**< Unrecognized packet */
} my_auth_result_type_t;

/**
 * @brief Parsed auth response details
 */
typedef struct my_auth_result {
    my_auth_result_type_t   type;
    uint16_t                err_code;           /**< MySQL error code (if ERR) */
    char                    err_msg[256];       /**< Error message (if ERR) */

    /* AuthSwitchRequest fields */
    char                    switch_plugin[64];  /**< New plugin name */
    uint8_t                 switch_scramble[21];/**< New scramble */
    size_t                  switch_scramble_len;

    /* AuthMoreData fields */
    uint8_t                 more_data_status;   /**< 0x03 = fast auth ok, 0x04 = full auth */
} my_auth_result_t;

/* ============================================================================
 * Wire Helpers
 * ============================================================================ */

static inline uint16_t my_rdle16(const uint8_t* p) {
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}

static inline uint32_t my_rdle24(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16);
}

static inline void my_wrle24(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
}

/* ============================================================================
 * Pure Functions
 * ============================================================================ */

/**
 * @brief Compute mysql_native_password scramble.
 *
 * SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password)))
 *
 * @param password  Plaintext password
 * @param scramble  Server-provided scramble
 * @param scramble_len  Scramble length
 * @param[out] out Output buffer (20 bytes)
 */
void my_scramble_native(const char* password,
                        const uint8_t* scramble, size_t scramble_len,
                        uint8_t* out);

/**
 * @brief Compute caching_sha2_password scramble.
 *
 * SHA256(password) XOR SHA256(SHA256(SHA256(password)) + scramble)
 *
 * @param password  Plaintext password
 * @param scramble  Server-provided scramble
 * @param scramble_len  Scramble length
 * @param[out] out Output buffer (32 bytes)
 */
void my_scramble_caching_sha2(const char* password,
                              const uint8_t* scramble, size_t scramble_len,
                              uint8_t* out);

/**
 * @brief Parse MySQL Initial Handshake (protocol v10).
 *
 * Extracts scramble, server_caps, auth plugin name, seq_id.
 *
 * @param data  Raw packet data (including 4-byte MySQL header)
 * @param len   Total data length
 * @param[out] out Output handshake info
 * @return 0 on success, -1 on parse error
 */
int my_parse_greeting(const uint8_t* data, size_t len,
                      my_handshake_info_t* out);

/**
 * @brief Build MySQL Handshake Response packet.
 *
 * @param hs        Parsed handshake info (scramble, caps, plugin, seq)
 * @param user      Username
 * @param database  Database name (may be NULL)
 * @param password  Password (may be NULL for anonymous)
 * @param[out] out Output buffer
 * @param out_max   Output buffer size
 * @return Total packet length (including header), or -1 on error
 */
ssize_t my_build_handshake_response(const my_handshake_info_t* hs,
                                    const char* user,
                                    const char* database,
                                    const char* password,
                                    uint8_t* out, size_t out_max);

/**
 * @brief Parse an auth result packet (OK/ERR/Switch/MoreData).
 *
 * @param data  Raw packet data (including 4-byte MySQL header)
 * @param len   Total data length
 * @param[out] out Output parsed result
 * @return 0 on success, -1 on parse error
 */
int my_parse_auth_result(const uint8_t* data, size_t len,
                         my_auth_result_t* out);

/**
 * @brief Build an auth response packet after AuthSwitchRequest.
 *
 * @param plugin        Auth plugin name
 * @param scramble      New scramble from auth switch
 * @param scramble_len  Scramble length
 * @param password      Password
 * @param seq_id        Sequence ID to use (caller increments)
 * @param[out] out Output buffer
 * @param out_max       Output buffer size
 * @return Total packet length, or -1 on error
 */
ssize_t my_build_auth_switch_response(const char* plugin,
                                      const uint8_t* scramble,
                                      size_t scramble_len,
                                      const char* password,
                                      uint8_t seq_id,
                                      uint8_t* out, size_t out_max);

/**
 * @brief Build RSA public key request packet (sends 0x02).
 *
 * @param seq_id  Sequence ID
 * @param out     Output buffer
 * @param out_max Buffer size
 * @return Total packet length, or -1 on error
 */
ssize_t my_build_rsa_key_request(uint8_t seq_id, uint8_t* out, size_t out_max);

/**
 * @brief Encrypt password using RSA public key and build auth packet.
 *
 * XORs password+NUL with scramble, then RSA-OAEP encrypts with server's
 * public key.
 *
 * @param pem_key       PEM-encoded RSA public key
 * @param pem_key_len   PEM key length
 * @param password      Plaintext password
 * @param scramble      Server scramble
 * @param scramble_len  Scramble length
 * @param seq_id        Sequence ID
 * @param out           Output buffer
 * @param out_max       Buffer size
 * @return Total packet length, or -1 on error
 */
ssize_t my_build_rsa_encrypted_password(const char* pem_key, size_t pem_key_len,
                                        const char* password,
                                        const uint8_t* scramble,
                                        size_t scramble_len,
                                        uint8_t seq_id,
                                        uint8_t* out, size_t out_max);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_MYSQL_BACKEND_AUTH_H */
