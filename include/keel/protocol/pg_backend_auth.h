/**
 * @file pg_backend_auth.h
 * @brief Pure PostgreSQL backend-auth wire helpers.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * These helpers isolate PostgreSQL authentication packet construction and SCRAM
 * state handling from any concrete I/O strategy. That lets synchronous probes,
 * asynchronous backend-connect state machines, and tests share one source of truth
 * for message building and parsing-sensitive helper logic.
 */

#ifndef KEEL_PG_BACKEND_AUTH_H
#define KEEL_PG_BACKEND_AUTH_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/types.h>  /* ssize_t */

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Wire Helpers
 * ============================================================================ */

static inline uint32_t pg_be32(const uint8_t* p) {
    return ((uint32_t)p[0]<<24)|((uint32_t)p[1]<<16)|
           ((uint32_t)p[2]<<8) | p[3];
}

static inline void pg_wr32be(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v>>24); p[1] = (uint8_t)(v>>16);
    p[2] = (uint8_t)(v>>8);  p[3] = (uint8_t)v;
}

/* ============================================================================
 * Base64
 * ============================================================================ */

/**
 * @brief Base64 encode.
 * @param in Input bytes.
 * @param len Input length.
 * @param[out] out Destination character buffer.
 * @param omax Output buffer capacity.
 * @return Number of characters written (excluding NUL), or 0 on error.
 */
size_t pg_b64_encode(const uint8_t* in, size_t len, char* out, size_t omax);

/**
 * @brief Base64 decode.
 * @param in Input Base64 string.
 * @param len Input string length.
 * @param[out] out Destination byte buffer.
 * @param omax Output buffer capacity.
 * @return Number of bytes decoded, or 0 on error.
 */
size_t pg_b64_decode(const char* in, size_t len, uint8_t* out, size_t omax);

/* ============================================================================
 * StartupMessage
 * ============================================================================ */

/**
 * @brief Build a PostgreSQL StartupMessage (protocol 3.0).
 *
 * @param user      Username
 * @param database  Database name
 * @param[out] out Output buffer
 * @param out_max   Buffer size
 * @return Bytes written, or -1 on error
 */
ssize_t pg_build_startup_message(const char* user, const char* database,
                                 uint8_t* out, size_t out_max);

/* ============================================================================
 * SASL Message Builders
 * ============================================================================ */

/**
 * @brief Build a SASLInitialResponse message ('p' tag).
 *
 * @param mechanism  SASL mechanism name (e.g. "SCRAM-SHA-256")
 * @param data       Client-first message data
 * @param dlen       Data length
 * @param[out] out Output buffer
 * @param omax       Buffer size
 * @return Bytes written, or -1 on error
 */
ssize_t pg_sasl_initial_response(const char* mechanism,
                                 const char* data, size_t dlen,
                                 uint8_t* out, size_t omax);

/**
 * @brief Build a SASLResponse message ('p' tag).
 *
 * @param data  Client-final message data
 * @param dlen  Data length
 * @param[out] out Output buffer
 * @param omax  Buffer size
 * @return Bytes written, or -1 on error
 */
ssize_t pg_sasl_response(const uint8_t* data, size_t dlen,
                         uint8_t* out, size_t omax);

/* ============================================================================
 * SCRAM-SHA-256
 * ============================================================================ */

/**
 * @brief SCRAM context for multi-step auth.
 *
 * Stores intermediate state across the SCRAM handshake steps.
 */
typedef struct pg_scram_ctx {
    char    client_nonce_b64[32];       /**< Base64-encoded client nonce */
    char    client_first_bare[128];     /**< "n=user,r=nonce" */
    char    server_first[256];          /**< Full server-first-message */
} pg_scram_ctx_t;

/**
 * @brief Build SCRAM client-first message and SASLInitialResponse packet.
 *
 * Generates a random client nonce, builds the client-first-message,
 * wraps it in a SASLInitialResponse packet.
 *
 * @param user      Username
 * @param scram     SCRAM context to store intermediate state
 * @param out       Output buffer for the wire packet
 * @param out_max   Buffer size
 * @return Bytes written, or -1 on error
 */
ssize_t pg_scram_build_client_first(const char* user,
                                    pg_scram_ctx_t* scram,
                                    uint8_t* out, size_t out_max);

/**
 * @brief Process server-first-message and build client-final-message packet.
 *
 * Parses the server-first (nonce, salt, iterations), derives keys,
 * computes SCRAM proof, builds the SASLResponse packet.
 *
 * @param server_first      Server-first-message data
 * @param server_first_len  Length
 * @param password          Plaintext password
 * @param scram             SCRAM context with client-first state
 * @param out               Output buffer for the wire packet
 * @param out_max           Buffer size
 * @return Bytes written, or -1 on error
 */
ssize_t pg_scram_build_client_final(const char* server_first,
                                    size_t server_first_len,
                                    const char* password,
                                    pg_scram_ctx_t* scram,
                                    uint8_t* out, size_t out_max);

/**
 * @brief Build a cleartext PasswordMessage ('p' tag).
 *
 * @param password  Plaintext password
 * @param out       Output buffer
 * @param out_max   Buffer size
 * @return Bytes written, or -1 on error
 */
ssize_t pg_build_password_message(const char* password,
                                  uint8_t* out, size_t out_max);

#ifdef __cplusplus
}
#endif

#endif /* KEEL_PG_BACKEND_AUTH_H */
