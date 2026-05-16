/**
 * @file probe_postgres.c
 * @brief PostgreSQL wire-protocol health and role probe.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * The PostgreSQL probe speaks the backend wire protocol directly instead of shelling
 * out to client tools or depending on libpq. That keeps the dependency surface small,
 * makes timeout handling explicit, and lets the probe participate cleanly in KEEL's
 * own logging and error model. The implementation pays the cost of opening a fresh
 * connection per check, but that tradeoff is acceptable at probe cadence and avoids
 * persistent probe sessions interfering with backend connection accounting.
 */

#include "keel/probe/probe.h"
#include "keel/probe/probe_common.h"
#include "keel/engine/engine.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel/protocol/pg_backend_auth.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include "keel/util/platform_compat.h"
#include <openssl/evp.h>

/* ============================================================================
 * Probe Context
 * ============================================================================ */

/**
 * Per-server probe context.  Created once per server in create(),
 * reused across all check() calls for that server.
 */
typedef struct pg_probe_ctx {
    uint32_t    timeout_ms;     /**< Connect/read timeout from probe config */
} pg_probe_ctx_t;

typedef enum pg_probe_auth_mode {
    PG_PROBE_AUTH_AUTO = 0,
    PG_PROBE_AUTH_TRUST,
    PG_PROBE_AUTH_PASSWORD,
    PG_PROBE_AUTH_MD5,
    PG_PROBE_AUTH_SCRAM,
} pg_probe_auth_mode_t;

/* Forward declarations for low-level helpers used by auth_until_ready(). */
static int read_full(int fd, void* buf, size_t len, uint32_t timeout_ms);
static int write_full(int fd, const void* buf, size_t len);
static uint32_t get_be32(const uint8_t* buf);

static pg_probe_auth_mode_t parse_probe_auth_mode(const char* s)
{
    if (!s || !*s) return PG_PROBE_AUTH_AUTO;
    if (strcasecmp(s, "trust") == 0) return PG_PROBE_AUTH_TRUST;
    if (strcasecmp(s, "password") == 0) return PG_PROBE_AUTH_PASSWORD;
    if (strcasecmp(s, "md5") == 0) return PG_PROBE_AUTH_MD5;
    if (strcasecmp(s, "scram") == 0 ||
        strcasecmp(s, "scram-sha-256") == 0)
        return PG_PROBE_AUTH_SCRAM;
    return PG_PROBE_AUTH_AUTO;
}

static const char* probe_auth_mode_name(pg_probe_auth_mode_t m)
{
    switch (m) {
    case PG_PROBE_AUTH_TRUST: return "trust";
    case PG_PROBE_AUTH_PASSWORD: return "password";
    case PG_PROBE_AUTH_MD5: return "md5";
    case PG_PROBE_AUTH_SCRAM: return "scram";
    default: return "auto";
    }
}

/**
 * @brief Check whether a PG probe auth mode is compatible with a PG auth type.
 *
 * PG auth type integers from the AuthenticationRequest message:
 *  0  = AuthenticationOk (trust)  3  = ClearTextPassword
 *  5  = MD5Password               10/11/12 = SCRAM-SHA-256 variants
 *
 * @param m         Configured probe auth mode.
 * @param auth_type Integer auth type from the server's AuthenticationRequest.
 * @return true if the mode can satisfy the server's request.
 */
static bool auth_mode_allows(pg_probe_auth_mode_t m, uint32_t auth_type)
{
    if (m == PG_PROBE_AUTH_AUTO || m == PG_PROBE_AUTH_PASSWORD) {
        return auth_type == 0 || auth_type == 3 || auth_type == 5 ||
               auth_type == 10 || auth_type == 11 || auth_type == 12;
    }
    if (m == PG_PROBE_AUTH_TRUST) return auth_type == 0;
    if (m == PG_PROBE_AUTH_MD5) return auth_type == 5 || auth_type == 0;
    if (m == PG_PROBE_AUTH_SCRAM)
        return auth_type == 10 || auth_type == 11 || auth_type == 12 || auth_type == 0;
    return false;
}

/**
 * @brief Compute an MD5 digest and encode it as a lowercase hex string.
 *
 * Uses OpenSSL EVP_DigestInit/Update/Final to compute the digest of
 * @p in_len bytes at @p in, then writes the 32-character hex representation
 * plus a NUL terminator into @p out_hex[33].
 *
 * @param in      Input bytes.
 * @param in_len  Byte count.
 * @param[out] out_hex  Output buffer of exactly 33 bytes (32 hex + NUL).
 * @return 0 on success, -1 if the EVP context allocation or digest fails.
 */
static int md5_hex(const uint8_t* in, size_t in_len, char out_hex[33])
{
    uint8_t digest[16];
    unsigned int dlen = 0;
    EVP_MD_CTX* md = EVP_MD_CTX_new();
    if (!md) return -1;

    int ok = EVP_DigestInit_ex(md, EVP_md5(), NULL) == 1 &&
             EVP_DigestUpdate(md, in, in_len) == 1 &&
             EVP_DigestFinal_ex(md, digest, &dlen) == 1;
    EVP_MD_CTX_free(md);
    if (!ok || dlen != 16) return -1;

    for (size_t i = 0; i < 16; i++) {
        static const char* h = "0123456789abcdef";
        out_hex[i * 2]     = h[digest[i] >> 4];
        out_hex[i * 2 + 1] = h[digest[i] & 0x0F];
    }
    out_hex[32] = '\0';
    return 0;
}

/**
 * @brief Build a PostgreSQL MD5 password authentication response.
 *
 * Implements the PG MD5 scheme: MD5(MD5(password || user) || salt), prefixed
 * with "md5".  Calls pg_build_password_message() to wrap the result in a
 * PasswordMessage ('p') wire frame.
 *
 * @param user      PG username (used in inner hash).
 * @param password  Cleartext password.
 * @param salt      4-byte random salt from AuthenticationMD5Password.
 * @param out       Output buffer.
 * @param out_max   Buffer capacity in bytes.
 * @return Byte count written, or -1 on error (buffer too small or hash failure).
 */
static ssize_t pg_build_md5_password_message(const char* user,
                                             const char* password,
                                             const uint8_t salt[4],
                                             uint8_t* out,
                                             size_t out_max)
{
    char inner_in[512];
    int inner_len = snprintf(inner_in, sizeof(inner_in), "%s%s", password, user);
    if (inner_len < 0 || (size_t)inner_len >= sizeof(inner_in)) return -1;

    char inner_hex[33];
    if (md5_hex((const uint8_t*)inner_in, (size_t)inner_len, inner_hex) != 0)
        return -1;

    uint8_t outer_in[36]; /* 32 hex chars + 4-byte salt */
    memcpy(outer_in, inner_hex, 32);
    memcpy(outer_in + 32, salt, 4);

    char outer_hex[33];
    if (md5_hex(outer_in, sizeof(outer_in), outer_hex) != 0)
        return -1;

    char final_md5[36]; /* "md5" + 32 hex + '\0' */
    memcpy(final_md5, "md5", 3);
    memcpy(final_md5 + 3, outer_hex, 33);

    return pg_build_password_message(final_md5, out, out_max);
}

/**
 * @brief Check whether an AuthenticationSASL message advertises a SASL mechanism.
 *
 * Scans the NUL-separated mechanism list in the body of an Authentication
 * SASL message (auth_type 10) looking for an exact match with @p mechanism.
 *
 * @param body       AuthenticationRequest body (after the int32 auth type).
 * @param body_len   Length of body.
 * @param mechanism  Mechanism name to search for (e.g. "SCRAM-SHA-256").
 * @return true if the mechanism is present.
 */
static bool sasl_mechanism_supported(const uint8_t* body, uint32_t body_len,
                                     const char* mechanism)
{
    if (body_len <= 4) return false;
    const uint8_t* p = body + 4;
    const uint8_t* end = body + body_len;
    while (p < end && *p != '\0') {
        const char* m = (const char*)p;
        size_t mlen = strnlen(m, (size_t)(end - p));
        if (mlen == strlen(mechanism) && strncmp(m, mechanism, mlen) == 0)
            return true;
        p += mlen + 1;
    }
    return false;
}

/**
 * @brief Extract the human-readable message field from a PG ErrorResponse body.
 *
 * Walks the 'E' message field list looking for field type 'M' (MESSAGE) and
 * writes it into @p errbuf.  Falls back to "PG error response" if not found.
 *
 * @param body     ErrorResponse body (after the type byte and length).
 * @param body_len Length of @p body.
 * @param errbuf   Output error string buffer.
 * @param errlen   Capacity of @p errbuf.
 */
static void parse_pg_error_message(const uint8_t* body, uint32_t body_len,
                                   char* errbuf, size_t errlen)
{
    const uint8_t* p = body;
    const uint8_t* end = body + body_len;
    while (p < end && *p != '\0') {
        char field_type = (char)*p++;
        const char* field_val = (const char*)p;
        size_t flen = strnlen(field_val, (size_t)(end - p));
        p += flen + 1;
        if (field_type == 'M') {
            snprintf(errbuf, errlen, "PG error: %s", field_val);
            return;
        }
    }
    snprintf(errbuf, errlen, "PG error response");
}

/**
 * @brief Drive the PostgreSQL authentication exchange until ReadyForQuery.
 *
 * Handles AuthenticationOk (trust), MD5, ClearText, and SCRAM-SHA-256
 * authentication.  Loops reading auth messages until the server sends
 * ReadyForQuery ('Z') or an error occurs.
 *
 * @param fd          Connected backend socket (blocking).
 * @param timeout_ms  Per-read timeout in milliseconds.
 * @param user        Database username.
 * @param password    Cleartext password (may be NULL for trust).
 * @param auth_mode   Configured auth mode policy.
 * @param errbuf      Output error buffer.
 * @param errlen      Capacity of @p errbuf.
 * @return 0 on success (server sent ReadyForQuery), -1 on failure.
 */
static int auth_until_ready(int fd,
                            uint32_t timeout_ms,
                            const char* user,
                            const char* password,
                            pg_probe_auth_mode_t auth_mode,
                            char* errbuf,
                            size_t errlen)
{
    uint8_t hdr[5];
    uint8_t body[65536];
    uint8_t out[2048];
    pg_scram_ctx_t scram;
    bool scram_started = false;

    for (;;) {
        if (read_full(fd, hdr, 5, timeout_ms) < 0) {
            snprintf(errbuf, errlen, "read auth header failed");
            return -1;
        }

        char type = (char)hdr[0];
        uint32_t msglen = get_be32(hdr + 1);
        if (msglen < 4 || msglen > sizeof(body) + 4) {
            snprintf(errbuf, errlen, "bad auth message length: %u", msglen);
            return -1;
        }

        uint32_t body_len = msglen - 4;
        if (body_len > 0 && read_full(fd, body, body_len, timeout_ms) < 0) {
            snprintf(errbuf, errlen, "read auth body failed");
            return -1;
        }

        if (type == 'E') {
            parse_pg_error_message(body, body_len, errbuf, errlen);
            return -1;
        }

        if (type == 'Z') {
            return 0;
        }

        if (type != 'R') {
            continue; /* S/K/N etc */
        }

        if (body_len < 4) {
            snprintf(errbuf, errlen, "short auth message");
            return -1;
        }

        uint32_t auth_type = get_be32(body);
        if (!auth_mode_allows(auth_mode, auth_type)) {
            snprintf(errbuf, errlen,
                     "probe_auth=%s incompatible with server auth type %u",
                     probe_auth_mode_name(auth_mode), auth_type);
            return -1;
        }

        switch (auth_type) {
        case 0:
            /* AuthenticationOk */
            break;
        case 3: {
            if (!password || !*password) {
                snprintf(errbuf, errlen, "cleartext auth requested but probe password is empty");
                return -1;
            }
            ssize_t n = pg_build_password_message(password, out, sizeof(out));
            if (n <= 0 || write_full(fd, out, (size_t)n) < 0) {
                snprintf(errbuf, errlen, "send cleartext password failed");
                return -1;
            }
            break;
        }
        case 5: {
            if (!password || !*password) {
                snprintf(errbuf, errlen, "MD5 auth requested but probe password is empty");
                return -1;
            }
            if (!user || !*user) {
                snprintf(errbuf, errlen, "MD5 auth requested but probe user is empty");
                return -1;
            }
            if (body_len < 8) {
                snprintf(errbuf, errlen, "short MD5 auth challenge");
                return -1;
            }
            const uint8_t* salt = body + 4;
            ssize_t n = pg_build_md5_password_message(user, password, salt, out, sizeof(out));
            if (n <= 0 || write_full(fd, out, (size_t)n) < 0) {
                snprintf(errbuf, errlen, "send MD5 password failed");
                return -1;
            }
            break;
        }
        case 10: {
            if (!password || !*password) {
                snprintf(errbuf, errlen, "SCRAM auth requested but probe password is empty");
                return -1;
            }
            if (!sasl_mechanism_supported(body, body_len, "SCRAM-SHA-256")) {
                snprintf(errbuf, errlen, "server does not offer SCRAM-SHA-256");
                return -1;
            }
            ssize_t n = pg_scram_build_client_first(user, &scram, out, sizeof(out));
            if (n <= 0 || write_full(fd, out, (size_t)n) < 0) {
                snprintf(errbuf, errlen, "send SCRAM client-first failed");
                return -1;
            }
            scram_started = true;
            break;
        }
        case 11: {
            if (!scram_started || !password || !*password) {
                snprintf(errbuf, errlen, "unexpected SCRAM continue");
                return -1;
            }
            const char* server_first = (const char*)(body + 4);
            size_t server_first_len = body_len - 4;
            ssize_t n = pg_scram_build_client_final(server_first, server_first_len,
                                                    password, &scram, out, sizeof(out));
            if (n <= 0 || write_full(fd, out, (size_t)n) < 0) {
                snprintf(errbuf, errlen, "send SCRAM client-final failed");
                return -1;
            }
            break;
        }
        case 12:
            /* AuthenticationSASLFinal: wait for AuthOk and ReadyForQuery */
            break;
        default:
            snprintf(errbuf, errlen, "unsupported auth type %u", auth_type);
            return -1;
        }
    }
}

/* ============================================================================
 * Low-level helpers
 * ============================================================================ */

/** @brief Monotonic microsecond timestamp for latency measurement. */
static uint64_t now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* tcp_connect() removed: use keel_probe_tcp_connect() from keel/probe/probe_common.h */

/**
 * Read exactly `len` bytes with poll timeout.
 */
static int read_full(int fd, void* buf, size_t len, uint32_t timeout_ms)
{
    size_t got = 0;
    while (got < len) {
        /* Use epoll to wait for data — avoids poll() overhead */
        int pr = keel_fd_wait(fd, KEEL_FD_WAIT_READ, (int)timeout_ms);
        if (pr <= 0) return -1;

        ssize_t n = read(fd, (char*)buf + got, len - got);
        if (n <= 0) return -1;
        got += (size_t)n;
    }
    return 0;
}

/**
 * Write all bytes.
 */
static int write_full(int fd, const void* buf, size_t len)
{
    const char* p = (const char*)buf;
    size_t left = len;
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n <= 0) return -1;
        p += n;
        left -= (size_t)n;
    }
    return 0;
}

/* ============================================================================
 * PG Wire Protocol Helpers (minimal)
 * ============================================================================ */

/** @brief Write @p val as a big-endian 32-bit integer into @p buf (4 bytes). */
static void put_be32(uint8_t* buf, uint32_t val)
{
    buf[0] = (uint8_t)(val >> 24);
    buf[1] = (uint8_t)(val >> 16);
    buf[2] = (uint8_t)(val >>  8);
    buf[3] = (uint8_t)(val      );
}

/** @brief Read a big-endian 32-bit integer from @p buf (4 bytes). @return Decoded uint32_t value. */
static uint32_t get_be32(const uint8_t* buf)
{
    return ((uint32_t)buf[0] << 24) |
           ((uint32_t)buf[1] << 16) |
           ((uint32_t)buf[2] <<  8) |
           ((uint32_t)buf[3]      );
}

/**
 * Build a StartupMessage.
 * Format: int32 length, int32 protocol(3.0), "user\0" val "\0" "database\0" val "\0" "\0"
 */
static size_t build_startup(uint8_t* buf, size_t cap,
                            const char* user, const char* database)
{
    size_t user_len = strlen(user);
    size_t db_len   = database ? strlen(database) : 0;

    /* Calculate size: 4(len) + 4(proto) + "user\0" + user + "\0" + optional "database\0" + db + "\0" + "\0" */
    size_t payload = 4 + 4 + 5 + user_len + 1;
    if (database && db_len > 0)
        payload += 9 + db_len + 1;       /* "database\0" + val + "\0" */
    payload += 1;  /* trailing NUL */
    size_t total = payload;

    if (total > cap) return 0;

    uint8_t* p = buf;
    put_be32(p, (uint32_t)total); p += 4;
    put_be32(p, 0x00030000);       p += 4;  /* protocol 3.0 */

    memcpy(p, "user", 5);  p += 5;          /* includes NUL */
    memcpy(p, user, user_len + 1); p += user_len + 1;

    if (database && db_len > 0) {
        memcpy(p, "database", 9); p += 9;    /* includes NUL */
        memcpy(p, database, db_len + 1); p += db_len + 1;
    }

    *p++ = '\0';  /* parameter list terminator */
    (void)p;
    return total;
}

/**
 * Build a simple Query message.
 * Format: 'Q' int32(len) query_string '\0'
 */
static size_t build_query(uint8_t* buf, size_t cap, const char* sql)
{
    size_t sql_len = strlen(sql);
    size_t total = 1 + 4 + sql_len + 1; /* type + len + sql + NUL */
    if (total > cap) return 0;

    buf[0] = 'Q';
    put_be32(buf + 1, (uint32_t)(4 + sql_len + 1));
    memcpy(buf + 5, sql, sql_len + 1);
    return total;
}

/**
 * SQL query for role detection.
 * Returns 't' on the primary (NOT in recovery) and 'f' on replicas.
 * Requires a user with CONNECT privilege — no superuser needed.
 */
static const char ROLE_SQL[] = "SELECT NOT pg_is_in_recovery() AS is_primary";

/* ============================================================================
 * Probe check implementation
 * ============================================================================ */

/**
 * Read PG messages until ReadyForQuery ('Z') is received.
 * Along the way, extract the first DataRow ('D') column[0] value
 * into `value_out` (for role detection).
 *
 * Handles: 'R' (Auth), 'K' (BackendKeyData), 'S' (ParameterStatus),
 *          'Z' (ReadyForQuery), 'T' (RowDescription), 'D' (DataRow),
 *          'C' (CommandComplete), 'E' (ErrorResponse)
 *
 * Returns 0 on success (Z received), -1 on error.
 */
static int read_until_ready(int fd, uint32_t timeout_ms,
                            char* value_out, size_t value_cap,
                            char* errbuf, size_t errlen,
                            bool* got_error)
{
    uint8_t hdr[5];
    *got_error = false;

    for (;;) {
        if (read_full(fd, hdr, 5, timeout_ms) < 0) {
            snprintf(errbuf, errlen, "read header failed");
            return -1;
        }

        char type = (char)hdr[0];
        uint32_t msglen = get_be32(hdr + 1);  /* includes self (4 bytes) */

        if (msglen < 4 || msglen > 65536) {
            snprintf(errbuf, errlen, "bad message length: %u", msglen);
            return -1;
        }

        uint32_t body_len = msglen - 4;
        uint8_t body[65536];
        if (body_len > 0) {
            if (read_full(fd, body, body_len, timeout_ms) < 0) {
                snprintf(errbuf, errlen, "read body failed");
                return -1;
            }
        }

        switch (type) {
        case 'R': {
            /* Authentication message */
            if (body_len >= 4) {
                uint32_t auth_type = get_be32(body);
                if (auth_type == 0) {
                    /* AuthenticationOk */
                } else if (auth_type == 3) {
                    /* AuthenticationCleartextPassword — not supported for probe */
                    snprintf(errbuf, errlen, "cleartext auth not supported by probe");
                    return -1;
                } else if (auth_type == 5) {
                    /* AuthenticationMD5Password — not supported for probe */
                    snprintf(errbuf, errlen, "MD5 auth not supported by probe");
                    return -1;
                } else if (auth_type == 10) {
                    /* AuthenticationSASL — not supported for probe (would need full SCRAM) */
                    snprintf(errbuf, errlen, "SASL auth required — set pg_hba.conf trust for probe user");
                    return -1;
                } else {
                    snprintf(errbuf, errlen, "unsupported auth type %u", auth_type);
                    return -1;
                }
            }
            break;
        }
        case 'E': {
            /* ErrorResponse — extract message */
            *got_error = true;
            /* Fields are 'type' byte + NUL-terminated string, terminated by '\0' byte */
            const uint8_t* p = body;
            const uint8_t* end = body + body_len;
            while (p < end && *p != '\0') {
                char field_type = (char)*p++;
                const char* field_val = (const char*)p;
                size_t flen = strnlen(field_val, (size_t)(end - p));
                p += flen + 1;
                if (field_type == 'M') {
                    snprintf(errbuf, errlen, "PG error: %s", field_val);
                }
            }
            break;
        }
        case 'D': {
            /* DataRow — extract first column value */
            if (body_len >= 6) {
                /* int16 num_columns, then for each: int32 col_len, col_data */
                uint32_t col_len = get_be32(body + 2);
                if (col_len > 0 && col_len < value_cap && col_len <= body_len - 6) {
                    memcpy(value_out, body + 6, col_len);
                    value_out[col_len] = '\0';
                }
            }
            break;
        }
        case 'Z':
            /* ReadyForQuery — done */
            return 0;
        case 'S':  /* ParameterStatus */
        case 'K':  /* BackendKeyData */
        case 'T':  /* RowDescription */
        case 'C':  /* CommandComplete */
        case 'N':  /* NoticeResponse */
            /* Skip these */
            break;
        default:
            /* Unknown message type — skip */
            break;
        }
    }
}

/* ============================================================================
 * Vtable Implementation
 * ============================================================================ */

/**
 * @brief Create PostgreSQL probe context for one server.
 *
 * Allocates a pg_probe_ctx_t.  The timeout_ms is initialised to a
 * safe default (3000 ms) and may be overridden by the probe manager
 * via the probe_config.
 *
 * @param server  Backend server being probed (host/port/user/db)
 * @param extra   Unused for the postgres probe (reserved for future use)
 * @return Opaque context pointer, or NULL on allocation failure
 */
static void* pg_probe_create(const keel_backend_server_t* server, const char* extra)
{
    (void)server;
    (void)extra;

    pg_probe_ctx_t* ctx = keel_calloc(1, sizeof(pg_probe_ctx_t));
    if (!ctx) return NULL;
    ctx->timeout_ms = 3000;  /* Will be overridden by manager */
    return ctx;
}

/**
 * @brief Execute one health + role check against a PostgreSQL backend.
 *
 * Opens a TCP connection, performs the PG startup handshake, sends the
 * role-detection query, and parses the result.  Always returns KEEL_OK
 * (probe execution succeeded) — the actual server health is reported
 * via result->health.
 *
 * @param opaque  Context from pg_probe_create()
 * @param server  Current backend config (host/port/user/db)
 * @param result  Output: health, detected_role, latency_us, message
 * @return KEEL_OK always (server health is in result->health)
 */
static keel_error_t pg_probe_check(void* opaque, const keel_backend_server_t* server,
                                   keel_probe_check_t* result)
{
    pg_probe_ctx_t* ctx = (pg_probe_ctx_t*)opaque;
    memset(result, 0, sizeof(*result));

    uint64_t t0 = now_us();

    /* 1. TCP connect */
    int fd = keel_probe_tcp_connect(server->host, server->port, ctx->timeout_ms,
                         result->message, sizeof(result->message));
    if (fd < 0) {
        result->health = KEEL_HEALTH_DOWN;
        result->detected_role = KEEL_SERVER_ROLE_AUTO; /* unknown */
        result->latency_us = now_us() - t0;
        result->error = KEEL_ERR_CONNECT;
        return KEEL_OK;  /* probe executed, server is DOWN */
    }

    const char* probe_user = (server->probe_user && *server->probe_user)
                           ? server->probe_user
                           : (server->user ? server->user : "postgres");
    const char* probe_password = (server->probe_password && *server->probe_password)
                               ? server->probe_password
                               : server->password;
    const char* db = server->database ? server->database : "postgres";
    const char* probe_auth = (server->probe_auth && *server->probe_auth)
                           ? server->probe_auth
                           : "auto";
    pg_probe_auth_mode_t auth_mode = parse_probe_auth_mode(probe_auth);

    /* 2. Send StartupMessage */
    uint8_t buf[4096];
    size_t slen = build_startup(buf, sizeof(buf), probe_user, db);
    if (slen == 0 || write_full(fd, buf, slen) < 0) {
        snprintf(result->message, sizeof(result->message), "startup send failed");
        result->health = KEEL_HEALTH_DOWN;
        result->error = KEEL_ERR_IO;
        result->latency_us = now_us() - t0;
        close(fd);
        return KEEL_OK;
    }

    /* 3. Complete startup authentication and wait for ReadyForQuery */
    if (auth_until_ready(fd, ctx->timeout_ms,
                         probe_user, probe_password, auth_mode,
                         result->message, sizeof(result->message)) < 0) {
        result->health = KEEL_HEALTH_DOWN;
        result->error = KEEL_ERR_AUTH;
        result->latency_us = now_us() - t0;
        close(fd);
        return KEEL_OK;
    }

    /* 4. Send role-detection query */
    size_t qlen = build_query(buf, sizeof(buf), ROLE_SQL);
    if (qlen == 0 || write_full(fd, buf, qlen) < 0) {
        /* Server is healthy (auth OK) but query failed */
        result->health = KEEL_HEALTH_UP;
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
        snprintf(result->message, sizeof(result->message), "query send failed");
        result->latency_us = now_us() - t0;
        close(fd);
        return KEEL_OK;
    }

    /* 5. Read query result until ReadyForQuery */
    char value[64] = {0};
    bool got_error = false;
    if (read_until_ready(fd, ctx->timeout_ms, value, sizeof(value),
                         result->message, sizeof(result->message), &got_error) < 0) {
        result->health = KEEL_HEALTH_UP;  /* Was responsive, query read failed */
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
        result->latency_us = now_us() - t0;
        close(fd);
        return KEEL_OK;
    }

    /* 6. Parse role from DataRow value */
    result->health = KEEL_HEALTH_UP;
    result->latency_us = now_us() - t0;

    if (got_error) {
        /* SQL error — server is up but role unknown */
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
    } else if (value[0] == 't') {
        result->detected_role = KEEL_SERVER_ROLE_RW;
        snprintf(result->message, sizeof(result->message), "RW (is_primary=t)");
    } else if (value[0] == 'f') {
        result->detected_role = KEEL_SERVER_ROLE_RO;
        snprintf(result->message, sizeof(result->message), "RO (is_primary=f)");
    } else {
        result->detected_role = KEEL_SERVER_ROLE_AUTO;  /* couldn't determine */
        snprintf(result->message, sizeof(result->message),
                 "role unknown (value='%s')", value);
    }

    /* 7. Send Terminate */
    buf[0] = 'X';
    put_be32(buf + 1, 4);
    write_full(fd, buf, 5);

    close(fd);
    return KEEL_OK;
}

/** @brief Free PostgreSQL probe context. */
static void pg_probe_destroy(void* opaque)
{
    keel_free(opaque);
}

/* ============================================================================
 * Exported Vtable
 * ============================================================================ */

const keel_probe_ops_t keel_probe_postgres_ops = {
    .name    = "postgres",
    .create  = pg_probe_create,
    .check   = pg_probe_check,
    .destroy = pg_probe_destroy,
};
