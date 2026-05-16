/**
 * @file probe_mysql.c
 * @brief MySQL and MariaDB wire-protocol health and role probe.
 * @author Charly Batista
 *
 * Copyright (C) Virtlabs, https://virtlabs.io
 *
 * Like the PostgreSQL probe, this implementation speaks the database protocol
 * directly so it can authenticate, execute one lightweight role-detection query,
 * and interpret the result without pulling in a full client library. The logic is
 * slightly more involved because MySQL-family authentication can switch plugins at
 * runtime, but the direct approach keeps control over timeout behavior and makes it
 * easier to support both MySQL and MariaDB with the same probe skeleton.
 */

#include "keel/probe/probe.h"
#include "keel/probe/probe_common.h"
#include "keel/engine/engine.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"

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

#include <openssl/sha.h>
#include <openssl/evp.h>

/* ============================================================================
 * MySQL Wire Protocol Constants
 * ============================================================================ */

#define MY_HDR  4           /* 3-byte LE length + 1-byte seq_id */
#define MY_OK   0x00
#define MY_ERR  0xFF
#define MY_EOF  0xFE
#define MY_AUTH_SWITCH    0xFE
#define MY_AUTH_MORE_DATA 0x01

#define MY_COM_QUERY 0x03
#define MY_COM_QUIT  0x01

#define MY_CAP_CONNECT_WITH_DB   (1U <<  3)
#define MY_CAP_SECURE_CONNECTION (1U << 15)
#define MY_CAP_PROTOCOL_41       (1U <<  9)
#define MY_CAP_PLUGIN_AUTH       (1U << 19)

/* ============================================================================
 * Probe Context
 * ============================================================================ */

typedef struct my_probe_ctx {
    uint32_t  timeout_ms;
} my_probe_ctx_t;

/* ============================================================================
 * Byte Helpers
 * ============================================================================ */

/** @brief Read a little-endian 16-bit value from a byte buffer.
 *  @param p Pointer to at least 2 readable bytes.
 *  @return Decoded 16-bit unsigned value.
 */
static inline uint16_t probe_rdle16(const uint8_t* p) {
    return (uint16_t)((unsigned)p[0] | ((unsigned)p[1] << 8));
}

/** @brief Read a little-endian 24-bit value from a byte buffer.
 *  @param p Pointer to at least 3 readable bytes.
 *  @return Decoded value in the lower 24 bits of a uint32_t.
 */
static inline uint32_t probe_rdle24(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16);
}

/** @brief Write a value as a little-endian 24-bit integer into a byte buffer.
 *  @param p Pointer to at least 3 writable bytes.
 *  @param v Value to encode; only the lower 24 bits are stored.
 */
static inline void probe_wrle24(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
}

/* ============================================================================
 * Time Helpers
 * ============================================================================ */

/** @brief Return the current monotonic clock value in microseconds.
 *  @return Monotonic timestamp (µs) suitable for measuring elapsed time.
 */
static uint64_t now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* ============================================================================
 * Low-level I/O Helpers
 * ============================================================================ */

/**
 * @brief Resolve a host and establish a non-blocking TCP connection.
 *
 * @param host       Target hostname or IP address string.
 * @param port       Target TCP port number.
 * @param timeout_ms Connect timeout in milliseconds.
 * @param errbuf     Buffer to receive a human-readable error message.
 * @param errlen     Size of @p errbuf in bytes.
 * @return Connected and blocking file descriptor, or -1 on failure.
 */
/* probe_tcp_connect() removed: use keel_probe_tcp_connect() from keel/probe/probe_common.h */

/**
 * @brief Read exactly @p len bytes from a socket, polling for each chunk.
 *
 * @param fd         Socket file descriptor.
 * @param buf        Destination buffer of at least @p len bytes.
 * @param len        Number of bytes to read.
 * @param timeout_ms Per-iteration poll timeout in milliseconds.
 * @return 0 on success, -1 on poll timeout or I/O error.
 */
static int probe_read_full(int fd, void* buf, size_t len, uint32_t timeout_ms)
{
    size_t got = 0;
    while (got < len) {
        int pr = keel_fd_wait(fd, KEEL_FD_WAIT_READ, (int)timeout_ms);
        if (pr <= 0) return -1;
        ssize_t n = read(fd, (char*)buf + got, len - got);
        if (n <= 0) return -1;
        got += (size_t)n;
    }
    return 0;
}

/**
 * @brief Write exactly @p len bytes to a socket.
 *
 * @param fd   Socket file descriptor.
 * @param buf  Source data buffer.
 * @param len  Number of bytes to write.
 * @return 0 on success, -1 on I/O error.
 */
static int probe_write_full(int fd, const void* buf, size_t len)
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

/**
 * Read a MySQL packet: 4-byte header (3 LE length + 1 seq) + payload.
 * Returns total bytes read (header + payload), or -1 on error.
 * Caller must provide buf with at least max_len bytes.
 */
static ssize_t probe_read_mysql_pkt(int fd, uint8_t* buf, size_t max_len,
                                    uint32_t timeout_ms)
{
    /* Read header */
    if (probe_read_full(fd, buf, MY_HDR, timeout_ms) < 0)
        return -1;

    uint32_t payload_len = probe_rdle24(buf);
    if (payload_len > max_len - MY_HDR)
        return -1;

    /* Read payload */
    if (payload_len > 0) {
        if (probe_read_full(fd, buf + MY_HDR, payload_len, timeout_ms) < 0)
            return -1;
    }

    return (ssize_t)(MY_HDR + payload_len);
}

/* ============================================================================
 * MySQL Authentication — Scramble Functions
 * ============================================================================ */

/**
 * @brief Compute the mysql_native_password scramble response.
 *
 * Computes SHA1(password) XOR SHA1( SHA1(SHA1(password)) || scramble ).
 *
 * @param password     Plaintext password string.
 * @param scramble     20-byte server nonce from the Initial Handshake.
 * @param scramble_len Length of @p scramble (typically 20).
 * @param out          Output buffer for the 20-byte scramble response.
 */
static void probe_scramble_native(const char* password,
                                  const uint8_t* scramble, size_t scramble_len,
                                  uint8_t* out)
{
    uint8_t stage1[SHA_DIGEST_LENGTH];
    SHA1((const uint8_t*)password, strlen(password), stage1);

    uint8_t stage2[SHA_DIGEST_LENGTH];
    SHA1(stage1, SHA_DIGEST_LENGTH, stage2);

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    uint8_t digest[SHA_DIGEST_LENGTH];
    unsigned int dlen = SHA_DIGEST_LENGTH;
    EVP_DigestInit_ex(ctx, EVP_sha1(), NULL);
    EVP_DigestUpdate(ctx, scramble, scramble_len);
    EVP_DigestUpdate(ctx, stage2, SHA_DIGEST_LENGTH);
    EVP_DigestFinal_ex(ctx, digest, &dlen);
    EVP_MD_CTX_free(ctx);

    for (int i = 0; i < SHA_DIGEST_LENGTH; i++)
        out[i] = stage1[i] ^ digest[i];
}

/**
 * @brief Compute the caching_sha2_password scramble response.
 *
 * Computes SHA256(password) XOR SHA256( SHA256(SHA256(password)) || scramble ).
 *
 * @param password     Plaintext password string.
 * @param scramble     Server-provided nonce from the Initial Handshake.
 * @param scramble_len Length of @p scramble.
 * @param out          Output buffer for the 32-byte scramble response.
 */
static void probe_scramble_sha2(const char* password,
                                const uint8_t* scramble, size_t scramble_len,
                                uint8_t* out)
{
    uint8_t stage1[SHA256_DIGEST_LENGTH];
    SHA256((const uint8_t*)password, strlen(password), stage1);

    uint8_t stage2[SHA256_DIGEST_LENGTH];
    SHA256(stage1, SHA256_DIGEST_LENGTH, stage2);

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    uint8_t digest[SHA256_DIGEST_LENGTH];
    unsigned int dlen = SHA256_DIGEST_LENGTH;
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, stage2, SHA256_DIGEST_LENGTH);
    EVP_DigestUpdate(ctx, scramble, scramble_len);
    EVP_DigestFinal_ex(ctx, digest, &dlen);
    EVP_MD_CTX_free(ctx);

    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
        out[i] = stage1[i] ^ digest[i];
}

/* ============================================================================
 * MySQL Handshake Parsing and Response Building
 * ============================================================================ */

typedef struct {
    uint32_t caps;
    uint8_t  scramble[21];
    size_t   scramble_len;
    char     plugin[64];
    uint8_t  seq_id;
} probe_handshake_t;

/**
 * @brief Parse a MySQL protocol-v10 Initial Handshake packet.
 *
 * @param data      Raw packet buffer including the 4-byte packet header.
 * @param total_len Total bytes available in @p data.
 * @param[out] hs   Populated handshake descriptor on success.
 * @return 0 on success, -1 if the buffer is too short or the protocol version is not 10.
 */
static int probe_parse_handshake(const uint8_t* data, size_t total_len,
                                 probe_handshake_t* hs)
{
    memset(hs, 0, sizeof(*hs));
    if (total_len < MY_HDR + 1) return -1;

    hs->seq_id = data[3];
    const uint8_t* payload = data + MY_HDR;
    uint32_t plen = probe_rdle24(data);

    if (payload[0] != 10) return -1; /* protocol v10 */

    size_t pos = 1;
    /* server version (NUL-terminated) */
    size_t vlen = strnlen((const char*)(payload + pos), plen - pos);
    pos += vlen + 1;
    if (pos + 4 > plen) return -1;
    pos += 4; /* connection_id */

    /* scramble part 1 (8 bytes) */
    if (pos + 8 > plen) return -1;
    memcpy(hs->scramble, payload + pos, 8);
    pos += 8;
    pos += 1; /* filler */

    /* caps lower 2 bytes */
    if (pos + 2 > plen) return -1;
    hs->caps = probe_rdle16(payload + pos);
    pos += 2;

    if (pos >= plen) {
        hs->scramble_len = 8;
        strcpy(hs->plugin, "mysql_native_password");
        return 0;
    }

    pos += 1; /* charset */
    if (pos + 2 > plen) return -1;
    pos += 2; /* status flags */
    if (pos + 2 > plen) return -1;
    hs->caps |= ((uint32_t)probe_rdle16(payload + pos)) << 16;
    pos += 2;

    uint8_t auth_data_len = 0;
    if (pos < plen) auth_data_len = payload[pos];
    pos += 1;

    if (pos + 10 > plen) return -1;
    pos += 10; /* reserved */

    /* scramble part 2 */
    if (hs->caps & MY_CAP_SECURE_CONNECTION) {
        size_t part2_len = auth_data_len > 8 ? (size_t)(auth_data_len - 8) : 13;
        if (pos + part2_len > plen) part2_len = plen - pos;
        size_t copy = part2_len > 12 ? 12 : part2_len;
        memcpy(hs->scramble + 8, payload + pos, copy);
        hs->scramble_len = 8 + copy;
        pos += part2_len;
    } else {
        hs->scramble_len = 8;
    }

    /* auth plugin name */
    if ((hs->caps & MY_CAP_PLUGIN_AUTH) && pos < plen) {
        size_t nlen = strnlen((const char*)(payload + pos), plen - pos);
        if (nlen < sizeof(hs->plugin))
            memcpy(hs->plugin, payload + pos, nlen);
    }
    if (hs->plugin[0] == '\0')
        strcpy(hs->plugin, "mysql_native_password");

    return 0;
}

/**
 * @brief Serialise a MySQL Handshake Response (protocol-41) into a packet buffer.
 *
 * @param hs       Parsed server handshake supplying capabilities and nonce.
 * @param user     Database username (NUL-terminated).
 * @param db       Initial database name; may be empty.
 * @param password Plaintext password; may be empty.
 * @param buf      Output buffer for the serialised packet.
 * @param blen     Capacity of @p buf in bytes.
 * @return Total bytes written (header + payload), or -1 if @p buf is too small.
 */
static ssize_t probe_build_handshake_resp(
    const probe_handshake_t* hs,
    const char* user, const char* db, const char* password,
    uint8_t* buf, size_t blen)
{
    uint8_t pl[512];
    size_t pos = 0;

    uint8_t auth_data[32];
    size_t auth_len = 0;
    const char* auth_plugin = hs->plugin;

    if (password && password[0]) {
        if (strcmp(auth_plugin, "caching_sha2_password") == 0) {
            probe_scramble_sha2(password, hs->scramble, hs->scramble_len,
                                auth_data);
            auth_len = 32;
        } else {
            probe_scramble_native(password, hs->scramble, hs->scramble_len,
                                  auth_data);
            auth_len = 20;
        }
    }

    uint32_t caps = MY_CAP_PROTOCOL_41 | MY_CAP_SECURE_CONNECTION |
                    MY_CAP_PLUGIN_AUTH;
    if (db && db[0]) caps |= MY_CAP_CONNECT_WITH_DB;
    caps &= hs->caps;
    caps |= MY_CAP_PROTOCOL_41 | MY_CAP_SECURE_CONNECTION;

    pl[pos++] = (uint8_t)caps;
    pl[pos++] = (uint8_t)(caps >> 8);
    pl[pos++] = (uint8_t)(caps >> 16);
    pl[pos++] = (uint8_t)(caps >> 24);

    pl[pos++] = 0xFF; pl[pos++] = 0xFF;
    pl[pos++] = 0xFF; pl[pos++] = 0x00;
    pl[pos++] = 0x2d; /* utf8mb4 */
    memset(pl + pos, 0, 23); pos += 23;

    size_t ulen = strlen(user);
    memcpy(pl + pos, user, ulen); pos += ulen;
    pl[pos++] = 0;

    pl[pos++] = (uint8_t)auth_len;
    if (auth_len > 0) { memcpy(pl + pos, auth_data, auth_len); pos += auth_len; }

    if (caps & MY_CAP_CONNECT_WITH_DB) {
        size_t dlen = strlen(db);
        memcpy(pl + pos, db, dlen); pos += dlen;
        pl[pos++] = 0;
    }

    if (caps & MY_CAP_PLUGIN_AUTH) {
        size_t plen = strlen(auth_plugin);
        memcpy(pl + pos, auth_plugin, plen); pos += plen;
        pl[pos++] = 0;
    }

    size_t total = MY_HDR + pos;
    if (total > blen) return -1;
    probe_wrle24(buf, (uint32_t)pos);
    buf[3] = hs->seq_id + 1;
    memcpy(buf + MY_HDR, pl, pos);
    return (ssize_t)total;
}

/* ============================================================================
 * MySQL Probe Authentication (full multi-round)
 * ============================================================================ */

/**
 * @brief Perform the full MySQL authentication handshake on an open socket.
 *
 * Handles protocol v10, mysql_native_password, caching_sha2_password, and
 * multi-round AuthSwitchRequest sequences (up to 10 rounds).
 *
 * @param fd          Connected socket file descriptor.
 * @param timeout_ms  Per-read poll timeout in milliseconds.
 * @param user        Database username.
 * @param db          Initial database name; may be empty.
 * @param password    Plaintext password; may be empty.
 * @param errbuf      Buffer to receive a human-readable error message.
 * @param errlen      Size of @p errbuf in bytes.
 * @return 0 on successful authentication, -1 on failure.
 */
static int probe_mysql_auth(int fd, uint32_t timeout_ms,
                            const char* user, const char* db,
                            const char* password,
                            char* errbuf, size_t errlen)
{
    uint8_t buf[4096];

    /* Read Initial Handshake */
    ssize_t n = probe_read_mysql_pkt(fd, buf, sizeof(buf), timeout_ms);
    if (n < 0) {
        snprintf(errbuf, errlen, "read handshake failed");
        return -1;
    }

    probe_handshake_t hs;
    if (probe_parse_handshake(buf, (size_t)n, &hs) < 0) {
        snprintf(errbuf, errlen, "parse handshake failed");
        return -1;
    }

    /* Build and send Handshake Response */
    uint8_t resp[1024];
    ssize_t rlen = probe_build_handshake_resp(&hs, user, db, password,
                                              resp, sizeof(resp));
    if (rlen < 0 || probe_write_full(fd, resp, (size_t)rlen) < 0) {
        snprintf(errbuf, errlen, "send handshake response failed");
        return -1;
    }

    /* Read auth responses (may be multi-round for caching_sha2) */
    for (int round = 0; round < 10; round++) {
        n = probe_read_mysql_pkt(fd, buf, sizeof(buf), timeout_ms);
        if (n < (ssize_t)(MY_HDR + 1)) {
            snprintf(errbuf, errlen, "read auth response failed (round %d)", round);
            return -1;
        }

        uint32_t pkt_len = probe_rdle24(buf);
        uint8_t seq = buf[3];
        const uint8_t* payload = buf + MY_HDR;

        if (payload[0] == MY_OK) {
            return 0; /* Authenticated! */
        }

        if (payload[0] == MY_ERR) {
            if (pkt_len >= 3) {
                uint16_t ec = probe_rdle16(payload + 1);
                snprintf(errbuf, errlen, "MySQL error %u", ec);
            } else {
                snprintf(errbuf, errlen, "MySQL error");
            }
            return -1;
        }

        if (payload[0] == MY_AUTH_SWITCH && pkt_len > 1) {
            /* AuthSwitchRequest */
            size_t pp = 1;
            const char* new_plugin = (const char*)(payload + pp);
            size_t nlen = strnlen(new_plugin, pkt_len - pp);
            pp += nlen + 1;
            const uint8_t* new_scramble = payload + pp;
            size_t ns_len = pkt_len - pp;
            if (ns_len > 0 && new_scramble[ns_len - 1] == 0) ns_len--;

            uint8_t auth_data[32];
            size_t auth_len = 0;

            if (strcmp(new_plugin, "mysql_native_password") == 0) {
                probe_scramble_native(password, new_scramble, ns_len, auth_data);
                auth_len = 20;
            } else if (strcmp(new_plugin, "caching_sha2_password") == 0) {
                probe_scramble_sha2(password, new_scramble, ns_len, auth_data);
                auth_len = 32;
            } else {
                snprintf(errbuf, errlen, "unsupported auth plugin: %.200s", new_plugin);
                return -1;
            }

            uint8_t ar[MY_HDR + 32];
            probe_wrle24(ar, (uint32_t)auth_len);
            ar[3] = seq + 1;
            memcpy(ar + MY_HDR, auth_data, auth_len);
            if (probe_write_full(fd, ar, MY_HDR + auth_len) < 0) {
                snprintf(errbuf, errlen, "send auth switch response failed");
                return -1;
            }
            continue;
        }

        if (payload[0] == MY_AUTH_MORE_DATA && pkt_len >= 2) {
            /* caching_sha2 continuation */
            uint8_t status = payload[1];
            if (status == 0x03) {
                /* Fast auth success — continue to read final OK */
                continue;
            }
            if (status == 0x04) {
                /* Full auth needed — send cleartext password */
                size_t pwlen = strlen(password) + 1;
                uint8_t pkt[MY_HDR + 256];
                if (pwlen > 255) {
                    snprintf(errbuf, errlen, "password too long for cleartext fallback");
                    return -1;
                }
                probe_wrle24(pkt, (uint32_t)pwlen);
                pkt[3] = seq + 1;
                memcpy(pkt + MY_HDR, password, pwlen);
                if (probe_write_full(fd, pkt, MY_HDR + pwlen) < 0) {
                    snprintf(errbuf, errlen, "send cleartext password failed");
                    return -1;
                }
                continue;
            }
            snprintf(errbuf, errlen, "unexpected caching_sha2 status: 0x%02x", status);
            return -1;
        }

        snprintf(errbuf, errlen, "unexpected auth marker: 0x%02x", payload[0]);
        return -1;
    }

    snprintf(errbuf, errlen, "too many auth rounds");
    return -1;
}

/* ============================================================================
 * MySQL Result Set Parsing (Minimal)
 * ============================================================================
 *
 * After COM_QUERY, the server sends:
 *   1. Column count (1 packet with column_count as lenenc int)
 *   2. Column definitions (column_count packets)
 *   3. EOF marker (if not CLIENT_DEPRECATE_EOF)
 *   4. Row data packets (text protocol: NUL-terminated or length-encoded strings)
 *   5. EOF marker (or OK packet)
 *
 * We only need the first column of the first row.
 */

/**
 * @brief Send COM_QUERY and read first column of first row.
 *
 * @param fd       Connected and authenticated MySQL socket
 * @param sql      SQL query to execute
 * @param value    Output buffer for first column value
 * @param val_cap  Size of value buffer
 * @param timeout_ms  Read timeout
 * @param errbuf   Error message buffer
 * @param errlen   Error buffer length
 * @return 0 on success, -1 on error
 */
static int probe_mysql_query(int fd, const char* sql,
                             char* value, size_t val_cap,
                             uint32_t timeout_ms,
                             char* errbuf, size_t errlen)
{
    uint8_t buf[4096];
    size_t slen = strlen(sql);
    size_t payload_len = 1 + slen; /* COM_QUERY + sql */

    /* Build COM_QUERY packet */
    probe_wrle24(buf, (uint32_t)payload_len);
    buf[3] = 0; /* seq_id = 0 for new command */
    buf[MY_HDR] = MY_COM_QUERY;
    memcpy(buf + MY_HDR + 1, sql, slen);

    if (probe_write_full(fd, buf, MY_HDR + payload_len) < 0) {
        snprintf(errbuf, errlen, "send query failed");
        return -1;
    }

    /* Read response */
    ssize_t n = probe_read_mysql_pkt(fd, buf, sizeof(buf), timeout_ms);
    if (n < (ssize_t)(MY_HDR + 1)) {
        snprintf(errbuf, errlen, "read query response failed");
        return -1;
    }

    uint32_t pkt_len = probe_rdle24(buf);
    const uint8_t* payload = buf + MY_HDR;

    /* Check for OK (column_count=0, e.g. for non-SELECT) */
    if (payload[0] == MY_OK) {
        value[0] = '\0';
        return 0;
    }

    /* Check for ERR */
    if (payload[0] == MY_ERR) {
        snprintf(errbuf, errlen, "MySQL query error");
        return -1;
    }

    /* Column count (lenenc int) */
    size_t cpos = 0;
    uint8_t column_count = payload[cpos];
    if (column_count == 0 || column_count > 100) {
        snprintf(errbuf, errlen, "unexpected column_count: %u", column_count);
        return -1;
    }

    /* Skip column definition packets */
    for (uint8_t i = 0; i < column_count; i++) {
        n = probe_read_mysql_pkt(fd, buf, sizeof(buf), timeout_ms);
        if (n < (ssize_t)MY_HDR) {
            snprintf(errbuf, errlen, "read column def failed");
            return -1;
        }
    }

    /* Read EOF packet after column definitions */
    n = probe_read_mysql_pkt(fd, buf, sizeof(buf), timeout_ms);
    if (n < (ssize_t)(MY_HDR + 1)) {
        snprintf(errbuf, errlen, "read column EOF failed");
        return -1;
    }
    /* Could be EOF (0xFE) or could be an OK if CLIENT_DEPRECATE_EOF */

    /* Read first data row */
    n = probe_read_mysql_pkt(fd, buf, sizeof(buf), timeout_ms);
    if (n < (ssize_t)(MY_HDR + 1)) {
        snprintf(errbuf, errlen, "read data row failed");
        return -1;
    }

    pkt_len = probe_rdle24(buf);
    const uint8_t* row_payload = buf + MY_HDR;

    /* Check if this is already EOF (empty result set) */
    if (row_payload[0] == MY_EOF && pkt_len < 9) {
        value[0] = '\0';
        return 0; /* Empty result, technically OK */
    }

    /* Parse first column value (text protocol: length-encoded string) */
    size_t rpos = 0;
    if (row_payload[rpos] == 0xFB) {
        /* NULL value */
        value[0] = '\0';
    } else if (row_payload[rpos] < 0xFB) {
        /* Length-encoded string: first byte is length */
        uint8_t vlen = row_payload[rpos];
        rpos++;
        if (rpos + vlen <= pkt_len && vlen < val_cap) {
            memcpy(value, row_payload + rpos, vlen);
            value[vlen] = '\0';
        } else {
            value[0] = '\0';
        }
    } else {
        value[0] = '\0';
    }

    /* Drain remaining packets (more rows + final EOF) until we see EOF */
    for (int drain = 0; drain < 100; drain++) {
        n = probe_read_mysql_pkt(fd, buf, sizeof(buf), timeout_ms);
        if (n < (ssize_t)(MY_HDR + 1)) break;
        pkt_len = probe_rdle24(buf);
        if ((buf[MY_HDR] == MY_EOF && pkt_len < 9) || buf[MY_HDR] == MY_OK)
            break;
    }

    return 0;
}

/* ============================================================================
 * Vtable Implementation
 * ============================================================================ */

/**
 * @brief Allocate and initialise a MySQL/MariaDB probe context.
 *
 * @param server Backend server descriptor (reserved; currently unused).
 * @param extra  Extra configuration string (reserved; currently unused).
 * @return Opaque probe context pointer, or `NULL` on allocation failure.
 */
static void* my_probe_create(const keel_backend_server_t* server,
                             const char* extra)
{
    (void)server;
    (void)extra;

    my_probe_ctx_t* ctx = keel_calloc(1, sizeof(*ctx));
    if (!ctx) return NULL;
    ctx->timeout_ms = 3000;
    return ctx;
}

static const char ROLE_SQL[] = "SELECT @@read_only AS ro";

/**
 * @brief Execute one MySQL health-check and role-detection probe cycle.
 *
 * Connects, authenticates, executes <tt>SELECT @@read_only</tt>, and populates
 * @p result with health status, detected role, and round-trip latency.
 *
 * @param opaque      Probe context returned by my_probe_create().
 * @param server      Backend server descriptor with connection parameters.
 * @param[out] result Probe outcome populated on return.
 * @return Always `KEEL_OK`; errors are encoded in @p result.
 */
static keel_error_t my_probe_check(void* opaque,
                                  const keel_backend_server_t* server,
                                  keel_probe_check_t* result)
{
    my_probe_ctx_t* ctx = (my_probe_ctx_t*)opaque;
    memset(result, 0, sizeof(*result));

    uint64_t t0 = now_us();

    /* 1. TCP connect */
    int fd = keel_probe_tcp_connect(server->host, server->port, ctx->timeout_ms,
                               result->message, sizeof(result->message));
    if (fd < 0) {
        result->health = KEEL_HEALTH_DOWN;
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
        result->latency_us = now_us() - t0;
        result->error = KEEL_ERR_CONNECT;
        return KEEL_OK;
    }

    /* 2. MySQL handshake + authentication */
    const char* user = server->user ? server->user : "root";
    const char* db   = server->database ? server->database : "";
    const char* pass = server->password ? server->password : "";

    if (probe_mysql_auth(fd, ctx->timeout_ms, user, db, pass,
                         result->message, sizeof(result->message)) < 0) {
        result->health = KEEL_HEALTH_DOWN;
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
        result->latency_us = now_us() - t0;
        result->error = KEEL_ERR_AUTH;
        close(fd);
        return KEEL_OK;
    }

    /* 3. Send role-detection query: SELECT @@read_only */
    char value[64] = {0};
    if (probe_mysql_query(fd, ROLE_SQL, value, sizeof(value),
                          ctx->timeout_ms,
                          result->message, sizeof(result->message)) < 0) {
        result->health = KEEL_HEALTH_UP; /* Auth OK, query failed */
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
        result->latency_us = now_us() - t0;
        close(fd);
        return KEEL_OK;
    }

    /* 4. Parse role */
    result->health = KEEL_HEALTH_UP;
    result->latency_us = now_us() - t0;

    if (value[0] == '0') {
        result->detected_role = KEEL_SERVER_ROLE_RW;
        snprintf(result->message, sizeof(result->message),
                 "RW (@@read_only=0)");
    } else if (value[0] == '1') {
        result->detected_role = KEEL_SERVER_ROLE_RO;
        snprintf(result->message, sizeof(result->message),
                 "RO (@@read_only=1)");
    } else {
        result->detected_role = KEEL_SERVER_ROLE_AUTO;
        snprintf(result->message, sizeof(result->message),
                 "role unknown (@@read_only='%s')", value);
    }

    /* 5. Send COM_QUIT and close */
    uint8_t quit[MY_HDR + 1];
    probe_wrle24(quit, 1);
    quit[3] = 0;
    quit[MY_HDR] = MY_COM_QUIT;
    probe_write_full(fd, quit, sizeof(quit));
    close(fd);

    return KEEL_OK;
}

/**
 * @brief Release a MySQL probe context allocated by my_probe_create().
 *
 * @param opaque Probe context to free.
 */
static void my_probe_destroy(void* opaque)
{
    keel_free(opaque);
}

/* ============================================================================
 * Exported Vtable
 * ============================================================================ */

const keel_probe_ops_t keel_probe_mysql_ops = {
    .name    = "mysql",
    .create  = my_probe_create,
    .check   = my_probe_check,
    .destroy = my_probe_destroy,
};
