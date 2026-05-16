/**
 * @file probe_postgres_discovery.c
 * @brief PostgreSQL wire-protocol implementation of keel_discovery_probe_fn.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This file belongs to the probe layer and MUST NOT be linked into keelcore.
 * It implements keel_pg_discovery_probe(), a concrete keel_discovery_probe_fn
 * for PostgreSQL backends, using only POSIX I/O and keel_probe_tcp_connect().
 *
 * Supported auth: trust (AuthenticationOk, type 0) and cleartext password
 * (type 3).  For MD5/SCRAM environments configure a dedicated discovery
 * user with trust authentication.
 */

#include "keel/probe/probe_postgres_discovery.h"
#include "keel/probe/probe_common.h"
#include "keel/log/log.h"
#include "keel/util/platform_compat.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <inttypes.h>

/* ============================================================================
 * PostgreSQL role/state queries (PostgreSQL 10+)
 * ============================================================================ */

/**
 * Columns returned (in order):
 *   0  is_in_recovery  BOOL    — true on replicas
 *   1  is_readonly     BOOL    — true when default_transaction_read_only is on
 *   2  current_lsn     TEXT    — current WAL position (X/XXXXXXXX format)
 *   3  replay_lsn      TEXT    — replay position (NULL on primary)
 *   4  start_time      TEXT    — pg_postmaster_start_time()
 *   5  version_num     TEXT    — numeric version (e.g. "150002")
 *   6  version         TEXT    — human-readable version string
 *   7  timeline_id     TEXT    — WAL timeline
 */
static const char PG_DISC_SQL_SERVER[] =
    "SELECT "
    "  pg_is_in_recovery() AS is_in_recovery,"
    "  current_setting('default_transaction_read_only')::boolean AS is_readonly,"
    "  CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn()"
    "       ELSE pg_current_wal_lsn() END AS current_lsn,"
    "  CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn()"
    "       ELSE NULL END AS replay_lsn,"
    "  pg_postmaster_start_time() AS start_time,"
    "  current_setting('server_version_num')::integer AS version_num,"
    "  current_setting('server_version') AS version,"
    "  (SELECT timeline_id FROM pg_control_checkpoint()) AS timeline_id";

/**
 * Columns returned (in order):
 *   0  lag_bytes   BIGINT  — WAL lag in bytes (0 on primary)
 *   1  lag_seconds FLOAT   — estimated time lag in seconds (0 on primary)
 */
static const char PG_DISC_SQL_LAG[] =
    "SELECT "
    "  CASE WHEN pg_is_in_recovery() THEN"
    "    COALESCE(pg_wal_lsn_diff(pg_last_wal_receive_lsn(),"
    "                             pg_last_wal_replay_lsn()), 0)::bigint"
    "  ELSE 0 END AS lag_bytes,"
    "  CASE WHEN pg_is_in_recovery() THEN"
    "    COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())), 0)"
    "  ELSE 0 END AS lag_seconds";

/* ============================================================================
 * Internal wire helpers
 * ============================================================================ */

static uint16_t pg_disc_be16(const uint8_t* p) {
    return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}

static uint32_t pg_disc_be32(const uint8_t* p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

static void pg_disc_put_be32(uint8_t* p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8); p[3] = (uint8_t) v;
}

static int pg_disc_write_full(int fd, const void* buf, size_t len) {
    const char* p = (const char*)buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) return -1;
        p += n; len -= (size_t)n;
    }
    return 0;
}

static int pg_disc_read_full(int fd, void* buf, size_t len, uint32_t timeout_ms) {
    char*  p   = (char*)buf;
    size_t got = 0;
    while (got < len) {
        int pr = keel_fd_wait(fd, KEEL_FD_WAIT_READ, (int)timeout_ms);
        if (pr <= 0) return -1;
        ssize_t n = read(fd, p + got, len - got);
        if (n <= 0) return -1;
        got += (size_t)n;
    }
    return 0;
}

/** Build a PostgreSQL StartupMessage. Returns message length or 0 on overflow. */
static size_t pg_disc_build_startup(uint8_t* buf, size_t cap,
                                     const char* user, const char* db)
{
    if (cap < 64) return 0;
    uint8_t* p   = buf + 8; /* skip length + protocol fields */
    uint8_t* end = buf + cap;

#define PG_DISC_APPEND(s) do { \
    size_t _l = strlen(s) + 1; \
    if ((size_t)(end - p) < _l) return 0; \
    memcpy(p, (s), _l); p += _l; \
} while (0)

    PG_DISC_APPEND("user");     PG_DISC_APPEND(user ? user : "postgres");
    PG_DISC_APPEND("database"); PG_DISC_APPEND(db   ? db   : "postgres");
    if (p >= end) return 0;
    *p++ = '\0'; /* parameter terminator */

#undef PG_DISC_APPEND

    uint32_t total = (uint32_t)(p - buf);
    pg_disc_put_be32(buf,     total);
    pg_disc_put_be32(buf + 4, 0x00030000u); /* protocol 3.0 */
    return (size_t)(p - buf);
}

/** Build a simple Query ('Q') message. Returns total length or 0 on overflow. */
static size_t pg_disc_build_query(uint8_t* buf, size_t cap, const char* sql)
{
    size_t sql_len = strlen(sql);
    size_t total   = 1 + 4 + sql_len + 1;
    if (total > cap) return 0;
    buf[0] = 'Q';
    pg_disc_put_be32(buf + 1, (uint32_t)(4 + sql_len + 1));
    memcpy(buf + 5, sql, sql_len);
    buf[5 + sql_len] = '\0';
    return total;
}

/**
 * Handle PostgreSQL authentication exchange until ReadyForQuery.
 * Supports: trust (type 0), cleartext password (type 3).
 * Returns 0 on success, -1 on error (errbuf is populated).
 */
static int pg_disc_auth(int fd, uint32_t timeout_ms,
                         const char* pass,
                         char* errbuf, size_t errlen)
{
    uint8_t hdr[5], body[4096];

    for (;;) {
        if (pg_disc_read_full(fd, hdr, 5, timeout_ms) < 0) {
            snprintf(errbuf, errlen, "auth read failed");
            return -1;
        }
        char     type    = (char)hdr[0];
        uint32_t msglen  = pg_disc_be32(hdr + 1);
        if (msglen < 4 || msglen > sizeof(body) + 4) {
            snprintf(errbuf, errlen, "bad auth msg len %u", msglen);
            return -1;
        }
        uint32_t body_len = msglen - 4;
        if (body_len > 0 &&
            pg_disc_read_full(fd, body, body_len, timeout_ms) < 0) {
            snprintf(errbuf, errlen, "auth body read failed");
            return -1;
        }

        if (type == 'E') {
            /* ErrorResponse — extract the 'M' (message) field */
            uint32_t i = 0;
            while (i < body_len) {
                char field = (char)body[i++];
                if (field == '\0') break;
                const char* val = (const char*)(body + i);
                size_t      vl  = strnlen(val, body_len - i);
                if (field == 'M') { snprintf(errbuf, errlen, "%s", val); break; }
                i += (uint32_t)(vl + 1);
            }
            return -1;
        }
        if (type == 'Z') return 0; /* ReadyForQuery */
        if (type != 'R') continue; /* ParameterStatus / BackendKeyData / etc. */

        if (body_len < 4) { snprintf(errbuf, errlen, "short R msg"); return -1; }
        uint32_t auth_type = pg_disc_be32(body);

        if (auth_type == 0) continue; /* AuthenticationOk — wait for Z */

        if (auth_type == 3) {
            /* CleartextPassword */
            if (!pass || !*pass) {
                snprintf(errbuf, errlen,
                         "server requests password but none configured");
                return -1;
            }
            size_t  plen = strlen(pass);
            uint8_t pmsg[520];
            if (plen + 6 > sizeof(pmsg)) {
                snprintf(errbuf, errlen, "password too long");
                return -1;
            }
            pmsg[0] = 'p';
            pg_disc_put_be32(pmsg + 1, (uint32_t)(4 + plen + 1));
            memcpy(pmsg + 5, pass, plen + 1);
            if (pg_disc_write_full(fd, pmsg, 5 + plen + 1) < 0) {
                snprintf(errbuf, errlen, "send password failed");
                return -1;
            }
            continue;
        }

        /* MD5 (type 5) and SCRAM (type 10) are intentionally not supported.
         * Configure a discovery user with trust or password auth. */
        snprintf(errbuf, errlen,
                 "unsupported auth type %u — configure trust or password auth "
                 "for the discovery user", auth_type);
        return -1;
    }
}

/**
 * Execute @p sql as a simple query and capture the first DataRow.
 * @p col_bufs  — array of max_cols char[128] output buffers.
 * @p ncols_out — receives column count (capped to max_cols).
 * Returns 0 (ReadyForQuery reached) or -1 on error.
 */
static int pg_disc_run_query(int fd, uint32_t timeout_ms, const char* sql,
                              char (*col_bufs)[128], int max_cols, int* ncols_out,
                              char* errbuf, size_t errlen)
{
    uint8_t qbuf[8192];
    size_t  qlen = pg_disc_build_query(qbuf, sizeof(qbuf), sql);
    if (qlen == 0 || pg_disc_write_full(fd, qbuf, qlen) < 0) {
        snprintf(errbuf, errlen, "query send failed");
        return -1;
    }

    *ncols_out   = 0;
    bool got_row = false;
    uint8_t hdr[5], body[65536];

    for (;;) {
        if (pg_disc_read_full(fd, hdr, 5, timeout_ms) < 0) {
            snprintf(errbuf, errlen, "response read failed");
            return -1;
        }
        char     type    = (char)hdr[0];
        uint32_t msglen  = pg_disc_be32(hdr + 1);
        if (msglen < 4 || msglen > sizeof(body) + 4) {
            snprintf(errbuf, errlen, "bad msg len %u", msglen);
            return -1;
        }
        uint32_t body_len = msglen - 4;
        if (body_len > 0 &&
            pg_disc_read_full(fd, body, body_len, timeout_ms) < 0) {
            snprintf(errbuf, errlen, "body read failed");
            return -1;
        }

        if (type == 'Z') return 0; /* ReadyForQuery */

        if (type == 'E') {
            uint32_t i = 0;
            while (i < body_len) {
                char field = (char)body[i++];
                if (field == '\0') break;
                const char* val = (const char*)(body + i);
                size_t      vl  = strnlen(val, body_len - i);
                if (field == 'M') { snprintf(errbuf, errlen, "%s", val); break; }
                i += (uint32_t)(vl + 1);
            }
            return -1;
        }

        if (type == 'D' && !got_row && body_len >= 2) {
            int ncols = (int)pg_disc_be16(body);
            if (ncols > max_cols) ncols = max_cols;
            *ncols_out = ncols;
            uint32_t off = 2;
            for (int c = 0; c < ncols && off + 4 <= body_len; c++) {
                int32_t col_len = (int32_t)pg_disc_be32(body + off);
                off += 4;
                if (col_len < 0) {
                    col_bufs[c][0] = '\0'; /* NULL */
                } else {
                    uint32_t cl   = (uint32_t)col_len;
                    if (cl > body_len - off) cl = body_len - off;
                    size_t   copy = (cl < 127u) ? (size_t)cl : 127u;
                    memcpy(col_bufs[c], body + off, copy);
                    col_bufs[c][copy] = '\0';
                    off += (uint32_t)col_len;
                }
            }
            got_row = true;
        }
        /* RowDescription ('T'), CommandComplete ('C'), NoticeResponse ('N') — skip */
    }
}

/** Parse a PostgreSQL LSN string ("A/BBBBBBBB") into a uint64_t. */
static uint64_t pg_disc_parse_lsn(const char* s) {
    if (!s || !*s) return 0;
    uint32_t hi = 0, lo = 0;
    if (sscanf(s, "%X/%X", &hi, &lo) == 2)
        return ((uint64_t)hi << 32) | lo;
    return 0;
}

/** Decode the PostgreSQL numeric version into major/minor. */
static void pg_disc_parse_version(int ver, int* major, int* minor) {
    /* PG 10+: XXXYYZZ  (e.g. 150002 → 15.2) */
    /* PG 9.x: XXYYZZ   (e.g. 090604 → 9.6.4) */
    *major = ver / 10000;
    *minor = (ver / 100) % 100;
}

/* ============================================================================
 * Public probe function
 * ============================================================================ */

keel_error_t keel_pg_discovery_probe(
    const char*                          host,
    uint16_t                             port,
    const char*                          user,
    const char*                          pass,
    const char*                          dbname,
    const keel_discovery_probe_params_t* params,
    keel_server_info_t*                  info
) {
    /* info is zero-filled by the caller */
    strncpy(info->host, host, sizeof(info->host) - 1);
    info->port = port;

    char errbuf[256] = {'\0'};

    /* 1. TCP connect */
    int fd = keel_probe_tcp_connect(host, port, params->timeout_ms,
                                    errbuf, sizeof(errbuf));
    if (fd < 0) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                       "[pg-disc] %s:%d TCP connect failed: %s",
                       host, port, errbuf);
        info->health = KEEL_HEALTH_DOWN;
        return KEEL_OK;
    }

    const char* probe_user = (user   && *user)   ? user   : "postgres";
    const char* probe_db   = (dbname && *dbname) ? dbname : "postgres";

    /* 2. Send StartupMessage */
    uint8_t startup[512];
    size_t  slen = pg_disc_build_startup(startup, sizeof(startup),
                                          probe_user, probe_db);
    if (slen == 0 || pg_disc_write_full(fd, startup, slen) < 0) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                       "[pg-disc] %s:%d startup send failed", host, port);
        info->health = KEEL_HEALTH_DOWN;
        close(fd);
        return KEEL_OK;
    }

    /* 3. Authenticate */
    if (pg_disc_auth(fd, params->timeout_ms, pass, errbuf, sizeof(errbuf)) < 0) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                       "[pg-disc] %s:%d auth failed: %s", host, port, errbuf);
        info->health = KEEL_HEALTH_DOWN;
        close(fd);
        return KEEL_OK;
    }

    /* 4. Execute server state query */
    char srv[8][128];
    memset(srv, 0, sizeof(srv));
    int srv_n = 0;
    if (pg_disc_run_query(fd, params->timeout_ms, PG_DISC_SQL_SERVER,
                          srv, 8, &srv_n, errbuf, sizeof(errbuf)) < 0) {
        KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                       "[pg-disc] %s:%d server query failed: %s",
                       host, port, errbuf);
        /* Auth OK → server is responsive */
        info->health = KEEL_HEALTH_UP;
        close(fd);
        return KEEL_OK;
    }

    /* 5. Parse results */
    bool is_replica       = (srv_n > 0 && srv[0][0] == 't');
    info->is_primary      = !is_replica;
    info->is_standby      = is_replica;
    info->is_readonly     = (srv_n > 1 && srv[1][0] == 't') || is_replica;
    info->accepting_writes = !is_replica;
    info->wal_lsn         = (srv_n > 2) ? pg_disc_parse_lsn(srv[2]) : 0;
    info->replay_lsn      = (srv_n > 3) ? pg_disc_parse_lsn(srv[3]) : 0;
    if (srv_n > 5 && srv[5][0] != '\0') {
        int ver = atoi(srv[5]);
        pg_disc_parse_version(ver, &info->pg_major, &info->pg_minor);
    }
    if (srv_n > 7 && srv[7][0] != '\0')
        info->timeline = atoi(srv[7]);

    info->health = KEEL_HEALTH_UP;

    /* 6. Replication lag (replicas only) */
    if (is_replica) {
        char lag[2][128];
        memset(lag, 0, sizeof(lag));
        int lag_n = 0;
        if (pg_disc_run_query(fd, params->timeout_ms, PG_DISC_SQL_LAG,
                              lag, 2, &lag_n, errbuf, sizeof(errbuf)) == 0
                && lag_n >= 2) {
            info->lag_bytes   = (uint64_t)strtoull(lag[0], NULL, 10);
            info->lag_seconds = strtod(lag[1], NULL);
            if (info->lag_bytes   > params->max_lag_bytes ||
                info->lag_seconds > params->max_lag_seconds) {
                info->health = KEEL_HEALTH_DEGRADED;
            }
        }
    }

    /* 7. Terminate */
    uint8_t term[5] = { 'X', 0, 0, 0, 4 };
    pg_disc_write_full(fd, term, sizeof(term));
    close(fd);

    KEEL_LOG_DEBUG(KEEL_LOG_CAT_POOL,
                   "[pg-disc] %s:%d OK: primary=%s pg=%d.%d "
                   "lag_bytes=%" PRIu64 " lag_s=%.2f",
                   host, port,
                   info->is_primary ? "yes" : "no",
                   info->pg_major, info->pg_minor,
                   info->lag_bytes, info->lag_seconds);

    return KEEL_OK;
}
