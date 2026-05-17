/**
 * @file admin.c
 * @brief Administrative control plane for SQL-style introspection and HTTP metrics.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * This translation unit implements KEEL's low-throughput control plane. It is
 * intentionally separate from the worker fast path and accepts that control
 * operations may be synchronous, serialized, and allocation-heavy because they
 * are used for observability, operational control, and debugging rather than
 * data-plane query forwarding.
 *
 * Responsibilities implemented here:
 * - A PostgreSQL-wire admin endpoint compatible with `psql`-style clients.
 * - PgBouncer-inspired `SHOW ...` commands for runtime inspection.
 * - Mutating administrative commands such as pause, resume, drain, reload, and
 *   dynamic backend membership changes.
 * - A small HTTP server exposing Prometheus/OpenMetrics-style text metrics.
 * - Conversion of tabular admin results to JSON when callers request
 *   `FORMAT JSON`.
 *
 * Execution model:
 * - A single dedicated thread polls up to two listening sockets.
 * - Each accepted connection is processed synchronously to completion.
 * - The design avoids contaminating worker-thread hot paths with admin-only
 *   protocol state, synchronization, or formatting logic.
 *
 * Important operational constraints:
 * - This code favors observability and simplicity over scalability.
 * - Many responses are snapshots and may race with live worker activity.
 * - Administrative mutations are best-effort and rely on engine/pool APIs for
 *   correctness; this file does not add extra locking around subsystems.
 * - The admin thread must remain responsive, so socket timeouts and bounded
 *   buffers are used at protocol edges.
 */

#include "keel/core/admin.h"
#include "keel/core/web_ui.h"
#include "keel/core/compress.h"
#include "keel/core/auth.h"
#include "keel/core/cluster.h"
#include "keel/core/router.h"
#include "keel/core/query_rules.h"
#include "keel/core/throttle.h"
#include "keel/core/router_discovery.h"
#include "keel/session/hardpin.h"
#include "keel/engine/engine.h"
#include "keel/core/stats.h"
#include "keel/engine/worker.h"
#include "keel/engine/backend_pool.h"
#include "keel/core/config.h"
#include "keel/log/log.h"
#include "keel/mem/mem.h"
#include "keel/protocol/tls_context.h"
#include "keel/protocol/ktls.h"
#include "keel/trace/trace.h"
#include "keel/sql/sql_ast.h"
#include "keel/util/endian.h"
#include "keel/util/util.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#if defined(__linux__)
#include <sys/epoll.h>
#endif
#include "keel/util/platform_compat.h"
#ifdef KEEL_HAS_OPENSSL
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#endif

/* ============================================================================
 * Internal state
 * ============================================================================ */

struct keel_admin {
    keel_admin_config_t  cfg;
    keel_engine_t       *engine;
    keel_cluster_t      *cluster;
    keel_router_t       *router;
    keel_query_rules_t  *query_rules;
    keel_throttle_rules_t *throttle_rules;
    keel_discovery_t    *discovery;
    pthread_t           thread;
    volatile bool       running;
    int                 admin_fd;
    int                 prom_fd;
    keel_auth_manager_t *auth_mgr;
};

/* Helpers wr16/wr32/rd32 replaced by keel_be16_put/keel_be32_put/keel_be32_get
 * from keel/util/endian.h */
#define wr16(p,v) keel_be16_put((p),(v))
#define wr32(p,v) keel_be32_put((p),(v))
#define rd32(p)   keel_be32_get(p)

/**
 * @brief Send an entire buffer to a socket unless a hard error occurs.
 *
 * @param fd Connected socket descriptor.
 * @param buf Buffer to send.
 * @param len Exact number of bytes to transmit.
 * @return Number of bytes sent on success, or `-1` on error.
 *
 * Errors handled:
 * - Retries automatically on `EINTR`.
 * - Returns `-1` on all other `send(2)` failures.
 *
 * Corner cases:
 * - Partial sends are retried until the full payload is written.
 * - `MSG_NOSIGNAL` suppresses `SIGPIPE`, so callers only observe the return
 *   value and do not need signal-level handling.
 *
 * Main uses:
 * - Flushing PostgreSQL admin responses.
 * - Sending HTTP response headers and metric bodies.
 */
static ssize_t safe_send(int fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, p + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)n;
    }
    return (ssize_t)sent;
}

/**
 * @brief Read exactly `len` bytes from a socket unless EOF or error interrupts the operation.
 *
 * @param fd Connected socket descriptor.
 * @param[out] buf Destination buffer.
 * @param len Number of bytes expected.
 * @return `len` on full success, a short byte count on EOF, or `-1` on error.
 *
 * Errors handled:
 * - Retries on `EINTR`.
 * - Returns `-1` for non-recoverable receive errors.
 *
 * Corner cases:
 * - If the peer closes early, the function returns the number of bytes already
 *   received so callers can distinguish truncated frames from hard errors.
 * - Callers are responsible for treating short reads as protocol failure when
 *   message framing requires exact lengths.
 *
 * Main uses:
 * - Reading PostgreSQL startup packets.
 * - Reading simple-query frames and HTTP request data.
 */
static ssize_t safe_recv(int fd, void *buf, size_t len) {
    uint8_t *p = (uint8_t *)buf;
    size_t got = 0;
    while (got < len) {
        ssize_t n = recv(fd, p + got, len - got, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return n == 0 ? (ssize_t)got : -1;
        }
        got += (size_t)n;
    }
    return (ssize_t)got;
}

/**
 * @brief Create, bind, and listen on an IPv4 TCP socket.
 *
 * @param addr Textual IPv4 address to bind to.
 * @param port TCP port in host order.
 * @return Listening file descriptor on success, or `-1` on failure.
 *
 * Errors handled:
 * - Socket creation, bind, or listen failure cause cleanup and `-1`.
 *
 * Corner cases:
 * - Invalid textual addresses are passed to `inet_pton`; if conversion fails,
 *   bind will fail and surface as a generic listener creation failure.
 * - The helper only supports `AF_INET`; IPv6 is not handled here.
 *
 * Main uses:
 * - Creating the PostgreSQL-wire admin listener.
 * - Creating the Prometheus HTTP listener.
 */
static int create_listen_fd(const char *addr, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in sa = {
        .sin_family = AF_INET,
        .sin_port   = htons(port),
    };
    inet_pton(AF_INET, addr, &sa.sin_addr);

    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 8) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* ============================================================================
 * PG Wire Protocol — Response builders
 *
 * Layout refresher:
 *   RowDescription     'T' int32(len) int16(ncols) { name\0 int32(tableoid)
 *                       int16(colno) int32(typeoid) int16(typlen) int32(typmod)
 *                       int16(fmt) } ...
 *   DataRow            'D' int32(len) int16(ncols) { int32(collen) bytes } ...
 *   CommandComplete    'C' int32(len) tag\0
 *   ReadyForQuery      'Z' int32(5) byte('I')
 *   AuthenticationOk   'R' int32(8) int32(0)
 *   ParameterStatus    'S' int32(len) name\0 value\0
 *   ErrorResponse      'E' int32(len) 'S' sev\0 'V' sev\0 'C' code\0 'M' msg\0 \0
 * ============================================================================ */

/* Dynamic buffer for building PG messages */
typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} pgbuf_t;

/**
 * @brief Initialize an empty dynamic PostgreSQL message buffer.
 *
 * @param[out] b Buffer state to initialize.
 * @return Nothing.
 *
 * @note The buffer starts detached from heap storage and grows lazily.
 */
static void pgbuf_init(pgbuf_t *b) { b->data = NULL; b->len = 0; b->cap = 0; }

/**
 * @brief Release a PostgreSQL message buffer and reset it to the empty state.
 *
 * @param[in,out] b Buffer to release.
 * @return Nothing.
 *
 * @note Safe to call on an already-empty buffer.
 */
static void pgbuf_free(pgbuf_t *b) { keel_free(b->data); b->data = NULL; b->len = 0; b->cap = 0; }

/**
 * @brief Ensure a PostgreSQL buffer has room for additional bytes.
 *
 * @param[in,out] b Buffer to grow.
 * @param need Additional bytes the caller intends to append.
 * @return Nothing.
 *
 * Corner cases:
 * - Starts at 4 KiB for the first allocation and doubles thereafter.
 * - Assumes `keel_realloc()` succeeds or aborts via the project's allocator
 *   policy; there is no local recovery path.
 *
 * Main uses:
 * - Every admin response builder funnels allocation growth through here.
 */
static void pgbuf_ensure(pgbuf_t *b, size_t need) {
    if (b->len + need <= b->cap) return;
    size_t newcap = b->cap ? b->cap * 2 : 4096;
    while (newcap < b->len + need) newcap *= 2;
    b->data = keel_realloc(b->data, newcap);
    b->cap = newcap;
}

/**
 * @brief Append raw bytes to a PostgreSQL message buffer.
 *
 * @param[in,out] b Destination buffer.
 * @param p Source bytes.
 * @param n Number of bytes to append.
 * @return Nothing.
 */
static void pgbuf_add(pgbuf_t *b, const void *p, size_t n) {
    pgbuf_ensure(b, n);
    memcpy(b->data + b->len, p, n);
    b->len += n;
}

/**
 * @brief Append one byte to a PostgreSQL message buffer.
 *
 * @param[in,out] b Destination buffer.
 * @param c Byte value to append.
 * @return Nothing.
 */
static void pgbuf_addbyte(pgbuf_t *b, uint8_t c) { pgbuf_ensure(b, 1); b->data[b->len++] = c; }

/**
 * @brief Append a 16-bit network-order integer to a PostgreSQL message buffer.
 *
 * @param[in,out] b Destination buffer.
 * @param v Host-order value to append.
 * @return Nothing.
 */
static void pgbuf_add16(pgbuf_t *b, uint16_t v) {
    uint8_t t[2]; wr16(t, v); pgbuf_add(b, t, 2);
}
/**
 * @brief Append a 32-bit network-order integer to a PostgreSQL message buffer.
 *
 * @param[in,out] b Destination buffer.
 * @param v Host-order value to append.
 * @return Nothing.
 */
static void pgbuf_add32(pgbuf_t *b, uint32_t v) {
    uint8_t t[4]; wr32(t, v); pgbuf_add(b, t, 4);
}
/**
 * @brief Append a NUL-terminated C string including its terminator.
 *
 * @param[in,out] b Destination buffer.
 * @param s String to append.
 * @return Nothing.
 *
 * @note PostgreSQL protocol fields frequently use C-string framing, so this
 *       helper keeps builders concise and consistent.
 */
static void pgbuf_addstr(pgbuf_t *b, const char *s) {
    size_t n = strlen(s) + 1; pgbuf_add(b, s, n);
}

/**
 * @brief Append a PostgreSQL `AuthenticationOk` message.
 *
 * @param[in,out] b Destination buffer.
 * @return Nothing.
 *
 * Main uses:
 * - Sent after the admin endpoint accepts any startup packet. The admin
 *   console currently trusts all clients that can connect to the socket.
 */
static void pg_auth_ok(pgbuf_t *b) {
    pgbuf_addbyte(b, 'R');
    pgbuf_add32(b, 8);     /* length incl self */
    pgbuf_add32(b, 0);     /* AuthenticationOk */
}

/**
 * @brief Append a PostgreSQL `ParameterStatus` message.
 *
 * @param[in,out] b Destination buffer.
 * @param name Parameter name.
 * @param val Parameter value.
 * @return Nothing.
 *
 * Corner cases:
 * - Both strings must be valid NUL-terminated strings.
 * - The function does not validate semantic correctness of the pair; callers
 *   decide which session parameters to advertise.
 */
static void pg_param_status(pgbuf_t *b, const char *name, const char *val) {
    uint32_t len = 4 + (uint32_t)strlen(name) + 1 + (uint32_t)strlen(val) + 1;
    pgbuf_addbyte(b, 'S');
    pgbuf_add32(b, len);
    pgbuf_addstr(b, name);
    pgbuf_addstr(b, val);
}

/**
 * @brief Append a synthetic `BackendKeyData` message.
 *
 * @param[in,out] b Destination buffer.
 * @return Nothing.
 *
 * @note The admin endpoint does not support cancellation, so the secret key is
 *       intentionally fake. The message is emitted for protocol compatibility.
 */
static void pg_backend_key(pgbuf_t *b) {
    pgbuf_addbyte(b, 'K');
    pgbuf_add32(b, 12);
    pgbuf_add32(b, (uint32_t)getpid());
    pgbuf_add32(b, 0);
}

/**
 * @brief Append `ReadyForQuery` with idle transaction status.
 *
 * @param[in,out] b Destination buffer.
 * @return Nothing.
 *
 * Main uses:
 * - Ends startup handshake.
 * - Terminates each simple-query response cycle.
 */
static void pg_ready(pgbuf_t *b) {
    pgbuf_addbyte(b, 'Z');
    pgbuf_add32(b, 5);
    pgbuf_addbyte(b, 'I');
}

/**
 * @brief Build a `RowDescription` describing a text-only result set.
 *
 * @param[in,out] b Destination buffer.
 * @param cols Column names.
 * @param ncols Number of columns.
 * @return Nothing.
 *
 * Design notes:
 * - Every admin result column is exposed as PostgreSQL `TEXT` for simplicity.
 * - Table metadata is intentionally zeroed because these results do not map to
 *   real relations.
 *
 * Corner cases:
 * - `ncols` is assumed non-negative and small enough that the payload length
 *   remains within the protocol's 32-bit message framing.
 */
static void pg_row_desc(pgbuf_t *b, const char **cols, int ncols) {
    /* Calculate payload length */
    uint32_t payload = 2;  /* int16 ncols */
    for (int i = 0; i < ncols; i++) {
        payload += (uint32_t)strlen(cols[i]) + 1; /* name\0 */
        payload += 18; /* tableoid(4) + colno(2) + typeoid(4) + typlen(2) + typmod(4) + fmt(2) */
    }
    pgbuf_addbyte(b, 'T');
    pgbuf_add32(b, 4 + payload);
    pgbuf_add16(b, (uint16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        pgbuf_addstr(b, cols[i]);
        pgbuf_add32(b, 0);    /* table OID */
        pgbuf_add16(b, 0);    /* column number */
        pgbuf_add32(b, 25);   /* type OID = TEXT */
        pgbuf_add16(b, (uint16_t)-1); /* type length */
        pgbuf_add32(b, (uint32_t)-1); /* type modifier */
        pgbuf_add16(b, 0);    /* format = text */
    }
}

/**
 * @brief Append one text-mode PostgreSQL `DataRow`.
 *
 * @param[in,out] b Destination buffer.
 * @param vals Column values for the row.
 * @param ncols Number of values to encode.
 * @return Nothing.
 *
 * Corner cases:
 * - Values are treated as non-NULL, already-NUL-terminated text.
 * - Binary format and PostgreSQL NULL semantics are intentionally unsupported
 *   because admin output is simple text diagnostics.
 */
static void pg_data_row(pgbuf_t *b, const char **vals, int ncols) {
    /* pre-compute length */
    uint32_t payload = 2; /* int16 ncols */
    for (int i = 0; i < ncols; i++)
        payload += 4 + (uint32_t)strlen(vals[i]);
    pgbuf_addbyte(b, 'D');
    pgbuf_add32(b, 4 + payload);
    pgbuf_add16(b, (uint16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        uint32_t slen = (uint32_t)strlen(vals[i]);
        pgbuf_add32(b, slen);
        pgbuf_add(b, vals[i], slen);
    }
}

/**
 * @brief Append a generic `CommandComplete` tag for `SHOW` commands.
 *
 * @param[in,out] b Destination buffer.
 * @param nrows Number of rows returned.
 * @return Nothing.
 *
 * @note The tag uses `SHOW <nrows>` even for internal commands because clients
 *       only require a valid completion tag, not exact SQL semantics.
 */
static void pg_cmd_complete(pgbuf_t *b, int nrows) {
    char tag[64];
    snprintf(tag, sizeof(tag), "SHOW %d", nrows);
    uint32_t len = 4 + (uint32_t)strlen(tag) + 1;
    pgbuf_addbyte(b, 'C');
    pgbuf_add32(b, len);
    pgbuf_addstr(b, tag);
}

/**
 * @brief Append a PostgreSQL `ErrorResponse` message.
 *
 * @param[in,out] b Destination buffer.
 * @param msg Human-readable error text.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Invalid commands.
 * - Missing statistics collectors.
 * - Invalid server indexes and unsafe operations.
 *
 * @note The SQLSTATE is hard-coded to `42601` for simplicity even when the
 *       failure is operational rather than syntactic.
 */
static void pg_error(pgbuf_t *b, const char *msg) {
    uint32_t mlen = (uint32_t)strlen(msg) + 1;
    /* S severity + V severity + C code + M message + terminator */
    uint32_t len = 4 + 2 + 6 + 2 + 6 + 2 + 6 + 2 + mlen + 1;
    pgbuf_addbyte(b, 'E');
    pgbuf_add32(b, len);
    pgbuf_addbyte(b, 'S'); pgbuf_addstr(b, "ERROR");
    pgbuf_addbyte(b, 'V'); pgbuf_addstr(b, "ERROR");
    pgbuf_addbyte(b, 'C'); pgbuf_addstr(b, "42601");
    pgbuf_addbyte(b, 'M'); pgbuf_addstr(b, msg);
    pgbuf_addbyte(b, '\0');
}

/* ============================================================================
 * JSON output helpers (for FORMAT JSON mode)
 * ============================================================================ */

/* Build a JSON array-of-objects string from column names + row data.
 * Caller must keel_free() the returned string. */
typedef struct json_builder {
    char  *buf;
    size_t len;
    size_t cap;
    int    ncols;
    const char **cols;
    int    nrows;
} json_builder_t;

/**
 * @brief Initialize a JSON array-of-objects builder for a tabular result set.
 *
 * @param[out] jb Builder state.
 * @param cols Column names describing object keys.
 * @param ncols Number of columns in the source table.
 * @return Nothing.
 */
static void jb_init(json_builder_t *jb, const char **cols, int ncols) {
    jb->cap = 4096;
    jb->buf = keel_calloc(1, jb->cap);
    jb->len = 0;
    jb->ncols = ncols;
    jb->cols = cols;
    jb->nrows = 0;
    jb->len += (size_t)snprintf(jb->buf + jb->len, jb->cap - jb->len, "[");
}

/**
 * @brief Ensure the JSON builder has space for additional serialized text.
 *
 * @param[in,out] jb Builder state.
 * @param need Estimated extra bytes required.
 * @return Nothing.
 *
 * @note Reallocation copies existing content into a fresh zeroed buffer. This
 *       is not the most allocation-efficient approach, but admin JSON output is
 *       small and infrequent.
 */
static void jb_ensure(json_builder_t *jb, size_t need) {
    while (jb->len + need >= jb->cap) {
        jb->cap *= 2;
        char *nb = keel_calloc(1, jb->cap);
        memcpy(nb, jb->buf, jb->len);
        keel_free(jb->buf);
        jb->buf = nb;
    }
}

/**
 * @brief Append one JSON string literal, escaping control characters as needed.
 *
 * @param[in,out] jb Builder state.
 * @param s Source string, or `NULL` for JSON `null`.
 * @return Nothing.
 *
 * Corner cases:
 * - Only a minimal escape set is handled because admin output is textual and
 *   controlled; this is sufficient for quotes, backslashes, newlines, and tabs.
 */
static void jb_add_escaped(json_builder_t *jb, const char *s) {
    if (!s) { jb_ensure(jb, 5); jb->len += (size_t)snprintf(jb->buf + jb->len, jb->cap - jb->len, "null"); return; }
    jb_ensure(jb, strlen(s) * 2 + 3);
    jb->buf[jb->len++] = '"';
    for (const char *p = s; *p; p++) {
        if (*p == '"' || *p == '\\') { jb->buf[jb->len++] = '\\'; jb->buf[jb->len++] = *p; }
        else if (*p == '\n') { jb->buf[jb->len++] = '\\'; jb->buf[jb->len++] = 'n'; }
        else if (*p == '\t') { jb->buf[jb->len++] = '\\'; jb->buf[jb->len++] = 't'; }
        else jb->buf[jb->len++] = *p;
    }
    jb->buf[jb->len++] = '"';
    jb->buf[jb->len] = '\0';
}

/**
 * @brief Append one row object to the JSON array.
 *
 * @param[in,out] jb Builder state.
 * @param vals Row values.
 * @param ncols Number of values available in `vals`.
 * @return Nothing.
 *
 * Corner cases:
 * - If a row exposes fewer columns than the header, trailing columns are
 *   omitted from the JSON object.
 */
static void jb_add_row(json_builder_t *jb, const char **vals, int ncols) {
    jb_ensure(jb, 256);
    if (jb->nrows > 0) jb->buf[jb->len++] = ',';
    jb->buf[jb->len++] = '{';
    for (int i = 0; i < ncols && i < jb->ncols; i++) {
        if (i > 0) jb->buf[jb->len++] = ',';
        jb_add_escaped(jb, jb->cols[i]);
        jb->buf[jb->len++] = ':';
        jb_add_escaped(jb, vals[i]);
    }
    jb_ensure(jb, 2);
    jb->buf[jb->len++] = '}';
    jb->buf[jb->len] = '\0';
    jb->nrows++;
}

/**
 * @brief Finalize the JSON array and return ownership of the heap buffer.
 *
 * @param[in,out] jb Builder state.
 * @return Heap-allocated JSON text owned by the caller.
 */
static char *jb_finish(json_builder_t *jb) {
    jb_ensure(jb, 2);
    jb->buf[jb->len++] = ']';
    jb->buf[jb->len] = '\0';
    return jb->buf;
}

/**
 * @brief Wrap JSON text as a one-column PostgreSQL result set.
 *
 * @param[in,out] b Destination PG wire buffer.
 * @param json_str Heap-allocated JSON string; ownership is transferred.
 * @param nrows Number of logical rows represented in the JSON payload.
 * @return Nothing.
 *
 * @note The admin protocol still returns a normal PostgreSQL result set even in
 *       JSON mode; the JSON is exposed inside a single `json` column.
 */
static void pg_json_result(pgbuf_t *b, char *json_str, int nrows) {
    const char *cols[] = { "json" };
    pg_row_desc(b, cols, 1);
    const char *vals[] = { json_str };
    pg_data_row(b, vals, 1);
    pg_cmd_complete(b, nrows);
    keel_free(json_str);
}

/**
 * @brief Convert a previously built PostgreSQL tabular result into JSON form.
 *
 * @param[in,out] dst Destination buffer that receives the JSON-wrapped result.
 * @param src Source buffer containing a PostgreSQL wire result.
 * @return Nothing.
 *
 * Behavior:
 * - Parses `RowDescription` and subsequent `DataRow` frames.
 * - Re-serializes rows into a JSON array of objects.
 * - Emits a new single-column PG result containing the JSON text.
 * - Falls back to copying `src` unchanged if the input is not the expected
 *   shape, which preserves error messages and unsupported responses.
 *
 * Corner cases:
 * - Assumes a small column count and bounded row values suitable for admin use.
 * - Long values may rely on existing NUL termination in the source buffer.
 */
static void pgbuf_to_json(pgbuf_t *dst, pgbuf_t *src) {
    const uint8_t *p = (const uint8_t *)src->data;
    const uint8_t *end = p + src->len;

    /* First message should be 'T' RowDescription.  If not, pass through. */
    if (p >= end || *p != 'T') {
        pgbuf_ensure(dst, src->len);
        memcpy(dst->data + dst->len, src->data, src->len);
        dst->len += src->len;
        return;
    }

    /* Parse RowDescription: 'T' int32(len) int16(ncols) { str col_name, ... } */
    p++; /* skip 'T' */
    if (p + 4 > end) goto passthrough;
    uint32_t rdlen = rd32(p); p += 4;
    const uint8_t *rd_end = p + rdlen - 4;
    if (rd_end > end) goto passthrough;

    int16_t ncols = (int16_t)((p[0] << 8) | p[1]); p += 2;
    if (ncols <= 0 || ncols > 256) goto passthrough;

    /* Collect column names */
    const char *col_names[256];
    for (int i = 0; i < ncols; i++) {
        col_names[i] = (const char *)p;
        while (p < rd_end && *p) p++;
        p++; /* skip NUL */
        p += 18; /* skip table_oid(4) + col_attr(2) + type_oid(4) + type_len(2) + type_mod(4) + fmt(2) */
        if (p > rd_end) goto passthrough;
    }

    /* Build JSON */
    json_builder_t jb;
    jb_init(&jb, col_names, ncols);

    /* Parse DataRow messages: 'D' int32(len) int16(ncols) { int32(vlen) bytes } */
    while (p < end && *p == 'D') {
        p++; /* skip 'D' */
        if (p + 4 > end) break;
        uint32_t dlen = rd32(p); p += 4;
        const uint8_t *row_end = p + dlen - 4;
        if (row_end > end) break;

        int16_t rncols = (int16_t)((p[0] << 8) | p[1]); p += 2;
        const char *vals[256];
        char val_bufs[256][128]; /* temporary storage for non-null terminated */
        for (int i = 0; i < rncols && i < ncols; i++) {
            if (p + 4 > row_end) { vals[i] = ""; continue; }
            int32_t vlen = (int32_t)rd32(p); p += 4;
            if (vlen < 0) { vals[i] = NULL; continue; }
            if (p + vlen > row_end) { vals[i] = ""; p = row_end; break; }
            if ((size_t)vlen < sizeof(val_bufs[i])) {
                memcpy(val_bufs[i], p, (size_t)vlen);
                val_bufs[i][vlen] = '\0';
                vals[i] = val_bufs[i];
            } else {
                vals[i] = (const char *)p; /* assume NUL-terminated from pg_data_row */
            }
            p += vlen;
        }
        jb_add_row(&jb, vals, rncols < ncols ? rncols : ncols);
        p = row_end;
    }

    char *json = jb_finish(&jb);
    pg_json_result(dst, json, jb.nrows);
    return;

passthrough:
    pgbuf_ensure(dst, src->len);
    memcpy(dst->data + dst->len, src->data, src->len);
    dst->len += src->len;
}

/* ============================================================================
 * SHOW command handlers
 * ============================================================================ */

/**
 * @brief Format an unsigned 64-bit integer into a caller-supplied buffer.
 *
 * @param[out] buf Output buffer.
 * @param sz Buffer size in bytes.
 * @param v Value to format.
 * @return `buf` for convenient inline use.
 */
static const char *fmt_u64(char *buf, size_t sz, uint64_t v) {
    snprintf(buf, sz, "%llu", (unsigned long long)v);
    return buf;
}
/**
 * @brief Format a signed 64-bit integer into a caller-supplied buffer.
 *
 * @param[out] buf Output buffer.
 * @param sz Buffer size in bytes.
 * @param v Value to format.
 * @return `buf` for convenient inline use.
 */
static const char *fmt_i64(char *buf, size_t sz, int64_t v) {
    snprintf(buf, sz, "%lld", (long long)v);
    return buf;
}
/**
 * @brief Format a double with fixed two-decimal precision.
 *
 * @param[out] buf Output buffer.
 * @param sz Buffer size in bytes.
 * @param v Value to format.
 * @return `buf` for convenient inline use.
 */
static const char *fmt_dbl(char *buf, size_t sz, double v) {
    snprintf(buf, sz, "%.2f", v);
    return buf;
}

/**
 * @brief Read the current monotonic time in nanoseconds.
 *
 * @return Monotonic clock value in nanoseconds.
 *
 * Main uses:
 * - Computing client session age.
 * - Detecting stale worker heartbeats in metrics output.
 */
static uint64_t admin_now_ns(void) { return (uint64_t)keel_time_now(); }

/**
 * @brief Emit the built-in admin command reference.
 *
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Main uses:
 * - Returned for `SHOW HELP`.
 * - Serves as the canonical discoverability surface for operators using `psql`.
 */
static void show_help(pgbuf_t *b) {
    const char *cols[] = { "command", "description" };
    pg_row_desc(b, cols, 2);

    const char *rows[][2] = {
        { "SHOW HELP",         "This help message" },
        { "SHOW VERSION",      "KEEL version string" },
        { "SHOW STATS",        "Aggregated L1 counters" },
        { "SHOW STATS_DETAIL", "Per-worker L1 breakdown" },
        { "SHOW SERVERS",      "Configured backend servers with health status" },
        { "SHOW POOLS",        "Backend pool connections (active/idle/waiting)" },
        { "SHOW CLIENTS",      "Individual frontend session detail" },
        { "SHOW CONFIG",       "Running configuration" },
        { "SHOW LATENCY",      "Latency histograms (if level >= extended)" },
        { "SHOW SYSTEM",       "CPU / RSS / fd stats (if level >= system)" },
        { "SHOW REBALANCE",    "Per-worker load and migration stats" },
        { "SHOW SHARD RULES",  "Registered shard routing rules (table/column/count)" },
        { "SHOW QUERY RULES",  "Declarative query routing/blocking/rewriting rules" },
        { "SHOW CACHE STATS",  "Per-worker query cache hit/miss/eviction counters" },
        { "SHOW THROTTLE RULES", "Rate-limiting rules with per-rule rejection counters" },
        { "SHOW TOPOLOGY",     "Cluster topology discovered by the discovery subsystem" },
        { "FLUSH QUERY CACHE", "Flush all per-worker query result caches immediately" },
        { "SHOW OSC SESSIONS", "Active Online Schema Change tool sessions (gh-ost/pt-osc)" },
        { "PAUSE",             "Pause all pools (drain idle, reject new borrows)" },
        { "RESUME",            "Resume all pools after PAUSE" },
        { "DISABLE SERVER <n>","Mark server <n> unhealthy (0-indexed)" },
        { "ENABLE SERVER <n>", "Mark server <n> healthy (0-indexed)" },
        { "ADD SERVER <def>",  "Add backend at runtime (host=... port=... role=... weight=...)" },
        { "REMOVE SERVER <n>", "Remove backend server <n> (0-indexed)" },
        { "KILL CLIENT <id>",  "Force-disconnect client session by ID" },
        { "DRAIN [timeout_ms]","Initiate graceful drain (optional timeout)" },
        { "SET <key> = <val>", "Change runtime setting (pool sizes, timeouts, stats)" },
        { "RELOAD",            "Reload configuration (TLS certs, log level)" },
        { "RESTART WORKERS [timeout_ms]", "Rolling restart of all worker threads" },
        { "SHOW CLUSTER",     "Cluster peer table (requires cluster mode)" },
        { "SHOW CLUSTER CONFIG", "Local runtime cluster config snapshot" },
        { "SHOW DISCOVERED PEERS", "Cluster peers discovered via JOIN/GOSSIP" },
        { "SHOW CLUSTER STATS","Cluster-wide statistics" },
        { "ADD PEER <host:port>","Add a cluster peer at runtime" },
        { "REMOVE PEER <host:port>","Remove a cluster peer" },
        { "SHOW TRACING",      "Tracing runtime status, sample rate, export stats" },
        { "SET tracing = on|off", "Enable/disable tracing at runtime" },
        { "SET trace_sample_rate = <ppm>", "Change sample rate (parts-per-million)" },
        { "SHOW CERTIFICATES", "TLS certificate details (subject, issuer, validity, fingerprint)" },
        /* SQL-syntax alternatives (ProxySQL-style) */
        { "SELECT * FROM <table>", "SQL read: stats, servers, pools, clients, config, "
                    "latency, system, rebalance, cluster, cluster_config, discovered_peers, cluster_stats, tracing, help, version" },
        { "UPDATE config SET value='v' WHERE key='k'", "SQL equivalent of SET <k> = <v>" },
        { "UPDATE servers SET enabled=true WHERE index=N", "SQL equivalent of ENABLE/DISABLE SERVER" },
        { "INSERT INTO servers (host,port,...) VALUES (...)", "SQL equivalent of ADD SERVER" },
        { "INSERT INTO peers (host) VALUES ('host:port')", "SQL equivalent of ADD PEER" },
        { "DELETE FROM servers WHERE index=N", "SQL equivalent of REMOVE SERVER" },
        { "DELETE FROM clients WHERE id=N", "SQL equivalent of KILL CLIENT" },
        { "DELETE FROM peers WHERE host='host:port'", "SQL equivalent of REMOVE PEER" },
    };
    int nrows = (int)(sizeof(rows) / sizeof(rows[0]));
    for (int i = 0; i < nrows; i++)
        pg_data_row(b, rows[i], 2);
    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit the KEEL version string as a one-row result.
 *
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 */
static void show_version(pgbuf_t *b) {
    const char *cols[] = { "version" };
    pg_row_desc(b, cols, 1);
    const char *vals[] = { "KEEL " KEEL_VERSION };
    pg_data_row(b, vals, 1);
    pg_cmd_complete(b, 1);
}

/**
 * @brief Emit aggregate runtime counters across all workers.
 *
 * @param admin Admin subsystem handle used to reach engine state.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Returns a PostgreSQL error response when the stats collector is disabled or
 *   unavailable.
 *
 * Main uses:
 * - `SHOW STATS` in the admin console.
 * - Human inspection of counters mirrored by Prometheus.
 *
 * Implementation notes:
 * - Takes a point-in-time snapshot from the stats collector.
 * - Mixes snapshot counters with global TLS/kTLS counters queried live.
 * - Uses macro helpers to keep the large row list mechanically consistent.
 */
static void show_stats(keel_admin_t *admin, pgbuf_t *b) {
    keel_stats_collector_t *sc = keel_engine_get_stats_collector(admin->engine);
    if (!sc) {
        pg_error(b, "Stats collector not available (stats_level = off?)");
        return;
    }

    keel_stats_snapshot_t snap;
    keel_stats_snapshot_take(sc, &snap);

    const char *cols[] = { "stat", "value" };
    pg_row_desc(b, cols, 2);

    char vbuf[32];
    int nrows = 0;
    keel_tls_stats_t tls_stats = keel_tls_get_stats();
    keel_ktls_stats_t ktls_stats = keel_ktls_get_stats();

#define ROW_COUNTER(name, fld) do { \
        const char *row[] = { name, fmt_u64(vbuf, sizeof(vbuf), keel_counter_get(&snap.basic.fld)) }; \
        pg_data_row(b, row, 2); nrows++; \
    } while (0)

#define ROW_GAUGE(name, fld) do { \
        const char *row[] = { name, fmt_i64(vbuf, sizeof(vbuf), keel_gauge_get(&snap.basic.fld)) }; \
        pg_data_row(b, row, 2); nrows++; \
    } while (0)

    ROW_COUNTER("sessions_created",  sessions_created);
    ROW_COUNTER("sessions_closed",   sessions_closed);
    ROW_GAUGE  ("sessions_active",   sessions_active);
    ROW_COUNTER("pool_borrows",      pool_borrows);
    ROW_COUNTER("pool_returns",      pool_returns);
    ROW_COUNTER("pool_hits",         pool_hits);
    ROW_COUNTER("pool_misses",       pool_misses);
    ROW_COUNTER("pool_creates",      pool_creates);
    ROW_COUNTER("pool_destroys",     pool_destroys);
    ROW_COUNTER("pool_borrow_attempts",          pool_borrow_attempts);
    ROW_COUNTER("pool_borrow_exact_state_match", pool_borrow_exact_state_match);
    ROW_COUNTER("pool_borrow_exact_stmt_match",  pool_borrow_exact_stmt_match);
    ROW_COUNTER("pool_borrow_state_replay",      pool_borrow_state_replay);
    ROW_COUNTER("pool_borrow_stmt_replay",       pool_borrow_stmt_replay);
    ROW_COUNTER("pool_borrow_cleanup_required",  pool_borrow_cleanup_required);
    ROW_COUNTER("backend_borrow_success",        backend_borrow_success);
    ROW_COUNTER("backend_borrow_failed_incompatible", backend_borrow_failed_incompatible);
    ROW_COUNTER("backend_borrow_failed_quarantined",  backend_borrow_failed_quarantined);
    ROW_COUNTER("queries_total",     queries_total);
    ROW_COUNTER("queries_read",      queries_read);
    ROW_COUNTER("queries_write",     queries_write);
    ROW_COUNTER("queries_tx",        queries_tx);
    ROW_COUNTER("errors_total",      errors_total);
    ROW_COUNTER("errors_auth",       errors_auth);
    ROW_COUNTER("errors_proto",      errors_proto);
    ROW_COUNTER("errors_backend",    errors_backend);
    ROW_COUNTER("errors_timeout",    errors_timeout);
    ROW_COUNTER("bytes_recv",        bytes_recv);
    ROW_COUNTER("bytes_sent",        bytes_sent);
    ROW_COUNTER("bytes_backend_recv",bytes_backend_recv);
    ROW_COUNTER("bytes_backend_sent",bytes_backend_sent);
    ROW_COUNTER("bytes_spliced",     bytes_spliced);
    ROW_COUNTER("loop_iterations",   loop_iterations);
    ROW_COUNTER("ops_submitted",     ops_submitted);
    ROW_COUNTER("ops_completed",     ops_completed);
    ROW_COUNTER("flow_wait_pool_events",      flow_wait_pool_events);
    ROW_COUNTER("flow_wait_pool_ns_total",    flow_wait_pool_ns_total);
    ROW_COUNTER("flow_wait_backend_events",   flow_wait_backend_events);
    ROW_COUNTER("flow_wait_backend_ns_total", flow_wait_backend_ns_total);
    ROW_COUNTER("flow_wait_backend_query_events",   flow_wait_backend_query_events);
    ROW_COUNTER("flow_wait_backend_query_ns_total", flow_wait_backend_query_ns_total);
    ROW_COUNTER("flow_wait_backend_query_exec_ns_total", flow_wait_backend_query_exec_ns_total);
    ROW_COUNTER("flow_wait_backend_query_io_ns_total", flow_wait_backend_query_io_ns_total);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ns_total", flow_wait_backend_query_io_reactor_ns_total);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_ns_total", flow_wait_backend_query_io_reactor_ready_ns_total);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_wakeup_ns_total", flow_wait_backend_query_io_reactor_ready_wakeup_ns_total);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_sched_ns_total", flow_wait_backend_query_io_reactor_ready_sched_ns_total);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_sched_head_ns_total", flow_wait_backend_query_io_reactor_ready_sched_head_ns_total);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_sched_tail_ns_total", flow_wait_backend_query_io_reactor_ready_sched_tail_ns_total);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_sched_batch_size_sum", flow_wait_backend_query_io_reactor_ready_sched_batch_size_sum);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_sched_batch_index_sum", flow_wait_backend_query_io_reactor_ready_sched_batch_index_sum);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_sched_batch_size_1_events", flow_wait_backend_query_io_reactor_ready_sched_batch_size_1_events);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_sched_batch_size_2_events", flow_wait_backend_query_io_reactor_ready_sched_batch_size_2_events);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_sched_batch_size_3_events", flow_wait_backend_query_io_reactor_ready_sched_batch_size_3_events);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_ready_sched_batch_size_4p_events", flow_wait_backend_query_io_reactor_ready_sched_batch_size_4p_events);
    ROW_COUNTER("flow_wait_backend_query_io_reactor_dispatch_ns_total", flow_wait_backend_query_io_reactor_dispatch_ns_total);
    ROW_COUNTER("flow_wait_backend_query_io_service_ns_total", flow_wait_backend_query_io_service_ns_total);
    ROW_COUNTER("flow_wait_backend_query_framing_ns_total", flow_wait_backend_query_framing_ns_total);
    ROW_COUNTER("flow_wait_backend_query_deferred_send_events", flow_wait_backend_query_deferred_send_events);
    ROW_COUNTER("flow_wait_backend_query_deferred_send_ns_total", flow_wait_backend_query_deferred_send_ns_total);
    ROW_COUNTER("flow_wait_backend_replay_events",  flow_wait_backend_replay_events);
    ROW_COUNTER("flow_wait_backend_replay_ns_total",flow_wait_backend_replay_ns_total);
    ROW_COUNTER("flow_wait_backend_discard_events", flow_wait_backend_discard_events);
    ROW_COUNTER("flow_wait_backend_discard_ns_total",flow_wait_backend_discard_ns_total);
    ROW_COUNTER("pool_wait_queue_enqueued",     pool_wait_queue_enqueued);
    ROW_COUNTER("pool_wait_queue_full_rejects", pool_wait_queue_full_rejects);
    ROW_COUNTER("pool_wait_resume_success",     pool_wait_resume_success);
    ROW_COUNTER("pool_wait_resume_requeues",    pool_wait_resume_requeues);
    ROW_COUNTER("pool_wait_timeout_events",     pool_wait_timeout_events);
    ROW_COUNTER("pool_wait_cancelled",          pool_wait_cancelled);
    ROW_COUNTER("proxy_state_desync_total", proxy_state_desync_total);
    ROW_COUNTER("proxy_orphaned_transactions_total", proxy_orphaned_transactions_total);
    ROW_COUNTER("proxy_backend_reuse_failure_total", proxy_backend_reuse_failure_total);
    ROW_COUNTER("proxy_io_uring_sq_overflow_total", proxy_io_uring_sq_overflow_total);
    ROW_COUNTER("discard_all_count", discard_all_count);
    ROW_COUNTER("discard_all_failure", discard_all_failure);
    ROW_COUNTER("state_sync_count", state_sync_count);
    ROW_COUNTER("backend_close_dead_idle", backend_close_dead_idle);
    ROW_COUNTER("backend_close_cleanup_error", backend_close_cleanup_error);
    ROW_COUNTER("backend_close_cleanup_timeout", backend_close_cleanup_timeout);
    ROW_COUNTER("backend_close_client_disconnect", backend_close_client_disconnect);
    ROW_COUNTER("cleaning_timeout_total", cleaning_timeout_total);
    ROW_COUNTER("pin_reason_transaction", pin_reason_transaction);
    ROW_COUNTER("pin_reason_extended_protocol", pin_reason_extended_protocol);
    ROW_COUNTER("pin_reason_prepared_stmt", pin_reason_prepared_stmt);
    ROW_COUNTER("pin_reason_other", pin_reason_other);
    ROW_COUNTER("commit_in_doubt_started", commit_in_doubt_started);
    ROW_COUNTER("commit_in_doubt_resolved", commit_in_doubt_resolved);
    ROW_COUNTER("commit_in_doubt_failed", commit_in_doubt_failed);
    ROW_COUNTER("notify_relayed",     notify_relayed);
    ROW_COUNTER("osc_sessions_detected", osc_sessions_detected);
    ROW_GAUGE("sessions_pinned", sessions_pinned);
    ROW_GAUGE("sessions_pinned_transaction", sessions_pinned_transaction);
    ROW_GAUGE("sessions_pinned_extended_protocol", sessions_pinned_extended_protocol);
    ROW_GAUGE("sessions_pinned_prepared_stmt", sessions_pinned_prepared_stmt);
    ROW_GAUGE("sessions_commit_in_doubt", sessions_commit_in_doubt);
    ROW_GAUGE("backends_cleaning", backends_cleaning);
    ROW_GAUGE("proxy_buffer_pool_utilization_bytes", proxy_buffer_pool_utilization_bytes);
    ROW_GAUGE("proxy_connection_age_seconds", proxy_connection_age_seconds);
    ROW_GAUGE("proxy_heartbeat_last_ns", proxy_heartbeat_last_ns);

    /* TLS/kTLS global stats */
    {
        const char *row1[] = { "tls_connections_total", fmt_u64(vbuf, sizeof(vbuf), tls_stats.connections_total) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "tls_connections_succeeded", fmt_u64(vbuf, sizeof(vbuf), tls_stats.connections_succeeded) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "tls_connections_failed", fmt_u64(vbuf, sizeof(vbuf), tls_stats.connections_failed) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "tls_ktls_active", fmt_u64(vbuf, sizeof(vbuf), tls_stats.ktls_active) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "tls_ktls_fallback", fmt_u64(vbuf, sizeof(vbuf), tls_stats.ktls_fallback) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "ktls_installations_attempted", fmt_u64(vbuf, sizeof(vbuf), ktls_stats.installations_attempted) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "ktls_installations_succeeded", fmt_u64(vbuf, sizeof(vbuf), ktls_stats.installations_succeeded) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "ktls_installations_failed", fmt_u64(vbuf, sizeof(vbuf), ktls_stats.installations_failed) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "ktls_cipher_incompatible", fmt_u64(vbuf, sizeof(vbuf), ktls_stats.cipher_incompatible) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "ktls_kernel_errors", fmt_u64(vbuf, sizeof(vbuf), ktls_stats.kernel_errors) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "tls_cert_reloads", fmt_u64(vbuf, sizeof(vbuf), tls_stats.cert_reloads) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "tls_cert_reload_failures", fmt_u64(vbuf, sizeof(vbuf), tls_stats.cert_reload_failures) };
        pg_data_row(b, row1, 2); nrows++;
    }
    {
        const char *row1[] = { "tls_downgrade_rejected", fmt_u64(vbuf, sizeof(vbuf), tls_stats.downgrade_rejected) };
        pg_data_row(b, row1, 2); nrows++;
    }

    /* Uptime */
    {
        double up = (double)snap.uptime_ns / 1.0e9;
        const char *row[] = { "uptime_seconds", fmt_dbl(vbuf, sizeof(vbuf), up) };
        pg_data_row(b, row, 2); nrows++;
    }

#undef ROW_COUNTER
#undef ROW_GAUGE

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit a compact per-worker statistics table.
 *
 * @param admin Admin subsystem handle used to reach engine state.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Returns an error if the stats collector is unavailable.
 *
 * Corner cases:
 * - Workers without an attached stats context are skipped rather than emitted
 *   with empty rows.
 */
static void show_stats_detail(keel_admin_t *admin, pgbuf_t *b) {
    keel_stats_collector_t *sc = keel_engine_get_stats_collector(admin->engine);
    if (!sc) { pg_error(b, "Stats collector not available"); return; }

    uint32_t nw = keel_engine_get_num_workers(admin->engine);

    const char *cols[] = {
        "worker", "sessions_created", "sessions_closed", "sessions_active",
        "bytes_recv", "bytes_sent", "errors_total",
        "loop_iterations", "ops_submitted", "ops_completed"
    };
    int ncols = 10;
    pg_row_desc(b, cols, ncols);

    int nrows = 0;
    char wbuf[16], c1[20], c2[20], c3[20], c4[20], c5[20], c6[20], c7[20], c8[20], c9[20];

    for (uint32_t i = 0; i < nw; i++) {
        keel_stats_ctx_t *ctx = keel_stats_collector_get_ctx(sc, i);
        if (!ctx) continue;

        snprintf(wbuf, sizeof(wbuf), "%u", i);
        fmt_u64(c1, sizeof(c1), keel_counter_get(&ctx->basic.sessions_created));
        fmt_u64(c2, sizeof(c2), keel_counter_get(&ctx->basic.sessions_closed));
        fmt_i64(c3, sizeof(c3), keel_gauge_get(&ctx->basic.sessions_active));
        fmt_u64(c4, sizeof(c4), keel_counter_get(&ctx->basic.bytes_recv));
        fmt_u64(c5, sizeof(c5), keel_counter_get(&ctx->basic.bytes_sent));
        fmt_u64(c6, sizeof(c6), keel_counter_get(&ctx->basic.errors_total));
        fmt_u64(c7, sizeof(c7), keel_counter_get(&ctx->basic.loop_iterations));
        fmt_u64(c8, sizeof(c8), keel_counter_get(&ctx->basic.ops_submitted));
        fmt_u64(c9, sizeof(c9), keel_counter_get(&ctx->basic.ops_completed));

        const char *vals[] = { wbuf, c1, c2, c3, c4, c5, c6, c7, c8, c9 };
        pg_data_row(b, vals, ncols);
        nrows++;
    }

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit the configured backend server list and current health flags.
 *
 * @param admin Admin subsystem handle used to reach server-pool configuration.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Returns an error when no server pool exists or no servers are configured.
 *
 * Main uses:
 * - Verifying runtime topology.
 * - Checking whether manual enable/disable operations took effect.
 */
static void show_servers(keel_admin_t *admin, pgbuf_t *b) {
    keel_server_pool_t *sp = keel_engine_get_server_pool(admin->engine);
    if (!sp || sp->count == 0) {
        pg_error(b, "No servers configured");
        return;
    }

    const char *cols[] = { "index", "host", "port", "role", "weight", "healthy", "database", "user" };
    int ncols = 8;
    pg_row_desc(b, cols, ncols);

    int nrows = 0;
    char ibuf[24], pbuf[24], wbuf[24];

    for (size_t i = 0; i < sp->count; i++) {
        const keel_backend_server_t *srv = &sp->servers[i];
        snprintf(ibuf, sizeof(ibuf), "%zu", i);
        snprintf(pbuf, sizeof(pbuf), "%u", srv->port);
        snprintf(wbuf, sizeof(wbuf), "%u", srv->weight);

        const char *role = (srv->role == KEEL_SERVER_ROLE_RW)   ? "rw" :
                           (srv->role == KEEL_SERVER_ROLE_RO)   ? "ro" :
                           (srv->role == KEEL_SERVER_ROLE_WO)   ? "wo" :
                           (srv->role == KEEL_SERVER_ROLE_AUTO) ? "auto" : "?";

        const char *vals[] = {
            ibuf,
            srv->host     ? srv->host     : "(null)",
            pbuf,
            role,
            wbuf,
            srv->healthy  ? "yes" : "no",
            srv->database ? srv->database : "(null)",
            srv->user     ? srv->user     : "(null)",
        };
        pg_data_row(b, vals, ncols);
        nrows++;
    }

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit per-worker, per-server backend pool state.
 *
 * @param admin Admin subsystem handle used to traverse workers and pools.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Main uses:
 * - Diagnosing pool saturation, pinning, cleaning backlog, and waiter growth.
 *
 * Corner cases:
 * - Missing pool entries are skipped because workers may not have fully built
 *   pool arrays yet for every topology slot.
 */
static void show_pools(keel_admin_t *admin, pgbuf_t *b) {
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    keel_server_pool_t *sp = keel_engine_get_server_pool(admin->engine);

    const char *cols[] = {
        "worker", "server", "host", "port",
        "active", "idle", "clean", "stateful", "dirty", "cleaning",
        "pinned", "waiting", "closed", "total",
        "min", "max"
    };
    int ncols = 16;
    pg_row_desc(b, cols, ncols);

    int nrows = 0;
    char c_worker[24], c_srv[24], c_port[24];
    char c_active[16], c_idle[16], c_clean[16], c_stateful[16];
    char c_dirty[16], c_cleaning[16], c_pinned[16], c_waiting[16];
    char c_closed[16], c_total[16];
    char c_min[16], c_max[16];

    for (uint32_t i = 0; i < nw; i++) {
        const keel_worker_t *w = keel_engine_get_worker(admin->engine, i);
        if (!w) continue;

        snprintf(c_worker, sizeof(c_worker), "%u", i);

        for (size_t s = 0; s < w->server_pool_count; s++) {
            backend_pool_t *pool = w->server_pools ? w->server_pools[s] : NULL;
            if (!pool) continue;

            backend_pool_stats_t st;
            backend_pool_get_stats(pool, &st);

            snprintf(c_srv, sizeof(c_srv), "%zu", s);
            const char *host = "(default)";
            const char *port_s = "";
            if (sp && s < sp->count) {
                host = sp->servers[s].host ? sp->servers[s].host : "(null)";
                snprintf(c_port, sizeof(c_port), "%u", sp->servers[s].port);
                port_s = c_port;
            }

            fmt_u64(c_active,   sizeof(c_active),   st.active_connections);
            fmt_u64(c_idle,     sizeof(c_idle),      st.idle_connections);
            fmt_u64(c_clean,    sizeof(c_clean),     st.clean_connections);
            fmt_u64(c_stateful, sizeof(c_stateful),  st.stateful_connections);
            fmt_u64(c_dirty,    sizeof(c_dirty),     st.dirty_connections);
            fmt_u64(c_cleaning, sizeof(c_cleaning),  st.cleaning_count);
            fmt_u64(c_pinned,   sizeof(c_pinned),    st.pinned_count);
            fmt_u64(c_waiting,  sizeof(c_waiting),   st.waiting_sessions);
            fmt_u64(c_closed,   sizeof(c_closed),    st.closed_connections);
            fmt_u64(c_total,    sizeof(c_total),     st.total_connections);
            fmt_u64(c_min,      sizeof(c_min),       pool->config.min_connections);
            fmt_u64(c_max,      sizeof(c_max),       pool->config.max_connections);

            const char *vals[] = {
                c_worker, c_srv, host, port_s,
                c_active, c_idle, c_clean, c_stateful, c_dirty, c_cleaning,
                c_pinned, c_waiting, c_closed, c_total,
                c_min, c_max
            };
            pg_data_row(b, vals, ncols);
            nrows++;
        }
    }

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit one row per active frontend session across all workers.
 *
 * @param admin Admin subsystem handle used to walk worker session slabs.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Main uses:
 * - Inspecting live sessions before manual intervention.
 * - Locating session IDs for `KILL CLIENT`.
 *
 * Corner cases:
 * - Session slabs are sampled lock-free; rows may reflect a transient view if a
 *   worker is concurrently creating or tearing down a session.
 */
static void show_clients(keel_admin_t *admin, pgbuf_t *b) {
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    uint64_t now = admin_now_ns();

    const char *cols[] = {
        "id", "worker", "username", "database", "state",
        "client_fd", "server_fd", "in_txn", "query_count",
        "age_ms", "tls", "pinned", "pin_reason", "commit_in_doubt"
    };
    int ncols = 14;
    pg_row_desc(b, cols, ncols);

    int nrows = 0;
    char c_id[20], c_wk[24], c_cfd[8], c_sfd[8], c_qc[16], c_age[20];
    char c_pin_reason[24];

    for (uint32_t i = 0; i < nw; i++) {
        const keel_worker_t *w = keel_engine_get_worker(admin->engine, i);
        if (!w) continue;

        snprintf(c_wk, sizeof(c_wk), "%u", i);

        for (size_t s = 0; s < w->sessions.capacity; s++) {
            const keel_session_t *sess = &w->sessions.sessions[s];
            if (sess->client_fd < 0) continue;  /* slot is free */

            fmt_u64(c_id, sizeof(c_id), sess->id);
            snprintf(c_cfd, sizeof(c_cfd), "%d", sess->client_fd);
            snprintf(c_sfd, sizeof(c_sfd), "%d", sess->server_fd);
            fmt_u64(c_qc, sizeof(c_qc), sess->query_count);

            uint64_t age_ms = 0;
            if (sess->created_at > 0 && now > sess->created_at)
                age_ms = (now - sess->created_at) / 1000000ULL;
            fmt_u64(c_age, sizeof(c_age), age_ms);

            const char *state = keel_session_state_name(sess->state);
            const char *tls   = (sess->flags & KEEL_SESSION_FLAG_SSL) ? "yes" : "no";
            const char *txn   = sess->in_transaction ? "yes" : "no";
            const char *pin   = sess->hard_pinned ? "yes" : "no";
            snprintf(c_pin_reason, sizeof(c_pin_reason), "0x%x", sess->pin_reason);
            const char *cid   = sess->commit_in_doubt ? "yes" : "no";

            const char *vals[] = {
                c_id, c_wk,
                sess->username[0] ? sess->username : "(none)",
                sess->database[0] ? sess->database : "(none)",
                state, c_cfd, c_sfd, txn, c_qc, c_age, tls, pin,
                c_pin_reason, cid
            };
            pg_data_row(b, vals, ncols);
            nrows++;
        }
    }

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit a selected subset of the live engine configuration.
 *
 * @param admin Admin subsystem handle used to fetch mutable config.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Returns an error if the engine config handle is unavailable.
 *
 * @note This intentionally exposes operator-relevant settings rather than every
 *       field in `keel_engine_config_t`.
 */
static void show_config(keel_admin_t *admin, pgbuf_t *b) {
    const keel_engine_config_t *cfg = keel_engine_get_config(admin->engine);
    if (!cfg) { pg_error(b, "No config"); return; }

    const char *cols[] = { "key", "value" };
    pg_row_desc(b, cols, 2);

    int nrows = 0;
    char vbuf[64];

#define CFG_ROW(k, v) do { const char *row[] = { k, v }; pg_data_row(b, row, 2); nrows++; } while (0)

    snprintf(vbuf, sizeof(vbuf), "%u", cfg->num_workers); CFG_ROW("num_workers", vbuf);
    CFG_ROW("pin_workers", cfg->pin_workers ? "true" : "false");
    snprintf(vbuf, sizeof(vbuf), "%zu", cfg->session_pool_size); CFG_ROW("session_pool_size", vbuf);
    snprintf(vbuf, sizeof(vbuf), "%zu", cfg->pool_min_size); CFG_ROW("pool_min_size", vbuf);
    snprintf(vbuf, sizeof(vbuf), "%zu", cfg->pool_max_size); CFG_ROW("pool_max_size", vbuf);
    snprintf(vbuf, sizeof(vbuf), "%u", cfg->idle_timeout_ms); CFG_ROW("idle_timeout_ms", vbuf);
    snprintf(vbuf, sizeof(vbuf), "%u", cfg->connect_timeout_ms); CFG_ROW("connect_timeout_ms", vbuf);
    CFG_ROW("stats_level", keel_stats_level_to_str((keel_stats_level_t)cfg->stats_level));
    snprintf(vbuf, sizeof(vbuf), "%u", cfg->stats_interval_ms); CFG_ROW("stats_interval_ms", vbuf);
    CFG_ROW("backend_host", cfg->backend_host ? cfg->backend_host : "(null)");
    snprintf(vbuf, sizeof(vbuf), "%u", cfg->backend_port); CFG_ROW("backend_port", vbuf);
    CFG_ROW("backend_database", cfg->backend_database ? cfg->backend_database : "(null)");

#undef CFG_ROW

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit latency percentile summaries from extended statistics histograms.
 *
 * @param admin Admin subsystem handle used to reach the stats collector.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Returns an error if stats are disabled.
 * - Returns an error when the configured stats level is below `extended`.
 */
static void show_latency(keel_admin_t *admin, pgbuf_t *b) {
    keel_stats_collector_t *sc = keel_engine_get_stats_collector(admin->engine);
    if (!sc) { pg_error(b, "Stats collector not available"); return; }

    keel_stats_snapshot_t snap;
    keel_stats_snapshot_take(sc, &snap);

    if (snap.level < KEEL_STATS_EXTENDED) {
        pg_error(b, "Requires stats level >= extended");
        return;
    }

    const char *cols[] = { "histogram", "p50_ns", "p95_ns", "p99_ns", "count" };
    pg_row_desc(b, cols, 5);

    char p50[20], p95[20], p99[20], cnt[20];
    int nrows = 0;

#define LAT_ROW(name, field) do { \
        fmt_u64(p50, sizeof(p50), keel_histogram_percentile(&snap.extended.field, 0.50)); \
        fmt_u64(p95, sizeof(p95), keel_histogram_percentile(&snap.extended.field, 0.95)); \
        fmt_u64(p99, sizeof(p99), keel_histogram_percentile(&snap.extended.field, 0.99)); \
        fmt_u64(cnt, sizeof(cnt), snap.extended.field.count); \
        const char *row[] = { name, p50, p95, p99, cnt }; \
        pg_data_row(b, row, 5); nrows++; \
    } while (0)

    LAT_ROW("query_latency",   query_latency_ns);
    LAT_ROW("backend_latency", backend_latency_ns);
    LAT_ROW("connect_latency", connect_latency_ns);
    LAT_ROW("session_duration",session_duration_ns);
    LAT_ROW("wait_latency",    wait_latency_ns);

#undef LAT_ROW

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit coarse process-level system metrics.
 *
 * @param admin Admin subsystem handle used to reach the stats collector.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Returns an error if stats are unavailable.
 * - Returns an error when the configured stats level is below `system`.
 *
 * Main uses:
 * - Quick operational triage without attaching an external profiler.
 */
static void show_system(keel_admin_t *admin, pgbuf_t *b) {
    keel_stats_collector_t *sc = keel_engine_get_stats_collector(admin->engine);
    if (!sc) { pg_error(b, "Stats not available"); return; }

    /* Sample fresh system stats before responding */
    keel_stats_sample_system(sc);

    keel_stats_snapshot_t snap;
    keel_stats_snapshot_take(sc, &snap);

    if (snap.level < KEEL_STATS_SYSTEM) {
        pg_error(b, "Requires stats level >= system");
        return;
    }

    const char *cols[] = { "stat", "value" };
    pg_row_desc(b, cols, 2);

    char vbuf[32];
    int nrows = 0;

#define SYS_ROW(name, val) do { const char *row[] = { name, val }; pg_data_row(b, row, 2); nrows++; } while (0)

    SYS_ROW("cpu_user_pct",      fmt_dbl(vbuf, sizeof(vbuf), snap.system.cpu_user_pct));
    SYS_ROW("cpu_sys_pct",       fmt_dbl(vbuf, sizeof(vbuf), snap.system.cpu_sys_pct));
    SYS_ROW("rss_bytes",         fmt_u64(vbuf, sizeof(vbuf), snap.system.rss_bytes));
    SYS_ROW("vm_bytes",          fmt_u64(vbuf, sizeof(vbuf), snap.system.vm_bytes));
    SYS_ROW("fd_open",           fmt_u64(vbuf, sizeof(vbuf), snap.system.fd_open));
    SYS_ROW("fd_limit",          fmt_u64(vbuf, sizeof(vbuf), snap.system.fd_limit));
    SYS_ROW("ctx_switches_vol",  fmt_u64(vbuf, sizeof(vbuf), snap.system.ctx_switches_vol));
    SYS_ROW("ctx_switches_inv",  fmt_u64(vbuf, sizeof(vbuf), snap.system.ctx_switches_inv));

#undef SYS_ROW

    pg_cmd_complete(b, nrows);
}

/* ============================================================================
 * Action command handlers (PAUSE / RESUME / DISABLE / ENABLE)
 * ============================================================================ */

/**
 * @brief Append a caller-specified `CommandComplete` tag.
 *
 * @param[in,out] b Destination PG wire buffer.
 * @param tag Completion tag text.
 * @return Nothing.
 */
static void pg_cmd_complete_tag(pgbuf_t *b, const char *tag) {
    uint32_t len = 4 + (uint32_t)strlen(tag) + 1;
    pgbuf_addbyte(b, 'C');
    pgbuf_add32(b, len);
    pgbuf_addstr(b, tag);
}

/**
 * @brief Drain idle backend connections across every worker pool.
 *
 * @param admin Admin subsystem handle used to reach worker pools.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Behavior:
 * - Iterates all workers and all server pools.
 * - Calls `backend_pool_drain_idle()` on each pool.
 * - Reports the total number of idle backend connections closed.
 *
 * @note This does not block new pool creation forever. It is an operational
 *       signal that removes current idle capacity so operators can force a
 *       quiescent pool baseline.
 */
static void cmd_pause(keel_admin_t *admin, pgbuf_t *b) {
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    uint64_t drained = 0;

    for (uint32_t i = 0; i < nw; i++) {
        const keel_worker_t *w = keel_engine_get_worker(admin->engine, i);
        if (!w) continue;
        for (size_t s = 0; s < w->server_pool_count; s++) {
            backend_pool_t *pool = w->server_pools ? w->server_pools[s] : NULL;
            if (pool)
                drained += backend_pool_drain_idle(pool);
        }
    }

    const char *cols[] = { "result", "idle_closed" };
    pg_row_desc(b, cols, 2);

    char cbuf[20];
    fmt_u64(cbuf, sizeof(cbuf), drained);
    const char *vals[] = { "PAUSED", cbuf };
    pg_data_row(b, vals, 2);
    pg_cmd_complete_tag(b, "PAUSE 1");
}

/**
 * @brief Acknowledge a logical resume after `PAUSE`.
 *
 * @param admin Admin subsystem handle. Currently unused.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * @note There is no separate paused state machine in this file. Resume is a
 *       user-facing confirmation command because pool refill mechanisms recover
 *       capacity automatically after idle drains.
 */
static void cmd_resume(keel_admin_t *admin, pgbuf_t *b) {
    (void)admin;

    const char *cols[] = { "result" };
    pg_row_desc(b, cols, 1);
    const char *vals[] = { "RESUMED — pools will refill automatically" };
    pg_data_row(b, vals, 1);
    pg_cmd_complete_tag(b, "RESUME 1");
}

/**
 * @brief Mark one backend server unhealthy by index.
 *
 * @param admin Admin subsystem handle used to reach the server pool.
 * @param[in,out] b Destination PG wire buffer.
 * @param idx Zero-based server index supplied by the operator.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Invalid indexes return a PostgreSQL error response.
 *
 * Main uses:
 * - Manual traffic shedding from a bad or degraded backend.
 */
static void cmd_disable_server(keel_admin_t *admin, pgbuf_t *b, int idx) {
    keel_server_pool_t *sp = keel_engine_get_server_pool(admin->engine);
    if (!sp || idx < 0 || (size_t)idx >= sp->count) {
        char msg[128];
        snprintf(msg, sizeof(msg), "Invalid server index %d (0..%zu)", idx,
                 sp ? sp->count - 1 : 0);
        pg_error(b, msg);
        return;
    }

    sp->servers[idx].healthy = false;
    keel_server_pool_rebuild_indices(sp);

    const char *cols[] = { "result", "server", "host", "port" };
    pg_row_desc(b, cols, 4);

    char ibuf[24], pbuf[24];
    snprintf(ibuf, sizeof(ibuf), "%d", idx);
    snprintf(pbuf, sizeof(pbuf), "%u", sp->servers[idx].port);
    const char *vals[] = {
        "DISABLED",
        ibuf,
        sp->servers[idx].host ? sp->servers[idx].host : "(null)",
        pbuf
    };
    pg_data_row(b, vals, 4);
    pg_cmd_complete_tag(b, "DISABLE 1");
}

/**
 * @brief Mark one backend server healthy by index.
 *
 * @param admin Admin subsystem handle used to reach the server pool.
 * @param[in,out] b Destination PG wire buffer.
 * @param idx Zero-based server index supplied by the operator.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Invalid indexes return a PostgreSQL error response.
 */
static void cmd_enable_server(keel_admin_t *admin, pgbuf_t *b, int idx) {
    keel_server_pool_t *sp = keel_engine_get_server_pool(admin->engine);
    if (!sp || idx < 0 || (size_t)idx >= sp->count) {
        char msg[128];
        snprintf(msg, sizeof(msg), "Invalid server index %d (0..%zu)", idx,
                 sp ? sp->count - 1 : 0);
        pg_error(b, msg);
        return;
    }

    sp->servers[idx].healthy = true;
    keel_server_pool_rebuild_indices(sp);

    const char *cols[] = { "result", "server", "host", "port" };
    pg_row_desc(b, cols, 4);

    char ibuf[24], pbuf[24];
    snprintf(ibuf, sizeof(ibuf), "%d", idx);
    snprintf(pbuf, sizeof(pbuf), "%u", sp->servers[idx].port);
    const char *vals[] = {
        "ENABLED",
        ibuf,
        sp->servers[idx].host ? sp->servers[idx].host : "(null)",
        pbuf
    };
    pg_data_row(b, vals, 4);
    pg_cmd_complete_tag(b, "ENABLE 1");
}

/* ============================================================================
 * ADD SERVER / REMOVE SERVER / KILL CLIENT
 * ============================================================================ */

/**
 * @brief Release heap-owned strings attached to a dynamically added server.
 *
 * @param[in,out] srv Server descriptor to clean up.
 * @return Nothing.
 *
 * Corner cases:
 * - Static/config-backed servers are ignored because they do not own these
 *   string pointers.
 * - After cleanup the string fields are nulled to avoid accidental reuse.
 */
static void free_dynamic_server(keel_backend_server_t *srv) {
    if (!srv->dynamic) return;
    keel_free((void*)srv->host);
    keel_free((void*)srv->user);
    keel_free((void*)srv->password);
    keel_free((void*)srv->database);
    keel_free((void*)srv->probe_user);
    keel_free((void*)srv->probe_password);
    keel_free((void*)srv->probe_auth);
    srv->host = srv->user = srv->password = srv->database = NULL;
    srv->probe_user = srv->probe_password = srv->probe_auth = NULL;
}

/**
 * @brief Add a backend server to the runtime topology.
 *
 * @param admin Admin subsystem handle used to mutate server pools and workers.
 * @param[in,out] b Destination PG wire buffer.
 * @param def Key-value server definition string.
 * @return Nothing.
 *
 * Supported parameters:
 * - `host=`
 * - `port=`
 * - `user=`
 * - `password=`
 * - `dbname=`
 * - `role=`
 * - `weight=`
 *
 * Errors surfaced:
 * - No server pool available.
 * - Maximum server count reached.
 *
 * Corner cases:
 * - Unknown tokens are skipped rather than rejected.
 * - A newly created per-worker pool may fail independently; the topology entry
 *   still exists, so the response reflects logical addition rather than strict
 *   pool-warmup success.
 * - Password may be omitted, in which case the stored pointer is `NULL`.
 *
 * Main uses:
 * - Runtime topology expansion during maintenance or failover response.
 */
static void cmd_add_server(keel_admin_t *admin, pgbuf_t *b, const char *def) {
    keel_server_pool_t *sp = keel_engine_get_server_pool(admin->engine);
    if (!sp) { pg_error(b, "No server pool"); return; }
    if (sp->count >= KEEL_MAX_SERVERS) {
        char msg[128];
        snprintf(msg, sizeof(msg), "Server pool full (%d/%d)", KEEL_MAX_SERVERS, KEEL_MAX_SERVERS);
        pg_error(b, msg);
        return;
    }

    /* Defaults */
    char host[256] = "127.0.0.1";
    uint16_t port = 5432;
    char user[256] = "postgres";
    char password[256] = "";
    char database[256] = "postgres";
    keel_server_role_t role = KEEL_SERVER_ROLE_AUTO;
    uint32_t weight = 100;

    /* Parse key=value pairs (same format as config file) */
    const char *p = def;
    while (*p) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;

        if (strncmp(p, "host=", 5) == 0) {
            p += 5; char *d = host;
            while (*p && !isspace((unsigned char)*p) && (size_t)(d - host) < sizeof(host) - 1) *d++ = *p++;
            *d = '\0';
        } else if (strncmp(p, "port=", 5) == 0) {
            p += 5; port = (uint16_t)atoi(p);
            while (*p && !isspace((unsigned char)*p)) p++;
        } else if (strncmp(p, "user=", 5) == 0) {
            p += 5; char *d = user;
            while (*p && !isspace((unsigned char)*p) && (size_t)(d - user) < sizeof(user) - 1) *d++ = *p++;
            *d = '\0';
        } else if (strncmp(p, "password=", 9) == 0) {
            p += 9; char *d = password;
            while (*p && !isspace((unsigned char)*p) && (size_t)(d - password) < sizeof(password) - 1) *d++ = *p++;
            *d = '\0';
        } else if (strncmp(p, "dbname=", 7) == 0) {
            p += 7; char *d = database;
            while (*p && !isspace((unsigned char)*p) && (size_t)(d - database) < sizeof(database) - 1) *d++ = *p++;
            *d = '\0';
        } else if (strncmp(p, "role=", 5) == 0) {
            p += 5;
            if (strncasecmp(p, "RW", 2) == 0 || strncasecmp(p, "primary", 7) == 0)
                role = KEEL_SERVER_ROLE_RW;
            else if (strncasecmp(p, "RO", 2) == 0 || strncasecmp(p, "replica", 7) == 0)
                role = KEEL_SERVER_ROLE_RO;
            else if (strncasecmp(p, "WO", 2) == 0)
                role = KEEL_SERVER_ROLE_WO;
            else
                role = KEEL_SERVER_ROLE_AUTO;
            while (*p && !isspace((unsigned char)*p)) p++;
        } else if (strncmp(p, "weight=", 7) == 0) {
            p += 7; weight = (uint32_t)atoi(p);
            while (*p && !isspace((unsigned char)*p)) p++;
        } else {
            while (*p && !isspace((unsigned char)*p)) p++;
        }
    }

    /* Populate server slot with heap-allocated strings */
    size_t idx = sp->count;
    keel_backend_server_t *srv = &sp->servers[idx];
    srv->host           = keel_strdup(host);
    srv->port           = port;
    srv->user           = keel_strdup(user);
    srv->password       = password[0] ? keel_strdup(password) : NULL;
    srv->database       = keel_strdup(database);
    srv->probe_user     = NULL;
    srv->probe_password = NULL;
    srv->probe_auth     = NULL;
    srv->role           = role;
    srv->weight         = weight;
    srv->healthy        = true;
    srv->dynamic        = true;
    sp->count++;
    keel_server_pool_rebuild_indices(sp);

    /* Create a backend pool for the new server on every worker */
    const keel_engine_config_t *cfg = keel_engine_get_config(admin->engine);
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    size_t per_worker_min = cfg ? cfg->pool_min_size : 10;
    size_t per_worker_max = cfg ? cfg->pool_max_size : 50;
    size_t max_waiting    = cfg && cfg->pool_max_waiting > 0 ? cfg->pool_max_waiting : per_worker_max * 2;
    uint64_t idle_to      = cfg ? cfg->pool_idle_timeout_ms : 300000;
    uint64_t wait_to      = cfg && cfg->connect_timeout_ms > 0 ? cfg->connect_timeout_ms : 10000;

    backend_pool_config_t pool_cfg = {
        .host            = srv->host,
        .port            = srv->port,
        .user            = srv->user,
        .password        = srv->password,
        .database        = srv->database,
        .protocol        = cfg ? cfg->default_protocol : "postgres",
        .min_connections  = per_worker_min,
        .max_connections  = per_worker_max,
        .max_waiting     = max_waiting,
        .idle_timeout_ms = idle_to,
        .wait_timeout_ms = wait_to,
    };

    for (uint32_t wi = 0; wi < nw; wi++) {
        keel_worker_t *w = keel_engine_get_worker_mut(admin->engine, wi);
        if (!w) continue;

        /* Grow the server_pools array by one slot */
        struct backend_pool **new_arr = keel_calloc(sp->count, sizeof(struct backend_pool *));
        if (w->server_pools && w->server_pool_count > 0) {
            memcpy(new_arr, w->server_pools, w->server_pool_count * sizeof(struct backend_pool *));
            keel_free(w->server_pools);
        }
        w->server_pools = new_arr;
        w->server_pool_count = sp->count;

        backend_pool_t *bp = backend_pool_create(&pool_cfg);
        if (bp) backend_pool_set_wait_callback(bp, NULL); /* admin-added pools: no resume callback */
        w->server_pools[idx] = bp;
    }

    KEEL_LOG_INFO(KEEL_LOG_CAT_ADMIN, "ADD SERVER [%zu]: %s:%u role=%d weight=%u",
                  idx, host, port, (int)role, weight);

    /* Response */
    const char *cols[] = { "result", "server", "host", "port", "role" };
    pg_row_desc(b, cols, 5);
    char ibuf[24], pbuf[24];
    snprintf(ibuf, sizeof(ibuf), "%zu", idx);
    snprintf(pbuf, sizeof(pbuf), "%u", port);
    const char *role_str = role == KEEL_SERVER_ROLE_RW ? "RW" :
                           role == KEEL_SERVER_ROLE_RO ? "RO" :
                           role == KEEL_SERVER_ROLE_WO ? "WO" : "AUTO";
    const char *vals[] = { "ADDED", ibuf, host, pbuf, role_str };
    pg_data_row(b, vals, 5);
    pg_cmd_complete_tag(b, "ADD 1");
}

/**
 * @brief Remove a backend server from the runtime topology.
 *
 * @param admin Admin subsystem handle used to mutate server pools and workers.
 * @param[in,out] b Destination PG wire buffer.
 * @param idx Zero-based server index to remove.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Invalid server index.
 * - Attempt to remove the final remaining server.
 *
 * Implementation notes:
 * - Destroys per-worker backend pools for the removed slot.
 * - Compacts worker pool arrays and the shared server array in place.
 * - Rebuilds server role/health indices after compaction.
 */
static void cmd_remove_server(keel_admin_t *admin, pgbuf_t *b, int idx) {
    keel_server_pool_t *sp = keel_engine_get_server_pool(admin->engine);
    if (!sp || idx < 0 || (size_t)idx >= sp->count) {
        char msg[128];
        snprintf(msg, sizeof(msg), "Invalid server index %d (0..%zu)", idx,
                 sp ? (sp->count > 0 ? sp->count - 1 : 0) : 0);
        pg_error(b, msg);
        return;
    }
    if (sp->count <= 1) {
        pg_error(b, "Cannot remove the last server");
        return;
    }

    /* Capture info for the response before we shift things */
    char host_buf[256], pbuf[24], ibuf[24];
    snprintf(host_buf, sizeof(host_buf), "%s", sp->servers[idx].host ? sp->servers[idx].host : "?");
    snprintf(pbuf, sizeof(pbuf), "%u", sp->servers[idx].port);
    snprintf(ibuf, sizeof(ibuf), "%d", idx);

    /* Destroy backend pool for this server on every worker, shift pools down */
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    for (uint32_t wi = 0; wi < nw; wi++) {
        keel_worker_t *w = keel_engine_get_worker_mut(admin->engine, wi);
        if (!w || !w->server_pools) continue;

        if ((size_t)idx < w->server_pool_count && w->server_pools[idx]) {
            backend_pool_destroy(w->server_pools[idx]);
            w->server_pools[idx] = NULL;
        }

        /* Shift remaining pools down */
        for (size_t i = (size_t)idx; i + 1 < w->server_pool_count; i++)
            w->server_pools[i] = w->server_pools[i + 1];
        if (w->server_pool_count > 0) {
            w->server_pool_count--;
            w->server_pools[w->server_pool_count] = NULL;
        }
    }

    /* Free heap-allocated strings if this was a dynamic server */
    free_dynamic_server(&sp->servers[idx]);

    /* Shift server array down */
    for (size_t i = (size_t)idx; i + 1 < sp->count; i++)
        sp->servers[i] = sp->servers[i + 1];
    memset(&sp->servers[sp->count - 1], 0, sizeof(keel_backend_server_t));
    sp->count--;
    keel_server_pool_rebuild_indices(sp);

    KEEL_LOG_INFO(KEEL_LOG_CAT_ADMIN, "REMOVE SERVER [%s]: %s:%s", ibuf, host_buf, pbuf);

    /* Response */
    const char *cols[] = { "result", "server", "host", "port" };
    pg_row_desc(b, cols, 4);
    const char *vals[] = { "REMOVED", ibuf, host_buf, pbuf };
    pg_data_row(b, vals, 4);
    pg_cmd_complete_tag(b, "REMOVE 1");
}

/**
 * @brief Force-close a frontend session by session identifier.
 *
 * @param admin Admin subsystem handle used to walk workers and sessions.
 * @param[in,out] b Destination PG wire buffer.
 * @param session_id Target session identifier.
 * @return Nothing.
 *
 * Errors surfaced:
 * - Session not found.
 * - Session already closed.
 * - Session currently marked commit-in-doubt, in which case the kill is
 *   rejected to avoid risking ambiguous transaction outcome.
 *
 * Main uses:
 * - Operator intervention for wedged or abusive clients.
 */
static void cmd_kill_client(keel_admin_t *admin, pgbuf_t *b, uint64_t session_id) {
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    bool found = false;

    for (uint32_t wi = 0; wi < nw && !found; wi++) {
        keel_worker_t *w = keel_engine_get_worker_mut(admin->engine, wi);
        if (!w) continue;

        keel_session_slab_t *slab = &w->sessions;
        for (size_t si = 0; si < slab->capacity; si++) {
            keel_session_t *sess = &slab->sessions[si];
            if (sess->id != session_id || sess->client_fd < 0)
                continue;

            /* Skip commit-in-doubt sessions to avoid losing transactions */
            if (sess->commit_in_doubt) {
                pg_error(b, "Session has commit-in-doubt active; cannot kill");
                return;
            }

            KEEL_LOG_WARN(KEEL_LOG_CAT_ADMIN,
                "KILL CLIENT: closing session %" PRIu64 " on worker %u "
                "(client_fd=%d, server_fd=%d)",
                session_id, wi, sess->client_fd, sess->server_fd);

            if (sess->client_fd >= 0) { close(sess->client_fd); sess->client_fd = -1; }
            if (sess->server_fd >= 0) { close(sess->server_fd); sess->server_fd = -1; }

            /* Decrement engine active_connections atomically */
            keel_engine_dec_connections(admin->engine);
            found = true;
            break;
        }
    }

    if (!found) {
        char msg[128];
        snprintf(msg, sizeof(msg), "Session %" PRIu64 " not found or already closed", session_id);
        pg_error(b, msg);
        return;
    }

    const char *cols[] = { "result", "session_id" };
    pg_row_desc(b, cols, 2);
    char id_buf[24];
    snprintf(id_buf, sizeof(id_buf), "%" PRIu64, session_id);
    const char *vals[] = { "KILLED", id_buf };
    pg_data_row(b, vals, 2);
    pg_cmd_complete_tag(b, "KILL 1");
}

/**
 * @brief Emit worker load and migration counters for the rebalance subsystem.
 *
 * @param admin Admin subsystem handle used to reach workers and config.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * @note The final summary row encodes rebalance mode rather than another worker
 *       sample, so consumers should not assume homogenous row semantics.
 */

/* ============================================================================
 * Shard routing virtual tables
 * ============================================================================ */

/**
 * @brief SHOW OSC SESSIONS — active Online Schema Change tool sessions.
 *
 * Columns: id, worker, username, database, tool, state, age_ms, query_count
 *
 * "tool" is inferred from the pin flag (always "osc" until per-tool
 * distinction is added to the flow).
 */
static void show_osc_sessions(keel_admin_t *admin, pgbuf_t *b) {
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    uint64_t now = admin_now_ns();

    const char *cols[] = {
        "id", "worker", "username", "database", "tool", "state", "age_ms", "query_count"
    };
    pg_row_desc(b, cols, 8);

    int nrows = 0;
    char c_id[20], c_wk[12], c_age[20], c_qc[16];

    for (uint32_t i = 0; i < nw; i++) {
        const keel_worker_t *w = keel_engine_get_worker(admin->engine, i);
        if (!w) continue;

        snprintf(c_wk, sizeof(c_wk), "%u", i);

        for (size_t s = 0; s < w->sessions.capacity; s++) {
            const keel_session_t *sess = &w->sessions.sessions[s];
            if (sess->client_fd < 0) continue;
            if (!(sess->pin_reason & (uint32_t)KEEL_PIN_OSC)) continue;

            fmt_u64(c_id,  sizeof(c_id),  sess->id);
            fmt_u64(c_qc,  sizeof(c_qc),  sess->query_count);

            uint64_t age_ms = 0;
            if (sess->created_at > 0 && now > sess->created_at)
                age_ms = (now - sess->created_at) / 1000000ULL;
            fmt_u64(c_age, sizeof(c_age), age_ms);

            const char *state = keel_session_state_name(sess->state);

            const char *vals[] = {
                c_id, c_wk,
                sess->username[0] ? sess->username : "(none)",
                sess->database[0] ? sess->database : "(none)",
                "osc",
                state, c_age, c_qc
            };
            pg_data_row(b, vals, 8);
            nrows++;
        }
    }

    pg_cmd_complete(b, nrows);
}

/**
 * @brief SHOW QUERY RULES — list all loaded declarative query rules.
 *
 * Columns: priority, name, match_regex, match_user, match_db,
 *          action, route_to, error_msg, rewrite_to, enabled
 */
static void show_query_rules(keel_admin_t *admin, pgbuf_t *b) {
    const keel_query_rules_t *rl = admin->query_rules;

    const char *cols[] = {
        "priority", "name", "match_regex", "match_user", "match_db",
        "action", "route_to", "error_msg", "rewrite_to", "enabled"
    };
    pg_row_desc(b, cols, 10);

    if (!rl || rl->count == 0) {
        pg_cmd_complete(b, 0);
        return;
    }

    char prio_buf[16];
    int nrows = 0;
    for (size_t i = 0; i < rl->count; i++) {
        const keel_query_rule_t *r = &rl->rules[i];
        snprintf(prio_buf, sizeof(prio_buf), "%u", r->priority);
        const char *vals[] = {
            prio_buf,
            r->name        ? r->name        : "",
            r->match_regex ? r->match_regex  : "",
            r->match_user  ? r->match_user   : "",
            r->match_db    ? r->match_db     : "",
            keel_qr_action_name(r->action),
            r->action == KEEL_QR_ACTION_ROUTE
                           ? keel_qr_route_name(r->route_to) : "",
            r->error_msg   ? r->error_msg    : "",
            r->rewrite_to  ? r->rewrite_to   : "",
            r->enabled     ? "yes"           : "no",
        };
        pg_data_row(b, vals, 10);
        nrows++;
    }
    pg_cmd_complete(b, nrows);
}

/**
 * @brief SHOW SHARD RULES — list all registered shard rules.
 *
 * Columns: table, column, shard_count
 */
static void show_shard_rules(keel_admin_t *admin, pgbuf_t *b) {
    if (!admin->router) {
        pg_error(b, "No router attached. Call keel_admin_set_router() first.");
        return;
    }

    const char *cols[] = { "table", "column", "shard_count" };
    pg_row_desc(b, cols, 3);

    size_t n = keel_router_shard_rule_count(admin->router);
    char scbuf[24];
    int nrows = 0;

    for (size_t i = 0; i < n; i++) {
        const keel_shard_rule_t *r =
            keel_router_get_shard_rule_at(admin->router, i);
        if (!r) continue;
        snprintf(scbuf, sizeof(scbuf), "%zu", r->shard_count);
        const char *vals[] = {
            r->table  ? r->table  : "(null)",
            r->column ? r->column : "(null)",
            scbuf,
        };
        pg_data_row(b, vals, 3);
        nrows++;
    }

    pg_cmd_complete(b, nrows);
}

/**
 * @brief SHOW CACHE STATS — per-worker query cache hit/miss/eviction counters.
 */
static void show_cache_stats(keel_admin_t *admin, pgbuf_t *b) {
    if (!admin->engine) {
        pg_error(b, "No engine attached.");
        return;
    }
    const char *cols[] = { "worker", "hits", "misses", "evictions", "expirations",
                           "entries", "memory_used_bytes", "memory_max_bytes" };
    pg_row_desc(b, cols, 8);

    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    int nrows = 0;
    for (uint32_t i = 0; i < nw; i++) {
        const keel_worker_t *w = keel_engine_get_worker(admin->engine, i);
        if (!w || !w->query_cache) continue;

        keel_query_cache_stats_t st = {0};
        keel_query_cache_stats(w->query_cache, &st);

        char wbuf[16], hits[24], misses[24], evict[24], expir[24], ent[24], mu[24], mm[24];
        snprintf(wbuf,  sizeof(wbuf),  "%u",  i);
        snprintf(hits,  sizeof(hits),  "%" PRIu64, st.hits);
        snprintf(misses, sizeof(misses),"%" PRIu64, st.misses);
        snprintf(evict, sizeof(evict), "%" PRIu64, st.evictions);
        snprintf(expir, sizeof(expir), "%" PRIu64, st.expirations);
        snprintf(ent,   sizeof(ent),   "%zu",  st.entries_count);
        snprintf(mu,    sizeof(mu),    "%zu",  st.memory_used_bytes);
        snprintf(mm,    sizeof(mm),    "%zu",  st.memory_max_bytes);
        const char *vals[] = { wbuf, hits, misses, evict, expir, ent, mu, mm };
        pg_data_row(b, vals, 8);
        nrows++;
    }
    pg_cmd_complete(b, nrows);
}

/**
 * @brief FLUSH QUERY CACHE — flush all per-worker query result caches.
 */
static void cmd_flush_query_cache(keel_admin_t *admin, pgbuf_t *b) {
    if (!admin->engine) {
        pg_error(b, "No engine attached.");
        return;
    }
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    int flushed = 0;
    for (uint32_t i = 0; i < nw; i++) {
        keel_worker_t *w = keel_engine_get_worker_mut(admin->engine, i);
        if (!w || !w->query_cache) continue;
        keel_query_cache_flush(w->query_cache);
        flushed++;
    }
    const char *cols[] = { "result", "workers_flushed" };
    pg_row_desc(b, cols, 2);
    char fbuf[16];
    snprintf(fbuf, sizeof(fbuf), "%d", flushed);
    const char *vals[] = { "OK", fbuf };
    pg_data_row(b, vals, 2);
    pg_cmd_complete(b, 1);
}

/**
 * @brief SHOW THROTTLE RULES — rate-limiting rules with counters.
 */
static void show_throttle_rules(keel_admin_t *admin, pgbuf_t *b) {
    const keel_throttle_rules_t *tr = admin->throttle_rules;
    const char *cols[] = { "name", "match_regex", "match_user", "match_db",
                           "per_client", "enabled", "total_rejected" };
    pg_row_desc(b, cols, 7);
    if (!tr || tr->count == 0) {
        pg_cmd_complete(b, 0);
        return;
    }
    uint64_t global_rejected = keel_throttle_total_rejected(tr);
    (void)global_rejected;  /* per-rule counters used below */
    int nrows = 0;
    for (size_t i = 0; i < tr->count; i++) {
        const keel_throttle_rule_t *r = &tr->rules[i];
        char rejected_buf[24];
        /* Per-rule rejected count lives in the token bucket; expose global
         * total on the first rule and zero on subsequent ones as an approximation
         * until a per-rule counter API is added. */
        snprintf(rejected_buf, sizeof(rejected_buf), "%" PRIu64,
                 i == 0 ? keel_throttle_total_rejected(tr) : (uint64_t)0);
        const char *vals[] = {
            r->name[0]       ? r->name       : "-",
            r->match_regex[0]? r->match_regex : "-",
            r->match_user[0] ? r->match_user  : "-",
            r->match_db[0]   ? r->match_db    : "-",
            r->per_client    ? "true" : "false",
            r->enabled       ? "true" : "false",
            rejected_buf,
        };
        pg_data_row(b, vals, 7);
        nrows++;
    }
    pg_cmd_complete(b, nrows);
}

/**
 * @brief SHOW TOPOLOGY — cluster topology from the discovery subsystem.
 *
 * When discovery is not configured this falls back to the static server list
 * from the engine pool, which is also updated by probe health checks.
 */
static void show_topology(keel_admin_t *admin, pgbuf_t *b) {
    const char *cols[] = { "name", "host", "port", "role", "health", "discovery" };
    pg_row_desc(b, cols, 6);

    keel_server_pool_t *sp = keel_engine_get_server_pool(admin->engine);
    if (!sp || sp->count == 0) {
        pg_cmd_complete(b, 0);
        return;
    }
    bool disc_active = admin->discovery && keel_discovery_is_running(admin->discovery);
    int nrows = 0;
    char pbuf[8], nbuf[24];
    for (size_t i = 0; i < sp->count; i++) {
        const keel_backend_server_t *srv = &sp->servers[i];
        snprintf(pbuf, sizeof(pbuf), "%u", srv->port);
        snprintf(nbuf, sizeof(nbuf), "%zu", i);
        const char *role = (srv->role == KEEL_SERVER_ROLE_RW)   ? "primary"
                         : (srv->role == KEEL_SERVER_ROLE_RO)   ? "replica"
                         : (srv->role == KEEL_SERVER_ROLE_WO)   ? "write-only"
                         : (srv->role == KEEL_SERVER_ROLE_AUTO) ? "auto" : "unknown";
        const char *vals[] = {
            nbuf,
            srv->host ? srv->host : "-",
            pbuf,
            role,
            srv->healthy ? "UP" : "DOWN",
            disc_active  ? "active" : "static",
        };
        pg_data_row(b, vals, 6);
        nrows++;
    }
    pg_cmd_complete(b, nrows);
}

/**
 * @brief EXPLAIN SHARD PLAN FOR '<sql>' — show the routing plan for a query.
 *
 * Columns:
 *   kind            — "SINGLE", "SCATTER", or "UNSUPPORTED"
 *   shard_index     — target shard (SINGLE) or "-" (SCATTER / UNSUPPORTED)
 *   shard_count     — number of scatter shards, or "-"
 *   agg_type        — aggregation strategy: NONE, SCALAR_AGG, AVG, GROUP_BY,
 *                     GROUP_AGG, COUNT_DISTINCT; "-" for non-scatter
 *   merge_strategy  — post-merge operations applied by the proxy, e.g.
 *                     "GROUP+SORT+LIMIT"; "PASSTHROUGH" when none; "-" for
 *                     non-scatter
 *   has_order_by    — "true" / "false" / "-"
 *   has_limit       — "true" / "false" / "-"
 */
static void show_shard_plan(keel_admin_t *admin, pgbuf_t *b, const char *sql_text) {
    if (!admin->router) {
        pg_error(b, "No router attached. Call keel_admin_set_router() first.");
        return;
    }
    if (!sql_text || sql_text[0] == '\0') {
        pg_error(b, "EXPLAIN SHARD PLAN FOR requires a SQL argument");
        return;
    }

    /* Strip leading whitespace */
    const char *src = sql_text;
    while (*src == ' ') src++;

    /* Strip optional surrounding single-quotes */
    char unquoted[4096];
    size_t slen = strlen(src);
    if (slen >= 2 && src[0] == '\'' && src[slen - 1] == '\'') {
        size_t inner = slen - 2;
        if (inner >= sizeof(unquoted)) {
            pg_error(b, "SQL argument too long for EXPLAIN SHARD PLAN");
            return;
        }
        memcpy(unquoted, src + 1, inner);
        unquoted[inner] = '\0';
        src = unquoted;
    }

    keel_str_t sql = { .data = src, .len = strlen(src) };
    keel_shard_plan_t plan = { .kind = KEEL_SHARD_PLAN_UNSUPPORTED, .shard_index = 0 };
    keel_router_plan_sql(admin->router, sql, NULL, &plan);

    /* For SCATTER, call dispatch_sql to obtain the full merge strategy details:
     * shard count, aggregate type, ORDER BY / LIMIT flags.  Session and params
     * are NULL (no live session context at explain time; $N bindings use their
     * declared types, not values). */
    keel_dispatch_result_t dr;
    memset(&dr, 0, sizeof(dr));
    bool has_dispatch = false;
    if (plan.kind == KEEL_SHARD_PLAN_SCATTER) {
        keel_error_t derr = keel_router_dispatch_sql(admin->router, sql,
                                                      NULL, NULL, false, &dr);
        has_dispatch = (derr == KEEL_OK);
    }

    const char *cols[] = {
        "kind", "shard_index", "shard_count",
        "agg_type", "merge_strategy",
        "has_order_by", "has_limit", "window_func"
    };
    pg_row_desc(b, cols, 8);

    /* ---- kind & shard_index ---- */
    const char *kind_str =
        (plan.kind == KEEL_SHARD_PLAN_SINGLE)   ? "SINGLE"      :
        (plan.kind == KEEL_SHARD_PLAN_SCATTER)   ? "SCATTER"     :
                                                   "UNSUPPORTED";

    char idx_buf[24];
    if (plan.kind == KEEL_SHARD_PLAN_SINGLE)
        snprintf(idx_buf, sizeof(idx_buf), "%zu", plan.shard_index);
    else
        snprintf(idx_buf, sizeof(idx_buf), "-");

    /* ---- scatter-specific columns ---- */
    char shard_count_buf[24]    = "-";
    char agg_type_buf[32]       = "-";
    char merge_strategy_buf[64] = "-";
    char has_order_by_buf[8]    = "-";
    char has_limit_buf[8]       = "-";
    char window_func_buf[80]    = "-";

    if (has_dispatch && plan.kind == KEEL_SHARD_PLAN_SCATTER) {
        snprintf(shard_count_buf, sizeof(shard_count_buf),
                 "%zu", dr.scatter.count);

        /* Aggregate type — most-specific classification wins */
        if (dr.requires_count_distinct)
            snprintf(agg_type_buf, sizeof(agg_type_buf), "COUNT_DISTINCT");
        else if (dr.ngroup_key_cols > 0 && dr.nagg_specs > 0)
            snprintf(agg_type_buf, sizeof(agg_type_buf), "GROUP_AGG");
        else if (dr.ngroup_key_cols > 0)
            snprintf(agg_type_buf, sizeof(agg_type_buf), "GROUP_BY");
        else if (dr.requires_avg_rewrite)
            snprintf(agg_type_buf, sizeof(agg_type_buf), "AVG");
        else if (dr.nagg_specs > 0)
            snprintf(agg_type_buf, sizeof(agg_type_buf), "SCALAR_AGG");
        else
            snprintf(agg_type_buf, sizeof(agg_type_buf), "NONE");

        /* Merge strategy — list the phases that will execute post-fan-out */
        if (!dr.requires_merge) {
            snprintf(merge_strategy_buf, sizeof(merge_strategy_buf),
                     "PASSTHROUGH");
        } else {
            char parts[64] = "";
            if (dr.ngroup_key_cols > 0)
                strncat(parts, "GROUP", sizeof(parts) - strlen(parts) - 1);
            else if (dr.nagg_specs > 0)
                strncat(parts, "AGG", sizeof(parts) - strlen(parts) - 1);
            if (dr.nhaving_preds > 0) {
                if (parts[0])
                    strncat(parts, "+HAVING", sizeof(parts) - strlen(parts) - 1);
                else
                    strncat(parts, "HAVING", sizeof(parts) - strlen(parts) - 1);
            }
            if (dr.norder_keys > 0) {
                if (parts[0])
                    strncat(parts, "+SORT", sizeof(parts) - strlen(parts) - 1);
                else
                    strncat(parts, "SORT", sizeof(parts) - strlen(parts) - 1);
            }
            if (dr.nwindow_col_specs > 0) {
                if (parts[0])
                    strncat(parts, "+WINDOW", sizeof(parts) - strlen(parts) - 1);
                else
                    strncat(parts, "WINDOW", sizeof(parts) - strlen(parts) - 1);
            }
            if (dr.limit_count > 0 || dr.limit_offset > 0) {
                if (parts[0])
                    strncat(parts, "+LIMIT", sizeof(parts) - strlen(parts) - 1);
                else
                    strncat(parts, "LIMIT", sizeof(parts) - strlen(parts) - 1);
            }
            if (!parts[0])
                strncpy(parts, "PASSTHROUGH", sizeof(parts) - 1);
            strncpy(merge_strategy_buf, parts, sizeof(merge_strategy_buf) - 1);
        }

        snprintf(has_order_by_buf, sizeof(has_order_by_buf),
                 "%s", dr.norder_keys > 0 ? "true" : "false");
        snprintf(has_limit_buf, sizeof(has_limit_buf),
                 "%s", (dr.limit_count > 0 || dr.limit_offset > 0) ? "true" : "false");

        /* Window function info */
        if (dr.nwindow_col_specs > 0) {
            static const char* wfunc_names[] = {
                "ROW_NUMBER", "RANK", "DENSE_RANK",
                "NTILE", "PERCENT_RANK", "CUME_DIST"
            };
            char wparts[64] = "";
            for (uint16_t wi = 0; wi < dr.nwindow_col_specs; wi++) {
                keel_window_func_t wf = dr.window_col_specs[wi].func;
                const char* wn = (wf < 6) ? wfunc_names[wf] : "UNKNOWN";
                if (wparts[0])
                    strncat(wparts, "+", sizeof(wparts) - strlen(wparts) - 1);
                strncat(wparts, wn, sizeof(wparts) - strlen(wparts) - 1);
            }
            snprintf(window_func_buf, sizeof(window_func_buf),
                     "PHASE_F(%s)", wparts);
        } else if (dr.has_window_funcs && dr.window_forced_single) {
            snprintf(window_func_buf, sizeof(window_func_buf), "FORCED_SINGLE");
        } else if (dr.has_window_funcs) {
            snprintf(window_func_buf, sizeof(window_func_buf), "UNSUPPORTED");
        }
    }

    const char *vals[] = {
        kind_str, idx_buf, shard_count_buf,
        agg_type_buf, merge_strategy_buf,
        has_order_by_buf, has_limit_buf, window_func_buf
    };
    pg_data_row(b, vals, 8);
    pg_cmd_complete(b, 1);

    if (has_dispatch)
        keel_dispatch_result_cleanup(&dr);
}

static void show_rebalance(keel_admin_t *admin, pgbuf_t *b) {
    const keel_engine_config_t *cfg = keel_engine_get_config(admin->engine);
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    keel_stats_collector_t *sc = keel_engine_get_stats_collector(admin->engine);

    const char *cols[] = { "worker", "sessions", "migrations_sent",
                           "migrations_recv", "rebalance_checks",
                           "rebalance_moves" };
    pg_row_desc(b, cols, 6);

    char wbuf[24], sbuf[16], msbuf[16], mrbuf[16], rcbuf[16], rmbuf[16];
    int nrows = 0;

    for (uint32_t i = 0; i < nw; i++) {
        const keel_worker_t *w = keel_engine_get_worker(admin->engine, i);
        if (!w) continue;

        snprintf(wbuf, sizeof(wbuf), "%u", i);
        snprintf(sbuf, sizeof(sbuf), "%zu", w->sessions.allocated);

        if (w->stats_ctx && sc) {
            keel_stats_snapshot_t snap;
            keel_stats_snapshot_take(sc, &snap);
            /* Per-worker stats: use the collector's per-worker context.
             * For now show aggregate migration counters from the worker struct. */
            fmt_u64(msbuf, sizeof(msbuf), w->migration.sent);
            fmt_u64(mrbuf, sizeof(mrbuf), w->migration.received);
            fmt_u64(rcbuf, sizeof(rcbuf),
                    w->stats_ctx ? keel_counter_get(&w->stats_ctx->basic.rebalance_checks) : 0);
            fmt_u64(rmbuf, sizeof(rmbuf),
                    w->stats_ctx ? keel_counter_get(&w->stats_ctx->basic.rebalance_migrations) : 0);
        } else {
            fmt_u64(msbuf, sizeof(msbuf), w->migration.sent);
            fmt_u64(mrbuf, sizeof(mrbuf), w->migration.received);
            snprintf(rcbuf, sizeof(rcbuf), "0");
            snprintf(rmbuf, sizeof(rmbuf), "0");
        }

        const char *row[] = { wbuf, sbuf, msbuf, mrbuf, rcbuf, rmbuf };
        pg_data_row(b, row, 6);
        nrows++;
    }

    /* Add a summary row */
    const char *sum_row[] = {
        "---", "---", "---", "---",
        cfg && cfg->rebalance_enabled ? "ENABLED" : "DISABLED",
        cfg && cfg->rebalance_enabled ? "auto" : "off"
    };
    pg_data_row(b, sum_row, 6);
    nrows++;

    pg_cmd_complete(b, nrows);
}

/* ============================================================================
 * Cluster Commands
 * ============================================================================ */

/**
 * @brief Emit the cluster peer table as a result set.
 */
static void show_cluster(keel_admin_t *admin, pgbuf_t *b) {
    if (!admin->cluster || !keel_cluster_is_active(admin->cluster)) {
        const char *cols[] = { "status" };
        pg_row_desc(b, cols, 1);
        const char *vals[] = { "cluster mode disabled" };
        pg_data_row(b, vals, 1);
        pg_cmd_complete(b, 1);
        return;
    }

    const char *cols[] = { "node_id", "addr", "port", "status", "source",
                           "active", "latency_us", "heartbeats", "failures",
                           "consecutive_failures", "last_hb_ms_ago",
                           "config_checksum", "engine_state",
                           "clients", "backends", "uptime_sec", "discovered_at" };
    pg_row_desc(b, cols, 17);

    size_t count = keel_cluster_peer_count(admin->cluster);
    char pbuf[8], lbuf[16], hbuf[16], fbuf[16], cffbuf[16], hb_age[24];
    char cbuf[16], bbuf[16], ubuf[16], ckbuf[32], dsbuf[32];
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t now_ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    int nrows = 0;

    for (size_t i = 0; i < count; i++) {
        keel_cluster_peer_t peer;
        if (!keel_cluster_get_peer(admin->cluster, i, &peer)) continue;
        if (!peer.active) continue;

        int st = atomic_load(&peer.status);
        const char *status_str = "UNKNOWN";
        switch (st) {
        case KEEL_PEER_UP:      status_str = "UP"; break;
        case KEEL_PEER_SUSPECT: status_str = "SUSPECT"; break;
        case KEEL_PEER_DOWN:    status_str = "DOWN"; break;
        case KEEL_PEER_LEFT:    status_str = "LEFT"; break;
        default: break;
        }

        const char *source_str = "unknown";
        switch (peer.source) {
        case KEEL_PEER_SOURCE_BOOTSTRAP: source_str = "bootstrap"; break;
        case KEEL_PEER_SOURCE_ADMIN:     source_str = "admin"; break;
        case KEEL_PEER_SOURCE_JOIN:      source_str = "join"; break;
        case KEEL_PEER_SOURCE_GOSSIP:    source_str = "gossip"; break;
        default: break;
        }

        snprintf(pbuf, sizeof(pbuf), "%u", peer.port);
        snprintf(lbuf, sizeof(lbuf), "%llu",
                 (unsigned long long)atomic_load(&peer.last_latency_us));
        snprintf(hbuf, sizeof(hbuf), "%llu",
                 (unsigned long long)atomic_load(&peer.total_heartbeats));
        snprintf(fbuf, sizeof(fbuf), "%llu",
                 (unsigned long long)atomic_load(&peer.total_failures));
        snprintf(cffbuf, sizeof(cffbuf), "%u",
                 atomic_load(&peer.consecutive_failures));
        uint64_t last_hb_ns = atomic_load(&peer.last_heartbeat_ns);
        unsigned long long hb_ms_ago = 0;
        if (last_hb_ns > 0 && now_ns > last_hb_ns) {
            hb_ms_ago = (unsigned long long)((now_ns - last_hb_ns) / 1000000ULL);
        }
        snprintf(hb_age, sizeof(hb_age), "%llu", hb_ms_ago);
        snprintf(cbuf, sizeof(cbuf), "%u", peer.num_clients);
        snprintf(bbuf, sizeof(bbuf), "%u", peer.num_backends);
        snprintf(ubuf, sizeof(ubuf), "%llu", (unsigned long long)peer.uptime_sec);
        snprintf(ckbuf, sizeof(ckbuf), "0x%016llx",
                 (unsigned long long)atomic_load(&peer.config_checksum));
        snprintf(dsbuf, sizeof(dsbuf), "%llu",
                 (unsigned long long)peer.discovered_at_sec);

        const char *row[] = {
            peer.node_id[0] ? peer.node_id : "(unknown)",
            peer.addr, pbuf, status_str, source_str,
            peer.active ? "true" : "false",
            lbuf, hbuf, fbuf, cffbuf,
            hb_age,
            ckbuf,
            atomic_load(&peer.engine_state) == 1 ? "ACTIVE" : "UNKNOWN",
            cbuf, bbuf, ubuf, dsbuf
        };
        pg_data_row(b, row, 17);
        nrows++;
    }

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit local runtime cluster config as a dedicated key/value view.
 */
static void show_cluster_config(keel_admin_t *admin, pgbuf_t *b) {
    if (!admin->cluster || !keel_cluster_is_active(admin->cluster)) {
        const char *cols[] = { "status" };
        pg_row_desc(b, cols, 1);
        const char *vals[] = { "cluster mode disabled" };
        pg_data_row(b, vals, 1);
        pg_cmd_complete(b, 1);
        return;
    }

    keel_cluster_runtime_config_t cfg;
    if (!keel_cluster_get_runtime_config(admin->cluster, &cfg)) {
        pg_error(b, "failed to read cluster runtime config");
        return;
    }

    keel_cluster_stats_t stats;
    keel_cluster_get_stats(admin->cluster, &stats);

    const char *cols[] = { "key", "value" };
    pg_row_desc(b, cols, 2);

    char vbuf[64];
    int nrows = 0;

#define CLUSTER_CFG_STR(name, val) do { \
    const char *r[] = { (name), (val) }; \
    pg_data_row(b, r, 2); \
    nrows++; \
} while (0)

#define CLUSTER_CFG_U64(name, val) do { \
    snprintf(vbuf, sizeof(vbuf), "%llu", (unsigned long long)(val)); \
    const char *r[] = { (name), vbuf }; \
    pg_data_row(b, r, 2); \
    nrows++; \
} while (0)

    CLUSTER_CFG_STR("node_id", cfg.node_id[0] ? cfg.node_id : "(unset)");
    CLUSTER_CFG_STR("listen_addr", cfg.listen_addr[0] ? cfg.listen_addr : "(unset)");
    CLUSTER_CFG_U64("listen_port", cfg.listen_port);
    CLUSTER_CFG_U64("active_peers", cfg.active_peers);
    CLUSTER_CFG_U64("config_checksum", cfg.config_checksum);
    CLUSTER_CFG_U64("heartbeat_interval_ms", cfg.heartbeat_interval_ms);
    CLUSTER_CFG_U64("heartbeat_timeout_ms", cfg.heartbeat_timeout_ms);
    CLUSTER_CFG_U64("failure_threshold", cfg.failure_threshold);
    CLUSTER_CFG_U64("auto_sync", cfg.auto_sync);
    CLUSTER_CFG_U64("running", cfg.running);
    CLUSTER_CFG_U64("discovered_peers_total", stats.discovered_peers_total);
    CLUSTER_CFG_U64("config_reconciliations", stats.config_reconciliations);
    CLUSTER_CFG_U64("last_sync_apply_ns", stats.last_sync_apply_ns);

#undef CLUSTER_CFG_STR
#undef CLUSTER_CFG_U64

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit peers discovered via JOIN/GOSSIP as a dedicated view.
 */
static void show_discovered_peers(keel_admin_t *admin, pgbuf_t *b) {
    if (!admin->cluster || !keel_cluster_is_active(admin->cluster)) {
        const char *cols[] = { "status" };
        pg_row_desc(b, cols, 1);
        const char *vals[] = { "cluster mode disabled" };
        pg_data_row(b, vals, 1);
        pg_cmd_complete(b, 1);
        return;
    }

    const char *cols[] = { "node_id", "addr", "port", "status",
                           "source", "last_hb_ms_ago", "discovered_at" };
    pg_row_desc(b, cols, 7);

    size_t count = keel_cluster_peer_count(admin->cluster);
    char pbuf[8], hb_age[24], dsbuf[32];
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    uint64_t now_ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
    int nrows = 0;

    for (size_t i = 0; i < count; i++) {
        keel_cluster_peer_t peer;
        if (!keel_cluster_get_peer(admin->cluster, i, &peer)) continue;
        if (!peer.active) continue;
        if (!(peer.source == KEEL_PEER_SOURCE_JOIN ||
              peer.source == KEEL_PEER_SOURCE_GOSSIP)) {
            continue;
        }

        int st = atomic_load(&peer.status);
        const char *status_str = "UNKNOWN";
        switch (st) {
        case KEEL_PEER_UP:      status_str = "UP"; break;
        case KEEL_PEER_SUSPECT: status_str = "SUSPECT"; break;
        case KEEL_PEER_DOWN:    status_str = "DOWN"; break;
        case KEEL_PEER_LEFT:    status_str = "LEFT"; break;
        default: break;
        }

        const char *source_str =
            (peer.source == KEEL_PEER_SOURCE_JOIN) ? "join" : "gossip";

        snprintf(pbuf, sizeof(pbuf), "%u", peer.port);
        uint64_t last_hb_ns = atomic_load(&peer.last_heartbeat_ns);
        unsigned long long hb_ms_ago = 0;
        if (last_hb_ns > 0 && now_ns > last_hb_ns) {
            hb_ms_ago =
                (unsigned long long)((now_ns - last_hb_ns) / 1000000ULL);
        }
        snprintf(hb_age, sizeof(hb_age), "%llu", hb_ms_ago);
        snprintf(dsbuf, sizeof(dsbuf), "%llu",
                 (unsigned long long)peer.discovered_at_sec);

        const char *row[] = {
            peer.node_id[0] ? peer.node_id : "(unknown)",
            peer.addr,
            pbuf,
            status_str,
            source_str,
            hb_age,
            dsbuf,
        };
        pg_data_row(b, row, 7);
        nrows++;
    }

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Emit cluster-wide statistics.
 */
static void show_cluster_stats(keel_admin_t *admin, pgbuf_t *b) {
    if (!admin->cluster || !keel_cluster_is_active(admin->cluster)) {
        const char *cols[] = { "status" };
        pg_row_desc(b, cols, 1);
        const char *vals[] = { "cluster mode disabled" };
        pg_data_row(b, vals, 1);
        pg_cmd_complete(b, 1);
        return;
    }

    keel_cluster_stats_t stats;
    keel_cluster_get_stats(admin->cluster, &stats);

    const char *cols[] = { "key", "value" };
    pg_row_desc(b, cols, 2);

    char vbuf[32];
    int nrows = 0;

#define CLUSTER_STAT(name, val) do { \
    snprintf(vbuf, sizeof(vbuf), "%llu", (unsigned long long)(val)); \
    const char *r[] = { (name), vbuf }; \
    pg_data_row(b, r, 2); \
    nrows++; \
} while (0)

    CLUSTER_STAT("total_peers",          stats.total_peers);
    CLUSTER_STAT("peers_up",             stats.peers_up);
    CLUSTER_STAT("peers_suspect",        stats.peers_suspect);
    CLUSTER_STAT("peers_down",           stats.peers_down);
    CLUSTER_STAT("peers_left",           stats.peers_left);
    CLUSTER_STAT("peers_unknown",        stats.peers_unknown);
    CLUSTER_STAT("discovered_peers",     stats.discovered_peers);
    CLUSTER_STAT("heartbeats_sent",      stats.heartbeats_sent);
    CLUSTER_STAT("heartbeats_received",  stats.heartbeats_received);
    CLUSTER_STAT("sync_requests_sent",   stats.sync_requests_sent);
    CLUSTER_STAT("sync_responses_sent",  stats.sync_responses_sent);
    CLUSTER_STAT("discovered_peers_total", stats.discovered_peers_total);
    CLUSTER_STAT("config_reconciliations", stats.config_reconciliations);
    CLUSTER_STAT("last_sync_apply_ns",   stats.last_sync_apply_ns);
    CLUSTER_STAT("server_notifications", stats.server_notifications);
    CLUSTER_STAT("local_heartbeat_interval_ms", stats.local_heartbeat_interval_ms);
    CLUSTER_STAT("local_heartbeat_timeout_ms",  stats.local_heartbeat_timeout_ms);
    CLUSTER_STAT("local_failure_threshold",     stats.local_failure_threshold);
    CLUSTER_STAT("local_auto_sync",             stats.local_auto_sync);

    /* Election statistics */
    CLUSTER_STAT("election_term",          stats.local_term);
    CLUSTER_STAT("elections_started",      stats.elections_started);
    CLUSTER_STAT("elections_won",          stats.elections_won);
    CLUSTER_STAT("votes_granted",          stats.votes_granted);
    CLUSTER_STAT("votes_denied",           stats.votes_denied);
    CLUSTER_STAT("leader_stepdowns",       stats.leader_stepdowns);
    {
        static const char* rnames[] = { "follower", "candidate", "leader" };
        int ri = (int)stats.local_role;
        if (ri < 0 || ri > 2) ri = 0;
        const char *rr[] = { "local_role", rnames[ri] };
        pg_data_row(b, rr, 2);
        nrows++;
        const char *rl[] = { "local_leader_id",
                             stats.local_leader_id[0] ? stats.local_leader_id
                                                       : "(none)" };
        pg_data_row(b, rl, 2);
        nrows++;
    }

    uint64_t total_hb = stats.heartbeats_sent + stats.heartbeats_received;
    CLUSTER_STAT("heartbeat_traffic_total", total_hb);

    uint64_t peer_health_percent = 0;
    if (stats.total_peers > 0) {
        peer_health_percent =
            (uint64_t)((stats.peers_up * 100) / stats.total_peers);
    }
    CLUSTER_STAT("peer_health_up_percent", peer_health_percent);

#undef CLUSTER_STAT

    pg_cmd_complete(b, nrows);
}

/**
 * @brief Add a peer to the cluster at runtime.
 */
static void cmd_add_peer(keel_admin_t *admin, pgbuf_t *b, const char *args) {
    if (!admin->cluster || !keel_cluster_is_active(admin->cluster)) {
        pg_error(b, "Cluster mode is not enabled");
        return;
    }

    /* Parse "host:port" */
    char buf[KEEL_CLUSTER_MAX_ADDR + 8];
    size_t alen = strlen(args);
    if (alen >= sizeof(buf)) alen = sizeof(buf) - 1;
    memcpy(buf, args, alen);
    buf[alen] = '\0';

    char *colon = strrchr(buf, ':');
    if (!colon || colon == buf) {
        pg_error(b, "Usage: ADD PEER <host>:<port>");
        return;
    }
    *colon = '\0';
    uint16_t port = (uint16_t)atoi(colon + 1);
    if (port == 0) {
        pg_error(b, "Invalid port number");
        return;
    }

    int rc = keel_cluster_add_peer(admin->cluster, buf, port);
    const char *cols[] = { "result" };
    pg_row_desc(b, cols, 1);
    const char *vals[] = { rc == 0 ? "OK" : "FAILED (peer table full)" };
    pg_data_row(b, vals, 1);
    pg_cmd_complete_tag(b, "ADD PEER 1");
}

/**
 * @brief Remove a peer from the cluster.
 */
static void cmd_remove_peer(keel_admin_t *admin, pgbuf_t *b, const char *args) {
    if (!admin->cluster || !keel_cluster_is_active(admin->cluster)) {
        pg_error(b, "Cluster mode is not enabled");
        return;
    }

    char buf[KEEL_CLUSTER_MAX_ADDR + 8];
    size_t alen = strlen(args);
    if (alen >= sizeof(buf)) alen = sizeof(buf) - 1;
    memcpy(buf, args, alen);
    buf[alen] = '\0';

    char *colon = strrchr(buf, ':');
    if (!colon || colon == buf) {
        pg_error(b, "Usage: REMOVE PEER <host>:<port>");
        return;
    }
    *colon = '\0';
    uint16_t port = (uint16_t)atoi(colon + 1);

    int rc = keel_cluster_remove_peer(admin->cluster, buf, port);
    const char *cols[] = { "result" };
    pg_row_desc(b, cols, 1);
    const char *vals[] = { rc == 0 ? "OK" : "FAILED (peer not found)" };
    pg_data_row(b, vals, 1);
    pg_cmd_complete_tag(b, "REMOVE PEER 1");
}

/**
 * @brief Transition the engine into graceful drain mode.
 *
 * @param admin Admin subsystem handle used to reach engine lifecycle control.
 * @param[in,out] b Destination PG wire buffer.
 * @param args Optional timeout text in milliseconds.
 * @return Nothing.
 *
 * Behavior:
 * - Optionally updates the drain timeout.
 * - Returns an informative row if the engine is already draining.
 * - Otherwise calls `keel_engine_drain()` and reports resulting state.
 */

/**
 * @brief Graceful rolling restart of all worker threads.
 *
 * Drains existing workers (stop accepting, let sessions complete), spawns fresh
 * workers, waits for active sessions to complete (or timeout), then cleans up.
 */
static void cmd_restart_workers(keel_admin_t *admin, pgbuf_t *b, const char *args) {
    uint32_t timeout_ms = 0;
    if (args && *args)
        timeout_ms = (uint32_t)atoi(args);

    int rc = keel_engine_restart_workers(admin->engine, timeout_ms);

    const char *cols[] = { "result" };
    pg_row_desc(b, cols, 1);

    const char *vals[] = { rc == 0 ? "OK" : "FAILED" };
    pg_data_row(b, vals, 1);
    pg_cmd_complete_tag(b, "RESTART WORKERS 1");
}

static void cmd_drain(keel_admin_t *admin, pgbuf_t *b, const char *args) {
    /* Parse optional timeout: DRAIN or DRAIN <timeout_ms> */
    if (args && *args) {
        int timeout = atoi(args);
        if (timeout > 0)
            keel_engine_set_drain_timeout(admin->engine, (uint32_t)timeout);
    }

    keel_engine_state_t st = keel_engine_get_state(admin->engine);
    if (st == KEEL_ENGINE_STATE_DRAINING) {
        const char *cols[] = { "result", "state", "active_connections" };
        pg_row_desc(b, cols, 3);

        char abuf[20];
        fmt_u64(abuf, sizeof(abuf), keel_engine_get_active_connections(admin->engine));
        const char *vals[] = { "ALREADY_DRAINING", "draining", abuf };
        pg_data_row(b, vals, 3);
        pg_cmd_complete_tag(b, "DRAIN 1");
        return;
    }

    int rc = keel_engine_drain(admin->engine);
    const char *cols[] = { "result", "state", "active_connections" };
    pg_row_desc(b, cols, 3);

    char abuf[20];
    fmt_u64(abuf, sizeof(abuf), keel_engine_get_active_connections(admin->engine));
    const char *vals[] = {
        rc == 0 ? "DRAINING" : "DRAIN_FAILED",
        rc == 0 ? "draining" : "active",
        abuf
    };
    pg_data_row(b, vals, 3);
    pg_cmd_complete_tag(b, "DRAIN 1");
}

/**
 * @brief Change one mutable runtime configuration setting.
 *
 * @param admin Admin subsystem handle used to reach mutable engine config.
 * @param[in,out] b Destination PG wire buffer.
 * @param args Command arguments after the `SET ` prefix.
 * @return Nothing.
 *
 * Supported syntax:
 * - `SET key = value`
 * - `SET key value`
 *
 * Errors surfaced:
/**
 * @brief SHOW TRACING — display tracing runtime status and export stats.
 */
static void show_tracing(keel_admin_t *admin, pgbuf_t *b) {
    keel_tracer_t *tracer = keel_engine_get_tracer(admin->engine);
    if (!tracer) {
        pg_error(b, "Tracing subsystem not initialised (enabled = false?)");
        return;
    }

    const char *cols[] = { "key", "value" };
    pg_row_desc(b, cols, 2);

    char buf[64];
    const char *row[2];
    int nrows = 0;

    row[0] = "enabled"; row[1] = keel_tracer_is_enabled(tracer) ? "on" : "off";
    pg_data_row(b, row, 2); nrows++;

    const keel_tracer_stats_t *st = keel_tracer_get_stats(tracer);
    if (st) {
        snprintf(buf, sizeof(buf), "%lu", (unsigned long)atomic_load(&st->spans_created));
        row[0] = "spans_created"; row[1] = buf;
        pg_data_row(b, row, 2); nrows++;

        snprintf(buf, sizeof(buf), "%lu", (unsigned long)atomic_load(&st->spans_exported));
        row[0] = "spans_exported"; row[1] = buf;
        pg_data_row(b, row, 2); nrows++;

        snprintf(buf, sizeof(buf), "%lu", (unsigned long)atomic_load(&st->spans_dropped));
        row[0] = "spans_dropped"; row[1] = buf;
        pg_data_row(b, row, 2); nrows++;

        snprintf(buf, sizeof(buf), "%lu", (unsigned long)atomic_load(&st->export_errors));
        row[0] = "export_errors"; row[1] = buf;
        pg_data_row(b, row, 2); nrows++;

        snprintf(buf, sizeof(buf), "%lu", (unsigned long)atomic_load(&st->export_batches));
        row[0] = "export_batches"; row[1] = buf;
        pg_data_row(b, row, 2); nrows++;
    }

    pg_cmd_complete(b, nrows);
}

/**
 * @brief SHOW CERTIFICATES — display TLS certificate info for the running engine.
 *
 * Shows subject, issuer, not-before/after, and SHA-256 fingerprint for each
 * configured certificate file (frontend CA, frontend server cert, backend CA).
 */
static void show_certificates(keel_admin_t *admin, pgbuf_t *b) {
    const keel_engine_config_t *cfg = keel_engine_get_config(admin->engine);
    if (!cfg) {
        pg_error(b, "Engine configuration unavailable");
        return;
    }

    const char *cols[] = { "role", "subject", "issuer", "not_before", "not_after", "fingerprint_sha256" };
    pg_row_desc(b, cols, 6);
    int nrows = 0;

#ifdef KEEL_HAS_OPENSSL
    /* Helper: emit one row for a cert file */
    const char *cert_paths[3][2] = {
        { "frontend_ca",     cfg->tls_config.ca_file     },
        { "frontend_cert",   cfg->tls_config.cert_file   },
        { "backend_ca",      cfg->backend_tls_config.ca_file },
    };

    for (int ci = 0; ci < 3; ci++) {
        const char *role     = cert_paths[ci][0];
        const char *cert_path = cert_paths[ci][1];
        if (!cert_path || !cert_path[0]) continue;

        FILE *fp = fopen(cert_path, "r");
        if (!fp) continue;

        X509 *cert = PEM_read_X509(fp, NULL, NULL, NULL);
        fclose(fp);
        if (!cert) continue;

        char subject_buf[256] = {0};
        char issuer_buf[256]  = {0};
        char nb_buf[32]       = {0};
        char na_buf[32]       = {0};
        char fp_buf[96]       = {0};

        /* Subject / Issuer */
        X509_NAME_oneline(X509_get_subject_name(cert), subject_buf, (int)sizeof(subject_buf));
        X509_NAME_oneline(X509_get_issuer_name(cert),  issuer_buf,  (int)sizeof(issuer_buf));

        /* Not Before / Not After as ISO-8601 strings */
        const ASN1_TIME *nb = X509_get0_notBefore(cert);
        const ASN1_TIME *na = X509_get0_notAfter(cert);
        {
            BIO *bio = BIO_new(BIO_s_mem());
            if (bio) {
                ASN1_TIME_print(bio, nb);
                BIO_read(bio, nb_buf, (int)sizeof(nb_buf) - 1);
                BIO_free(bio);
            }
        }
        {
            BIO *bio = BIO_new(BIO_s_mem());
            if (bio) {
                ASN1_TIME_print(bio, na);
                BIO_read(bio, na_buf, (int)sizeof(na_buf) - 1);
                BIO_free(bio);
            }
        }

        /* SHA-256 fingerprint */
        unsigned char digest[EVP_MAX_MD_SIZE];
        unsigned int  dlen = 0;
        if (X509_digest(cert, EVP_sha256(), digest, &dlen)) {
            char *p = fp_buf;
            size_t fp_remaining = sizeof(fp_buf);
            for (unsigned int di = 0; di < dlen && fp_remaining > 3; di++) {
                int written = snprintf(p, fp_remaining, "%s%02X",
                                       di ? ":" : "", digest[di]);
                if (written < 0) break;
                p += written;
                fp_remaining -= (size_t)written;
            }
        }

        X509_free(cert);

        const char *row[6] = { role, subject_buf, issuer_buf, nb_buf, na_buf, fp_buf };
        pg_data_row(b, row, 6);
        nrows++;
    }
#else
    (void)cfg;
    const char *row[6] = { "N/A", "OpenSSL not compiled in", "", "", "", "" };
    pg_data_row(b, row, 6);
    nrows++;
#endif /* KEEL_HAS_OPENSSL */

    pg_cmd_complete(b, nrows);
}

/**
 * @brief SET — runtime tuning of pool parameters, timeouts, and toggles.
 *
 * Parses `key = value` or `key value` forms (case-insensitive).  Every
 * recognized key is validated and applied immediately, returning a
 * single-row result with old and new values.
 *
 * Errors surfaced:
 * - Missing arguments.
 * - Invalid syntax.
 * - Engine config unavailable.
 * - Unknown or immutable key.
 *
 * Corner cases:
 * - Numeric parsing uses `atol()`/`atoi()` and therefore accepts partial text.
 * - `rebalance_threshold_pct` currently preserves the historical in-code clamp
 *   behavior, which falls back to `125` unless the supplied value exceeds 100.
 */
static void cmd_set(keel_admin_t *admin, pgbuf_t *b, const char *args) {
    if (!args || !*args) {
        pg_error(b, "Usage: SET <key> = <value>");
        return;
    }

    /* Silently accept standard PostgreSQL GUC names sent by libpq/psycopg2
     * during connection setup (e.g. SET datestyle TO 'ISO').  These have no
     * meaning inside the admin console so we just acknowledge them. */
    {
        static const char * const pg_gucs[] = {
            "datestyle", "client_encoding", "timezone",
            "extra_float_digits", "standard_conforming_strings",
            "integer_datetimes", "search_path", "application_name",
            "intervalstyle", "lc_monetary", "lc_numeric", "lc_time",
            "transaction_isolation", "transaction_read_only",
            "default_transaction_isolation", "default_transaction_read_only",
            NULL
        };
        /* Extract key: first whitespace- or '='-delimited word */
        char pg_key[128] = {0};
        const char *p = args;
        size_t ki = 0;
        while (*p && *p != '=' && *p != ' ' && *p != '\t' && ki < sizeof(pg_key)-1)
            pg_key[ki++] = *p++;
        pg_key[ki] = '\0';
        for (int gi = 0; pg_gucs[gi]; gi++) {
            if (strcasecmp(pg_key, pg_gucs[gi]) == 0) {
                pg_cmd_complete_tag(b, "SET");
                return;
            }
        }
    }

    /* Phase 4: Quorum-gated config commits.
     *
     * If the cluster is active and leader election has run (term > 0), only
     * the elected leader may apply config changes.  Non-leaders are rejected
     * with a redirect hint so the operator knows which node to connect to.
     *
     * On the leader node a lightweight 2-phase quorum check is performed
     * (CONFIG_PREPARE → ACKs → CONFIG_COMMIT) before the change is applied.
     * This prevents config changes from taking effect when the cluster has
     * lost quorum due to a network partition. */
    if (admin->cluster && keel_cluster_is_active(admin->cluster) &&
        keel_cluster_get_term(admin->cluster) > 0) {
        if (keel_cluster_get_role(admin->cluster) != KEEL_CLUSTER_ROLE_LEADER) {
            char errmsg[256];
            char lid[KEEL_CLUSTER_MAX_NODE_ID];
            keel_cluster_get_leader_id(admin->cluster, lid, sizeof(lid));
            snprintf(errmsg, sizeof(errmsg),
                "Not the cluster leader. Connect to node \"%s\" to issue "
                "configuration changes.",
                lid[0] ? lid : "(unknown — no leader elected yet)");
            pg_error(b, errmsg);
            return;
        }
        /* Leader: run quorum commit (checksum=0: hint not yet known) */
        if (keel_cluster_quorum_commit(admin->cluster, 0) != 0) {
            pg_error(b,
                "Cluster quorum check failed: cannot safely apply "
                "config change — insufficient active peers.");
            return;
        }
    }

    /* Parse "key = value" or "key value" */
    char key[128] = {0}, val[256] = {0};
    const char *eq = strchr(args, '=');
    if (eq) {
        size_t klen = (size_t)(eq - args);
        if (klen >= sizeof(key)) klen = sizeof(key) - 1;
        memcpy(key, args, klen);
        key[klen] = '\0';
        /* Trim trailing spaces from key */
        char *ke = key + strlen(key) - 1;
        while (ke > key && (*ke == ' ' || *ke == '\t')) *ke-- = '\0';
        /* Skip leading spaces in value */
        const char *vp = eq + 1;
        while (*vp == ' ' || *vp == '\t') vp++;
        snprintf(val, sizeof(val), "%s", vp);
    } else {
        /* "SET key value" form */
        if (sscanf(args, "%127s %255s", key, val) < 2) {
            pg_error(b, "Usage: SET <key> = <value>");
            return;
        }
    }

    keel_engine_config_t *cfg = keel_engine_get_config_mut(admin->engine);
    if (!cfg) { pg_error(b, "Engine config not available"); return; }

    char old_val[64] = {0};

    /* Match known settable keys */
    if (strcasecmp(key, "pool_min_size") == 0) {
        snprintf(old_val, sizeof(old_val), "%zu", cfg->pool_min_size);
        cfg->pool_min_size = (size_t)atol(val);
    } else if (strcasecmp(key, "pool_max_size") == 0) {
        snprintf(old_val, sizeof(old_val), "%zu", cfg->pool_max_size);
        cfg->pool_max_size = (size_t)atol(val);
    } else if (strcasecmp(key, "idle_timeout_ms") == 0) {
        snprintf(old_val, sizeof(old_val), "%u", cfg->idle_timeout_ms);
        cfg->idle_timeout_ms = (uint32_t)atol(val);
    } else if (strcasecmp(key, "connect_timeout_ms") == 0) {
        snprintf(old_val, sizeof(old_val), "%u", cfg->connect_timeout_ms);
        cfg->connect_timeout_ms = (uint32_t)atol(val);
    } else if (strcasecmp(key, "pool_idle_timeout_ms") == 0) {
        snprintf(old_val, sizeof(old_val), "%" PRIu64, cfg->pool_idle_timeout_ms);
        cfg->pool_idle_timeout_ms = (uint64_t)atol(val);
    } else if (strcasecmp(key, "pool_prune_interval_ms") == 0) {
        snprintf(old_val, sizeof(old_val), "%u", cfg->pool_prune_interval_ms);
        cfg->pool_prune_interval_ms = (uint32_t)atol(val);
    } else if (strcasecmp(key, "pool_max_waiting") == 0) {
        snprintf(old_val, sizeof(old_val), "%u", cfg->pool_max_waiting);
        cfg->pool_max_waiting = (uint32_t)atol(val);
    } else if (strcasecmp(key, "stats_level") == 0) {
        snprintf(old_val, sizeof(old_val), "%d", cfg->stats_level);
        cfg->stats_level = atoi(val);
    } else if (strcasecmp(key, "stats_interval_ms") == 0) {
        snprintf(old_val, sizeof(old_val), "%u", cfg->stats_interval_ms);
        cfg->stats_interval_ms = (uint32_t)atol(val);
    } else if (strcasecmp(key, "rebalance_enabled") == 0) {
        snprintf(old_val, sizeof(old_val), "%s", cfg->rebalance_enabled ? "on" : "off");
        cfg->rebalance_enabled = (strcasecmp(val, "on") == 0 || strcasecmp(val, "true") == 0 ||
                                  strcasecmp(val, "1") == 0);
    } else if (strcasecmp(key, "rebalance_interval_ms") == 0) {
        snprintf(old_val, sizeof(old_val), "%u", cfg->rebalance_interval_ms);
        cfg->rebalance_interval_ms = (uint32_t)atol(val);
    } else if (strcasecmp(key, "rebalance_threshold_pct") == 0) {
        snprintf(old_val, sizeof(old_val), "%u", cfg->rebalance_threshold_pct);
        uint32_t v = (uint32_t)atol(val);
        cfg->rebalance_threshold_pct = (v > 100) ? v : 125;
    } else if (strcasecmp(key, "rebalance_max_per_tick") == 0) {
        snprintf(old_val, sizeof(old_val), "%u", cfg->rebalance_max_per_tick);
        cfg->rebalance_max_per_tick = (uint32_t)atol(val);
    } else if (strcasecmp(key, "tracing") == 0) {
        keel_tracer_t *tracer = keel_engine_get_tracer(admin->engine);
        if (!tracer) { pg_error(b, "Tracing subsystem not initialised"); return; }
        snprintf(old_val, sizeof(old_val), "%s",
                 keel_tracer_is_enabled(tracer) ? "on" : "off");
        bool enabled = (strcasecmp(val, "on") == 0 || strcasecmp(val, "true") == 0 ||
                        strcasecmp(val, "1") == 0);
        keel_tracer_set_enabled(tracer, enabled);
    } else if (strcasecmp(key, "trace_sample_rate") == 0) {
        keel_tracer_t *tracer = keel_engine_get_tracer(admin->engine);
        if (!tracer) { pg_error(b, "Tracing subsystem not initialised"); return; }
        uint32_t ppm = (uint32_t)atol(val);
        snprintf(old_val, sizeof(old_val), "—");
        keel_tracer_set_sample_rate(tracer, ppm);
        snprintf(val, sizeof(val), "%u", ppm > 1000000 ? 1000000 : ppm);
    } else {
        char errmsg[512];
        snprintf(errmsg, sizeof(errmsg),
                 "Unknown or immutable setting: \"%s\". "
                 "Settable: pool_min_size, pool_max_size, idle_timeout_ms, "
                 "connect_timeout_ms, pool_idle_timeout_ms, pool_prune_interval_ms, "
                 "pool_max_waiting, stats_level, stats_interval_ms, "
                 "rebalance_enabled, rebalance_interval_ms, "
                 "rebalance_threshold_pct, rebalance_max_per_tick, "
                 "tracing, trace_sample_rate", key);
        pg_error(b, errmsg);
        return;
    }

    const char *cols[] = { "key", "old_value", "new_value" };
    pg_row_desc(b, cols, 3);
    const char *row[] = { key, old_val, val };
    pg_data_row(b, row, 3);
    pg_cmd_complete_tag(b, "SET 1");
}

/**
 * @brief Trigger a coordinated configuration reload through the main process.
 *
 * @param admin Admin subsystem handle used to validate engine config presence.
 * @param[in,out] b Destination PG wire buffer.
 * @return Nothing.
 *
 * Behavior:
 * - Sends `SIGHUP` to the current process.
 * - Delegates actual reload orchestration to the main process owner.
 *
 * Errors surfaced:
 * - Engine config unavailable.
 * - Failure to deliver the signal.
 */
static void cmd_reload(keel_admin_t *admin, pgbuf_t *b) {
    keel_engine_config_t *cfg = keel_engine_get_config_mut(admin->engine);
    if (!cfg) { pg_error(b, "Engine config not available"); return; }

    /* Phase 4: same leader-only + quorum check as cmd_set(). */
    if (admin->cluster && keel_cluster_is_active(admin->cluster) &&
        keel_cluster_get_term(admin->cluster) > 0) {
        if (keel_cluster_get_role(admin->cluster) != KEEL_CLUSTER_ROLE_LEADER) {
            char errmsg[256];
            char lid[KEEL_CLUSTER_MAX_NODE_ID];
            keel_cluster_get_leader_id(admin->cluster, lid, sizeof(lid));
            snprintf(errmsg, sizeof(errmsg),
                "Not the cluster leader. Connect to node \"%s\" to issue "
                "a config reload.",
                lid[0] ? lid : "(unknown — no leader elected yet)");
            pg_error(b, errmsg);
            return;
        }
        if (keel_cluster_quorum_commit(admin->cluster, 0) != 0) {
            pg_error(b,
                "Cluster quorum check failed: cannot safely reload "
                "config — insufficient active peers.");
            return;
        }
    }

    /* config_path is stored in g_config in main.c — we'll send SIGHUP to
     * the main process to trigger a full reload. This is the safest approach
     * since main.c owns the config file path and coordinated reload logic. */
    pid_t pid = getpid();
    if (kill(pid, SIGHUP) != 0) {
        char errmsg[128];
        snprintf(errmsg, sizeof(errmsg), "Failed to send SIGHUP: %s", strerror(errno));
        pg_error(b, errmsg);
        return;
    }

    const char *cols[] = { "result" };
    pg_row_desc(b, cols, 1);
    const char *vals[] = { "RELOAD signal sent — pool sizes, timeouts, weights, probes, TLS certs, and log level will be reloaded" };
    pg_data_row(b, vals, 1);
    pg_cmd_complete_tag(b, "RELOAD 1");
}

/* ============================================================================
 * Admin Authentication Helpers
 * ============================================================================ */

/**
 * @brief Parse a PostgreSQL StartupMessage to extract the "user" parameter.
 *
 * @param params Raw parameter bytes after the 8-byte header.
 * @param plen   Length of the parameter block.
 * @param user   Output buffer for the extracted username.
 * @param usize  Size of the output buffer.
 * @return `true` if a "user" parameter was found.
 */
static bool admin_parse_startup_user(const uint8_t *params, size_t plen,
                                     char *user, size_t usize) {
    const uint8_t *end = params + plen;
    const uint8_t *p = params;
    user[0] = '\0';

    while (p < end && *p != '\0') {
        const char *key = (const char *)p;
        size_t klen = strnlen(key, (size_t)(end - p));
        p += klen + 1;
        if (p >= end) break;

        const char *val = (const char *)p;
        size_t vlen = strnlen(val, (size_t)(end - p));
        p += vlen + 1;

        if (strcmp(key, "user") == 0) {
            snprintf(user, usize, "%s", val);
            return true;
        }
    }
    return false;
}

/**
 * @brief Check whether a username appears in a comma-separated allowlist.
 *
 * @param users Comma-separated list of allowed usernames.
 * @param name  Username to check.
 * @return `true` if the username is in the list.
 */
static bool admin_user_allowed(const char *users, const char *name) {
    if (!users || !name || !*name) return false;

    size_t nlen = strlen(name);
    const char *p = users;
    while (*p) {
        while (*p == ' ' || *p == ',') p++;
        const char *tok = p;
        while (*p && *p != ',') p++;
        size_t tlen = (size_t)(p - tok);
        /* Trim trailing spaces */
        while (tlen > 0 && tok[tlen - 1] == ' ') tlen--;
        if (tlen == nlen && strncmp(tok, name, tlen) == 0) return true;
    }
    return false;
}

/**
 * @brief Send a PG ErrorResponse for authentication failure and log.
 *
 * Uses SQLSTATE 28P01 (invalid_password) for auth failures and 28000
 * (invalid_authorization_specification) for unknown users.
 */
static void admin_send_auth_error(int fd, const char *sqlstate, const char *msg) {
    pgbuf_t wb;
    pgbuf_init(&wb);

    uint32_t mlen = (uint32_t)strlen(msg) + 1;
    uint32_t len = 4 + 2 + 6 + 2 + 6 + 2 + 6 + 2 + mlen + 1;
    pgbuf_addbyte(&wb, 'E');
    pgbuf_add32(&wb, len);
    pgbuf_addbyte(&wb, 'S'); pgbuf_addstr(&wb, "FATAL");
    pgbuf_addbyte(&wb, 'V'); pgbuf_addstr(&wb, "FATAL");
    pgbuf_addbyte(&wb, 'C'); pgbuf_addstr(&wb, sqlstate);
    pgbuf_addbyte(&wb, 'M'); pgbuf_addstr(&wb, msg);
    pgbuf_addbyte(&wb, '\0');

    safe_send(fd, wb.data, wb.len);
    pgbuf_free(&wb);
}

/**
 * @brief Send a PG Authentication message ('R') wrapping an auth payload.
 *
 * @param fd  Socket to write to.
 * @param msg Raw auth payload (starts with int32 auth-type).
 * @param len Length of the payload.
 * @return 0 on success, -1 on write failure.
 */
static int admin_send_auth_msg(int fd, const void *msg, size_t len) {
    uint8_t hdr[5];
    hdr[0] = 'R';
    wr32(hdr + 1, (uint32_t)(4 + len));
    if (safe_send(fd, hdr, 5) < 0) return -1;
    if (len > 0 && safe_send(fd, msg, len) < 0) return -1;
    return 0;
}

/**
 * @brief Receive a PG 'p' (password/SASL) message and extract the SASL data.
 *
 * @param fd        Socket to read from.
 * @param step      0 = SASLInitialResponse, 1 = SASLResponse.
 * @param data_out  Output: SASL data (caller must keel_free).
 * @param data_len  Output: length of extracted SASL data.
 * @return 0 on success, -1 on protocol/IO error.
 */
static int admin_recv_sasl(int fd, int step, void **data_out, size_t *data_len) {
    uint8_t hdr[5];
    if (safe_recv(fd, hdr, 5) < 5) return -1;
    if (hdr[0] != 'p') return -1;

    uint32_t mlen = rd32(hdr + 1);
    if (mlen < 4 || mlen > 65536) return -1;

    size_t payload_len = mlen - 4;
    uint8_t *payload = keel_malloc(payload_len + 1);
    if (!payload) return -1;

    if (safe_recv(fd, payload, payload_len) < (ssize_t)payload_len) {
        keel_free(payload);
        return -1;
    }
    payload[payload_len] = '\0';

    if (step == 0) {
        /* SASLInitialResponse: mechanism\0 + int32(response_len) + response */
        size_t mech_len = strnlen((const char *)payload, payload_len);
        size_t pos = mech_len + 1;  /* skip mechanism + NUL */
        if (pos + 4 > payload_len) { keel_free(payload); return -1; }

        int32_t resp_len = (int32_t)rd32(payload + pos);
        pos += 4;
        if (resp_len < 0 || pos + (size_t)resp_len > payload_len) {
            keel_free(payload);
            return -1;
        }

        void *out = keel_malloc((size_t)resp_len + 1);
        if (!out) { keel_free(payload); return -1; }
        memcpy(out, payload + pos, (size_t)resp_len);
        ((uint8_t *)out)[resp_len] = '\0';
        *data_out = out;
        *data_len = (size_t)resp_len;
        keel_free(payload);
    } else {
        /* SASLResponse: raw SASL data */
        *data_out = payload;
        *data_len = payload_len;
    }
    return 0;
}

/**
 * @brief Drive a full SCRAM-SHA-256 authentication exchange on the admin socket.
 *
 * @param admin Admin context (for auth_mgr).
 * @param fd    Client socket.
 * @param user  Username from the StartupMessage.
 * @return `true` if the client authenticated successfully.
 */
static bool admin_scram_authenticate(keel_admin_t *admin, int fd,
                                     const char *user) {
    keel_auth_context_t *ctx = NULL;
    keel_error_t err = keel_auth_manager_start(admin->auth_mgr, user, &ctx);
    if (err != KEEL_OK || !ctx) {
        admin_send_auth_error(fd, "28000", "admin authentication failed");
        return false;
    }

    /* Step 0: send AuthenticationSASL challenge (type 10). */
    void *msg = NULL;
    size_t msg_len = 0;
    int msg_type = 0;
    err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
    if (err != KEEL_OK) {
        keel_auth_context_free(ctx);
        admin_send_auth_error(fd, "28000", "admin authentication failed");
        return false;
    }
    if (admin_send_auth_msg(fd, msg, msg_len) < 0) {
        keel_free(msg);
        keel_auth_context_free(ctx);
        return false;
    }
    keel_free(msg);

    /* Step 1: receive SASLInitialResponse, feed client-first-message. */
    void *sasl_data = NULL;
    size_t sasl_len = 0;
    if (admin_recv_sasl(fd, 0, &sasl_data, &sasl_len) < 0) {
        keel_auth_context_free(ctx);
        return false;
    }

    keel_auth_state_t state = keel_auth_process(ctx, sasl_data, sasl_len);
    keel_free(sasl_data);

    if (state != KEEL_AUTH_STATE_CHALLENGE) {
        keel_auth_context_free(ctx);
        admin_send_auth_error(fd, "28P01", "password authentication failed");
        return false;
    }

    /* Send AuthenticationSASLContinue (type 11). */
    msg = NULL;
    err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
    if (err != KEEL_OK) {
        keel_auth_context_free(ctx);
        admin_send_auth_error(fd, "28000", "admin authentication failed");
        return false;
    }
    if (admin_send_auth_msg(fd, msg, msg_len) < 0) {
        keel_free(msg);
        keel_auth_context_free(ctx);
        return false;
    }
    keel_free(msg);

    /* Step 2: receive SASLResponse, feed client-final-message. */
    if (admin_recv_sasl(fd, 1, &sasl_data, &sasl_len) < 0) {
        keel_auth_context_free(ctx);
        return false;
    }

    state = keel_auth_process(ctx, sasl_data, sasl_len);
    keel_free(sasl_data);

    if (state != KEEL_AUTH_STATE_SUCCESS) {
        keel_auth_context_free(ctx);
        admin_send_auth_error(fd, "28P01", "password authentication failed");
        return false;
    }

    /* Send AuthenticationSASLFinal (type 12). */
    msg = NULL;
    err = keel_auth_get_message(ctx, &msg, &msg_len, &msg_type);
    if (err != KEEL_OK) {
        keel_auth_context_free(ctx);
        admin_send_auth_error(fd, "28000", "admin authentication failed");
        return false;
    }
    if (admin_send_auth_msg(fd, msg, msg_len) < 0) {
        keel_free(msg);
        keel_auth_context_free(ctx);
        return false;
    }
    keel_free(msg);

    keel_auth_context_free(ctx);
    return true;
}

/* ============================================================================
 * SQL-Syntax Admin Query Dispatch
 * ============================================================================
 *
 * Enables operators to query admin virtual tables using standard SQL instead of
 * PgBouncer-style keywords:
 *
 *   SELECT * FROM stats                       → SHOW STATS
 *   SELECT * FROM servers                     → SHOW SERVERS
 *   SELECT * FROM pools                       → SHOW POOLS
 *   SELECT * FROM clients                     → SHOW CLIENTS
 *   SELECT * FROM config                      → SHOW CONFIG
 *   SELECT * FROM stats_detail                → SHOW STATS_DETAIL
 *   SELECT * FROM latency                     → SHOW LATENCY
 *   SELECT * FROM system                      → SHOW SYSTEM
 *   SELECT * FROM rebalance                   → SHOW REBALANCE
 *   UPDATE config SET value = 'X' WHERE key = 'pool_max_size'
 *   INSERT INTO servers (host, port, ...) VALUES (...)
 *   DELETE FROM servers WHERE index = N
 *
 * Legacy SHOW/SET/PAUSE/etc. commands remain fully supported.
 */

/**
 * @brief Extract the table name from a FROM clause node.
 *
 * @param from  The AST node from a SELECT/UPDATE/DELETE →from or →table field.
 * @return      A NUL-terminated C string (from the arena) or NULL.
 *
 * Handles: TABLE_REF (simple table), or if the node is a list, the first item.
 * Returns NULL for subqueries, joins, and other complex FROM types.
 */
static const char* admin_extract_table_name(keel_sql_node_t* from,
                                            keel_arena_t* arena) {
    if (!from) return NULL;

    /* Direct table reference */
    if (from->kind == KEEL_SQL_NODE_TABLE_REF) {
        keel_sql_table_ref_t* tr = (keel_sql_table_ref_t*)from;
        if (tr->table.data && tr->table.len > 0)
            return keel_arena_sprintf(arena, "%.*s",
                                     (int)tr->table.len, tr->table.data);
    }

    /* FROM may be a list of table refs for multi-table SELECTs */
    if (from->kind == KEEL_SQL_NODE_LIST) {
        keel_sql_list_t* list = (keel_sql_list_t*)from;
        if (list->count > 0 && list->head)
            return admin_extract_table_name(list->head, arena);
    }

    /* JOIN or subquery — not supported in admin context */
    return NULL;
}

/**
 * @brief Extract a simple WHERE key = 'value' predicate from a binary EQ node.
 *
 * @param where   AST WHERE node.
 * @param col_out [out] Column name from the predicate (NUL-terminated).
 * @param val_out [out] String value from the predicate (NUL-terminated), or NULL for int literals.
 * @param arena   Arena for NUL-terminated string allocation.
 * @return        true if a simple `col = 'literal'` was extracted.
 */
static bool admin_extract_eq_where(keel_sql_node_t* where,
                                   const char** col_out,
                                   const char** val_out,
                                   keel_arena_t* arena) {
    if (!where || where->kind != KEEL_SQL_NODE_EXPR_BINARY) return false;

    keel_sql_expr_binary_t* bin = (keel_sql_expr_binary_t*)where;
    if (bin->op != KEEL_SQL_BINOP_EQ) return false;

    /* Expect: column = literal (either order) */
    keel_sql_node_t* col_node = NULL;
    keel_sql_node_t* val_node = NULL;

    if (bin->left && bin->left->kind == KEEL_SQL_NODE_EXPR_COLUMN &&
        bin->right && bin->right->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
        col_node = bin->left;
        val_node = bin->right;
    } else if (bin->left && bin->left->kind == KEEL_SQL_NODE_EXPR_LITERAL &&
               bin->right && bin->right->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
        col_node = bin->right;
        val_node = bin->left;
    } else {
        return false;
    }

    keel_sql_expr_column_t* col = (keel_sql_expr_column_t*)col_node;
    keel_sql_expr_literal_t* lit = (keel_sql_expr_literal_t*)val_node;

    if (col->column.data && col->column.len > 0)
        *col_out = keel_arena_sprintf(arena, "%.*s",
                                      (int)col->column.len, col->column.data);
    else
        return false;

    if (lit->lit_type == KEEL_SQL_LIT_STRING && lit->value.str_val.data)
        *val_out = keel_arena_sprintf(arena, "%.*s",
                                      (int)lit->value.str_val.len,
                                      lit->value.str_val.data);
    else if (lit->lit_type == KEEL_SQL_LIT_INT) {
        /* Integer literals are sometimes used without quotes (WHERE index = 3) */
        *val_out = NULL; /* Caller handles integer case via lit->value.int_val */
        return true;
    } else
        return false;

    return true;
}

/**
 * @brief Dispatch a parsed SQL DML statement to the correct admin handler.
 *
 * @param admin  Admin subsystem handle.
 * @param out    PG wire output buffer.
 * @param ast    Parsed AST root node.
 * @param arena  Arena used during parsing (for string allocation).
 * @return       true if the query was handled; false to fall through to legacy dispatch.
 */
static bool admin_sql_dispatch(keel_admin_t* admin, pgbuf_t* out,
                               keel_sql_node_t* ast, keel_arena_t* arena) {
    if (!ast) return false;

    /* ---- SELECT → virtual table read ---- */
    if (ast->kind == KEEL_SQL_NODE_STMT_SELECT) {
        keel_sql_stmt_select_t* sel = (keel_sql_stmt_select_t*)ast;
        const char* tbl = admin_extract_table_name(sel->from, arena);
        if (!tbl) return false;

        /* Map table names to existing show_* handlers */
        if (strcasecmp(tbl, "stats") == 0)
            show_stats(admin, out);
        else if (strcasecmp(tbl, "stats_detail") == 0)
            show_stats_detail(admin, out);
        else if (strcasecmp(tbl, "servers") == 0)
            show_servers(admin, out);
        else if (strcasecmp(tbl, "pools") == 0)
            show_pools(admin, out);
        else if (strcasecmp(tbl, "clients") == 0)
            show_clients(admin, out);
        else if (strcasecmp(tbl, "config") == 0)
            show_config(admin, out);
        else if (strcasecmp(tbl, "latency") == 0)
            show_latency(admin, out);
        else if (strcasecmp(tbl, "system") == 0)
            show_system(admin, out);
        else if (strcasecmp(tbl, "rebalance") == 0)
            show_rebalance(admin, out);
        else if (strcasecmp(tbl, "shard_rules") == 0)
            show_shard_rules(admin, out);
        else if (strcasecmp(tbl, "query_rules") == 0)
            show_query_rules(admin, out);
        else if (strcasecmp(tbl, "osc_sessions") == 0)
            show_osc_sessions(admin, out);
        else if (strcasecmp(tbl, "cluster") == 0)
            show_cluster(admin, out);
        else if (strcasecmp(tbl, "cluster_config") == 0)
            show_cluster_config(admin, out);
        else if (strcasecmp(tbl, "discovered_peers") == 0)
            show_discovered_peers(admin, out);
        else if (strcasecmp(tbl, "cluster_stats") == 0)
            show_cluster_stats(admin, out);
        else if (strcasecmp(tbl, "tracing") == 0)
            show_tracing(admin, out);
        else if (strcasecmp(tbl, "certificates") == 0)
            show_certificates(admin, out);
        else if (strcasecmp(tbl, "help") == 0)
            show_help(out);
        else if (strcasecmp(tbl, "version") == 0)
            show_version(out);
        else {
            char errmsg[256];
            snprintf(errmsg, sizeof(errmsg),
                     "Unknown admin table: \"%s\"", tbl);
            pg_error(out, errmsg);
        }
        return true;
    }

    /* ---- UPDATE config|servers ---- */
    if (ast->kind == KEEL_SQL_NODE_STMT_UPDATE) {
        keel_sql_stmt_update_t* upd = (keel_sql_stmt_update_t*)ast;
        const char* tbl = admin_extract_table_name(upd->table, arena);
        if (!tbl) return false;

        /* ---- UPDATE servers SET enabled = true/false WHERE index = N ---- */
        if (strcasecmp(tbl, "servers") == 0) {
            /* Extract SET clause: enabled = 'true'/'false' or boolean */
            if (!upd->set_list || upd->set_list->count == 0) {
                pg_error(out, "UPDATE servers requires SET enabled = true/false");
                return true;
            }
            keel_sql_set_item_t* item = (keel_sql_set_item_t*)upd->set_list->head;
            if (!item || !item->column ||
                item->column->kind != KEEL_SQL_NODE_EXPR_COLUMN) {
                pg_error(out, "UPDATE servers SET column must be 'enabled'");
                return true;
            }
            keel_sql_expr_column_t* sc = (keel_sql_expr_column_t*)item->column;
            if (sc->column.len != 7 ||
                strncasecmp(sc->column.data, "enabled", 7) != 0) {
                pg_error(out, "UPDATE servers only supports SET enabled = ...");
                return true;
            }

            /* Determine enable/disable from value */
            bool enable = false;
            if (item->value && item->value->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                keel_sql_expr_literal_t* vl = (keel_sql_expr_literal_t*)item->value;
                if (vl->lit_type == KEEL_SQL_LIT_STRING) {
                    enable = (strncasecmp(vl->value.str_val.data, "true",
                                          vl->value.str_val.len) == 0 ||
                              strncasecmp(vl->value.str_val.data, "on",
                                          vl->value.str_val.len) == 0 ||
                              strncasecmp(vl->value.str_val.data, "1",
                                          vl->value.str_val.len) == 0);
                } else if (vl->lit_type == KEEL_SQL_LIT_INT) {
                    enable = (vl->value.int_val != 0);
                } else if (vl->lit_type == KEEL_SQL_LIT_BOOL) {
                    enable = vl->value.bool_val;
                }
            } else if (item->value &&
                       item->value->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
                /* Parser may treat bare true/false as identifiers */
                keel_sql_expr_column_t* vc = (keel_sql_expr_column_t*)item->value;
                enable = (vc->column.len == 4 &&
                          strncasecmp(vc->column.data, "true", 4) == 0);
            }

            /* Extract WHERE index = N */
            const char* where_col = NULL;
            const char* where_val = NULL;
            bool has_eq = admin_extract_eq_where(upd->where, &where_col,
                                                  &where_val, arena);
            if (!has_eq || strcasecmp(where_col, "index") != 0) {
                pg_error(out, "UPDATE servers requires WHERE index = N");
                return true;
            }
            int idx = 0;
            if (where_val) {
                idx = atoi(where_val);
            } else if (upd->where &&
                       upd->where->kind == KEEL_SQL_NODE_EXPR_BINARY) {
                keel_sql_expr_binary_t* bin =
                    (keel_sql_expr_binary_t*)upd->where;
                keel_sql_node_t* lit = (bin->right &&
                    bin->right->kind == KEEL_SQL_NODE_EXPR_LITERAL) ?
                    bin->right : bin->left;
                if (lit && lit->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                    keel_sql_expr_literal_t* el =
                        (keel_sql_expr_literal_t*)lit;
                    if (el->lit_type == KEEL_SQL_LIT_INT)
                        idx = (int)el->value.int_val;
                }
            }

            if (enable)
                cmd_enable_server(admin, out, idx);
            else
                cmd_disable_server(admin, out, idx);
            return true;
        }

        if (strcasecmp(tbl, "config") != 0) {
            pg_error(out, "UPDATE supports 'config' and 'servers' tables");
            return true;
        }

        /* Extract WHERE key = '...' to identify the setting name */
        const char* where_col = NULL;
        const char* where_val = NULL;
        if (!admin_extract_eq_where(upd->where, &where_col, &where_val, arena) ||
            !where_val) {
            pg_error(out, "UPDATE config requires WHERE key = 'setting_name'");
            return true;
        }
        if (strcasecmp(where_col, "key") != 0) {
            pg_error(out, "UPDATE config WHERE must filter on 'key' column");
            return true;
        }

        /* Extract SET value = '...' from set_list */
        if (!upd->set_list || upd->set_list->count == 0) {
            pg_error(out, "UPDATE config requires SET value = '...'");
            return true;
        }
        keel_sql_set_item_t* item = (keel_sql_set_item_t*)upd->set_list->head;
        if (!item || !item->value ||
            item->value->kind != KEEL_SQL_NODE_EXPR_LITERAL) {
            pg_error(out, "UPDATE config SET value must be a literal");
            return true;
        }
        keel_sql_expr_literal_t* val_lit = (keel_sql_expr_literal_t*)item->value;
        const char* new_val = NULL;
        char int_buf[32];
        if (val_lit->lit_type == KEEL_SQL_LIT_STRING)
            new_val = keel_arena_sprintf(arena, "%.*s",
                                         (int)val_lit->value.str_val.len,
                                         val_lit->value.str_val.data);
        else if (val_lit->lit_type == KEEL_SQL_LIT_INT) {
            snprintf(int_buf, sizeof(int_buf), "%ld",
                     (long)val_lit->value.int_val);
            new_val = int_buf;
        } else {
            pg_error(out, "UPDATE config SET value must be a string or integer");
            return true;
        }

        /* Synthesize "key = value" string and delegate to cmd_set */
        char set_args[384];
        snprintf(set_args, sizeof(set_args), "%.*s = %s",
                 (int)strlen(where_val), where_val, new_val);
        cmd_set(admin, out, set_args);
        return true;
    }

    /* ---- INSERT INTO servers|peers ---- */
    if (ast->kind == KEEL_SQL_NODE_STMT_INSERT) {
        keel_sql_stmt_insert_t* ins = (keel_sql_stmt_insert_t*)ast;
        const char* tbl = admin_extract_table_name(ins->table, arena);
        if (!tbl) return false;

        /* ---- INSERT INTO peers (host) VALUES ('host:port') ---- */
        if (strcasecmp(tbl, "peers") == 0) {
            /* Extract single value from first column/value pair */
            if (!ins->columns || !ins->source) {
                pg_error(out,
                    "INSERT INTO peers (host) VALUES ('host:port')");
                return true;
            }
            keel_sql_list_t* vals_outer = NULL;
            if (ins->source->kind == KEEL_SQL_NODE_LIST)
                vals_outer = (keel_sql_list_t*)ins->source;
            keel_sql_list_t* vals = NULL;
            if (vals_outer && vals_outer->count > 0 &&
                vals_outer->head->kind == KEEL_SQL_NODE_LIST)
                vals = (keel_sql_list_t*)vals_outer->head;
            else if (vals_outer)
                vals = vals_outer;

            if (!vals || vals->count < 1) {
                pg_error(out,
                    "INSERT INTO peers requires VALUES ('host:port')");
                return true;
            }

            /* Extract the host:port value */
            keel_sql_node_t* v0 = vals->head;
            if (!v0 || v0->kind != KEEL_SQL_NODE_EXPR_LITERAL) {
                pg_error(out,
                    "INSERT INTO peers: value must be a string literal");
                return true;
            }
            keel_sql_expr_literal_t* l0 = (keel_sql_expr_literal_t*)v0;
            if (l0->lit_type != KEEL_SQL_LIT_STRING || !l0->value.str_val.data) {
                pg_error(out,
                    "INSERT INTO peers: value must be a string literal");
                return true;
            }

            const char* peer_addr = keel_arena_sprintf(arena, "%.*s",
                (int)l0->value.str_val.len, l0->value.str_val.data);
            cmd_add_peer(admin, out, peer_addr);
            return true;
        }

        if (strcasecmp(tbl, "servers") != 0) {
            pg_error(out, "INSERT supports 'servers' and 'peers' tables");
            return true;
        }

        /* Extract column names and values to build a server definition string.
         * Expected: INSERT INTO servers (host,port,role,weight,...) VALUES (...) */
        if (!ins->columns || !ins->source) {
            pg_error(out, "INSERT INTO servers requires (columns) VALUES (...)");
            return true;
        }

        /* Build "host=X port=Y role=Z weight=W" server definition */
        keel_sql_list_t* cols = ins->columns;
        /* source is a VALUES node (list of row lists) */
        keel_sql_list_t* vals_outer = NULL;
        if (ins->source->kind == KEEL_SQL_NODE_LIST)
            vals_outer = (keel_sql_list_t*)ins->source;

        keel_sql_list_t* vals = NULL;
        if (vals_outer && vals_outer->count > 0 &&
            vals_outer->head->kind == KEEL_SQL_NODE_LIST)
            vals = (keel_sql_list_t*)vals_outer->head;
        else if (vals_outer)
            vals = vals_outer; /* Single row as flat list */

        if (!vals || cols->count != vals->count) {
            pg_error(out, "INSERT INTO servers: column/value count mismatch");
            return true;
        }

        char srv_def[512];
        size_t pos = 0;
        keel_sql_node_t* cn_iter = cols->head;
        keel_sql_node_t* vn_iter = vals->head;
        for (size_t i = 0; i < cols->count && pos < sizeof(srv_def) - 64 && cn_iter && vn_iter; i++, cn_iter = cn_iter->next, vn_iter = vn_iter->next) {
            keel_sql_node_t* cn = cn_iter;
            keel_sql_node_t* vn = vn_iter;

            const char* cname = NULL;
            size_t cname_len = 0;
            if (cn->kind == KEEL_SQL_NODE_EXPR_COLUMN) {
                keel_sql_expr_column_t* ec = (keel_sql_expr_column_t*)cn;
                cname = ec->column.data;
                cname_len = ec->column.len;
            }
            const char* cval = NULL;
            char ibuf[32];
            if (vn->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                keel_sql_expr_literal_t* el = (keel_sql_expr_literal_t*)vn;
                if (el->lit_type == KEEL_SQL_LIT_STRING)
                    cval = keel_arena_sprintf(arena, "%.*s",
                                              (int)el->value.str_val.len,
                                              el->value.str_val.data);
                else if (el->lit_type == KEEL_SQL_LIT_INT) {
                    snprintf(ibuf, sizeof(ibuf), "%ld",
                             (long)el->value.int_val);
                    cval = ibuf;
                }
            }

            if (cname && cval) {
                if (pos > 0)
                    pos += (size_t)snprintf(srv_def + pos,
                                            sizeof(srv_def) - pos, " ");
                pos += (size_t)snprintf(srv_def + pos,
                                        sizeof(srv_def) - pos, "%.*s=%s",
                                        (int)cname_len, cname, cval);
            }
        }
        srv_def[pos] = '\0';
        cmd_add_server(admin, out, srv_def);
        return true;
    }

    /* ---- DELETE FROM servers|clients|peers ---- */
    if (ast->kind == KEEL_SQL_NODE_STMT_DELETE) {
        keel_sql_stmt_delete_t* del = (keel_sql_stmt_delete_t*)ast;
        const char* tbl = admin_extract_table_name(del->table, arena);
        if (!tbl) return false;

        if (strcasecmp(tbl, "servers") == 0) {
            const char* where_col = NULL;
            const char* where_val = NULL;
            bool has_eq = admin_extract_eq_where(del->where, &where_col,
                                                  &where_val, arena);
            if (!has_eq || strcasecmp(where_col, "index") != 0) {
                pg_error(out, "DELETE FROM servers requires WHERE index = N");
                return true;
            }

            int idx = 0;
            if (where_val)
                idx = atoi(where_val);
            else if (del->where &&
                     del->where->kind == KEEL_SQL_NODE_EXPR_BINARY) {
                keel_sql_expr_binary_t* bin =
                    (keel_sql_expr_binary_t*)del->where;
                keel_sql_node_t* lit = (bin->right &&
                    bin->right->kind == KEEL_SQL_NODE_EXPR_LITERAL) ?
                    bin->right : bin->left;
                if (lit && lit->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                    keel_sql_expr_literal_t* el =
                        (keel_sql_expr_literal_t*)lit;
                    if (el->lit_type == KEEL_SQL_LIT_INT)
                        idx = (int)el->value.int_val;
                }
            }

            cmd_remove_server(admin, out, idx);
            return true;
        }

        /* ---- DELETE FROM clients WHERE id = N ---- */
        if (strcasecmp(tbl, "clients") == 0) {
            const char* where_col = NULL;
            const char* where_val = NULL;
            bool has_eq = admin_extract_eq_where(del->where, &where_col,
                                                  &where_val, arena);
            if (!has_eq || strcasecmp(where_col, "id") != 0) {
                pg_error(out, "DELETE FROM clients requires WHERE id = N");
                return true;
            }

            uint64_t sid = 0;
            if (where_val) {
                sid = strtoull(where_val, NULL, 10);
            } else if (del->where &&
                       del->where->kind == KEEL_SQL_NODE_EXPR_BINARY) {
                keel_sql_expr_binary_t* bin =
                    (keel_sql_expr_binary_t*)del->where;
                keel_sql_node_t* lit = (bin->right &&
                    bin->right->kind == KEEL_SQL_NODE_EXPR_LITERAL) ?
                    bin->right : bin->left;
                if (lit && lit->kind == KEEL_SQL_NODE_EXPR_LITERAL) {
                    keel_sql_expr_literal_t* el =
                        (keel_sql_expr_literal_t*)lit;
                    if (el->lit_type == KEEL_SQL_LIT_INT)
                        sid = (uint64_t)el->value.int_val;
                }
            }

            cmd_kill_client(admin, out, sid);
            return true;
        }

        /* ---- DELETE FROM peers WHERE host = 'host:port' ---- */
        if (strcasecmp(tbl, "peers") == 0) {
            const char* where_col = NULL;
            const char* where_val = NULL;
            bool has_eq = admin_extract_eq_where(del->where, &where_col,
                                                  &where_val, arena);
            if (!has_eq || !where_val ||
                strcasecmp(where_col, "host") != 0) {
                pg_error(out,
                    "DELETE FROM peers requires WHERE host = 'host:port'");
                return true;
            }

            cmd_remove_peer(admin, out, where_val);
            return true;
        }

        pg_error(out,
            "DELETE supports 'servers', 'clients', and 'peers' tables");
        return true;
    }

    return false; /* Not a DML statement — fall through to legacy dispatch */
}

/* ============================================================================
 * PG Admin Connection Handler
 * ============================================================================ */

/**
 * @brief Serve one accepted PostgreSQL-wire admin connection.
 *
 * @param admin Admin subsystem handle.
 * @param fd Accepted client socket.
 * @return Nothing.
 *
 * Protocol flow:
 * - Read startup packet.
 * - Optionally reject SSL negotiation with a one-byte `N`.
 * - Send a minimal successful startup sequence.
 * - Enter a simple-query loop supporting only `Q` messages.
 * - Dispatch commands, optionally convert results to JSON, append
 *   `ReadyForQuery`, and send the response.
 *
 * Errors and corner cases:
 * - Unsupported message types are skipped and answered with an error.
 * - Partial reads, EOF, or send failure terminate the connection.
 * - Only simple query protocol is supported; extended query protocol is not.
 * - `FORMAT JSON` is implemented as a post-processing suffix, not full SQL.
 */
static void handle_admin_pg(keel_admin_t *admin, int fd) {
    /* Step 1: read and parse the startup message. */
    uint8_t hdr[8];
    if (safe_recv(fd, hdr, 8) < 8) return;

    uint32_t startup_len = rd32(hdr);
    uint32_t proto_ver   = rd32(hdr + 4);

    /* The admin socket is plaintext-only, so reject SSLRequest explicitly. */
    if (proto_ver == 80877103u) {
        uint8_t no_ssl = 'N';
        safe_send(fd, &no_ssl, 1);
        /* Re-read startup message */
        if (safe_recv(fd, hdr, 8) < 8) return;
        startup_len = rd32(hdr);
        proto_ver   = rd32(hdr + 4);
    }

    /* Read startup parameters to extract the "user" field. */
    char admin_user[64] = "";
    if (startup_len > 8 && startup_len <= 8 + 4096) {
        size_t plen = startup_len - 8;
        uint8_t params[4096];
        if (safe_recv(fd, params, plen) < (ssize_t)plen) return;
        admin_parse_startup_user(params, plen, admin_user, sizeof(admin_user));
    } else if (startup_len > 8) {
        /* Oversized startup — drain and reject */
        size_t rem = startup_len - 8;
        uint8_t skip[4096];
        while (rem > 0) {
            size_t chunk = rem > sizeof(skip) ? sizeof(skip) : rem;
            if (safe_recv(fd, skip, chunk) <= 0) return;
            rem -= chunk;
        }
    }

    (void)proto_ver;

    /* Step 2: authenticate the client. */
    if (admin->auth_mgr) {
        /* Check username against the allowlist. */
        if (!admin_user[0] || !admin_user_allowed(admin->cfg.admin_users, admin_user)) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "Admin auth rejected: user \"%s\" not in allowlist",
                          admin_user);
            admin_send_auth_error(fd, "28000",
                                 "user not permitted to connect to admin console");
            return;
        }

        if (!admin_scram_authenticate(admin, fd, admin_user)) {
            KEEL_LOG_WARN(KEEL_LOG_CAT_CORE, "Admin SCRAM auth failed for user \"%s\"",
                          admin_user);
            return;
        }
        KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Admin authenticated user \"%s\"", admin_user);
    }

    /* Step 3: advertise a minimally sane backend identity to the client. */
    pgbuf_t wb;
    pgbuf_init(&wb);

    pg_auth_ok(&wb);
    pg_param_status(&wb, "server_version", KEEL_VERSION);
    pg_param_status(&wb, "application_name", "keel-admin");
    pg_param_status(&wb, "server_encoding", "UTF8");
    pg_param_status(&wb, "client_encoding", "UTF8");
    pg_param_status(&wb, "is_superuser", "on");
    pg_backend_key(&wb);
    pg_ready(&wb);

    if (safe_send(fd, wb.data, wb.len) < 0) { pgbuf_free(&wb); return; }
    wb.len = 0;

    /* Step 4: handle one simple query at a time until the client disconnects. */
    while (admin->running) {
        /* Every frontend message starts with a one-byte tag and int32 length. */
        uint8_t msg_hdr[5];
        if (safe_recv(fd, msg_hdr, 5) < 5) break;

        uint8_t mtype = msg_hdr[0];
        uint32_t mlen = rd32(msg_hdr + 1);

        if (mtype == 'X') break;  /* Terminate */

        if (mtype != 'Q') {
            /* Skip non-query messages */
            if (mlen > 4) {
                size_t skip = mlen - 4;
                uint8_t discard[4096];
                while (skip > 0) {
                    size_t chunk = skip > sizeof(discard) ? sizeof(discard) : skip;
                    if (safe_recv(fd, discard, chunk) <= 0) goto done;
                    skip -= chunk;
                }
            }
            wb.len = 0;
            pg_error(&wb, "Only SimpleQuery (SHOW ...) is supported");
            pg_ready(&wb);
            safe_send(fd, wb.data, wb.len);
            continue;
        }

        /* SimpleQuery carries the SQL text as a length-delimited C string. */
        size_t qlen = mlen - 4;
        char *query = keel_calloc(1, qlen + 1);
        if (!query) break;
        if (safe_recv(fd, query, qlen) < (ssize_t)qlen) { keel_free(query); break; }
        query[qlen] = '\0';

        /* Normalize input so operators can type natural psql-style commands. */
        size_t slen = strlen(query);
        while (slen > 0 && (query[slen-1] == '\0' || query[slen-1] == ';' ||
                            query[slen-1] == ' '  || query[slen-1] == '\n' ||
                            query[slen-1] == '\r' || query[slen-1] == '\t'))
            query[--slen] = '\0';

        /* `FORMAT JSON` is parsed as a suffix rather than by a SQL grammar. */
        bool json_mode = false;
        slen = strlen(query);
        if (slen > 12 && strcasecmp(query + slen - 11, "FORMAT JSON") == 0) {
            json_mode = true;
            query[slen - 11] = '\0';
            /* Trim trailing whitespace after removing FORMAT JSON */
            slen = strlen(query);
            while (slen > 0 && (query[slen-1] == ' ' || query[slen-1] == '\t'))
                query[--slen] = '\0';
        }

        /* Build the normal PG result first, then transform it if JSON was requested. */
        pgbuf_t tmp;
        pgbuf_init(&tmp);
        pgbuf_t *out = json_mode ? &tmp : &wb;
        wb.len = 0;

        /* Command matching is deliberately simple, flat, and case-insensitive. */
        if (strcasecmp(query, "SHOW HELP") == 0)
            show_help(out);
        else if (strcasecmp(query, "SHOW VERSION") == 0)
            show_version(out);
        else if (strcasecmp(query, "SHOW STATS") == 0)
            show_stats(admin, out);
        else if (strcasecmp(query, "SHOW STATS_DETAIL") == 0)
            show_stats_detail(admin, out);
        else if (strcasecmp(query, "SHOW SERVERS") == 0)
            show_servers(admin, out);
        else if (strcasecmp(query, "SHOW POOLS") == 0)
            show_pools(admin, out);
        else if (strcasecmp(query, "SHOW CLIENTS") == 0)
            show_clients(admin, out);
        else if (strcasecmp(query, "SHOW CONFIG") == 0)
            show_config(admin, out);
        else if (strcasecmp(query, "SHOW LATENCY") == 0)
            show_latency(admin, out);
        else if (strcasecmp(query, "SHOW SYSTEM") == 0)
            show_system(admin, out);
        else if (strcasecmp(query, "SHOW REBALANCE") == 0)
            show_rebalance(admin, out);
        else if (strcasecmp(query, "SHOW SHARD RULES") == 0)
            show_shard_rules(admin, out);
        else if (strcasecmp(query, "SHOW QUERY RULES") == 0)
            show_query_rules(admin, out);
        else if (strcasecmp(query, "SHOW CACHE STATS") == 0)
            show_cache_stats(admin, out);
        else if (strcasecmp(query, "FLUSH QUERY CACHE") == 0)
            cmd_flush_query_cache(admin, out);
        else if (strcasecmp(query, "SHOW THROTTLE RULES") == 0)
            show_throttle_rules(admin, out);
        else if (strcasecmp(query, "SHOW TOPOLOGY") == 0)
            show_topology(admin, out);
        else if (strcasecmp(query, "SHOW OSC SESSIONS") == 0)
            show_osc_sessions(admin, out);
        else if (strncasecmp(query, "EXPLAIN SHARD PLAN FOR ", 23) == 0)
            show_shard_plan(admin, out, query + 23);
        else if (strcasecmp(query, "SHOW CLUSTER") == 0)
            show_cluster(admin, out);
        else if (strcasecmp(query, "SHOW CLUSTER CONFIG") == 0)
            show_cluster_config(admin, out);
        else if (strcasecmp(query, "SHOW DISCOVERED PEERS") == 0)
            show_discovered_peers(admin, out);
        else if (strcasecmp(query, "SHOW CLUSTER STATS") == 0)
            show_cluster_stats(admin, out);
        else if (strcasecmp(query, "SHOW TRACING") == 0)
            show_tracing(admin, out);
        else if (strcasecmp(query, "SHOW CERTIFICATES") == 0)
            show_certificates(admin, out);
        else if (strncasecmp(query, "ADD PEER ", 9) == 0)
            cmd_add_peer(admin, out, query + 9);
        else if (strncasecmp(query, "REMOVE PEER ", 12) == 0)
            cmd_remove_peer(admin, out, query + 12);
        else if (strcasecmp(query, "PAUSE") == 0)
            cmd_pause(admin, out);
        else if (strcasecmp(query, "RESUME") == 0)
            cmd_resume(admin, out);
        else if (strncasecmp(query, "DISABLE SERVER ", 15) == 0) {
            int idx = atoi(query + 15);
            cmd_disable_server(admin, out, idx);
        }
        else if (strncasecmp(query, "ENABLE SERVER ", 14) == 0) {
            int idx = atoi(query + 14);
            cmd_enable_server(admin, out, idx);
        }
        else if (strncasecmp(query, "ADD SERVER ", 11) == 0)
            cmd_add_server(admin, out, query + 11);
        else if (strncasecmp(query, "REMOVE SERVER ", 14) == 0) {
            int idx = atoi(query + 14);
            cmd_remove_server(admin, out, idx);
        }
        else if (strncasecmp(query, "KILL CLIENT ", 12) == 0) {
            uint64_t sid = strtoull(query + 12, NULL, 10);
            cmd_kill_client(admin, out, sid);
        }
        else if (strcasecmp(query, "DRAIN") == 0)
            cmd_drain(admin, out, NULL);
        else if (strncasecmp(query, "DRAIN ", 6) == 0)
            cmd_drain(admin, out, query + 6);
        else if (strncasecmp(query, "SET ", 4) == 0)
            cmd_set(admin, out, query + 4);
        else if (strcasecmp(query, "RELOAD") == 0)
            cmd_reload(admin, out);
        else if (strcasecmp(query, "RESTART WORKERS") == 0)
            cmd_restart_workers(admin, out, NULL);
        else if (strncasecmp(query, "RESTART WORKERS ", 16) == 0)
            cmd_restart_workers(admin, out, query + 16);
        else {
            /* Try parsing as a SQL DML statement (ProxySQL-style admin tables).
             * Falls back to "Unknown command" if the parse fails or the
             * statement type is not a recognized DML (SELECT/UPDATE/INSERT/DELETE). */
            bool sql_handled = false;
            slen = strlen(query);
            if (slen > 0) {
                keel_arena_t* arena = keel_arena_create(4096);
                if (arena) {
                    keel_sql_parser_t parser;
                    keel_str_t sql = { .data = query, .len = slen };
                    keel_sql_parser_init(&parser, sql, arena);
                    keel_sql_node_t* ast = keel_sql_parse(&parser);
                    if (!parser.has_error && ast)
                        sql_handled = admin_sql_dispatch(admin, out, ast, arena);
                    keel_arena_destroy(arena);
                }
            }
            if (!sql_handled) {
                char errmsg[256];
                snprintf(errmsg, sizeof(errmsg),
                         "Unknown command: \"%s\". Type SHOW HELP for available commands.",
                         query);
                pg_error(out, errmsg);
            }
        }

        /* JSON mode preserves existing command builders and converts after dispatch. */
        if (json_mode) {
            pgbuf_to_json(&wb, &tmp);
            pgbuf_free(&tmp);
        }

        pg_ready(&wb);
        safe_send(fd, wb.data, wb.len);
        keel_free(query);
    }

done:
    pgbuf_free(&wb);
}

/* ============================================================================
 * Prometheus /metrics handler
 * ============================================================================ */

/**
 * @brief Serialize the current metrics snapshot as a Prometheus text response.
 *
 * @param admin Admin subsystem handle.
 * @param fd Accepted HTTP client socket.
 * @return Nothing.
 *
 * Behavior:
 * - Builds the full body in memory with `open_memstream()`.
 * - Writes per-worker counters, aggregate totals, derived gauges, latency
 *   summaries, optional system metrics, and uptime.
 * - Sends a complete HTTP/1.1 response and frees the temporary body.
 *
 * Corner cases:
 * - If the metrics stream cannot be opened, the function silently returns.
 * - Metrics are best-effort snapshots; values may change while serialization is
 *   in progress.
 */
/**
 * @brief Write a keel_histogram_t as a Prometheus histogram metric.
 *
 * Converts log2 buckets to cumulative Prometheus histogram_bucket lines
 * with human-meaningful nanosecond boundaries plus _sum, _count, and
 * summary quantiles (p50/p95/p99) for backward compatibility.
 */
static void prom_write_histogram(FILE *f, const char *name, const char *help,
                                  const keel_histogram_t *h) {
    keel_histogram_snapshot_t hs;
    keel_histogram_snapshot(h, &hs);

    fprintf(f, "# HELP %s %s\n", name, help);
    fprintf(f, "# TYPE %s histogram\n", name);

    /* Prometheus histogram buckets: cumulative count for each le boundary.
     * We map selected log2 bucket boundaries to readable ns thresholds:
     *   bucket 10 = 1024 ns ≈ 1 μs
     *   bucket 13 = 8192 ns ≈ 10 μs
     *   bucket 17 = 131072 ns ≈ 100 μs
     *   bucket 20 = 1048576 ns ≈ 1 ms
     *   bucket 23 = 8388608 ns ≈ 10 ms
     *   bucket 27 = 134217728 ns ≈ 100 ms
     *   bucket 30 = 1073741824 ns ≈ 1 s
     *   bucket 33 = 8589934592 ns ≈ 10 s
     */
    static const struct { int bucket; const char *le; } bounds[] = {
        { 10, "1000"        },  /* 1 μs */
        { 13, "10000"       },  /* 10 μs */
        { 17, "100000"      },  /* 100 μs */
        { 20, "1000000"     },  /* 1 ms */
        { 23, "10000000"    },  /* 10 ms */
        { 27, "100000000"   },  /* 100 ms */
        { 30, "1000000000"  },  /* 1 s */
        { 33, "10000000000" },  /* 10 s */
    };

    uint64_t cum = 0;
    size_t bi = 0;
    for (int b = 0; b < KEEL_HISTOGRAM_BUCKETS && bi < sizeof(bounds)/sizeof(bounds[0]); b++) {
        cum += hs.buckets[b];
        if (b == bounds[bi].bucket) {
            fprintf(f, "%s_bucket{le=\"%s\"} %llu\n",
                    name, bounds[bi].le, (unsigned long long)cum);
            bi++;
        }
    }
    /* Remaining buckets for +Inf */
    for (int b = (bi < sizeof(bounds)/sizeof(bounds[0]) ?
                  bounds[sizeof(bounds)/sizeof(bounds[0])-1].bucket + 1 : 0);
         b < KEEL_HISTOGRAM_BUCKETS; b++)
        cum += hs.buckets[b];
    fprintf(f, "%s_bucket{le=\"+Inf\"} %llu\n", name, (unsigned long long)cum);
    fprintf(f, "%s_sum %llu\n", name, (unsigned long long)hs.sum);
    fprintf(f, "%s_count %llu\n", name, (unsigned long long)hs.count);

    /* Also emit summary quantiles for backward compatibility */
    fprintf(f, "%s{quantile=\"0.5\"} %llu\n",
            name, (unsigned long long)keel_histogram_percentile(h, 0.50));
    fprintf(f, "%s{quantile=\"0.95\"} %llu\n",
            name, (unsigned long long)keel_histogram_percentile(h, 0.95));
    fprintf(f, "%s{quantile=\"0.99\"} %llu\n",
            name, (unsigned long long)keel_histogram_percentile(h, 0.99));
}

static void prom_write_metrics(keel_admin_t *admin, int fd, bool accept_gzip) {
    keel_stats_collector_t *sc = keel_engine_get_stats_collector(admin->engine);
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    uint64_t now_ns = admin_now_ns();

    /* Build the body first so Content-Length is exact and writes stay simple. */
    char *body = NULL;
    size_t body_len = 0;
    FILE *f = open_memstream(&body, &body_len);
    if (!f) return;
    (void)accept_gzip;  /* used below after body is built */

    /* Export both per-worker label sets and already-aggregated totals. */
    if (sc) {
        keel_stats_snapshot_t snap;
        keel_stats_snapshot_take(sc, &snap);
        keel_tls_stats_t tls_stats = keel_tls_get_stats();
        keel_ktls_stats_t ktls_stats = keel_ktls_get_stats();

#define PROM_COUNTER(metric, help, field) do { \
    fprintf(f, "# HELP keel_" metric " " help "\n"); \
    fprintf(f, "# TYPE keel_" metric " counter\n"); \
    for (uint32_t i = 0; i < nw; i++) { \
        keel_stats_ctx_t *ctx = keel_stats_collector_get_ctx(sc, i); \
        if (ctx) fprintf(f, "keel_" metric "{worker=\"%u\"} %llu\n", \
            i, (unsigned long long)keel_counter_get(&ctx->basic.field)); \
    } \
    fprintf(f, "keel_" metric "_total %llu\n", \
        (unsigned long long)keel_counter_get(&snap.basic.field)); \
} while (0)

#define PROM_GAUGE(metric, help, field) do { \
    fprintf(f, "# HELP keel_" metric " " help "\n"); \
    fprintf(f, "# TYPE keel_" metric " gauge\n"); \
    for (uint32_t i = 0; i < nw; i++) { \
        keel_stats_ctx_t *ctx = keel_stats_collector_get_ctx(sc, i); \
        if (ctx) fprintf(f, "keel_" metric "{worker=\"%u\"} %lld\n", \
            i, (long long)keel_gauge_get(&ctx->basic.field)); \
    } \
    fprintf(f, "keel_" metric "_total %lld\n", \
        (long long)keel_gauge_get(&snap.basic.field)); \
} while (0)

        PROM_COUNTER("sessions_created",  "Total frontend sessions created",   sessions_created);
        PROM_COUNTER("sessions_closed",   "Total frontend sessions closed",    sessions_closed);
        PROM_GAUGE  ("sessions_active",   "Currently active sessions",         sessions_active);

        PROM_COUNTER("pool_borrows",      "Backend connections borrowed",      pool_borrows);
        PROM_COUNTER("pool_returns",      "Backend connections returned",      pool_returns);
        PROM_COUNTER("pool_hits",         "Pool had idle connection ready",    pool_hits);
        PROM_COUNTER("pool_misses",       "Pool empty, created new backend",   pool_misses);
        PROM_COUNTER("pool_creates",      "Backend connections opened",        pool_creates);
        PROM_COUNTER("pool_destroys",     "Backend connections destroyed",     pool_destroys);
        PROM_COUNTER("pool_borrow_attempts", "Pool borrow decisions attempted", pool_borrow_attempts);
        PROM_COUNTER("pool_borrow_exact_state_match", "Borrows satisfied by exact session-state match", pool_borrow_exact_state_match);
        PROM_COUNTER("pool_borrow_exact_stmt_match", "Borrows satisfied by exact prepared-statement match", pool_borrow_exact_stmt_match);
        PROM_COUNTER("pool_borrow_state_replay", "Borrows requiring state replay", pool_borrow_state_replay);
        PROM_COUNTER("pool_borrow_stmt_replay", "Borrows requiring prepared-statement replay", pool_borrow_stmt_replay);
        PROM_COUNTER("pool_borrow_cleanup_required", "Borrows requiring setup cleanup before use", pool_borrow_cleanup_required);
        PROM_COUNTER("backend_borrow_total_success", "Borrow attempts that passed lifecycle predicate", backend_borrow_success);
        PROM_COUNTER("backend_borrow_total_failed_incompatible", "Borrow attempts rejected by lifecycle incompatibility", backend_borrow_failed_incompatible);
        PROM_COUNTER("backend_borrow_total_failed_quarantined", "Borrow attempts rejected because backend was quarantined", backend_borrow_failed_quarantined);
        PROM_COUNTER("pool_wait_queue_enqueued", "Sessions enqueued waiting for backend", pool_wait_queue_enqueued);
        PROM_COUNTER("pool_wait_queue_full_rejects", "Pool wait enqueue attempts rejected because queue was full", pool_wait_queue_full_rejects);
        PROM_COUNTER("pool_wait_resume_success", "Pool wait callbacks that resumed with a backend", pool_wait_resume_success);
        PROM_COUNTER("pool_wait_resume_requeues", "Pool wait callbacks that requeued because no backend was available", pool_wait_resume_requeues);
        PROM_COUNTER("pool_wait_timeout_events", "Pool waiters expired by wait_timeout_ms", pool_wait_timeout_events);
        PROM_COUNTER("pool_wait_cancelled", "Pool waiters cancelled because session closed", pool_wait_cancelled);

        PROM_COUNTER("queries_total",     "Total queries routed",              queries_total);
        PROM_COUNTER("queries_read",      "Read-only queries",                 queries_read);
        PROM_COUNTER("queries_write",     "Write queries",                     queries_write);
        PROM_COUNTER("queries_tx",        "Explicit transactions",             queries_tx);

        PROM_COUNTER("errors_total",      "Total errors",                      errors_total);
        PROM_COUNTER("errors_auth",       "Authentication failures",           errors_auth);
        PROM_COUNTER("errors_proto",      "Protocol errors",                   errors_proto);
        PROM_COUNTER("errors_backend",    "Backend errors",                    errors_backend);
        PROM_COUNTER("errors_timeout",    "Timeout errors",                    errors_timeout);

        PROM_COUNTER("bytes_recv",        "Bytes received from frontends",     bytes_recv);
        PROM_COUNTER("bytes_sent",        "Bytes sent to frontends",           bytes_sent);
        PROM_COUNTER("bytes_backend_recv","Bytes received from backends",      bytes_backend_recv);
        PROM_COUNTER("bytes_backend_sent","Bytes sent to backends",            bytes_backend_sent);

        PROM_COUNTER("loop_iterations",   "Event loop iterations",             loop_iterations);
        PROM_COUNTER("ops_submitted",     "io_uring SQEs submitted",           ops_submitted);
        PROM_COUNTER("ops_completed",     "CQEs reaped",                       ops_completed);
        PROM_COUNTER("proxy_state_desync_total", "Protocol state desynchronization events", proxy_state_desync_total);
        PROM_COUNTER("proxy_orphaned_transactions_total", "Sessions closed with open backend transaction", proxy_orphaned_transactions_total);
        PROM_COUNTER("proxy_backend_reuse_failure_total", "Backend cleanup/reuse failures", proxy_backend_reuse_failure_total);
        PROM_COUNTER("proxy_io_uring_sq_overflow_total", "io_uring SQ overflow events sampled by workers", proxy_io_uring_sq_overflow_total);
        PROM_COUNTER("discard_all_count", "Full backend cleanup commands issued", discard_all_count);
        PROM_COUNTER("discard_all_failure", "Full backend cleanup failures", discard_all_failure);
        PROM_COUNTER("state_sync_count", "Session-state sync replays issued", state_sync_count);
        PROM_COUNTER("backend_close_dead_idle", "Idle backends closed after liveness failure", backend_close_dead_idle);
        PROM_COUNTER("backend_close_cleanup_error", "Backends closed after cleanup/protocol error", backend_close_cleanup_error);
        PROM_COUNTER("backend_close_cleanup_timeout", "Backends closed after cleanup timeout", backend_close_cleanup_timeout);
        PROM_COUNTER("backend_close_client_disconnect", "Backends closed because owning client disconnected", backend_close_client_disconnect);
        PROM_COUNTER("cleaning_timeout_total", "Backend cleanup timeout events", cleaning_timeout_total);
        PROM_COUNTER("pin_reason_transaction", "Transaction pin activations", pin_reason_transaction);
        PROM_COUNTER("pin_reason_extended_protocol", "Extended protocol pin activations", pin_reason_extended_protocol);
        PROM_COUNTER("pin_reason_prepared_stmt", "Prepared statement pin activations", pin_reason_prepared_stmt);
        PROM_COUNTER("pin_reason_other", "Other pin reason activations", pin_reason_other);
        PROM_COUNTER("commit_in_doubt_started", "Commit-in-doubt recovery sessions started", commit_in_doubt_started);
        PROM_COUNTER("commit_in_doubt_resolved", "Commit-in-doubt recovery sessions resolved", commit_in_doubt_resolved);
        PROM_COUNTER("commit_in_doubt_failed", "Commit-in-doubt recovery sessions unresolved or failed", commit_in_doubt_failed);
        PROM_GAUGE("proxy_buffer_pool_utilization_bytes", "Recv buffer pool utilization in bytes", proxy_buffer_pool_utilization_bytes);
        PROM_GAUGE("proxy_connection_age_seconds", "Oldest active frontend connection age", proxy_connection_age_seconds);
        PROM_GAUGE("proxy_heartbeat_last_ns", "Last worker heartbeat monotonic timestamp", proxy_heartbeat_last_ns);
        PROM_GAUGE("sessions_pinned", "Sessions with any active pin reason", sessions_pinned);
        PROM_GAUGE("sessions_pinned_transaction", "Sessions pinned by transaction state", sessions_pinned_transaction);
        PROM_GAUGE("sessions_pinned_extended_protocol", "Sessions pinned by extended protocol", sessions_pinned_extended_protocol);
        PROM_GAUGE("sessions_pinned_prepared_stmt", "Sessions pinned by prepared statements", sessions_pinned_prepared_stmt);
        PROM_GAUGE("sessions_commit_in_doubt", "Sessions currently resolving commit outcome", sessions_commit_in_doubt);
        PROM_GAUGE("backends_cleaning", "Backends in cleanup state machine", backends_cleaning);

        PROM_COUNTER("migrations_sent",   "Sessions migrated to another worker", migrations_sent);
        PROM_COUNTER("migrations_received","Sessions received from another worker", migrations_received);
        PROM_COUNTER("rebalance_checks",  "Rebalance timer ticks",               rebalance_checks);
        PROM_COUNTER("rebalance_migrations","Sessions migrated by auto-rebalance", rebalance_migrations);
        PROM_COUNTER("rebalance_skipped", "Rebalance checks skipped (within threshold)", rebalance_skipped);
        PROM_COUNTER("notify_relayed",    "NotificationResponse messages relayed to LISTEN clients", notify_relayed);
        PROM_COUNTER("osc_sessions_detected", "Sessions identified as Online Schema Change tool connections (gh-ost/pt-osc)", osc_sessions_detected);

#undef PROM_COUNTER
#undef PROM_GAUGE

        {
            uint64_t backend_active = 0;
            uint64_t sq_overflow_total = 0;
            uint64_t stalled_workers = 0;
            uint64_t stale_conn_workers = 0;
            int64_t sessions_active = keel_gauge_get(&snap.basic.sessions_active);
            int64_t sticky_sessions = keel_gauge_get(&snap.basic.sessions_pinned);

            for (uint32_t i = 0; i < nw; i++) {
                const keel_worker_t* w = keel_engine_get_worker(admin->engine, i);
                if (w) {
                    for (size_t s = 0; s < w->server_pool_count; s++) {
                        if (w->server_pools && w->server_pools[s])
                            backend_active += w->server_pools[s]->active_count;
                    }
                    if (w->reactor) {
                        keel_reactor_stats_t rst;
                        memset(&rst, 0, sizeof(rst));
                        keel_reactor_get_stats(w->reactor, &rst);
                        sq_overflow_total += rst.sq_overflow;
                    }
                }

                keel_stats_ctx_t *ctx = keel_stats_collector_get_ctx(sc, i);
                if (!ctx)
                    continue;
                int64_t hb = keel_gauge_get(&ctx->basic.proxy_heartbeat_last_ns);
                if (hb <= 0 || now_ns <= (uint64_t)hb || (now_ns - (uint64_t)hb) > 20000000000ULL)
                    stalled_workers++;
                if (keel_gauge_get(&ctx->basic.proxy_connection_age_seconds) > 300)
                    stale_conn_workers++;
            }

            fprintf(f, "# HELP proxy_state_desync_total Protocol state desynchronization events\n");
            fprintf(f, "# TYPE proxy_state_desync_total counter\n");
            fprintf(f, "proxy_state_desync_total %llu\n",
                (unsigned long long)keel_counter_get(&snap.basic.proxy_state_desync_total));

            fprintf(f, "# HELP proxy_orphaned_transactions_total Sessions closed with open backend transaction\n");
            fprintf(f, "# TYPE proxy_orphaned_transactions_total counter\n");
            fprintf(f, "proxy_orphaned_transactions_total %llu\n",
                (unsigned long long)keel_counter_get(&snap.basic.proxy_orphaned_transactions_total));

            fprintf(f, "# HELP proxy_backend_reuse_failure_total Backend cleanup/reuse failures\n");
            fprintf(f, "# TYPE proxy_backend_reuse_failure_total counter\n");
            fprintf(f, "proxy_backend_reuse_failure_total %llu\n",
                (unsigned long long)keel_counter_get(&snap.basic.proxy_backend_reuse_failure_total));

            fprintf(f, "# HELP proxy_buffer_pool_utilization_bytes Active recv-context buffer usage in bytes\n");
            fprintf(f, "# TYPE proxy_buffer_pool_utilization_bytes gauge\n");
            fprintf(f, "proxy_buffer_pool_utilization_bytes %lld\n",
                (long long)keel_gauge_get(&snap.basic.proxy_buffer_pool_utilization_bytes));

            fprintf(f, "# HELP proxy_multiplex_ratio Frontend active sessions divided by active backend connections\n");
            fprintf(f, "# TYPE proxy_multiplex_ratio gauge\n");
            fprintf(f, "proxy_multiplex_ratio %.6f\n",
                (double)sessions_active / (double)(backend_active ? backend_active : 1ULL));

            fprintf(f, "# HELP proxy_io_uring_sq_overflow_total io_uring SQ ring overflow events\n");
            fprintf(f, "# TYPE proxy_io_uring_sq_overflow_total counter\n");
            fprintf(f, "proxy_io_uring_sq_overflow_total %llu\n", (unsigned long long)sq_overflow_total);

            fprintf(f, "# HELP proxy_connection_age_seconds Oldest active frontend connection age\n");
            fprintf(f, "# TYPE proxy_connection_age_seconds gauge\n");
            fprintf(f, "proxy_connection_age_seconds %lld\n",
                (long long)keel_gauge_get(&snap.basic.proxy_connection_age_seconds));

            fprintf(f, "# HELP proxy_sticky_sessions Sessions currently pinned/sticky to backend\n");
            fprintf(f, "# TYPE proxy_sticky_sessions gauge\n");
            fprintf(f, "proxy_sticky_sessions %lld\n", (long long)sticky_sessions);

            fprintf(f, "# HELP proxy_backend_error_transient_total Transient backend error classification count\n");
            fprintf(f, "# TYPE proxy_backend_error_transient_total counter\n");
            fprintf(f, "proxy_backend_error_transient_total %llu\n",
                (unsigned long long)keel_counter_get(&snap.basic.backend_error_transient));

            fprintf(f, "# HELP proxy_backend_error_fatal_total Fatal backend error classification count\n");
            fprintf(f, "# TYPE proxy_backend_error_fatal_total counter\n");
            fprintf(f, "proxy_backend_error_fatal_total %llu\n",
                (unsigned long long)keel_counter_get(&snap.basic.backend_error_fatal));

            fprintf(f, "# HELP proxy_heartbeat_stalled_workers Workers with stale heartbeat over 20 seconds\n");
            fprintf(f, "# TYPE proxy_heartbeat_stalled_workers gauge\n");
            fprintf(f, "proxy_heartbeat_stalled_workers %llu\n", (unsigned long long)stalled_workers);

            fprintf(f, "# HELP proxy_stale_connection_workers Workers with oldest connection age over 300 seconds\n");
            fprintf(f, "# TYPE proxy_stale_connection_workers gauge\n");
            fprintf(f, "proxy_stale_connection_workers %llu\n", (unsigned long long)stale_conn_workers);

            fprintf(f, "# HELP keel_tls_connections_total Total TLS contexts created\n");
            fprintf(f, "# TYPE keel_tls_connections_total counter\n");
            fprintf(f, "keel_tls_connections_total %llu\n", (unsigned long long)tls_stats.connections_total);

            fprintf(f, "# HELP keel_tls_connections_succeeded Total successful TLS handshakes\n");
            fprintf(f, "# TYPE keel_tls_connections_succeeded counter\n");
            fprintf(f, "keel_tls_connections_succeeded %llu\n", (unsigned long long)tls_stats.connections_succeeded);

            fprintf(f, "# HELP keel_tls_connections_failed Total failed TLS handshakes\n");
            fprintf(f, "# TYPE keel_tls_connections_failed counter\n");
            fprintf(f, "keel_tls_connections_failed %llu\n", (unsigned long long)tls_stats.connections_failed);

            fprintf(f, "# HELP keel_tls_ktls_active Active connections with kTLS enabled\n");
            fprintf(f, "# TYPE keel_tls_ktls_active gauge\n");
            fprintf(f, "keel_tls_ktls_active %llu\n", (unsigned long long)tls_stats.ktls_active);

            fprintf(f, "# HELP keel_tls_ktls_fallback Total kTLS activation fallback events\n");
            fprintf(f, "# TYPE keel_tls_ktls_fallback counter\n");
            fprintf(f, "keel_tls_ktls_fallback %llu\n", (unsigned long long)tls_stats.ktls_fallback);

            fprintf(f, "# HELP keel_ktls_installations_attempted Total attempted kernel TLS installs\n");
            fprintf(f, "# TYPE keel_ktls_installations_attempted counter\n");
            fprintf(f, "keel_ktls_installations_attempted %llu\n", (unsigned long long)ktls_stats.installations_attempted);

            fprintf(f, "# HELP keel_ktls_installations_succeeded Total successful kernel TLS installs\n");
            fprintf(f, "# TYPE keel_ktls_installations_succeeded counter\n");
            fprintf(f, "keel_ktls_installations_succeeded %llu\n", (unsigned long long)ktls_stats.installations_succeeded);

            fprintf(f, "# HELP keel_ktls_installations_failed Total failed kernel TLS installs\n");
            fprintf(f, "# TYPE keel_ktls_installations_failed counter\n");
            fprintf(f, "keel_ktls_installations_failed %llu\n", (unsigned long long)ktls_stats.installations_failed);

            fprintf(f, "# HELP keel_ktls_cipher_incompatible Total kTLS attempts rejected by cipher compatibility\n");
            fprintf(f, "# TYPE keel_ktls_cipher_incompatible counter\n");
            fprintf(f, "keel_ktls_cipher_incompatible %llu\n", (unsigned long long)ktls_stats.cipher_incompatible);

            fprintf(f, "# HELP keel_ktls_kernel_errors Total kernel errors during kTLS installation\n");
            fprintf(f, "# TYPE keel_ktls_kernel_errors counter\n");
            fprintf(f, "keel_ktls_kernel_errors %llu\n", (unsigned long long)ktls_stats.kernel_errors);

            fprintf(f, "# HELP keel_tls_cert_reloads Total certificate reload events\n");
            fprintf(f, "# TYPE keel_tls_cert_reloads counter\n");
            fprintf(f, "keel_tls_cert_reloads %llu\n", (unsigned long long)tls_stats.cert_reloads);

            fprintf(f, "# HELP keel_tls_cert_reload_failures Total failed certificate reloads\n");
            fprintf(f, "# TYPE keel_tls_cert_reload_failures counter\n");
            fprintf(f, "keel_tls_cert_reload_failures %llu\n", (unsigned long long)tls_stats.cert_reload_failures);

            fprintf(f, "# HELP keel_tls_downgrade_rejected Plaintext connections rejected in TLS-require mode\n");
            fprintf(f, "# TYPE keel_tls_downgrade_rejected counter\n");
            fprintf(f, "keel_tls_downgrade_rejected %llu\n", (unsigned long long)tls_stats.downgrade_rejected);
        }

        /* Higher stats levels unlock progressively more expensive metrics. */
        if (snap.level >= KEEL_STATS_EXTENDED) {
            prom_write_histogram(f, "keel_query_latency_ns",
                "End-to-end query latency", &snap.extended.query_latency_ns);
            prom_write_histogram(f, "keel_backend_latency_ns",
                "Backend response latency", &snap.extended.backend_latency_ns);
            prom_write_histogram(f, "keel_connect_latency_ns",
                "Backend connect latency", &snap.extended.connect_latency_ns);
            prom_write_histogram(f, "keel_session_duration_ns",
                "Frontend session duration", &snap.extended.session_duration_ns);
            prom_write_histogram(f, "keel_wait_latency_ns",
                "Time waiting for pool backend", &snap.extended.wait_latency_ns);
        }

        /* System metrics are sampled on demand because they are colder-path data. */
        if (snap.level >= KEEL_STATS_SYSTEM) {
            keel_stats_sample_system(sc);
            keel_stats_snapshot_take(sc, &snap);

            fprintf(f, "# HELP keel_cpu_user_pct CPU user percentage\n");
            fprintf(f, "# TYPE keel_cpu_user_pct gauge\n");
            fprintf(f, "keel_cpu_user_pct %.2f\n", snap.system.cpu_user_pct);

            fprintf(f, "# HELP keel_cpu_sys_pct CPU system percentage\n");
            fprintf(f, "# TYPE keel_cpu_sys_pct gauge\n");
            fprintf(f, "keel_cpu_sys_pct %.2f\n", snap.system.cpu_sys_pct);

            fprintf(f, "# HELP keel_rss_bytes Resident set size in bytes\n");
            fprintf(f, "# TYPE keel_rss_bytes gauge\n");
            fprintf(f, "keel_rss_bytes %llu\n", (unsigned long long)snap.system.rss_bytes);

            fprintf(f, "# HELP keel_fd_open Open file descriptors\n");
            fprintf(f, "# TYPE keel_fd_open gauge\n");
            fprintf(f, "keel_fd_open %u\n", snap.system.fd_open);

            fprintf(f, "# HELP keel_fd_limit File descriptor limit\n");
            fprintf(f, "# TYPE keel_fd_limit gauge\n");
            fprintf(f, "keel_fd_limit %u\n", snap.system.fd_limit);
        }

        /* Uptime is always exported as the simplest cross-cutting liveness metric. */
        fprintf(f, "# HELP keel_uptime_seconds Proxy uptime in seconds\n");
        fprintf(f, "# TYPE keel_uptime_seconds gauge\n");
        fprintf(f, "keel_uptime_seconds %.1f\n", (double)snap.uptime_ns / 1.0e9);
    }

    /* Connection pool utilization gauges — aggregated across all workers. */
    {
        size_t pool_active = 0, pool_idle = 0, pool_total = 0;
        size_t pool_clean = 0, pool_stateful = 0, pool_dirty = 0, pool_closed = 0;
        size_t pool_waiting = 0, pool_cleaning = 0, pool_pinned = 0;
        for (uint32_t i = 0; i < nw; i++) {
            const keel_worker_t *w = keel_engine_get_worker(admin->engine, i);
            if (!w) continue;
            for (size_t s = 0; s < w->server_pool_count; s++) {
                backend_pool_t *pool = w->server_pools ? w->server_pools[s] : NULL;
                if (!pool) continue;
                backend_pool_stats_t st;
                backend_pool_get_stats(pool, &st);
                pool_active   += st.active_connections;
                pool_idle     += st.idle_connections;
                pool_clean    += st.clean_connections;
                pool_stateful += st.stateful_connections;
                pool_dirty    += st.dirty_connections;
                pool_closed   += st.closed_connections;
                pool_total    += st.total_connections;
                pool_waiting  += st.waiting_sessions;
                pool_cleaning += st.cleaning_count;
                pool_pinned   += st.pinned_count;
            }
        }
        fprintf(f, "# HELP keel_pool_connections_active Backend connections currently in use\n");
        fprintf(f, "# TYPE keel_pool_connections_active gauge\n");
        fprintf(f, "keel_pool_connections_active %zu\n", pool_active);

        fprintf(f, "# HELP keel_pool_connections_idle Backend connections idle in pool\n");
        fprintf(f, "# TYPE keel_pool_connections_idle gauge\n");
        fprintf(f, "keel_pool_connections_idle %zu\n", pool_idle);

        fprintf(f, "# HELP keel_pool_connections_clean Backend connections on clean idle list\n");
        fprintf(f, "# TYPE keel_pool_connections_clean gauge\n");
        fprintf(f, "keel_pool_connections_clean %zu\n", pool_clean);

        fprintf(f, "# HELP keel_pool_connections_stateful Backend connections on stateful idle list\n");
        fprintf(f, "# TYPE keel_pool_connections_stateful gauge\n");
        fprintf(f, "keel_pool_connections_stateful %zu\n", pool_stateful);

        fprintf(f, "# HELP keel_pool_connections_dirty Backend connections waiting for cleanup\n");
        fprintf(f, "# TYPE keel_pool_connections_dirty gauge\n");
        fprintf(f, "keel_pool_connections_dirty %zu\n", pool_dirty);

        fprintf(f, "# HELP keel_pool_connections_closed Backend connection slots closed and awaiting refill\n");
        fprintf(f, "# TYPE keel_pool_connections_closed gauge\n");
        fprintf(f, "keel_pool_connections_closed %zu\n", pool_closed);

        fprintf(f, "# HELP keel_pool_connections_total Total backend connection slots\n");
        fprintf(f, "# TYPE keel_pool_connections_total gauge\n");
        fprintf(f, "keel_pool_connections_total %zu\n", pool_total);

        fprintf(f, "# HELP keel_pool_waiting_sessions Sessions waiting for a backend connection\n");
        fprintf(f, "# TYPE keel_pool_waiting_sessions gauge\n");
        fprintf(f, "keel_pool_waiting_sessions %zu\n", pool_waiting);

        fprintf(f, "# HELP keel_pool_connections_cleaning Backend connections being cleaned\n");
        fprintf(f, "# TYPE keel_pool_connections_cleaning gauge\n");
        fprintf(f, "keel_pool_connections_cleaning %zu\n", pool_cleaning);

        fprintf(f, "# HELP keel_pool_connections_pinned Backend connections pinned to sessions\n");
        fprintf(f, "# TYPE keel_pool_connections_pinned gauge\n");
        fprintf(f, "keel_pool_connections_pinned %zu\n", pool_pinned);

        if (pool_total > 0) {
            fprintf(f, "# HELP keel_pool_utilization_ratio Ratio of active to total backend connections\n");
            fprintf(f, "# TYPE keel_pool_utilization_ratio gauge\n");
            fprintf(f, "keel_pool_utilization_ratio %.4f\n",
                    (double)pool_active / (double)pool_total);
        }
    }

    /* Static-ish process metadata is emitted after the main snapshot block. */
    fprintf(f, "# HELP keel_workers Number of worker threads\n");
    fprintf(f, "# TYPE keel_workers gauge\n");
    fprintf(f, "keel_workers %u\n", nw);

    /* Distributed tracing metrics (from tracer, not per-worker stats) */
    {
        keel_tracer_t *tracer = keel_engine_get_tracer(admin->engine);
        if (tracer) {
            const keel_tracer_stats_t *ts = keel_tracer_get_stats(tracer);
            if (ts) {
                fprintf(f, "# HELP keel_trace_spans_created Total trace spans created\n");
                fprintf(f, "# TYPE keel_trace_spans_created counter\n");
                fprintf(f, "keel_trace_spans_created %llu\n",
                    (unsigned long long)atomic_load_explicit(&ts->spans_created, memory_order_relaxed));

                fprintf(f, "# HELP keel_trace_spans_exported Total trace spans exported\n");
                fprintf(f, "# TYPE keel_trace_spans_exported counter\n");
                fprintf(f, "keel_trace_spans_exported %llu\n",
                    (unsigned long long)atomic_load_explicit(&ts->spans_exported, memory_order_relaxed));

                fprintf(f, "# HELP keel_trace_spans_dropped Total trace spans dropped (ring full)\n");
                fprintf(f, "# TYPE keel_trace_spans_dropped counter\n");
                fprintf(f, "keel_trace_spans_dropped %llu\n",
                    (unsigned long long)atomic_load_explicit(&ts->spans_dropped, memory_order_relaxed));

                fprintf(f, "# HELP keel_trace_export_errors Total trace export errors\n");
                fprintf(f, "# TYPE keel_trace_export_errors counter\n");
                fprintf(f, "keel_trace_export_errors %llu\n",
                    (unsigned long long)atomic_load_explicit(&ts->export_errors, memory_order_relaxed));

                fprintf(f, "# HELP keel_trace_export_batches Total trace export batches sent\n");
                fprintf(f, "# TYPE keel_trace_export_batches counter\n");
                fprintf(f, "keel_trace_export_batches %llu\n",
                    (unsigned long long)atomic_load_explicit(&ts->export_batches, memory_order_relaxed));
            }
        }
    }

    /* Router sharding metrics — emitted when a router is attached */
    if (admin->router) {
        char rbuf[32768];
        size_t rn = keel_router_write_prometheus(admin->router, rbuf, sizeof(rbuf));
        if (rn > 0) fwrite(rbuf, 1, rn, f);
    }

    /* Cluster election metrics */
    if (admin->cluster && keel_cluster_is_active(admin->cluster)) {
        keel_cluster_stats_t cs;
        keel_cluster_get_stats(admin->cluster, &cs);
        static const char* role_str[] = { "follower", "candidate", "leader" };
        int ri = (int)cs.local_role;
        if (ri < 0 || ri > 2) ri = 0;

        fprintf(f, "# HELP keel_cluster_peers_up Cluster peers currently UP\n");
        fprintf(f, "# TYPE keel_cluster_peers_up gauge\n");
        fprintf(f, "keel_cluster_peers_up %zu\n", cs.peers_up);

        fprintf(f, "# HELP keel_cluster_peers_down Cluster peers DOWN\n");
        fprintf(f, "# TYPE keel_cluster_peers_down gauge\n");
        fprintf(f, "keel_cluster_peers_down %zu\n", cs.peers_down);

        fprintf(f, "# HELP keel_cluster_election_term Current Raft election term\n");
        fprintf(f, "# TYPE keel_cluster_election_term counter\n");
        fprintf(f, "keel_cluster_election_term %llu\n",
                (unsigned long long)cs.local_term);

        fprintf(f, "# HELP keel_cluster_is_leader 1 if this node is the elected leader\n");
        fprintf(f, "# TYPE keel_cluster_is_leader gauge\n");
        fprintf(f, "keel_cluster_is_leader{role=\"%s\",leader_id=\"%s\"} %d\n",
                role_str[ri], cs.local_leader_id,
                cs.local_role == KEEL_CLUSTER_ROLE_LEADER ? 1 : 0);

        fprintf(f, "# HELP keel_cluster_elections_total Elections started by this node\n");
        fprintf(f, "# TYPE keel_cluster_elections_total counter\n");
        fprintf(f, "keel_cluster_elections_total %llu\n",
                (unsigned long long)cs.elections_started);

        fprintf(f, "# HELP keel_cluster_elections_won_total Elections won by this node\n");
        fprintf(f, "# TYPE keel_cluster_elections_won_total counter\n");
        fprintf(f, "keel_cluster_elections_won_total %llu\n",
                (unsigned long long)cs.elections_won);

        fprintf(f, "# HELP keel_cluster_heartbeats_sent_total Heartbeats sent\n");
        fprintf(f, "# TYPE keel_cluster_heartbeats_sent_total counter\n");
        fprintf(f, "keel_cluster_heartbeats_sent_total %llu\n",
                (unsigned long long)cs.heartbeats_sent);
    }

    fclose(f);

    keel_http_send_response(fd,
        "text/plain; version=0.0.4; charset=utf-8",
        body, body_len, accept_gzip);
    /* `body` was allocated by glibc inside open_memstream(), not by
     * keel_malloc(); it has no keel_alloc_header_t prefix and must be
     * released with libc free().  Routing it through keel_free() makes
     * the allocator misinterpret glibc's malloc-chunk header as a
     * corrupted keel header (size_field/magic look like garbage),
     * producing spurious "Invalid memory block in free" errors. */
    free(body); /* NOLINT(keel-syscall) */
}

/* ============================================================================
 * Web Management UI — embedded SPA and JSON status API
 * ============================================================================ */

/*
 * keel_ui_html is defined in src/admin/web_ui.c and declared in
 * keel/core/web_ui.h.  It is kept in a separate translation unit so that
 * tests can link only that file without pulling in the full admin subsystem.
 */

/**
 * @brief Send the embedded SPA HTML in response to GET /ui.
 *
 * @param fd Accepted HTTP client socket.
 */
static void serve_web_ui(int fd) {
    size_t html_len = strlen(keel_ui_html); /* exclude NUL */
    char hdr[256];
    int hl = snprintf(hdr, sizeof(hdr),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html; charset=utf-8\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n"
        "\r\n", html_len);
    safe_send(fd, hdr, (size_t)hl);
    safe_send(fd, keel_ui_html, html_len);
}

/**
 * @brief Serve a JSON snapshot of engine state and key metrics (GET /api/status.json).
 *
 * @param admin Admin subsystem handle.
 * @param fd    Accepted HTTP client socket.
 *
 * Response structure:
 * @code
 * {
 *   "state":           "active",
 *   "workers":         4,
 *   "uptime_seconds":  120.5,
 *   "sessions":        {"active":N,"created":N,"closed":N},
 *   "pool":            {"active":N,"idle":N,"cleaning":N,"pinned":N,"dirty":N,"closed":N,"total":N,"waiting":N},
 *   "queries":         {"total":N,"read":N,"write":N,"tx":N},
 *   "errors":          {"total":N,"auth":N,"timeout":N}
 * }
 * @endcode
 */
static void write_status_json(keel_admin_t *admin, int fd) {
    keel_stats_collector_t *sc = keel_engine_get_stats_collector(admin->engine);
    uint32_t nw = keel_engine_get_num_workers(admin->engine);
    keel_engine_state_t estate = keel_engine_get_state(admin->engine);

    const char *state_str;
    switch (estate) {
        case KEEL_ENGINE_STATE_ACTIVE:   state_str = "active";   break;
        case KEEL_ENGINE_STATE_DRAINING: state_str = "draining"; break;
        case KEEL_ENGINE_STATE_STOPPING: state_str = "stopping"; break;
        case KEEL_ENGINE_STATE_STOPPED:  state_str = "stopped";  break;
        default:                         state_str = "created";  break;
    }

    uint64_t sessions_active  = 0, sessions_created = 0, sessions_closed = 0;
    uint64_t queries_total    = 0, queries_read     = 0;
    uint64_t queries_write    = 0, queries_tx       = 0;
    uint64_t errors_total     = 0, errors_auth      = 0, errors_timeout  = 0;
    double   uptime_seconds   = 0.0;

    if (sc) {
        keel_stats_snapshot_t snap;
        keel_stats_snapshot_take(sc, &snap);
        sessions_active  = (uint64_t)keel_gauge_get(&snap.basic.sessions_active);
        sessions_created = keel_counter_get(&snap.basic.sessions_created);
        sessions_closed  = keel_counter_get(&snap.basic.sessions_closed);
        queries_total    = keel_counter_get(&snap.basic.queries_total);
        queries_read     = keel_counter_get(&snap.basic.queries_read);
        queries_write    = keel_counter_get(&snap.basic.queries_write);
        queries_tx       = keel_counter_get(&snap.basic.queries_tx);
        errors_total     = keel_counter_get(&snap.basic.errors_total);
        errors_auth      = keel_counter_get(&snap.basic.errors_auth);
        errors_timeout   = keel_counter_get(&snap.basic.errors_timeout);
        uptime_seconds   = (double)snap.uptime_ns / 1.0e9;
    }

    /* Aggregate pool stats across all workers. */
    size_t pool_active = 0, pool_idle = 0, pool_total = 0, pool_waiting = 0;
    size_t pool_cleaning = 0, pool_pinned = 0, pool_dirty = 0, pool_closed = 0;
    for (uint32_t i = 0; i < nw; i++) {
        const keel_worker_t *w = keel_engine_get_worker(admin->engine, i);
        if (!w) continue;
        for (size_t s = 0; s < w->server_pool_count; s++) {
            backend_pool_t *pool = w->server_pools ? w->server_pools[s] : NULL;
            if (!pool) continue;
            backend_pool_stats_t bst;
            backend_pool_get_stats(pool, &bst);
            pool_active  += bst.active_connections;
            pool_idle    += bst.idle_connections;
            pool_total   += bst.total_connections;
            pool_waiting += bst.waiting_sessions;
            pool_cleaning += bst.cleaning_count;
            pool_pinned   += bst.pinned_count;
            pool_dirty    += bst.dirty_connections;
            pool_closed   += bst.closed_connections;
        }
    }

    /* Cluster election identity */
    const char *cluster_role_str = "none";
    uint64_t    cluster_term     = 0;
    char        cluster_leader[KEEL_CLUSTER_MAX_NODE_ID] = "";
    if (admin->cluster && keel_cluster_is_active(admin->cluster)) {
        keel_cluster_role_t cr = keel_cluster_get_role(admin->cluster);
        cluster_term = keel_cluster_get_term(admin->cluster);
        keel_cluster_get_leader_id(admin->cluster,
                                   cluster_leader, sizeof(cluster_leader));
        if      (cr == KEEL_CLUSTER_ROLE_LEADER)    cluster_role_str = "leader";
        else if (cr == KEEL_CLUSTER_ROLE_CANDIDATE) cluster_role_str = "candidate";
        else                                        cluster_role_str = "follower";
    }

    char *body = NULL;
    size_t body_len = 0;
    FILE *f = open_memstream(&body, &body_len);
    if (!f) return;

    fprintf(f,
        "{\n"
        "  \"state\": \"%s\",\n"
        "  \"workers\": %u,\n"
        "  \"uptime_seconds\": %.1f,\n"
        "  \"sessions\": {\"active\":%llu,\"created\":%llu,\"closed\":%llu},\n"
        "  \"pool\": {\"active\":%zu,\"idle\":%zu,\"cleaning\":%zu,\"pinned\":%zu,\"dirty\":%zu,\"closed\":%zu,\"total\":%zu,\"waiting\":%zu},\n"
        "  \"queries\": {\"total\":%llu,\"read\":%llu,\"write\":%llu,\"tx\":%llu},\n"
        "  \"errors\": {\"total\":%llu,\"auth\":%llu,\"timeout\":%llu},\n"
        "  \"cluster\": {\"role\":\"%s\",\"term\":%llu,\"leader\":\"%s\"}\n"
        "}\n",
        state_str, nw, uptime_seconds,
        (unsigned long long)sessions_active,
        (unsigned long long)sessions_created,
        (unsigned long long)sessions_closed,
        pool_active, pool_idle, pool_cleaning, pool_pinned,
        pool_dirty, pool_closed, pool_total, pool_waiting,
        (unsigned long long)queries_total,
        (unsigned long long)queries_read,
        (unsigned long long)queries_write,
        (unsigned long long)queries_tx,
        (unsigned long long)errors_total,
        (unsigned long long)errors_auth,
        (unsigned long long)errors_timeout,
        cluster_role_str,
        (unsigned long long)cluster_term,
        cluster_leader);
    fclose(f);

    char hdr[256];
    int hl = snprintf(hdr, sizeof(hdr),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %zu\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "Connection: close\r\n"
        "\r\n", body_len);
    safe_send(fd, hdr, (size_t)hl);
    safe_send(fd, body, body_len);
    /* See comment in handle_prom_http(): open_memstream() returns a
     * libc-malloc'd buffer that must be released with free(), not
     * keel_free(). */
    free(body); /* NOLINT(keel-syscall) */
}

/**
 * @brief Serve one accepted HTTP admin connection.
 *
 * @param admin Admin subsystem handle.
 * @param fd Accepted HTTP client socket.
 * @return Nothing.
 *
 * Supported endpoints:
 * - `GET /metrics`
 * - `GET /`
 * - `GET /ui`
 * - `GET /api/status.json`
 * - `GET /healthz`
 * - `GET /readyz`
 * - `GET /livez`
 * - `GET /leadz`
 *
 * Corner cases:
 * - The parser only inspects the first request line prefix and does not
 *   implement general HTTP parsing.
 * - Unsupported paths receive a small `404` body.
 */
static void handle_prom_http(keel_admin_t *admin, int fd) {
    /* A single recv is enough because requests are expected to be tiny GETs. */
    char buf[4096];
    ssize_t n = recv(fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) return;
    buf[n] = '\0';

    bool accept_gzip = keel_http_accepts_gzip(buf, (size_t)n);

    if (strncmp(buf, "GET /metrics", 12) == 0 ||
        strncmp(buf, "GET / ", 6) == 0) {
        prom_write_metrics(admin, fd, accept_gzip);
    }
    /* K8s health endpoints */
    else if (strncmp(buf, "GET /healthz", 12) == 0) {
        const char *resp =
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: 2\r\n"
            "Connection: close\r\n"
            "\r\n"
            "ok";
        safe_send(fd, resp, strlen(resp));
    }
    else if (strncmp(buf, "GET /readyz", 11) == 0) {
        keel_engine_state_t st = keel_engine_get_state(admin->engine);
        if (st == KEEL_ENGINE_STATE_ACTIVE) {
            const char *resp =
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain\r\n"
                "Content-Length: 2\r\n"
                "Connection: close\r\n"
                "\r\n"
                "ok";
            safe_send(fd, resp, strlen(resp));
        } else {
            const char *resp =
                "HTTP/1.1 503 Service Unavailable\r\n"
                "Content-Type: text/plain\r\n"
                "Content-Length: 9\r\n"
                "Connection: close\r\n"
                "\r\n"
                "not ready";
            safe_send(fd, resp, strlen(resp));
        }
    }
    else if (strncmp(buf, "GET /livez", 10) == 0) {
        keel_engine_state_t st = keel_engine_get_state(admin->engine);
        if (st != KEEL_ENGINE_STATE_STOPPED) {
            const char *resp =
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain\r\n"
                "Content-Length: 2\r\n"
                "Connection: close\r\n"
                "\r\n"
                "ok";
            safe_send(fd, resp, strlen(resp));
        } else {
            const char *resp =
                "HTTP/1.1 503 Service Unavailable\r\n"
                "Content-Type: text/plain\r\n"
                "Content-Length: 4\r\n"
                "Connection: close\r\n"
                "\r\n"
                "dead";
            safe_send(fd, resp, strlen(resp));
        }
    }
    else if (strncmp(buf, "GET /leadz", 10) == 0) {
        /* Returns 200 if this node is the elected cluster leader (or if the
         * cluster is not running / election is disabled — standalone mode).
         * Use this endpoint as the backend health check for Keepalived VRRP
         * or a Kubernetes readinessProbe when running active/standby. */
        bool is_lead = true;
        if (admin->cluster && keel_cluster_is_active(admin->cluster) &&
            admin->cluster /* election_enabled is on by default */
        ) {
            keel_cluster_role_t role = keel_cluster_get_role(admin->cluster);
            /* If a term has been established (election has run at least once)
             * only the leader responds OK. */
            if (keel_cluster_get_term(admin->cluster) > 0)
                is_lead = (role == KEEL_CLUSTER_ROLE_LEADER);
        }
        if (is_lead) {
            const char *resp =
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: text/plain\r\n"
                "Content-Length: 6\r\n"
                "Connection: close\r\n"
                "\r\n"
                "leader";
            safe_send(fd, resp, strlen(resp));
        } else {
            const char *resp =
                "HTTP/1.1 503 Service Unavailable\r\n"
                "Content-Type: text/plain\r\n"
                "Content-Length: 8\r\n"
                "Connection: close\r\n"
                "\r\n"
                "follower";
            safe_send(fd, resp, strlen(resp));
        }
    }
    else if (strncmp(buf, "GET /ui", 7) == 0) {
        serve_web_ui(fd);
    }
    else if (strncmp(buf, "GET /api/status.json", 20) == 0) {
        write_status_json(admin, fd);
    }
    else {
        const char *resp =
            "HTTP/1.1 404 Not Found\r\n"
            "Content-Length: 36\r\n"
            "Connection: close\r\n"
            "\r\n"
            "Use GET /metrics or GET /ui\r\n";
        safe_send(fd, resp, strlen(resp));
    }
}

/* ============================================================================
 * Admin thread
 * ============================================================================ */

/**
 * @brief Entry point for the dedicated admin thread.
 *
 * @param arg Opaque pointer to `keel_admin_t`.
 * @return Always `NULL` on thread exit.
 *
 * Behavior:
 * - Names the thread when supported.
/**
 * @brief Admin thread: accept-and-serve loop for admin + Prometheus listeners.
 *
 * Uses epoll on Linux (O(1) dispatch, edge-triggered for HUP/ERR) with a
 * 1-second timeout so the running flag is polled at worst every second.
 * Falls back to select on BSD/macOS.
 *
 * - Builds a compact listener set for the enabled sockets.
 * - Accepts and serially serves PostgreSQL or HTTP admin clients.
 * - Applies conservative socket timeouts per accepted connection.
 *
 * Corner cases:
 * - Because processing is synchronous, one slow admin client can delay another,
 *   but timeouts bound the worst-case stall.
 */
static void *admin_thread_func(void *arg) {
    keel_admin_t *admin = (keel_admin_t *)arg;

#if defined(__APPLE__) || defined(__FreeBSD__)
    pthread_setname_np("keel-admin");
#endif

#if defined(__linux__)
    /* ---- epoll-based event loop ----------------------------------------- */
    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_ADMIN, "epoll_create1 failed: %s", strerror(errno));
        return NULL;
    }

#define ADMIN_EPOLL_MAX 2
    int admin_token = 1, prom_token = 2;

    if (admin->admin_fd >= 0) {
        struct epoll_event ev = { .events = EPOLLIN, .data.u32 = (uint32_t)admin_token };
        epoll_ctl(epfd, EPOLL_CTL_ADD, admin->admin_fd, &ev);
    }
    if (admin->prom_fd >= 0) {
        struct epoll_event ev = { .events = EPOLLIN, .data.u32 = (uint32_t)prom_token };
        epoll_ctl(epfd, EPOLL_CTL_ADD, admin->prom_fd, &ev);
    }

    struct epoll_event events[ADMIN_EPOLL_MAX];

    while (admin->running) {
        int n = epoll_wait(epfd, events, ADMIN_EPOLL_MAX, 1000 /* ms */);
        if (n < 0) {
            if (errno == EINTR) continue;
            KEEL_LOG_ERROR(KEEL_LOG_CAT_ADMIN, "epoll_wait: %s", strerror(errno));
            break;
        }

        for (int i = 0; i < n; i++) {
            if (!(events[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP)))
                continue;

            if ((int)events[i].data.u32 == admin_token && admin->admin_fd >= 0) {
                int cfd = accept(admin->admin_fd, NULL, NULL);
                if (cfd >= 0) {
                    int one = 1;
                    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                    struct timeval tv = { .tv_sec = 30, .tv_usec = 0 };
                    setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                    setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
                    handle_admin_pg(admin, cfd);
                    close(cfd);
                }
            } else if ((int)events[i].data.u32 == prom_token && admin->prom_fd >= 0) {
                int cfd = accept(admin->prom_fd, NULL, NULL);
                if (cfd >= 0) {
                    struct timeval tv = { .tv_sec = 5, .tv_usec = 0 };
                    setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                    setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
                    handle_prom_http(admin, cfd);
                    close(cfd);
                }
            }
        }
    }

    close(epfd);
#undef ADMIN_EPOLL_MAX

#else
    /* ---- BSD/macOS fallback: select-based loop --------------------------- */
    while (admin->running) {
        fd_set rset;
        FD_ZERO(&rset);
        int maxfd = -1;
        if (admin->admin_fd >= 0) { FD_SET(admin->admin_fd, &rset); if (admin->admin_fd > maxfd) maxfd = admin->admin_fd; }
        if (admin->prom_fd  >= 0) { FD_SET(admin->prom_fd,  &rset); if (admin->prom_fd  > maxfd) maxfd = admin->prom_fd;  }
        if (maxfd < 0) { usleep(100000); continue; }

        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
        int ret = select(maxfd + 1, &rset, NULL, NULL, &tv);
        if (ret <= 0) continue;

        if (admin->admin_fd >= 0 && FD_ISSET(admin->admin_fd, &rset)) {
            int cfd = accept(admin->admin_fd, NULL, NULL);
            if (cfd >= 0) {
                int one = 1;
                setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                struct timeval stv = { .tv_sec = 30, .tv_usec = 0 };
                setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &stv, sizeof(stv));
                setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &stv, sizeof(stv));
                handle_admin_pg(admin, cfd);
                close(cfd);
            }
        }
        if (admin->prom_fd >= 0 && FD_ISSET(admin->prom_fd, &rset)) {
            int cfd = accept(admin->prom_fd, NULL, NULL);
            if (cfd >= 0) {
                struct timeval stv = { .tv_sec = 5, .tv_usec = 0 };
                setsockopt(cfd, SOL_SOCKET, SO_RCVTIMEO, &stv, sizeof(stv));
                setsockopt(cfd, SOL_SOCKET, SO_SNDTIMEO, &stv, sizeof(stv));
                handle_prom_http(admin, cfd);
                close(cfd);
            }
        }
    }
#endif /* __linux__ */

    return NULL;
}

/* ============================================================================
 * Public API
 * ============================================================================ */

/**
 * @brief Start the admin subsystem and its listener thread.
 *
 * @param cfg Admin configuration copied into the new subsystem instance.
 * @param engine Engine handle used for introspection and control.
 * @return New admin subsystem handle, or `NULL` if startup fails or the feature
 *         is disabled.
 *
 * Errors handled:
 * - Null input pointers.
 * - Both admin surfaces disabled.
 * - Allocation failure.
 * - Listener bind failures.
 * - Thread creation failure.
 *
 * Corner cases:
 * - If only one of the two listeners binds successfully, the subsystem still
 *   starts and serves the surviving interface.
 */
keel_admin_t *keel_admin_start(const keel_admin_config_t *cfg,
                             keel_engine_t *engine)
{
    if (!cfg || !engine) return NULL;
    if (!cfg->admin_enabled && !cfg->prom_enabled) return NULL;

    keel_admin_t *admin = keel_calloc(1, sizeof(*admin));
    if (!admin) return NULL;

    admin->cfg      = *cfg;
    admin->engine   = engine;
    admin->running  = true;
    admin->admin_fd = -1;
    admin->prom_fd  = -1;
    admin->auth_mgr = NULL;

    /* Initialize SCRAM auth manager when a password is configured. */
    if (cfg->admin_password && cfg->admin_password[0]) {
        keel_auth_manager_config_t acfg = {
            .default_method    = KEEL_AUTH_SCRAM_SHA_256,
            .scram_iterations  = 4096,
        };
        admin->auth_mgr = keel_auth_manager_create(&acfg);
        if (admin->auth_mgr) {
            keel_auth_manager_register(admin->auth_mgr,
                                       keel_auth_scram_sha256_ops(), NULL);

            /* Build user record. Supports both pre-hashed SCRAM strings and
               plaintext passwords (which are hashed here at startup). */
            keel_auth_user_t au = {
                .username  = (char *)(cfg->admin_users ? cfg->admin_users : "admin"),
                .can_login = true,
            };

            if (strncmp(cfg->admin_password, "SCRAM-SHA-256$", 14) == 0) {
                /* Pre-hashed — parse into stored_key / server_key. */
                char *salt = NULL;
                int iters = 0;
                if (keel_auth_scram_parse_hash(cfg->admin_password, &salt,
                                               &iters, au.stored_key,
                                               au.server_key) == KEEL_OK) {
                    au.has_scram_keys = true;
                    au.iterations = iters;
                    au.password_salt = salt;  /* manager copies */
                }
            } else {
                /* Plaintext — hash at startup. */
                char *hash = NULL;
                if (keel_auth_scram_hash_password(cfg->admin_password,
                                                  4096, &hash) == KEEL_OK) {
                    char *salt = NULL;
                    int iters = 0;
                    if (keel_auth_scram_parse_hash(hash, &salt, &iters,
                                                   au.stored_key,
                                                   au.server_key) == KEEL_OK) {
                        au.has_scram_keys = true;
                        au.iterations = iters;
                        au.password_salt = salt;
                    }
                    keel_free(hash);
                }
            }

            if (au.has_scram_keys) {
                /* Register each comma-separated admin user with same password. */
                const char *p = cfg->admin_users ? cfg->admin_users : "admin";
                while (*p) {
                    while (*p == ' ' || *p == ',') p++;
                    const char *tok = p;
                    while (*p && *p != ',') p++;
                    size_t tlen = (size_t)(p - tok);
                    while (tlen > 0 && tok[tlen - 1] == ' ') tlen--;
                    if (tlen > 0 && tlen < 64) {
                        char uname[64];
                        memcpy(uname, tok, tlen);
                        uname[tlen] = '\0';
                        au.username = uname;
                        keel_auth_add_user(admin->auth_mgr, &au);
                    }
                }
                keel_free(au.password_salt);
                KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                              "Admin console SCRAM-SHA-256 auth enabled");
            } else {
                KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE,
                               "Failed to configure admin auth — falling back to trust");
                keel_auth_manager_destroy(admin->auth_mgr);
                admin->auth_mgr = NULL;
            }
        }
    }

    /* Create admin console listen socket */
    if (cfg->admin_enabled) {
        admin->admin_fd = create_listen_fd(cfg->admin_addr, cfg->admin_port);
        if (admin->admin_fd < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Failed to bind admin console on %s:%u: %s",
                    cfg->admin_addr, cfg->admin_port, strerror(errno));
        } else {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE, "Console listening on %s:%u (psql -h %s -p %u -U admin)",
                    cfg->admin_addr, cfg->admin_port,
                    cfg->admin_addr, cfg->admin_port);
        }
    }

    /* Create Prometheus listen socket */
    if (cfg->prom_enabled) {
        admin->prom_fd = create_listen_fd(cfg->prom_addr, cfg->prom_port);
        if (admin->prom_fd < 0) {
            KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Failed to bind Prometheus on %s:%u: %s",
                    cfg->prom_addr, cfg->prom_port, strerror(errno));
        } else {
            KEEL_LOG_INFO(KEEL_LOG_CAT_CORE,
                    "Prometheus metrics at http://%s:%u/metrics "
                    "(health: /healthz /readyz /livez /leadz)",
                    cfg->prom_addr, cfg->prom_port);
        }
    }

    /* If neither socket opened, bail out */
    if (admin->admin_fd < 0 && admin->prom_fd < 0) {
        keel_free(admin);
        return NULL;
    }

    /* Spawn admin thread */
    if (pthread_create(&admin->thread, NULL, admin_thread_func, admin) != 0) {
        KEEL_LOG_ERROR(KEEL_LOG_CAT_CORE, "Failed to create admin thread: %s", strerror(errno));
        if (admin->admin_fd >= 0) close(admin->admin_fd);
        if (admin->prom_fd  >= 0) close(admin->prom_fd);
        keel_free(admin);
        return NULL;
    }

#ifdef __linux__
    pthread_setname_np(admin->thread, "keel-admin");
#endif
    return admin;
}

/**
 * @brief Stop the admin subsystem and release all owned resources.
 *
 * @param admin Admin subsystem handle, or `NULL`.
 * @return Nothing.
 *
 * Behavior:
 * - Flips the running flag so the admin thread exits its poll loop.
 * - Joins the thread before closing listener sockets and freeing memory.
 *
 * Corner cases:
 * - Safe to call with `NULL`.
 */
void keel_admin_stop(keel_admin_t *admin) {
    if (!admin) return;

    admin->running = false;
    pthread_join(admin->thread, NULL);

    if (admin->auth_mgr) keel_auth_manager_destroy(admin->auth_mgr);
    if (admin->admin_fd >= 0) close(admin->admin_fd);
    if (admin->prom_fd  >= 0) close(admin->prom_fd);

    keel_free(admin);
}

void keel_admin_set_cluster(keel_admin_t *admin, keel_cluster_t *cluster) {
    if (admin) admin->cluster = cluster;
}

void keel_admin_set_router(keel_admin_t *admin, struct keel_router *router) {
    if (admin) admin->router = router;
}

struct keel_router *keel_admin_get_router(keel_admin_t *admin) {
    return admin ? admin->router : NULL;
}

void keel_admin_set_query_rules(keel_admin_t *admin,
                                keel_query_rules_t *rules) {
    if (admin) admin->query_rules = rules;
}

void keel_admin_set_throttle_rules(keel_admin_t *admin,
                                   keel_throttle_rules_t *throttle_rules) {
    if (admin) admin->throttle_rules = throttle_rules;
}

void keel_admin_set_discovery(keel_admin_t *admin, keel_discovery_t *discovery) {
    if (admin) admin->discovery = discovery;
}
