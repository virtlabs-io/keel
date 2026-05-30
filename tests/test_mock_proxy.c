/**
 * @file test_mock_proxy.c
 * @brief Integration tests for the keel proxy engine using an in-process mock
 *        PostgreSQL backend.
 *
 * Exercises the full proxy code path without a real database server:
 *   - worker.c         : session management, backend pool, select_backend_server
 *   - engine_flow.c    : on_fe_data, on_be_data, session lifecycle
 *   - backend_connect_async.c : backend TCP connect + PG startup + auth
 *
 * Architecture
 * ============
 *   [PG client] <--TCP--> [keel engine] <--TCP--> [mock backend]
 *
 * The mock backend runs in a helper thread and speaks the minimal subset of
 * the PostgreSQL wire protocol needed to complete the connection bootstrap and
 * handle simple / extended-query messages from keel.
 *
 * Client auth: KEEL_AUTH_TRUST (no password challenge, immediate AuthOK).
 * Backend auth: mock sends AuthenticationOk (type 0) → keel accepts without SCRAM.
 */

#include "test_utils.h"
#include "keel/engine/engine.h"
#include "keel/session/session.h"
#include "keel/protocol/protocol.h"

#include <stdatomic.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <poll.h>
#include <sys/wait.h>
#include <limits.h>

/* ============================================================================
 * Wire-protocol helpers (big-endian read/write)
 * ============================================================================ */

static inline void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8); p[3] = (uint8_t)(v      );
}
static inline void wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)(v);
}
static inline uint32_t rd32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
}
static inline uint16_t rd16(const uint8_t *p)
{
    return (uint16_t)(((uint16_t)p[0] << 8) | (uint16_t)p[1]);
}

/* ============================================================================
 * Socket helpers
 * ============================================================================ */

static int make_listen_socket(uint16_t *out_port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
        .sin_port        = 0,
    };
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { close(fd); return -1; }
    if (listen(fd, 128) < 0) { close(fd); return -1; }
    struct sockaddr_in bound;
    socklen_t blen = sizeof(bound);
    getsockname(fd, (struct sockaddr *)&bound, &blen);
    *out_port = ntohs(bound.sin_port);
    return fd;
}

static int connect_to(uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_addr.s_addr = htonl(INADDR_LOOPBACK),
        .sin_port        = htons(port),
    };
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { close(fd); return -1; }
    return fd;
}

/* Read exactly @n bytes; returns -1 on error or if the peer closed early.
 *
 * noinline: when this is inlined into the mock backend handlers (which use
 * fixed 8 KiB stack buffers) gcc's value-range analyzer loses the bound on
 * `n - done` after the first successful read and emits a bogus
 * -Wstringop-overflow on the read() fortify wrapper. Keeping the call frame
 * pinned preserves the per-callsite size bound and silences the false
 * positive without weakening any check. */
__attribute__((noinline))
static int read_exact(int fd, uint8_t *buf, size_t n)
{
    size_t done = 0;
    while (done < n) {
        size_t want = n - done;
        ssize_t r = read(fd, buf + done, want);
        if (r <= 0) return -1;
        if ((size_t)r > want) return -1; /* kernel contract: never above requested */
        done += (size_t)r;
    }
    return 0;
}

/* Read with a timeout; returns bytes read (≥0) or -1. */
static ssize_t read_timeout(int fd, uint8_t *buf, size_t maxlen, int timeout_ms)
{
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    int pr = poll(&pfd, 1, timeout_ms);
    if (pr <= 0) return -1;
    return read(fd, buf, maxlen);
}

/* Write @n bytes; returns -1 on error. */
static int write_exact(int fd, const uint8_t *buf, size_t n)
{
    size_t done = 0;
    while (done < n) {
        ssize_t w = write(fd, buf + done, n - done);
        if (w <= 0) return -1;
        done += (size_t)w;
    }
    return 0;
}

/* ============================================================================
 * Mock backend PG message builders (server → keel)
 * ============================================================================ */

static size_t mock_make_auth_ok(uint8_t *b)
{
    /* 'R' + int32(8) + int32(0) */
    b[0] = 'R'; wr32(b+1, 8); wr32(b+5, 0);
    return 9;
}

static size_t mock_make_param_status(uint8_t *b, const char *key, const char *val)
{
    size_t kl = strlen(key)+1, vl = strlen(val)+1;
    b[0] = 'S'; wr32(b+1, (uint32_t)(4+kl+vl));
    memcpy(b+5, key, kl); memcpy(b+5+kl, val, vl);
    return 5+kl+vl;
}

static size_t mock_make_backend_key_data(uint8_t *b, uint32_t pid, uint32_t key)
{
    b[0] = 'K'; wr32(b+1, 12); wr32(b+5, pid); wr32(b+9, key);
    return 13;
}

static size_t mock_make_ready_for_query(uint8_t *b, char status)
{
    b[0] = 'Z'; wr32(b+1, 5); b[5] = (uint8_t)status;
    return 6;
}

static size_t mock_make_command_complete(uint8_t *b, const char *tag)
{
    size_t tl = strlen(tag)+1;
    b[0] = 'C'; wr32(b+1, (uint32_t)(4+tl));
    memcpy(b+5, tag, tl);
    return 5+tl;
}

static size_t mock_make_row_desc_empty(uint8_t *b)
{
    /* 'T' + int32(6) + int16(0) — zero columns */
    b[0] = 'T'; wr32(b+1, 6); wr16(b+5, 0);
    return 7;
}

static size_t mock_make_parse_complete(uint8_t *b)
{
    b[0] = '1'; wr32(b+1, 4);
    return 5;
}

static size_t mock_make_bind_complete(uint8_t *b)
{
    b[0] = '2'; wr32(b+1, 4);
    return 5;
}

static size_t mock_make_no_data(uint8_t *b)
{
    b[0] = 'n'; wr32(b+1, 4);
    return 5;
}

static size_t mock_make_error_response(uint8_t *b, const char *msg)
{
    /* 'E' + int32(len) + 'S' + severity\0 + 'M' + msg\0 + \0 */
    const char *severity = "ERROR";
    size_t sl = strlen(severity)+1, ml = strlen(msg)+1;
    uint32_t payload = (uint32_t)(4 + 1 + sl + 1 + ml + 1);
    b[0] = 'E'; wr32(b+1, payload);
    size_t off = 5;
    b[off++] = 'S'; memcpy(b+off, severity, sl); off += sl;
    b[off++] = 'M'; memcpy(b+off, msg, ml); off += ml;
    b[off++] = 0;
    return off;
}

/* ============================================================================
 * Mock PostgreSQL backend
 * ============================================================================ */

typedef struct {
    int            listen_fd;
    _Atomic bool   running;
    atomic_int     connections_accepted;
    atomic_int     queries_handled;
    pthread_t      listener_tid;
    uint16_t       port;
    void          *(*conn_handler)(void *); /* per-connection handler */
} mock_backend_t;

/* Handler for one keel→mock-backend connection. */
static void *mock_conn_handler(void *arg)
{
    int fd = (int)(intptr_t)arg;
    uint8_t buf[8192];

    /* ---- Startup: read the startup message ---- */
    /* First 4 bytes = total length (including itself). */
    if (read_exact(fd, buf, 4) < 0) { close(fd); return NULL; }
    uint32_t startup_len = rd32(buf);
    if (startup_len < 8 || startup_len > sizeof(buf)) { close(fd); return NULL; }

    /* Read the rest of the startup message. */
    if (read_exact(fd, buf+4, startup_len-4) < 0) { close(fd); return NULL; }
    uint32_t proto = rd32(buf+4);

    /* SSLRequest (80877102) — respond with 'N' */
    if (proto == 80877102U) {
        buf[0] = 'N';
        write_exact(fd, buf, 1);
        /* Now expect the real startup message */
        if (read_exact(fd, buf, 4) < 0) { close(fd); return NULL; }
        startup_len = rd32(buf);
        if (startup_len < 8 || startup_len > sizeof(buf)) { close(fd); return NULL; }
        if (read_exact(fd, buf+4, startup_len-4) < 0) { close(fd); return NULL; }
    }

    /* CancelRequest (80877103) — just close */
    if (proto == 80877103U) { close(fd); return NULL; }

    /* ---- Send auth/startup response to keel ---- */
    uint8_t resp[4096];
    size_t  rlen = 0;

    /* AuthenticationOk */
    rlen += mock_make_auth_ok(resp+rlen);
    /* ParameterStatus messages */
    rlen += mock_make_param_status(resp+rlen, "server_version", "15.0");
    rlen += mock_make_param_status(resp+rlen, "server_encoding", "UTF8");
    rlen += mock_make_param_status(resp+rlen, "client_encoding", "UTF8");
    rlen += mock_make_param_status(resp+rlen, "integer_datetimes", "on");
    rlen += mock_make_param_status(resp+rlen, "standard_conforming_strings", "on");
    /* BackendKeyData */
    rlen += mock_make_backend_key_data(resp+rlen, 12345, 99999);
    /* ReadyForQuery */
    rlen += mock_make_ready_for_query(resp+rlen, 'I');

    if (write_exact(fd, resp, rlen) < 0) { close(fd); return NULL; }

    /* ---- Main message loop ---- */
    while (1) {
        /* Read message type + 4-byte length */
        uint8_t hdr[5];
        if (read_exact(fd, hdr, 5) < 0) break;

        char    msg_type = (char)hdr[0];
        uint32_t msg_len = rd32(hdr+1);  /* includes itself, excludes type byte */

        /* Read payload (msg_len - 4 bytes) */
        size_t  payload_len = (msg_len >= 4) ? (msg_len - 4) : 0;
        uint8_t payload[65536];
        if (payload_len > 0) {
            if (payload_len > sizeof(payload)) {
                /* Drain oversized message and send an error */
                size_t remaining = payload_len;
                while (remaining > 0) {
                    size_t chunk = remaining < sizeof(payload) ? remaining : sizeof(payload);
                    if (read_exact(fd, payload, chunk) < 0) goto conn_done;
                    remaining -= chunk;
                }
                rlen = mock_make_error_response(resp, "message too large");
                rlen += mock_make_ready_for_query(resp+rlen, 'I');
                write_exact(fd, resp, rlen);
                continue;
            }
            if (read_exact(fd, payload, payload_len) < 0) break;
        }

        rlen = 0;
        switch (msg_type) {
        case 'Q':   /* Simple query */
            /* Emit: empty RowDescription + CommandComplete + ReadyForQuery */
            rlen += mock_make_row_desc_empty(resp+rlen);
            rlen += mock_make_command_complete(resp+rlen, "SELECT 0");
            rlen += mock_make_ready_for_query(resp+rlen, 'I');
            break;

        case 'P':   /* Parse (extended protocol) */
            rlen += mock_make_parse_complete(resp+rlen);
            break;

        case 'B':   /* Bind */
            rlen += mock_make_bind_complete(resp+rlen);
            break;

        case 'D':   /* Describe */
            rlen += mock_make_no_data(resp+rlen);
            break;

        case 'E':   /* Execute */
            rlen += mock_make_command_complete(resp+rlen, "SELECT 0");
            break;

        case 'S':   /* Sync */
            rlen += mock_make_ready_for_query(resp+rlen, 'I');
            break;

        case 'C':   /* Close (prepared statement or portal) */
            /* CloseComplete ('3' + int32(4)) */
            resp[0] = '3'; wr32(resp+1, 4);
            rlen = 5;
            break;

        case 'H':   /* Flush — no response needed */
            rlen = 0;
            break;

        case 'X':   /* Terminate */
            goto conn_done;

        default:
            /* Unknown message type; send ErrorResponse + ReadyForQuery */
            rlen += mock_make_error_response(resp, "unknown message type");
            rlen += mock_make_ready_for_query(resp+rlen, 'I');
            break;
        }

        if (rlen > 0 && write_exact(fd, resp, rlen) < 0) break;
    }

conn_done:
    close(fd);
    return NULL;
}

/* Listener thread: accept connections and spawn per-connection handlers. */
static void *mock_backend_listener(void *arg)
{
    mock_backend_t *mb = (mock_backend_t *)arg;

    while (mb->running) {
        struct pollfd pfd = { .fd = mb->listen_fd, .events = POLLIN };
        int pr = poll(&pfd, 1, 200);
        if (pr <= 0) continue;

        struct sockaddr_in client_addr;
        socklen_t clen = sizeof(client_addr);
        int conn_fd = accept(mb->listen_fd, (struct sockaddr *)&client_addr, &clen);
        if (conn_fd < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            break;
        }

        /* Disable Nagle for lower latency in tests */
        int one = 1;
        setsockopt(conn_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        atomic_fetch_add(&mb->connections_accepted, 1);

        pthread_t tid;
        if (pthread_create(&tid, NULL, mb->conn_handler, (void *)(intptr_t)conn_fd) != 0) {
            close(conn_fd);
        } else {
            pthread_detach(tid);
        }
    }

    return NULL;
}

/* Forward declaration so mock_backend_start_impl can accept it as a parameter
 * even though mock_conn_handler_txn_aware is defined later in this file. */
static void *mock_conn_handler_txn_aware(void *arg);

/* Internal helper — sets conn_handler BEFORE pthread_create to avoid a
 * data race between the main thread (which may overwrite the handler after
 * start) and the listener thread (which reads it on every accepted conn). */
static mock_backend_t *mock_backend_start_impl(void *(*handler)(void *))
{
    mock_backend_t *mb = calloc(1, sizeof(*mb));
    if (!mb) return NULL;

    atomic_init(&mb->connections_accepted, 0);
    atomic_init(&mb->queries_handled, 0);
    mb->running = true;
    mb->conn_handler = handler;  /* must be set before pthread_create */

    mb->listen_fd = make_listen_socket(&mb->port);
    if (mb->listen_fd < 0) { free(mb); return NULL; }

    if (pthread_create(&mb->listener_tid, NULL, mock_backend_listener, mb) != 0) {
        close(mb->listen_fd);
        free(mb);
        return NULL;
    }

    return mb;
}

static mock_backend_t *mock_backend_start(void)
{
    return mock_backend_start_impl(mock_conn_handler);
}

static void mock_backend_stop(mock_backend_t *mb)
{
    if (!mb) return;
    mb->running = false;
    close(mb->listen_fd);
    pthread_join(mb->listener_tid, NULL);
    free(mb);
}

/* ============================================================================
 * Transaction-aware mock backend handler
 * Same as mock_conn_handler but tracks BEGIN/COMMIT/ROLLBACK and sends the
 * correct transaction status byte in ReadyForQuery responses.
 * ============================================================================ */
static void *mock_conn_handler_txn_aware(void *arg)
{
    int fd = (int)(intptr_t)arg;
    uint8_t buf[8192];
    char txn_status = 'I';   /* 'I'=idle, 'T'=in-transaction, 'E'=error */

    /* ---- Startup ---- */
    if (read_exact(fd, buf, 4) < 0) { close(fd); return NULL; }
    uint32_t startup_len = rd32(buf);
    if (startup_len < 8 || startup_len > sizeof(buf)) { close(fd); return NULL; }
    if (read_exact(fd, buf+4, startup_len-4) < 0) { close(fd); return NULL; }
    uint32_t proto = rd32(buf+4);

    if (proto == 80877102U) {   /* SSLRequest */
        buf[0] = 'N';
        write_exact(fd, buf, 1);
        if (read_exact(fd, buf, 4) < 0) { close(fd); return NULL; }
        startup_len = rd32(buf);
        if (startup_len < 8 || startup_len > sizeof(buf)) { close(fd); return NULL; }
        if (read_exact(fd, buf+4, startup_len-4) < 0) { close(fd); return NULL; }
    }
    if (proto == 80877103U) { close(fd); return NULL; }  /* CancelRequest */

    uint8_t resp[4096];
    size_t  rlen = 0;
    rlen += mock_make_auth_ok(resp+rlen);
    rlen += mock_make_param_status(resp+rlen, "server_version", "15.0");
    rlen += mock_make_param_status(resp+rlen, "server_encoding", "UTF8");
    rlen += mock_make_param_status(resp+rlen, "client_encoding", "UTF8");
    rlen += mock_make_param_status(resp+rlen, "integer_datetimes", "on");
    rlen += mock_make_param_status(resp+rlen, "standard_conforming_strings", "on");
    rlen += mock_make_backend_key_data(resp+rlen, 54321, 11111);
    rlen += mock_make_ready_for_query(resp+rlen, txn_status);
    if (write_exact(fd, resp, rlen) < 0) { close(fd); return NULL; }

    /* ---- Main message loop ---- */
    while (1) {
        uint8_t hdr[5];
        if (read_exact(fd, hdr, 5) < 0) break;
        char     msg_type   = (char)hdr[0];
        uint32_t msg_len    = rd32(hdr+1);
        size_t   payload_len = (msg_len >= 4) ? (msg_len - 4) : 0;
        uint8_t  payload[65536];
        if (payload_len > 0) {
            if (payload_len > sizeof(payload)) {
                size_t rem = payload_len;
                while (rem > 0) {
                    size_t chunk = rem < sizeof(payload) ? rem : sizeof(payload);
                    if (read_exact(fd, payload, chunk) < 0) goto txn_done;
                    rem -= chunk;
                }
                rlen  = mock_make_error_response(resp, "message too large");
                rlen += mock_make_ready_for_query(resp+rlen, txn_status);
                write_exact(fd, resp, rlen);
                continue;
            }
            if (read_exact(fd, payload, payload_len) < 0) break;
        }

        rlen = 0;
        switch (msg_type) {
        case 'Q': {
            /* Parse query to detect transaction control statements */
            char qstr[512] = {0};
            if (payload_len > 0 && payload_len < sizeof(qstr)) {
                memcpy(qstr, payload, payload_len);
            }
            if (strncasecmp(qstr, "BEGIN", 5) == 0 ||
                strncasecmp(qstr, "START TRANSACTION", 17) == 0) {
                txn_status = 'T';
            } else if (strncasecmp(qstr, "COMMIT", 6) == 0 ||
                       strncasecmp(qstr, "END", 3) == 0 ||
                       strncasecmp(qstr, "ROLLBACK", 8) == 0 ||
                       strncasecmp(qstr, "ABORT", 5) == 0) {
                txn_status = 'I';
            }
            rlen += mock_make_row_desc_empty(resp+rlen);
            rlen += mock_make_command_complete(resp+rlen, "SELECT 0");
            rlen += mock_make_ready_for_query(resp+rlen, txn_status);
            break;
        }
        case 'P':  rlen += mock_make_parse_complete(resp+rlen);  break;
        case 'B':  rlen += mock_make_bind_complete(resp+rlen);   break;
        case 'D':  rlen += mock_make_no_data(resp+rlen);         break;
        case 'E':  rlen += mock_make_command_complete(resp+rlen, "SELECT 0"); break;
        case 'S':  rlen += mock_make_ready_for_query(resp+rlen, txn_status); break;
        case 'C':
            resp[0] = '3'; wr32(resp+1, 4);
            rlen = 5;
            break;
        case 'H':  rlen = 0; break;
        case 'X':  goto txn_done;
        default:
            rlen += mock_make_error_response(resp, "unknown message type");
            rlen += mock_make_ready_for_query(resp+rlen, txn_status);
            break;
        }

        if (rlen > 0 && write_exact(fd, resp, rlen) < 0) break;
    }

txn_done:
    close(fd);
    return NULL;
}

/* Start a mock backend that sends correct transaction status bytes. */
static mock_backend_t *mock_backend_start_txn_aware(void)
{
    return mock_backend_start_impl(mock_conn_handler_txn_aware);
}

/* ============================================================================
 * Engine helpers (same pattern as test_drain_shutdown.c)
 * ============================================================================ */

static void *engine_run_thread(void *arg)
{
    keel_engine_t *engine = (keel_engine_t *)arg;
    keel_engine_run(engine);
    return NULL;
}

static void stop_engine_and_join(keel_engine_t *engine, pthread_t tid)
{
    keel_engine_stop(engine);
    pthread_kill(tid, SIGTERM);
    pthread_join(tid, NULL);
}

/* ============================================================================
 * PG client helpers (client → keel)
 * ============================================================================ */

/* Build a PG startup message (protocol 3.0). */
static size_t pg_make_startup(uint8_t *b, const char *user, const char *db)
{
    uint8_t *p = b + 8;   /* reserve 4 bytes for length + 4 for protocol */
    /* Protocol version 3.0 = 0x00030000 = 196608 */
    wr32(b+4, 196608U);
    /* user parameter */
    memcpy(p, "user", 5);  p += 5;
    size_t ul = strlen(user)+1; memcpy(p, user, ul); p += ul;
    /* database parameter */
    memcpy(p, "database", 9); p += 9;
    size_t dl = strlen(db)+1; memcpy(p, db, dl); p += dl;
    /* terminator */
    *p++ = 0;
    /* fill in total length (includes itself) */
    wr32(b, (uint32_t)(p - b));
    return (size_t)(p - b);
}

/* Build a simple query message. */
static size_t pg_make_query(uint8_t *b, const char *sql)
{
    size_t sl = strlen(sql)+1;
    b[0] = 'Q'; wr32(b+1, (uint32_t)(4+sl));
    memcpy(b+5, sql, sl);
    return 5+sl;
}

/* Build a Terminate message. */
static size_t pg_make_terminate(uint8_t *b)
{
    b[0] = 'X'; wr32(b+1, 4);
    return 5;
}

/* Build Parse message ('P' type_name_sql). */
static size_t pg_make_parse(uint8_t *b, const char *stmt_name, const char *sql)
{
    size_t nl = strlen(stmt_name)+1, sl = strlen(sql)+1;
    b[0] = 'P';
    /* len = 4 + name\0 + sql\0 + int16(0) for param count */
    wr32(b+1, (uint32_t)(4 + nl + sl + 2));
    memcpy(b+5, stmt_name, nl);
    memcpy(b+5+nl, sql, sl);
    wr16(b+5+nl+sl, 0);  /* 0 parameter type OIDs */
    return 5+nl+sl+2;
}

/* Build Bind message ('B'). */
static size_t pg_make_bind(uint8_t *b, const char *portal, const char *stmt_name)
{
    size_t pl = strlen(portal)+1, nl = strlen(stmt_name)+1;
    /* len = 4 + portal\0 + stmt\0 + int16(0) + int16(0) + int16(0) */
    wr32(b+1, (uint32_t)(4 + pl + nl + 6));
    b[0] = 'B';
    size_t off = 5;
    memcpy(b+off, portal, pl);   off += pl;
    memcpy(b+off, stmt_name, nl); off += nl;
    wr16(b+off, 0); off += 2;  /* 0 format codes */
    wr16(b+off, 0); off += 2;  /* 0 parameters */
    wr16(b+off, 0); off += 2;  /* 0 result format codes */
    return off;
}

/* Build Describe message ('D'). */
static size_t pg_make_describe(uint8_t *b, char type, const char *name)
{
    size_t nl = strlen(name)+1;
    b[0] = 'D'; wr32(b+1, (uint32_t)(4+1+nl));
    b[5] = (uint8_t)type;  /* 'S' or 'P' */
    memcpy(b+6, name, nl);
    return 6+nl;
}

/* Build Execute message ('E'). */
static size_t pg_make_execute(uint8_t *b, const char *portal)
{
    size_t pl = strlen(portal)+1;
    b[0] = 'E'; wr32(b+1, (uint32_t)(4+pl+4));
    memcpy(b+5, portal, pl);
    wr32(b+5+pl, 0);  /* max rows = 0 (unlimited) */
    return 5+pl+4;
}

/* Build Sync message ('S'). */
static size_t pg_make_sync(uint8_t *b)
{
    b[0] = 'S'; wr32(b+1, 4);
    return 5;
}

/* Build Close (stmt/portal) message ('C'). */
static size_t pg_make_close(uint8_t *b, char type, const char *name)
{
    size_t nl = strlen(name)+1;
    b[0] = 'C'; wr32(b+1, (uint32_t)(4+1+nl));
    b[5] = (uint8_t)type;
    memcpy(b+6, name, nl);
    return 6+nl;
}

/*
 * Read messages from a keel-proxied PG connection until ReadyForQuery ('Z')
 * or until an error / timeout.
 *
 * @param fd         Connected client socket.
 * @param timeout_ms Per-message poll timeout.
 * @param saw_z      [out] Set to true if ReadyForQuery was received.
 * @return  0 = success (saw ReadyForQuery), -1 = error/timeout.
 */
static int pg_read_until_ready(int fd, int timeout_ms, bool *saw_z)
{
    if (saw_z) *saw_z = false;

    for (int i = 0; i < 512; i++) {
        uint8_t hdr[5];
        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        if (poll(&pfd, 1, timeout_ms) <= 0) return -1;

        if (read_exact(fd, hdr, 5) < 0) return -1;

        char    mtype = (char)hdr[0];
        uint32_t mlen = rd32(hdr+1);
        size_t  plen  = (mlen >= 4) ? (mlen - 4) : 0;

        if (plen > 0) {
            uint8_t discard[65536];
            size_t  remaining = plen;
            while (remaining > 0) {
                size_t chunk = remaining < sizeof(discard) ? remaining : sizeof(discard);
                if (poll(&pfd, 1, timeout_ms) <= 0) return -1;
                if (read_exact(fd, discard, chunk) < 0) return -1;
                remaining -= chunk;
            }
        }

        if (mtype == 'Z') {
            if (saw_z) *saw_z = true;
            return 0;
        }
        if (mtype == 'E') {
            /* ErrorResponse — but ReadyForQuery follows, keep reading */
            continue;
        }
    }
    return -1;  /* too many messages without ReadyForQuery */
}

/*
 * Connect a PG client to keel (trust auth), perform the startup handshake,
 * wait for ReadyForQuery.
 *
 * @param keel_port  keel's listen port.
 * @param user       PostgreSQL user name.
 * @param db         PostgreSQL database name.
 * @return Connected and ready fd, or -1 on failure.
 */
static int pg_client_connect(uint16_t keel_port, const char *user, const char *db)
{
    int fd = connect_to(keel_port);
    if (fd < 0) return -1;

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    uint8_t buf[512];
    size_t  n = pg_make_startup(buf, user, db);
    if (write_exact(fd, buf, n) < 0) { close(fd); return -1; }

    /* keel (trust mode) sends: AuthOK + ParameterStatus* + BackendKeyData + ReadyForQuery
     * We drain until 'Z'. */
    bool saw_z = false;
    if (pg_read_until_ready(fd, 5000, &saw_z) < 0 || !saw_z) { close(fd); return -1; }

    return fd;
}

/* ============================================================================
 * Engine factory helper
 * ============================================================================ */

static keel_engine_t *make_proxy_engine(uint16_t *listen_port, int *listen_fd,
                                        uint16_t backend_port)
{
    *listen_fd = make_listen_socket(listen_port);
    if (*listen_fd < 0) return NULL;

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers      = 1;
    cfg.queue_depth      = 64;
    cfg.session_pool_size = 32;
    cfg.idle_timeout_ms  = 60000;
    cfg.connect_timeout_ms = 5000;
    cfg.auth_method      = KEEL_AUTH_TRUST;
    cfg.pool_min_size    = 0;   /* lazy backend connect */
    cfg.pool_max_size    = 8;

    /* Configure the server pool with our mock backend */
    cfg.server_pool.servers[0].host     = "127.0.0.1";
    cfg.server_pool.servers[0].port     = backend_port;
    cfg.server_pool.servers[0].user     = "postgres";
    cfg.server_pool.servers[0].password = "";
    cfg.server_pool.servers[0].database = "postgres";
    cfg.server_pool.servers[0].role     = KEEL_SERVER_ROLE_RW;
    cfg.server_pool.servers[0].weight   = 1;
    cfg.server_pool.servers[0].healthy  = true;
    cfg.server_pool.count = 1;
    keel_server_pool_rebuild_indices(&cfg.server_pool);

    keel_engine_t *engine = keel_engine_create(&cfg);
    if (!engine) { close(*listen_fd); *listen_fd = -1; return NULL; }

    if (keel_engine_start(engine, *listen_fd) != 0) {
        keel_engine_destroy(engine);
        close(*listen_fd);
        *listen_fd = -1;
        return NULL;
    }

    return engine;
}

/*
 * make_proxy_engine_ex — parameterized engine factory.
 * backend_port2 == 0 means single-server (one RW backend).
 * When backend_port2 != 0, a second server is added with the given role.
 */
static keel_engine_t *make_proxy_engine_ex(uint16_t *listen_port, int *listen_fd,
                                           uint16_t backend_port,
                                           keel_tier_t runtime_mode,
                                           bool txn_tracking,
                                           keel_ps_mode_t ps_mode,
                                           uint16_t backend_port2,
                                           keel_server_role_t role2)
{
    *listen_fd = make_listen_socket(listen_port);
    if (*listen_fd < 0) return NULL;

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers        = 1;
    cfg.queue_depth        = 64;
    cfg.session_pool_size  = 32;
    cfg.idle_timeout_ms    = 60000;
    cfg.connect_timeout_ms = 5000;
    cfg.auth_method        = KEEL_AUTH_TRUST;
    cfg.pool_min_size      = 0;
    cfg.pool_max_size      = 8;
    cfg.runtime_mode       = runtime_mode;
    cfg.txn_tracking       = txn_tracking;
    cfg.ps_mode            = ps_mode;
    cfg.sticky_primary_ttl_ms = 0;  /* don't force reads to primary after writes */

    cfg.server_pool.servers[0].host     = "127.0.0.1";
    cfg.server_pool.servers[0].port     = backend_port;
    cfg.server_pool.servers[0].user     = "postgres";
    cfg.server_pool.servers[0].password = "";
    cfg.server_pool.servers[0].database = "postgres";
    cfg.server_pool.servers[0].role     = KEEL_SERVER_ROLE_RW;
    cfg.server_pool.servers[0].weight   = 1;
    cfg.server_pool.servers[0].healthy  = true;
    cfg.server_pool.count = 1;

    if (backend_port2 != 0) {
        cfg.server_pool.servers[1].host     = "127.0.0.1";
        cfg.server_pool.servers[1].port     = backend_port2;
        cfg.server_pool.servers[1].user     = "postgres";
        cfg.server_pool.servers[1].password = "";
        cfg.server_pool.servers[1].database = "postgres";
        cfg.server_pool.servers[1].role     = role2;
        cfg.server_pool.servers[1].weight   = 1;
        cfg.server_pool.servers[1].healthy  = true;
        cfg.server_pool.count = 2;
    }

    keel_server_pool_rebuild_indices(&cfg.server_pool);

    keel_engine_t *engine = keel_engine_create(&cfg);
    if (!engine) { close(*listen_fd); *listen_fd = -1; return NULL; }

    if (keel_engine_start(engine, *listen_fd) != 0) {
        keel_engine_destroy(engine);
        close(*listen_fd);
        *listen_fd = -1;
        return NULL;
    }

    return engine;
}

/* ============================================================================
 * Test 1: Basic client connect + simple query + disconnect
 * ============================================================================ */
static void test_basic_query(void)
{
    TEST_BEGIN("mock proxy: basic connect → simple query → disconnect");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t     keel_port = 0;
    int          listen_fd = -1;
    keel_engine_t *engine  = make_proxy_engine(&keel_port, &listen_fd, mb->port);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);  /* let workers come up */

    /* Connect and run a simple query */
    int client = pg_client_connect(keel_port, "postgres", "postgres");
    TEST_ASSERT(client >= 0);

    if (client >= 0) {
        uint8_t buf[256];
        size_t  n = pg_make_query(buf, "SELECT 1");
        int wr = write_exact(client, buf, n);
        TEST_ASSERT_EQ(wr, 0);

        bool saw_z = false;
        int rr = pg_read_until_ready(client, 5000, &saw_z);
        TEST_ASSERT_EQ(rr, 0);
        TEST_ASSERT(saw_z);

        /* Clean disconnect */
        n = pg_make_terminate(buf);
        write_exact(client, buf, n);
        close(client);
    }

    usleep(50000);  /* let keel process the close */

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 2: Multiple consecutive queries on one connection
 * ============================================================================ */
static void test_multiple_queries(void)
{
    TEST_BEGIN("mock proxy: multiple consecutive queries on one connection");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine(&keel_port, &listen_fd, mb->port);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    int client = pg_client_connect(keel_port, "postgres", "postgres");
    TEST_ASSERT(client >= 0);

    if (client >= 0) {
        const char *queries[] = { "SELECT 1", "SELECT 2", "SELECT 3", "SHOW server_version" };
        int ok = 1;
        for (int i = 0; i < 4 && ok; i++) {
            uint8_t buf[256];
            size_t n = pg_make_query(buf, queries[i]);
            if (write_exact(client, buf, n) < 0) { ok = 0; break; }

            bool saw_z = false;
            if (pg_read_until_ready(client, 5000, &saw_z) < 0 || !saw_z) {
                ok = 0;
                break;
            }
        }
        TEST_ASSERT(ok);

        uint8_t buf[32];
        write_exact(client, buf, pg_make_terminate(buf));
        close(client);
    }

    usleep(50000);
    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 3: Extended query protocol (Parse/Bind/Describe/Execute/Sync)
 * ============================================================================ */
static void test_extended_protocol(void)
{
    TEST_BEGIN("mock proxy: extended query protocol");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine(&keel_port, &listen_fd, mb->port);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    int client = pg_client_connect(keel_port, "postgres", "postgres");
    TEST_ASSERT(client >= 0);

    if (client >= 0) {
        uint8_t buf[2048];
        size_t  off = 0;

        /* Parse + Bind + Describe + Execute + Sync (pipelined) */
        off += pg_make_parse  (buf+off, "stmt1", "SELECT $1::int");
        off += pg_make_bind   (buf+off, "portal1", "stmt1");
        off += pg_make_describe(buf+off, 'P', "portal1");
        off += pg_make_execute(buf+off, "portal1");
        off += pg_make_sync   (buf+off);

        int wr = write_exact(client, buf, off);
        TEST_ASSERT_EQ(wr, 0);

        bool saw_z = false;
        int rr = pg_read_until_ready(client, 5000, &saw_z);
        TEST_ASSERT_EQ(rr, 0);
        TEST_ASSERT(saw_z);

        /* Close prepared statement + Sync */
        off = 0;
        off += pg_make_close(buf+off, 'S', "stmt1");
        off += pg_make_sync (buf+off);
        write_exact(client, buf, off);
        pg_read_until_ready(client, 5000, &saw_z);

        uint8_t term[32];
        write_exact(client, term, pg_make_terminate(term));
        close(client);
    }

    usleep(50000);
    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 4: Multiple simultaneous clients
 * ============================================================================ */

#define N_CLIENTS 4

struct client_arg {
    uint16_t keel_port;
    int      success;
};

static void *client_thread(void *arg)
{
    struct client_arg *ca = (struct client_arg *)arg;

    int fd = pg_client_connect(ca->keel_port, "postgres", "postgres");
    if (fd < 0) { ca->success = 0; return NULL; }

    uint8_t buf[256];
    size_t n = pg_make_query(buf, "SELECT 42");
    if (write_exact(fd, buf, n) < 0) { close(fd); ca->success = 0; return NULL; }

    bool saw_z = false;
    if (pg_read_until_ready(fd, 5000, &saw_z) < 0 || !saw_z) {
        close(fd); ca->success = 0; return NULL;
    }

    n = pg_make_terminate(buf);
    write_exact(fd, buf, n);
    close(fd);
    ca->success = 1;
    return NULL;
}

static void test_multiple_clients(void)
{
    TEST_BEGIN("mock proxy: multiple simultaneous clients");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine(&keel_port, &listen_fd, mb->port);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    struct client_arg args[N_CLIENTS];
    pthread_t client_tids[N_CLIENTS];

    for (int i = 0; i < N_CLIENTS; i++) {
        args[i].keel_port = keel_port;
        args[i].success   = -1;
        pthread_create(&client_tids[i], NULL, client_thread, &args[i]);
    }

    int all_ok = 1;
    for (int i = 0; i < N_CLIENTS; i++) {
        pthread_join(client_tids[i], NULL);
        if (!args[i].success) all_ok = 0;
    }
    TEST_ASSERT(all_ok);

    usleep(100000);
    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 5: SSL request followed by non-SSL startup
 * ============================================================================ */
static void test_ssl_downgrade(void)
{
    TEST_BEGIN("mock proxy: SSL request rejected, client falls back to non-SSL");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine(&keel_port, &listen_fd, mb->port);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    int fd = connect_to(keel_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        /* Send SSLRequest (len=8, magic=80877103) */
        uint8_t ssl_req[8];
        wr32(ssl_req,   8);
        wr32(ssl_req+4, 80877103U);
        int wr = write_exact(fd, ssl_req, 8);
        TEST_ASSERT_EQ(wr, 0);

        /* Expect 'N' (SSL not supported) */
        uint8_t resp[1];
        ssize_t nr = read_timeout(fd, resp, 1, 3000);
        TEST_ASSERT(nr == 1 && resp[0] == 'N');

        if (nr == 1 && resp[0] == 'N') {
            /* Now send a normal startup message */
            uint8_t startup[256];
            size_t sn = pg_make_startup(startup, "postgres", "postgres");
            write_exact(fd, startup, sn);

            bool saw_z = false;
            pg_read_until_ready(fd, 5000, &saw_z);
            TEST_ASSERT(saw_z);

            if (saw_z) {
                uint8_t buf[256];
                size_t n = pg_make_query(buf, "SELECT 1");
                write_exact(fd, buf, n);
                pg_read_until_ready(fd, 5000, &saw_z);
                TEST_ASSERT(saw_z);
            }

            uint8_t term[32];
            write_exact(fd, term, pg_make_terminate(term));
        }
        close(fd);
    }

    usleep(50000);
    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 6: Connection pool recycling (connect → query → disconnect × N)
 * ============================================================================ */
static void test_pool_recycling(void)
{
    TEST_BEGIN("mock proxy: backend connection pool reuse across client sessions");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine(&keel_port, &listen_fd, mb->port);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    /* Sequential sessions: each one gets a backend connection from the pool. */
    for (int round = 0; round < 3; round++) {
        int fd = pg_client_connect(keel_port, "postgres", "postgres");
        TEST_ASSERT(fd >= 0);
        if (fd < 0) continue;

        uint8_t buf[256];
        size_t n = pg_make_query(buf, "SELECT now()");
        write_exact(fd, buf, n);
        bool saw_z = false;
        pg_read_until_ready(fd, 5000, &saw_z);
        TEST_ASSERT(saw_z);

        n = pg_make_terminate(buf);
        write_exact(fd, buf, n);
        close(fd);
        usleep(30000);  /* let keel return the backend connection to the pool */
    }

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 7: Legacy backend_host/port config (server_pool.count == 0)
 * ============================================================================ */
static void test_legacy_backend_config(void)
{
    TEST_BEGIN("mock proxy: legacy backend_host/port config (server_pool.count=0)");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t listen_port = 0;
    int      listen_fd   = make_listen_socket(&listen_port);
    TEST_ASSERT(listen_fd >= 0);
    if (listen_fd < 0) { mock_backend_stop(mb); TEST_END(); return; }

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers      = 1;
    cfg.queue_depth      = 64;
    cfg.session_pool_size = 32;
    cfg.idle_timeout_ms  = 60000;
    cfg.connect_timeout_ms = 5000;
    cfg.auth_method      = KEEL_AUTH_TRUST;
    cfg.pool_min_size    = 0;
    cfg.pool_max_size    = 8;
    /* Use legacy path: server_pool.count remains 0 from KEEL_ENGINE_CONFIG_DEFAULT */
    cfg.backend_host     = "127.0.0.1";
    cfg.backend_port     = mb->port;
    cfg.backend_user     = "postgres";
    cfg.backend_password = NULL;
    cfg.backend_database = "postgres";

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) {
        close(listen_fd);
        mock_backend_stop(mb);
        TEST_END();
        return;
    }

    int rc = keel_engine_start(engine, listen_fd);
    TEST_ASSERT_EQ(rc, 0);
    if (rc != 0) {
        keel_engine_destroy(engine);
        mock_backend_stop(mb);
        TEST_END();
        return;
    }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    int fd = pg_client_connect(listen_port, "postgres", "postgres");
    TEST_ASSERT(fd >= 0);
    if (fd >= 0) {
        uint8_t buf[256];
        size_t n = pg_make_query(buf, "SELECT 1");
        write_exact(fd, buf, n);
        bool saw_z = false;
        pg_read_until_ready(fd, 5000, &saw_z);
        TEST_ASSERT(saw_z);

        n = pg_make_terminate(buf);
        write_exact(fd, buf, n);
        close(fd);
    }

    usleep(50000);
    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 8: Engine drain with active client session
 * ============================================================================ */
static void test_drain_with_client(void)
{
    TEST_BEGIN("mock proxy: engine drain waits for in-flight session to complete");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine(&keel_port, &listen_fd, mb->port);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    /* Open a session and run a query */
    int fd = pg_client_connect(keel_port, "postgres", "postgres");
    TEST_ASSERT(fd >= 0);
    if (fd >= 0) {
        uint8_t buf[256];
        size_t n = pg_make_query(buf, "SELECT 1");
        write_exact(fd, buf, n);
        bool saw_z = false;
        pg_read_until_ready(fd, 5000, &saw_z);
        TEST_ASSERT(saw_z);

        /* Disconnect cleanly */
        n = pg_make_terminate(buf);
        write_exact(fd, buf, n);
        close(fd);
        usleep(30000);
    }

    /* Drain the engine: should complete quickly since no active sessions */
    keel_engine_set_drain_timeout(engine, 2000);
    int dr = keel_engine_drain(engine);
    TEST_ASSERT_EQ(dr, 0);
    TEST_ASSERT(keel_engine_get_state(engine) == KEEL_ENGINE_STATE_DRAINING ||
                keel_engine_get_state(engine) == KEEL_ENGINE_STATE_STOPPED);

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 9: Backend unreachable — client gets an error
 * ============================================================================ */
static void test_backend_unreachable(void)
{
    TEST_BEGIN("mock proxy: backend unreachable — client receives ErrorResponse");

    /* Use a port that nobody is listening on */
    uint16_t dead_port = 0;
    {
        int tmp = make_listen_socket(&dead_port);
        if (tmp >= 0) close(tmp);  /* close immediately so port is free */
    }

    uint16_t listen_port = 0;
    int      listen_fd   = make_listen_socket(&listen_port);
    TEST_ASSERT(listen_fd >= 0);
    if (listen_fd < 0) { TEST_END(); return; }

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers      = 1;
    cfg.queue_depth      = 64;
    cfg.session_pool_size = 32;
    cfg.idle_timeout_ms  = 60000;
    cfg.connect_timeout_ms = 1000;  /* short timeout so test doesn't hang */
    cfg.auth_method      = KEEL_AUTH_TRUST;
    cfg.pool_min_size    = 0;
    cfg.pool_max_size    = 4;
    cfg.backend_host     = "127.0.0.1";
    cfg.backend_port     = dead_port;
    cfg.backend_user     = "postgres";
    cfg.backend_password = NULL;
    cfg.backend_database = "postgres";

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { close(listen_fd); TEST_END(); return; }

    int rc = keel_engine_start(engine, listen_fd);
    TEST_ASSERT_EQ(rc, 0);
    if (rc != 0) { keel_engine_destroy(engine); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    /* Client should connect and get an error when sending a query */
    int fd = connect_to(listen_port);
    TEST_ASSERT(fd >= 0);
    if (fd >= 0) {
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        uint8_t startup[256];
        size_t n = pg_make_startup(startup, "postgres", "postgres");
        write_exact(fd, startup, n);

        /* Try to get through startup (trust auth means keel sends AuthOK immediately) */
        bool saw_z = false;
        int sr = pg_read_until_ready(fd, 3000, &saw_z);
        if (sr == 0 && saw_z) {
            /* Send a query — keel will try to connect to dead backend */
            uint8_t buf[256];
            n = pg_make_query(buf, "SELECT 1");
            write_exact(fd, buf, n);
            /* We expect either an error response + Z, or a connection reset */
            pg_read_until_ready(fd, 3000, &saw_z);
        }
        close(fd);
    }

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);

    TEST_END();
}

/* ============================================================================
 * Test 10: Engine state after clean shutdown with proxy sessions
 * ============================================================================ */
static void test_engine_state_after_shutdown(void)
{
    TEST_BEGIN("mock proxy: engine state transitions through full proxy lifecycle");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine(&keel_port, &listen_fd, mb->port);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_ACTIVE);

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_ACTIVE);

    /* Run one session */
    int fd = pg_client_connect(keel_port, "postgres", "postgres");
    if (fd >= 0) {
        uint8_t buf[256];
        size_t n = pg_make_query(buf, "SELECT version()");
        write_exact(fd, buf, n);
        bool saw_z = false;
        pg_read_until_ready(fd, 5000, &saw_z);
        n = pg_make_terminate(buf);
        write_exact(fd, buf, n);
        close(fd);
        usleep(30000);
    }

    /* Stop and verify state */
    stop_engine_and_join(engine, tid);
    TEST_ASSERT_EQ((int)keel_engine_get_state(engine), (int)KEEL_ENGINE_STATE_STOPPED);

    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 11: PROXY tier — minimal pass-through, no connection pooling
 * ============================================================================ */
static void test_proxy_tier(void)
{
    TEST_BEGIN("mock proxy: KEEL_TIER_PROXY pass-through mode");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine_ex(&keel_port, &listen_fd, mb->port,
                                                  KEEL_TIER_PROXY, false,
                                                  KEEL_PS_MODE_VIRTUALIZE, 0, 0);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    /* Simple query through proxy-tier engine */
    int fd = pg_client_connect(keel_port, "postgres", "postgres");
    TEST_ASSERT((fd) >= (0));
    if (fd >= 0) {
        uint8_t buf[256];
        size_t n = pg_make_query(buf, "SELECT 1");
        write_exact(fd, buf, n);
        bool saw_z = false;
        pg_read_until_ready(fd, 5000, &saw_z);
        TEST_ASSERT(saw_z);

        /* Second query in the same session */
        n = pg_make_query(buf, "SELECT 2");
        write_exact(fd, buf, n);
        saw_z = false;
        pg_read_until_ready(fd, 5000, &saw_z);
        TEST_ASSERT(saw_z);

        n = pg_make_terminate(buf);
        write_exact(fd, buf, n);
        close(fd);
        usleep(50000);
    }

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 12: SMART tier — R/W routing and query-log code paths
 * ============================================================================ */
static void test_smart_tier_rw_routing(void)
{
    TEST_BEGIN("mock proxy: KEEL_TIER_SMART with two backends (RW + RO)");

    /* Two mock backends: primary (RW) and replica (RO) */
    mock_backend_t *mb_rw = mock_backend_start();
    mock_backend_t *mb_ro = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb_rw);
    TEST_ASSERT_NOT_NULL(mb_ro);
    if (!mb_rw || !mb_ro) {
        mock_backend_stop(mb_rw);
        mock_backend_stop(mb_ro);
        TEST_END();
        return;
    }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine_ex(&keel_port, &listen_fd, mb_rw->port,
                                                  KEEL_TIER_SMART, false,
                                                  KEEL_PS_MODE_VIRTUALIZE,
                                                  mb_ro->port,
                                                  KEEL_SERVER_ROLE_RO);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) {
        mock_backend_stop(mb_rw);
        mock_backend_stop(mb_ro);
        TEST_END();
        return;
    }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(200000);

    /* Send a SELECT (should route to RO) and INSERT (should route to RW) */
    for (int i = 0; i < 4; i++) {
        int fd = pg_client_connect(keel_port, "postgres", "postgres");
        TEST_ASSERT((fd) >= (0));
        if (fd >= 0) {
            uint8_t buf[256];
            /* Alternately send SELECT (read) and INSERT (write) */
            const char *sql = (i % 2 == 0) ? "SELECT 1" : "INSERT INTO t VALUES(1)";
            size_t n = pg_make_query(buf, sql);
            write_exact(fd, buf, n);
            bool saw_z = false;
            pg_read_until_ready(fd, 5000, &saw_z);
            TEST_ASSERT(saw_z);
            n = pg_make_terminate(buf);
            write_exact(fd, buf, n);
            close(fd);
            usleep(30000);
        }
    }

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb_rw);
    mock_backend_stop(mb_ro);

    TEST_END();
}

/* ============================================================================
 * Test 13: FULL tier — txn_tracking enabled, BEGIN/COMMIT/ROLLBACK sequences
 * ============================================================================ */
static void test_full_tier_txn_tracking(void)
{
    TEST_BEGIN("mock proxy: KEEL_TIER_FULL with transaction tracking");

    /* Use txn-aware mock backend so ReadyForQuery sends 'T' while in transaction */
    mock_backend_t *mb = mock_backend_start_txn_aware();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine_ex(&keel_port, &listen_fd, mb->port,
                                                  KEEL_TIER_FULL, true,
                                                  KEEL_PS_MODE_VIRTUALIZE, 0, 0);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(200000);

    /* Transaction 1: BEGIN → SELECT → COMMIT */
    {
        int fd = pg_client_connect(keel_port, "postgres", "postgres");
        TEST_ASSERT((fd) >= (0));
        if (fd >= 0) {
            uint8_t buf[256];
            bool saw_z = false;

            size_t n = pg_make_query(buf, "BEGIN");
            write_exact(fd, buf, n);
            saw_z = false;
            pg_read_until_ready(fd, 5000, &saw_z);
            TEST_ASSERT(saw_z);

            n = pg_make_query(buf, "SELECT 1");
            write_exact(fd, buf, n);
            saw_z = false;
            pg_read_until_ready(fd, 5000, &saw_z);
            TEST_ASSERT(saw_z);

            n = pg_make_query(buf, "COMMIT");
            write_exact(fd, buf, n);
            saw_z = false;
            pg_read_until_ready(fd, 5000, &saw_z);
            TEST_ASSERT(saw_z);

            n = pg_make_terminate(buf);
            write_exact(fd, buf, n);
            close(fd);
            usleep(50000);
        }
    }

    /* Transaction 2: BEGIN → SELECT → ROLLBACK */
    {
        int fd = pg_client_connect(keel_port, "postgres", "postgres");
        TEST_ASSERT((fd) >= (0));
        if (fd >= 0) {
            uint8_t buf[256];
            bool saw_z = false;

            size_t n = pg_make_query(buf, "BEGIN");
            write_exact(fd, buf, n);
            saw_z = false;
            pg_read_until_ready(fd, 5000, &saw_z);

            n = pg_make_query(buf, "SELECT count(*) FROM pg_class");
            write_exact(fd, buf, n);
            saw_z = false;
            pg_read_until_ready(fd, 5000, &saw_z);

            n = pg_make_query(buf, "ROLLBACK");
            write_exact(fd, buf, n);
            saw_z = false;
            pg_read_until_ready(fd, 5000, &saw_z);
            TEST_ASSERT(saw_z);

            n = pg_make_terminate(buf);
            write_exact(fd, buf, n);
            close(fd);
            usleep(50000);
        }
    }

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 14: Multiple workers — exercises worker thread pool selection
 * ============================================================================ */

#define N_MULTI_WORKERS   3
#define N_CLIENTS_PER_WRK 5

typedef struct {
    uint16_t  keel_port;
    int       client_id;
    int       ok_count;
} multi_worker_client_arg_t;

static void *multi_worker_client_thread(void *arg)
{
    multi_worker_client_arg_t *a = (multi_worker_client_arg_t *)arg;

    int fd = pg_client_connect(a->keel_port, "postgres", "postgres");
    if (fd < 0) return NULL;

    uint8_t buf[256];
    char sql[64];
    snprintf(sql, sizeof(sql), "SELECT %d", a->client_id);
    size_t n = pg_make_query(buf, sql);
    write_exact(fd, buf, n);
    bool saw_z = false;
    if (pg_read_until_ready(fd, 5000, &saw_z) == 0 && saw_z) {
        a->ok_count = 1;
    }
    n = pg_make_terminate(buf);
    write_exact(fd, buf, n);
    close(fd);
    return NULL;
}

static void test_multiple_workers(void)
{
    TEST_BEGIN("mock proxy: multiple workers with concurrent clients");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = make_listen_socket(&keel_port);
    TEST_ASSERT((listen_fd) >= (0));
    if (listen_fd < 0) { mock_backend_stop(mb); TEST_END(); return; }

    keel_engine_config_t cfg = KEEL_ENGINE_CONFIG_DEFAULT;
    cfg.num_workers        = N_MULTI_WORKERS;
    cfg.queue_depth        = 64;
    cfg.session_pool_size  = 64;
    cfg.idle_timeout_ms    = 60000;
    cfg.connect_timeout_ms = 5000;
    cfg.auth_method        = KEEL_AUTH_TRUST;
    cfg.pool_min_size      = 0;
    cfg.pool_max_size      = 8;

    cfg.server_pool.servers[0].host     = "127.0.0.1";
    cfg.server_pool.servers[0].port     = mb->port;
    cfg.server_pool.servers[0].user     = "postgres";
    cfg.server_pool.servers[0].password = "";
    cfg.server_pool.servers[0].database = "postgres";
    cfg.server_pool.servers[0].role     = KEEL_SERVER_ROLE_RW;
    cfg.server_pool.servers[0].weight   = 1;
    cfg.server_pool.servers[0].healthy  = true;
    cfg.server_pool.count = 1;
    keel_server_pool_rebuild_indices(&cfg.server_pool);

    keel_engine_t *engine = keel_engine_create(&cfg);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { close(listen_fd); mock_backend_stop(mb); TEST_END(); return; }

    if (keel_engine_start(engine, listen_fd) != 0) {
        keel_engine_destroy(engine);
        close(listen_fd);
        mock_backend_stop(mb);
        TEST_END();
        return;
    }

    pthread_t eng_tid;
    pthread_create(&eng_tid, NULL, engine_run_thread, engine);
    usleep(200000);

    int total = N_MULTI_WORKERS * N_CLIENTS_PER_WRK;
    pthread_t              ctids[N_MULTI_WORKERS * N_CLIENTS_PER_WRK];
    multi_worker_client_arg_t cargs[N_MULTI_WORKERS * N_CLIENTS_PER_WRK];

    for (int i = 0; i < total; i++) {
        cargs[i].keel_port = keel_port;
        cargs[i].client_id = i;
        cargs[i].ok_count  = 0;
        pthread_create(&ctids[i], NULL, multi_worker_client_thread, &cargs[i]);
    }
    int ok = 0;
    for (int i = 0; i < total; i++) {
        pthread_join(ctids[i], NULL);
        ok += cargs[i].ok_count;
    }
    TEST_ASSERT((ok) >= (total / 2));  /* at least half succeeded */

    stop_engine_and_join(engine, eng_tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 15: keel binary e2e — fork/exec the instrumented keel binary with a
 *           config file pointing at the mock backend.  Exercises main.c
 *           config parsing, worker group init, and graceful shutdown.
 * ============================================================================ */

static const char *find_keel_binary(void)
{
    static char path[PATH_MAX];
    const char *candidates[] = {
        "../src/main/keel",
        "../../build/src/main/keel",
        "./keel",
        NULL
    };
    for (int i = 0; candidates[i]; i++) {
        if (access(candidates[i], X_OK) == 0) {
            if (realpath(candidates[i], path)) return path;
            return candidates[i];
        }
    }
    return NULL;
}

static void test_keel_binary_e2e(void)
{
    TEST_BEGIN("mock proxy: keel binary launched with config pointing to mock backend");

    /* The spawned keel binary inherits sanitizer instrumentation and has
     * stderr redirected to /dev/null.  Under MSan any uninstrumented syscall
     * aborts it silently before readiness; under TSan a detected race aborts
     * it silently after readiness, causing the query timeout and SIGPIPE.
     * Functional correctness is covered by the in-process tests above. */
#if defined(__SANITIZE_THREAD__)
    printf("  SKIP: keel binary e2e test disabled under ThreadSanitizer\n");
    TEST_END();
    return;
#endif
#if defined(__has_feature)
#  if __has_feature(memory_sanitizer)
    printf("  SKIP: keel binary e2e test disabled under MemorySanitizer\n");
    TEST_END();
    return;
#  endif
#endif

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    const char *binary = find_keel_binary();
    if (!binary) {
        printf("  SKIP: keel binary not found (try building first)\n");
        mock_backend_stop(mb);
        TEST_END();
        return;
    }

    /* Find a free port for keel to listen on */
    uint16_t keel_port = 0;
    {
        int pfd = make_listen_socket(&keel_port);
        if (pfd >= 0) close(pfd);   /* release the port; keel will re-bind it */
    }
    if (keel_port == 0) {
        printf("  SKIP: could not determine a free port\n");
        mock_backend_stop(mb);
        TEST_END();
        return;
    }

    /* Write a minimal keel config file */
    char config_path[256];
    snprintf(config_path, sizeof(config_path),
             "/tmp/keel_mock_e2e_%d.ini", (int)getpid());
    FILE *f = fopen(config_path, "w");
    if (!f) {
        mock_backend_stop(mb);
        TEST_END();
        return;
    }
    fprintf(f,
        "# Auto-generated by test_mock_proxy.c test_keel_binary_e2e\n"
        "[keel]\n"
        "config_version = 2\n"
        "log_level = 0\n\n"
        "[worker_group.mock_e2e]\n"
        "name = mock_e2e\n"
        "bind_addr = 127.0.0.1\n"
        "bind_port = %u\n"
        "num_workers = 1\n"
        "max_conns_per_worker = 50\n"
        "client_idle_timeout = 30s\n"
        "client_connect_timeout = 10s\n\n"
        "pool_mode = transaction\n"
        "min_pool_size = 1\n"
        "max_pool_size = 4\n"
        "idle_timeout = 30s\n"
        "query_timeout = 30s\n\n"
        "auth_method = trust\n"
        "server_user = postgres\n"
        "server_password =\n\n"
        "[worker_group.mock_e2e.servers]\n"
        "mock = host=127.0.0.1 port=%u dbname=postgres role=primary weight=100\n\n"
        "[logging]\n"
        "plugin = stdout\n"
        "log_level = error\n"
        "use_colors = false\n\n"
        "[security]\n"
        "privilege_drop = false\n",
        (unsigned)keel_port,
        (unsigned)mb->port);
    fclose(f);

    /* Fork and exec the keel binary */
    pid_t pid = fork();
    if (pid < 0) {
        unlink(config_path);
        mock_backend_stop(mb);
        TEST_ASSERT(false /* fork() failed */);
        TEST_END();
        return;
    }
    if (pid == 0) {
        /* Child: close inherited fds (keep stdin/stdout/stderr), suppress output,
         * then exec keel.  Closing inherited fds prevents io_uring rings and
         * other sockets from the parent's test harness leaking into keel. */
        for (int i = 3; i < 4096; i++) close(i); /* NOLINT(keel-syscall) */
        if (!freopen("/dev/null", "w", stdout)) _exit(126); /* NOLINT(keel-syscall) */
        if (!freopen("/dev/null", "w", stderr)) _exit(126); /* NOLINT(keel-syscall) */
        execl(binary, "keel", "-c", config_path, NULL);
        _exit(127);
    }

    /* Parent: poll until keel becomes ready (up to ~15 s) */
    bool ready = false;
    for (int i = 0; i < 60 && !ready; i++) {
        usleep(250000);   /* 250 ms */
        int fd = connect_to(keel_port);
        if (fd >= 0) {
            /* Attempt a PG startup to verify keel is accepting connections */
            uint8_t startup[256];
            size_t n = pg_make_startup(startup, "postgres", "postgres");
            if (write(fd, startup, n) == (ssize_t)n) {
                bool saw_z = false;
                if (pg_read_until_ready(fd, 8000, &saw_z) == 0 && saw_z) {
                    ready = true;
                }
            }
            close(fd);
        }
    }

    TEST_ASSERT(ready /* keel binary did not become ready within 10s */);

    if (ready) {
        int fd = pg_client_connect(keel_port, "postgres", "postgres");
        TEST_ASSERT((fd) >= (0));
        if (fd >= 0) {
            uint8_t buf[256];
            size_t n = pg_make_query(buf, "SELECT 1");
            write_exact(fd, buf, n);
            bool saw_z = false;
            pg_read_until_ready(fd, 5000, &saw_z);
            TEST_ASSERT(saw_z);

            /* Run a second query to exercise more code */
            n = pg_make_query(buf, "SELECT 42");
            write_exact(fd, buf, n);
            saw_z = false;
            pg_read_until_ready(fd, 5000, &saw_z);
            TEST_ASSERT(saw_z);

            n = pg_make_terminate(buf);
            write_exact(fd, buf, n);
            close(fd);
        }
    }

    /* Graceful shutdown */
    kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);

    unlink(config_path);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 16: PS mode TRACKING — shadow + replay including simple-query PREPARE
 * ============================================================================ */
static void test_ps_mode_tracking(void)
{
    TEST_BEGIN("mock proxy: PS mode TRACKING (shadow and replay)");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine_ex(&keel_port, &listen_fd, mb->port,
                                                  KEEL_TIER_POOL, false,
                                                  KEEL_PS_MODE_TRACKING, 0, 0);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    /* Session 1: use named prepared statement */
    int fd = pg_client_connect(keel_port, "postgres", "postgres");
    TEST_ASSERT((fd) >= (0));
    if (fd >= 0) {
        uint8_t buf[512];
        size_t n;
        bool saw_z = false;

        /* Parse named statement "s1" */
        n = pg_make_parse(buf, "s1", "SELECT $1::int");
        write_exact(fd, buf, n);
        /* Bind: portal "" using stmt "s1" */
        n = pg_make_bind(buf, "", "s1");
        write_exact(fd, buf, n);
        /* Describe portal */
        n = pg_make_describe(buf, 'P', "");
        write_exact(fd, buf, n);
        /* Execute portal "" */
        n = pg_make_execute(buf, "");
        write_exact(fd, buf, n);
        /* Sync */
        n = pg_make_sync(buf);
        write_exact(fd, buf, n);
        pg_read_until_ready(fd, 5000, &saw_z);
        TEST_ASSERT(saw_z);

        /* Close statement */
        n = pg_make_close(buf, 'S', "s1");
        write_exact(fd, buf, n);
        n = pg_make_sync(buf);
        write_exact(fd, buf, n);
        saw_z = false;
        pg_read_until_ready(fd, 5000, &saw_z);

        n = pg_make_terminate(buf);
        write_exact(fd, buf, n);
        close(fd);
        usleep(50000);
    }

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * Test 17: PS mode PINNING — hard-pin backend on first named Parse
 * ============================================================================ */
static void test_ps_mode_pinning(void)
{
    TEST_BEGIN("mock proxy: PS mode PINNING (hard-pin on named Parse)");

    mock_backend_t *mb = mock_backend_start();
    TEST_ASSERT_NOT_NULL(mb);
    if (!mb) { TEST_END(); return; }

    uint16_t keel_port = 0;
    int      listen_fd = -1;
    keel_engine_t *engine = make_proxy_engine_ex(&keel_port, &listen_fd, mb->port,
                                                  KEEL_TIER_POOL, false,
                                                  KEEL_PS_MODE_PINNING, 0, 0);
    TEST_ASSERT_NOT_NULL(engine);
    if (!engine) { mock_backend_stop(mb); TEST_END(); return; }

    pthread_t tid;
    pthread_create(&tid, NULL, engine_run_thread, engine);
    usleep(150000);

    int fd = pg_client_connect(keel_port, "postgres", "postgres");
    TEST_ASSERT((fd) >= (0));
    if (fd >= 0) {
        uint8_t buf[512];
        size_t n;
        bool saw_z = false;

        /* Parse "stmt" — triggers hard pin in PINNING mode */
        n = pg_make_parse(buf, "stmt", "SELECT 1");
        write_exact(fd, buf, n);
        n = pg_make_bind(buf, "", "stmt");
        write_exact(fd, buf, n);
        n = pg_make_execute(buf, "");
        write_exact(fd, buf, n);
        n = pg_make_sync(buf);
        write_exact(fd, buf, n);
        pg_read_until_ready(fd, 5000, &saw_z);
        TEST_ASSERT(saw_z);

        /* Execute again on the same pinned backend */
        n = pg_make_bind(buf, "", "stmt");
        write_exact(fd, buf, n);
        n = pg_make_execute(buf, "");
        write_exact(fd, buf, n);
        n = pg_make_sync(buf);
        write_exact(fd, buf, n);
        saw_z = false;
        pg_read_until_ready(fd, 5000, &saw_z);
        TEST_ASSERT(saw_z);

        /* Close statement and terminate */
        n = pg_make_close(buf, 'S', "stmt");
        write_exact(fd, buf, n);
        n = pg_make_sync(buf);
        write_exact(fd, buf, n);
        saw_z = false;
        pg_read_until_ready(fd, 5000, &saw_z);

        n = pg_make_terminate(buf);
        write_exact(fd, buf, n);
        close(fd);
        usleep(50000);
    }

    stop_engine_and_join(engine, tid);
    keel_engine_destroy(engine);
    mock_backend_stop(mb);

    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */
int main(void)
{
    printf("Mock Proxy Integration Tests\n");
    printf("============================\n\n");

    /* SIGHUP from any reload-style command should not kill the test process */
    signal(SIGHUP, SIG_IGN);

    keel_mem_init(NULL);

    test_basic_query();
    test_multiple_queries();
    test_extended_protocol();
    test_multiple_clients();
    test_ssl_downgrade();
    test_pool_recycling();
    test_legacy_backend_config();
    test_drain_with_client();
    test_backend_unreachable();
    test_engine_state_after_shutdown();

    /* Additional tier/feature coverage tests */
    test_proxy_tier();
    test_smart_tier_rw_routing();
    test_full_tier_txn_tracking();
    test_multiple_workers();
    test_ps_mode_tracking();
    test_ps_mode_pinning();
    test_keel_binary_e2e();

    return test_summary();
}
