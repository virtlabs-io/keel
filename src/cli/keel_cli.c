/**
 * @file keel_cli.c
 * @brief Standalone command-line client for KEEL's PostgreSQL-wire admin endpoint.
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 *
 * `keel-cli` is a deliberately small operational tool that speaks just enough
 * PostgreSQL wire protocol to authenticate against KEEL's admin socket, send a
 * single SimpleQuery command, and render the tabular response to stdout.
 *
 * Responsibilities:
 * - parse CLI arguments or stdin command input
 * - open a TCP connection to the admin endpoint
 * - perform the minimal PostgreSQL startup handshake
 * - send one admin command using SimpleQuery protocol
 * - print tab-separated output or human-readable errors
 *
 * Scope limits:
 * - no TLS
 * - no extended query protocol
 * - no interactive shell state
 * - no binary column handling
 */

#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ============================================================================
 * Wire helpers
 * ============================================================================ */

/**
 * @brief Encode a 32-bit integer in network byte order.
 */
static inline void wr32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

/**
 * @brief Decode a 16-bit integer from network byte order.
 */
static inline uint16_t rd16(const uint8_t *p) {
    return (uint16_t)((uint16_t)p[0] << 8 | p[1]);
}

/**
 * @brief Decode a 32-bit integer from network byte order.
 */
static inline uint32_t rd32(const uint8_t *p) {
    return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 |
           (uint32_t)p[2] << 8  | (uint32_t)p[3];
}

/**
 * @brief Decode a signed 32-bit integer from network byte order.
 */
static inline int32_t rd32s(const uint8_t *p) {
    uint32_t v = rd32(p);
    return (int32_t)v;
}

/**
 * @brief Receive exactly `n` bytes unless EOF or hard error interrupts the read.
 *
 * @return Exact byte count on success, short count on EOF, or `-1` on error.
 */
static ssize_t xrecv(int fd, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    size_t got = 0;
    while (got < n) {
        ssize_t r = recv(fd, p + got, n - got, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (r == 0) return (ssize_t)got;   /* EOF */
        got += (size_t)r;
    }
    return (ssize_t)got;
}

/**
 * @brief Send an entire buffer unless a hard error occurs.
 *
 * @return Exact byte count on success, or `-1` on error.
 */
static ssize_t xsend(int fd, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t sent = 0;
    while (sent < n) {
        ssize_t r = send(fd, p + sent, n - sent, MSG_NOSIGNAL);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)r;
    }
    return (ssize_t)sent;
}

/* ============================================================================
 * PG startup + query
 * ============================================================================ */

/**
 * @brief Send the minimal PostgreSQL StartupMessage accepted by the admin server.
 *
 * @param fd Connected admin socket.
 * @return `0` on success, `-1` on send failure.
 *
 * @note The client hard-codes `user=admin` because the current admin endpoint
 *       trusts socket reachability rather than performing full authentication.
 */
static int pg_startup(int fd) {
    /* StartupMessage: length(4) + protocol(4) + user\0admin\0 + \0 */
    const char *params = "user\0admin\0\0";
    size_t plen = 12;   /* strlen("user") + 1 + strlen("admin") + 1 + 1 */
    uint32_t total = 4 + 4 + (uint32_t)plen;

    uint8_t buf[64];
    wr32(buf, total);
    wr32(buf + 4, 0x00030000u);   /* protocol 3.0 */
    memcpy(buf + 8, params, plen);

    return xsend(fd, buf, total) < 0 ? -1 : 0;
}

/**
 * @brief Consume startup responses until `ReadyForQuery` or an error arrives.
 *
 * @param fd Connected admin socket.
 * @return `0` once the server is ready for a query, `-1` on protocol or I/O failure.
 */
static int pg_wait_ready(int fd) {
    for (;;) {
        uint8_t hdr[5];
        if (xrecv(fd, hdr, 5) < 5) return -1;

        uint8_t mtype = hdr[0];
        uint32_t mlen = rd32(hdr + 1);
        size_t body = mlen - 4;

        if (body > 0) {
            /* Read and discard payload */
            uint8_t discard[4096];
            size_t rem = body;
            while (rem > 0) {
                size_t chunk = rem > sizeof(discard) ? sizeof(discard) : rem;
                if (xrecv(fd, discard, chunk) < (ssize_t)chunk) return -1;
                rem -= chunk;
            }
        }

        if (mtype == 'Z') return 0;   /* ReadyForQuery */
        if (mtype == 'E') return -1;   /* Error during startup */
    }
}

/**
 * @brief Send one PostgreSQL SimpleQuery message.
 *
 * @param fd Connected admin socket.
 * @param query Command text to send.
 * @return `0` on success, `-1` on send failure.
 */
static int pg_simple_query(int fd, const char *query) {
    size_t qlen = strlen(query) + 1;   /* include NUL */
    uint32_t total = 4 + (uint32_t)qlen;

    uint8_t hdr[5];
    hdr[0] = 'Q';
    wr32(hdr + 1, total);

    if (xsend(fd, hdr, 5) < 0) return -1;
    if (xsend(fd, query, qlen) < 0) return -1;
    return 0;
}

/* ============================================================================
 * Result printer
 * ============================================================================ */

/**
 * @brief Read, decode, and print one PostgreSQL result stream.
 *
 * @param fd Connected admin socket.
 * @return Process exit code: `0` for success, `1` for protocol/I/O/server error.
 *
 * Behavior:
 * - prints `RowDescription` column names as a tab-separated header
 * - prints each `DataRow` as a tab-separated line
 * - extracts `ErrorResponse` message fields for stderr
 * - stops when `ReadyForQuery` is received
 *
 * Corner cases:
 * - malformed message bodies cause the function to abandon parsing and return
 *   a non-zero exit code
 * - all columns are treated as textual payloads
 */
static int process_results(int fd) {
    int ncols = 0;
    bool header_printed = false;
    int exit_code = 0;

    for (;;) {
        uint8_t hdr[5];
        if (xrecv(fd, hdr, 5) < 5) { exit_code = 1; break; }

        uint8_t mtype = hdr[0];
        uint32_t mlen = rd32(hdr + 1);
        size_t body_len = mlen - 4;

        /* Allocate body */
        uint8_t *body = NULL;
        if (body_len > 0) {
            body = malloc(body_len);
            if (!body) { exit_code = 1; break; }
            if (xrecv(fd, body, body_len) < (ssize_t)body_len) {
                free(body);
                exit_code = 1;
                break;
            }
        }

        switch (mtype) {
        case 'T': {
            /* RowDescription establishes column names used for subsequent DataRow output. */
            if (!body || body_len < 2) break;
            ncols = rd16(body);
            /* Print column headers */
            size_t off = 2;
            for (int i = 0; i < ncols; i++) {
                /* Find NUL-terminated name */
                const char *name = (const char *)(body + off);
                size_t nlen = strnlen(name, body_len - off);
                if (i > 0) putchar('\t');
                fwrite(name, 1, nlen, stdout);
                off += nlen + 1;   /* skip name + NUL */
                off += 18;         /* skip tableoid(4)+colno(2)+typeoid(4)+typlen(2)+typmod(4)+fmt(2) */
            }
            putchar('\n');
            header_printed = true;
            break;
        }
        case 'D': {
            /* DataRow values are rendered as a single tab-separated output line. */
            if (!body || body_len < 2) break;
            int nc = rd16(body);
            size_t off = 2;
            for (int i = 0; i < nc; i++) {
                if (off + 4 > body_len) break;
                int32_t clen = rd32s(body + off);
                off += 4;
                if (i > 0) putchar('\t');
                if (clen < 0) {
                    fputs("NULL", stdout);
                } else {
                    if (off + (size_t)clen > body_len) break;
                    fwrite(body + off, 1, (size_t)clen, stdout);
                    off += (size_t)clen;
                }
            }
            putchar('\n');
            break;
        }
        case 'C':
            /* CommandComplete — ignore */
            break;
        case 'E': {
            /* Extract only the human-readable message field for operator-friendly stderr output. */
            if (body && body_len > 0) {
                size_t off = 0;
                while (off < body_len && body[off] != '\0') {
                    uint8_t field = body[off++];
                    const char *val = (const char *)(body + off);
                    size_t vlen = strnlen(val, body_len - off);
                    if (field == 'M') {
                        fprintf(stderr, "ERROR: %.*s\n", (int)vlen, val);
                    }
                    off += vlen + 1;
                }
            }
            exit_code = 1;
            break;
        }
        case 'Z':
            /* ReadyForQuery — done */
            free(body);
            return exit_code;
        default:
            /* Unknown message type — skip */
            break;
        }

        free(body);
    }

    return exit_code;
}

/* ============================================================================
 * Main
 * ============================================================================ */

/**
 * @brief Print command-line usage text.
 *
 * @param prog Executable name used in examples.
 * @return Nothing.
 */
static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [options] <command ...>\n"
        "\n"
        "Options:\n"
        "  -h, --host <addr>   Admin host (default: 127.0.0.1)\n"
        "  -p, --port <port>   Admin port (default: 6433)\n"
        "  -s, --stdin         Read command from stdin\n"
        "  --help              Show this help\n"
        "\n"
        "Examples:\n"
        "  %s SHOW SERVERS\n"
        "  %s SHOW POOLS\n"
        "  %s SHOW CLIENTS\n"
        "  %s PAUSE\n"
        "  %s RESUME\n"
        "  %s DISABLE SERVER 0\n"
        "  %s ENABLE SERVER 0\n"
        "  echo 'SHOW STATS' | %s --stdin\n",
        prog, prog, prog, prog, prog, prog, prog, prog, prog);
}

/**
 * @brief Entry point for the `keel-cli` utility.
 *
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return Process exit status.
 *
 * Exit behavior:
 * - returns `0` on successful query execution
 * - returns non-zero on argument errors, socket failures, handshake failure,
 *   query send failure, or server-side error response
 */
int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    uint16_t port = 6433;
    bool from_stdin = false;

    static struct option longopts[] = {
        { "host",  required_argument, NULL, 'h' },
        { "port",  required_argument, NULL, 'p' },
        { "stdin", no_argument,       NULL, 's' },
        { "help",  no_argument,       NULL, 'H' },
        { NULL, 0, NULL, 0 }
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "h:p:s", longopts, NULL)) != -1) {
        switch (opt) {
        case 'h': host = optarg; break;
        case 'p': port = (uint16_t)atoi(optarg); break;
        case 's': from_stdin = true; break;
        case 'H':
        default:
            usage(argv[0]);
            return opt == 'H' ? 0 : 1;
        }
    }

    /* Normalize the command into one contiguous SQL/admin command string. */
    char cmd[4096];
    cmd[0] = '\0';

    if (from_stdin) {
        if (!fgets(cmd, sizeof(cmd), stdin)) {
            fprintf(stderr, "Error: no command on stdin\n");
            return 1;
        }
        /* Stdin mode accepts exactly one line and trims line endings. */
        size_t len = strlen(cmd);
        while (len > 0 && (cmd[len-1] == '\n' || cmd[len-1] == '\r'))
            cmd[--len] = '\0';
    } else if (optind < argc) {
        /* Positional mode rebuilds the command with spaces to match shell intent. */
        size_t off = 0;
        for (int i = optind; i < argc; i++) {
            if (i > optind) {
                if (off < sizeof(cmd) - 1) cmd[off++] = ' ';
            }
            size_t alen = strlen(argv[i]);
            if (off + alen >= sizeof(cmd) - 1) {
                fprintf(stderr, "Error: command too long\n");
                return 1;
            }
            memcpy(cmd + off, argv[i], alen);
            off += alen;
        }
        cmd[off] = '\0';
    } else {
        usage(argv[0]);
        return 1;
    }

    if (cmd[0] == '\0') {
        fprintf(stderr, "Error: empty command\n");
        return 1;
    }

    /* Establish the raw TCP connection to the admin listener. */
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        fprintf(stderr, "socket: %s\n", strerror(errno));
        return 1;
    }

    struct sockaddr_in sa = {
        .sin_family = AF_INET,
        .sin_port   = htons(port),
    };
    if (inet_pton(AF_INET, host, &sa.sin_addr) <= 0) {
        fprintf(stderr, "Invalid host: %s\n", host);
        close(fd);
        return 1;
    }

    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        fprintf(stderr, "connect %s:%u: %s\n", host, port, strerror(errno));
        close(fd);
        return 1;
    }

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    /* Perform the minimal PostgreSQL startup sequence expected by the admin endpoint. */
    if (pg_startup(fd) < 0) {
        fprintf(stderr, "Failed to send startup message\n");
        close(fd);
        return 1;
    }

    if (pg_wait_ready(fd) < 0) {
        fprintf(stderr, "Failed during PG startup handshake\n");
        close(fd);
        return 1;
    }

    /* The client sends exactly one SimpleQuery command per invocation. */
    if (pg_simple_query(fd, cmd) < 0) {
        fprintf(stderr, "Failed to send query\n");
        close(fd);
        return 1;
    }

    /* Stream the server result to stdout/stderr and translate that to the process exit code. */
    int rc = process_results(fd);

    /* Terminate politely even though the process is about to exit. */
    uint8_t term[5] = { 'X', 0, 0, 0, 4 };
    xsend(fd, term, 5);

    close(fd);
    return rc;
}
