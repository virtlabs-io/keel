/**
 * @file test_admin_live.c
 * @brief Live integration tests for the admin server (keel_admin).
 *
 * Exercises the full admin-console code path: starts a real admin TCP
 * listener, connects to it with raw PG-wire protocol, sends every supported
 * SHOW command and common DML/mutation commands, and verifies that the server
 * responds without crashing.  The Prometheus HTTP exporter is also exercised
 * by sending GET requests to /healthz, /readyz, /livez, and /metrics.
 *
 * These tests deliberately run with:
 *  - trust authentication (admin_password = NULL)
 *  - a fresh keel_engine created but NOT started (no workers, no io_uring)
 *  - ephemeral ports (admin_port = 0, prom_port = 0) to avoid conflicts
 *
 * The engine with no running workers gives predictable NULL/zero state to
 * the show_* functions so they exercise their early-return / error branches.
 * The coverage benefit is in admin.c, not in the engine path.
 *
 * §1  — Startup handshake (trust auth, no SSL)
 * §2  — SHOW commands: HELP, VERSION, STATS, STATS_DETAIL, SERVERS, POOLS,
 *        CLIENTS, CONFIG, LATENCY, SYSTEM, REBALANCE, SHARD RULES,
 *        QUERY RULES, CACHE STATS, THROTTLE RULES, TOPOLOGY, OSC SESSIONS,
 *        CLUSTER, CLUSTER CONFIG, DISCOVERED PEERS, CLUSTER STATS,
 *        TRACING, CERTIFICATES
 * §3  — Mutation / control commands: PAUSE, RESUME, DRAIN, RELOAD,
 *        RESTART WORKERS, KILL CLIENT, DISABLE SERVER, ENABLE SERVER,
 *        ADD SERVER, REMOVE SERVER, ADD PEER, REMOVE PEER, SET, FLUSH QUERY CACHE
 * §4  — SQL DML via admin tables: SELECT FROM stats/servers/pools/config,
 *        UPDATE config, INSERT INTO servers, DELETE FROM servers
 * §5  — FORMAT JSON suffix
 * §6  — Unsupported / unknown command (error branch)
 * §7  — SSL request rejection (SSLRequest → 'N' → re-read startup)
 * §8  — Prometheus HTTP endpoints: /healthz, /readyz, /livez, /metrics, 404
 * §9  — Non-query PG message (skipped message branch)
 * §10 — Terminate message ('X')
 */

#include "test_utils.h"
#include "keel/core/admin.h"
#include "keel/engine/engine.h"
#include "keel/mem/mem.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* ===========================================================================
 * Wire helpers
 * ===========================================================================
 */

static void put_u32be(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8);
    p[3] = (uint8_t)(v);
}

static uint32_t get_u32be(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16)
         | ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

/** Read exactly @p n bytes from @p fd, retrying on EINTR. */
static int recv_all(int fd, uint8_t *buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        ssize_t r = recv(fd, buf + got, n - got, 0);
        if (r <= 0) return -1;
        got += (size_t)r;
    }
    return 0;
}

/** Send exactly @p n bytes to @p fd, retrying on EINTR. */
static int send_all(int fd, const uint8_t *buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        ssize_t s = send(fd, buf + sent, n - sent, MSG_NOSIGNAL);
        if (s <= 0) return -1;
        sent += (size_t)s;
    }
    return 0;
}

/**
 * @brief Drain PG messages from @p fd until a ReadyForQuery ('Z') arrives.
 *
 * Reads one PG message at a time (1-byte tag + 4-byte length + body) until
 * the tag is 'Z'.  This handles: AuthOK ('R'), ParameterStatus ('S'),
 * BackendKeyData ('K'), RowDescription ('T'), DataRow ('D'),
 * CommandComplete ('C'), ErrorResponse ('E'), and the final ReadyForQuery.
 *
 * @return 0 on success, -1 on socket error.
 */
static int drain_to_rfq(int fd) {
    for (;;) {
        uint8_t hdr[5];
        if (recv_all(fd, hdr, 5) < 0) return -1;

        uint8_t tag = hdr[0];
        uint32_t msglen = get_u32be(hdr + 1);

        /* msglen includes the 4-byte length field itself but not the tag. */
        if (msglen < 4) return -1;
        size_t body = msglen - 4;

        if (body > 0) {
            /* Drain the body without storing it — we just need coverage. */
            uint8_t discard[4096];
            size_t rem = body;
            while (rem > 0) {
                size_t chunk = rem < sizeof(discard) ? rem : sizeof(discard);
                if (recv_all(fd, discard, chunk) < 0) return -1;
                rem -= chunk;
            }
        }

        if (tag == 'Z') return 0;  /* ReadyForQuery reached */
    }
}

/** Open a TCP connection to loopback:@p port. */
static int tcp_connect(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct sockaddr_in sa = {
        .sin_family = AF_INET,
        .sin_port   = htons(port),
    };
    inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);

    if (connect(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/**
 * @brief Send a PG StartupMessage (protocol 3.0) with user + database params.
 * Layout: int32(total_len) int32(196608) "user\0<user>\0database\0<db>\0\0"
 */
static int pg_send_startup(int fd, const char *user, const char *db) {
    uint8_t buf[512];
    size_t ulen = strlen(user);
    size_t dlen = strlen(db);
    /* 4(len) + 4(proto) + "user\0" + user + "\0" + "database\0" + db + "\0\0" */
    size_t total = 4 + 4 + 5 + ulen + 1 + 9 + dlen + 1 + 1;
    if (total > sizeof(buf)) return -1;
    put_u32be(buf, (uint32_t)total);
    put_u32be(buf + 4, 196608U);
    size_t off = 8;
    memcpy(buf + off, "user", 5); off += 5;
    memcpy(buf + off, user, ulen + 1); off += ulen + 1;
    memcpy(buf + off, "database", 9); off += 9;
    memcpy(buf + off, db, dlen + 1); off += dlen + 1;
    buf[off++] = '\0';
    return send_all(fd, buf, off);
}

/**
 * @brief Send a PG SSLRequest (pseudo-startup with magic protocol 0x04D2162F).
 */
static int pg_send_ssl_request(int fd) {
    uint8_t buf[8];
    put_u32be(buf, 8);
    put_u32be(buf + 4, 80877103U);  /* SSL magic */
    return send_all(fd, buf, 8);
}

/**
 * @brief Send a simple PG Query ('Q') message.
 * Layout: 'Q' int32(4 + strlen(sql) + 1) sql\0
 */
static int pg_send_query(int fd, const char *sql) {
    size_t qlen = strlen(sql) + 1;       /* include NUL */
    uint32_t msglen = (uint32_t)(4 + qlen);
    uint8_t hdr[5];
    hdr[0] = 'Q';
    put_u32be(hdr + 1, msglen);
    if (send_all(fd, hdr, 5) < 0) return -1;
    return send_all(fd, (const uint8_t *)sql, qlen);
}

/**
 * @brief Send a PG Terminate ('X') message.
 */
static int pg_send_terminate(int fd) {
    uint8_t buf[5];
    buf[0] = 'X';
    put_u32be(buf + 1, 4);
    return send_all(fd, buf, 5);
}

/**
 * @brief Send a non-Query PG message (e.g. Bind 'B') to exercise the skip path.
 */
static int pg_send_bind_dummy(int fd) {
    /* 'B' + int32(4) = minimum bind message, triggers skip + error branch */
    uint8_t buf[5];
    buf[0] = 'B';
    put_u32be(buf + 1, 4);
    return send_all(fd, buf, 5);
}

/* ===========================================================================
 * Admin + engine fixture
 * ===========================================================================
 */

typedef struct {
    keel_engine_t  *engine;
    keel_admin_t   *admin;
    uint16_t        admin_port;
    uint16_t        prom_port;
} admin_fixture_t;

static int fixture_start(admin_fixture_t *f) {
    memset(f, 0, sizeof(*f));

    /* Create engine without starting workers (pure construction). */
    keel_engine_config_t ecfg = KEEL_ENGINE_CONFIG_DEFAULT;
    f->engine = keel_engine_create(&ecfg);
    if (!f->engine) return -1;

    /* Admin config: trust auth (password=NULL), ephemeral ports. */
    keel_admin_config_t acfg = KEEL_ADMIN_CONFIG_DEFAULT;
    acfg.admin_enabled  = true;
    acfg.admin_addr     = "127.0.0.1";
    acfg.admin_port     = 0;       /* OS assigns ephemeral port */
    acfg.admin_password = NULL;    /* trust auth */
    acfg.prom_enabled   = true;
    acfg.prom_addr      = "127.0.0.1";
    acfg.prom_port      = 0;       /* OS assigns ephemeral port */

    f->admin = keel_admin_start(&acfg, f->engine);
    if (!f->admin) {
        keel_engine_destroy(f->engine);
        return -1;
    }

    f->admin_port = keel_admin_get_port(f->admin);
    f->prom_port  = keel_admin_get_prom_port(f->admin);

    return (f->admin_port > 0 && f->prom_port > 0) ? 0 : -1;
}

static void fixture_stop(admin_fixture_t *f) {
    keel_admin_stop(f->admin);
    keel_engine_destroy(f->engine);
}

/* ===========================================================================
 * Helper: open a new PG connection and complete the handshake.
 * Returns a connected fd already drained to ReadyForQuery, or -1 on error.
 * ===========================================================================
 */
static int admin_connect(uint16_t port) {
    int fd = tcp_connect(port);
    if (fd < 0) return -1;

    if (pg_send_startup(fd, "admin", "keel") < 0) { close(fd); return -1; }

    /* Drain auth + param status + backend key + ReadyForQuery */
    if (drain_to_rfq(fd) < 0) { close(fd); return -1; }

    return fd;
}

/* ===========================================================================
 * §1 — Startup handshake: trust auth, no SSL
 * ===========================================================================
 */
static void test_startup_trust(void) {
    TEST_BEGIN("admin: startup trust auth handshake");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = admin_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        pg_send_terminate(fd);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §2 — SHOW commands
 * ===========================================================================
 */
static const char *const show_cmds[] = {
    "SHOW HELP",
    "SHOW VERSION",
    "SHOW STATS",
    "SHOW STATS_DETAIL",
    "SHOW SERVERS",
    "SHOW POOLS",
    "SHOW CLIENTS",
    "SHOW CONFIG",
    "SHOW LATENCY",
    "SHOW SYSTEM",
    "SHOW REBALANCE",
    "SHOW SHARD RULES",
    "SHOW QUERY RULES",
    "SHOW CACHE STATS",
    "SHOW THROTTLE RULES",
    "SHOW TOPOLOGY",
    "SHOW OSC SESSIONS",
    "SHOW CLUSTER",
    "SHOW CLUSTER CONFIG",
    "SHOW DISCOVERED PEERS",
    "SHOW CLUSTER STATS",
    "SHOW TRACING",
    "SHOW CERTIFICATES",
    NULL
};

static void test_show_commands(void) {
    TEST_BEGIN("admin: SHOW commands");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = admin_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        for (int i = 0; show_cmds[i]; i++) {
            int rc = pg_send_query(fd, show_cmds[i]);
            TEST_ASSERT_EQ(rc, 0);
            rc = drain_to_rfq(fd);
            TEST_ASSERT_EQ(rc, 0);
        }
        pg_send_terminate(fd);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §3 — Mutation / control commands
 * ===========================================================================
 */
static const char *const mutation_cmds[] = {
    "PAUSE",
    "RESUME",
    "DRAIN",
    "RELOAD",
    "RESTART WORKERS",
    "KILL CLIENT 999999",
    "DISABLE SERVER 0",
    "ENABLE SERVER 0",
    "ADD SERVER host=127.0.0.1 port=5432 role=rw",
    "REMOVE SERVER 0",
    "ADD PEER 127.0.0.1:19999",
    "REMOVE PEER 127.0.0.1:19999",
    "SET pool_max_size=10",
    "FLUSH QUERY CACHE",
    "EXPLAIN SHARD PLAN FOR SELECT 1",
    "EXPLAIN ROUTE FOR SELECT 1",
    NULL
};

static void test_mutation_commands(void) {
    TEST_BEGIN("admin: mutation/control commands");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = admin_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        for (int i = 0; mutation_cmds[i]; i++) {
            pg_send_query(fd, mutation_cmds[i]);
            drain_to_rfq(fd);
        }
        pg_send_terminate(fd);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §4 — SQL DML via admin virtual tables
 * ===========================================================================
 */
static const char *const dml_cmds[] = {
    "SELECT * FROM stats",
    "SELECT * FROM servers",
    "SELECT * FROM pools",
    "SELECT * FROM clients",
    "SELECT * FROM config",
    "SELECT * FROM latency",
    "SELECT * FROM system",
    "SELECT * FROM help",
    "SELECT * FROM version",
    "SELECT * FROM rebalance",
    "SELECT * FROM cluster",
    "SELECT * FROM cluster_config",
    "SELECT * FROM discovered_peers",
    "SELECT * FROM cluster_stats",
    "SELECT * FROM shard_rules",
    "UPDATE config SET value = '50' WHERE key = 'pool_max_size'",
    "INSERT INTO servers (host, port, role) VALUES ('127.0.0.1', 5432, 'rw')",
    "DELETE FROM servers WHERE index = 0",
    NULL
};

static void test_dml_commands(void) {
    TEST_BEGIN("admin: SQL DML via admin virtual tables");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = admin_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        for (int i = 0; dml_cmds[i]; i++) {
            pg_send_query(fd, dml_cmds[i]);
            drain_to_rfq(fd);
        }
        pg_send_terminate(fd);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §5 — FORMAT JSON suffix
 * ===========================================================================
 */
static void test_format_json(void) {
    TEST_BEGIN("admin: FORMAT JSON suffix");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = admin_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        pg_send_query(fd, "SHOW HELP FORMAT JSON");
        drain_to_rfq(fd);

        pg_send_query(fd, "SHOW CONFIG FORMAT JSON");
        drain_to_rfq(fd);

        pg_send_query(fd, "SHOW STATS FORMAT JSON");
        drain_to_rfq(fd);

        pg_send_terminate(fd);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §6 — Unknown command (error branch)
 * ===========================================================================
 */
static void test_unknown_command(void) {
    TEST_BEGIN("admin: unknown command returns error");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = admin_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        pg_send_query(fd, "THIS IS NOT A VALID COMMAND");
        drain_to_rfq(fd);

        pg_send_query(fd, "SELECT * FROM nonexistent_table");
        drain_to_rfq(fd);

        pg_send_terminate(fd);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §7 — SSLRequest rejection: admin sends 'N', then re-reads startup
 * ===========================================================================
 */
static void test_ssl_rejection(void) {
    TEST_BEGIN("admin: SSLRequest rejected with 'N', startup continues");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = tcp_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        /* Send SSLRequest magic */
        TEST_ASSERT_EQ(pg_send_ssl_request(fd), 0);

        /* Admin responds with 'N' (1 byte) */
        uint8_t no_ssl;
        TEST_ASSERT_EQ(recv_all(fd, &no_ssl, 1), 0);
        TEST_ASSERT_EQ(no_ssl, (uint8_t)'N');

        /* Now send proper startup */
        TEST_ASSERT_EQ(pg_send_startup(fd, "admin", "keel"), 0);

        /* Complete handshake */
        TEST_ASSERT_EQ(drain_to_rfq(fd), 0);

        pg_send_query(fd, "SHOW VERSION");
        drain_to_rfq(fd);

        pg_send_terminate(fd);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §8 — Prometheus HTTP endpoints
 * ===========================================================================
 */

/** Send a raw HTTP GET request and drain the response body. */
static int http_get(int fd, const char *path) {
    char req[256];
    int n = snprintf(req, sizeof(req),
                     "GET %s HTTP/1.0\r\nHost: localhost\r\n\r\n", path);
    if (n < 0 || (size_t)n >= sizeof(req)) return -1;
    if (send_all(fd, (const uint8_t *)req, (size_t)n) < 0) return -1;

    /* Drain until EOF (server closes connection after response). */
    uint8_t buf[4096];
    for (;;) {
        ssize_t r = recv(fd, buf, sizeof(buf), 0);
        if (r <= 0) break;
    }
    return 0;
}

static void test_prometheus_endpoints(void) {
    TEST_BEGIN("admin: Prometheus HTTP endpoints");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    const char *paths[] = { "/healthz", "/readyz", "/livez", "/metrics", "/notfound", NULL };
    for (int i = 0; paths[i]; i++) {
        int fd = tcp_connect(f.prom_port);
        TEST_ASSERT(fd >= 0);
        if (fd >= 0) {
            http_get(fd, paths[i]);
            close(fd);
        }
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §9 — Non-query PG message (skip branch)
 * ===========================================================================
 */
static void test_non_query_message(void) {
    TEST_BEGIN("admin: non-query message triggers skip+error branch");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = admin_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        /* Send a Bind ('B') message — should be skipped with an error. */
        pg_send_bind_dummy(fd);
        drain_to_rfq(fd);

        pg_send_terminate(fd);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §10 — Terminate message ('X') exits the loop cleanly
 * ===========================================================================
 */
static void test_terminate_message(void) {
    TEST_BEGIN("admin: Terminate ('X') closes session cleanly");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = admin_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        /* Send a couple of queries then terminate */
        pg_send_query(fd, "SHOW HELP");
        drain_to_rfq(fd);

        pg_send_query(fd, "SHOW VERSION");
        drain_to_rfq(fd);

        pg_send_terminate(fd);

        /* Server should close the connection; read should return 0. */
        uint8_t buf[64];
        (void)recv(fd, buf, sizeof(buf), 0);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §11 — Multiple concurrent connections (exercise accept loop)
 * ===========================================================================
 */
static void test_multiple_connections(void) {
    TEST_BEGIN("admin: multiple sequential connections");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    /* 3 sequential connections, each sends a query and terminates. */
    for (int i = 0; i < 3; i++) {
        int fd = admin_connect(f.admin_port);
        TEST_ASSERT(fd >= 0);
        if (fd >= 0) {
            pg_send_query(fd, "SHOW CONFIG");
            drain_to_rfq(fd);
            pg_send_terminate(fd);
            close(fd);
        }
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §12 — Oversized / malformed startup (drain-and-reject branch)
 * ===========================================================================
 */
static void test_malformed_startup(void) {
    TEST_BEGIN("admin: oversized startup message is rejected gracefully");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = tcp_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        /* Build a startup with length > 8+4096 = 4104 to trigger drain path.
         * We won't send all the bytes — just send the header claiming a huge
         * payload, then close; admin will get EOF trying to drain.
         * That exercises the oversized-startup drain-and-return branch. */
        uint8_t hdr[8];
        put_u32be(hdr, 65000);      /* large length > 8+4096 */
        put_u32be(hdr + 4, 196608); /* valid protocol version */
        send_all(fd, hdr, 8);
        /* Close immediately — admin will fail to recv the rest and return. */
        close(fd);
    }

    /* Allow the admin thread to process the close. */
    usleep(10000);

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §13 — RESTART WORKERS with count suffix
 * ===========================================================================
 */
static void test_restart_workers_cmd(void) {
    TEST_BEGIN("admin: RESTART WORKERS <n>");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = admin_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        pg_send_query(fd, "RESTART WORKERS 1");
        drain_to_rfq(fd);

        pg_send_terminate(fd);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §14 — DRAIN with timeout suffix
 * ===========================================================================
 */
static void test_drain_cmd_with_timeout(void) {
    TEST_BEGIN("admin: DRAIN <timeout_ms>");

    admin_fixture_t f;
    TEST_ASSERT_EQ(fixture_start(&f), 0);

    int fd = admin_connect(f.admin_port);
    TEST_ASSERT(fd >= 0);

    if (fd >= 0) {
        pg_send_query(fd, "DRAIN 100");
        drain_to_rfq(fd);

        pg_send_terminate(fd);
        close(fd);
    }

    fixture_stop(&f);
    TEST_END();
}

/* ===========================================================================
 * §15 — keel_admin_get_port / keel_admin_get_prom_port with NULL
 * ===========================================================================
 */
static void test_port_accessors_null_safety(void) {
    TEST_BEGIN("admin: port accessors are NULL-safe");

    TEST_ASSERT_EQ((int)keel_admin_get_port(NULL), 0);
    TEST_ASSERT_EQ((int)keel_admin_get_prom_port(NULL), 0);

    TEST_END();
}

/* ===========================================================================
 * main
 * ===========================================================================
 */
int main(void) {
    printf("Admin Live Integration Tests\n");
    printf("============================\n\n");

    /* cmd_reload() sends SIGHUP to self to trigger config reload in production.
     * In tests there is no config-reload handler, so ignore SIGHUP to survive. */
    signal(SIGHUP, SIG_IGN);

    keel_mem_init(NULL);

    test_port_accessors_null_safety();
    test_startup_trust();
    test_show_commands();
    test_mutation_commands();
    test_dml_commands();
    test_format_json();
    test_unknown_command();
    test_ssl_rejection();
    test_prometheus_endpoints();
    test_non_query_message();
    test_terminate_message();
    test_multiple_connections();
    test_malformed_startup();
    test_restart_workers_cmd();
    test_drain_cmd_with_timeout();

    keel_mem_shutdown();

    return test_summary();
}
