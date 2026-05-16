/**
 * @file test_e2e.c
 * @brief End-to-end integration checks against the Docker PostgreSQL cluster harness.
 *
 * Unlike protocol unit tests, this suite validates full process-level behavior:
 * environment discovery, real socket connects, authentication round trips, and
 * repeated query execution over live backend nodes. The tests intentionally use
 * the shared `test_integration` harness so node discovery and startup policy
 * stay consistent with other integration binaries.
 *
 * These tests do not attempt to exhaustively cover failover policy internals.
 * Their role is to assert that the expected "happy-path" cluster interactions
 * remain usable after engine, protocol, or worker refactors.
 */

#include "test_utils.h"
#include "test_integration.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ============================================================================
 * Test Configuration
 * ============================================================================ */

static bool g_cluster_started_by_test = false;
static bool g_skip_tests = false;

/**
 * @brief Ensure a PostgreSQL integration cluster is running for this test file.
 * @return
 *
 * The setup path prefers reusing an already-running cluster to reduce suite
 * latency and avoid clobbering developer-managed environments. It only starts
 * the cluster when necessary and records ownership so teardown can be safe.
 */
static void setup_cluster(void) {
    /* Check if Docker is available */
    if (!integ_docker_available()) {
        printf("SKIP: Docker not available - install Docker to run e2e tests\n");
        g_skip_tests = true;
        return;
    }
    
    /* If cluster is already running, use it */
    if (integ_cluster_running()) {
        printf("Using existing PostgreSQL cluster\n");
        return;
    }
    
    /* Start the cluster */
    printf("Starting PostgreSQL cluster via Docker...\n");
    if (integ_cluster_start(true)) {
        g_cluster_started_by_test = true;
        printf("Cluster started successfully\n");
    } else {
        printf("SKIP: Failed to start test cluster\n");
        g_skip_tests = true;
    }
}

/**
 * @brief Stop the integration cluster only when this binary started it.
 * @return
 */
static void teardown_cluster(void) {
    if (g_cluster_started_by_test) {
        printf("\nStopping test cluster...\n");
        integ_cluster_stop(true);
    }
}

/* ============================================================================
 * Basic Connection Tests
 * ============================================================================ */

static void test_direct_connect(void) {
    TEST_BEGIN("direct PostgreSQL connection");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (conn) {
        TEST_ASSERT(integ_pg_is_alive(conn));
        integ_pg_close(conn);
    }
    
    TEST_END();
}

static void test_simple_query(void) {
    TEST_BEGIN("simple query execution");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    /* Test SELECT */
    int64_t result;
    bool ok = integ_pg_query_int(conn, "SELECT 42", &result);
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(result, 42);
    
    /* Test arithmetic */
    ok = integ_pg_query_int(conn, "SELECT 1 + 2 + 3", &result);
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(result, 6);
    
    integ_pg_close(conn);
    
    TEST_END();
}

static void test_string_query(void) {
    TEST_BEGIN("string query result");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    char result[256];
    bool ok = integ_pg_query_string(conn, "SELECT 'hello world'", result, sizeof(result));
    TEST_ASSERT(ok);
    TEST_ASSERT_STR_EQ(result, "hello world");
    
    /* Test version */
    ok = integ_pg_query_string(conn, "SELECT version()", result, sizeof(result));
    TEST_ASSERT(ok);
    TEST_ASSERT(strstr(result, "PostgreSQL") != NULL);
    
    integ_pg_close(conn);
    
    TEST_END();
}

static void test_server_version(void) {
    TEST_BEGIN("server version query");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    char result[32];
    bool ok = integ_pg_query_string(conn, "SHOW server_version", result, sizeof(result));
    TEST_ASSERT(ok);
    /* Should be something like "16.0" */
    TEST_ASSERT(strlen(result) > 0);
    
    integ_pg_close(conn);
    
    TEST_END();
}

/* ============================================================================
 * Multiple Connection Tests
 * ============================================================================ */

static void test_multiple_connections(void) {
    TEST_BEGIN("multiple concurrent connections");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    
    /* Open multiple connections */
    #define NUM_CONNS 5
    integ_pg_conn_t* conns[NUM_CONNS] = {0};
    
    for (int i = 0; i < NUM_CONNS; i++) {
        conns[i] = integ_pg_connect(
            "127.0.0.1", port,
            INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
        );
        TEST_ASSERT_NOT_NULL(conns[i]);
    }
    
    /* Execute queries on each */
    for (int i = 0; i < NUM_CONNS; i++) {
        if (!conns[i]) continue;
        
        int64_t result;
        char query[64];
        snprintf(query, sizeof(query), "SELECT %d * 10", i);
        
        bool ok = integ_pg_query_int(conns[i], query, &result);
        TEST_ASSERT(ok);
        TEST_ASSERT_EQ(result, i * 10);
    }
    
    /* Close all */
    for (int i = 0; i < NUM_CONNS; i++) {
        integ_pg_close(conns[i]);
    }
    
    TEST_END();
}

static void test_connection_reuse(void) {
    TEST_BEGIN("connection reuse for multiple queries");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    /* Execute many queries on same connection */
    for (int i = 0; i < 100; i++) {
        int64_t result;
        bool ok = integ_pg_query_int(conn, "SELECT 1", &result);
        TEST_ASSERT(ok);
        TEST_ASSERT_EQ(result, 1);
    }
    
    integ_pg_close(conn);
    
    TEST_END();
}

/* ============================================================================
 * DDL/DML Tests
 * ============================================================================ */

static void test_table_operations(void) {
    TEST_BEGIN("table create/insert/select/drop");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    /* Drop table if exists */
    integ_pg_exec(conn, "DROP TABLE IF EXISTS test_e2e");
    
    /* Create table */
    bool ok = integ_pg_exec(conn, "CREATE TABLE test_e2e (id SERIAL PRIMARY KEY, name TEXT)");
    TEST_ASSERT(ok);
    
    /* Insert rows */
    ok = integ_pg_exec(conn, "INSERT INTO test_e2e (name) VALUES ('Alice'), ('Bob'), ('Charlie')");
    TEST_ASSERT(ok);
    
    /* Count rows */
    int64_t count;
    ok = integ_pg_query_int(conn, "SELECT COUNT(*) FROM test_e2e", &count);
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(count, 3);
    
    /* Query specific row */
    char name[64];
    ok = integ_pg_query_string(conn, "SELECT name FROM test_e2e WHERE id = 2", name, sizeof(name));
    TEST_ASSERT(ok);
    TEST_ASSERT_STR_EQ(name, "Bob");
    
    /* Drop table */
    ok = integ_pg_exec(conn, "DROP TABLE test_e2e");
    TEST_ASSERT(ok);
    
    integ_pg_close(conn);
    
    TEST_END();
}

static void test_transaction(void) {
    TEST_BEGIN("transaction commit and rollback");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    /* Setup */
    integ_pg_exec(conn, "DROP TABLE IF EXISTS test_txn");
    integ_pg_exec(conn, "CREATE TABLE test_txn (id INT)");
    
    /* Test COMMIT */
    bool ok = integ_pg_exec(conn, "BEGIN");
    TEST_ASSERT(ok);
    
    ok = integ_pg_exec(conn, "INSERT INTO test_txn VALUES (1)");
    TEST_ASSERT(ok);
    
    ok = integ_pg_exec(conn, "COMMIT");
    TEST_ASSERT(ok);
    
    int64_t count;
    ok = integ_pg_query_int(conn, "SELECT COUNT(*) FROM test_txn", &count);
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(count, 1);
    
    /* Test ROLLBACK */
    ok = integ_pg_exec(conn, "BEGIN");
    TEST_ASSERT(ok);
    
    ok = integ_pg_exec(conn, "INSERT INTO test_txn VALUES (2)");
    TEST_ASSERT(ok);
    
    ok = integ_pg_exec(conn, "ROLLBACK");
    TEST_ASSERT(ok);
    
    ok = integ_pg_query_int(conn, "SELECT COUNT(*) FROM test_txn", &count);
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(count, 1);  /* Still 1, rollback worked */
    
    /* Cleanup */
    integ_pg_exec(conn, "DROP TABLE test_txn");
    integ_pg_close(conn);
    
    TEST_END();
}

/* ============================================================================
 * Error Handling Tests
 * ============================================================================ */

static void test_invalid_query(void) {
    TEST_BEGIN("invalid query handling");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    /* Syntax error - but connection should survive */
    bool ok = integ_pg_exec(conn, "SELECTT 1");
    TEST_ASSERT(!ok);
    
    /* Connection should still work */
    int64_t result;
    ok = integ_pg_query_int(conn, "SELECT 1", &result);
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(result, 1);
    
    integ_pg_close(conn);
    
    TEST_END();
}

static void test_nonexistent_table(void) {
    TEST_BEGIN("nonexistent table handling");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    /* Query non-existent table */
    bool ok = integ_pg_exec(conn, "SELECT * FROM nonexistent_table_xyz");
    TEST_ASSERT(!ok);
    
    /* Connection should still work */
    int64_t result;
    ok = integ_pg_query_int(conn, "SELECT 42", &result);
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(result, 42);
    
    integ_pg_close(conn);
    
    TEST_END();
}

/* ============================================================================
 * Replica Tests (when available)
 * ============================================================================ */

static void test_connect_to_replicas(void) {
    TEST_BEGIN("connect to replica nodes");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t ports[] = {INTEG_PG_PORT1, INTEG_PG_PORT2, INTEG_PG_PORT3};
    int connected = 0;
    
    for (int i = 0; i < 3; i++) {
        integ_pg_conn_t* conn = integ_pg_connect(
            "127.0.0.1", ports[i],
            INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
        );
        
        if (conn) {
            connected++;
            
            /* Check if it's primary or replica */
            char is_recovery[16];
            if (integ_pg_query_string(conn, "SELECT pg_is_in_recovery()::text", 
                                       is_recovery, sizeof(is_recovery))) {
                printf("  Node %d (port %d): %s\n", i + 1, ports[i],
                       strcmp(is_recovery, "t") == 0 ? "REPLICA" : "PRIMARY");
            }
            
            integ_pg_close(conn);
        }
    }
    
    TEST_ASSERT(connected >= 1);  /* At least primary should be up */
    
    TEST_END();
}

static void test_read_from_replica(void) {
    TEST_BEGIN("read query on replica");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    /* Find a replica */
    uint16_t replica_port = 0;
    uint16_t ports[] = {INTEG_PG_PORT1, INTEG_PG_PORT2, INTEG_PG_PORT3};
    
    for (int i = 0; i < 3; i++) {
        integ_pg_conn_t* conn = integ_pg_connect(
            "127.0.0.1", ports[i],
            INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
        );
        
        if (conn) {
            char is_recovery[16];
            if (integ_pg_query_string(conn, "SELECT pg_is_in_recovery()::text",
                                       is_recovery, sizeof(is_recovery))) {
                if (strcmp(is_recovery, "t") == 0) {
                    replica_port = ports[i];
                }
            }
            integ_pg_close(conn);
            
            if (replica_port) break;
        }
    }
    
    if (!replica_port) {
        printf("SKIP: No replica available\n");
        return;
    }
    
    /* Execute read on replica */
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", replica_port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    int64_t result;
    bool ok = integ_pg_query_int(conn, "SELECT 123", &result);
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(result, 123);
    
    integ_pg_close(conn);
    
    TEST_END();
}

/* ============================================================================
 * Large Data Tests
 * ============================================================================ */

static void test_large_result(void) {
    TEST_BEGIN("large result set");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    /* Generate many rows */
    int64_t count;
    bool ok = integ_pg_query_int(conn, 
        "SELECT COUNT(*) FROM generate_series(1, 1000)", &count);
    TEST_ASSERT(ok);
    TEST_ASSERT_EQ(count, 1000);
    
    integ_pg_close(conn);
    
    TEST_END();
}

static void test_long_query(void) {
    TEST_BEGIN("long running query");
    
    if (!integ_cluster_running()) {
        printf("SKIP\n");
        return;
    }
    
    uint16_t port = integ_get_primary_port();
    integ_pg_conn_t* conn = integ_pg_connect(
        "127.0.0.1", port,
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE
    );
    
    TEST_ASSERT_NOT_NULL(conn);
    if (!conn) return;
    
    /* Query that takes some time */
    bool ok = integ_pg_exec(conn, "SELECT pg_sleep(0.5)");
    TEST_ASSERT(ok);
    
    integ_pg_close(conn);
    
    TEST_END();
}

/* ============================================================================
 * Main Test Runner
 * ============================================================================ */

int main(void) {
    printf("=== End-to-End Integration Tests ===\n\n");
    
    setup_cluster();
    
    if (g_skip_tests) {
        printf("\nNo tests run - see above for reason\n");
        return 0;
    }
    
    printf("\n--- Basic Connection Tests ---\n");
    test_direct_connect();
    test_simple_query();
    test_string_query();
    test_server_version();
    
    printf("\n--- Multiple Connection Tests ---\n");
    test_multiple_connections();
    test_connection_reuse();
    
    printf("\n--- DDL/DML Tests ---\n");
    test_table_operations();
    test_transaction();
    
    printf("\n--- Error Handling Tests ---\n");
    test_invalid_query();
    test_nonexistent_table();
    
    printf("\n--- Replica Tests ---\n");
    test_connect_to_replicas();
    test_read_from_replica();
    
    printf("\n--- Large Data Tests ---\n");
    test_large_result();
    test_long_query();
    
    teardown_cluster();
    
    printf("\n");
    return test_summary();
}
