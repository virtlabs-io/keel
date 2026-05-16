/**
 * @file test_integration.h
 * @brief Shared harness helpers for Docker-backed integration tests.
 *
 * Unlike the small unit tests, the E2E and integration binaries exercise KEEL
 * against real PostgreSQL nodes and a real containerized cluster. This header
 * centralizes the operational details those tests need repeatedly: environment
 * discovery, cluster lifecycle, endpoint lookup, and a small direct PostgreSQL
 * client used as a probe/oracle.
 *
 * The helpers are intentionally pragmatic rather than general-purpose. They are
 * tuned to KEEL's own test topology and favor clear diagnostics over API
 * elegance.
 *
 * @author Charly Batista
 * @copyright Copyright (c) Virtlabs, https://virtlabs.io
 * @license GNU Affero General Public License v3.0
 */

#ifndef TEST_INTEGRATION_H
#define TEST_INTEGRATION_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Configuration
 * ============================================================================ */

/* Default port map for the containerized PostgreSQL cluster. */
#define INTEG_PG_PORT1      15432
#define INTEG_PG_PORT2      15433
#define INTEG_PG_PORT3      15434
#define INTEG_PATRONI_PORT1 8008
#define INTEG_PATRONI_PORT2 8009
#define INTEG_PATRONI_PORT3 8010

/* Test credentials */
#define INTEG_PG_USER       "postgres"
#define INTEG_PG_PASSWORD   "postgres"
#define INTEG_PG_DATABASE   "postgres"

/* Timeouts */
#define INTEG_CONNECT_TIMEOUT_MS 5000
#define INTEG_QUERY_TIMEOUT_MS   30000

/* Path to the Docker test assets, injected by CMake when needed. */
#ifndef INTEG_DOCKER_DIR
#define INTEG_DOCKER_DIR "."
#endif

/* ============================================================================
 * Test Environment Management
 * ============================================================================ */

/**
 * @brief Check whether Docker is installed and the daemon is reachable.
 *
 * @return true when Docker-backed integration helpers are usable.
 */
bool integ_docker_available(void);

/**
 * @brief Check if PostgreSQL test cluster is running
 * @return true if cluster is running and healthy
 */
bool integ_cluster_running(void);

/**
 * @brief Start the integration-test cluster via Docker Compose.
 *
 * @param wait_for_ready Whether to wait for a healthy primary before returning.
 * @return true on success.
 */
bool integ_cluster_start(bool wait_for_ready);

/**
 * @brief Stop the test cluster
 * @param remove_volumes Remove data volumes
 * @return true on success
 */
bool integ_cluster_stop(bool remove_volumes);

/**
 * @brief Get the primary node port
 * @return Port number or 0 if not available
 */
uint16_t integ_get_primary_port(void);

/**
 * @brief Get the primary node host
 * @return Hostname for the discovered primary endpoint
 */
const char* integ_get_primary_host(void);

/**
 * @brief Get the discovered host for a cluster node
 * @param node_index 1-based node index
 * @return Hostname or NULL if index is invalid
 */
const char* integ_get_node_host(int node_index);

/**
 * @brief Get the discovered port for a cluster node
 * @param node_index 1-based node index
 * @return Port or 0 if index is invalid
 */
uint16_t integ_get_node_port(int node_index);

/**
 * @brief Wait for cluster to be ready
 * @param timeout_secs Maximum time to wait
 * @return true if cluster is ready within timeout
 */
bool integ_wait_for_cluster(int timeout_secs);

/* ============================================================================
 * Simple PostgreSQL Client for Testing
 * ============================================================================ */

/* Forward declaration */
typedef struct integ_pg_conn integ_pg_conn_t;

/**
 * @brief Open a minimal direct PostgreSQL connection for harness operations.
 *
 * This client is not intended to mirror the full proxy protocol stack. It is a
 * small synchronous probe/oracle used by tests to verify backend cluster state
 * and to run simple control queries.
 *
 * @param host Hostname.
 * @param port Port number.
 * @param user Username.
 * @param password Password.
 * @param database Database name.
 * @return Open connection, or `NULL` on failure.
 */
integ_pg_conn_t* integ_pg_connect(const char* host, uint16_t port,
                                   const char* user, const char* password,
                                   const char* database);

/**
 * @brief Close connection
 */
void integ_pg_close(integ_pg_conn_t* conn);

/**
 * @brief Check if connection is alive
 */
bool integ_pg_is_alive(integ_pg_conn_t* conn);

/**
 * @brief Execute a simple query
 * @param conn Connection
 * @param query SQL query
 * @return true on success
 */
bool integ_pg_exec(integ_pg_conn_t* conn, const char* query);

/**
 * @brief Execute query and return single integer result
 * @param conn Connection
 * @param query SQL query
 * @param result Output: result value
 * @return true on success
 */
bool integ_pg_query_int(integ_pg_conn_t* conn, const char* query, int64_t* result);

/**
 * @brief Execute query and return single string result
 * @param conn Connection
 * @param query SQL query
 * @param result Output buffer
 * @param result_size Buffer size
 * @return true on success
 */
bool integ_pg_query_string(integ_pg_conn_t* conn, const char* query,
                            char* result, size_t result_size);

/**
 * @brief Get last error message
 */
const char* integ_pg_last_error(integ_pg_conn_t* conn);

/* ============================================================================
 * Test Assertions for Integration Tests
 * ============================================================================ */

#define INTEG_SKIP_IF_NO_DOCKER() \
    do { \
        if (!integ_docker_available()) { \
            printf("SKIP: Docker not available\n"); \
            return; \
        } \
    } while(0)

#define INTEG_SKIP_IF_NO_CLUSTER() \
    do { \
        if (!integ_cluster_running()) { \
            printf("SKIP: Test cluster not running\n"); \
            return; \
        } \
    } while(0)

#ifdef __cplusplus
}
#endif

#endif /* TEST_INTEGRATION_H */
