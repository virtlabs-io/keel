/**
 * @file test_2pc_fault_inject.c
 * @brief Fault-injection integration tests for the two-phase commit coordinator.
 *
 * These tests drive @ref keel_2pc_coord_t against real PostgreSQL connections,
 * exercising the coordinator alongside actual PREPARE TRANSACTION / COMMIT
 * PREPARED / ROLLBACK PREPARED statements.  Each test uses a single PostgreSQL
 * node, opening multiple connections that simulate independent shard backends.
 *
 * Two classes of tests are covered:
 *
 * Happy-path (E2E):
 *   BEGIN + INSERT on 2 shards → PREPARE on both → coordinator all_prepared()
 *   → COMMIT PREPARED on both → coordinator commit_all() → data persisted.
 *
 * Fault-injection (N-1 partial prepare):
 *   BEGIN + INSERT on 3 shards → PREPARE on shards 0 and 1 → simulate a
 *   prepare failure on shard 2 (coordinator records prepare_failed) → call
 *   rollback_all() → issue ROLLBACK PREPARED for shards 0/1 and plain
 *   ROLLBACK for shard 2 → verify all data is absent.
 *
 * Skips:
 *   - When no PostgreSQL cluster is reachable (set KEEL_TEST_PG_HOST1/PORT1).
 *   - When max_prepared_transactions = 0 (2PC is disabled in postgresql.conf).
 *     Add `max_prepared_transactions = 10` to postgresql-overrides.conf and
 *     restart the cluster to enable these tests.
 *
 * Test table: `keel_2pc_fi_test(shard INT, marker TEXT)`
 *   Each shard inserts a row tagged with its shard index.  Tests verify the
 *   row count to confirm commit or rollback semantics.
 */

#include "test_utils.h"
#include "test_integration.h"
#include "keel/core/scatter_2pc.h"
#include "keel/core/router.h"
#include "keel/mem/mem.h"

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

/* ============================================================================
 * Globals
 * ============================================================================ */

int g_tests_run    = 0;
int g_tests_passed = 0;
int g_tests_failed = 0;

int test_summary(void) { return g_tests_failed ? 1 : 0; }

static bool g_skip_tests     = false;
static bool g_skip_2pc_tests = false; /* skip if max_prepared_transactions = 0 */

/* ============================================================================
 * Helpers
 * ============================================================================ */

/** Build a scatter plan with exactly the given shard-index bits set. */
static keel_scatter_plan_t make_plan(uint64_t mask)
{
    keel_scatter_plan_t p;
    memset(&p, 0, sizeof p);
    p.participating_shards_mask = mask;
    size_t cnt = 0;
    for (uint64_t m = mask; m; m &= m - 1) cnt++;
    p.count = cnt;
    return p;
}

/**
 * @brief Open a test connection to node 1.
 *
 * All fault-inject tests connect to the same PostgreSQL node (multiple
 * connections simulate different shards).
 */
static integ_pg_conn_t* open_conn(void)
{
    return integ_pg_connect(
        integ_get_node_host(1), integ_get_node_port(1),
        INTEG_PG_USER, INTEG_PG_PASSWORD, INTEG_PG_DATABASE);
}

/**
 * @brief Issue PREPARE TRANSACTION '<gid>' over @p conn.
 */
static bool pg_prepare_txn(integ_pg_conn_t* conn, const char* gid)
{
    char sql[128];
    snprintf(sql, sizeof sql, "PREPARE TRANSACTION '%s'", gid);
    return integ_pg_exec(conn, sql);
}

/**
 * @brief Issue COMMIT PREPARED '<gid>' over @p conn.
 */
static bool pg_commit_prepared(integ_pg_conn_t* conn, const char* gid)
{
    char sql[128];
    snprintf(sql, sizeof sql, "COMMIT PREPARED '%s'", gid);
    return integ_pg_exec(conn, sql);
}

/**
 * @brief Issue ROLLBACK PREPARED '<gid>' over @p conn.
 */
static bool pg_rollback_prepared(integ_pg_conn_t* conn, const char* gid)
{
    char sql[128];
    snprintf(sql, sizeof sql, "ROLLBACK PREPARED '%s'", gid);
    return integ_pg_exec(conn, sql);
}

/**
 * @brief Clean up any orphaned prepared transactions that match our prefix.
 *
 * Called at test teardown to avoid "prepared transaction already exists" errors
 * if a previous test run crashed mid-way.
 */
static void cleanup_prepared_txns(integ_pg_conn_t* conn)
{
    /* Roll back any leftover prepared transactions from previous failed runs */
    integ_pg_exec(conn,
        "DO $$ DECLARE r RECORD; BEGIN "
        "  FOR r IN SELECT gid FROM pg_prepared_xacts "
        "           WHERE gid LIKE 'keel\\_%' LOOP "
        "    EXECUTE format('ROLLBACK PREPARED %L', r.gid); "
        "  END LOOP; "
        "END $$");
}

/* ============================================================================
 * Schema setup / teardown
 * ============================================================================ */

static bool setup_fi_schema(void)
{
    integ_pg_conn_t* c = open_conn();
    if (!c) return false;

    cleanup_prepared_txns(c);
    integ_pg_exec(c, "DROP TABLE IF EXISTS keel_2pc_fi_test");
    bool ok = integ_pg_exec(c,
        "CREATE TABLE keel_2pc_fi_test (shard INT, marker TEXT)");
    integ_pg_close(c);
    return ok;
}

static void teardown_fi_schema(void)
{
    integ_pg_conn_t* c = open_conn();
    if (!c) return;
    cleanup_prepared_txns(c);
    integ_pg_exec(c, "DROP TABLE IF EXISTS keel_2pc_fi_test");
    integ_pg_close(c);
}

/* ============================================================================
 * Probe: check max_prepared_transactions
 * ============================================================================ */

static void test_2pc_fi_probe_config(void)
{
    TEST_BEGIN("2pc fault-inject: probe max_prepared_transactions setting");

    if (g_skip_tests) {
        printf("  SKIP (no cluster)\n");
        TEST_END();
        return;
    }

    integ_pg_conn_t* c = open_conn();
    TEST_ASSERT(c != NULL);

    int64_t max_prepared = 0;
    bool ok = integ_pg_query_int(c,
        "SELECT current_setting('max_prepared_transactions')::int", &max_prepared);
    integ_pg_close(c);

    TEST_ASSERT(ok);

    if (max_prepared == 0) {
        printf("  NOTE: max_prepared_transactions=0 — "
               "2PC tests will be skipped.\n"
               "  Add 'max_prepared_transactions = 10' to "
               "postgresql-overrides.conf and restart the cluster.\n");
        g_skip_2pc_tests = true;
    }

    /* The probe itself always passes; the skip flag controls remaining tests. */
    TEST_ASSERT(ok);
    TEST_END();
}

/* ============================================================================
 * Happy path: 2 shards, PREPARE → COMMIT PREPARED, data persisted
 * ============================================================================ */

static void test_2pc_fi_happy_path_commit(void)
{
    TEST_BEGIN("2pc fault-inject: happy path — prepare + commit 2 shards");

    if (g_skip_tests || g_skip_2pc_tests) {
        printf("  SKIP\n");
        TEST_END();
        return;
    }

    integ_pg_conn_t* c0 = open_conn();
    integ_pg_conn_t* c1 = open_conn();
    TEST_ASSERT(c0 != NULL && c1 != NULL);

    /* Truncate table for clean state */
    integ_pg_exec(c0, "TRUNCATE keel_2pc_fi_test");

    /* Initialise 2PC coordinator for shards 0 and 1 */
    keel_2pc_coord_t coord;
    keel_2pc_coord_init(&coord, 1001ULL, 1ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&coord, &plan), KEEL_OK);
    TEST_ASSERT_EQ(coord.state, KEEL_2PC_ACTIVE);
    TEST_ASSERT_EQ(coord.count, (size_t)2);

    /* Phase 0: BEGIN + INSERT on each simulated shard */
    TEST_ASSERT(integ_pg_exec(c0, "BEGIN"));
    TEST_ASSERT(integ_pg_exec(c0,
        "INSERT INTO keel_2pc_fi_test VALUES (0, 'happy-shard0')"));

    TEST_ASSERT(integ_pg_exec(c1, "BEGIN"));
    TEST_ASSERT(integ_pg_exec(c1,
        "INSERT INTO keel_2pc_fi_test VALUES (1, 'happy-shard1')"));

    /* Phase 1: PREPARE on each shard */
    const char* gid0 = keel_2pc_coord_gid(&coord, 0);
    const char* gid1 = keel_2pc_coord_gid(&coord, 1);
    TEST_ASSERT(gid0 != NULL && gid1 != NULL);

    TEST_ASSERT(pg_prepare_txn(c0, gid0));
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&coord, 0), KEEL_OK);

    TEST_ASSERT(pg_prepare_txn(c1, gid1));
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&coord, 1), KEEL_OK);

    /* Coordinator must confirm all prepared */
    TEST_ASSERT(keel_2pc_coord_all_prepared(&coord));

    /* Phase 2: COMMIT PREPARED on each shard, then record in coordinator */
    TEST_ASSERT(pg_commit_prepared(c0, gid0));
    TEST_ASSERT(pg_commit_prepared(c1, gid1));
    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(&coord), KEEL_OK);

    /* Verify coordinator state */
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 0), KEEL_2PC_COMMITTED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 1), KEEL_2PC_COMMITTED);

    /* Verify data persisted on PostgreSQL */
    int64_t total = 0;
    TEST_ASSERT(integ_pg_query_int(c0,
        "SELECT COUNT(*) FROM keel_2pc_fi_test", &total));
    TEST_ASSERT_EQ(total, (int64_t)2);

    integ_pg_close(c0);
    integ_pg_close(c1);
    TEST_END();
}

/* ============================================================================
 * Partial prepare failure: 3 shards, prepare 2, fail 1 → rollback_all
 *
 * The "fault" is simulated by:
 *   - issuing PREPARE TRANSACTION on shards 0 and 1 (success),
 *   - deliberately NOT issuing PREPARE on shard 2 (simulating a network
 *     failure or backend error during phase-1 on the last shard),
 *   - calling keel_2pc_coord_prepare_failed() to record the failure,
 *   - calling keel_2pc_coord_rollback_all(),
 *   - then issuing ROLLBACK PREPARED for shards 0/1 and plain ROLLBACK
 *     for shard 2 (which still holds an active transaction).
 *
 * Expected outcome:
 *   - Coordinator: shard 0 ROLLED_BACK, shard 1 ROLLED_BACK, shard 2 ABORTED
 *   - PostgreSQL:  zero rows in keel_2pc_fi_test
 * ============================================================================ */

static void test_2pc_fi_n_minus_1_rollback(void)
{
    TEST_BEGIN("2pc fault-inject: N-1 partial prepare failure → rollback_all");

    if (g_skip_tests || g_skip_2pc_tests) {
        printf("  SKIP\n");
        TEST_END();
        return;
    }

    integ_pg_conn_t* c0 = open_conn();
    integ_pg_conn_t* c1 = open_conn();
    integ_pg_conn_t* c2 = open_conn();
    TEST_ASSERT(c0 != NULL && c1 != NULL && c2 != NULL);

    integ_pg_exec(c0, "TRUNCATE keel_2pc_fi_test");

    /* Coordinator: 3 shards */
    keel_2pc_coord_t coord;
    keel_2pc_coord_init(&coord, 2002ULL, 2ULL);
    keel_scatter_plan_t plan =
        make_plan((1ULL << 0) | (1ULL << 1) | (1ULL << 2));
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&coord, &plan), KEEL_OK);

    /* BEGIN + INSERT on all 3 shards */
    TEST_ASSERT(integ_pg_exec(c0, "BEGIN"));
    TEST_ASSERT(integ_pg_exec(c0,
        "INSERT INTO keel_2pc_fi_test VALUES (0, 'partial-s0')"));

    TEST_ASSERT(integ_pg_exec(c1, "BEGIN"));
    TEST_ASSERT(integ_pg_exec(c1,
        "INSERT INTO keel_2pc_fi_test VALUES (1, 'partial-s1')"));

    TEST_ASSERT(integ_pg_exec(c2, "BEGIN"));
    TEST_ASSERT(integ_pg_exec(c2,
        "INSERT INTO keel_2pc_fi_test VALUES (2, 'partial-s2')"));

    /* Phase 1: PREPARE on shards 0 and 1 only */
    const char* gid0 = keel_2pc_coord_gid(&coord, 0);
    const char* gid1 = keel_2pc_coord_gid(&coord, 1);
    TEST_ASSERT(gid0 != NULL && gid1 != NULL);

    TEST_ASSERT(pg_prepare_txn(c0, gid0));
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&coord, 0), KEEL_OK);

    TEST_ASSERT(pg_prepare_txn(c1, gid1));
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&coord, 1), KEEL_OK);

    /* Fault: shard 2 prepare NOT issued (simulating network/backend failure) */
    TEST_ASSERT_EQ(keel_2pc_coord_prepare_failed(&coord, 2), KEEL_OK);

    /* all_prepared must be false — cannot commit */
    TEST_ASSERT(!keel_2pc_coord_all_prepared(&coord));

    /* Coordinator: rollback_all transitions PREPARED → ROLLED_BACK,
     * leaves ABORTED unchanged */
    TEST_ASSERT_EQ(keel_2pc_coord_rollback_all(&coord), KEEL_OK);

    /* Verify coordinator per-shard states:
     * - shards 0, 1 were PREPARED → now ROLLED_BACK
     * - shard 2 was ABORTED → still ABORTED (rollback_all leaves it alone) */
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 0), KEEL_2PC_ROLLED_BACK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 1), KEEL_2PC_ROLLED_BACK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 2), KEEL_2PC_ABORTED);

    /* Issue the actual database rollback commands matching each state:
     * ROLLED_BACK (were PREPARED) → ROLLBACK PREPARED '<gid>'
     * ABORTED     (still active)  → plain ROLLBACK                    */
    TEST_ASSERT(pg_rollback_prepared(c0, gid0));
    TEST_ASSERT(pg_rollback_prepared(c1, gid1));
    TEST_ASSERT(integ_pg_exec(c2, "ROLLBACK"));

    /* Verify: zero rows — all insertions must be rolled back */
    int64_t total = 0;
    TEST_ASSERT(integ_pg_query_int(c0,
        "SELECT COUNT(*) FROM keel_2pc_fi_test", &total));
    TEST_ASSERT_EQ(total, (int64_t)0);

    integ_pg_close(c0);
    integ_pg_close(c1);
    integ_pg_close(c2);
    TEST_END();
}

/* ============================================================================
 * All-prepared then rollback_all (abort after full prepare phase)
 *
 * Models the scenario where the coordinator chooses to abort even after all
 * shards have successfully prepared (e.g. application-level veto).
 * All prepared transactions must be rolled back via ROLLBACK PREPARED.
 * ============================================================================ */

static void test_2pc_fi_all_prepared_then_rollback(void)
{
    TEST_BEGIN("2pc fault-inject: all shards prepared → rollback_all aborts cleanly");

    if (g_skip_tests || g_skip_2pc_tests) {
        printf("  SKIP\n");
        TEST_END();
        return;
    }

    integ_pg_conn_t* c0 = open_conn();
    integ_pg_conn_t* c1 = open_conn();
    TEST_ASSERT(c0 != NULL && c1 != NULL);

    integ_pg_exec(c0, "TRUNCATE keel_2pc_fi_test");

    keel_2pc_coord_t coord;
    keel_2pc_coord_init(&coord, 3003ULL, 3ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&coord, &plan), KEEL_OK);

    TEST_ASSERT(integ_pg_exec(c0, "BEGIN"));
    TEST_ASSERT(integ_pg_exec(c0,
        "INSERT INTO keel_2pc_fi_test VALUES (0, 'veto-s0')"));

    TEST_ASSERT(integ_pg_exec(c1, "BEGIN"));
    TEST_ASSERT(integ_pg_exec(c1,
        "INSERT INTO keel_2pc_fi_test VALUES (1, 'veto-s1')"));

    const char* gid0 = keel_2pc_coord_gid(&coord, 0);
    const char* gid1 = keel_2pc_coord_gid(&coord, 1);

    TEST_ASSERT(pg_prepare_txn(c0, gid0));
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&coord, 0), KEEL_OK);

    TEST_ASSERT(pg_prepare_txn(c1, gid1));
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&coord, 1), KEEL_OK);

    TEST_ASSERT(keel_2pc_coord_all_prepared(&coord));

    /* Decide to abort anyway */
    TEST_ASSERT_EQ(keel_2pc_coord_rollback_all(&coord), KEEL_OK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 0), KEEL_2PC_ROLLED_BACK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 1), KEEL_2PC_ROLLED_BACK);

    /* Both shards were PREPARED — use ROLLBACK PREPARED */
    TEST_ASSERT(pg_rollback_prepared(c0, gid0));
    TEST_ASSERT(pg_rollback_prepared(c1, gid1));

    int64_t total = 0;
    TEST_ASSERT(integ_pg_query_int(c0,
        "SELECT COUNT(*) FROM keel_2pc_fi_test", &total));
    TEST_ASSERT_EQ(total, (int64_t)0);

    integ_pg_close(c0);
    integ_pg_close(c1);
    TEST_END();
}

/* ============================================================================
 * Coordinator state after commit_all verified via inspection API
 * ============================================================================ */

static void test_2pc_fi_coord_state_after_commit(void)
{
    TEST_BEGIN("2pc fault-inject: coordinator overall state COMMITTED after commit_all");

    if (g_skip_tests || g_skip_2pc_tests) {
        printf("  SKIP\n");
        TEST_END();
        return;
    }

    integ_pg_conn_t* c0 = open_conn();
    integ_pg_conn_t* c1 = open_conn();
    TEST_ASSERT(c0 != NULL && c1 != NULL);

    integ_pg_exec(c0, "TRUNCATE keel_2pc_fi_test");

    keel_2pc_coord_t coord;
    keel_2pc_coord_init(&coord, 4004ULL, 4ULL);
    keel_scatter_plan_t plan = make_plan((1ULL << 0) | (1ULL << 1));
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&coord, &plan), KEEL_OK);

    TEST_ASSERT(integ_pg_exec(c0, "BEGIN"));
    TEST_ASSERT(integ_pg_exec(c0,
        "INSERT INTO keel_2pc_fi_test VALUES (0, 'state-s0')"));
    TEST_ASSERT(integ_pg_exec(c1, "BEGIN"));
    TEST_ASSERT(integ_pg_exec(c1,
        "INSERT INTO keel_2pc_fi_test VALUES (1, 'state-s1')"));

    const char* gid0 = keel_2pc_coord_gid(&coord, 0);
    const char* gid1 = keel_2pc_coord_gid(&coord, 1);

    TEST_ASSERT(pg_prepare_txn(c0, gid0));
    keel_2pc_coord_prepare(&coord, 0);
    TEST_ASSERT(pg_prepare_txn(c1, gid1));
    keel_2pc_coord_prepare(&coord, 1);

    /* Commit */
    TEST_ASSERT(pg_commit_prepared(c0, gid0));
    TEST_ASSERT(pg_commit_prepared(c1, gid1));
    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(&coord), KEEL_OK);

    /* Verify overall coordinator state through public API */
    TEST_ASSERT_EQ(keel_2pc_coord_overall_state(&coord), KEEL_2PC_COMMITTED);
    TEST_ASSERT_EQ(keel_2pc_coord_participant_count(&coord), (size_t)2);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 0), KEEL_2PC_COMMITTED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 1), KEEL_2PC_COMMITTED);

    integ_pg_close(c0);
    integ_pg_close(c1);
    TEST_END();
}

/* ============================================================================
 * Coordinator state machine: partial failure leaves ABORTED shard correctly
 * ============================================================================ */

static void test_2pc_fi_coord_partial_state_inspection(void)
{
    TEST_BEGIN("2pc fault-inject: partial failure — per-shard state inspection");

    if (g_skip_tests || g_skip_2pc_tests) {
        printf("  SKIP\n");
        TEST_END();
        return;
    }

    integ_pg_conn_t* c0 = open_conn();
    integ_pg_conn_t* c1 = open_conn();
    integ_pg_conn_t* c2 = open_conn();
    TEST_ASSERT(c0 != NULL && c1 != NULL && c2 != NULL);

    integ_pg_exec(c0, "TRUNCATE keel_2pc_fi_test");

    keel_2pc_coord_t coord;
    keel_2pc_coord_init(&coord, 5005ULL, 5ULL);
    keel_scatter_plan_t plan =
        make_plan((1ULL << 0) | (1ULL << 1) | (1ULL << 2));
    keel_2pc_coord_begin(&coord, &plan);

    integ_pg_exec(c0, "BEGIN");
    integ_pg_exec(c0, "INSERT INTO keel_2pc_fi_test VALUES (0, 'insp-s0')");
    integ_pg_exec(c1, "BEGIN");
    integ_pg_exec(c1, "INSERT INTO keel_2pc_fi_test VALUES (1, 'insp-s1')");
    integ_pg_exec(c2, "BEGIN");
    integ_pg_exec(c2, "INSERT INTO keel_2pc_fi_test VALUES (2, 'insp-s2')");

    /* Only prepare shards 0 and 1 */
    const char* gid0 = keel_2pc_coord_gid(&coord, 0);
    const char* gid1 = keel_2pc_coord_gid(&coord, 1);

    pg_prepare_txn(c0, gid0);
    keel_2pc_coord_prepare(&coord, 0);

    pg_prepare_txn(c1, gid1);
    keel_2pc_coord_prepare(&coord, 1);

    /* Shard 2: record failure without touching the live transaction */
    keel_2pc_coord_prepare_failed(&coord, 2);

    /* Before rollback_all: check states */
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 0), KEEL_2PC_PREPARED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 1), KEEL_2PC_PREPARED);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 2), KEEL_2PC_ABORTED);
    TEST_ASSERT(!keel_2pc_coord_all_prepared(&coord));

    keel_2pc_coord_rollback_all(&coord);

    /* After rollback_all: PREPARED→ROLLED_BACK; ABORTED stays ABORTED */
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 0), KEEL_2PC_ROLLED_BACK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 1), KEEL_2PC_ROLLED_BACK);
    TEST_ASSERT_EQ(keel_2pc_coord_shard_state(&coord, 2), KEEL_2PC_ABORTED);

    /* Issue database rollbacks to clean up */
    pg_rollback_prepared(c0, gid0);
    pg_rollback_prepared(c1, gid1);
    integ_pg_exec(c2, "ROLLBACK");

    /* Data: 0 rows */
    int64_t total = 0;
    integ_pg_query_int(c0, "SELECT COUNT(*) FROM keel_2pc_fi_test", &total);
    TEST_ASSERT_EQ(total, (int64_t)0);

    integ_pg_close(c0);
    integ_pg_close(c1);
    integ_pg_close(c2);
    TEST_END();
}

/* ============================================================================
 * Edge case: single-shard 2PC (degenerate case — no scatter needed, but
 * the coordinator must handle it correctly)
 * ============================================================================ */

static void test_2pc_fi_single_shard(void)
{
    TEST_BEGIN("2pc fault-inject: single-shard coordinator happy path");

    if (g_skip_tests || g_skip_2pc_tests) {
        printf("  SKIP\n");
        TEST_END();
        return;
    }

    integ_pg_conn_t* c0 = open_conn();
    TEST_ASSERT(c0 != NULL);

    integ_pg_exec(c0, "TRUNCATE keel_2pc_fi_test");

    keel_2pc_coord_t coord;
    keel_2pc_coord_init(&coord, 6006ULL, 6ULL);
    keel_scatter_plan_t plan = make_plan(1ULL << 0);
    TEST_ASSERT_EQ(keel_2pc_coord_begin(&coord, &plan), KEEL_OK);
    TEST_ASSERT_EQ(coord.count, (size_t)1);

    integ_pg_exec(c0, "BEGIN");
    integ_pg_exec(c0, "INSERT INTO keel_2pc_fi_test VALUES (0, 'single')");

    const char* gid0 = keel_2pc_coord_gid(&coord, 0);
    TEST_ASSERT(gid0 != NULL);

    TEST_ASSERT(pg_prepare_txn(c0, gid0));
    TEST_ASSERT_EQ(keel_2pc_coord_prepare(&coord, 0), KEEL_OK);
    TEST_ASSERT(keel_2pc_coord_all_prepared(&coord));

    TEST_ASSERT(pg_commit_prepared(c0, gid0));
    TEST_ASSERT_EQ(keel_2pc_coord_commit_all(&coord), KEEL_OK);
    TEST_ASSERT_EQ(keel_2pc_coord_overall_state(&coord), KEEL_2PC_COMMITTED);

    int64_t total = 0;
    TEST_ASSERT(integ_pg_query_int(c0,
        "SELECT COUNT(*) FROM keel_2pc_fi_test", &total));
    TEST_ASSERT_EQ(total, (int64_t)1);

    integ_pg_close(c0);
    TEST_END();
}

/* ============================================================================
 * Reset: coordinator can be reused across transactions in the same session
 * ============================================================================ */

static void test_2pc_fi_coord_reset_and_reuse(void)
{
    TEST_BEGIN("2pc fault-inject: coordinator reset and reuse");

    if (g_skip_tests || g_skip_2pc_tests) {
        printf("  SKIP\n");
        TEST_END();
        return;
    }

    integ_pg_conn_t* c0 = open_conn();
    TEST_ASSERT(c0 != NULL);

    integ_pg_exec(c0, "TRUNCATE keel_2pc_fi_test");

    keel_2pc_coord_t coord;
    keel_2pc_coord_init(&coord, 7007ULL, 7ULL);
    keel_scatter_plan_t plan = make_plan(1ULL << 0);

    /* First transaction: commit */
    keel_2pc_coord_begin(&coord, &plan);
    integ_pg_exec(c0, "BEGIN");
    integ_pg_exec(c0, "INSERT INTO keel_2pc_fi_test VALUES (0, 'txn1')");
    const char* gid_first = keel_2pc_coord_gid(&coord, 0);
    char gid_copy[KEEL_2PC_GID_MAX];
    strncpy(gid_copy, gid_first, sizeof gid_copy - 1);
    gid_copy[sizeof gid_copy - 1] = '\0';
    pg_prepare_txn(c0, gid_copy);
    keel_2pc_coord_prepare(&coord, 0);
    pg_commit_prepared(c0, gid_copy);
    keel_2pc_coord_commit_all(&coord);

    /* Reset and reuse with a new sequence number */
    keel_2pc_coord_init(&coord, 7007ULL, 8ULL);
    keel_2pc_coord_begin(&coord, &plan);

    integ_pg_exec(c0, "BEGIN");
    integ_pg_exec(c0, "INSERT INTO keel_2pc_fi_test VALUES (0, 'txn2')");
    const char* gid_second = keel_2pc_coord_gid(&coord, 0);
    char gid_copy2[KEEL_2PC_GID_MAX];
    strncpy(gid_copy2, gid_second, sizeof gid_copy2 - 1);
    gid_copy2[sizeof gid_copy2 - 1] = '\0';

    /* GIDs must be distinct across transactions */
    TEST_ASSERT(strcmp(gid_copy, gid_copy2) != 0);

    pg_prepare_txn(c0, gid_copy2);
    keel_2pc_coord_prepare(&coord, 0);
    pg_commit_prepared(c0, gid_copy2);
    keel_2pc_coord_commit_all(&coord);
    TEST_ASSERT_EQ(keel_2pc_coord_overall_state(&coord), KEEL_2PC_COMMITTED);

    /* Both transactions committed — 2 rows */
    int64_t total = 0;
    integ_pg_query_int(c0, "SELECT COUNT(*) FROM keel_2pc_fi_test", &total);
    TEST_ASSERT_EQ(total, (int64_t)2);

    integ_pg_close(c0);
    TEST_END();
}

/* ============================================================================
 * main
 * ============================================================================ */

int main(void)
{
    keel_mem_init(NULL);

    if (!integ_cluster_running()) {
        printf("SKIP: no PostgreSQL cluster reachable — set KEEL_TEST_PG_HOST1/"
               "KEEL_TEST_PG_PORT1 or start the test cluster\n");
        g_skip_tests = true;
    }

    if (!g_skip_tests) {
        if (!setup_fi_schema()) {
            printf("SKIP: failed to create fault-inject test schema\n");
            g_skip_tests = true;
        }
    }

    /* Always probe the configuration first so the skip flag is set */
    test_2pc_fi_probe_config();

    test_2pc_fi_happy_path_commit();
    test_2pc_fi_n_minus_1_rollback();
    test_2pc_fi_all_prepared_then_rollback();
    test_2pc_fi_coord_state_after_commit();
    test_2pc_fi_coord_partial_state_inspection();
    test_2pc_fi_single_shard();
    test_2pc_fi_coord_reset_and_reuse();

    if (!g_skip_tests)
        teardown_fi_schema();

    printf("\n%d tests run — %d passed, %d failed\n",
           g_tests_run, g_tests_passed, g_tests_failed);
    return test_summary();
}
