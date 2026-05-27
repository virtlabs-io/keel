/*
 * Test: does an extended-protocol Parse create a prepared statement whose
 * plan gets invalidated when a temp table shadows the permanent table?
 * Compare with simple-query PREPARE.
 *
 * Build: gcc -o /tmp/test_pg_parse /tmp/test_pg_parse.c -lpq
 * Run:   /tmp/test_pg_parse
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>

static void die(PGconn *conn, const char *msg) {
    fprintf(stderr, "FATAL: %s: %s\n", msg, PQerrorMessage(conn));
    PQfinish(conn);
    exit(1);
}

static void exec_ok(PGconn *conn, const char *sql) {
    PGresult *r = PQexec(conn, sql);
    if (PQresultStatus(r) != PGRES_COMMAND_OK &&
        PQresultStatus(r) != PGRES_TUPLES_OK) {
        fprintf(stderr, "ERROR running: %s\n  -> %s\n", sql, PQresultErrorMessage(r));
        PQclear(r);
        PQfinish(conn);
        exit(1);
    }
    PQclear(r);
}

static long long query_int(PGconn *conn, const char *sql) {
    PGresult *r = PQexec(conn, sql);
    if (PQresultStatus(r) != PGRES_TUPLES_OK) {
        fprintf(stderr, "ERROR in query: %s -> %s\n", sql, PQresultErrorMessage(r));
        PQclear(r);
        return -1;
    }
    long long v = atoll(PQgetvalue(r, 0, 0));
    PQclear(r);
    return v;
}

int main(void) {
    const char *conninfo = "host=127.0.0.1 port=15432 user=postgres dbname=postgres";
    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "SKIP: no PostgreSQL on 127.0.0.1:15432 (%s)\n",
                PQerrorMessage(conn));
        PQfinish(conn);
        return 77; /* CTest SKIP_RETURN_CODE */
    }

    /* Setup */
    exec_ok(conn, "DROP TABLE IF EXISTS public.ssv_ugly_target");
    exec_ok(conn, "CREATE TABLE public.ssv_ugly_target (id int)");
    exec_ok(conn, "INSERT INTO public.ssv_ugly_target VALUES (1),(2),(3)");

    /* ===== Test 1: Simple-query PREPARE ===== */
    printf("=== Test 1: Simple-query PREPARE ===\n");
    exec_ok(conn, "PREPARE ssv_ugly_count AS SELECT COUNT(*) FROM ssv_ugly_target");
    long long r1 = query_int(conn, "EXECUTE ssv_ugly_count");
    printf("Outside tx: %lld (expected 3)\n", r1);

    exec_ok(conn, "BEGIN");
    exec_ok(conn, "CREATE TEMP TABLE ssv_ugly_target (id int)");
    exec_ok(conn, "INSERT INTO ssv_ugly_target VALUES (1)");
    long long r1a = query_int(conn, "SELECT COUNT(*) FROM ssv_ugly_target");
    long long r1b = query_int(conn, "EXECUTE ssv_ugly_count");
    printf("Inside tx - direct COUNT: %lld (expected 1)\n", r1a);
    printf("Inside tx - EXECUTE (simple PREPARE): %lld (expected 1)\n", r1b);
    exec_ok(conn, "ROLLBACK");

    exec_ok(conn, "DEALLOCATE ssv_ugly_count");
    exec_ok(conn, "DISCARD TEMP");

    /* ===== Test 2: Extended-protocol Parse via PQprepare ===== */
    printf("\n=== Test 2: Extended-protocol Parse (PQprepare) ===\n");
    /*
     * PQprepare() sends an extended-protocol Parse 'P' message.
     * The statement is then accessible via both:
     *   - extended-protocol Execute 'E'
     *   - simple-query EXECUTE name
     */
    PGresult *prep = PQprepare(conn, "ssv_ugly_count",
                               "SELECT COUNT(*) FROM ssv_ugly_target",
                               0, NULL);
    if (PQresultStatus(prep) != PGRES_COMMAND_OK) {
        fprintf(stderr, "PQprepare failed: %s\n", PQresultErrorMessage(prep));
        PQclear(prep);
        PQfinish(conn);
        return 1;
    }
    PQclear(prep);

    /* Execute outside tx via PQexecPrepared (extended-protocol Execute) */
    PGresult *r = PQexecPrepared(conn, "ssv_ugly_count", 0, NULL, NULL, NULL, 0);
    long long r2 = atoll(PQgetvalue(r, 0, 0));
    PQclear(r);
    printf("Outside tx (extended Execute): %lld (expected 3)\n", r2);

    /* Execute outside tx via simple-query EXECUTE */
    long long r2s = query_int(conn, "EXECUTE ssv_ugly_count");
    printf("Outside tx (simple EXECUTE): %lld (expected 3)\n", r2s);

    /* Inside transaction with temp table */
    exec_ok(conn, "BEGIN");
    exec_ok(conn, "CREATE TEMP TABLE ssv_ugly_target (id int)");
    exec_ok(conn, "INSERT INTO ssv_ugly_target VALUES (1)");

    long long r2a = query_int(conn, "SELECT COUNT(*) FROM ssv_ugly_target");
    printf("Inside tx - direct COUNT: %lld (expected 1)\n", r2a);

    /* Simple-query EXECUTE on extended-protocol-prepared stmt */
    long long r2b = query_int(conn, "EXECUTE ssv_ugly_count");
    printf("Inside tx - EXECUTE (simple, ext-proto prepared): %lld (expected 1, keel gets 3)\n", r2b);

    /* Extended-protocol Execute on same stmt */
    PGresult *r2c_res = PQexecPrepared(conn, "ssv_ugly_count", 0, NULL, NULL, NULL, 0);
    long long r2c = atoll(PQgetvalue(r2c_res, 0, 0));
    PQclear(r2c_res);
    printf("Inside tx - ext-proto Execute (extended prepared): %lld (expected 1)\n", r2c);

    exec_ok(conn, "ROLLBACK");

    /* Cleanup */
    exec_ok(conn, "DROP TABLE public.ssv_ugly_target");
    PQfinish(conn);
    return 0;
}
