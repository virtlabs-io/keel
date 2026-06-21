#!/usr/bin/env bash

set -euo pipefail

host="${KEEL_HOST:-127.0.0.1}"
port="${KEEL_PORT:-7432}"
user_name="${KEEL_USER:-usr_test}"
db_name="${KEEL_DB:-testdb}"

if ! command -v psql >/dev/null 2>&1; then
    echo "psql is required for scripts/run_pg_debug_session.sh" >&2
    exit 1
fi

export PGPASSWORD="${PGPASSWORD:-qaz123}"

psql "host=${host} port=${port} user=${user_name} dbname=${db_name}" \
    -X \
    -v ON_ERROR_STOP=1 <<'SQL'
SELECT pg_backend_pid() AS first_backend_pid;
SELECT 1 AS first_simple_query;
PREPARE keel_dbg AS SELECT 42 AS prepared_answer;
EXECUTE keel_dbg;
DEALLOCATE keel_dbg;
BEGIN;
SELECT 7 AS in_tx;
COMMIT;
SQL