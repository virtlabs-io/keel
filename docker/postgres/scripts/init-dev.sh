#!/usr/bin/env bash
# Init script for the dev PostgreSQL instance.
# Creates the sbtest user and database used by sysbench.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- sysbench user + database
    CREATE USER sbtest WITH ENCRYPTED PASSWORD 'sbtest';
    CREATE DATABASE sbtest OWNER sbtest;

    -- Allow all connections from the Docker network (dev only)
    ALTER SYSTEM SET listen_addresses = '*';
EOSQL

# Allow password auth from any Docker network address
cat >> "$PGDATA/pg_hba.conf" <<EOF

# Dev: allow all connections with any password (not for production)
host    all     all     0.0.0.0/0     md5
EOF

echo "[init-dev] sbtest user and database created."
