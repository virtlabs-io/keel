#!/bin/bash
# Initialize MySQL replica — configure replication from primary
# Runs via docker-entrypoint-initdb.d (during first startup only)
set -e

PRIMARY_HOST="${MYSQL_PRIMARY_HOST:-mysql-primary}"
PRIMARY_PORT="${MYSQL_PRIMARY_PORT:-3306}"

echo "[KEEL] Waiting for primary to accept connections..."
for i in $(seq 1 60); do
    if mysqladmin ping -h "$PRIMARY_HOST" -P "$PRIMARY_PORT" -uroot -proot --silent 2>/dev/null; then
        echo "[KEEL] Primary is ready."
        break
    fi
    echo "[KEEL] Waiting for primary ($i/60)..."
    sleep 2
done

echo "[KEEL] Configuring GTID-based replication..."
mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<-EOSQL
    CHANGE REPLICATION SOURCE TO
        SOURCE_HOST      = '${PRIMARY_HOST}',
        SOURCE_PORT      = ${PRIMARY_PORT},
        SOURCE_USER      = 'replicator',
        SOURCE_PASSWORD  = 'replicator',
        SOURCE_AUTO_POSITION = 1,
        GET_SOURCE_PUBLIC_KEY = 1;

    START REPLICA;
EOSQL

echo "[KEEL] Replica replication started."
