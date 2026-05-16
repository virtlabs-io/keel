#!/bin/bash
# Initialize primary for replication
set -e

# Create replication user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator';
EOSQL

# Remove the default scram-sha-256 rule (comes before our trust rules)
sed -i '/host all all all scram-sha-256/d' "$PGDATA/pg_hba.conf"

# Configure pg_hba.conf for replication
# Use 'trust' for testing - in production, use scram-sha-256 or md5
cat >> "$PGDATA/pg_hba.conf" <<EOF

# Replication connections
host    replication     replicator      0.0.0.0/0               trust
# Test connections from host (trust for simplicity in test environment)
host    all             all             0.0.0.0/0               trust
EOF

# Include custom configuration overrides (mounted from docker/config/)
# This goes at the end of postgresql.conf so our values take precedence.
echo "" >> "$PGDATA/postgresql.conf"
echo "# Custom overrides (from docker/config/postgresql-overrides.conf)" >> "$PGDATA/postgresql.conf"
echo "include_if_exists = '/etc/postgresql/conf.d/postgresql-overrides.conf'" >> "$PGDATA/postgresql.conf"

echo "Primary initialized for replication"
