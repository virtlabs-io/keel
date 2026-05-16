#!/bin/bash
# =============================================================================
# Initialize E2E Test Primary PostgreSQL
# =============================================================================
# Creates:
# - Replication user for replicas
# - Test database with proper configuration
# - pg_hba.conf entries for replication and proxy access
# =============================================================================

set -e

echo "[init-primary] Setting up E2E test primary..."

# -----------------------------------------------------------------------------
# Create replication user
# -----------------------------------------------------------------------------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create replication user
    CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator';
    
    -- Create pgbench user (for stress testing)
    CREATE USER pgbench WITH ENCRYPTED PASSWORD 'pgbench';
    GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_DB TO pgbench;
    
    -- Ensure postgres user has full access
    ALTER USER postgres WITH SUPERUSER;
EOSQL

echo "[init-primary] Created replication and test users"

# -----------------------------------------------------------------------------
# Configure pg_hba.conf for replication and proxy access
# Trust-based auth for E2E testing simplicity
# -----------------------------------------------------------------------------
cat > "$PGDATA/pg_hba.conf" <<EOF
# =============================================================================
# E2E Test Configuration - Trust-based for testing
# =============================================================================

# TYPE  DATABASE        USER            ADDRESS                 METHOD

# Local connections
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust

# Replication connections from replicas
local   replication     all                                     trust
host    replication     all             0.0.0.0/0               trust

# Allow all connections from docker network (for KEEL proxy)
host    all             all             0.0.0.0/0               trust
EOF

echo "[init-primary] Updated pg_hba.conf with trust auth"

# -----------------------------------------------------------------------------
# Include custom PostgreSQL configuration overrides
# Appended at the end of postgresql.conf so our values take precedence
# over defaults. The file is mounted from docker/config/.
# -----------------------------------------------------------------------------
echo "" >> "$PGDATA/postgresql.conf"
echo "# Custom overrides (from docker/config/postgresql-overrides.conf)" >> "$PGDATA/postgresql.conf"
echo "include_if_exists = '/etc/postgresql/conf.d/postgresql-overrides.conf'" >> "$PGDATA/postgresql.conf"

echo "[init-primary] Added postgresql-overrides.conf include"

# -----------------------------------------------------------------------------
# Reload configuration
# -----------------------------------------------------------------------------
pg_ctl reload -D "$PGDATA"

echo "[init-primary] Primary initialization complete!"
