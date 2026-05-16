-- =============================================================================
-- MySQL Group Replication — Joiner Node Init (nodes 2 & 3)
-- =============================================================================

-- Replication user (must exist locally for recovery)
SET SQL_LOG_BIN = 0;
CREATE USER IF NOT EXISTS 'replicator'@'%' IDENTIFIED BY 'replicator';
GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%';
GRANT CONNECTION_ADMIN ON *.* TO 'replicator'@'%';
GRANT BACKUP_ADMIN ON *.* TO 'replicator'@'%';
GRANT GROUP_REPLICATION_STREAM ON *.* TO 'replicator'@'%';

-- Test user
CREATE USER IF NOT EXISTS 'keel'@'%' IDENTIFIED BY 'keel';
GRANT ALL PRIVILEGES ON *.* TO 'keel'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SET SQL_LOG_BIN = 1;

-- Configure recovery channel
CHANGE REPLICATION SOURCE TO SOURCE_USER = 'replicator', SOURCE_PASSWORD = 'replicator'
    FOR CHANNEL 'group_replication_recovery';
