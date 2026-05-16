-- =============================================================================
-- MySQL Group Replication — Node 1 Init (user setup + test data)
-- =============================================================================

-- Replication user for GR internal communication
SET SQL_LOG_BIN = 0;
CREATE USER IF NOT EXISTS 'replicator'@'%' IDENTIFIED BY 'replicator';
GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%';
GRANT CONNECTION_ADMIN ON *.* TO 'replicator'@'%';
GRANT BACKUP_ADMIN ON *.* TO 'replicator'@'%';
GRANT GROUP_REPLICATION_STREAM ON *.* TO 'replicator'@'%';

-- Test user for KEEL proxy
CREATE USER IF NOT EXISTS 'keel'@'%' IDENTIFIED BY 'keel';
GRANT ALL PRIVILEGES ON *.* TO 'keel'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SET SQL_LOG_BIN = 1;

-- Configure recovery channel
CHANGE REPLICATION SOURCE TO SOURCE_USER = 'replicator', SOURCE_PASSWORD = 'replicator'
    FOR CHANNEL 'group_replication_recovery';

-- Test data
CREATE DATABASE IF NOT EXISTS test;
USE test;

CREATE TABLE IF NOT EXISTS t1 (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    val  VARCHAR(255),
    ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO t1 (val) VALUES ('initial-row-1'), ('initial-row-2'), ('initial-row-3');
