-- =============================================================================
-- Initialize MySQL Primary for Replication
-- =============================================================================

-- Create replication user
CREATE USER IF NOT EXISTS 'replicator'@'%' IDENTIFIED BY 'replicator';
GRANT REPLICATION SLAVE ON *.* TO 'replicator'@'%';

-- Create test user with full privileges (for KEEL proxy testing)
CREATE USER IF NOT EXISTS 'keel'@'%' IDENTIFIED BY 'keel';
GRANT ALL PRIVILEGES ON *.* TO 'keel'@'%' WITH GRANT OPTION;

-- Create test database and sample table
CREATE DATABASE IF NOT EXISTS test;
USE test;

CREATE TABLE IF NOT EXISTS t1 (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    val  VARCHAR(255),
    ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO t1 (val) VALUES ('initial-row-1'), ('initial-row-2'), ('initial-row-3');

FLUSH PRIVILEGES;
