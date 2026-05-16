-- =============================================================================
-- Percona XtraDB Cluster — Node 1 Init (test data + users)
-- Runs via docker-entrypoint-initdb.d on first startup
-- =============================================================================

-- Test user for KEEL proxy
CREATE USER IF NOT EXISTS 'keel'@'%' IDENTIFIED WITH mysql_native_password BY 'keel';
GRANT ALL PRIVILEGES ON *.* TO 'keel'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

-- Test database and table
CREATE DATABASE IF NOT EXISTS test;
USE test;

CREATE TABLE IF NOT EXISTS t1 (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    val  VARCHAR(255),
    ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

INSERT INTO t1 (val) VALUES ('initial-row-1'), ('initial-row-2'), ('initial-row-3');
