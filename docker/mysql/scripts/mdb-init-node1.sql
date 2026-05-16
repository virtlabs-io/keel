-- =============================================================================
-- MariaDB Galera Cluster — Node 1 Init (test data)
-- Runs via docker-entrypoint-initdb.d on first startup
-- =============================================================================

-- Note: 'keel' user is created by MARIADB_USER/MARIADB_PASSWORD env vars
-- Grant full privileges for proxy testing
GRANT ALL PRIVILEGES ON *.* TO 'keel'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

-- Test table
USE test;

CREATE TABLE IF NOT EXISTS t1 (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    val  VARCHAR(255),
    ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

INSERT INTO t1 (val) VALUES ('initial-row-1'), ('initial-row-2'), ('initial-row-3');
