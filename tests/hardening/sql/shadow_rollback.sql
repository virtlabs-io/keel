-- §15.5 Shadow correctness workload: explicit transactions with ROLLBACK
--
-- Verifies that KEEL does not confuse the transaction state between sessions.
-- A rolled-back INSERT must not persist.  Rows inserted in committed
-- transactions must be visible.  The diff between direct-PG and proxy output
-- must be empty.

CREATE TABLE IF NOT EXISTS keel_shadow_rollback (
    id   SERIAL PRIMARY KEY,
    val  TEXT NOT NULL
);

-- Committed transaction — row must survive.
BEGIN;
INSERT INTO keel_shadow_rollback(val) VALUES ('committed_row');
COMMIT;

-- Rolled-back transaction — row must not survive.
BEGIN;
INSERT INTO keel_shadow_rollback(val) VALUES ('rolled_back_row');
ROLLBACK;

-- Read-only query: only the committed row must be visible.
SELECT val FROM keel_shadow_rollback ORDER BY val;

-- Savepoint rollback: inner savepoint rolled back, outer committed.
BEGIN;
INSERT INTO keel_shadow_rollback(val) VALUES ('outer_row');
SAVEPOINT sp1;
INSERT INTO keel_shadow_rollback(val) VALUES ('savepoint_rolled_back');
ROLLBACK TO SAVEPOINT sp1;
COMMIT;

SELECT val FROM keel_shadow_rollback ORDER BY val;

-- Teardown
DROP TABLE keel_shadow_rollback;
