-- =============================================================================
-- write_delete.sql — Old event DELETE with SKIP LOCKED (pgbench weight: 2 %)
-- =============================================================================
--
-- Deletes one processed event older than 120 days using a CTE with
-- FOR UPDATE SKIP LOCKED.  This pattern mimics a background cleanup job
-- running concurrently with the benchmark:
--
--   1. The CTE selects one eligible row, locking it exclusively.
--      SKIP LOCKED avoids deadlocks when many clients run in parallel.
--   2. The outer DELETE uses the CTE result to remove the row.
--
-- Low weight (2 %) keeps dead-tuple accumulation modest, while still
-- exercising Keel's write routing, CTE detection, and DELETE handling.
-- =============================================================================

WITH victim AS (
    SELECT id
    FROM keel_events
    WHERE processed = true
      AND created_at < now() - interval '120 days'
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
DELETE FROM keel_events e
USING victim
WHERE e.id = victim.id;
