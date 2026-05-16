-- =============================================================================
-- scatter_group_agg.sql — Scatter-merge GROUP BY aggregate workload
-- =============================================================================
--
-- Aggregate event counts and totals by event_type with no shard-key filter,
-- so keel fans out to ALL shards.  This exercises the full scatter-merge
-- pipeline: fan-out → parallel per-shard GROUP BY → hash-merge → ORDER BY →
-- LIMIT.
--
-- Why no shard-key predicate:
--   A WHERE tenant_id = :x would be routed to a single shard (point query).
--   Omitting the shard key forces the scatter path regardless of shard count.
--   The query still uses an index on event_type if one exists.
--
-- Expected keel behaviour:
--   1. Dispatch rewrites to run on every shard (no shard key in predicate).
--   2. Each shard executes GROUP BY event_type independently.
--   3. keel merges partial groups: SUM(cnt) + SUM(total_amount) per event_type.
--   4. Final result sorted by event_type, LIMIT 20 applied post-merge.
--
-- Result cardinality: low (≤ event_type cardinality, typically ~10–20 rows).
-- Server-side cost: full table scan with hash-aggregate — representative of
-- OLAP scatter workloads that would be routed cross-shard.
-- =============================================================================

SELECT event_type,
       count(*)      AS cnt,
       sum(amount)   AS total_amount
FROM   keel_events
GROUP  BY event_type
ORDER  BY event_type
LIMIT  20;
