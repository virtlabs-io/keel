-- =============================================================================
-- read_aggregate.sql — COUNT/SUM/AVG aggregate per tenant (pgbench weight: 10 %)
-- =============================================================================
--
-- Aggregates event counts and amounts for a random tenant over a 7-day
-- window.  Returns a single row (GROUP BY tenant_id), so the result set
-- is tiny but the server-side work is non-trivial.
--
-- Stresses the proxy's ability to relay compute-heavy read queries
-- without adding measurable overhead.  Routed to a replica.
-- =============================================================================

\set tenant_id random(1, 1000)
SELECT tenant_id,
       count(*) AS total_events,
       sum(amount) AS total_amount,
       avg(amount) AS avg_amount
FROM keel_events
WHERE tenant_id = :tenant_id
  AND created_at >= now() - interval '7 days'
GROUP BY tenant_id;
