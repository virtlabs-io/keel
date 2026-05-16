-- =============================================================================
-- read_range.sql — Tenant range scan with date window (pgbench weight: 15 %)
-- =============================================================================
--
-- Scans the keel_events table by tenant_id within a 30-day window,
-- ordered by created_at DESC, limited to 50 rows.  Uses the composite
-- index idx_keel_events_tenant_created(tenant_id, created_at DESC).
--
-- This workload tests the proxy's handling of multi-row result sets
-- and ORDER BY + LIMIT pushdown.  Keel routes it to a replica.
-- =============================================================================

\set tenant_id random(1, 1000)
SELECT id, account_id, tenant_id, event_type, amount, created_at, processed
FROM keel_events
WHERE tenant_id = :tenant_id
  AND created_at >= now() - interval '30 days'
ORDER BY created_at DESC
LIMIT 50;
