-- =============================================================================
-- read_point.sql — Single-row PK lookup (pgbench weight: 45 %)
-- =============================================================================
--
-- The most common query in the mix.  Fetches one account by primary key,
-- which resolves to an index-only scan on the keel_accounts PK index.
-- This is a pure read — Keel should route it to a replica.
--
-- Exercises the hot-path: fast parse → route-to-replica → splice/sendfile
-- data path.  The dominant workload for measuring baseline per-query
-- overhead through the proxy.
-- =============================================================================

\set account_id random(1, 200000)
SELECT id, tenant_id, status, balance, credit_limit, updated_at, score
FROM keel_accounts
WHERE id = :account_id;
