-- =============================================================================
-- pgbench_read_rw_split.sql — read-only transaction using BEGIN READ ONLY
-- =============================================================================
-- Uses an explicit BEGIN READ ONLY transaction so R/W split proxies route
-- this to a replica backend.
--
-- Plain BEGIN wraps a transaction where writes MIGHT follow; the proxy must
-- route to primary to be safe. BEGIN READ ONLY signals reads only → replica.
--
-- Usage: pgbench -f pgbench_read_rw_split.sql -M simple ...
-- =============================================================================
\set aid random(1, 100000 * :scale)
\set aid2 random(1, 100000 * :scale)

BEGIN READ ONLY;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
SELECT abalance FROM pgbench_accounts WHERE aid = :aid2;
SELECT sum(abalance) FROM pgbench_accounts WHERE aid BETWEEN :aid AND :aid + 100;
COMMIT;
