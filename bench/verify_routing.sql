-- =============================================================================
-- verify_routing.sql — Post-benchmark routing correctness diagnostics
-- =============================================================================
--
-- Five diagnostic queries to verify that Keel's read/write split operated
-- correctly during a pgbench run.  Execute against both the primary and
-- each replica, then compare the output:
--
--   1. pg_stat_database      — transaction commits/rollbacks, tuple I/O
--   2. pg_stat_statements     — per-query call counts and latencies
--                               (requires the pg_stat_statements extension)
--   3. pg_stat_replication    — replication lag and sync state on the primary
--   4. Recent events count    — confirms write traffic stayed on the primary
--   5. pg_stat_user_tables    — insert/update/delete churn and dead-tuple count
--
-- What to look for:
--   - Replicas should show high SELECT call counts and zero INSERTs/UPDATEs.
--   - The primary should carry all DML and have correspondingly higher
--     tup_inserted / tup_updated / tup_deleted.
--   - Replication lag (write_lag, flush_lag, replay_lag) should be < 1 s
--     under normal network conditions.
--   - n_dead_tup on the primary indicates autovacuum pressure from the
--     UPDATE/DELETE workloads.
--
-- Usage:
--   psql -h <primary> -d keeltest -f verify_routing.sql
--   psql -h <replica> -d keeltest -f verify_routing.sql
-- =============================================================================

-- Run on primary and replicas separately and compare.

-- 1. Basic database activity
SELECT datname, xact_commit, xact_rollback, blks_read, blks_hit,
       tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted
FROM pg_stat_database
WHERE datname = current_database();

-- 2. Statement mix (requires pg_stat_statements)
SELECT query,
       calls,
       total_exec_time,
       mean_exec_time,
       rows
FROM pg_stat_statements
WHERE query ILIKE '%keel_accounts%'
   OR query ILIKE '%keel_events%'
ORDER BY calls DESC
LIMIT 30;

-- 3. Replication visibility on primary
SELECT pid, usename, application_name, client_addr, state, sync_state,
       write_lag, flush_lag, replay_lag
FROM pg_stat_replication;

-- 4. Confirm whether writes stayed on primary
SELECT now() AS sample_time,
       count(*) FILTER (WHERE created_at >= now() - interval '5 minutes') AS recent_events,
       count(*) FILTER (WHERE processed = false AND created_at >= now() - interval '5 minutes') AS recent_unprocessed
FROM keel_events;

-- 5. Check table growth and churn
SELECT relname,
       n_tup_ins,
       n_tup_upd,
       n_tup_del,
       n_live_tup,
       n_dead_tup
FROM pg_stat_user_tables
WHERE relname IN ('keel_accounts', 'keel_events')
ORDER BY relname;
