-- =============================================================================
-- write_update.sql — Account balance/score UPDATE (pgbench weight: 20 %)
-- =============================================================================
--
-- Updates a single account's balance, score, and timestamps by PK lookup.
-- This is the primary write workload.  Keel must detect the UPDATE verb
-- and route to the primary.
--
-- The update pattern is additive (balance + delta) rather than absolute to
-- avoid serialisation conflicts under concurrency.  The WHERE clause uses
-- the PK, so the plan is a simple index scan + heap update.
-- =============================================================================

\set account_id random(1, 200000)
\set delta random(1, 500)
\set score_delta random(1, 10)
UPDATE keel_accounts
SET balance = balance + (:delta / 100.0),
    score = score + :score_delta,
    updated_at = now(),
    last_seen_at = now()
WHERE id = :account_id;
