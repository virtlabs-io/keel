-- =============================================================================
-- write_insert.sql — New event INSERT with JSONB payload (pgbench weight: 8 %)
-- =============================================================================
--
-- Inserts a new event row with a JSONB payload.  The payload includes a
-- random MD5 token to produce variable-length JSONB data, stressing the
-- wire protocol's handling of binary TOAST-able columns.
--
-- Keel must classify this as a write and route to the primary.  The
-- insert has a foreign key to keel_accounts(id), so referential integrity
-- checks run at commit time.
-- =============================================================================

\set account_id random(1, 200000)
\set tenant_id random(1, 1000)
\set event_type random(1, 20)
\set amount_cents random(100, 500000)
\set client_id :client_id
INSERT INTO keel_events (account_id, tenant_id, event_type, amount, payload, created_at, processed)
VALUES (
    :account_id,
    :tenant_id,
    :event_type,
    (:amount_cents / 100.0),
    jsonb_build_object('source', 'benchmark', 'client', :client_id, 'token', md5(random()::text)),
    now(),
    false
);
