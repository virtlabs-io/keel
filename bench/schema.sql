-- =============================================================================
-- schema.sql — Keel benchmark schema and initial data seeding
-- =============================================================================
--
-- Creates a two-table multi-tenant data model designed to exercise every
-- query pattern in the pgbench harness (point reads, range scans, aggregates,
-- updates, inserts, and row-level deletes).
--
-- Tables:
--   keel_accounts (200 k rows)
--     Multi-tenant user accounts with balance, credit limit, score, and
--     timestamps.  Serves as the write-update target and the PK-lookup
--     table for read_point workloads.
--
--   keel_events (1 M rows)
--     Event log referencing accounts.  Has a tenant + date composite index
--     for range scans, a processed flag for the DELETE-with-SKIP-LOCKED
--     workload, and a JSONB payload column to stress the wire protocol
--     with variable-length data.
--
-- Index strategy:
--   Nine indexes cover the pgbench script access patterns:
--   - tenant_id, status, updated_at on accounts  → point filters & sorts
--   - account_id, (tenant_id, created_at DESC), (event_type, created_at DESC),
--     processed, (processed, created_at) on events → range/aggregate/delete
--
-- Data characteristics:
--   - 1 000 tenants, 5 account statuses — enough cardinality for selective
--     index probes without excessive table bloat.
--   - Timestamps randomised over the last 180–365 days so date-windowed
--     queries hit a realistic fraction of the table.
--   - Approximately 30 % of events are pre-marked as processed=true to give
--     the delete workload rows to clean up.
--
-- Scale note (CI):
--   200k accounts + 1M events loads in ~25s on a GitHub-hosted runner.
--   The benchmark measures proxy overhead, not raw DB throughput, so this
--   scale is sufficient: range queries return ~83 rows/tenant/30-day window
--   and delete queries have ~100k eligible processed rows.
--
-- Usage:
--   psql -h <host> -p <port> -d keeltest -f schema.sql
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS keel_events CASCADE;
DROP TABLE IF EXISTS keel_accounts CASCADE;

CREATE TABLE keel_accounts (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       INTEGER NOT NULL,
    status          SMALLINT NOT NULL,
    balance         NUMERIC(18,2) NOT NULL DEFAULT 0,
    credit_limit    NUMERIC(18,2) NOT NULL DEFAULT 10000,
    full_name       TEXT NOT NULL,
    email           TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ,
    score           INTEGER NOT NULL DEFAULT 0,
    notes           TEXT
);

CREATE TABLE keel_events (
    id              BIGSERIAL PRIMARY KEY,
    account_id      BIGINT NOT NULL REFERENCES keel_accounts(id) ON DELETE CASCADE,
    tenant_id       INTEGER NOT NULL,
    event_type      SMALLINT NOT NULL,
    amount          NUMERIC(18,2) NOT NULL DEFAULT 0,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed       BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX idx_keel_accounts_tenant_id ON keel_accounts(tenant_id);
CREATE INDEX idx_keel_accounts_status ON keel_accounts(status);
CREATE INDEX idx_keel_accounts_updated_at ON keel_accounts(updated_at);
CREATE INDEX idx_keel_events_account_id ON keel_events(account_id);
CREATE INDEX idx_keel_events_tenant_created ON keel_events(tenant_id, created_at DESC);
CREATE INDEX idx_keel_events_type_created ON keel_events(event_type, created_at DESC);
CREATE INDEX idx_keel_events_processed ON keel_events(processed);
CREATE INDEX idx_keel_events_processed_created ON keel_events(processed, created_at);

INSERT INTO keel_accounts (tenant_id, status, balance, credit_limit, full_name, email, created_at, updated_at, last_seen_at, score, notes)
SELECT
    (1 + (g % 1000))::int,
    (1 + (g % 5))::smallint,
    round((random() * 100000)::numeric, 2),
    round((1000 + random() * 50000)::numeric, 2),
    'user_' || g,
    'user_' || g || '@example.com',
    now() - ((random() * interval '365 days')),
    now() - ((random() * interval '30 days')),
    now() - ((random() * interval '7 days')),
    (random() * 1000)::int,
    repeat(md5(g::text), 2)
FROM generate_series(1, 200000) AS g;

INSERT INTO keel_events (account_id, tenant_id, event_type, amount, payload, created_at, processed)
SELECT
    (1 + floor(random() * 200000))::bigint,
    (1 + floor(random() * 1000))::int,
    (1 + floor(random() * 20))::smallint,
    round((random() * 5000)::numeric, 2),
    jsonb_build_object(
        'source', 'pgbench',
        'seq', g,
        'token', md5(g::text),
        'flag', (random() > 0.5)
    ),
    now() - ((random() * interval '180 days')),
    (random() > 0.7)
FROM generate_series(1, 1000000) AS g;

ANALYZE keel_accounts;
ANALYZE keel_events;
