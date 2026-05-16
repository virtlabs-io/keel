-- =============================================================================
-- seed.sql — Optional data expansion for the Keel benchmark schema
-- =============================================================================
--
-- Adds 100 000 extra accounts and 1 000 000 extra events to the tables
-- already created by schema.sql.  Run this when you need a larger working
-- set (e.g. to push buffer-pool hit ratios down and stress I/O).
--
-- The inserts follow the same distribution and column patterns as schema.sql
-- so that query plans remain stable after expansion.  Account names are
-- prefixed with "extra_user_" to distinguish them from the base data set.
--
-- Usage:
--   psql -h <host> -p <port> -d keeltest -f seed.sql
-- =============================================================================

-- Optional helper inserts if you want to expand the data set later.

INSERT INTO keel_accounts (tenant_id, status, balance, credit_limit, full_name, email, created_at, updated_at, last_seen_at, score, notes)
SELECT
    (1 + (g % 1000))::int,
    (1 + (g % 5))::smallint,
    round((random() * 100000)::numeric, 2),
    round((1000 + random() * 50000)::numeric, 2),
    'extra_user_' || g,
    'extra_user_' || g || '@example.com',
    now() - ((random() * interval '365 days')),
    now() - ((random() * interval '30 days')),
    now() - ((random() * interval '7 days')),
    (random() * 1000)::int,
    repeat(md5((1000000 + g)::text), 2)
FROM generate_series(1, 100000) AS g;

INSERT INTO keel_events (account_id, tenant_id, event_type, amount, payload, created_at, processed)
SELECT
    (1 + floor(random() * (SELECT max(id) FROM keel_accounts)))::bigint,
    (1 + floor(random() * 1000))::int,
    (1 + floor(random() * 20))::smallint,
    round((random() * 5000)::numeric, 2),
    jsonb_build_object('source', 'seed', 'seq', g, 'token', md5((2000000 + g)::text)),
    now() - ((random() * interval '180 days')),
    (random() > 0.7)
FROM generate_series(1, 1000000) AS g;

ANALYZE keel_accounts;
ANALYZE keel_events;
