-- §15.5 Shadow correctness workload: SET search_path multi-tenant routing
--
-- Verifies that KEEL correctly replays SET search_path during state sync when
-- a backend borrowed from the pool has a different search_path.  If the replay
-- fails the wrong schema is queried.
--
-- Run against a database that has (at minimum) the 'public' schema.

-- Baseline: default search_path
SET search_path TO public;
SELECT current_setting('search_path');

-- Switch to a different search_path (empty schema list falls back to public)
SET search_path TO pg_catalog, public;
SELECT current_setting('search_path');

SELECT schemaname, tablename
FROM pg_tables
WHERE schemaname IN ('public', 'pg_catalog')
ORDER BY schemaname, tablename
LIMIT 10;

-- Reset
RESET search_path;
SELECT current_setting('search_path');

-- Verify that schema-qualified queries work correctly after reset
SELECT n.nspname AS schema, c.relname AS table
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
ORDER BY c.relname
LIMIT 5;
