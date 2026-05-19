-- §15.5 Shadow correctness workload: DDL followed by prepared-statement usage
--
-- Verifies that KEEL handles CREATE TABLE / DROP TABLE correctly in the same
-- session as a PREPARE / EXECUTE sequence.  If the backend state (e.g.
-- search_path or cached plan invalidation) is not managed properly, the proxy
-- output will differ from the direct-PG output.

CREATE TABLE keel_shadow_ddl_ps (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO keel_shadow_ddl_ps(name) VALUES ('alice'), ('bob'), ('carol');

PREPARE fetch_user(text) AS
    SELECT id, name FROM keel_shadow_ddl_ps WHERE name = $1;

EXECUTE fetch_user('alice');
EXECUTE fetch_user('bob');
EXECUTE fetch_user('carol');
EXECUTE fetch_user('nobody');

DEALLOCATE fetch_user;

-- Schema change: add a column, re-prepare, verify new plan
ALTER TABLE keel_shadow_ddl_ps ADD COLUMN score INT DEFAULT 0;

PREPARE fetch_with_score(text) AS
    SELECT id, name, score FROM keel_shadow_ddl_ps WHERE name = $1;

EXECUTE fetch_with_score('alice');
EXECUTE fetch_with_score('bob');

DEALLOCATE fetch_with_score;

DROP TABLE keel_shadow_ddl_ps;
