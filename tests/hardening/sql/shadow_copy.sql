-- §15.5 Shadow correctness workload: COPY TO STDOUT
--
-- COPY uses a distinct sub-protocol (CopyOutResponse / CopyData / CopyDone).
-- KEEL must forward the byte stream without modification.  The diff between
-- direct-PG output and proxy output must be empty.
--
-- Note: hardening-shadow-diff.sh uses psql and captures stdout, so COPY TO
-- STDOUT naturally lands in the output file for diffing.

CREATE TABLE keel_shadow_copy (
    id   INT,
    val  TEXT
);

INSERT INTO keel_shadow_copy
SELECT i, 'row_' || i::text
FROM generate_series(1, 200) AS g(i);

COPY (SELECT id, val FROM keel_shadow_copy ORDER BY id) TO STDOUT WITH (FORMAT text);

COPY (SELECT id, val FROM keel_shadow_copy ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER);

DROP TABLE keel_shadow_copy;
