-- §15.5 Shadow correctness workload: large result sets
--
-- Verifies that KEEL correctly forwards multi-packet responses without
-- dropping or duplicating rows.  Large row counts force the backend to
-- split the DataRow stream across multiple TCP segments.

-- 10 000-row result: one integer column
SELECT i
FROM generate_series(1, 10000) AS g(i)
ORDER BY i;

-- 1 000-row result: multiple columns, variable-width text
SELECT
    i,
    repeat('x', (i % 64) + 1) AS padding,
    md5(i::text)               AS digest
FROM generate_series(1, 1000) AS g(i)
ORDER BY i;

-- Wide row: 40 columns
SELECT
    1  AS c01, 2  AS c02, 3  AS c03, 4  AS c04, 5  AS c05,
    6  AS c06, 7  AS c07, 8  AS c08, 9  AS c09, 10 AS c10,
    11 AS c11, 12 AS c12, 13 AS c13, 14 AS c14, 15 AS c15,
    16 AS c16, 17 AS c17, 18 AS c18, 19 AS c19, 20 AS c20,
    21 AS c21, 22 AS c22, 23 AS c23, 24 AS c24, 25 AS c25,
    26 AS c26, 27 AS c27, 28 AS c28, 29 AS c29, 30 AS c30,
    31 AS c31, 32 AS c32, 33 AS c33, 34 AS c34, 35 AS c35,
    36 AS c36, 37 AS c37, 38 AS c38, 39 AS c39, 40 AS c40
FROM generate_series(1, 500) AS g(i)
ORDER BY c01;
