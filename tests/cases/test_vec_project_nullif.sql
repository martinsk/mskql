-- Test vectorized NULLIF(col, lit)
CREATE TABLE t_nullif (a INT, b BIGINT, c FLOAT);
INSERT INTO t_nullif VALUES (1, 100, 3.14);
INSERT INTO t_nullif VALUES (0, 0, 0.0);
INSERT INTO t_nullif VALUES (5, 100, 3.14);

-- NULLIF returns NULL when col == lit
SELECT NULLIF(a, 0) FROM t_nullif ORDER BY a;
-- expected: (null for 0)
-- expected: 1
-- expected: 5

SELECT NULLIF(b, 100) FROM t_nullif ORDER BY a;
-- expected: (null for 100 rows)
-- expected: 0
-- expected: (null for 100 rows)
