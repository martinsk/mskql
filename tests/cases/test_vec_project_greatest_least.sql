-- Test vectorized GREATEST/LEAST
CREATE TABLE t_gl (a INT, b BIGINT, c FLOAT);
INSERT INTO t_gl VALUES (-5, -100, -2.5);
INSERT INTO t_gl VALUES (3, 50, 1.5);
INSERT INTO t_gl VALUES (10, 200, 7.0);

-- GREATEST(col, lit)
SELECT GREATEST(a, 0) FROM t_gl ORDER BY a;
-- expected: 0
-- expected: 3
-- expected: 10

SELECT GREATEST(b, 0) FROM t_gl ORDER BY b;
-- expected: 0
-- expected: 50
-- expected: 200

SELECT GREATEST(c, 0.0) FROM t_gl ORDER BY c;
-- expected: 0
-- expected: 1.5
-- expected: 7

-- LEAST(col, lit)
SELECT LEAST(a, 5) FROM t_gl ORDER BY a;
-- expected: -5
-- expected: 3
-- expected: 5

SELECT LEAST(c, 5.0) FROM t_gl ORDER BY c;
-- expected: -2.5
-- expected: 1.5
-- expected: 5
