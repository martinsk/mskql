-- Test vectorized unary NEG and NOT
CREATE TABLE t_unary (a INT, b BIGINT, c FLOAT, d BOOLEAN, e SMALLINT);
INSERT INTO t_unary VALUES (5, 100, 3.14, true, 7);
INSERT INTO t_unary VALUES (-3, -50, -2.5, false, -4);
INSERT INTO t_unary VALUES (0, 0, 0.0, true, 0);

-- Negate INT
SELECT -a FROM t_unary ORDER BY a;
-- expected: 3
-- expected: 0
-- expected: -5

-- Negate BIGINT
SELECT -b FROM t_unary ORDER BY a;
-- expected: 50
-- expected: 0
-- expected: -100

-- Negate FLOAT
SELECT -c FROM t_unary ORDER BY a;
-- expected: 2.5
-- expected: 0
-- expected: -3.14

-- Negate SMALLINT
SELECT -e FROM t_unary ORDER BY a;
-- expected: 4
-- expected: 0
-- expected: -7

-- NOT BOOLEAN
SELECT NOT d FROM t_unary ORDER BY a;
-- expected: t
-- expected: f
-- expected: f
