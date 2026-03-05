-- Test vectorized VEC_LIT_OP_COL: lit - col, lit / col, lit % col
CREATE TABLE t_loc (a INT, b BIGINT, c FLOAT);
INSERT INTO t_loc VALUES (2, 10, 2.5);
INSERT INTO t_loc VALUES (3, 20, 5.0);
INSERT INTO t_loc VALUES (5, 30, 10.0);

-- lit - col (INT)
SELECT 100 - a FROM t_loc ORDER BY a;
-- expected: 98
-- expected: 97
-- expected: 95

-- lit / col (INT)
SELECT 100 / a FROM t_loc ORDER BY a;
-- expected: 50
-- expected: 33
-- expected: 20

-- lit % col (INT)
SELECT 100 % a FROM t_loc ORDER BY a;
-- expected: 0
-- expected: 1
-- expected: 0

-- lit - col (BIGINT)
SELECT 100 - b FROM t_loc ORDER BY b;
-- expected: 90
-- expected: 80
-- expected: 70

-- lit / col (FLOAT)
SELECT 100.0 / c FROM t_loc ORDER BY c;
-- expected: 40
-- expected: 20
-- expected: 10
