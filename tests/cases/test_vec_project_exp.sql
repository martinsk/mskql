-- Test vectorized OP_EXP (^ operator)
CREATE TABLE t_exp (a INT, b FLOAT);
INSERT INTO t_exp VALUES (2, 2.0);
INSERT INTO t_exp VALUES (3, 3.0);
INSERT INTO t_exp VALUES (5, 0.5);

-- INT ^ lit
SELECT a ^ 3 FROM t_exp ORDER BY a;
-- expected: 8
-- expected: 27
-- expected: 125

-- FLOAT ^ lit
SELECT b ^ 2.0 FROM t_exp ORDER BY a;
-- expected: 4
-- expected: 9
-- expected: 0.25
