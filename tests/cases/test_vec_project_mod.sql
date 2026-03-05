-- Test vectorized OP_MOD for col % lit and col % col
CREATE TABLE t_mod (a INT, b INT, c BIGINT, d FLOAT);
INSERT INTO t_mod VALUES (10, 3, 100, 7.5);
INSERT INTO t_mod VALUES (17, 5, 99, 3.2);
INSERT INTO t_mod VALUES (20, 7, 50, 2.0);

-- col % lit (INT)
SELECT a % 3 FROM t_mod ORDER BY a;
-- expected: 1
-- expected: 2
-- expected: 2

-- col % col (INT)
SELECT a % b FROM t_mod ORDER BY a;
-- expected: 1
-- expected: 2
-- expected: 6

-- col % lit (BIGINT)
SELECT c % 7 FROM t_mod ORDER BY c;
-- expected: 1
-- expected: 1
-- expected: 2

-- col % lit (FLOAT via fmod)
SELECT d % 2.0 FROM t_mod ORDER BY a;
-- expected: 1.5
-- expected: 1.2
-- expected: 0
