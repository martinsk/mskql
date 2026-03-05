-- Test vectorized binary ops on SMALLINT columns
CREATE TABLE t_si (a SMALLINT, b SMALLINT);
INSERT INTO t_si VALUES (10, 3);
INSERT INTO t_si VALUES (20, 5);
INSERT INTO t_si VALUES (30, 7);

-- col + lit
SELECT a + 5 FROM t_si ORDER BY a;
-- expected: 15
-- expected: 25
-- expected: 35

-- col * col
SELECT a * b FROM t_si ORDER BY a;
-- expected: 30
-- expected: 100
-- expected: 210

-- col - lit
SELECT a - 2 FROM t_si ORDER BY a;
-- expected: 8
-- expected: 18
-- expected: 28

-- col % lit
SELECT a % 7 FROM t_si ORDER BY a;
-- expected: 3
-- expected: 6
-- expected: 2

-- col % col
SELECT a % b FROM t_si ORDER BY a;
-- expected: 1
-- expected: 0
-- expected: 2
