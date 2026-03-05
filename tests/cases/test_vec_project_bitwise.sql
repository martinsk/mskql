-- Test vectorized bitwise ops
CREATE TABLE t_bit (a INT, b INT);
INSERT INTO t_bit VALUES (15, 9);
INSERT INTO t_bit VALUES (255, 240);
INSERT INTO t_bit VALUES (7, 3);

-- col & lit
SELECT a & 12 FROM t_bit ORDER BY a;
-- expected: 4
-- expected: 12
-- expected: 12

-- col | lit
SELECT a | 16 FROM t_bit ORDER BY a;
-- expected: 23
-- expected: 31
-- expected: 255

-- col << lit
SELECT a << 1 FROM t_bit ORDER BY a;
-- expected: 14
-- expected: 30
-- expected: 510

-- col >> lit
SELECT a >> 1 FROM t_bit ORDER BY a;
-- expected: 3
-- expected: 7
-- expected: 127

-- col & col
SELECT a & b FROM t_bit ORDER BY a;
-- expected: 3
-- expected: 9
-- expected: 240

-- col | col
SELECT a | b FROM t_bit ORDER BY a;
-- expected: 7
-- expected: 15
-- expected: 255
