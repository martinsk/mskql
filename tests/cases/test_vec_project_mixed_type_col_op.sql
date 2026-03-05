-- Test vectorized mixed-type col OP col
CREATE TABLE t_mix (a INT, b BIGINT, c FLOAT, d SMALLINT);
INSERT INTO t_mix VALUES (10, 100, 3.14, 5);
INSERT INTO t_mix VALUES (20, 200, 2.5, 3);
INSERT INTO t_mix VALUES (30, 300, 1.0, 7);

-- INT + BIGINT → BIGINT
SELECT a + b FROM t_mix ORDER BY a;
-- expected: 110
-- expected: 220
-- expected: 330

-- INT * FLOAT → FLOAT
SELECT a * c FROM t_mix ORDER BY a;
-- expected: 31.4
-- expected: 50
-- expected: 30

-- SMALLINT + INT → INT
SELECT d + a FROM t_mix ORDER BY a;
-- expected: 15
-- expected: 23
-- expected: 37

-- INT - BIGINT → BIGINT
SELECT a - b FROM t_mix ORDER BY a;
-- expected: -90
-- expected: -180
-- expected: -270
