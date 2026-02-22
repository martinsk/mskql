-- Test vectorized SQRT and SIGN projection
-- setup:
CREATE TABLE t (id INT, val FLOAT, ival INT);
INSERT INTO t VALUES (1, 4.0, 5);
INSERT INTO t VALUES (2, 9.0, -3);
INSERT INTO t VALUES (3, 16.0, 0);
-- input:
SELECT SQRT(val), SIGN(ival) FROM t ORDER BY id;
-- expected output:
2|1
3|-1
4|0
