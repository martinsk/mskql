-- Test vectorized MOD and POWER projection
-- setup:
CREATE TABLE t (id INT, ival INT, fval FLOAT);
INSERT INTO t VALUES (1, 17, 2.0);
INSERT INTO t VALUES (2, 10, 3.0);
INSERT INTO t VALUES (3, 7, 0.5);
-- input:
SELECT MOD(ival, 5), POWER(fval, 2) FROM t ORDER BY id;
-- expected output:
2|4
0|9
2|0.25
