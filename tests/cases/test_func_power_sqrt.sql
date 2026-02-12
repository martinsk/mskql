-- POWER and SQRT functions
-- setup:
CREATE TABLE t1 (id INT, val FLOAT);
INSERT INTO t1 VALUES (1, 4.0), (2, 9.0), (3, 2.0);
-- input:
SELECT id, POWER(val, 2), SQRT(val) FROM t1 ORDER BY id;
-- expected output:
1|16|2
2|81|3
3|4|1.41421
-- expected status: 0
