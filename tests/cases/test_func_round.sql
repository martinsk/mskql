-- ROUND function
-- setup:
CREATE TABLE t1 (id INT, val FLOAT);
INSERT INTO t1 VALUES (1, 2.567), (2, -1.234), (3, 3.5);
-- input:
SELECT id, ROUND(val), ROUND(val, 2) FROM t1 ORDER BY id;
-- expected output:
1|3|2.57
2|-1|-1.23
3|4|3.5
-- expected status: 0
