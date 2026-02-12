-- SIGN function
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, -5), (2, 0), (3, 42);
-- input:
SELECT id, SIGN(val) FROM t1 ORDER BY id;
-- expected output:
1|-1
2|0
3|1
-- expected status: 0
