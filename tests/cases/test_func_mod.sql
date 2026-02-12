-- MOD function
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 VALUES (1, 10, 3), (2, 7, 2), (3, 15, 5);
-- input:
SELECT id, MOD(a, b) FROM t1 ORDER BY id;
-- expected output:
1|1
2|1
3|0
-- expected status: 0
