-- MIN(expr) with GROUP BY
-- setup:
CREATE TABLE t1 (dept TEXT, a INT, b INT);
INSERT INTO t1 VALUES ('eng', 10, 5);
INSERT INTO t1 VALUES ('eng', 3, 2);
INSERT INTO t1 VALUES ('sales', 20, 1);
INSERT INTO t1 VALUES ('sales', 4, 3);
-- input:
SELECT dept, MIN(a + b) FROM t1 GROUP BY dept ORDER BY dept;
-- expected output:
eng|5
sales|7
-- expected status: 0
