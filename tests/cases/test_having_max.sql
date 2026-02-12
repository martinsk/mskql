-- HAVING with MAX aggregate
-- setup:
CREATE TABLE t1 (dept TEXT, salary INT);
INSERT INTO t1 VALUES ('eng', 100);
INSERT INTO t1 VALUES ('eng', 200);
INSERT INTO t1 VALUES ('sales', 50);
INSERT INTO t1 VALUES ('sales', 60);
-- input:
SELECT dept, MAX(salary) FROM t1 GROUP BY dept HAVING MAX(salary) > 100 ORDER BY dept;
-- expected output:
eng|200
-- expected status: 0
