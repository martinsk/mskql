-- CTE with aggregate in the CTE body
-- setup:
CREATE TABLE t1 (dept TEXT, salary INT);
INSERT INTO t1 (dept, salary) VALUES ('eng', 100), ('eng', 200), ('sales', 150);
-- input:
WITH dept_totals AS (SELECT dept, SUM(salary) AS total FROM t1 GROUP BY dept) SELECT dept, total FROM dept_totals ORDER BY dept;
-- expected output:
eng|300
sales|150
-- expected status: 0
