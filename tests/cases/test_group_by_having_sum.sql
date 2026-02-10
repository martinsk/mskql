-- GROUP BY with HAVING on SUM aggregate
-- setup:
CREATE TABLE t1 (dept TEXT, salary INT);
INSERT INTO t1 (dept, salary) VALUES ('eng', 100), ('eng', 200), ('sales', 50), ('sales', 40);
-- input:
SELECT dept, SUM(salary) FROM t1 GROUP BY dept HAVING sum > 100 ORDER BY dept;
-- expected output:
eng|300
-- expected status: 0
