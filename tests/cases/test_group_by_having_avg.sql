-- GROUP BY with HAVING on AVG aggregate
-- setup:
CREATE TABLE t1 (dept TEXT, salary INT);
INSERT INTO t1 (dept, salary) VALUES ('eng', 100), ('eng', 200), ('sales', 50), ('sales', 60);
-- input:
SELECT dept, AVG(salary) FROM t1 GROUP BY dept HAVING avg > 100 ORDER BY dept;
-- expected output:
eng|150
-- expected status: 0
