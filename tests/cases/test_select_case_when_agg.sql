-- CASE WHEN used with aggregate result in GROUP BY query
-- setup:
CREATE TABLE t1 (dept TEXT, salary INT);
INSERT INTO t1 (dept, salary) VALUES ('eng', 100), ('eng', 200), ('sales', 50);
-- input:
SELECT dept, SUM(salary) FROM t1 GROUP BY dept ORDER BY dept;
-- expected output:
eng|300
sales|50
-- expected status: 0
