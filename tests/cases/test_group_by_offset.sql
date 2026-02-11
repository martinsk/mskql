-- GROUP BY with LIMIT and OFFSET
-- setup:
CREATE TABLE t1 (dept TEXT, salary INT);
INSERT INTO t1 (dept, salary) VALUES ('a', 10), ('b', 20), ('c', 30), ('d', 40);
-- input:
SELECT dept, SUM(salary) FROM t1 GROUP BY dept ORDER BY dept LIMIT 2 OFFSET 1;
-- expected output:
b|20
c|30
-- expected status: 0
