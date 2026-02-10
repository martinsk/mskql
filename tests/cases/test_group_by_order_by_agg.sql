-- GROUP BY with ORDER BY on aggregate column
-- setup:
CREATE TABLE t1 (dept TEXT, salary INT);
INSERT INTO t1 (dept, salary) VALUES ('eng', 300), ('sales', 100), ('hr', 200);
-- input:
SELECT dept, SUM(salary) FROM t1 GROUP BY dept ORDER BY sum;
-- expected output:
sales|100
hr|200
eng|300
-- expected status: 0
