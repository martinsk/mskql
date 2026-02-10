-- GROUP BY with ORDER BY aggregate DESC
-- setup:
CREATE TABLE t1 (dept TEXT, salary INT);
INSERT INTO t1 (dept, salary) VALUES ('eng', 300), ('sales', 100), ('hr', 200);
-- input:
SELECT dept, SUM(salary) FROM t1 GROUP BY dept ORDER BY sum DESC;
-- expected output:
eng|300
hr|200
sales|100
-- expected status: 0
