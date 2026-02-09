-- GROUP BY with COUNT(*) should count all rows including those with NULLs
-- setup:
CREATE TABLE t1 (dept TEXT, name TEXT);
INSERT INTO t1 (dept, name) VALUES ('eng', 'alice'), ('eng', NULL), ('sales', 'bob');
-- input:
SELECT dept, COUNT(*) FROM t1 GROUP BY dept ORDER BY dept;
-- expected output:
eng|2
sales|1
-- expected status: 0
