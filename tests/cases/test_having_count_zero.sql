-- HAVING COUNT(*) = 0 should return no groups (every group has at least 1 row)
-- setup:
CREATE TABLE t1 (dept TEXT, name TEXT);
INSERT INTO t1 (dept, name) VALUES ('eng', 'alice'), ('sales', 'bob');
-- input:
SELECT dept, COUNT(*) FROM t1 GROUP BY dept HAVING count = 0;
-- expected output:
-- expected status: 0
