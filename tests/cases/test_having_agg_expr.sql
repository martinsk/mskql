-- HAVING with aggregate expression: HAVING COUNT(*) > 1
-- The HAVING condition parser expects a column name, not an aggregate function call
-- setup:
CREATE TABLE t1 (dept TEXT, name TEXT);
INSERT INTO t1 (dept, name) VALUES ('eng', 'alice'), ('eng', 'bob'), ('sales', 'carol');
-- input:
SELECT dept, COUNT(*) FROM t1 GROUP BY dept HAVING COUNT(*) > 1;
-- expected output:
eng|2
-- expected status: 0
