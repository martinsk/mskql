-- GROUP BY with HAVING COUNT(*) filter
-- setup:
CREATE TABLE t1 (dept TEXT, name TEXT);
INSERT INTO t1 (dept, name) VALUES ('eng', 'a'), ('eng', 'b'), ('eng', 'c'), ('sales', 'x');
-- input:
SELECT dept, COUNT(*) FROM t1 GROUP BY dept HAVING count > 1 ORDER BY dept;
-- expected output:
eng|3
-- expected status: 0
