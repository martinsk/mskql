-- GROUP BY HAVING COUNT(col) = 0 should return groups where all values are NULL
-- setup:
CREATE TABLE t1 (dept TEXT, val INT);
INSERT INTO t1 (dept, val) VALUES ('eng', 10), ('eng', 20), ('sales', NULL), ('sales', NULL);
-- input:
SELECT dept, COUNT(val) FROM t1 GROUP BY dept HAVING COUNT(val) = 0;
-- expected output:
sales|0
-- expected status: 0
