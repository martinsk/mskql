-- GROUP BY MIN where all values in a group are NULL should return NULL (not 0)
-- setup:
CREATE TABLE t1 (dept TEXT, val INT);
INSERT INTO t1 (dept, val) VALUES ('a', 10), ('a', 20), ('b', NULL), ('b', NULL);
-- input:
SELECT dept, MIN(val) FROM t1 GROUP BY dept ORDER BY dept;
-- expected output:
a|10
b|
-- expected status: 0
