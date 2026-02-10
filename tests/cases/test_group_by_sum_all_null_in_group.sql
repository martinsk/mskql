-- GROUP BY SUM where all values in a group are NULL should return NULL for that group (not 0)
-- setup:
CREATE TABLE t1 (dept TEXT, val INT);
INSERT INTO t1 (dept, val) VALUES ('a', 10), ('a', 20), ('b', NULL), ('b', NULL);
-- input:
SELECT dept, SUM(val) FROM t1 GROUP BY dept ORDER BY dept;
-- expected output:
a|30
b|
-- expected status: 0
