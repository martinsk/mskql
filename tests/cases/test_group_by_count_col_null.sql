-- GROUP BY with COUNT(col) should count only non-NULL values in each group
-- setup:
CREATE TABLE t1 (dept TEXT, val INT);
INSERT INTO t1 (dept, val) VALUES ('a', 10), ('a', NULL), ('a', 30), ('b', NULL), ('b', NULL);
-- input:
SELECT dept, COUNT(val) FROM t1 GROUP BY dept ORDER BY dept;
-- expected output:
a|2
b|0
-- expected status: 0
