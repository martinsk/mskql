-- GROUP BY multiple columns where one group key is NULL
-- setup:
CREATE TABLE t1 (a TEXT, b TEXT, val INT);
INSERT INTO t1 (a, b, val) VALUES ('x', 'y', 1), ('x', NULL, 2), ('x', NULL, 3), ('x', 'y', 4);
-- input:
SELECT a, b, SUM(val) FROM t1 GROUP BY a, b ORDER BY a, b;
-- expected output:
x|y|5
x||5
-- expected status: 0
