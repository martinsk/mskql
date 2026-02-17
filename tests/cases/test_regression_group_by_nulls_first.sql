-- GROUP BY with ORDER BY ASC NULLS FIRST should put NULL group first
-- setup:
CREATE TABLE t (cat TEXT, val INT);
INSERT INTO t VALUES ('b', 20), (NULL, 30), ('a', 10);
-- input:
SELECT cat, SUM(val) FROM t GROUP BY cat ORDER BY cat ASC NULLS FIRST;
-- expected output:
|30
a|10
b|20
-- expected status: 0
