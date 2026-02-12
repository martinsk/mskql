-- GROUP BY with HAVING that filters out all groups
-- setup:
CREATE TABLE t1 (grp TEXT, val INT);
INSERT INTO t1 VALUES ('a', 1), ('a', 2), ('b', 3);
-- input:
SELECT grp, SUM(val) FROM t1 GROUP BY grp HAVING SUM(val) > 100;
-- expected output:
-- expected status: 0
