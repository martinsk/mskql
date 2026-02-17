-- INTERSECT should deduplicate (unlike INTERSECT ALL)
-- setup:
CREATE TABLE t1 (val INT);
INSERT INTO t1 VALUES (1),(2),(2),(3),(3),(3);
CREATE TABLE t2 (val INT);
INSERT INTO t2 VALUES (2),(3),(4);
-- input:
SELECT val FROM t1 INTERSECT SELECT val FROM t2 ORDER BY val;
-- expected output:
2
3
-- expected status: 0
