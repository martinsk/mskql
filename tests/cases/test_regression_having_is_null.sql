-- BUG: HAVING with IS NULL check on aggregate fails with 'expected comparison operator in WHERE'
-- setup:
CREATE TABLE t (grp TEXT, val INT);
INSERT INTO t VALUES ('A', 10), ('A', 20), ('B', NULL), ('B', NULL);
-- input:
SELECT grp, SUM(val) FROM t GROUP BY grp HAVING SUM(val) IS NULL ORDER BY grp;
-- expected output:
B|
-- expected status: 0
