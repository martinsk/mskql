-- BUG: HAVING with NULLIF wrapping aggregate fails with 'expected comparison operator in WHERE'
-- setup:
CREATE TABLE t (grp TEXT, val INT);
INSERT INTO t VALUES ('A', 10), ('A', 20), ('B', 0), ('B', 0);
-- input:
SELECT grp, SUM(val) FROM t GROUP BY grp HAVING NULLIF(SUM(val), 0) IS NOT NULL ORDER BY grp;
-- expected output:
A|30
-- expected status: 0
