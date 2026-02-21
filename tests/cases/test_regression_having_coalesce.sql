-- BUG: HAVING with COALESCE wrapping aggregate returns no rows
-- setup:
CREATE TABLE t (grp TEXT, val INT);
INSERT INTO t VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40), ('C', 5);
-- input:
SELECT grp, SUM(val) FROM t GROUP BY grp HAVING COALESCE(SUM(val), 0) > 20 ORDER BY grp;
-- expected output:
A|30
B|70
-- expected status: 0
