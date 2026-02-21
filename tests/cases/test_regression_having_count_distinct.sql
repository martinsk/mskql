-- BUG: HAVING with COUNT(DISTINCT col) fails with parse error
-- setup:
CREATE TABLE t (grp TEXT, val INT);
INSERT INTO t VALUES ('A', 1), ('A', 1), ('A', 2), ('B', 3), ('B', 3);
-- input:
SELECT grp, COUNT(DISTINCT val) FROM t GROUP BY grp HAVING COUNT(DISTINCT val) > 1;
-- expected output:
A|2
-- expected status: 0
