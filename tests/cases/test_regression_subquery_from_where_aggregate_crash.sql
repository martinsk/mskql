-- BUG: SELECT from subquery with GROUP BY + aggregate, with outer WHERE, crashes server
-- setup:
CREATE TABLE t (grp TEXT, val INT);
INSERT INTO t VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40), ('C', 50);
-- input:
SELECT * FROM (SELECT grp, SUM(val) AS total FROM t GROUP BY grp) sub WHERE total > 30 ORDER BY grp;
-- expected output:
B|70
C|50
-- expected status: 0
