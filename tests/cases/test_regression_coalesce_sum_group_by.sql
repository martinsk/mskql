-- bug: COALESCE(SUM(val), 0) with GROUP BY returns empty instead of computed values
-- setup:
CREATE TABLE t_coalesce_grp (id INT, grp TEXT, val INT);
INSERT INTO t_coalesce_grp VALUES (1,'a',10),(2,'a',20),(3,'b',NULL),(4,'b',NULL);
-- input:
SELECT grp, COALESCE(SUM(val), 0) AS total FROM t_coalesce_grp GROUP BY grp ORDER BY grp;
-- expected output:
a|30
b|0
-- expected status: 0
