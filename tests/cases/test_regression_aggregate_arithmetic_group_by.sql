-- bug: arithmetic on aggregate results with GROUP BY fails (e.g. SUM(val) + 1)
-- setup:
CREATE TABLE t_agg_arith_gb (id INT, grp TEXT, val INT);
INSERT INTO t_agg_arith_gb VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
-- input:
SELECT grp, SUM(val) + 1 AS total_plus FROM t_agg_arith_gb GROUP BY grp ORDER BY grp;
-- expected output:
a|31
b|71
-- expected status: 0
