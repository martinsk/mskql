-- bug: CASE WHEN with aggregate in SELECT fails with "expected FROM"
-- setup:
CREATE TABLE t_cwa (grp TEXT, val INT);
INSERT INTO t_cwa VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 100);
-- input:
SELECT grp, CASE WHEN SUM(val) > 50 THEN 'high' ELSE 'low' END as label FROM t_cwa GROUP BY grp ORDER BY grp;
-- expected output:
a|low
b|high
