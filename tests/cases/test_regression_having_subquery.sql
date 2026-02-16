-- bug: HAVING with subquery does not filter groups correctly
-- setup:
CREATE TABLE t_sh (grp TEXT, val INT);
INSERT INTO t_sh VALUES ('a', 10), ('a', 20), ('b', 5), ('c', 100);
-- input:
SELECT grp, SUM(val) as total FROM t_sh GROUP BY grp HAVING SUM(val) > (SELECT AVG(val) FROM t_sh) ORDER BY grp;
-- expected output:
c|100
