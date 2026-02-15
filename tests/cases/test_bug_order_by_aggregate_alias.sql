-- bug: ORDER BY on aggregate alias is ignored (results unsorted)
-- setup:
CREATE TABLE t_oaa (grp TEXT, val INT);
INSERT INTO t_oaa VALUES ('a', 30), ('b', 10), ('c', 50), ('a', 40), ('b', 20);
-- input:
SELECT grp, SUM(val) as total FROM t_oaa GROUP BY grp ORDER BY total;
-- expected output:
b|30
c|50
a|70
