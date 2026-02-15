-- regression: ORDER BY aggregate alias
-- setup:
CREATE TABLE t (grp TEXT, val INT);
INSERT INTO t VALUES ('a',30),('b',10),('c',50),('a',40),('b',20);
-- input:
SELECT grp, SUM(val) as total FROM t GROUP BY grp ORDER BY total;
-- expected output:
b|30
c|50
a|70
