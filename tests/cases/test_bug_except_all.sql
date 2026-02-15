-- bug: EXCEPT ALL removes all copies instead of one per matching row
-- setup:
CREATE TABLE t_ea1 (val INT);
CREATE TABLE t_ea2 (val INT);
INSERT INTO t_ea1 VALUES (1), (1), (2), (3);
INSERT INTO t_ea2 VALUES (1), (3);
-- input:
SELECT val FROM t_ea1 EXCEPT ALL SELECT val FROM t_ea2 ORDER BY val;
-- expected output:
1
2
