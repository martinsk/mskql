-- regression: UNION ALL returns correct row count
-- setup:
CREATE TABLE t_a (v INT);
CREATE TABLE t_b (v INT);
INSERT INTO t_a VALUES (1),(2);
INSERT INTO t_b VALUES (3),(4);
-- input:
SELECT v FROM t_a UNION ALL SELECT v FROM t_b ORDER BY v;
-- expected output:
1
2
3
4
