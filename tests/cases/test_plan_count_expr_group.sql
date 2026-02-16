-- Test COUNT(expr) with GROUP BY through plan executor
-- setup:
CREATE TABLE t_ceg (category TEXT, val INT);
INSERT INTO t_ceg VALUES ('a', 10);
INSERT INTO t_ceg VALUES ('a', NULL);
INSERT INTO t_ceg VALUES ('a', 30);
INSERT INTO t_ceg VALUES ('b', NULL);
INSERT INTO t_ceg VALUES ('b', NULL);
INSERT INTO t_ceg VALUES ('b', 60);
-- input:
SELECT category, COUNT(val) FROM t_ceg GROUP BY category ORDER BY category;
-- expected output:
a|2
b|1
