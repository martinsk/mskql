-- plan: GROUP BY with mixed columns and aggregates (grp=1 agg=3 parsed=4)
-- setup:
CREATE TABLE t_mix (cat TEXT, val INT, score INT);
INSERT INTO t_mix VALUES ('a', 1, 10), ('a', 2, 20), ('b', 3, 30);
-- input:
SELECT cat, SUM(val), AVG(score), COUNT(*) FROM t_mix GROUP BY cat ORDER BY cat
-- expected output:
a|3|15|2
b|3|30|1
