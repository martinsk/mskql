-- parser: aggregate queries populate parsed_columns
-- setup:
CREATE TABLE t_agg_pc (cat TEXT, val INT, score INT);
INSERT INTO t_agg_pc VALUES ('a', 1, 10), ('a', 2, 20), ('b', 3, 30);
-- input:
SELECT cat, SUM(val), AVG(score), COUNT(*) FROM t_agg_pc GROUP BY cat ORDER BY cat
-- expected output:
a|3|15|2
b|3|30|1
