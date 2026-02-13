-- adversarial: aggregates on completely empty table
-- setup:
CREATE TABLE t_empty (a INT, b TEXT);
-- input:
SELECT COUNT(*), COUNT(a), SUM(a), AVG(a), MIN(a), MAX(a) FROM t_empty;
-- expected output:
0|0||||
