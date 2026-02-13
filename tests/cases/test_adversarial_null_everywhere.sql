-- adversarial: NULL in every position — aggregates, functions, arithmetic, comparisons
-- setup:
CREATE TABLE t_nulls (a INT, b TEXT, c FLOAT);
INSERT INTO t_nulls VALUES (NULL, NULL, NULL);
INSERT INTO t_nulls VALUES (NULL, NULL, NULL);
-- input:
SELECT COUNT(*), COUNT(a), SUM(a), AVG(a), MIN(a), MAX(a) FROM t_nulls;
-- expected output:
2|0||||
