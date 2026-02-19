-- bug: comparison operators (>, <, >=, <=, <>) should work as expressions in SELECT list
-- setup:
CREATE TABLE t_cmp_expr (id INT, val INT);
INSERT INTO t_cmp_expr VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, val > 15 AS gt15, val < 25 AS lt25, val >= 20 AS gte20, val <= 20 AS lte20, val <> 20 AS ne20 FROM t_cmp_expr ORDER BY id;
-- expected output:
1|f|t|f|t|t
2|t|t|t|t|f
3|t|f|t|f|t
-- expected status: 0
