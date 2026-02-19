-- bug: BETWEEN operator should work as expression in SELECT list
-- setup:
CREATE TABLE t_between_expr (id INT, val INT);
INSERT INTO t_between_expr VALUES (1, 5), (2, 15), (3, 25);
-- input:
SELECT id, val BETWEEN 10 AND 20 AS in_range FROM t_between_expr ORDER BY id;
-- expected output:
1|f
2|t
3|f
-- expected status: 0
