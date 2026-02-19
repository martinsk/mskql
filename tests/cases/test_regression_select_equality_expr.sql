-- bug: equality and inequality operators should work as expressions in SELECT list
-- setup:
CREATE TABLE t_eq_expr (id INT, val INT);
INSERT INTO t_eq_expr VALUES (1, 10), (2, 20), (3, 10);
-- input:
SELECT id, val = 10 AS is_ten FROM t_eq_expr ORDER BY id;
-- expected output:
1|t
2|f
3|t
-- expected status: 0
