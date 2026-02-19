-- bug: IN operator should work as expression in SELECT list
-- setup:
CREATE TABLE t_in_expr (id INT, val INT);
INSERT INTO t_in_expr VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, val IN (10, 30) AS in_set FROM t_in_expr ORDER BY id;
-- expected output:
1|t
2|f
3|t
-- expected status: 0
