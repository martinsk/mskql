-- bug: LIKE operator should work as expression in SELECT list
-- setup:
CREATE TABLE t_like_expr (id INT, name TEXT);
INSERT INTO t_like_expr VALUES (1, 'hello'), (2, 'world'), (3, 'help');
-- input:
SELECT id, name LIKE 'hel%' AS matches FROM t_like_expr ORDER BY id;
-- expected output:
1|t
2|f
3|t
-- expected status: 0
