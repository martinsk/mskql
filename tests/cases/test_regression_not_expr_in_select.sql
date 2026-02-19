-- bug: NOT expression fails in SELECT list ('expected expression in SELECT column list')
-- setup:
CREATE TABLE t_not_expr (id INT, active BOOLEAN);
INSERT INTO t_not_expr VALUES (1, true), (2, false), (3, NULL);
-- input:
SELECT id, NOT active AS inactive FROM t_not_expr ORDER BY id;
-- expected output:
1|f
2|t
3|
-- expected status: 0
