-- bug: SELECT *, expr FROM table should work (comma after star)
-- setup:
CREATE TABLE t_star_expr (id INT, a INT, b TEXT);
INSERT INTO t_star_expr VALUES (1, 10, 'x'), (2, 20, 'y');
-- input:
SELECT *, a * 2 AS doubled FROM t_star_expr ORDER BY id;
-- expected output:
1|10|x|20
2|20|y|40
-- expected status: 0
