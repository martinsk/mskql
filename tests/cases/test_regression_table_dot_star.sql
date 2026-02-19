-- bug: table.* syntax should work in SELECT list
-- setup:
CREATE TABLE t_dot_star (id INT, val TEXT);
INSERT INTO t_dot_star VALUES (1, 'a'), (2, 'b');
-- input:
SELECT t_dot_star.* FROM t_dot_star ORDER BY id;
-- expected output:
1|a
2|b
-- expected status: 0
