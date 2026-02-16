-- Test COUNT(expr) through plan executor
-- setup:
CREATE TABLE t_ce (id INT, val INT, name TEXT);
INSERT INTO t_ce VALUES (1, 10, 'a');
INSERT INTO t_ce VALUES (2, NULL, 'b');
INSERT INTO t_ce VALUES (3, 30, NULL);
INSERT INTO t_ce VALUES (4, NULL, NULL);
INSERT INTO t_ce VALUES (5, 50, 'e');
-- input:
SELECT COUNT(val) FROM t_ce;
-- expected output:
3
