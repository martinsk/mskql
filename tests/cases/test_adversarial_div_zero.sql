-- adversarial: division by zero should not crash
-- setup:
CREATE TABLE t_div (a INT, b INT);
INSERT INTO t_div VALUES (10, 0);
INSERT INTO t_div VALUES (42, 0);
INSERT INTO t_div VALUES (7, 2);
-- input:
SELECT a / b FROM t_div;
-- expected output:
0
0
3
