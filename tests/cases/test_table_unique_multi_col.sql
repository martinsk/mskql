-- table-level UNIQUE (a, b)
-- setup:
CREATE TABLE t_tuq (a INT, b INT, val TEXT, UNIQUE (a, b));
INSERT INTO t_tuq VALUES (1, 10, 'x');
INSERT INTO t_tuq VALUES (1, 20, 'y');
INSERT INTO t_tuq VALUES (2, 10, 'z');
-- input:
SELECT val FROM t_tuq WHERE a = 2 AND b = 10;
-- expected output:
z
