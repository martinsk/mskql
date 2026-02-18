-- table-level PRIMARY KEY (a, b)
-- setup:
CREATE TABLE t_tpk (a INT, b INT, c TEXT, PRIMARY KEY (a, b));
INSERT INTO t_tpk VALUES (1, 2, 'hello');
INSERT INTO t_tpk VALUES (1, 3, 'world');
INSERT INTO t_tpk VALUES (2, 2, 'foo');
-- input:
SELECT c FROM t_tpk WHERE a = 1 AND b = 3;
-- expected output:
world
