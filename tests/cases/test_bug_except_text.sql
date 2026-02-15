-- bug: EXCEPT on TEXT columns returns empty instead of removing matching rows
-- setup:
CREATE TABLE t_ex (val TEXT);
INSERT INTO t_ex VALUES ('a'), ('b'), ('c');
-- input:
SELECT val FROM t_ex WHERE val IN ('a', 'b') EXCEPT SELECT val FROM t_ex WHERE val IN ('b', 'c');
-- expected output:
a
