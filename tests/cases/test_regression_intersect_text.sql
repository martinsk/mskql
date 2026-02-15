-- regression: INTERSECT works on TEXT columns
-- setup:
CREATE TABLE t_a (v TEXT);
CREATE TABLE t_b (v TEXT);
INSERT INTO t_a VALUES ('a'),('b'),('c');
INSERT INTO t_b VALUES ('b'),('c'),('d');
-- input:
SELECT v FROM t_a INTERSECT SELECT v FROM t_b ORDER BY v;
-- expected output:
b
c
