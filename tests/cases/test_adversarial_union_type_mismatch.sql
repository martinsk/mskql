-- adversarial: UNION with different column types — potential type confusion
-- setup:
CREATE TABLE t_u1 (v INT);
CREATE TABLE t_u2 (v TEXT);
INSERT INTO t_u1 VALUES (42);
INSERT INTO t_u2 VALUES ('hello');
-- input:
SELECT v FROM t_u1 UNION ALL SELECT v FROM t_u2;
-- expected output:
42
hello
