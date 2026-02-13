-- adversarial: INSERT ... SELECT from empty table
-- setup:
CREATE TABLE t_src (id INT, name TEXT);
CREATE TABLE t_dst (id INT, name TEXT);
-- input:
INSERT INTO t_dst SELECT * FROM t_src;
SELECT COUNT(*) FROM t_dst;
-- expected output:
INSERT 0 0
0
