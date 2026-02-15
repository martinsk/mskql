-- regression: INSERT...SELECT into SERIAL table auto-assigns ids
-- setup:
CREATE TABLE t_src (x TEXT);
INSERT INTO t_src VALUES ('c'), ('d');
CREATE TABLE t_dst (id SERIAL, val TEXT);
INSERT INTO t_dst (val) VALUES ('a'), ('b');
-- input:
INSERT INTO t_dst (val) SELECT x FROM t_src;
SELECT id, val FROM t_dst ORDER BY id;
-- expected output:
INSERT 0 2
1|a
2|b
3|c
4|d
