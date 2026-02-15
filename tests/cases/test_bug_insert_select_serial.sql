-- bug: INSERT...SELECT into SERIAL table should auto-assign ids
-- setup:
CREATE TABLE t_serial (id SERIAL, val TEXT);
INSERT INTO t_serial (val) VALUES ('a'), ('b');
CREATE TABLE t_src (x TEXT);
INSERT INTO t_src VALUES ('c'), ('d'), ('e');
-- input:
INSERT INTO t_serial (val) SELECT x FROM t_src;
SELECT id, val FROM t_serial ORDER BY id;
-- expected output:
INSERT 0 3
1|a
2|b
3|c
4|d
5|e
