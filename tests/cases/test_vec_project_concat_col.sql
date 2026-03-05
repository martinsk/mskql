-- Test vectorized text col || col
CREATE TABLE t_cat (a TEXT, b TEXT);
INSERT INTO t_cat VALUES ('hello', ' world');
INSERT INTO t_cat VALUES ('foo', 'bar');
INSERT INTO t_cat VALUES ('', 'empty');

SELECT a || b FROM t_cat ORDER BY a;
-- expected: empty
-- expected: foobar
-- expected: hello world
