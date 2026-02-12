-- POSITION function
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'hello world'), (2, 'foobar'), (3, 'abcabc');
-- input:
SELECT id, POSITION('lo' IN name), POSITION('xyz' IN name) FROM t1 ORDER BY id;
-- expected output:
1|4|0
2|0|0
3|0|0
-- expected status: 0
