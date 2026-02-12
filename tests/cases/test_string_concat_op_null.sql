-- || operator with NULL operand returns NULL (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, a TEXT, b TEXT);
INSERT INTO t1 VALUES (1, 'hello', NULL), (2, NULL, 'world'), (3, 'foo', 'bar');
-- input:
SELECT id, a || b FROM t1 ORDER BY id;
-- expected output:
1|
2|
3|foobar
-- expected status: 0
