-- REPLACE function
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'hello world'), (2, 'foo bar foo');
-- input:
SELECT id, REPLACE(name, 'foo', 'baz') FROM t1 ORDER BY id;
-- expected output:
1|hello world
2|baz bar baz
-- expected status: 0
