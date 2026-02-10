-- UPPER on NULL should return NULL
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'hello'), (2, NULL);
-- input:
SELECT id, UPPER(name) FROM t1 ORDER BY id;
-- expected output:
1|HELLO
2|
-- expected status: 0
