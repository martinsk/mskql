-- LEFT and RIGHT functions
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'hello'), (2, 'world');
-- input:
SELECT id, LEFT(name, 3), RIGHT(name, 3) FROM t1 ORDER BY id;
-- expected output:
1|hel|llo
2|wor|rld
-- expected status: 0
