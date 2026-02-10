-- LENGTH on NULL should return NULL
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'hello'), (2, NULL), (3, '');
-- input:
SELECT id, LENGTH(name) FROM t1 ORDER BY id;
-- expected output:
1|5
2|
3|0
-- expected status: 0
