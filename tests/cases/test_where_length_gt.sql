-- WHERE with LENGTH function: WHERE LENGTH(name) > 3
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'ab'), (2, 'abcd'), (3, 'abcdef');
-- input:
SELECT id FROM t1 WHERE LENGTH(name) > 3 ORDER BY id;
-- expected output:
2
3
-- expected status: 0
