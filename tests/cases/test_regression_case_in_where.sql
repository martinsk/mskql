-- BUG: CASE expression in WHERE clause errors with 'expected comparison operator'
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t VALUES (1, 'short'), (2, 'medium len'), (3, 'a very long name');
-- input:
SELECT * FROM t WHERE CASE WHEN LENGTH(name) > 10 THEN TRUE ELSE FALSE END ORDER BY id;
-- expected output:
3|a very long name
-- expected status: 0
