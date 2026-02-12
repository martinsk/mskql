-- LIKE with just % should match everything including empty string
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'hello'), (2, ''), (3, 'world');
-- input:
SELECT id FROM t1 WHERE name LIKE '%' ORDER BY id;
-- expected output:
1
2
3
-- expected status: 0
