-- LIKE on NULL column should not match (three-valued logic)
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, NULL), (3, 'bob');
-- input:
SELECT id FROM t1 WHERE name LIKE '%li%' ORDER BY id;
-- expected output:
1
-- expected status: 0
