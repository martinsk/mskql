-- DELETE RETURNING with no matching rows returns empty
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob');
-- input:
DELETE FROM t1 WHERE id = 99 RETURNING *;
-- expected output:
DELETE 0
-- expected status: 0
