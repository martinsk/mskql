-- DELETE RETURNING with no matching rows should return no rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
DELETE FROM t1 WHERE id = 999 RETURNING *;
-- expected output:
DELETE 0
-- expected status: 0
