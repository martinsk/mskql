-- DELETE all rows with RETURNING should return all deleted rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
DELETE FROM t1 RETURNING *;
-- expected output:
1|alice
2|bob
DELETE 2
-- expected status: 0
