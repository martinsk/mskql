-- DELETE with WHERE and RETURNING should return deleted rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
-- input:
DELETE FROM t1 WHERE id > 1 RETURNING id, name;
-- expected output:
2|bob
3|carol
DELETE 2
-- expected status: 0
