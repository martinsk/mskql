-- transaction ROLLBACK should undo DELETE
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
BEGIN;
DELETE FROM t1 WHERE id = 1;
ROLLBACK;
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
BEGIN
DELETE 1
ROLLBACK
1|alice
2|bob
-- expected status: 0
