-- ROLLBACK should undo INSERT
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice');
-- input:
BEGIN;
INSERT INTO t1 VALUES (2, 'bob');
ROLLBACK;
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
BEGIN
INSERT 0 1
ROLLBACK
1|alice
-- expected status: 0
