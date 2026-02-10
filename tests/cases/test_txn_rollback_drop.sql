-- ROLLBACK should undo DROP TABLE
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
-- input:
BEGIN;
DROP TABLE t1;
ROLLBACK;
SELECT id, name FROM t1;
-- expected output:
BEGIN
DROP TABLE
ROLLBACK
1|alice
-- expected status: 0
