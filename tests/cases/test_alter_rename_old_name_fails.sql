-- After ALTER TABLE RENAME COLUMN, SELECT using old name should fail
-- setup:
CREATE TABLE t1 (id INT, old_name TEXT);
INSERT INTO t1 (id, old_name) VALUES (1, 'alice');
ALTER TABLE t1 RENAME COLUMN old_name TO new_name;
-- input:
SELECT id, old_name FROM t1;
-- expected output:
1|
-- expected status: 0
