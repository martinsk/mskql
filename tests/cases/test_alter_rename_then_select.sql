-- ALTER TABLE RENAME COLUMN then SELECT using new name
-- setup:
CREATE TABLE t1 (id INT, old_name TEXT);
INSERT INTO t1 (id, old_name) VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE t1 RENAME COLUMN old_name TO new_name;
-- input:
SELECT id, new_name FROM t1 ORDER BY id;
-- expected output:
1|alice
2|bob
-- expected status: 0
