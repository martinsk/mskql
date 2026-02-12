-- ALTER TABLE RENAME COLUMN then select by new name
-- setup:
CREATE TABLE t1 (id INT, old_name TEXT);
INSERT INTO t1 VALUES (1, 'alice');
ALTER TABLE t1 RENAME COLUMN old_name TO new_name;
-- input:
SELECT id, new_name FROM t1;
-- expected output:
1|alice
-- expected status: 0
