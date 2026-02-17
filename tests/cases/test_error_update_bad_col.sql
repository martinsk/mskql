-- UPDATE SET with nonexistent column should error
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
-- input:
UPDATE t1 SET nonexistent = 'x' WHERE id = 1;
-- expected output:
ERROR:  column "nonexistent" of relation "t1" does not exist
-- expected status: 1
