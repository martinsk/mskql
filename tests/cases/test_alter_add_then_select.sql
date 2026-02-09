-- ALTER TABLE ADD COLUMN then SELECT should show NULL for existing rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE t1 ADD COLUMN age INT;
-- input:
SELECT id, name, age FROM t1 ORDER BY id;
-- expected output:
1|alice|
2|bob|
-- expected status: 0
