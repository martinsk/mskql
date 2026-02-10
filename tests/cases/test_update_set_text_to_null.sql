-- UPDATE SET text column to NULL
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
UPDATE t1 SET name = NULL WHERE id = 1;
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
UPDATE 1
1|
2|bob
-- expected status: 0
