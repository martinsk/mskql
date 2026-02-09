-- LEFT JOIN with empty right table should return all left rows with NULLs
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT, val INT);
-- input:
SELECT t1.name, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY t1.name;
-- expected output:
alice|
bob|
-- expected status: 0
