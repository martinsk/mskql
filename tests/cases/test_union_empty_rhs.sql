-- UNION with empty RHS table
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT, name TEXT);
-- input:
SELECT id, name FROM t1 UNION SELECT id, name FROM t2 ORDER BY id;
-- expected output:
1|alice
2|bob
-- expected status: 0
