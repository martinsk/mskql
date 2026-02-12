-- INSERT INTO ... SELECT from another table
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob');
INSERT INTO t2 SELECT id, name FROM t1;
-- input:
SELECT id, name FROM t2 ORDER BY id;
-- expected output:
1|alice
2|bob
-- expected status: 0
