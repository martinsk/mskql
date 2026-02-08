-- except
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t2 (id, name) VALUES (2, 'bob'), (3, 'charlie');
-- input:
SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t2;
-- expected output:
1|alice
-- expected status: 0
