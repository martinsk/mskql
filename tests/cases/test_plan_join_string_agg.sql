-- plan executor: STRING_AGG in join aggregate
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT, dept TEXT);
INSERT INTO t2 (id, dept) VALUES (1, 'eng'), (2, 'sales');
-- input:
SELECT STRING_AGG(t1.name, ',') FROM t1 JOIN t2 ON t1.id = t2.id;
EXPLAIN SELECT STRING_AGG(t1.name, ',') FROM t1 JOIN t2 ON t1.id = t2.id
-- expected output:
alice,bob
Legacy Row Executor
