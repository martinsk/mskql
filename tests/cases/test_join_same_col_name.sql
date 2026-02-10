-- JOIN two tables with same column name should resolve correctly with table prefix
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t2 (id, name) VALUES (1, 'eng'), (2, 'sales');
-- input:
SELECT t1.name, t2.name FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t1.id;
-- expected output:
alice|eng
bob|sales
-- expected status: 0
