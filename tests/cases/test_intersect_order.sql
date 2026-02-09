-- intersect with order by on combined result
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (3, 'charlie'), (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t2 (id, name) VALUES (2, 'bob'), (3, 'charlie');
-- input:
SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2 ORDER BY id;
-- expected output:
2|bob
3|charlie
-- expected status: 0
