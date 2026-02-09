-- INTERSECT with no overlapping rows should return empty
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t2 (id, name) VALUES (2, 'bob');
-- input:
SELECT id, name FROM t1 INTERSECT SELECT id, name FROM t2;
-- expected output:
-- expected status: 0
