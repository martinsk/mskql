-- SMALLINT WHERE filter
-- setup:
CREATE TABLE t1 (id SMALLINT, name TEXT);
INSERT INTO t1 (id, name) VALUES (10, 'alice');
INSERT INTO t1 (id, name) VALUES (200, 'bob');
INSERT INTO t1 (id, name) VALUES (50, 'carol');
-- input:
SELECT name FROM t1 WHERE id > 30 ORDER BY id;
-- expected output:
carol
bob
-- expected status: 0
