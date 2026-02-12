-- UNIQUE constraint violation should report column name
-- setup:
CREATE TABLE t1 (id INT UNIQUE, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
-- input:
INSERT INTO t1 (id, name) VALUES (1, 'bob');
-- expected output:
ERROR:  UNIQUE constraint violated for column 'id'
-- expected status: 1
