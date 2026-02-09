-- INSERT violating UNIQUE constraint should fail
-- setup:
CREATE TABLE t1 (id INT UNIQUE, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
-- input:
INSERT INTO t1 (id, name) VALUES (1, 'bob');
-- expected status: 1
