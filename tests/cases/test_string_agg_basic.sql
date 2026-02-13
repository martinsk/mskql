-- STRING_AGG basic usage
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
INSERT INTO t1 (id, name) VALUES (2, 'bob');
INSERT INTO t1 (id, name) VALUES (3, 'carol');
-- input:
SELECT STRING_AGG(name, ',') FROM t1;
-- expected output:
alice,bob,carol
-- expected status: 0
