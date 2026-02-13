-- STRING_AGG with DISTINCT
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
INSERT INTO t1 (id, name) VALUES (2, 'bob');
INSERT INTO t1 (id, name) VALUES (3, 'alice');
INSERT INTO t1 (id, name) VALUES (4, 'bob');
INSERT INTO t1 (id, name) VALUES (5, 'carol');
-- input:
SELECT STRING_AGG(DISTINCT name, ',') FROM t1;
-- expected output:
alice,bob,carol
-- expected status: 0
