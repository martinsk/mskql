-- EXCEPT where both sides are identical should return empty
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
SELECT id, name FROM t1 EXCEPT SELECT id, name FROM t1;
-- expected output:
-- expected status: 0
