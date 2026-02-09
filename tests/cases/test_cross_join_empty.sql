-- CROSS JOIN with empty table should return no rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
CREATE TABLE t2 (val INT);
-- input:
SELECT t1.name, t2.val FROM t1 CROSS JOIN t2;
-- expected output:
-- expected status: 0
