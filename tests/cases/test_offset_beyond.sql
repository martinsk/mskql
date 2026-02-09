-- OFFSET beyond total row count should return no rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
SELECT * FROM t1 OFFSET 100;
-- expected output:
-- expected status: 0
