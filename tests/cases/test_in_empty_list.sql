-- IN with empty list is invalid SQL and should produce a parse error
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 (id) VALUES (1), (2);
-- input:
SELECT id FROM t1 WHERE id IN ();
-- expected output:
ERROR:  syntax error or unsupported statement
-- expected status: 1
