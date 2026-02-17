-- SELECT on nonexistent column should error
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
-- input:
SELECT nonexistent FROM t1;
-- expected output:
ERROR:  column "nonexistent" does not exist
-- expected status: 1
