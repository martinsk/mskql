-- Unary minus on text should error
-- setup:
CREATE TABLE t1 (name TEXT);
INSERT INTO t1 (name) VALUES ('hello');
-- input:
SELECT -name FROM t1;
-- expected output:
ERROR:  operator does not exist: - text
-- expected status: 1
