-- modulo by zero should error
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10);
-- input:
SELECT id, val % 0 FROM t1;
-- expected output:
ERROR:  division by zero
-- expected status: 0
