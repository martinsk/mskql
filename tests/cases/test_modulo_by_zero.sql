-- modulo by zero should return error or NULL, not crash
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10);
-- input:
SELECT val % 0 FROM t1;
-- expected output:

-- expected status: 0
