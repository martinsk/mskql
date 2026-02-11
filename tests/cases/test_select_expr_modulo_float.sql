-- Modulo with float operands should work correctly (or truncate to int)
-- setup:
CREATE TABLE t1 (id INT, val FLOAT);
INSERT INTO t1 (id, val) VALUES (1, 7.5);
-- input:
SELECT id, val % 3 FROM t1;
-- expected output:
1|1
-- expected status: 0
