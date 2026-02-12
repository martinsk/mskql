-- float division precision
-- setup:
CREATE TABLE t1 (id INT, a FLOAT, b FLOAT);
INSERT INTO t1 VALUES (1, 1.0, 3.0);
-- input:
SELECT a / b FROM t1;
-- expected output:
0.333333
-- expected status: 0
