-- unary minus in complex expression: 1 - -1
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 VALUES (1);
-- input:
SELECT 1 - -1 FROM t1;
-- expected output:
2
-- expected status: 0
