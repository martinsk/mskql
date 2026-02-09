-- division by zero in arithmetic expression should not crash
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 (id, a, b) VALUES (1, 10, 0);
-- input:
SELECT id, a / b FROM t1;
-- expected output:
1|0
-- expected status: 0
