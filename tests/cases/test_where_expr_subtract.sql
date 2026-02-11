-- WHERE clause with subtraction expression
-- setup:
CREATE TABLE t1 (a INT, b INT);
INSERT INTO t1 (a, b) VALUES (10, 3), (5, 5), (20, 8);
-- input:
SELECT a, b FROM t1 WHERE a - b > 5 ORDER BY a;
-- expected output:
10|3
20|8
-- expected status: 0
