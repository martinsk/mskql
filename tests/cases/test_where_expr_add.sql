-- WHERE clause with addition expression
-- setup:
CREATE TABLE t1 (a INT, b INT);
INSERT INTO t1 (a, b) VALUES (1, 10), (2, 20), (3, 5);
-- input:
SELECT a, b FROM t1 WHERE a + b > 15 ORDER BY a;
-- expected output:
2|20
-- expected status: 0
