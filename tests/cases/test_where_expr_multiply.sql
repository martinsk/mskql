-- WHERE clause with multiplication expression
-- setup:
CREATE TABLE t1 (a INT, b INT);
INSERT INTO t1 (a, b) VALUES (2, 3), (4, 5), (1, 1);
-- input:
SELECT a, b FROM t1 WHERE a * b >= 6 ORDER BY a;
-- expected output:
2|3
4|5
-- expected status: 0
