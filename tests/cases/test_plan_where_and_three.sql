-- plan: WHERE with AND of three comparisons (nested COND_AND)
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300), (4, 40, 400);
-- input:
SELECT id FROM t1 WHERE a >= 20 AND a <= 30 AND b > 100 ORDER BY id;
-- expected output:
2
3
-- expected status: 0
