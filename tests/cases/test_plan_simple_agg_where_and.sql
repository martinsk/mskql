-- plan: Simple aggregate (no GROUP BY) with compound AND WHERE
-- setup:
CREATE TABLE t1 (id INT, val INT, active INT);
INSERT INTO t1 VALUES (1, 10, 1), (2, 20, 0), (3, 30, 1), (4, 40, 1), (5, 50, 0);
-- input:
SELECT SUM(val) FROM t1 WHERE val > 15 AND active = 1;
-- expected output:
70
-- expected status: 0
