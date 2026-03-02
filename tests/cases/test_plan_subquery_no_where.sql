-- PLAN_SUBQUERY: FROM subquery with no outer WHERE (SELECT * passthrough)
-- setup:
CREATE TABLE t (x INT, y INT);
INSERT INTO t VALUES (3, 30), (1, 10), (2, 20);
-- input:
SELECT * FROM (SELECT x, y FROM t) sub ORDER BY x;
EXPLAIN SELECT * FROM (SELECT x, y FROM t) sub ORDER BY x
-- expected output:
1|10
2|20
3|30
Legacy Row Executor
-- expected status: 0
