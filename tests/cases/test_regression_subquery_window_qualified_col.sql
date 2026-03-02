-- Bug: qualified column reference sub.rn fails when rn comes from a window function
-- SELECT sub.rn FROM (SELECT n, ROW_NUMBER() OVER (ORDER BY n) AS rn FROM t) sub
-- returns error: column "sub.rn" does not exist
-- In PostgreSQL, qualified references to subquery output columns always work
-- setup:
CREATE TABLE t_wqual (n INT);
INSERT INTO t_wqual VALUES (1),(2),(3);
-- input:
SELECT sub.rn FROM (SELECT n, ROW_NUMBER() OVER (ORDER BY n) AS rn FROM t_wqual) sub WHERE sub.rn = 2;
-- expected output:
2
-- expected status: 0
