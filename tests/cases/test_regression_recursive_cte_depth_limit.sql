-- BUG: Recursive CTE stops at ~1000 iterations instead of completing
-- WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 5000)
-- should produce 5000 rows but stops early
-- setup:
-- input:
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5000) SELECT MAX(n) FROM t;
-- expected output:
5000
-- expected status: 0
