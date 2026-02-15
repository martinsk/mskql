-- bug: CTE referenced multiple times fails with "table not found"
-- setup:
-- input:
WITH cte AS (SELECT generate_series(1,3) as n) SELECT a.n + b.n as total FROM cte a CROSS JOIN cte b WHERE a.n = 1 AND b.n = 1;
-- expected output:
2
