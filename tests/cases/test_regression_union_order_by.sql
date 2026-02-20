-- BUG: ORDER BY on UNION result does not sort correctly
-- input:
SELECT 1 AS n UNION SELECT 3 UNION SELECT 2 ORDER BY n;
-- expected output:
1
2
3
-- expected status: 0
