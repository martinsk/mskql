-- BUG: LIMIT on UNION ALL is ignored, returns all rows
-- input:
SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY n LIMIT 2;
-- expected output:
1
2
-- expected status: 0
