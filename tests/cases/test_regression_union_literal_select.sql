-- bug: UNION/INTERSECT/EXCEPT with literal SELECTs (no FROM) fails to parse
-- setup:
-- input:
SELECT 1 AS a UNION SELECT 2 AS a ORDER BY a;
-- expected output:
1
2
-- expected status: 0
