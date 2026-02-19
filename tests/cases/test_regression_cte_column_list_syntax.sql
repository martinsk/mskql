-- bug: CTE with column list syntax WITH cte(col) AS (...) fails to parse
-- setup:
-- input:
WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5) SELECT * FROM cnt;
-- expected output:
1
2
3
4
5
-- expected status: 0
