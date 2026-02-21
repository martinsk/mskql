-- BUG: CROSS JOIN of two subqueries crashes the server
-- input:
SELECT a.x, b.y FROM (SELECT 1 AS x) a CROSS JOIN (SELECT 2 AS y) b;
-- expected output:
1|2
-- expected status: 0
