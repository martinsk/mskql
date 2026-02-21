-- BUG: Multiple subqueries in FROM (implicit cross join) only returns first subquery's columns
-- input:
SELECT * FROM (SELECT 1 AS x) a, (SELECT 2 AS y) b;
-- expected output:
1|2
-- expected status: 0
