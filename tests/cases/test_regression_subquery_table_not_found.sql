-- Bug: subquery in FROM clause with no table reference gives "table not found"
-- SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rn) t
-- returns error: table 't' not found
-- In PostgreSQL, a subquery with no FROM clause is valid
-- setup:
-- input:
SELECT rn FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rn) t;
-- expected output:
1
-- expected status: 0
