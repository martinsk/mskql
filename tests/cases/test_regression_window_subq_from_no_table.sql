-- Bug: subquery with no FROM clause gives "table not found" error
-- SELECT * FROM (SELECT 1 AS x) t works, but
-- SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rn) t gives "table 't' not found"
-- In PostgreSQL, a subquery with no FROM and window functions is valid
-- setup:
-- input:
SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) AS rn) t;
-- expected output:
1
-- expected status: 0
