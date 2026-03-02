-- Bug: CTE with column list aliases are not accessible in the outer query
-- WITH x(a, b) AS (SELECT 1, 2) SELECT a, b FROM x
-- returns error: column "a" does not exist
-- In PostgreSQL, CTE column aliases override the inner query column names
-- setup:
-- input:
WITH x(a, b) AS (SELECT 1, 2) SELECT a, b FROM x;
-- expected output:
1|2
-- expected status: 0
