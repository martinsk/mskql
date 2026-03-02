-- Bug: VALUES in FROM clause does not respect column aliases
-- SELECT x FROM (VALUES (1),(2),(3)) AS t(x) returns error: column "x" does not exist
-- In PostgreSQL, the column alias should be accessible as x
-- setup:
-- input:
SELECT x FROM (VALUES (1),(2),(3)) AS t(x) ORDER BY x;
-- expected output:
1
2
3
-- expected status: 0
