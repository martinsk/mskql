-- Bug: LIMIT is ignored when applied to a VALUES subquery
-- SELECT * FROM (VALUES (1),(2),(3)) t(v) LIMIT 1 returns all 3 rows
-- In PostgreSQL, LIMIT restricts the result to at most N rows
-- setup:
-- input:
SELECT * FROM (VALUES (1),(2),(3)) t(v) LIMIT 1;
-- expected output:
1
-- expected status: 0
