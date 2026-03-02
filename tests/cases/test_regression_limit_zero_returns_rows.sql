-- Bug: LIMIT 0 on VALUES subquery returns all rows instead of zero rows
-- In PostgreSQL, LIMIT 0 always returns an empty result set
-- mskql returns all rows when LIMIT 0 is applied to a VALUES subquery
-- setup:
-- input:
SELECT * FROM (VALUES (1),(2),(3)) t(v) LIMIT 0;
-- expected output:

-- expected status: 0
