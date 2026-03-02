-- Bug: casting a float to int truncates instead of rounding
-- In PostgreSQL, 3.5::int returns 4 (rounds to nearest, ties away from zero)
-- mskql returns 3 (truncates toward zero)
-- setup:
-- input:
SELECT 3.5::int;
-- expected output:
4
-- expected status: 0
