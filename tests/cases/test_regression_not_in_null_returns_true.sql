-- Bug: NOT IN with NULL in list returns true instead of NULL
-- In PostgreSQL, if any value in the NOT IN list is NULL and the value is not found,
-- the result is NULL (unknown), not true.
-- mskql returns true for: SELECT 5 NOT IN (1, NULL, 2)
-- setup:
-- input:
SELECT 5 NOT IN (1, NULL, 2);
-- expected output:

-- expected status: 0
