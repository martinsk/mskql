-- Bug: NULL IS NOT TRUE returns empty/NULL instead of true
-- In PostgreSQL, NULL IS NOT TRUE evaluates to true (NULL is not true)
-- mskql returns empty (NULL) instead of true
-- setup:
-- input:
SELECT NULL IS NOT TRUE;
-- expected output:
t
-- expected status: 0
