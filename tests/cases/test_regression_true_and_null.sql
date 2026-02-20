-- BUG: TRUE AND NULL should return NULL, not FALSE (three-valued logic)
-- input:
SELECT 1 AS marker, (TRUE AND NULL) AS result;
-- expected output:
1|
-- expected status: 0
