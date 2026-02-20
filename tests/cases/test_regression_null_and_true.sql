-- BUG: NULL AND TRUE should return NULL, not FALSE (three-valued logic)
-- input:
SELECT 1 AS marker, (NULL AND TRUE) AS result;
-- expected output:
1|
-- expected status: 0
