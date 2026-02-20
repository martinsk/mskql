-- BUG: NULL OR FALSE should return NULL, not FALSE (three-valued logic)
-- input:
SELECT 1 AS marker, (NULL OR FALSE) AS result;
-- expected output:
1|
-- expected status: 0
