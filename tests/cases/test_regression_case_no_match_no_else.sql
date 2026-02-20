-- BUG: Simple CASE with no matching WHEN and no ELSE should return NULL, not empty
-- input:
SELECT 1 AS marker, CASE 3 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END AS result;
-- expected output:
1|
-- expected status: 0
