-- BUG: NOT NULL should return NULL (row with NULL value), not suppress the row
-- input:
SELECT 1 AS marker, (NOT NULL) AS result;
-- expected output:
1|
-- expected status: 0
