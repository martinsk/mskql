-- BUG: UNION with different column counts should error, not return garbled output
-- input:
SELECT 1, 2 UNION SELECT 3;
-- expected output:
ERROR:  each UNION query must have the same number of columns
-- expected status: 0
