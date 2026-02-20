-- BUG: Integer division by zero should return an error, not empty result
-- input:
SELECT 1 / 0;
-- expected output:
ERROR:  division by zero
-- expected status: 0
