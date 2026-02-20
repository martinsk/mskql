-- BUG: Modulo by zero should return an error, not empty result
-- input:
SELECT 10 % 0;
-- expected output:
ERROR:  division by zero
-- expected status: 0
