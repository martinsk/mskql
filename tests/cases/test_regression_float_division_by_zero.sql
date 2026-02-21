-- BUG: Float division by zero should error, not return Infinity
-- input:
SELECT 1.0 / 0.0;
-- expected output:
ERROR:  division by zero
-- expected status: 0
