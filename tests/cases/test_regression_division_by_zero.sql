-- bug: division by zero returns 0 instead of error
-- setup:
-- input:
SELECT 1 / 0;
-- expected output:
-- expected status: ERROR
