-- Bug: SQRT of a negative number returns NaN instead of raising an error
-- In PostgreSQL, SQRT(-1) raises: "cannot take square root of a negative number"
-- mskql silently returns NaN
-- setup:
-- input:
SELECT SQRT(-1);
-- expected output:
ERROR:  cannot take square root of a negative number
-- expected status: 1
