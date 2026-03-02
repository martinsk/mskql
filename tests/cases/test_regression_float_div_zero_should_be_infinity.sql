-- Bug: float division by zero raises an error instead of returning Infinity
-- In PostgreSQL, dividing a float by zero returns Infinity (IEEE 754 behavior)
-- mskql raises "division by zero" error for float division
-- setup:
-- input:
SELECT 1.0 / 0.0;
-- expected output:
Infinity
-- expected status: 0
