-- BUG: Casting 'NaN' to FLOAT should work, not silently fail
-- input:
SELECT 'NaN'::FLOAT;
-- expected output:
NaN
-- expected status: 0
