-- BUG: Casting 'infinity' to FLOAT should work, not silently fail
-- input:
SELECT 'infinity'::FLOAT;
-- expected output:
Infinity
-- expected status: 0
