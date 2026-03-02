-- Bug: casting an out-of-range text value to INT returns -1 instead of raising an error
-- In PostgreSQL, '99999999999999999999'::int raises "integer out of range"
-- mskql silently returns -1
-- setup:
-- input:
SELECT '99999999999999999999'::int;
-- expected output:
ERROR:  integer out of range
-- expected status: 1
