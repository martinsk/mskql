-- BUG: BIGINT overflow silently wraps around instead of erroring
-- input:
SELECT 9223372036854775807::BIGINT + 1;
-- expected output:
ERROR:  bigint out of range
-- expected status: 0
