-- BUG: SMALLINT overflow silently wraps around instead of erroring
-- input:
SELECT 32767::SMALLINT + 1::SMALLINT;
-- expected output:
ERROR:  smallint out of range
-- expected status: 0
