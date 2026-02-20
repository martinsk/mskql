-- BUG: Nested aggregate calls should error, not return empty
-- input:
SELECT MAX(SUM(1));
-- expected output:
ERROR:  aggregate function calls cannot be nested
-- expected status: 0
