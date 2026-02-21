-- BUG: TRUNC() function not supported
-- input:
SELECT TRUNC(3.14159, 2);
-- expected output:
3.14
-- expected status: 0
