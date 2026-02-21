-- BUG: IS DISTINCT FROM not supported in SELECT expressions
-- input:
SELECT 1 IS DISTINCT FROM 2;
-- expected output:
t
-- expected status: 0
