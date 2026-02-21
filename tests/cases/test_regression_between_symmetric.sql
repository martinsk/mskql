-- BUG: BETWEEN SYMMETRIC not supported (errors with 'expected AND in BETWEEN')
-- input:
SELECT 3 BETWEEN SYMMETRIC 5 AND 1;
-- expected output:
t
-- expected status: 0
