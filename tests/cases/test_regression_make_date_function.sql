-- BUG: MAKE_DATE() function not supported
-- input:
SELECT MAKE_DATE(2024, 6, 15);
-- expected output:
2024-06-15
-- expected status: 0
