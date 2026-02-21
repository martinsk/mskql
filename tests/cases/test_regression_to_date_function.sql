-- BUG: TO_DATE() function not supported
-- input:
SELECT TO_DATE('2024-06-15', 'YYYY-MM-DD');
-- expected output:
2024-06-15
-- expected status: 0
