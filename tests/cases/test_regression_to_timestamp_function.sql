-- BUG: TO_TIMESTAMP() function not supported
-- input:
SELECT TO_TIMESTAMP('2024-06-15 12:30:00', 'YYYY-MM-DD HH24:MI:SS');
-- expected output:
2024-06-15 12:30:00
-- expected status: 0
