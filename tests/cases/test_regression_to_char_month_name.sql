-- Bug: TO_CHAR with 'Month' format pattern returns literal 'Month' instead of month name
-- In PostgreSQL, TO_CHAR('2024-01-15'::date, 'Month') returns 'January  '
-- mskql returns the literal string 'Month'
-- setup:
-- input:
SELECT TRIM(TO_CHAR('2024-01-15'::date, 'Month'));
-- expected output:
January
-- expected status: 0
