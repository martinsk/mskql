-- Bug: TO_CHAR with 'Day' format pattern returns literal 'Day' instead of the day name
-- In PostgreSQL, TO_CHAR('2024-01-15'::date, 'Day') returns 'Monday   '
-- mskql returns the literal string 'Day'
-- setup:
-- input:
SELECT TRIM(TO_CHAR('2024-01-15'::date, 'Day'));
-- expected output:
Monday
-- expected status: 0
