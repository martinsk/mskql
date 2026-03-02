-- Bug: TO_CHAR with 'Dy' format pattern returns literal 'Dy' instead of abbreviated day name
-- In PostgreSQL, TO_CHAR('2024-01-15'::date, 'Dy') returns 'Mon'
-- mskql returns the literal string 'Dy'
-- setup:
-- input:
SELECT TO_CHAR('2024-01-15'::date, 'Dy');
-- expected output:
Mon
-- expected status: 0
