-- Bug: CASE WHEN NULL = 1 THEN ... causes parse error "expected THEN in CASE"
-- In PostgreSQL, NULL = 1 evaluates to NULL (false), so ELSE branch is taken
-- mskql fails to parse this valid SQL expression
-- setup:
-- input:
SELECT CASE WHEN NULL = 1 THEN 'yes' ELSE 'no' END;
-- expected output:
no
-- expected status: 0
