-- Bug: EXTRACT(ISODOW FROM date) returns 0 for Monday instead of 1
-- In PostgreSQL, ISODOW returns 1 for Monday through 7 for Sunday (ISO 8601)
-- 2024-01-15 is a Monday, so ISODOW should return 1
-- mskql returns 0
-- setup:
-- input:
SELECT EXTRACT(ISODOW FROM '2024-01-15'::date);
-- expected output:
1
-- expected status: 0
