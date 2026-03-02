-- Bug: MAKE_DATE with invalid day silently wraps instead of raising an error
-- In PostgreSQL, MAKE_DATE(2024, 2, 30) raises: "date field value out of range"
-- mskql silently returns 2024-03-01 (wraps the overflow)
-- setup:
-- input:
SELECT MAKE_DATE(2024, 2, 30);
-- expected output:
ERROR:  date field value out of range: 2024-02-30
-- expected status: 1
