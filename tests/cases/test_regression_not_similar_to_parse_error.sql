-- Bug: NOT SIMILAR TO raises a parse error "expected FROM, got 'NOT'"
-- In PostgreSQL, NOT SIMILAR TO is a valid negation of SIMILAR TO
-- mskql fails to parse this operator
-- setup:
-- input:
SELECT 'hello' NOT SIMILAR TO 'world%';
-- expected output:
t
-- expected status: 0
