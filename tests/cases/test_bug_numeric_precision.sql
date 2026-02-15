-- bug: NUMERIC(precision, scale) cast ignores scale, does not round to specified decimal places
-- setup:
-- input:
SELECT 1.23456789::NUMERIC(10, 2);
-- expected output:
1.23
