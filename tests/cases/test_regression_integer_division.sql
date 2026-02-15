-- regression: integer division truncates
-- setup:
-- input:
SELECT 10 / 3;
SELECT 7 / 2;
-- expected output:
3
3
