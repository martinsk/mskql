-- regression: ROUND with numeric precision
-- setup:
-- input:
SELECT ROUND(3.14159::numeric, 2);
SELECT ROUND(3.7::numeric);
-- expected output:
3.14
4
