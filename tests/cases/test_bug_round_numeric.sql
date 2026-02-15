-- bug: ROUND(val::numeric, N) returns 0 instead of the rounded value
-- setup:
-- input:
SELECT ROUND(3.14159::numeric, 2);
SELECT ROUND(3.7::numeric);
SELECT ROUND(2.555::numeric, 2);
-- expected output:
3.14
4
2.56
