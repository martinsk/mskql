-- bug: integer division returns float instead of truncated integer
-- setup:
-- input:
SELECT 10 / 3;
SELECT 7 / 2;
SELECT 1 / 3;
-- expected output:
3
3
0
