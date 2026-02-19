-- bug: comparison operators (=, <>, >, <, etc.) should work as expressions in SELECT list
-- setup:
-- input:
SELECT 1 = 1;
-- expected output:
t
-- expected status: 0
