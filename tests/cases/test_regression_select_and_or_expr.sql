-- bug: AND/OR boolean operators should work as expressions in SELECT list
-- setup:
-- input:
SELECT true AND false;
SELECT true OR false;
-- expected output:
f
t
-- expected status: 0
