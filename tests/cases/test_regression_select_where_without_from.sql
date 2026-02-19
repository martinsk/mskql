-- bug: SELECT literal WHERE condition without FROM clause fails to parse
-- setup:
-- input:
SELECT 1 WHERE true;
-- expected output:
1
-- expected status: 0
