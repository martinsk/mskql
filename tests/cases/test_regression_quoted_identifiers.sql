-- bug: double-quoted identifiers for column/table names with spaces fail to parse
-- setup:
-- input:
SELECT 1 AS "my column";
-- expected output:
1
-- expected status: 0
