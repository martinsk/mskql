-- BUG: Bitwise shift operators (<< and >>) not supported in SELECT expressions
-- input:
SELECT 1 << 3;
-- expected output:
8
-- expected status: 0
