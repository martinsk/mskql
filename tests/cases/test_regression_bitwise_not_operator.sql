-- Bug: bitwise NOT operator ~ is not supported
-- In PostgreSQL, ~ is the bitwise NOT operator: SELECT ~5 returns -6
-- mskql raises "unexpected token in expression: '~'" parse error
-- setup:
-- input:
SELECT ~5;
-- expected output:
-6
-- expected status: 0
