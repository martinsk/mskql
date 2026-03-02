-- Bug: bitwise XOR operator # is not supported
-- In PostgreSQL, # is the bitwise XOR operator: SELECT 12 # 10 returns 6
-- mskql raises "expected FROM, got '#'" parse error
-- setup:
-- input:
SELECT 12 # 10;
-- expected output:
6
-- expected status: 0
