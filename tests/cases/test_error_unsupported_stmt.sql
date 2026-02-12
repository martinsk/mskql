-- Unsupported SQL keyword should report the keyword
-- input:
EXPLAIN SELECT 1;
-- expected output:
ERROR:  expected SQL keyword, got 'EXPLAIN'
-- expected status: 1
