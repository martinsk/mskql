-- Non-keyword at start of statement should report what was found
-- input:
123 SELECT;
-- expected output:
ERROR:  expected SQL keyword, got '123'
-- expected status: 1
