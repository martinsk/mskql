-- Unsupported SQL keyword should report the keyword
-- input:
GRANT ALL ON foo TO bar;
-- expected output:
ERROR:  expected SQL keyword, got 'GRANT'
-- expected status: 1
