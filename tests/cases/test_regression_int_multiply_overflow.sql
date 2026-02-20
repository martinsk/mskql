-- BUG: Integer multiplication overflow produces scientific notation instead of promoting to BIGINT
-- input:
SELECT 100000 * 100000;
-- expected output:
10000000000
-- expected status: 0
