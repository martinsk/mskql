-- BUG: Integer overflow produces scientific notation instead of promoting to BIGINT or erroring
-- input:
SELECT 2147483647 + 1;
-- expected output:
2147483648
-- expected status: 0
