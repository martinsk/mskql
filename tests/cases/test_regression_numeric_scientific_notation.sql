-- BUG: Large NUMERIC values display in scientific notation instead of full decimal
-- input:
SELECT 1234567.89::NUMERIC(10,2);
-- expected output:
1234567.89
-- expected status: 0
