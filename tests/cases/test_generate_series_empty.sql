-- generate_series returns empty when start > stop with positive step
-- input:
SELECT * FROM generate_series(5, 1);
-- expected output:
-- expected status: 0
