-- generate_series with LIMIT and OFFSET
-- input:
SELECT * FROM generate_series(1, 100) LIMIT 3 OFFSET 5;
-- expected output:
6
7
8
-- expected status: 0
