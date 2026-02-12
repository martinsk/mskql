-- generate_series with WHERE filter
-- input:
SELECT * FROM generate_series(1, 10) WHERE generate_series > 7;
-- expected output:
8
9
10
-- expected status: 0
