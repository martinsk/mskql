-- generate_series with custom step
-- input:
SELECT * FROM generate_series(0, 10, 3);
-- expected output:
0
3
6
9
-- expected status: 0
