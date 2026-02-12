-- playground example: generate_series with computed column
-- input:
SELECT n, n * n AS square FROM generate_series(1, 10) AS g(n);
-- expected output:
1|1
2|4
3|9
4|16
5|25
6|36
7|49
8|64
9|81
10|100
-- expected status: 0
