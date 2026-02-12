-- generate_series with AS alias and column alias
-- input:
SELECT n FROM generate_series(1, 3) AS g(n);
-- expected output:
1
2
3
-- expected status: 0
