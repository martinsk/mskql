-- generate_series with ORDER BY DESC
-- input:
SELECT * FROM generate_series(1, 5) ORDER BY generate_series DESC;
-- expected output:
5
4
3
2
1
-- expected status: 0
