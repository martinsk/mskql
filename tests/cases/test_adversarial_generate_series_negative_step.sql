-- adversarial: generate_series with negative step
-- input:
SELECT * FROM generate_series(5, 1, -1);
-- expected output:
5
4
3
2
1
