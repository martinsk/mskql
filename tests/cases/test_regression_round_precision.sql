-- BUG: ROUND with high precision (e.g. 10) does not produce enough decimal places
-- input:
SELECT ROUND(1.0 / 3.0, 10);
-- expected output:
0.3333333333
-- expected status: 0
