-- recursive CTE generating a sequence of numbers
-- setup:
CREATE TABLE t1 (id INT);
-- input:
WITH RECURSIVE nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 5) SELECT n FROM nums ORDER BY n;
-- expected output:
1
2
3
4
5
-- expected status: 0
