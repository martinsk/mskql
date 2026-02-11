-- Multiple CTEs where second CTE references the first
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
WITH cte1 AS (SELECT id, val FROM t1 WHERE val > 10), cte2 AS (SELECT id, val * 2 AS doubled FROM cte1) SELECT * FROM cte2 ORDER BY id;
-- expected output:
2|40
3|60
-- expected status: 0
