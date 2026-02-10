-- CTE referenced twice in the main query (via UNION)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20);
-- input:
WITH cte AS (SELECT id, val FROM t1 WHERE val > 10) SELECT * FROM cte;
-- expected output:
2|20
-- expected status: 0
