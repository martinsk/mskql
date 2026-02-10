-- multiple CTEs in a single WITH clause
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
WITH low AS (SELECT id, val FROM t1 WHERE val <= 15), high AS (SELECT id, val FROM t1 WHERE val >= 25) SELECT * FROM low UNION ALL SELECT * FROM high ORDER BY id;
-- expected output:
1|10
3|30
-- expected status: 0
