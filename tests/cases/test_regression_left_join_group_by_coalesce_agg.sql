-- REGRESSION: LEFT JOIN GROUP BY with multiple aggregates where one is wrapped in COALESCE drops the COALESCE column
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (id INT, t1_id INT, val INT);
INSERT INTO t1 VALUES (1, 'A'), (2, 'B');
INSERT INTO t2 VALUES (1, 1, 10), (2, 1, 20);
-- input:
SELECT t1.name, COUNT(t2.id), COALESCE(SUM(t2.val), 0) AS total FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id GROUP BY t1.name ORDER BY t1.name;
-- expected output:
A|2|30
B|0|0
-- expected status: 0
