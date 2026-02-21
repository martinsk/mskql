-- REGRESSION: Aggregate in ORDER BY without GROUP BY should error but silently returns all rows
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT * FROM t ORDER BY SUM(val);
-- expected output:
ERROR:  aggregate functions are not allowed in ORDER BY
-- expected status: 0
