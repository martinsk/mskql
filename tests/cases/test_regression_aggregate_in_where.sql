-- BUG: Aggregate in WHERE clause should error, not silently evaluate
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT * FROM t WHERE val > AVG(val);
-- expected output:
ERROR:  aggregate functions are not allowed in WHERE
-- expected status: 0
