-- REGRESSION: CASE with aggregate in SELECT without GROUP BY returns one row per input row instead of single row
-- setup:
CREATE TABLE t (val INT);
INSERT INTO t VALUES (10), (20), (30);
-- input:
SELECT CASE WHEN SUM(val) > 50 THEN 'high' ELSE 'low' END AS result FROM t;
-- expected output:
high
-- expected status: 0
