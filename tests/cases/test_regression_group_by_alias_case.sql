-- REGRESSION: GROUP BY with alias referencing a CASE expression fails with 'column not found'
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35);
-- input:
SELECT CASE WHEN val > 20 THEN 'high' ELSE 'low' END AS bucket, COUNT(*) FROM t GROUP BY bucket ORDER BY bucket;
-- expected output:
high|2
low|2
-- expected status: 0
