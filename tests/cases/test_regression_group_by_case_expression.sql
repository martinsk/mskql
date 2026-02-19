-- bug: GROUP BY with CASE expression fails ('GROUP BY column CASE not found')
-- setup:
CREATE TABLE t_gb_case (id INT, val INT);
INSERT INTO t_gb_case VALUES (1,10),(2,15),(3,20),(4,25),(5,30);
-- input:
SELECT CASE WHEN val >= 20 THEN 'high' ELSE 'low' END AS bucket, COUNT(*) FROM t_gb_case GROUP BY CASE WHEN val >= 20 THEN 'high' ELSE 'low' END ORDER BY bucket;
-- expected output:
high|3
low|2
-- expected status: 0
