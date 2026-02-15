-- regression: ORDER BY NULLS FIRST / NULLS LAST
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1,30),(2,NULL),(3,10);
-- input:
SELECT id, val FROM t ORDER BY val ASC NULLS FIRST;
-- expected output:
2|
3|10
1|30
