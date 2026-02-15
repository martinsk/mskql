-- regression: window function query respects LIMIT
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40);
-- input:
SELECT id, SUM(val) OVER (ORDER BY id) FROM t ORDER BY id LIMIT 2;
-- expected output:
1|10
2|30
