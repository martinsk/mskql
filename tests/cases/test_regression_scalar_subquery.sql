-- regression: non-correlated scalar subquery returns same value for all rows
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1,10),(2,20),(3,30);
-- input:
SELECT id, (SELECT MAX(val) FROM t) FROM t ORDER BY id;
-- expected output:
1|30
2|30
3|30
