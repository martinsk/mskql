-- regression: WHERE val > ANY (subquery) filters correctly
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40);
-- input:
SELECT id FROM t WHERE val > ANY (SELECT val FROM t WHERE val IN (20,30)) ORDER BY id;
-- expected output:
3
4
