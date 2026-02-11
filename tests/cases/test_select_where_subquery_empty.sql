-- Scalar subquery returning no rows should yield NULL in comparison
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20);
CREATE TABLE t2 (x INT);
-- input:
SELECT id FROM t1 WHERE val > (SELECT MAX(x) FROM t2);
-- expected output:
-- expected status: 0
