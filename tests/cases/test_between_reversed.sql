-- BETWEEN with reversed bounds should return no rows (low > high)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id FROM t1 WHERE val BETWEEN 30 AND 10;
-- expected output:
-- expected status: 0
