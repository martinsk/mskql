-- Test vectorized COALESCE(col, literal) projection
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (2, NULL);
INSERT INTO t VALUES (3, 30);
INSERT INTO t VALUES (4, NULL);
-- input:
SELECT COALESCE(val, 0) FROM t ORDER BY id;
-- expected output:
10
0
30
0
