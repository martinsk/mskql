-- Test vectorized CEIL and FLOOR projection
-- setup:
CREATE TABLE t (id INT, val FLOAT);
INSERT INTO t VALUES (1, 3.2);
INSERT INTO t VALUES (2, -1.7);
INSERT INTO t VALUES (3, 5.0);
INSERT INTO t VALUES (4, 0.5);
-- input:
SELECT CEIL(val), FLOOR(val) FROM t ORDER BY id;
-- expected output:
4|3
-1|-2
5|5
1|0
