-- Test vectorized CAST(float AS int) and CAST(float AS bigint) projection
-- setup:
CREATE TABLE t (id INT, val FLOAT);
INSERT INTO t VALUES (1, 3.7);
INSERT INTO t VALUES (2, -1.2);
INSERT INTO t VALUES (3, 100.0);
-- input:
SELECT val::int, val::bigint FROM t ORDER BY id;
-- expected output:
3|3
-1|-1
100|100
