-- SUM on NUMERIC should preserve decimal precision
-- setup:
CREATE TABLE t (id INT, val NUMERIC(10,2));
INSERT INTO t VALUES (1, 123.45), (2, -67.89), (3, 0.01);
-- input:
SELECT SUM(val) FROM t;
-- expected output:
55.57
-- expected status: 0
