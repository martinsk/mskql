-- NUMERIC type should preserve decimal precision
-- setup:
CREATE TABLE t1 (id INT, val NUMERIC);
INSERT INTO t1 VALUES (1, 123.456), (2, 0.001);
-- input:
SELECT id, val FROM t1 ORDER BY id;
-- expected output:
1|123.456
2|0.001
-- expected status: 0
