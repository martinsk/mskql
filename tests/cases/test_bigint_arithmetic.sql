-- BIGINT arithmetic with large values
-- setup:
CREATE TABLE t1 (id INT, val BIGINT);
INSERT INTO t1 VALUES (1, 2147483648), (2, -2147483649);
-- input:
SELECT id, val FROM t1 ORDER BY id;
-- expected output:
1|2147483648
2|-2147483649
-- expected status: 0
