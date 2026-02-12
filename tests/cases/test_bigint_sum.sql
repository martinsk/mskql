-- SUM of BIGINT values
-- setup:
CREATE TABLE t1 (id INT, val BIGINT);
INSERT INTO t1 VALUES (1, 1000000000), (2, 2000000000), (3, 3000000000);
-- input:
SELECT SUM(val) FROM t1;
-- expected output:
6000000000
-- expected status: 0
