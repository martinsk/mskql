-- SUM of large INT values that exceed 32-bit range
-- The SUM accumulator uses double but casts to (int) for non-float columns
-- This tests whether SUM correctly handles values that overflow int32
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 2000000000), (2, 2000000000);
-- input:
SELECT SUM(val) FROM t1;
-- expected output:
4000000000
-- expected status: 0
