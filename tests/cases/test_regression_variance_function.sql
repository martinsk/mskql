-- BUG: VARIANCE() aggregate function not supported
-- setup:
CREATE TABLE t (val INT);
INSERT INTO t VALUES (10), (20), (30), (40), (50);
-- input:
SELECT VARIANCE(val) FROM t;
-- expected output:
250
-- expected status: 0
