-- BUG: STDDEV() aggregate function not supported
-- setup:
CREATE TABLE t (val INT);
INSERT INTO t VALUES (10), (20), (30), (40), (50);
-- input:
SELECT STDDEV(val) FROM t;
-- expected output:
15.8114
-- expected status: 0
