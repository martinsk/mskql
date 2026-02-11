-- SUM should skip NULLs and sum only non-NULL values
-- setup:
CREATE TABLE t1 (val INT);
INSERT INTO t1 (val) VALUES (10), (NULL), (20), (NULL), (30);
-- input:
SELECT SUM(val) FROM t1;
-- expected output:
60
-- expected status: 0
