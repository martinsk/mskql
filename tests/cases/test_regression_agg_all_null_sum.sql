-- SUM on all-NULL values should return one row with NULL
-- setup:
CREATE TABLE t (val INT);
INSERT INTO t VALUES (NULL), (NULL), (NULL);
-- input:
SELECT SUM(val) FROM t;
-- expected output:

-- expected status: 0
