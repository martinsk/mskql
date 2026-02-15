-- regression: NOT BETWEEN filters correctly
-- setup:
CREATE TABLE t (val INT);
INSERT INTO t VALUES (10),(20),(30),(40),(50);
-- input:
SELECT val FROM t WHERE val NOT BETWEEN 20 AND 40 ORDER BY val;
-- expected output:
10
50
