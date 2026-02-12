-- DISTINCT ON with NULL values in the distinct column
-- setup:
CREATE TABLE t1 (grp INT, val INT);
INSERT INTO t1 VALUES (1, 10), (1, 20), (NULL, 30), (NULL, 40);
-- input:
SELECT DISTINCT ON (grp) grp, val FROM t1 ORDER BY grp, val;
-- expected output:
1|10
|30
-- expected status: 0
