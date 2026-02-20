-- BUG: SUM on empty result set should return one row with NULL, not zero rows
-- setup:
CREATE TABLE t (id INT, val INT);
-- input:
SELECT 1 AS marker, SUM(val) AS total FROM t;
-- expected output:
1|
-- expected status: 0
