-- SUM on empty table should return NULL, not 0 (SQL standard)
-- The query_aggregate path sets sums[a]=0 and returns (int)sums[a] without checking nonnull_count
-- setup:
CREATE TABLE t1 (id INT, val INT);
-- input:
SELECT SUM(val) FROM t1;
-- expected output:

-- expected status: 0
