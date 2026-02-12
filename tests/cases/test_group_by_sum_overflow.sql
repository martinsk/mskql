-- GROUP BY SUM that overflows int32
-- query_group_by casts sums[a] to (int) for non-float columns
-- setup:
CREATE TABLE t1 (grp TEXT, val INT);
INSERT INTO t1 VALUES ('a', 2000000000), ('a', 2000000000);
-- input:
SELECT grp, SUM(val) FROM t1 GROUP BY grp;
-- expected output:
a|4000000000
-- expected status: 0
