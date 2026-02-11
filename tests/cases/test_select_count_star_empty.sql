-- COUNT(*) on empty table should return 0, not empty result
-- setup:
CREATE TABLE t1 (id INT);
-- input:
SELECT COUNT(*) FROM t1;
-- expected output:
0
-- expected status: 0
