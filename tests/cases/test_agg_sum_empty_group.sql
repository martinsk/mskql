-- SUM on empty table should return NULL (not 0)
-- setup:
CREATE TABLE t1 (id INT, val INT);
-- input:
SELECT SUM(val) FROM t1;
-- expected output:
-- expected status: 0
