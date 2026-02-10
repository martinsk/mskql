-- MAX on empty table should return NULL, not 0 (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, val INT);
-- input:
SELECT MAX(val) FROM t1;
-- expected output:

-- expected status: 0
