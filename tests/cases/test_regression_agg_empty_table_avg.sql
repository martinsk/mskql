-- AVG on empty table should return one row with NULL
-- setup:
CREATE TABLE t (val INT);
-- input:
SELECT AVG(val) FROM t;
-- expected output:

-- expected status: 0
