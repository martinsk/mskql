-- SUM of column where all values are NULL returns NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, NULL), (2, NULL), (3, NULL);
-- input:
SELECT SUM(val) FROM t1;
-- expected output:

-- expected status: 0
