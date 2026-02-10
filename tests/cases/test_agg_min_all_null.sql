-- MIN on column where all values are NULL should return NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, NULL), (2, NULL);
-- input:
SELECT MIN(val) FROM t1;
-- expected output:
-- expected status: 0
