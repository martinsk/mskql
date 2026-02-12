-- UNIQUE constraint allows multiple NULLs (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, val INT UNIQUE);
INSERT INTO t1 VALUES (1, NULL);
-- input:
INSERT INTO t1 VALUES (2, NULL);
SELECT id, val FROM t1 ORDER BY id;
-- expected output:
INSERT 0 1
1|
2|
-- expected status: 0
