-- UNIQUE constraint should allow multiple NULLs (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, email TEXT UNIQUE);
INSERT INTO t1 (id, email) VALUES (1, NULL);
-- input:
INSERT INTO t1 (id, email) VALUES (2, NULL);
SELECT id, email FROM t1 ORDER BY id;
-- expected output:
INSERT 0 1
1|
2|
-- expected status: 0
