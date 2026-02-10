-- UNIQUE column should allow multiple NULLs (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, code INT UNIQUE);
INSERT INTO t1 (id, code) VALUES (1, NULL), (2, NULL), (3, 10);
-- input:
SELECT id, code FROM t1 ORDER BY id;
-- expected output:
1|
2|
3|10
-- expected status: 0
