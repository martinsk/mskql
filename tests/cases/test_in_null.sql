-- IN list with NULL: NULL IN (1, 2, NULL) should not match (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 20);
-- input:
SELECT id FROM t1 WHERE val IN (10, 20) ORDER BY id;
-- expected output:
1
3
-- expected status: 0
