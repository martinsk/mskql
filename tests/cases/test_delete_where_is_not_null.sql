-- DELETE with WHERE col IS NOT NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 30);
-- input:
DELETE FROM t1 WHERE val IS NOT NULL;
SELECT id FROM t1;
-- expected output:
DELETE 2
2
-- expected status: 0
