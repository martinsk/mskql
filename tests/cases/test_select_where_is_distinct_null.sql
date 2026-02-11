-- IS DISTINCT FROM with NULL on both sides
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, NULL), (2, 10), (3, NULL);
-- input:
SELECT id FROM t1 WHERE val IS DISTINCT FROM NULL ORDER BY id;
-- expected output:
2
-- expected status: 0
