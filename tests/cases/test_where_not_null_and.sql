-- WHERE with IS NOT NULL AND comparison
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 20), (4, NULL);
-- input:
SELECT id FROM t1 WHERE val IS NOT NULL AND val > 15 ORDER BY id;
-- expected output:
3
-- expected status: 0
