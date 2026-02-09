-- IS NOT DISTINCT FROM NULL should match NULL values
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 20), (4, NULL);
-- input:
SELECT id FROM t1 WHERE val IS NOT DISTINCT FROM NULL ORDER BY id;
-- expected output:
2
4
-- expected status: 0
