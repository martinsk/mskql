-- BETWEEN should be inclusive on both ends
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id FROM t1 WHERE val BETWEEN 10 AND 30 ORDER BY id;
-- expected output:
1
2
3
-- expected status: 0
