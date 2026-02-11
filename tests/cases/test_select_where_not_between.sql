-- WHERE NOT BETWEEN
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 5), (2, 15), (3, 25);
-- input:
SELECT id FROM t1 WHERE NOT val BETWEEN 10 AND 20 ORDER BY id;
-- expected output:
1
3
-- expected status: 0
