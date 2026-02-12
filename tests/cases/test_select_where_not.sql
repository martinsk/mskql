-- WHERE with NOT condition
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id FROM t1 WHERE NOT val = 20 ORDER BY id;
-- expected output:
1
3
-- expected status: 0
