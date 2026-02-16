-- plan: WHERE with BETWEEN
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 5), (2, 15), (3, 25), (4, 35), (5, 45);
-- input:
SELECT id FROM t1 WHERE val BETWEEN 10 AND 30 ORDER BY id;
-- expected output:
2
3
-- expected status: 0
