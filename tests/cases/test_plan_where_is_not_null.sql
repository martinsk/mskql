-- plan: WHERE with IS NOT NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10), (2, NULL), (3, 30), (4, NULL);
-- input:
SELECT id FROM t1 WHERE val IS NOT NULL ORDER BY id;
-- expected output:
1
3
-- expected status: 0
