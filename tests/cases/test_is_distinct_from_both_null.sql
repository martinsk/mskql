-- IS DISTINCT FROM where both sides are NULL (should be false)
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 VALUES (1, NULL, NULL), (2, 1, NULL), (3, NULL, 1), (4, 1, 1);
-- input:
SELECT id FROM t1 WHERE a IS DISTINCT FROM b ORDER BY id;
-- expected output:
2
3
-- expected status: 0
