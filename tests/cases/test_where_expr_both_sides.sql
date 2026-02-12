-- WHERE clause with expressions on both sides of comparison
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 VALUES (1, 3, 4), (2, 5, 2), (3, 1, 1);
-- input:
SELECT id FROM t1 WHERE a + b > 6 ORDER BY id;
-- expected output:
1
2
-- expected status: 0
