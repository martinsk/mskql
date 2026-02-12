-- ORDER BY using a SELECT alias
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 VALUES (1, 3, 4), (2, 5, 1), (3, 1, 2);
-- input:
SELECT id, a + b AS total FROM t1 ORDER BY total;
-- expected output:
3|3
2|6
1|7
-- expected status: 0
