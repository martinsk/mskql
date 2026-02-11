-- WHERE with expression across CROSS JOIN tables
-- setup:
CREATE TABLE t1 (x INT);
INSERT INTO t1 (x) VALUES (1), (2), (3);
CREATE TABLE t2 (y INT);
INSERT INTO t2 (y) VALUES (10), (20);
-- input:
SELECT x, y FROM t1 CROSS JOIN t2 WHERE x * y >= 20 ORDER BY x, y;
-- expected output:
1|20
2|10
2|20
3|10
3|20
-- expected status: 0
