-- CROSS JOIN with WHERE filter
-- setup:
CREATE TABLE t1 (a INT);
INSERT INTO t1 (a) VALUES (1), (2);
CREATE TABLE t2 (b INT);
INSERT INTO t2 (b) VALUES (10), (20);
-- input:
SELECT a, b FROM t1 CROSS JOIN t2 WHERE a + b > 11 ORDER BY a, b;
-- expected output:
1|20
2|10
2|20
-- expected status: 0
