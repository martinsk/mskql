-- plan: WHERE on SMALLINT column (was previously unsupported in simple filter path)
-- setup:
CREATE TABLE t1 (id INT, val SMALLINT);
INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (4, 40);
-- input:
SELECT id FROM t1 WHERE val > 15 ORDER BY id;
-- expected output:
2
3
4
-- expected status: 0
