-- Multi-column radix sort: 2x INT keys (32+32=64 bits, fits composite)
-- setup:
CREATE TABLE t (id INT, a INT, b INT);
INSERT INTO t VALUES (1, 10, 30);
INSERT INTO t VALUES (2, 10, 10);
INSERT INTO t VALUES (3, 20, 20);
INSERT INTO t VALUES (4, 20, 10);
INSERT INTO t VALUES (5, 10, 20);
INSERT INTO t VALUES (6, 30, 10);
-- input:
SELECT id, a, b FROM t ORDER BY a ASC, b ASC;
-- expected output:
2|10|10
5|10|20
1|10|30
4|20|10
3|20|20
6|30|10
-- input:
SELECT id, a, b FROM t ORDER BY a DESC, b ASC;
-- expected output:
6|30|10
4|20|10
3|20|20
2|10|10
5|10|20
1|10|30
-- input:
SELECT id, a, b FROM t ORDER BY a ASC, b DESC;
-- expected output:
1|10|30
5|10|20
2|10|10
3|20|20
4|20|10
6|30|10
-- input:
SELECT id, a, b FROM t ORDER BY a DESC, b DESC;
-- expected output:
6|30|10
3|20|20
4|20|10
1|10|30
5|10|20
2|10|10
