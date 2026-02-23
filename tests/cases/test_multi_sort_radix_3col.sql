-- Multi-column radix sort: 3x SMALLINT keys (16+16+16=48 bits, fits composite)
-- setup:
CREATE TABLE t (id INT, a SMALLINT, b SMALLINT, c SMALLINT);
INSERT INTO t VALUES (1, 1, 2, 3);
INSERT INTO t VALUES (2, 1, 2, 1);
INSERT INTO t VALUES (3, 1, 1, 2);
INSERT INTO t VALUES (4, 2, 1, 1);
INSERT INTO t VALUES (5, 2, 1, 3);
INSERT INTO t VALUES (6, 1, 1, 1);
-- input:
SELECT id, a, b, c FROM t ORDER BY a ASC, b ASC, c ASC;
-- expected output:
6|1|1|1
3|1|1|2
2|1|2|1
1|1|2|3
4|2|1|1
5|2|1|3
-- input:
SELECT id, a, b, c FROM t ORDER BY a DESC, b ASC, c DESC;
-- expected output:
5|2|1|3
4|2|1|1
3|1|1|2
6|1|1|1
1|1|2|3
2|1|2|1
