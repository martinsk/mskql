-- Multi-column radix sort with NULLs
-- setup:
CREATE TABLE t (id INT, a INT, b INT);
INSERT INTO t VALUES (1, 10, 30);
INSERT INTO t VALUES (2, NULL, 10);
INSERT INTO t VALUES (3, 20, NULL);
INSERT INTO t VALUES (4, 10, 10);
INSERT INTO t VALUES (5, NULL, NULL);
INSERT INTO t VALUES (6, 20, 20);
-- input:
SELECT id, a, b FROM t ORDER BY a ASC, b ASC;
-- expected output:
4|10|10
1|10|30
6|20|20
3|20|
2||10
5||
-- input:
SELECT id, a, b FROM t ORDER BY a DESC, b DESC;
-- expected output:
5||
2||10
3|20|
6|20|20
1|10|30
4|10|10
-- input:
SELECT id, a, b FROM t ORDER BY a ASC NULLS FIRST, b ASC NULLS FIRST;
-- expected output:
5||
2||10
4|10|10
1|10|30
3|20|
6|20|20
