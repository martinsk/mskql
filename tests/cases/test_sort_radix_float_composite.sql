-- Multi-key composite radix sort with FLOAT: INT + FLOAT (32+64=96 bits, exceeds 64)
-- Falls back to type-specialized comparator. Also tests single FLOAT key with NULLs.
-- setup:
CREATE TABLE t (id INT, category INT, score FLOAT);
INSERT INTO t VALUES (1, 1, 3.14);
INSERT INTO t VALUES (2, 1, -1.5);
INSERT INTO t VALUES (3, 2, 0.0);
INSERT INTO t VALUES (4, 2, 99.9);
INSERT INTO t VALUES (5, 1, 1.0);
INSERT INTO t VALUES (6, 2, -5.0);
-- input:
SELECT id, category, score FROM t ORDER BY category ASC, score ASC;
-- expected output:
2|1|-1.5
5|1|1
1|1|3.14
6|2|-5
3|2|0
4|2|99.9
-- input:
SELECT id, category, score FROM t ORDER BY category DESC, score DESC;
-- expected output:
4|2|99.9
3|2|0
6|2|-5
1|1|3.14
5|1|1
2|1|-1.5
