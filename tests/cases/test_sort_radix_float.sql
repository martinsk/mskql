-- Single-key FLOAT radix sort: exercises radix_sort_f64 path
-- setup:
CREATE TABLE t (id INT, score FLOAT);
INSERT INTO t VALUES (1, 3.14);
INSERT INTO t VALUES (2, -1.5);
INSERT INTO t VALUES (3, 0.0);
INSERT INTO t VALUES (4, 99.9);
INSERT INTO t VALUES (5, -99.9);
INSERT INTO t VALUES (6, 1.0);
INSERT INTO t VALUES (7, -0.001);
-- input:
SELECT id, score FROM t ORDER BY score ASC;
-- expected output:
5|-99.9
2|-1.5
7|-0.001
3|0
6|1
1|3.14
4|99.9
-- input:
SELECT id, score FROM t ORDER BY score DESC;
-- expected output:
4|99.9
1|3.14
6|1
3|0
7|-0.001
2|-1.5
5|-99.9
-- input:
SELECT id, score FROM t ORDER BY score ASC LIMIT 3;
-- expected output:
5|-99.9
2|-1.5
7|-0.001
-- input:
SELECT id, score FROM t ORDER BY score DESC LIMIT 3;
-- expected output:
4|99.9
1|3.14
6|1
