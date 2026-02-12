-- JOIN with compound ON condition (AND)
-- setup:
CREATE TABLE t1 (id INT, a INT);
INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
CREATE TABLE t2 (id INT, b INT);
INSERT INTO t2 VALUES (1, 10), (2, 99), (3, 30);
-- input:
SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id AND t1.a = t2.b ORDER BY t1.id;
-- expected output:
1
3
-- expected status: 0
