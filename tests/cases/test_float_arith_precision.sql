-- float arithmetic should preserve precision
-- setup:
CREATE TABLE t1 (id INT, val FLOAT);
INSERT INTO t1 (id, val) VALUES (1, 1.5), (2, 2.5);
-- input:
SELECT id, val * 2 FROM t1 ORDER BY id;
-- expected output:
1|3
2|5
-- expected status: 0
