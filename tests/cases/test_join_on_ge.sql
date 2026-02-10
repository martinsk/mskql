-- JOIN with >= operator in ON clause (non-equi join)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20);
CREATE TABLE t2 (id INT, threshold INT);
INSERT INTO t2 (id, threshold) VALUES (1, 10), (2, 25);
-- input:
SELECT t1.id, t2.id FROM t1 JOIN t2 ON t1.val >= t2.threshold ORDER BY t1.id, t2.id;
-- expected output:
1|1
2|1
-- expected status: 0
