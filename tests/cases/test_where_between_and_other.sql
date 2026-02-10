-- WHERE with BETWEEN combined with AND
-- setup:
CREATE TABLE t1 (id INT, val INT, active INT);
INSERT INTO t1 (id, val, active) VALUES (1, 5, 1), (2, 15, 1), (3, 25, 0), (4, 35, 1);
-- input:
SELECT id FROM t1 WHERE val BETWEEN 10 AND 30 AND active = 1 ORDER BY id;
-- expected output:
2
-- expected status: 0
