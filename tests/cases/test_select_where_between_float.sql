-- BETWEEN with float boundaries
-- setup:
CREATE TABLE t1 (id INT, val FLOAT);
INSERT INTO t1 (id, val) VALUES (1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5);
-- input:
SELECT id FROM t1 WHERE val BETWEEN 2.0 AND 4.0 ORDER BY id;
-- expected output:
2
3
-- expected status: 0
