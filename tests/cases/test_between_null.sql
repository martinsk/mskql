-- BETWEEN with NULL column value should not match (three-valued logic)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 5), (2, NULL), (3, 15);
-- input:
SELECT id FROM t1 WHERE val BETWEEN 1 AND 10 ORDER BY id;
-- expected output:
1
-- expected status: 0
