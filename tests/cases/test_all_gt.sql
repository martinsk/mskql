-- ALL operator with > should match only if greater than all array elements
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 50), (3, 30);
-- input:
SELECT id FROM t1 WHERE val > ALL(ARRAY[5, 20]) ORDER BY id;
-- expected output:
2
3
-- expected status: 0
