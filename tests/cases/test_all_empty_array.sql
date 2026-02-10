-- ALL with empty array should return true for all rows (vacuous truth)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20);
-- input:
SELECT id FROM t1 WHERE val > ALL(ARRAY[]) ORDER BY id;
-- expected output:
1
2
-- expected status: 0
