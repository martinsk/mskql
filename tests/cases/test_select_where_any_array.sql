-- WHERE col = ANY(ARRAY[...]) should match any value in the array
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id FROM t1 WHERE val = ANY(ARRAY[10, 30]) ORDER BY id;
-- expected output:
1
3
-- expected status: 0
