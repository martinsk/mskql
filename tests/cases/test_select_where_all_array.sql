-- WHERE col > ALL(ARRAY[...]) should match only if greater than all values
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id FROM t1 WHERE val > ALL(ARRAY[5, 15]) ORDER BY id;
-- expected output:
2
3
-- expected status: 0
